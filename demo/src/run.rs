//! The recipe's steps: set tenants up, ingest their load as `insert` operations, keep their tables compact with
//! `plan`, `merge` and `housekeep`, check every tenant, and query a sample through read-only fleets.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use futures_util::StreamExt;
use futures_util::stream;
use libch::catalog::Catalog;
use libch::inodedb::Grant;
use libch::keys::{KeyCache, KeyPolicy};
use libch::pool::{Pool, PoolOptions};
use libch::stock::{Blank, Exclusive, Fleet, StockWriter, StockWriterOptions};
use libch::worker::{OpWriter, Usage};
use libch::writer::{Budget, Insert, MergeCandidate, TableName};
use libch::{Error, Outcome, Tenant, Writer, check};
use libch_testkit::measure::process_cpu;
use libch_testkit::schema::{EVENTS_SCHEMA, daily, events};
use libch_testkit::{Stack, TenantFixture};
use tokio::task::JoinSet;

use crate::load::{Batch, Buffer, Zipf};
use crate::report::{Report, StockInsert, Tally};

/// The size of a run.
#[derive(Clone, Debug)]
pub struct Config {
    pub tenants: usize,
    /// The load in batches of `rows_per_batch`: the run generates their product in events.
    pub batches: usize,
    /// The rows that make a tenant's buffer a batch.
    pub rows_per_batch: usize,
    /// How large a batch grows while the tenant's last insert runs. A tenant has one insert in flight, so a
    /// busy tenant's rows gather meanwhile and go as one larger batch: its throughput grows with its load, and
    /// its inserts never contend for a block number. Equal to `rows_per_batch`, every insert carries one batch.
    pub max_batch_rows: usize,
    /// How long an event waits for its batch to fill.
    pub max_batch_age: Duration,
    /// The rate the load arrives at, in events a second across all tenants. `None` offers the whole load at
    /// once, and the run takes it as fast as its operations go.
    pub events_per_second: Option<u64>,
    /// The skew of the load across tenants.
    pub zipf_exponent: f64,
    /// Operations that run at a time, and workers kept started ahead of them.
    pub operations_at_once: usize,
    pub waiting_workers: usize,
    /// Tenants compacted at a time.
    pub compactions_at_once: usize,
    /// Active parts a partition may hold before the run plans merges for its table.
    pub part_threshold: usize,
    /// How long a covered part stays for queries that started before its merge.
    pub grace: Duration,
    /// Tenants queried through a fleet, and each fleet's servers.
    pub queried_tenants: usize,
    pub fleet_servers: usize,
    /// Tenants whose disks the invariant checker reads at the end.
    pub checked_tenants: usize,
    /// Tenants on which the run ends by measuring the stock recipe: a stock shared writer booted for the tenant,
    /// one batch inserted, and the writer stopped.
    pub stock_sample: usize,
    /// Kills the worker of one insert in this many, at a moment of its own.
    pub kill_one_in: Option<u32>,
    /// Tenants set up at a time.
    pub setups_at_once: usize,
    pub seed: u64,
    /// Whether the run says on stderr how far each step has come.
    pub progress: bool,
    /// How long the run waits for the store's free queue to hold what each checked tenant's removals freed. A
    /// tenant whose blobs cannot be counted by then goes uncounted.
    pub free_queue_wait: Duration,
    /// Ends without the last housekeeping pass, for a store on which that pass cannot finish. The covered parts
    /// that then remain are counted, where a run otherwise reports them as a problem.
    pub skip_last_housekeeping: bool,
    /// Leaves the tenants in the store at the end, for a stack that is wiped afterwards. Removing a tenant claims
    /// its freed files from the store's free queue, which takes seconds a tenant once a large run has filled it.
    pub leave_tenants: bool,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            tenants: 20,
            batches: 400,
            rows_per_batch: 1_000,
            max_batch_rows: 20_000,
            max_batch_age: Duration::from_secs(2),
            events_per_second: None,
            zipf_exponent: 1.0,
            operations_at_once: 16,
            waiting_workers: 8,
            compactions_at_once: 4,
            part_threshold: 8,
            grace: Duration::from_secs(3),
            queried_tenants: 4,
            fleet_servers: 2,
            checked_tenants: 20,
            stock_sample: 3,
            kill_one_in: None,
            setups_at_once: 8,
            seed: 1,
            progress: false,
            free_queue_wait: Duration::from_secs(600),
            skip_last_housekeeping: false,
            leave_tenants: false,
        }
    }
}

/// How long an operation may run before its worker is killed.
const DEADLINE: Duration = Duration::from_secs(120);
/// How often a batch goes out before the run gives up on it.
const ATTEMPTS: u32 = 8;
/// Rounds of plans and merges the last compaction of a tenant runs at most. A round merges up to four ranges a
/// table, so this brings a table that fell some thousands of parts behind to the threshold.
const ROUNDS_TO_THE_THRESHOLD: usize = 64;
/// Events the run holds in buffers before it stops generating.
const MAX_BUFFERED_ROWS: usize = 4_000_000;

/// What one step shares with its tasks.
#[derive(Clone)]
struct Shared {
    pool: Pool,
    keys: KeyCache,
    settings: Arc<str>,
    part_threshold: usize,
    /// Plans, merges and housekeepings that ended in an error and went again.
    failed_steps: Arc<AtomicU64>,
}

impl Shared {
    async fn writer(&self, tenant: &Tenant) -> Result<OpWriter, Error> {
        Ok(self.pool.writer(tenant, self.keys.key(tenant).await?).with_settings_xml(&*self.settings).with_deadline(DEADLINE))
    }

    /// Runs a step of compaction again while it ends in an error, up to `ATTEMPTS` times in all. A worker that
    /// died or outlived its deadline says nothing of what it did, and every operation is safe to repeat.
    async fn again<T, Step: Future<Output = Result<T, Error>>>(&self, mut step: impl FnMut() -> Step) -> Result<T, Error> {
        let mut failed = 0;
        loop {
            match step().await {
                Err(_) if failed + 1 < ATTEMPTS => {
                    failed += 1;
                    self.failed_steps.fetch_add(1, Ordering::Relaxed);
                }
                answer => return answer,
            }
        }
    }
}

/// What an insert task reports.
struct Inserted {
    tenant: usize,
    rows: usize,
    duplicate: bool,
    /// Attempts whose worker failed or was killed, before the one that answered.
    failed: u32,
    counter_conflicts: u64,
    usage: Usage,
}

/// What a compaction of one tenant reports.
#[derive(Default)]
struct Compacted {
    tenant: usize,
    plans: u64,
    merges: u64,
    claims_lost: u64,
    merged_tables: Vec<TableName>,
    usage: Usage,
}

/// What a housekeeping of one tenant's tables reports.
#[derive(Default)]
struct Housekept {
    tenant: usize,
    removed: u64,
    /// Covered parts the tables still hold: those inside their grace period.
    remaining: u64,
    /// The tables that hold covered parts or freed blobs for a later housekeeping.
    unfinished: Vec<TableName>,
    usage: Usage,
}

enum Done {
    Insert(Inserted),
    Compact(Compacted),
    Housekeep(Housekept),
}

/// Says on stderr how far a step has come: when it starts, and then every thirty seconds at most.
struct Progress {
    on: bool,
    step: &'static str,
    started: Instant,
    said: Instant,
}

impl Progress {
    fn start(on: bool, step: &'static str) -> Progress {
        if on {
            eprintln!("{step}: started");
        }
        Progress { on, step, started: Instant::now(), said: Instant::now() }
    }

    fn say(&mut self, detail: impl FnOnce() -> String) {
        if self.on && self.said.elapsed() >= Duration::from_secs(30) {
            eprintln!("{}: {} after {:.0} s", self.step, detail(), self.started.elapsed().as_secs_f64());
            self.said = Instant::now();
        }
    }
}

/// Adds the tables a compaction merged, or a housekeeping left unfinished, to those that await a housekeeping.
fn await_housekeeping(awaiting: &mut Vec<TableName>, tables: Vec<TableName>) {
    for table in tables {
        if !awaiting.contains(&table) {
            awaiting.push(table);
        }
    }
}

/// 6A: tenants, each with a directory in the store, an S3 prefix and the demo's schema.
async fn setup(stack: &Stack, config: &Config) -> Result<Vec<TenantFixture>, Error> {
    let creations = (0..config.tenants).map(|_| async move {
        let mut fixture = TenantFixture::create(stack).await?;
        if config.leave_tenants {
            fixture.leave();
        }
        Exclusive::run(&stack.stock_env(), &fixture.tenant, &fixture.key, &EVENTS_SCHEMA).await?;
        // The exclusive writer has stopped, so the disk goes to the workers, whose operations it fenced.
        OpWriter::new(stack.worker_command(), &fixture.tenant, &fixture.key).share().await?;
        Ok::<_, Error>(fixture)
    });
    let mut progress = Progress::start(config.progress, "setup");
    let mut creations = stream::iter(creations).buffer_unordered(config.setups_at_once);
    let mut created = Vec::with_capacity(config.tenants);
    while let Some(fixture) = creations.next().await {
        created.push(fixture);
        progress.say(|| format!("{} of {} tenants", created.len(), config.tenants));
    }
    created.into_iter().collect()
}

/// 6C: one batch as an `insert` operation. A batch whose worker failed, or was killed, goes again under the
/// same token, so it lands once however many times it is sent.
async fn insert(shared: Shared, tenant: Tenant, index: usize, batch: Batch, kill_after: Option<Duration>) -> Result<Done, Error> {
    let table = events();
    let (mut failed, mut usage) = (0, Usage::default());
    loop {
        let mut writer = shared.writer(&tenant).await?;
        if let (0, Some(after)) = (failed, kill_after) {
            writer = writer.with_deadline(after);
        }
        let result = writer.insert(Insert { table: &table, format: "JSONEachRow", token: &batch.token, data: batch.data.clone() }).await;
        usage += writer.usage();
        let (duplicate, counter_conflicts) = match result {
            Ok(Outcome::Committed(inserted)) => (false, inserted.counter_conflicts),
            Ok(Outcome::Duplicate) => (true, 0),
            Ok(other) => return Err(Error::Other(format!("batch {} ended {}", batch.token, other.name()))),
            Err(_) if failed + 1 < ATTEMPTS => {
                failed += 1;
                continue;
            }
            Err(error) => return Err(error),
        };
        return Ok(Done::Insert(Inserted { tenant: index, rows: batch.rows, duplicate, failed, counter_conflicts, usage }));
    }
}

/// 6D: one tenant's compaction. A scan of the catalog names the tables with a partition over the threshold; a
/// plan proposes merges for each, and each merge runs as an operation. With `to_the_threshold` a partition the
/// plan leaves over the threshold merges whole: the selector waits for a few small parts to age, and the caller
/// names them itself.
async fn compact(shared: Shared, tenant: Tenant, index: usize, to_the_threshold: bool) -> Result<Done, Error> {
    let mut done = Compacted { tenant: index, ..Compacted::default() };
    let writer = shared.writer(&tenant).await?;
    let key = shared.keys.key(&tenant).await?;
    let catalog = Catalog::connect(&tenant, key.as_str()).await?;
    let rounds = if to_the_threshold { ROUNDS_TO_THE_THRESHOLD } else { 1 };
    let budget = Budget { max_merges: 4 };
    for _ in 0..rounds {
        let crowded = catalog.crowded_tables(shared.part_threshold).await?;
        if crowded.is_empty() {
            break;
        }
        for table in &crowded {
            let name = &table.table.name;
            let planned = shared.again(|| writer.plan(name, &budget)).await?;
            done.plans += 1;
            let mut candidates: Vec<MergeCandidate> = planned.into_iter().map(|planned| planned.candidate).collect();
            if candidates.is_empty() && to_the_threshold {
                let parts = catalog.part_set(&table.table).await?;
                candidates = table
                    .partitions
                    .iter()
                    .map(|partition| MergeCandidate {
                        partition_id: partition.partition_id.clone(),
                        sources: parts.active_in(&partition.partition_id).cloned().collect(),
                    })
                    .collect();
            }
            for candidate in &candidates {
                match shared.again(|| writer.merge(name, candidate)).await? {
                    Outcome::Committed(_) => {
                        done.merges += 1;
                        if !done.merged_tables.contains(name) {
                            done.merged_tables.push(name.clone());
                        }
                    }
                    Outcome::ClaimLost => done.claims_lost += 1,
                    other => return Err(Error::Other(format!("a merge of {name} ended {}", other.name()))),
                }
            }
        }
    }
    done.usage = writer.usage();
    Ok(Done::Compact(done))
}

/// 6D: housekeeping for tables that merged, once `after` has passed: the grace period of the parts the merges
/// covered. A table that still holds covered parts inside their grace, or freed blobs that a lease holds back, is
/// named for a later housekeeping.
async fn housekeep(shared: Shared, tenant: Tenant, index: usize, tables: Vec<TableName>, after: Duration) -> Result<Done, Error> {
    tokio::time::sleep(after).await;
    let writer = shared.writer(&tenant).await?;
    let mut done = Housekept { tenant: index, ..Housekept::default() };
    for table in tables {
        let housekept = shared.again(|| writer.housekeep(&table)).await?;
        let housekept = housekept.committed().ok_or_else(|| Error::Other(format!("housekeeping of {table} committed nothing")))?;
        done.removed += housekept.removed.len() as u64;
        done.remaining += housekept.remaining.len() as u64;
        if !housekept.remaining.is_empty() || housekept.blobs_remain {
            done.unfinished.push(table);
        }
    }
    done.usage = writer.usage();
    Ok(Done::Housekeep(done))
}

/// What a tenant's fleet answered, beside what the generator expects.
async fn query(stack: &Stack, tenant: &Tenant, servers: usize) -> Result<(u64, u64, u64), Error> {
    let read_key = tenant.mint_key(&stack.admin().await?, Grant::Read, Duration::from_secs(600)).await?;
    let fleet = Fleet::assign(Blank::boot_many(&stack.stock_env(), servers).await?, tenant, &read_key).await?;
    let events = fleet.query("SELECT count(), sum(v) FROM d.events").await;
    let daily = fleet.query("SELECT sumMerge(total) FROM d.daily").await;
    fleet.stop().await?;
    let numbers: Vec<u64> = format!("{} {}", events?, daily?).split_whitespace().filter_map(|number| number.parse().ok()).collect();
    match numbers[..] {
        [rows, sum, through_the_view] => Ok((rows, sum, through_the_view)),
        _ => Err(Error::Other(format!("a fleet answered {numbers:?}, where three numbers are expected"))),
    }
}

/// Runs the recipe and reports what it did. An `Err` means a step failed outright; a run that finished with
/// wrong totals says so in [`Report::problems`].
pub async fn run(stack: &Stack, config: &Config) -> Result<Report, Error> {
    let mut report = Report { tenants: config.tenants, ..Report::default() };

    let started = Instant::now();
    let fixtures = setup(stack, config).await?;
    report.setup = started.elapsed();

    let grace_seconds = config.grace.as_secs().max(1);
    let tally = Arc::new(Tally::default());
    let options = PoolOptions { waiting: config.waiting_workers, concurrency: config.operations_at_once };
    let shared = Shared {
        pool: Pool::with_observer(stack.worker_command(), options, tally.clone())?,
        keys: KeyCache::new(stack.admin().await?, KeyPolicy::default())?,
        settings: format!("<merge_tree><old_parts_lifetime>{grace_seconds}</old_parts_lifetime></merge_tree>").into(),
        part_threshold: config.part_threshold,
        failed_steps: Arc::default(),
    };

    // 6B, 6C and 6D: generate, batch and insert; compact a tenant each time it has taken a threshold's worth of
    // batches; and housekeep the tables a compaction merged once the grace period has passed.
    let started = Instant::now();
    let mut progress = Progress::start(config.progress, "ingest");
    let after_grace = Duration::from_secs(grace_seconds) + Duration::from_millis(1_100);
    let mut zipf = Zipf::new(config.tenants, config.zipf_exponent, config.seed);
    let mut buffers: Vec<Buffer> = (0..config.tenants).map(|_| Buffer::default()).collect();
    let mut inserting = vec![false; config.tenants];
    let mut compacting = vec![false; config.tenants];
    let mut housekeeping = vec![false; config.tenants];
    let mut batches_since_scan = vec![0usize; config.tenants];
    // Each tenant's tables that merged since their last housekeeping.
    let mut awaiting: Vec<Vec<TableName>> = vec![Vec::new(); config.tenants];
    let total = config.batches * config.rows_per_batch;
    let mut remaining = total;
    let (mut buffered, mut inserts_running, mut compactions_running) = (0usize, 0usize, 0usize);
    let mut tasks: JoinSet<Result<Done, Error>> = JoinSet::new();
    loop {
        // The load so far: all of it, or what the rate has let arrive.
        let arrived = config.events_per_second.map_or(total, |rate| ((started.elapsed().as_secs_f64() * rate as f64) as usize).min(total));
        let (mut generated, mut started_inserts) = (0, 0);
        while total - remaining < arrived && buffered < MAX_BUFFERED_ROWS && generated < 50_000 {
            buffers[zipf.tenant()].push(config.rows_per_batch);
            (remaining, buffered, generated) = (remaining - 1, buffered + 1, generated + 1);
        }

        // A due tenant's turn comes by its backlog: the rows it holds, and a batch's worth for each batch age its
        // oldest event has waited. The busiest tenants go first while they hold the most, so each of their batches
        // carries what they gathered and none of them falls behind its load; a quiet tenant's few rows rise to
        // the front as they wait.
        let free = (config.operations_at_once * 2).saturating_sub(inserts_running);
        if free > 0 {
            let mut due: Vec<(u64, usize)> = buffers
                .iter()
                .enumerate()
                .filter_map(|(index, buffer)| {
                    let waited = buffer.since()?.elapsed();
                    let ripe = buffer.rows() >= config.rows_per_batch || remaining == 0 || waited >= config.max_batch_age;
                    let ages = waited.as_secs_f64() / config.max_batch_age.as_secs_f64().max(0.001);
                    (ripe && !inserting[index]).then(|| ((buffer.rows() as f64 + ages * config.rows_per_batch as f64) as u64, index))
                })
                .collect();
            if due.len() > free {
                due.select_nth_unstable_by(free - 1, |one, other| other.cmp(one));
                due.truncate(free);
            }
            for (_, index) in due {
                let batch = buffers[index].take(index, config.max_batch_rows).expect("the buffer holds rows");
                buffered -= batch.rows;
                let kill_after = config
                    .kill_one_in
                    .filter(|one_in| zipf.unit() * f64::from(*one_in) < 1.0)
                    .map(|_| Duration::from_millis(20 + (zipf.unit() * 300.0) as u64));
                report.kills += u64::from(kill_after.is_some());
                inserting[index] = true;
                inserts_running += 1;
                started_inserts += 1;
                tasks.spawn(insert(shared.clone(), fixtures[index].tenant.clone(), index, batch, kill_after));
            }
        }

        // A compaction's turn comes by need as well: the tenant that has taken the most batches since its last
        // one goes first, so the busiest tenant's parts are merged as fast as it makes them. Once the load has
        // landed, the pass after this loop brings every tenant to the threshold.
        let landed = remaining == 0 && buffered == 0 && inserts_running == 0;
        while compactions_running < config.compactions_at_once && !landed {
            let neediest = (0..config.tenants)
                .filter(|&index| batches_since_scan[index] >= config.part_threshold && !compacting[index])
                .max_by_key(|&index| batches_since_scan[index]);
            let Some(index) = neediest else { break };
            batches_since_scan[index] = 0;
            compacting[index] = true;
            compactions_running += 1;
            tasks.spawn(compact(shared.clone(), fixtures[index].tenant.clone(), index, false));
        }

        if tasks.is_empty() && remaining == 0 && buffered == 0 {
            break;
        }
        // A pass that moved nothing waits for a task to finish, or a moment for load to arrive and batches to age.
        let mut finished = Vec::new();
        if generated == 0 && started_inserts == 0 {
            tokio::select! {
                Some(done) = tasks.join_next() => finished.push(done),
                () = tokio::time::sleep(Duration::from_millis(10)) => {}
            }
        }
        while let Some(more) = tasks.try_join_next() {
            finished.push(more);
        }
        for done in finished {
            match done.map_err(|error| Error::Other(format!("a task of the run failed: {error}")))?? {
                Done::Insert(inserted) => {
                    inserting[inserted.tenant] = false;
                    inserts_running -= 1;
                    report.record_insert(inserted.rows, inserted.duplicate, inserted.failed, inserted.counter_conflicts, inserted.usage);
                    batches_since_scan[inserted.tenant] += 1;
                }
                Done::Compact(compacted) => {
                    let tenant = compacted.tenant;
                    compacting[tenant] = false;
                    compactions_running -= 1;
                    report.record_compaction(compacted.plans, compacted.merges, compacted.claims_lost, compacted.usage);
                    await_housekeeping(&mut awaiting[tenant], compacted.merged_tables);
                    if !housekeeping[tenant] && !awaiting[tenant].is_empty() {
                        housekeeping[tenant] = true;
                        let tables = std::mem::take(&mut awaiting[tenant]);
                        tasks.spawn(housekeep(shared.clone(), fixtures[tenant].tenant.clone(), tenant, tables, after_grace));
                    }
                }
                Done::Housekeep(housekept) => {
                    housekeeping[housekept.tenant] = false;
                    report.record_housekeeping(housekept.removed, 0, housekept.usage);
                    await_housekeeping(&mut awaiting[housekept.tenant], housekept.unfinished);
                }
            }
        }
        progress.say(|| {
            format!(
                "{} of {total} rows offered, {} landed in {} inserts, {} merges, {} covered parts removed",
                total - remaining,
                report.rows,
                report.batches,
                report.merges,
                report.parts_removed
            )
        });
    }
    report.ingest = started.elapsed();

    // 6D, to the end: every partition at or under the threshold, then the covered parts removed once their
    // grace has passed.
    let started = Instant::now();
    let mut progress = Progress::start(config.progress, "compaction");
    let compactions = fixtures.iter().enumerate().map(|(index, fixture)| compact(shared.clone(), fixture.tenant.clone(), index, true));
    let mut compactions = stream::iter(compactions).buffer_unordered(config.compactions_at_once * 2);
    let mut compacted_tenants = 0;
    while let Some(done) = compactions.next().await {
        let Done::Compact(compacted) = done? else { unreachable!("a compaction reports a compaction") };
        report.record_compaction(compacted.plans, compacted.merges, compacted.claims_lost, compacted.usage);
        await_housekeeping(&mut awaiting[compacted.tenant], compacted.merged_tables);
        compacted_tenants += 1;
        progress.say(|| format!("{compacted_tenants} of {} tenants brought to the threshold", config.tenants));
    }
    drop(compactions);

    // The last housekeeping of each tenant whose tables merged since their last one. The tables that still hold
    // something go once more, together, after another grace period: the run waits it out once, with no operation
    // held meanwhile.
    tokio::time::sleep(after_grace).await;
    let mut progress = Progress::start(config.progress, "housekeeping");
    if config.skip_last_housekeeping {
        awaiting.iter_mut().for_each(Vec::clear);
    }
    let mut pending: Vec<(usize, Vec<TableName>)> = std::mem::take(&mut awaiting).into_iter().enumerate().filter(|(_, tables)| !tables.is_empty()).collect();
    let mut housekept_tenants = 0;
    for again in [false, true] {
        if again {
            if pending.is_empty() {
                break;
            }
            report.housekept_twice = pending.len();
            tokio::time::sleep(after_grace).await;
        }
        let housekeepings = std::mem::take(&mut pending)
            .into_iter()
            .map(|(index, tables)| housekeep(shared.clone(), fixtures[index].tenant.clone(), index, tables, Duration::ZERO));
        let mut housekeepings = stream::iter(housekeepings).buffer_unordered(config.operations_at_once);
        while let Some(done) = housekeepings.next().await {
            let Done::Housekeep(mut housekept) = done? else { unreachable!("a housekeeping reports a housekeeping") };
            if !again && !housekept.unfinished.is_empty() {
                report.record_housekeeping(housekept.removed, 0, housekept.usage);
                pending.push((housekept.tenant, std::mem::take(&mut housekept.unfinished)));
                continue;
            }
            report.record_housekeeping(housekept.removed, housekept.remaining, housekept.usage);
            housekept_tenants += 1;
            progress.say(|| format!("{housekept_tenants} tenants housekept, {} covered parts removed in all", report.parts_removed));
        }
    }
    report.compaction = started.elapsed();

    // Every tenant against the generator: its rows from the parts' own counts, its partitions at or under the
    // threshold, and no covered part.
    let started = Instant::now();
    Progress::start(config.progress, "verification");
    let checks = fixtures.iter().enumerate().map(|(index, fixture)| {
        let (shared, expected) = (&shared, buffers[index].expected_rows);
        async move {
            let key = shared.keys.key(&fixture.tenant).await?;
            let catalog = Catalog::connect(&fixture.tenant, key.as_str()).await?;
            let mut state = TenantState { index, expected, ..TenantState::default() };
            for name in [events(), daily()] {
                let table = catalog.table(&name).await?.ok_or_else(|| Error::Other(format!("tenant {index} has no table {name}")))?;
                let parts = catalog.part_set(&table).await?;
                state.partitions.extend(parts.active_counts().into_iter().map(|(_, active)| active));
                state.covered += parts.covered.len();
                if name == events() {
                    state.rows = catalog.rows(&table).await?;
                }
            }
            Ok::<_, Error>(state)
        }
    });
    for state in stream::iter(checks).buffer_unordered(32).collect::<Vec<_>>().await {
        let state = state?;
        report.rows_expected += state.expected;
        if state.rows != state.expected {
            report.problems.push(format!("tenant {} holds {} rows, and the generator made {}", state.index, state.rows, state.expected));
        }
        if state.covered > 0 && config.skip_last_housekeeping {
            report.covered_left += state.covered;
            report.tenants_with_covered += 1;
        } else if state.covered > 0 {
            report.problems.push(format!("tenant {} keeps {} covered parts", state.index, state.covered));
        }
        for active in state.partitions {
            *report.partitions_by_parts.entry(active).or_default() += 1;
            if active > config.part_threshold {
                report.problems.push(format!("tenant {} ends with a partition of {active} parts", state.index));
            }
        }
    }

    // The invariant checker on a sample, spread across the tenants from the busiest to the quietest.
    let sample = |count: usize| -> Vec<usize> {
        let count = count.min(config.tenants);
        (0..count).map(|step| if count == 1 { 0 } else { step * (config.tenants - 1) / (count - 1) }).collect()
    };
    // The same sample's blobs: what S3 holds for each tenant beyond what its disk names. A tenant's count needs
    // the store's free queue to hold everything the tenant's removals freed, and a store works through removed
    // directories in its own time, so each count goes again until it succeeds or the wait runs out.
    let waiting = Instant::now();
    let mut progress = Progress::start(config.progress, "blob count");
    let invariants = sample(config.checked_tenants).into_iter().map(|index| {
        let fixture = &fixtures[index];
        async move {
            let checked = check::tenant(&fixture.tenant, &fixture.key).await;
            let left = loop {
                match check::orphans(&fixture.tenant, &fixture.key).await {
                    Err(error) if waiting.elapsed() < config.free_queue_wait => drop(error),
                    counted => break counted,
                }
            };
            (index, checked, left, waiting.elapsed())
        }
    });
    let mut invariants = stream::iter(invariants).buffer_unordered(8);
    while let Some((index, checked, left, waited)) = invariants.next().await {
        let checked = checked?;
        report.tenants_checked += 1;
        if !checked.is_clean() {
            report.problems.push(format!("tenant {index} breaks an invariant: {checked}"));
        }
        // A tenant whose count the store's free queue never caught up with goes uncounted: its rows and
        // invariants are checked above.
        match left {
            Ok(left) => {
                report.orphan_blobs += left.orphans.len();
                report.orphan_bytes += left.bytes();
                report.freed_blobs += left.freed;
                report.free_queue_wait = report.free_queue_wait.max(waited);
            }
            Err(error) => {
                report.blobs_uncounted += 1;
                report.uncounted_because.get_or_insert(error.to_string());
            }
        }
        progress.say(|| format!("{} of {} tenants checked", report.tenants_checked, config.checked_tenants.min(config.tenants)));
    }
    drop(invariants);
    report.verification = started.elapsed();

    // 6E: a fleet of blank servers for each sampled tenant, assigned to it under a read key, queried and let go.
    let started = Instant::now();
    let mut progress = Progress::start(config.progress, "queries");
    let queries = sample(config.queried_tenants).into_iter().map(|index| {
        let fixture = &fixtures[index];
        async move { (index, query(stack, &fixture.tenant, config.fleet_servers).await) }
    });
    let mut queries = stream::iter(queries).buffer_unordered(2);
    while let Some((index, answer)) = queries.next().await {
        progress.say(|| format!("{} tenants queried", report.tenants_queried));
        let (rows, sum, through_the_view) = answer?;
        let expected = (buffers[index].expected_rows, buffers[index].expected_sum, buffers[index].expected_sum);
        report.tenants_queried += 1;
        if (rows, sum, through_the_view) != expected {
            report.problems.push(format!("tenant {index}'s fleet answered {:?}, and the generator expects {expected:?}", (rows, sum, through_the_view)));
        }
    }
    drop(queries);
    report.queries = started.elapsed();

    // 6F: the stock recipe on a sample of the same tenants, as they stand at the end of the run. The writer's
    // CPU is read just before it stops, so it covers the boot, the load of the tenant's parts and the insert.
    for index in sample(config.stock_sample) {
        let fixture = &fixtures[index];
        let catalog = fixture.catalog().await?;
        let mut parts = 0;
        for table in catalog.all_tables().await?.iter().filter(|table| table.is_merge_tree()) {
            parts += catalog.part_set(table).await?.active.len();
        }
        let batch = libch_testkit::schema::batch(u64::from(u32::MAX) + index as u64, config.rows_per_batch as u64);
        let started = Instant::now();
        let writer = StockWriter::start(&stack.stock_env(), &fixture.tenant, &fixture.key, &StockWriterOptions::default()).await?;
        writer.insert(Insert { table: &events(), format: "JSONEachRow", token: &batch.token, data: batch.data.clone() }).await?;
        let cpu = writer.server().pid().and_then(process_cpu).unwrap_or_default();
        writer.stop().await?;
        report.stock_inserts.push(StockInsert { parts, cpu, wall: started.elapsed() });
    }

    report.failed_steps = shared.failed_steps.load(Ordering::Relaxed);
    report.operations = tally.counts();
    report.pool = shared.pool.stats();
    report.keys = shared.keys.stats();
    shared.pool.close().await;
    shared.keys.revoke_all().await?;
    let started = Instant::now();
    if config.leave_tenants {
        return Ok(report);
    }
    let mut progress = Progress::start(config.progress, "removal");
    let removals = fixtures.into_iter().map(TenantFixture::remove);
    let mut removals = stream::iter(removals).buffer_unordered(config.setups_at_once);
    let mut removed = Vec::with_capacity(config.tenants);
    while let Some(removal) = removals.next().await {
        removed.push(removal);
        progress.say(|| format!("{} of {} tenants removed", removed.len(), config.tenants));
    }
    removed.into_iter().collect::<Result<(), Error>>()?;
    report.removal = started.elapsed();
    Ok(report)
}

#[derive(Default)]
struct TenantState {
    index: usize,
    expected: u64,
    rows: u64,
    covered: usize,
    /// The active parts of each partition of the tenant's tables.
    partitions: Vec<usize>,
}
