//! The recipe's steps: set tenants up, ingest their load as `insert` operations, keep their tables compact with
//! `plan`, `merge` and `housekeep`, check every tenant, and query a sample through read-only fleets.

use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::StreamExt;
use futures_util::stream;
use libch::catalog::Catalog;
use libch::inodedb::Grant;
use libch::keys::{KeyCache, KeyPolicy};
use libch::pool::{Pool, PoolOptions};
use libch::stock::{Blank, Exclusive, Fleet};
use libch::worker::{OpWriter, Usage};
use libch::writer::{Budget, Insert, MergeCandidate, TableName};
use libch::{Error, Outcome, Tenant, Writer, check};
use libch_testkit::schema::{EVENTS_SCHEMA, daily, events};
use libch_testkit::{Stack, TenantFixture};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use crate::load::{Batch, Buffer, Zipf};
use crate::report::Report;

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
    /// Kills the worker of one insert in this many, at a moment of its own.
    pub kill_one_in: Option<u32>,
    /// Tenants set up at a time.
    pub setups_at_once: usize,
    pub seed: u64,
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
            kill_one_in: None,
            setups_at_once: 8,
            seed: 1,
        }
    }
}

/// How long an operation may run before its worker is killed.
const DEADLINE: Duration = Duration::from_secs(120);
/// How often a batch goes out before the run gives up on it.
const ATTEMPTS: u32 = 8;
/// Events the run holds in buffers before it stops generating.
const MAX_BUFFERED_ROWS: usize = 4_000_000;

/// What one step shares with its tasks.
#[derive(Clone)]
struct Shared {
    pool: Pool,
    keys: KeyCache,
    settings: Arc<str>,
    compactions: Arc<Semaphore>,
    part_threshold: usize,
}

impl Shared {
    async fn writer(&self, tenant: &Tenant) -> Result<OpWriter, Error> {
        Ok(self.pool.writer(tenant, self.keys.key(tenant).await?).with_settings_xml(&*self.settings).with_deadline(DEADLINE))
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

enum Done {
    Insert(Inserted),
    Compact(Compacted),
}

fn add(total: &mut Usage, more: Usage) {
    total.operations += more.operations;
    total.inodedb_commits += more.inodedb_commits;
    total.inodedb_stats += more.inodedb_stats;
    total.inodedb_reads += more.inodedb_reads;
    total.inodedb_lists += more.inodedb_lists;
    total.s3_reads += more.s3_reads;
    total.s3_writes += more.s3_writes;
    total.cpu += more.cpu;
    total.peak_anonymous_memory_bytes = total.peak_anonymous_memory_bytes.max(more.peak_anonymous_memory_bytes);
    total.peak_threads = total.peak_threads.max(more.peak_threads);
}

/// 6A: tenants, each with a directory in the store, an S3 prefix and the demo's schema.
async fn setup(stack: &Stack, config: &Config) -> Result<Vec<TenantFixture>, Error> {
    let creations = (0..config.tenants).map(|_| async move {
        let fixture = TenantFixture::create(stack).await?;
        Exclusive::run(&stack.stock_env(), &fixture.tenant, &fixture.key, &EVENTS_SCHEMA).await?;
        Ok::<_, Error>(fixture)
    });
    stream::iter(creations).buffer_unordered(config.setups_at_once).collect::<Vec<_>>().await.into_iter().collect()
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
        add(&mut usage, writer.usage());
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
    let _turn = shared.compactions.acquire().await.expect("the semaphore stays open");
    let mut done = Compacted { tenant: index, ..Compacted::default() };
    let writer = shared.writer(&tenant).await?;
    let key = shared.keys.key(&tenant).await?;
    let catalog = Catalog::connect(&tenant, key.as_str()).await?;
    for round in 0..4 {
        let crowded = catalog.crowded_tables(shared.part_threshold).await?;
        if crowded.is_empty() {
            break;
        }
        for table in &crowded {
            let name = &table.table.name;
            let planned = writer.plan(name, &Budget { max_merges: 4 }).await?;
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
                match writer.merge(name, candidate).await? {
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
        if !to_the_threshold || round == 3 {
            break;
        }
    }
    done.usage = writer.usage();
    Ok(Done::Compact(done))
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
    let shared = Shared {
        pool: Pool::new(stack.worker_command(), PoolOptions { waiting: config.waiting_workers, concurrency: config.operations_at_once })?,
        keys: KeyCache::new(stack.admin().await?, KeyPolicy::default())?,
        settings: format!("<merge_tree><old_parts_lifetime>{grace_seconds}</old_parts_lifetime></merge_tree>").into(),
        compactions: Arc::new(Semaphore::new(config.compactions_at_once)),
        part_threshold: config.part_threshold,
    };

    // 6B and 6C: generate, batch and insert, and compact a tenant each time it has taken a threshold's worth
    // of batches.
    let started = Instant::now();
    let mut zipf = Zipf::new(config.tenants, config.zipf_exponent, config.seed);
    let mut buffers: Vec<Buffer> = (0..config.tenants).map(|_| Buffer::default()).collect();
    let mut inserting = vec![false; config.tenants];
    let mut compacting = vec![false; config.tenants];
    let mut batches_since_scan = vec![0usize; config.tenants];
    let mut merged_tables: Vec<Vec<TableName>> = vec![Vec::new(); config.tenants];
    let total = config.batches * config.rows_per_batch;
    let mut remaining = total;
    let (mut buffered, mut inserts_running, mut cursor) = (0usize, 0usize, 0usize);
    let mut tasks: JoinSet<Result<Done, Error>> = JoinSet::new();
    loop {
        // The load so far: all of it, or what the rate has let arrive.
        let arrived = config.events_per_second.map_or(total, |rate| ((started.elapsed().as_secs_f64() * rate as f64) as usize).min(total));
        let (mut generated, mut started_inserts) = (0, 0);
        while total - remaining < arrived && buffered < MAX_BUFFERED_ROWS && generated < 50_000 {
            buffers[zipf.tenant()].push(config.rows_per_batch);
            (remaining, buffered, generated) = (remaining - 1, buffered + 1, generated + 1);
        }

        // Tenants take turns from where the last pass stopped, so the busiest never crowd out the rest.
        for step in 0..config.tenants {
            if inserts_running >= config.operations_at_once * 2 {
                break;
            }
            let index = (cursor + step) % config.tenants;
            let buffer = &mut buffers[index];
            let due = buffer.rows() >= config.rows_per_batch || remaining == 0 || buffer.since().is_some_and(|since| since.elapsed() >= config.max_batch_age);
            if inserting[index] || buffer.rows() == 0 || !due {
                continue;
            }
            let batch = buffer.take(index, config.max_batch_rows).expect("the buffer holds rows");
            buffered -= batch.rows;
            let kill_after =
                config.kill_one_in.filter(|one_in| zipf.unit() * f64::from(*one_in) < 1.0).map(|_| Duration::from_millis(20 + (zipf.unit() * 300.0) as u64));
            report.kills += u64::from(kill_after.is_some());
            inserting[index] = true;
            inserts_running += 1;
            started_inserts += 1;
            tasks.spawn(insert(shared.clone(), fixtures[index].tenant.clone(), index, batch, kill_after));
        }
        cursor = (cursor + 1) % config.tenants;

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
                    if batches_since_scan[inserted.tenant] >= config.part_threshold && !compacting[inserted.tenant] {
                        batches_since_scan[inserted.tenant] = 0;
                        compacting[inserted.tenant] = true;
                        tasks.spawn(compact(shared.clone(), fixtures[inserted.tenant].tenant.clone(), inserted.tenant, false));
                    }
                }
                Done::Compact(compacted) => {
                    compacting[compacted.tenant] = false;
                    report.record_compaction(compacted.plans, compacted.merges, compacted.claims_lost, compacted.usage);
                    for table in compacted.merged_tables {
                        if !merged_tables[compacted.tenant].contains(&table) {
                            merged_tables[compacted.tenant].push(table);
                        }
                    }
                }
            }
        }
    }
    report.ingest = started.elapsed();

    // 6D, to the end: every partition at or under the threshold, then the covered parts removed once their
    // grace has passed.
    let started = Instant::now();
    let compactions = fixtures.iter().enumerate().map(|(index, fixture)| compact(shared.clone(), fixture.tenant.clone(), index, true));
    for done in stream::iter(compactions).buffer_unordered(config.compactions_at_once * 2).collect::<Vec<_>>().await {
        let Done::Compact(compacted) = done? else { unreachable!("a compaction reports a compaction") };
        report.record_compaction(compacted.plans, compacted.merges, compacted.claims_lost, compacted.usage);
        for table in compacted.merged_tables {
            if !merged_tables[compacted.tenant].contains(&table) {
                merged_tables[compacted.tenant].push(table);
            }
        }
    }
    tokio::time::sleep(Duration::from_secs(grace_seconds) + Duration::from_millis(1_100)).await;
    let housekeeping =
        fixtures.iter().zip(&merged_tables).flat_map(|(fixture, tables)| tables.iter().map(move |table| (fixture, table))).map(|(fixture, table)| {
            let shared = &shared;
            async move {
                let writer = shared.writer(&fixture.tenant).await?;
                let housekept = writer.housekeep(table).await?.committed().ok_or_else(|| Error::Other(format!("housekeeping of {table} committed nothing")))?;
                Ok::<_, Error>((housekept.removed.len() as u64, housekept.remaining.len() as u64, writer.usage()))
            }
        });
    for housekept in stream::iter(housekeeping).buffer_unordered(config.operations_at_once).collect::<Vec<_>>().await {
        let (removed, remaining, usage) = housekept?;
        report.record_housekeeping(removed, remaining, usage);
    }
    report.compaction = started.elapsed();

    // Every tenant against the generator: its rows from the parts' own counts, its partitions at or under the
    // threshold, and no covered part.
    let started = Instant::now();
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
        if state.covered > 0 {
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
    let invariants = sample(config.checked_tenants).into_iter().map(|index| {
        let fixture = &fixtures[index];
        async move { (index, check::tenant(&fixture.tenant, &fixture.key).await) }
    });
    for (index, checked) in stream::iter(invariants).buffer_unordered(8).collect::<Vec<_>>().await {
        let checked = checked?;
        report.tenants_checked += 1;
        if !checked.is_clean() {
            report.problems.push(format!("tenant {index} breaks an invariant: {checked}"));
        }
    }
    report.verification = started.elapsed();

    // 6E: a fleet of blank servers for each sampled tenant, assigned to it under a read key, queried and let go.
    let started = Instant::now();
    let queries = sample(config.queried_tenants).into_iter().map(|index| {
        let fixture = &fixtures[index];
        async move { (index, query(stack, &fixture.tenant, config.fleet_servers).await) }
    });
    for (index, answer) in stream::iter(queries).buffer_unordered(2).collect::<Vec<_>>().await {
        let (rows, sum, through_the_view) = answer?;
        let expected = (buffers[index].expected_rows, buffers[index].expected_sum, buffers[index].expected_sum);
        report.tenants_queried += 1;
        if (rows, sum, through_the_view) != expected {
            report.problems.push(format!("tenant {index}'s fleet answered {:?}, and the generator expects {expected:?}", (rows, sum, through_the_view)));
        }
    }
    report.queries = started.elapsed();

    report.pool = shared.pool.stats();
    report.keys = shared.keys.stats();
    shared.pool.close().await;
    shared.keys.revoke_all().await?;
    let started = Instant::now();
    let removals = fixtures.into_iter().map(TenantFixture::remove);
    stream::iter(removals).buffer_unordered(config.setups_at_once).collect::<Vec<_>>().await.into_iter().collect::<Result<(), Error>>()?;
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
