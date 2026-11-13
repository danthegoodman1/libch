//! The stock baselines: what it costs to write through stock ClickHouse processes, which libch's operations
//! are measured against. Writes `BENCHMARKS.md` at the repository's root.
//!
//!     cargo bench --bench stock
//!
//! The benchmark refuses to record on a busy machine. `LIBCH_BENCH_FORCE=1` runs it anyway and prints the figures
//! without recording them.

use std::fmt::Write as _;
use std::process::Command;
use std::time::{Duration, Instant};

use bytes::Bytes;
use libch::catalog::{Catalog, Table, TableKind};
use libch::stock::{DiskMode, Exclusive, Server, ServerConfig, database_disk_xml, disk_xml, merge_tree_xml, policy_xml};
use libch::{Error, Tenant};
use libch_testkit::measure::{children_cpu, load_average, median, own_memory_mb, process_cpu, record, thread_count};
use libch_testkit::{Stack, TenantFixture, preflight};

/// Above this one-minute load average the machine is too busy for CPU figures to mean much.
const QUIET_LOAD: f64 = 2.0;
const RUNS: usize = 5;

/// The schema every measurement inserts into, unless it says otherwise: a table with a projection, and a
/// materialized view into an `AggregatingMergeTree` table.
const SCHEMA: [&str; 5] = [
    "CREATE DATABASE d",
    "CREATE TABLE d.events (ts DateTime, tenant LowCardinality(String), kind LowCardinality(String), v UInt64, \
     PROJECTION p (SELECT kind, sum(v) GROUP BY kind)) ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY (tenant, ts)",
    "CREATE TABLE d.agg (day Date, kind LowCardinality(String), s AggregateFunction(sum, UInt64)) ENGINE = AggregatingMergeTree ORDER BY (day, kind)",
    "CREATE MATERIALIZED VIEW d.mv TO d.agg AS SELECT toDate(ts) AS day, kind, sumState(v) AS s FROM d.events GROUP BY day, kind",
    "CREATE TABLE d.plain (ts DateTime, tenant LowCardinality(String), kind LowCardinality(String), v UInt64) ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY (tenant, ts)",
];

const INSERT_SELECT: &str =
    "INSERT INTO d.events SELECT toDateTime('2026-11-01 00:00:00') + number, 't1', ['a','b','c'][number % 3 + 1], number FROM numbers(1000)";

/// Turns a measured process's background merges off: its selector merges nothing above one byte. A process that
/// serves one batch and exits leaves merging to another, and with merges on it starts merges of the tenant's
/// parts at attach and cancels them as it stops, which costs CPU that has nothing to do with the insert.
const NO_MERGES: &str = "<max_bytes_to_merge_at_max_space_in_pool>1</max_bytes_to_merge_at_max_space_in_pool>";

/// One row of the report.
struct Row {
    name: String,
    cpu: Option<Duration>,
    wall: Option<Duration>,
    memory_threads: Option<(u64, usize)>,
}

/// How many parts each of the tenant's tables holds when a process's whole life is measured. A stock process
/// loads every part of every table it attaches, so its cost grows with them.
const TENANT_SIZES: [usize; 3] = [10, 100, 500];

fn main() {
    let runtime = tokio::runtime::Builder::new_multi_thread().enable_all().build().expect("a runtime");
    if let Err(error) = runtime.block_on(run()) {
        eprintln!("the stock baselines failed: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Error> {
    let stack = Stack::from_env();
    if let Err(error) = preflight::environment(&stack, preflight::BIGHOUSE_REV.trim()).await {
        return Err(Error::Other(error.to_string()));
    }
    let load = load_average();
    let forced = std::env::var_os("LIBCH_BENCH_FORCE").is_some();
    if load > QUIET_LOAD && !forced {
        return Err(Error::Other(format!(
            "the machine's load average is {load:.1}, above {QUIET_LOAD}: CPU figures taken now would mislead. Run again when it is idle, or set LIBCH_BENCH_FORCE=1 to record anyway."
        )));
    }

    let (mut servers, mut locals, mut running, mut warm, mut profile) = (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new());
    for parts in TENANT_SIZES {
        let fixture = tenant_with_parts(&stack, parts).await?;
        let definitions = Catalog::connect(&fixture.tenant, &fixture.key).await?.all_tables().await?;
        servers.push(server_cycle(&stack, &fixture, &definitions, parts).await?);
        locals.push(local_process(&stack, &fixture, parts)?);
        // Each insert adds one part to the events table and one to the view's target.
        let mut inserted = parts + 2 * RUNS;
        if parts == TENANT_SIZES[0] {
            profile = startup_profile(&stack).await?;
            warm.push(assignment(&stack, &fixture, &definitions, parts).await?);
            inserted += RUNS;
        }
        running.push(running_insert(&stack, &fixture, parts).await?);
        inserted += RUNNING_INSERTS as usize;
        unmerged(&fixture, &definitions, inserted).await?;
        if parts == TENANT_SIZES[0] {
            warm.extend(running_writer(&stack, &fixture).await?);
        }
        fixture.remove().await?;
    }
    let mut rows = servers;
    rows.extend(locals);
    rows.extend(running);
    rows.extend(warm);
    rows.push(idle_local(&stack)?);

    let report = report(&stack, &rows, &profile, load);
    print!("{report}");
    if forced {
        // A forced run is for looking at; the file keeps what a quiet machine recorded.
        eprintln!("the load average was {load:.1}, so BENCHMARKS.md is left as it was");
        return Ok(());
    }
    let path = Stack::repo_root().join("BENCHMARKS.md");
    record(&path, &report)?;
    eprintln!("wrote {}", path.display());
    Ok(())
}

/// A tenant with the schema and `parts` unmerged parts in the events table and in the view's target.
async fn tenant_with_parts(stack: &Stack, parts: usize) -> Result<TenantFixture, Error> {
    let fixture = TenantFixture::create(stack).await?;
    let writer = Exclusive::start(&stack.stock_env(), &fixture.tenant, &fixture.key).await?;
    for statement in SCHEMA {
        writer.execute(statement).await?;
    }
    writer.execute("SYSTEM STOP MERGES").await?;
    for _ in 0..parts {
        writer.execute(INSERT_SELECT).await?;
    }
    writer.stop().await?;
    Ok(fixture)
}

/// Fails unless the events table and the view's target each hold `expected` active parts: no measured process
/// merged them.
async fn unmerged(fixture: &TenantFixture, definitions: &[Table], expected: usize) -> Result<(), Error> {
    let catalog = Catalog::connect(&fixture.tenant, &fixture.key).await?;
    for table in definitions.iter().filter(|table| ["events", "agg"].contains(&table.name.table.as_str())) {
        let active = catalog.parts(table).await?.set.active.len();
        if active != expected {
            return Err(Error::Other(format!("`d.{}` holds {active} active parts where {expected} were inserted: a measured process merged", table.name.table)));
        }
    }
    Ok(())
}

/// A blank server whose background merges are off.
async fn blank(stack: &Stack) -> Result<Server, Error> {
    let config = ServerConfig { name: "blank".into(), extra_xml: merge_tree_xml(NO_MERGES), ..ServerConfig::default() };
    Server::start(&stack.stock_env(), &config).await
}

/// Gives a blank server the tenant's disk as its one writer, and attaches the tenant's tables from their
/// definitions in InodeDB, tables before the views that write to them.
async fn assign_writer(server: &Server, tenant: &Tenant, key: &str, definitions: &[Table]) -> Result<(), Error> {
    let config = format!(
        "<clickhouse><storage_configuration><disks>{}</disks><policies>{}</policies></storage_configuration></clickhouse>",
        disk_xml(tenant, key, &DiskMode::Exclusive),
        policy_xml()
    );
    server.add_config("tenant.xml", &config).await?;
    server.query("CREATE DATABASE d ENGINE = Atomic").await?;
    let mut ordered: Vec<&Table> = definitions.iter().collect();
    ordered.sort_by_key(|table| table.kind == TableKind::MaterializedView);
    for table in ordered {
        let object = if table.kind == TableKind::MaterializedView { "MATERIALIZED VIEW" } else { "TABLE" };
        let attach = table.definition.replacen(&format!("ATTACH {object} _ "), &format!("ATTACH {object} {} ", table.name.quoted()), 1);
        server.query(&attach).await?;
    }
    Ok(())
}

/// A server's whole life for one small insert: boot blank, take the tenant's disk, insert, stop.
async fn server_cycle(stack: &Stack, fixture: &TenantFixture, definitions: &[Table], parts: usize) -> Result<Row, Error> {
    let mut cpu = Vec::new();
    let mut wall = Vec::new();
    let mut memory_threads = None;
    for _ in 0..RUNS {
        let (cpu_before, started) = (children_cpu(), Instant::now());
        let mut server = blank(stack).await?;
        assign_writer(&server, &fixture.tenant, &fixture.key, definitions).await?;
        server.query(INSERT_SELECT).await?;
        let pid = server.pid().expect("the server runs");
        memory_threads = own_memory_mb(pid).zip(thread_count(pid));
        server.stop().await?;
        wall.push(started.elapsed());
        cpu.push(children_cpu() - cpu_before);
    }
    Ok(Row {
        name: format!("Stock server: boot, assign, one 1,000-row insert, stop; {parts} parts a table"),
        cpu: Some(median(cpu)),
        wall: Some(median(wall)),
        memory_threads,
    })
}

/// Runs one `clickhouse local` process for `query`, with the tenant's disk in its config when a tenant is
/// given, and returns the median CPU and wall time of [`RUNS`] such processes. `merges` turns the processes'
/// background merges on.
fn local_run(stack: &Stack, fixture: Option<&TenantFixture>, query: &str, merges: bool) -> Result<(Duration, Duration), Error> {
    let work = stack.work_dir.join(format!("bench-local-{:08x}", rand::random::<u32>()));
    std::fs::create_dir_all(&work)?;
    let config = work.join("config.xml");
    if let Some(fixture) = fixture {
        std::fs::write(
            &config,
            format!(
                "<clickhouse><storage_configuration><disks>{}</disks><policies>{}</policies></storage_configuration>{}{}</clickhouse>",
                disk_xml(&fixture.tenant, &fixture.key, &DiskMode::Exclusive),
                policy_xml(),
                merge_tree_xml(if merges { "" } else { NO_MERGES }),
                database_disk_xml()
            ),
        )?;
    }
    let mut cpu = Vec::new();
    let mut wall = Vec::new();
    for run in 0..RUNS {
        // `clickhouse local` loads databases from its disk only when its own path holds a metadata directory.
        let path = work.join(format!("run-{run}"));
        std::fs::create_dir_all(path.join("metadata"))?;
        let mut command = Command::new(&stack.clickhouse);
        command.arg("local").arg("--path").arg(&path).arg("--query").arg(query);
        if fixture.is_some() {
            command.arg("--config-file").arg(&config);
        }
        let (cpu_before, started) = (children_cpu(), Instant::now());
        let output = command.output()?;
        wall.push(started.elapsed());
        cpu.push(children_cpu() - cpu_before);
        if !output.status.success() {
            return Err(Error::Other(format!("clickhouse local failed: {}", String::from_utf8_lossy(&output.stderr).trim())));
        }
    }
    let _ = std::fs::remove_dir_all(&work);
    Ok((median(cpu), median(wall)))
}

/// The same insert as one `clickhouse local` process that takes the disk, inserts and exits.
fn local_process(stack: &Stack, fixture: &TenantFixture, parts: usize) -> Result<Row, Error> {
    let (cpu, wall) = local_run(stack, Some(fixture), INSERT_SELECT, false)?;
    Ok(Row {
        name: format!("`clickhouse local`: one process for the same insert; {parts} parts a table"),
        cpu: Some(cpu),
        wall: Some(wall),
        memory_threads: None,
    })
}

/// Where one `clickhouse local` process spends its time: the same process with one more step in each row, and
/// in the insert row the insert in place of the query. The last row turns background merges on.
///
/// The tenants with parts are the profile's own. A stock process removes the blobs of freed files as it stops,
/// so a tenant that other measurements wrote to adds their leftovers to the first row that attaches it. The
/// processes that run before the measured ones take what the tenant's own setup freed. Merges on would change
/// the tenant the rows after them attach, so that row has a tenant of its own.
async fn startup_profile(stack: &Stack) -> Result<Vec<Row>, Error> {
    let row = |name: &str, (cpu, wall): (Duration, Duration)| Row { name: name.to_string(), cpu: Some(cpu), wall: Some(wall), memory_threads: None };
    let empty = TenantFixture::create(stack).await?;
    let no_parts = tenant_with_parts(stack, 0).await?;
    let ten_parts = tenant_with_parts(stack, TENANT_SIZES[0]).await?;
    let merging = tenant_with_parts(stack, TENANT_SIZES[0]).await?;
    local_run(stack, Some(&ten_parts), "SELECT 1", false)?;
    local_run(stack, Some(&merging), "SELECT 1", false)?;
    let rows = vec![
        row("The engine alone: `SELECT 1`", local_run(stack, None, "SELECT 1", false)?),
        row("With an empty tenant's disk attached", local_run(stack, Some(&empty), "SELECT 1", false)?),
        row("With the schema's four tables and no parts", local_run(stack, Some(&no_parts), "SELECT 1", false)?),
        row("With 10 parts a table", local_run(stack, Some(&ten_parts), "SELECT 1", false)?),
        row("With 10 parts a table and one 1,000-row insert", local_run(stack, Some(&ten_parts), INSERT_SELECT, false)?),
        row("With 10 parts a table and background merges on", local_run(stack, Some(&merging), "SELECT 1", true)?),
    ];
    empty.remove().await?;
    no_parts.remove().await?;
    ten_parts.remove().await?;
    merging.remove().await?;
    Ok(rows)
}

/// A blank server that is already running: from its tenant config to its first insert done.
async fn assignment(stack: &Stack, fixture: &TenantFixture, definitions: &[Table], parts: usize) -> Result<Row, Error> {
    let mut wall = Vec::new();
    for _ in 0..RUNS {
        let mut server = blank(stack).await?;
        let started = Instant::now();
        assign_writer(&server, &fixture.tenant, &fixture.key, definitions).await?;
        server.query(INSERT_SELECT).await?;
        wall.push(started.elapsed());
        server.stop().await?;
    }
    Ok(Row {
        name: format!("A blank server already running: assignment through the first insert; {parts} parts a table"),
        cpu: None,
        wall: Some(median(wall)),
        memory_threads: None,
    })
}

type Settings = &'static [(&'static str, &'static str)];

/// How many inserts a running writer's figure on a tenant of N parts averages. Each adds a part to each table.
const RUNNING_INSERTS: u32 = 50;

/// 1,000 rows of the events table as JSON.
fn json_rows() -> Bytes {
    let body: String = (0..1_000)
        .map(|row| {
            format!("{{\"ts\":\"2026-11-01 00:{:02}:{:02}\",\"tenant\":\"t1\",\"kind\":\"{}\",\"v\":{row}}}\n", row / 60, row % 60, ["a", "b", "c"][row % 3])
        })
        .collect();
    Bytes::from(body)
}

/// A running writer's idle CPU, in seconds of CPU a second.
async fn idle_rate(pid: u32) -> f64 {
    let idle_before = process_cpu(pid).unwrap_or_default();
    tokio::time::sleep(Duration::from_secs(5)).await;
    (process_cpu(pid).unwrap_or_default() - idle_before).as_secs_f64() / 5.0
}

/// The CPU and wall time of one insert of `body` into `table`, averaged over `inserts`, with the writer's idle CPU
/// taken out.
async fn timed_inserts(writer: &Exclusive, idle_per_second: f64, table: &str, settings: Settings, body: &Bytes, inserts: u32) -> Result<(Duration, Duration), Error> {
    let pid = writer.server().pid().expect("the writer runs");
    let mut all = vec![("async_insert", "0")];
    all.extend_from_slice(settings);
    let sql = format!("INSERT INTO {table} FORMAT JSONEachRow");
    tokio::time::sleep(Duration::from_millis(500)).await;
    let (cpu_before, started) = (process_cpu(pid).unwrap_or_default(), Instant::now());
    for _ in 0..inserts {
        writer.server().run(&sql, &all, body.clone()).await?;
    }
    let wall = started.elapsed() / inserts;
    let cpu = (process_cpu(pid).unwrap_or_default() - cpu_before).as_secs_f64() / f64::from(inserts) - idle_per_second * wall.as_secs_f64();
    Ok((Duration::from_secs_f64(cpu.max(0.0)), wall))
}

/// Inserts of 1,000 JSON rows through the projection and the view into a writer that is already running on the
/// tenant, with background merges off: the insert alone, which the process rows above it boot and stop around.
async fn running_insert(stack: &Stack, fixture: &TenantFixture, parts: usize) -> Result<Row, Error> {
    let writer = Exclusive::start_with(&stack.stock_env(), &fixture.tenant, &fixture.key, NO_MERGES).await?;
    let idle_per_second = idle_rate(writer.server().pid().expect("the writer runs")).await;
    let (cpu, wall) = timed_inserts(&writer, idle_per_second, "d.events", &[], &json_rows(), RUNNING_INSERTS).await?;
    writer.stop().await?;
    Ok(Row {
        name: format!("1,000 JSON rows into a running writer, through the projection and the view; {parts} parts a table"),
        cpu: Some(cpu),
        wall: Some(wall),
        memory_threads: None,
    })
}

/// Inserts of 1,000 JSON rows into a writer that is already running, from empty tables, with merges stopped and
/// the writer's idle CPU taken out.
async fn running_writer(stack: &Stack, fixture: &TenantFixture) -> Result<Vec<Row>, Error> {
    let writer = Exclusive::start_with(&stack.stock_env(), &fixture.tenant, &fixture.key, NO_MERGES).await?;
    let idle_per_second = idle_rate(writer.server().pid().expect("the writer runs")).await;
    let body = json_rows();
    let one_thread: Settings = &[("max_threads", "1"), ("max_insert_threads", "1")];
    let cases: [(&str, &str, Settings); 4] = [
        ("1,000 JSON rows into a running writer, through the projection and the view; empty tables", "d.events", &[]),
        ("The same on one thread", "d.events", one_thread),
        ("1,000 JSON rows into a running writer's plain `MergeTree` table; empty tables", "d.plain", &[]),
        ("The same on one thread", "d.plain", one_thread),
    ];
    let mut rows = Vec::new();
    for (name, table, settings) in cases {
        // Every case starts from empty tables, so none pays for the parts of the one before.
        for table in ["d.events", "d.agg", "d.plain"] {
            writer.execute(&format!("TRUNCATE TABLE {table}")).await?;
        }
        let (cpu, wall) = timed_inserts(&writer, idle_per_second, table, settings, &body, 100).await?;
        rows.push(Row { name: name.to_string(), cpu: Some(cpu), wall: Some(wall), memory_threads: None });
    }
    writer.stop().await?;
    Ok(rows)
}

/// The engine with no server around it: `clickhouse local`, idle inside a query.
fn idle_local(stack: &Stack) -> Result<Row, Error> {
    let mut child = Command::new(&stack.clickhouse).args(["local", "--query", "SELECT sleep(3) FORMAT Null"]).spawn()?;
    std::thread::sleep(Duration::from_millis(1_500));
    let memory_threads = own_memory_mb(child.id()).zip(thread_count(child.id()));
    child.wait()?;
    Ok(Row { name: "`clickhouse local` idle inside a query".to_string(), cpu: None, wall: None, memory_threads })
}

fn report(stack: &Stack, rows: &[Row], profile: &[Row], load: f64) -> String {
    let git = |dir: &std::path::Path, args: &[&str]| {
        let output = Command::new("git").arg("-C").arg(dir).args(args).output().ok()?;
        output.status.success().then(|| String::from_utf8_lossy(&output.stdout).trim().to_string())
    };
    let cpu_model = std::fs::read_to_string("/proc/cpuinfo")
        .ok()
        .and_then(|text| {
            text.lines().find(|line| line.starts_with("model name")).and_then(|line| line.split_once(':')).map(|(_, model)| model.trim().to_string())
        })
        .unwrap_or_else(|| "an unknown CPU".into());
    let cores = std::thread::available_parallelism().map_or(0, usize::from);
    let bighouse = git(&stack.bighouse_dir, &["rev-parse", "--short", "HEAD"]).unwrap_or_else(|| "unknown".into());
    let fork = git(&stack.bighouse_dir.join("ClickHouse"), &["rev-parse", "--short", "HEAD"]).unwrap_or_else(|| "unknown".into());
    let date =
        Command::new("date").arg("+%-d %B %Y").output().ok().map(|output| String::from_utf8_lossy(&output.stdout).trim().to_string()).unwrap_or_default();

    let mut out = String::new();
    let _ = writeln!(out, "## Stock baselines\n");
    let _ = writeln!(
        out,
        "What it costs to write a tenant's disk through stock ClickHouse processes. libch's operations are measured against these. `cargo bench --bench stock` writes this section.\n"
    );
    let _ = writeln!(out, "- **Machine:** {cpu_model}, {cores} threads, load average {load:.1} at the start.");
    let _ = writeln!(out, "- **Build:** BigHouse `{bighouse}`, fork `{fork}`, exclusive mode, each server's watchdog off.");
    let _ = writeln!(out, "- **Date:** {date}.");
    let _ = writeln!(
        out,
        "- **Method:** the median of {RUNS} runs. CPU is user plus system time of the ClickHouse process. Memory is the process's own anonymous memory. Every measured process runs with background merges off (`max_bytes_to_merge_at_max_space_in_pool` = 1), as a process that serves one batch and exits would, and each tenant holds its parts unmerged to the end.\n"
    );
    let ms = |value: Option<Duration>| value.map_or(String::new(), |value| format_ms(value.as_secs_f64() * 1000.0));
    let _ = writeln!(out, "| Measurement | CPU | Wall | Own memory, threads |");
    let _ = writeln!(out, "| --- | --- | --- | --- |");
    for row in rows {
        let memory = row.memory_threads.map_or(String::new(), |(mb, threads)| format!("{mb} MB, {threads}"));
        let _ = writeln!(out, "| {} | {} | {} | {memory} |", row.name, ms(row.cpu), ms(row.wall));
    }
    let _ = writeln!(
        out,
        "\nThe 1,000-row insert goes through a projection and a materialized view into an `AggregatingMergeTree` table, except in the plain-table rows. A tenant of N parts a table holds N unmerged parts in the events table and N in the view's target before its first measured process, and each measured insert adds one to each. The running-writer rows leave out the writer's idle CPU. Every statement takes a new HTTP connection."
    );

    let _ = writeln!(out, "\n## Where a stock process spends its time\n");
    let _ = writeln!(
        out,
        "One `clickhouse local` process, start to exit. Each row adds a step to the one above and answers `SELECT 1`, with background merges off; the insert row runs the insert in place of that query. The last row turns merges on: the process starts merges of the tenant's parts as it attaches them and cancels them as it stops.\n"
    );
    let _ = writeln!(out, "| Process | CPU | Wall |");
    let _ = writeln!(out, "| --- | --- | --- |");
    for row in profile {
        let _ = writeln!(out, "| {} | {} | {} |", row.name, ms(row.cpu), ms(row.wall));
    }
    out
}

fn format_ms(ms: f64) -> String {
    if ms >= 20.0 { format!("{ms:.0} ms") } else { format!("{ms:.1} ms") }
}
