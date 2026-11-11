//! The stock baselines: what it costs to write through stock ClickHouse processes, which libch's operations
//! are measured against. Writes `BENCHMARKS.md` at the repository's root.
//!
//!     cargo bench --bench stock
//!
//! The benchmark refuses to record on a busy machine. `LIBCH_BENCH_FORCE=1` records anyway and notes the load.

use std::fmt::Write as _;
use std::process::Command;
use std::time::{Duration, Instant};

use bytes::Bytes;
use libch::catalog::{Catalog, Table, TableKind};
use libch::stock::{Blank, DiskMode, Exclusive, Server, database_disk_xml, disk_xml, merge_tree_xml, policy_xml};
use libch::{Error, Tenant};
use libch_testkit::measure::{children_cpu, load_average, median, own_memory_mb, process_cpu, thread_count};
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

    let (mut servers, mut locals, mut warm) = (Vec::new(), Vec::new(), Vec::new());
    for parts in TENANT_SIZES {
        let fixture = tenant_with_parts(&stack, parts).await?;
        let definitions = Catalog::connect(&fixture.tenant, &fixture.key).await?.all_tables().await?;
        servers.push(server_cycle(&stack, &fixture, &definitions, parts).await?);
        locals.push(local_process(&stack, &fixture, parts)?);
        if parts == TENANT_SIZES[0] {
            warm.push(assignment(&stack, &fixture, &definitions, parts).await?);
            warm.extend(running_writer(&stack, &fixture).await?);
        }
        fixture.remove().await?;
    }
    let mut rows = servers;
    rows.extend(locals);
    rows.extend(warm);
    rows.push(idle_local(&stack)?);

    let report = report(&stack, &rows, load, forced);
    let path = Stack::repo_root().join("BENCHMARKS.md");
    std::fs::write(&path, &report)?;
    print!("{report}");
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
        let mut server = Blank::boot(&stack.stock_env()).await?;
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

/// The same insert as one `clickhouse local` process that takes the disk, inserts and exits.
fn local_process(stack: &Stack, fixture: &TenantFixture, parts: usize) -> Result<Row, Error> {
    let work = stack.work_dir.join(format!("bench-local-{:08x}", rand::random::<u32>()));
    std::fs::create_dir_all(&work)?;
    let config = work.join("config.xml");
    std::fs::write(
        &config,
        format!(
            "<clickhouse><storage_configuration><disks>{}</disks><policies>{}</policies></storage_configuration>{}{}</clickhouse>",
            disk_xml(&fixture.tenant, &fixture.key, &DiskMode::Exclusive),
            policy_xml(),
            merge_tree_xml(""),
            database_disk_xml()
        ),
    )?;
    let mut cpu = Vec::new();
    let mut wall = Vec::new();
    for run in 0..RUNS {
        // `clickhouse local` loads databases from its disk only when its own path holds a metadata directory.
        let path = work.join(format!("run-{run}"));
        std::fs::create_dir_all(path.join("metadata"))?;
        let (cpu_before, started) = (children_cpu(), Instant::now());
        let output = Command::new(&stack.clickhouse)
            .arg("local")
            .arg("--config-file")
            .arg(&config)
            .arg("--path")
            .arg(&path)
            .arg("--query")
            .arg(INSERT_SELECT)
            .output()?;
        wall.push(started.elapsed());
        cpu.push(children_cpu() - cpu_before);
        if !output.status.success() {
            return Err(Error::Other(format!("clickhouse local failed: {}", String::from_utf8_lossy(&output.stderr).trim())));
        }
    }
    let _ = std::fs::remove_dir_all(&work);
    Ok(Row {
        name: format!("`clickhouse local`: one process for the same insert; {parts} parts a table"),
        cpu: Some(median(cpu)),
        wall: Some(median(wall)),
        memory_threads: None,
    })
}

/// A blank server that is already running: from its tenant config to its first insert done.
async fn assignment(stack: &Stack, fixture: &TenantFixture, definitions: &[Table], parts: usize) -> Result<Row, Error> {
    let mut wall = Vec::new();
    for _ in 0..RUNS {
        let mut server = Blank::boot(&stack.stock_env()).await?;
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

/// Inserts of 1,000 JSON rows into a writer that is already running, with merges stopped and the writer's
/// idle CPU taken out.
async fn running_writer(stack: &Stack, fixture: &TenantFixture) -> Result<Vec<Row>, Error> {
    let writer = Exclusive::start(&stack.stock_env(), &fixture.tenant, &fixture.key).await?;
    writer.execute("SYSTEM STOP MERGES").await?;
    let pid = writer.server().pid().expect("the writer runs");
    let body: String = (0..1_000)
        .map(|row| {
            format!("{{\"ts\":\"2026-11-01 00:{:02}:{:02}\",\"tenant\":\"t1\",\"kind\":\"{}\",\"v\":{row}}}\n", row / 60, row % 60, ["a", "b", "c"][row % 3])
        })
        .collect();
    let body = Bytes::from(body);

    let idle_before = process_cpu(pid).unwrap_or_default();
    tokio::time::sleep(Duration::from_secs(5)).await;
    let idle_per_second = (process_cpu(pid).unwrap_or_default() - idle_before).as_secs_f64() / 5.0;

    type Settings = &'static [(&'static str, &'static str)];
    let one_thread: Settings = &[("max_threads", "1"), ("max_insert_threads", "1")];
    let cases: [(&str, &str, Settings); 4] = [
        ("1,000 JSON rows into a running writer, through the projection and the view", "d.events", &[]),
        ("The same on one thread", "d.events", one_thread),
        ("1,000 JSON rows into a running writer's plain `MergeTree` table", "d.plain", &[]),
        ("The same on one thread", "d.plain", one_thread),
    ];
    let mut rows = Vec::new();
    for (name, table, settings) in cases {
        const INSERTS: u32 = 100;
        // Every case starts from empty tables, so none pays for the parts of the one before.
        for table in ["d.events", "d.agg", "d.plain"] {
            writer.execute(&format!("TRUNCATE TABLE {table}")).await?;
        }
        let mut all = vec![("async_insert", "0")];
        all.extend_from_slice(settings);
        let sql = format!("INSERT INTO {table} FORMAT JSONEachRow");
        tokio::time::sleep(Duration::from_millis(500)).await;
        let (cpu_before, started) = (process_cpu(pid).unwrap_or_default(), Instant::now());
        for _ in 0..INSERTS {
            writer.server().run(&sql, &all, body.clone()).await?;
        }
        let wall = started.elapsed() / INSERTS;
        let cpu = (process_cpu(pid).unwrap_or_default() - cpu_before).as_secs_f64() / f64::from(INSERTS) - idle_per_second * wall.as_secs_f64();
        rows.push(Row { name: name.to_string(), cpu: Some(Duration::from_secs_f64(cpu.max(0.0))), wall: Some(wall), memory_threads: None });
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

fn report(stack: &Stack, rows: &[Row], load: f64, forced: bool) -> String {
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
    let _ = writeln!(out, "# Benchmarks\n");
    let _ = writeln!(out, "## Stock baselines\n");
    let _ = writeln!(
        out,
        "What it costs to write a tenant's disk through stock ClickHouse processes. libch's operations are measured against these. `cargo bench --bench stock` writes this section.\n"
    );
    let _ = writeln!(
        out,
        "- **Machine:** {cpu_model}, {cores} threads, load average {load:.1} at the start{}.",
        if forced { " (recorded under `LIBCH_BENCH_FORCE`)" } else { "" }
    );
    let _ = writeln!(out, "- **Build:** BigHouse `{bighouse}`, fork `{fork}`, exclusive mode, each server's watchdog off.");
    let _ = writeln!(out, "- **Date:** {date}.");
    let _ = writeln!(
        out,
        "- **Method:** the median of {RUNS} runs. CPU is user plus system time of the ClickHouse process. Memory is the process's own anonymous memory.\n"
    );
    let _ = writeln!(out, "| Measurement | CPU | Wall | Own memory, threads |");
    let _ = writeln!(out, "| --- | --- | --- | --- |");
    for row in rows {
        let ms = |value: Option<Duration>| value.map_or(String::new(), |value| format_ms(value.as_secs_f64() * 1000.0));
        let memory = row.memory_threads.map_or(String::new(), |(mb, threads)| format!("{mb} MB, {threads}"));
        let _ = writeln!(out, "| {} | {} | {} | {memory} |", row.name, ms(row.cpu), ms(row.wall));
    }
    let _ = writeln!(
        out,
        "\nThe 1,000-row insert goes through a projection and a materialized view into an `AggregatingMergeTree` table, except in the plain-table rows. A tenant of N parts a table holds N unmerged parts in the events table and N in the view's target. The running-writer rows have merges stopped, start from empty tables, and leave out the writer's idle CPU. Every statement takes a new HTTP connection."
    );
    out
}

fn format_ms(ms: f64) -> String {
    if ms >= 20.0 { format!("{ms:.0} ms") } else { format!("{ms:.1} ms") }
}
