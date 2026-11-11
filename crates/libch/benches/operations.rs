//! What libch's operations cost through the fork's worker, on tenants of 10 to 10,000 parts a table. Writes the
//! "Operations" section of `BENCHMARKS.md`.
//!
//!     cargo bench --bench operations
//!
//! The benchmark refuses to record on a busy machine. `LIBCH_BENCH_FORCE=1` runs it anyway and prints the figures
//! without recording them.

use std::fmt::Write as _;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use futures_util::StreamExt;
use futures_util::stream;
use libch::pool::{Pool, PoolOptions};
use libch::stock::Exclusive;
use libch::worker::{OpWriter, Usage};
use libch::writer::{Budget, Insert, MergeCandidate, TableName};
use libch::{Error, PartInfo, Writer};
use libch_testkit::measure::{load_average, median, own_memory_mb, process_cpu, record, thread_count};
use libch_testkit::schema::{EVENTS_SCHEMA, batch, events};
use libch_testkit::{Stack, TenantFixture, preflight};

/// Above this one-minute load average the machine is too busy for the figures to mean much.
const QUIET_LOAD: f64 = 2.0;
const RUNS: usize = 5;
/// How many parts each of a tenant's two tables holds.
const TENANT_SIZES: [usize; 4] = [10, 100, 500, 10_000];
/// Limits that let an exclusive writer leave a table this many unmerged parts.
const MANY_PARTS: &str = "<parts_to_throw_insert>1000000</parts_to_throw_insert><parts_to_delay_insert>1000000</parts_to_delay_insert><max_parts_in_total>10000000</max_parts_in_total>";

/// One row of the report: an operation's medians over its runs.
struct Row {
    name: String,
    cpu: Duration,
    /// From the call to its answer, where the call starts its own worker.
    own: Option<Duration>,
    /// The same through a worker that was started and waiting.
    waiting: Option<Duration>,
    memory_mb: u64,
    threads: u32,
    commits: u64,
    calls: u64,
}

/// What an operation works on, by run, so that each run has parts and a batch of its own.
#[derive(Clone, Copy)]
enum Operation {
    Describe,
    Insert,
    Merge,
    Plan,
    Housekeep,
}

fn main() {
    let runtime = tokio::runtime::Builder::new_multi_thread().enable_all().build().expect("a runtime");
    if let Err(error) = runtime.block_on(run()) {
        eprintln!("the operations benchmark failed: {error}");
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
            "the machine's load average is {load:.1}, above {QUIET_LOAD}: figures taken now would mislead. Run again when it is idle, or set LIBCH_BENCH_FORCE=1 to run anyway."
        )));
    }

    let mut rows = vec![idle_worker(&stack)?];
    for parts in TENANT_SIZES {
        let fixture = tenant_with_parts(&stack, parts, 0).await?;
        let of = |operation: &str| format!("{operation}; {} parts a table", thousands(parts));
        rows.push(measure(&stack, &fixture, Operation::Describe, of("`describe`")).await?);
        rows.push(measure(&stack, &fixture, Operation::Insert, of("`insert` of 1,000 rows through the projection and the view")).await?);
        rows.push(measure(&stack, &fixture, Operation::Merge, of("`merge` of four parts")).await?);
        rows.push(measure(&stack, &fixture, Operation::Plan, of("`plan`")).await?);
        if parts == 500 {
            rows.push(measure(&stack, &fixture, Operation::Housekeep, of("`housekeep` with 40 covered parts inside their grace period")).await?);
        }
        fixture.remove().await?;
    }
    let wide = tenant_with_parts(&stack, 1, 300).await?;
    rows.push(measure(&stack, &wide, Operation::Describe, "`describe`; 300 tables and one part a table".to_string()).await?);
    wide.remove().await?;

    let report = report(&stack, &rows, load);
    print!("{report}");
    if forced {
        eprintln!("the load average was {load:.1}, so BENCHMARKS.md is left as it was");
        return Ok(());
    }
    let path = Stack::repo_root().join("BENCHMARKS.md");
    record(&path, &report)?;
    eprintln!("wrote {}", path.display());
    Ok(())
}

/// A tenant with the events schema, `parts` unmerged parts of 100 rows in the events table and in the view's
/// target, and `more_tables` further empty tables.
async fn tenant_with_parts(stack: &Stack, parts: usize, more_tables: usize) -> Result<TenantFixture, Error> {
    let fixture = TenantFixture::create(stack).await?;
    let writer = Exclusive::start_with(&stack.stock_env(), &fixture.tenant, &fixture.key, MANY_PARTS).await?;
    for statement in EVENTS_SCHEMA {
        writer.execute(statement).await?;
    }
    for table in 0..more_tables {
        writer
            .execute(&format!(
                "CREATE TABLE d.more_{table} (k UInt64, v String) ENGINE = MergeTree ORDER BY k SETTINGS non_replicated_deduplication_window = 10000"
            ))
            .await?;
    }
    writer.execute("SYSTEM STOP MERGES").await?;
    let inserts = (0..parts).map(|part| {
        let writer = &writer;
        async move {
            let rows = format!("SELECT toDateTime('2026-11-01 00:00:00') + number, ['a','b','c'][number % 3 + 1], {part} * 100 + number FROM numbers(100)");
            writer.execute(&format!("INSERT INTO d.events {rows}")).await
        }
    });
    for inserted in stream::iter(inserts).buffer_unordered(8).collect::<Vec<_>>().await {
        inserted?;
    }
    writer.stop().await?;
    Ok(fixture)
}

/// Runs one operation on a writer. `run` numbers the call among every call of the benchmark on this tenant.
async fn operate(writer: &OpWriter, operation: Operation, run: usize) -> Result<(), Error> {
    let table: TableName = events();
    match operation {
        Operation::Describe => writer.describe(false).await.map(drop),
        Operation::Insert => {
            let batch = batch(1_000_000 + run as u64, 1_000);
            let inserted = writer.insert(Insert { table: &table, format: "JSONEachRow", token: &batch.token, data: batch.data }).await?;
            inserted.committed().map(drop).ok_or_else(|| Error::Other("the benchmark's insert committed nothing".into()))
        }
        Operation::Merge => {
            // The exclusive writer numbered the table's parts from 1, and each run takes the next four.
            let first = 4 * run as i64 + 1;
            let sources = (first..first + 4).map(|block| PartInfo::new("202611", block, block, 0, 0)).collect();
            let merged = writer.merge(&table, &MergeCandidate { partition_id: "202611".into(), sources }).await?;
            merged.committed().map(drop).ok_or_else(|| Error::Other("the benchmark's merge committed nothing".into()))
        }
        Operation::Plan => writer.plan(&table, &Budget::default()).await.map(drop),
        Operation::Housekeep => writer.housekeep(&table).await.map(drop),
    }
}

/// Measures an operation: [`RUNS`] calls that each start their own worker, then as many through a worker that
/// was started and waiting.
async fn measure(stack: &Stack, fixture: &TenantFixture, operation: Operation, name: String) -> Result<Row, Error> {
    let (mut usages, mut own, mut waiting) = (Vec::new(), Vec::new(), Vec::new());
    for run in 0..RUNS {
        let writer = OpWriter::new(stack.worker_command(), &fixture.tenant, &fixture.key);
        let started = Instant::now();
        operate(&writer, operation, run).await?;
        own.push(started.elapsed());
        usages.push(writer.usage());
    }
    let pool = Pool::new(stack.worker_command(), PoolOptions { waiting: 1, concurrency: 1 })?;
    for run in RUNS..2 * RUNS {
        // A worker that has only just started is still loading; one that has waited a moment is what a pool
        // under steady load hands out.
        while pool.stats().waiting == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
        let writer = pool.writer(&fixture.tenant, fixture.key.as_str());
        let started = Instant::now();
        operate(&writer, operation, run).await?;
        waiting.push(started.elapsed());
        usages.push(writer.usage());
    }
    pool.close().await;

    let middle = |pick: fn(&Usage) -> u64| {
        let mut values: Vec<u64> = usages.iter().map(pick).collect();
        values.sort_unstable();
        values[values.len() / 2]
    };
    Ok(Row {
        name,
        cpu: median(usages.iter().map(|usage| usage.cpu).collect()),
        own: Some(median(own)),
        waiting: Some(median(waiting)),
        memory_mb: usages.iter().map(|usage| usage.peak_anonymous_memory_bytes >> 20).max().unwrap_or(0),
        threads: usages.iter().map(|usage| usage.peak_threads).max().unwrap_or(0),
        commits: middle(|usage| usage.inodedb_commits),
        calls: middle(Usage::inodedb_calls),
    })
}

/// A worker that has started its engine and waits for a request: what a pool holds for each waiting worker.
fn idle_worker(stack: &Stack) -> Result<Row, Error> {
    let command = stack.worker_command();
    let mut child = Command::new(&command.program).args(&command.args).stdin(Stdio::piped()).stdout(Stdio::null()).stderr(Stdio::null()).spawn()?;
    // The worker is ready once its CPU time stands still.
    let mut cpu = Duration::ZERO;
    for _ in 0..100 {
        std::thread::sleep(Duration::from_millis(200));
        let now = process_cpu(child.id()).unwrap_or_default();
        if now == cpu && now > Duration::ZERO {
            break;
        }
        cpu = now;
    }
    let (memory_mb, threads) = (own_memory_mb(child.id()).unwrap_or(0), thread_count(child.id()).unwrap_or(0) as u32);
    drop(child.stdin.take());
    child.wait()?;
    Ok(Row { name: "A worker started and waiting for a request".to_string(), cpu, own: None, waiting: None, memory_mb, threads, commits: 0, calls: 0 })
}

fn thousands(number: usize) -> String {
    if number >= 1_000 { format!("{},{:03}", number / 1_000, number % 1_000) } else { number.to_string() }
}

fn format_ms(duration: Duration) -> String {
    let ms = duration.as_secs_f64() * 1_000.0;
    if ms >= 20.0 { format!("{ms:.0} ms") } else { format!("{ms:.1} ms") }
}

fn report(stack: &Stack, rows: &[Row], load: f64) -> String {
    let git = |dir: &std::path::Path| {
        let output = Command::new("git").arg("-C").arg(dir).args(["rev-parse", "--short", "HEAD"]).output().ok()?;
        output.status.success().then(|| String::from_utf8_lossy(&output.stdout).trim().to_string())
    };
    let fork = stack.fork_dir.as_deref().and_then(git).unwrap_or_else(|| "unknown".into());
    let date =
        Command::new("date").arg("+%-d %B %Y").output().ok().map(|output| String::from_utf8_lossy(&output.stdout).trim().to_string()).unwrap_or_default();

    let mut out = String::new();
    let _ = writeln!(out, "## Operations\n");
    let _ = writeln!(
        out,
        "What libch's operations cost through the fork's worker. Each operation is a process of its own that attaches the tenant, does its work, answers and exits. `cargo bench --bench operations` writes this section.\n"
    );
    let _ = writeln!(out, "- **Build:** fork `{fork}`; load average {load:.1} at the start, on the machine of the stock baselines.");
    let _ = writeln!(out, "- **Date:** {date}.");
    let _ = writeln!(
        out,
        "- **Method:** the median of {} runs. CPU, memory, threads and InodeDB calls are the worker's own counts, from its start to its answer. Wall time runs from the call to its answer: with a worker the call starts, and through a worker that was started and waiting.\n",
        2 * RUNS
    );
    let _ = writeln!(out, "| Operation | Worker CPU | Wall, own worker | Wall, waiting worker | Worker memory, threads | InodeDB commits, calls |");
    let _ = writeln!(out, "| --- | --- | --- | --- | --- | --- |");
    for row in rows {
        let wall = |value: Option<Duration>| value.map_or(String::new(), format_ms);
        let calls = if row.calls == 0 { String::new() } else { format!("{}, {}", row.commits, row.calls) };
        let _ = writeln!(
            out,
            "| {} | {} | {} | {} | {} MB, {} | {calls} |",
            row.name,
            format_ms(row.cpu),
            wall(row.own),
            wall(row.waiting),
            row.memory_mb,
            row.threads
        );
    }
    let _ = writeln!(
        out,
        "\nA tenant of N parts a table holds N unmerged parts of 100 rows in the events table and N in the view's target. An insert writes both tables. A merge takes four of the events table's parts by name. A plan loads every part of its table, as a server's selector needs them, so its cost grows with the table where the other operations' costs stay level."
    );
    out
}
