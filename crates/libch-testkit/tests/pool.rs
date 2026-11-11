//! The worker pool, against a stand-in worker that touches no disk.

use std::collections::BTreeSet;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures_util::future::join_all;
use libch::pool::{Pool, PoolOptions};
use libch::worker::{OpWriter, WorkerCommand};
use libch::writer::{Insert, TableName};
use libch::{Error, PartInfo, S3Location, Tenant, Writer};

fn tenant(id: usize) -> Tenant {
    Tenant {
        store: "127.0.0.1:7170".into(),
        root: format!("tenants/t{id}/disk"),
        s3: S3Location {
            endpoint: "http://127.0.0.1:11211".into(),
            bucket: "libch".into(),
            prefix: format!("tenants/t{id}"),
            region: "us-east-1".into(),
            access_key: "access".into(),
            secret_key: "secret".into(),
        },
    }
}

fn command(mode: &str) -> WorkerCommand {
    WorkerCommand::new(env!("CARGO_BIN_EXE_libch-fake-worker")).env("LIBCH_FAKE_WORKER", mode)
}

fn pool(command: WorkerCommand, waiting: usize, concurrency: usize) -> Pool {
    Pool::new(command, PoolOptions { waiting, concurrency }).unwrap()
}

fn writer(pool: &Pool) -> OpWriter {
    pool.writer(&tenant(1), "17.key").with_deadline(Duration::from_secs(30))
}

/// A directory the stand-in workers of one test report to, removed with the value.
struct Scratch(PathBuf);

impl Scratch {
    fn new() -> Scratch {
        let dir = std::env::temp_dir().join(format!("libch-pool-{:016x}", rand::random::<u64>()));
        std::fs::create_dir(&dir).unwrap();
        Scratch(dir)
    }

    fn path(&self) -> String {
        self.0.display().to_string()
    }

    /// The processes of the workers that have started.
    fn started(&self) -> Vec<u32> {
        let names = std::fs::read_dir(&self.0).unwrap().map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned());
        names.filter_map(|name| name.strip_prefix("started-").map(|pid| pid.parse().unwrap())).collect()
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Whether a process has ended: it is gone, or it waits for its parent to collect it.
fn has_ended(pid: u32) -> bool {
    match std::fs::read_to_string(format!("/proc/{pid}/stat")) {
        Ok(stat) => stat.rsplit_once(") ").is_some_and(|(_, rest)| rest.starts_with('Z')),
        Err(_) => true,
    }
}

async fn until(what: &str, mut holds: impl FnMut() -> bool) {
    let started = Instant::now();
    while !holds() {
        assert!(started.elapsed() < Duration::from_secs(20), "waited 20 s for {what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// What the stand-in worker that served a description says of itself.
#[derive(Debug)]
struct Served {
    pid: u32,
    waited: Duration,
    together: usize,
    /// The tenant's key as the worker received it.
    key: String,
}

async fn describe(writer: &OpWriter) -> Result<Served, Error> {
    let described = writer.describe(false).await?;
    let worker = described.tables.last().expect("the worker's own table");
    assert_eq!(worker.name.database, "worker");
    Ok(Served {
        pid: worker.name.table.parse().unwrap(),
        waited: Duration::from_millis(worker.uuid.parse().unwrap()),
        together: worker.engine.as_deref().unwrap().parse().unwrap(),
        key: described.tables[0].name.table.clone(),
    })
}

#[tokio::test]
async fn workers_start_ahead_of_their_requests() {
    let scratch = Scratch::new();
    let pool = pool(command("").env("LIBCH_FAKE_WORKER_DIR", scratch.path()), 3, 4);
    until("three waiting workers", || pool.stats().waiting == 3 && scratch.started().len() == 3).await;
    let ahead: BTreeSet<u32> = scratch.started().into_iter().collect();
    tokio::time::sleep(Duration::from_millis(300)).await;

    let served = describe(&writer(&pool)).await.unwrap();
    assert!(ahead.contains(&served.pid), "the operation ran in a worker the pool had started");
    assert!(served.waited >= Duration::from_millis(300), "the worker had waited {:?} for its request", served.waited);
    assert_eq!(pool.stats().unpooled, 0);

    // A writer outside the pool starts its worker with the call.
    let outside = OpWriter::new(command(""), &tenant(1), "17.key");
    assert!(describe(&outside).await.unwrap().waited < served.waited);
}

#[tokio::test]
async fn each_worker_serves_one_operation_and_is_replaced() {
    let pool = pool(command(""), 2, 4);
    let writer = writer(&pool);
    let mut pids = BTreeSet::new();
    for _ in 0..10 {
        pids.insert(describe(&writer).await.unwrap().pid);
    }
    assert_eq!(pids.len(), 10, "every operation had a worker of its own");

    until("the pool to be full again", || pool.stats().waiting == 2).await;
    let stats = pool.stats();
    assert_eq!((stats.started, stats.running, stats.lost, stats.failed_starts), (12, 0, 0, 0), "ten served and two waiting: {stats:?}");
}

#[tokio::test]
async fn operations_run_within_the_concurrency_bound() {
    let scratch = Scratch::new();
    let pool = pool(command("slow").env("LIBCH_FAKE_WORKER_DIR", scratch.path()), 4, 3);
    let writer = writer(&pool);

    let watched = pool.clone();
    let watch = tokio::spawn(async move {
        let mut most = 0;
        for _ in 0..100 {
            most = most.max(watched.stats().running);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        most
    });
    let served = join_all((0..12).map(|_| describe(&writer))).await;
    let together: Vec<usize> = served.into_iter().map(|served| served.unwrap().together).collect();

    assert!(together.iter().all(|&count| count <= 3), "at most three workers held a request at once: {together:?}");
    assert!(together.contains(&3), "the pool used its whole bound: {together:?}");
    assert_eq!(watch.await.unwrap(), 3, "the pool counts three running at its busiest");
    assert_eq!(pool.stats().running, 0);
}

#[tokio::test]
async fn a_burst_beyond_the_waiting_workers_starts_its_own() {
    let scratch = Scratch::new();
    let pool = pool(command("slow").env("LIBCH_FAKE_WORKER_DIR", scratch.path()), 1, 8);
    until("one waiting worker", || pool.stats().waiting == 1).await;
    let writer = writer(&pool);
    let served = join_all((0..8).map(|_| describe(&writer))).await;
    let pids: BTreeSet<u32> = served.into_iter().map(|served| served.unwrap().pid).collect();
    assert_eq!(pids.len(), 8);
    let stats = pool.stats();
    assert!(stats.unpooled >= 1 && stats.unpooled <= 7, "{stats:?}");
}

#[tokio::test]
async fn many_tenants_share_a_pool() {
    let pool = pool(command(""), 8, 16);
    let calls = (0..64usize).map(|id| {
        let writer = pool.writer(&tenant(id), format!("{id}.key")).with_deadline(Duration::from_secs(30));
        async move {
            let served = describe(&writer).await.unwrap();
            assert_eq!(served.key, format!("{id}.key"), "tenant {id}'s request reached its own worker");
            let table = TableName::new("d", "events");
            let insert = Insert { table: &table, format: "JSONEachRow", token: "batch-1", data: Bytes::from(vec![b'x'; 2_000 + id]) };
            let parts = writer.insert(insert).await.unwrap().committed().unwrap().parts.unwrap();
            assert_eq!(parts[0].part, PartInfo::new("JSONEachRow", 2_000 + id as i64, 2_000 + id as i64, 0, 0), "tenant {id}'s batch arrived whole");
            served.pid
        }
    });
    let pids: BTreeSet<u32> = join_all(calls).await.into_iter().collect();
    assert_eq!(pids.len(), 64);
    assert_eq!(pool.stats().running, 0);
}

#[tokio::test]
async fn a_waiting_worker_that_died_is_passed_over() {
    let scratch = Scratch::new();
    let marker = Path::new(&scratch.path()).join("marker");
    std::fs::write(&marker, b"").unwrap();
    let command = command("").env("LIBCH_FAKE_WORKER_DIR", scratch.path()).env("LIBCH_FAKE_WORKER_MARKER", marker.display().to_string());
    let pool = pool(command, 2, 4);
    until("two workers to start and die", || scratch.started().len() == 2 && scratch.started().into_iter().all(has_ended)).await;
    assert_eq!(pool.stats().waiting, 2, "the pool learns of a death when an operation comes for the worker");

    // While workers die as they start, an operation passes over the dead ones, starts its own, and fails with
    // that worker's own words.
    let error = describe(&writer(&pool)).await.unwrap_err().to_string();
    assert!(error.contains("exit status: 3") && error.contains("the engine could not start"), "{error}");
    assert!(pool.stats().lost >= 2, "the two that died waiting were passed over: {:?}", pool.stats());

    // Once workers start again, so do operations, whatever dead workers the pool still holds.
    until("the pool to replace the two it lost", || pool.stats().waiting == 2 && scratch.started().len() >= 5 && scratch.started().into_iter().all(has_ended))
        .await;
    std::fs::remove_file(&marker).unwrap();
    describe(&writer(&pool)).await.unwrap();
    until("two workers that stay up", || pool.stats().waiting == 2 && scratch.started().into_iter().filter(|&pid| !has_ended(pid)).count() == 2).await;
    let waiting: BTreeSet<u32> = scratch.started().into_iter().filter(|&pid| !has_ended(pid)).collect();
    assert!(waiting.contains(&describe(&writer(&pool)).await.unwrap().pid), "the next operation runs in a worker the pool started");
}

#[tokio::test]
async fn a_pool_whose_workers_cannot_start_says_so_and_tries_again_slowly() {
    let pool = pool(WorkerCommand::clickhouse("/nonexistent/clickhouse"), 2, 4);
    tokio::time::sleep(Duration::from_millis(500)).await;
    let error = describe(&writer(&pool)).await.unwrap_err();
    assert!(matches!(&error, Error::Worker { message } if message.contains("/nonexistent/clickhouse") && message.contains("did not start")), "{error}");
    let stats = pool.stats();
    assert_eq!((stats.started, stats.waiting, stats.running), (0, 0, 0));
    assert!((2..=8).contains(&stats.failed_starts), "the pool waits longer after each failure: {stats:?}");
}

#[tokio::test]
async fn closing_kills_the_waiting_workers_and_refuses_operations() {
    let scratch = Scratch::new();
    let pool = pool(command("").env("LIBCH_FAKE_WORKER_DIR", scratch.path()), 3, 4);
    let writer = writer(&pool);
    until("three waiting workers", || pool.stats().waiting == 3 && scratch.started().len() == 3).await;
    let pids = scratch.started();

    pool.close().await;
    assert!(pids.iter().all(|pid| !Path::new(&format!("/proc/{pid}")).exists()), "the waiting workers are gone");
    assert_eq!(pool.stats().waiting, 0);
    let error = describe(&writer).await.unwrap_err();
    assert!(error.to_string().contains("the pool is closed"), "{error}");
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(scratch.started().len(), 3, "a closed pool starts no worker");
}

#[tokio::test]
async fn dropping_the_pool_and_its_writers_kills_the_waiting_workers() {
    let scratch = Scratch::new();
    let pool = pool(command("").env("LIBCH_FAKE_WORKER_DIR", scratch.path()), 3, 4);
    let writer = writer(&pool);
    until("three waiting workers", || pool.stats().waiting == 3 && scratch.started().len() == 3).await;
    let pids = scratch.started();

    drop(pool);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!pids.iter().any(|&pid| has_ended(pid)), "a writer keeps its pool");
    drop(writer);
    until("the waiting workers to end", || pids.iter().all(|&pid| has_ended(pid))).await;
}

#[tokio::test]
async fn a_worker_that_logs_while_it_waits_still_serves() {
    // A megabyte of log is far more than a pipe holds, so a worker nobody listened to would block in it.
    let chatty = |mode: &str| command(mode).env("LIBCH_FAKE_WORKER_CHATTER", (1 << 20).to_string());
    let serving = pool(chatty(""), 1, 2);
    until("a waiting worker", || serving.stats().waiting == 1).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    describe(&writer(&serving).with_deadline(Duration::from_secs(5))).await.unwrap();

    let failing = pool(chatty("exit-after-answer"), 1, 2);
    let error = describe(&writer(&failing)).await.unwrap_err().to_string();
    assert!(error.ends_with("ready for a request"), "the error carries the end of the log: {}", &error[error.len().saturating_sub(200)..]);
    assert!(error.len() < 4_000, "and only the end: {} bytes", error.len());
}

/// Runs a call that should end by itself, and fails the test when it hangs.
async fn within<T>(limit: Duration, what: &str, call: impl Future<Output = T>) -> T {
    tokio::time::timeout(limit, call).await.unwrap_or_else(|_| panic!("{what} did not end within {limit:?}"))
}

#[tokio::test]
async fn an_operation_that_ends_early_gives_its_turn_back() {
    let pool = pool(command("hang"), 1, 1);
    let writer = pool.writer(&tenant(1), "17.key").with_deadline(Duration::from_millis(300));

    // Past its deadline.
    let error = within(Duration::from_secs(10), "the first call", describe(&writer)).await.unwrap_err();
    assert!(error.to_string().contains("did not finish within 300ms"), "{error}");
    assert_eq!(pool.stats().running, 0);

    // Dropped by its caller.
    let patient = pool.writer(&tenant(1), "17.key").with_deadline(Duration::from_secs(60));
    assert!(tokio::time::timeout(Duration::from_millis(200), describe(&patient)).await.is_err());
    assert_eq!(pool.stats().running, 0);

    // The pool's one turn is free for the next call.
    let error = within(Duration::from_secs(10), "the third call", describe(&writer)).await.unwrap_err();
    assert!(error.to_string().contains("did not finish within 300ms"), "{error}");
}

#[tokio::test]
async fn a_pool_needs_a_bound_of_at_least_one() {
    let error = Pool::new(command(""), PoolOptions { waiting: 2, concurrency: 0 }).unwrap_err();
    assert!(error.to_string().contains("at least one operation"), "{error}");
}

#[test]
fn a_pool_is_made_inside_a_runtime() {
    let error = Pool::new(command(""), PoolOptions { waiting: 2, concurrency: 2 }).unwrap_err();
    assert!(error.to_string().contains("inside a Tokio runtime"), "{error}");
}
