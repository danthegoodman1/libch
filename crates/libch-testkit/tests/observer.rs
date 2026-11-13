//! Each operation's report, against a stand-in worker that touches no disk: one report for every call, however
//! it ends, through a pool and without one.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use libch::observe::{Ended, Failure, Kind, Observer, Report, Work};
use libch::pool::{Pool, PoolOptions};
use libch::worker::{OpWriter, WorkerCommand};
use libch::writer::{Budget, Insert, MergeCandidate, TableName};
use libch::{Error, Outcome, PartInfo, S3Location, Tenant, Writer};

/// Keeps every report it hears.
#[derive(Default)]
struct Reports(Mutex<Vec<Report>>);

impl Observer for Reports {
    fn operation(&self, report: &Report) {
        self.0.lock().unwrap().push(report.clone());
    }
}

impl Reports {
    fn take(&self) -> Vec<Report> {
        std::mem::take(&mut *self.0.lock().unwrap())
    }

    /// The one report a call left.
    fn only(&self) -> Report {
        let mut reports = self.take();
        assert_eq!(reports.len(), 1, "one report for one call: {reports:?}");
        reports.pop().unwrap()
    }
}

fn tenant() -> Tenant {
    Tenant {
        store: "127.0.0.1:7170".into(),
        root: "tenants/t1/disk".into(),
        s3: S3Location {
            endpoint: "http://127.0.0.1:11211".into(),
            bucket: "libch".into(),
            prefix: "tenants/t1".into(),
            region: "us-east-1".into(),
            access_key: "access".into(),
            secret_key: "secret".into(),
        },
    }
}

fn command(mode: &str) -> WorkerCommand {
    WorkerCommand::new(env!("CARGO_BIN_EXE_libch-fake-worker")).env("LIBCH_FAKE_WORKER", mode)
}

/// A writer whose stand-in worker does what `mode` says, reporting to `reports`.
fn writer(mode: &str, reports: &Arc<Reports>) -> OpWriter {
    OpWriter::new(command(mode), &tenant(), "17.key").with_deadline(Duration::from_secs(30)).observed(reports.clone())
}

fn events() -> TableName {
    TableName::new("d", "events")
}

async fn insert(writer: &OpWriter, size: usize) -> Result<Outcome<libch::writer::Inserted>, Error> {
    writer.insert(Insert { table: &events(), format: "JSONEachRow", token: "batch-42", data: Bytes::from(vec![b'x'; size]) }).await
}

fn candidate() -> MergeCandidate {
    MergeCandidate { partition_id: "all".into(), sources: ["all_1_1_0", "all_2_2_0", "all_3_3_0"].map(|name| PartInfo::parse(name).unwrap()).to_vec() }
}

#[tokio::test]
async fn each_operation_reports_its_kind_tenant_and_work() {
    let reports = Arc::new(Reports::default());
    let writer = writer("", &reports);

    insert(&writer, 1_000).await.unwrap();
    let report = reports.only();
    assert_eq!((report.kind, report.ended), (Kind::Insert, Ended::Committed));
    assert_eq!((report.store.as_str(), report.tenant.as_str(), report.table.as_ref()), ("127.0.0.1:7170", "tenants/t1/disk", Some(&events())));
    // The stand-in commits a part of 7 rows to the table, sized as the batch, and an empty one elsewhere.
    assert_eq!(report.work, Work { batch_bytes: 1_000, rows_inserted: 7, parts_committed: 2, part_bytes: 1_000, ..Work::default() });
    let usage = report.usage.expect("the worker counted its work");
    assert_eq!((usage.operations, usage.inodedb_commits, usage.inodedb_reads, usage.cpu), (1, 2, 3, Duration::from_millis(1)));
    assert!(report.elapsed > Duration::ZERO);
    assert_eq!((report.queued, report.waiting_worker), (Duration::ZERO, false), "a writer without a pool starts its own worker");

    let planned = writer.plan(&events(), &Budget { max_merges: 2 }).await.unwrap();
    assert_eq!(planned.len(), 2);
    let report = reports.only();
    assert_eq!((report.kind, report.ended, report.work), (Kind::Plan, Ended::Committed, Work { merges_planned: 2, ..Work::default() }));

    writer.merge(&events(), &candidate()).await.unwrap();
    let report = reports.only();
    assert_eq!((report.kind, report.ended, report.work), (Kind::Merge, Ended::Committed, Work { parts_committed: 1, parts_merged: 3, ..Work::default() }));

    writer.housekeep(&events()).await.unwrap();
    let report = reports.only();
    assert_eq!((report.kind, report.ended), (Kind::Housekeep, Ended::Committed));
    assert_eq!(report.work, Work { parts_removed: 2, blobs_removed: 5, ..Work::default() });

    writer.describe(false).await.unwrap();
    let report = reports.only();
    assert_eq!((report.kind, report.ended, report.table, report.work), (Kind::Describe, Ended::Committed, None, Work::default()));

    writer.share().await.unwrap();
    let report = reports.only();
    assert_eq!((report.kind, report.ended, report.table), (Kind::Share, Ended::Committed, None));
}

#[tokio::test]
async fn each_outcome_reports_its_ending_and_no_work() {
    let reports = Arc::new(Reports::default());
    for (mode, ended) in [("duplicate", Ended::Duplicate), ("schema-changed", Ended::SchemaChanged), ("fenced", Ended::Fenced), ("refused", Ended::Refused)] {
        insert(&writer(mode, &reports), 500).await.unwrap();
        let report = reports.only();
        assert_eq!(report.ended, ended, "{mode}");
        assert_eq!(report.work, Work { batch_bytes: 500, ..Work::default() }, "{mode}: a batch sent, and nothing committed");
        assert_eq!(report.usage.map(|usage| usage.operations), Some(1), "{mode}: an outcome is an answer");
    }
    writer("claim-lost", &reports).merge(&events(), &candidate()).await.unwrap();
    let report = reports.only();
    assert_eq!((report.kind, report.ended, report.work), (Kind::Merge, Ended::ClaimLost, Work::default()));
    writer("fenced", &reports).housekeep(&events()).await.unwrap();
    assert_eq!(reports.only().ended, Ended::Fenced);
}

#[tokio::test]
async fn each_failure_reports_its_kind() {
    let reports = Arc::new(Reports::default());
    let exception = |code| Ended::Failed(Failure::Exception { code });
    let protocol = Ended::Failed(Failure::Protocol);
    let exited = Ended::Failed(Failure::Exited);
    for (mode, ended, answered) in [
        ("exception", exception(Some(60)), true),
        ("failed", exception(None), true),
        ("no-result", protocol, true),
        ("unknown-status", protocol, true),
        ("bad-part", protocol, true),
        ("garbage", protocol, false),
        ("truncated", protocol, false),
        ("two-frames", protocol, false),
        ("exit", exited, false),
        ("exit-after-answer", exited, false),
    ] {
        assert!(insert(&writer(mode, &reports), 10).await.is_err(), "{mode}");
        let report = reports.only();
        assert_eq!(report.ended, ended, "{mode}");
        assert_eq!(report.usage.is_some(), answered, "{mode}: usage comes with an answer that decodes");
        assert_eq!(report.work, Work { batch_bytes: 10, ..Work::default() }, "{mode}");
    }

    // An answer that is not the operation's result is no answer the protocol allows.
    assert!(writer("fenced", &reports).plan(&events(), &Budget::default()).await.is_err());
    let report = reports.only();
    assert_eq!((report.kind, report.ended), (Kind::Plan, protocol));
    assert!(writer("refused", &reports).describe(false).await.is_err());
    assert_eq!(reports.only().ended, protocol);

    let hung = OpWriter::new(command("hang"), &tenant(), "17.key").with_deadline(Duration::from_millis(300)).observed(reports.clone());
    assert!(insert(&hung, 10).await.is_err());
    let report = reports.only();
    assert_eq!((report.ended, report.usage), (Ended::Failed(Failure::Deadline), None));
    assert!(report.elapsed >= Duration::from_millis(300), "{:?}", report.elapsed);

    // A caller that gives up on a call drops it, and the worker with it.
    let dropped = tokio::time::timeout(Duration::from_millis(300), insert(&writer("hang", &reports), 10)).await;
    assert!(dropped.is_err(), "the stand-in hangs past the caller's patience");
    let report = reports.only();
    assert_eq!((report.ended, report.usage, report.work.batch_bytes), (Ended::Failed(Failure::Cancelled), None, 10));

    let absent = OpWriter::new(WorkerCommand::clickhouse("/nonexistent/clickhouse"), &tenant(), "17.key").observed(reports.clone());
    assert!(absent.describe(false).await.is_err());
    assert_eq!(reports.only().ended, Ended::Failed(Failure::Start));
}

/// A directory the slow stand-in counts its workers in, removed with the value.
struct Scratch(PathBuf);

impl Scratch {
    fn new() -> Scratch {
        let dir = std::env::temp_dir().join(format!("libch-observer-{:016x}", rand::random::<u64>()));
        std::fs::create_dir(&dir).unwrap();
        Scratch(dir)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[tokio::test]
async fn a_pool_reports_each_operation_with_its_queue_and_its_worker() {
    let scratch = Scratch::new();
    let reports = Arc::new(Reports::default());
    let command = command("slow").env("LIBCH_FAKE_WORKER_DIR", scratch.0.display().to_string());
    let pool = Pool::with_observer(command, PoolOptions { waiting: 1, concurrency: 1 }, reports.clone()).unwrap();
    let writer = pool.writer(&tenant(), "17.key");
    let started = std::time::Instant::now();
    while pool.stats().waiting < 1 {
        assert!(started.elapsed() < Duration::from_secs(20), "a worker starts ahead of the operations");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // The stand-in holds each request for 300 ms, and the pool runs one operation at a time, so the second waits.
    let (first, second) = tokio::join!(writer.describe(false), writer.describe(false));
    first.unwrap();
    second.unwrap();
    let mut reports_seen = reports.take();
    assert_eq!(reports_seen.len(), 2, "{reports_seen:?}");
    reports_seen.sort_by_key(|report| report.queued);
    assert!(reports_seen[0].queued < Duration::from_millis(100), "{:?}", reports_seen[0].queued);
    assert!(reports_seen[1].queued >= Duration::from_millis(250), "{:?}", reports_seen[1].queued);
    assert!(reports_seen.iter().all(|report| report.elapsed >= report.queued));
    assert!(reports_seen[0].waiting_worker, "the first took the worker started ahead of it");

    // A writer's own observer takes the place of its pool's.
    let own = Arc::new(Reports::default());
    pool.writer(&tenant(), "17.key").observed(own.clone()).describe(false).await.unwrap();
    assert_eq!((own.take().len(), reports.take().len()), (1, 0));

    pool.close().await;
    assert!(writer.describe(false).await.is_err());
    let report = reports.only();
    assert_eq!((report.ended, report.waiting_worker, report.usage), (Ended::Failed(Failure::Closed), false, None));
}

#[tokio::test]
async fn a_pool_without_waiting_workers_reports_their_start() {
    let reports = Arc::new(Reports::default());
    let pool = Pool::with_observer(command(""), PoolOptions { waiting: 0, concurrency: 2 }, reports.clone()).unwrap();
    pool.writer(&tenant(), "17.key").describe(false).await.unwrap();
    let report = reports.only();
    assert_eq!((report.ended, report.waiting_worker), (Ended::Committed, false), "the operation started its own worker");

    let absent =
        Pool::with_observer(WorkerCommand::clickhouse("/nonexistent/clickhouse"), PoolOptions { waiting: 0, concurrency: 1 }, reports.clone()).unwrap();
    assert!(absent.writer(&tenant(), "17.key").describe(false).await.is_err());
    assert_eq!(reports.only().ended, Ended::Failed(Failure::Start));
}
