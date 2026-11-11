//! The crate's side of the worker protocol, against a stand-in worker that touches no disk.

use std::path::Path;
use std::time::{Duration, Instant};

use bytes::Bytes;
use libch::catalog::TableKind;
use libch::worker::{OpWriter, ViewEdge, WorkerCommand};
use libch::writer::{Budget, Insert, MergeCandidate, PlannedMerge, TableName};
use libch::{Error, Outcome, PartInfo, S3Location, Tenant, Writer};

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

/// A writer whose worker is the stand-in, doing what `mode` says.
fn writer(mode: &str) -> OpWriter {
    let command = WorkerCommand::new(env!("CARGO_BIN_EXE_libch-fake-worker")).env("LIBCH_FAKE_WORKER", mode);
    OpWriter::new(command, &tenant(), "17.key").with_deadline(Duration::from_secs(30))
}

fn events() -> TableName {
    TableName::new("d", "events")
}

fn batch(size: usize) -> Bytes {
    Bytes::from(vec![b'x'; size])
}

async fn insert(writer: &OpWriter, size: usize) -> Result<Outcome<libch::writer::Inserted>, Error> {
    writer.insert(Insert { table: &events(), format: "JSONEachRow", token: "batch-42", data: batch(size) }).await
}

#[tokio::test]
async fn a_request_carries_the_tenant_and_the_settings() {
    let described = writer("").with_settings_xml("<merge_tree><old_parts_lifetime>10</old_parts_lifetime></merge_tree>").describe(false).await.unwrap();
    let table = &described.tables[0];
    assert_eq!(table.name, TableName::new("127.0.0.1:7170", "17.key"), "the store and the key");
    assert_eq!(table.uuid, "http://127.0.0.1:11211|libch|tenants/t1|us-east-1|access|secret", "the S3 location");
    assert_eq!(table.engine.as_deref(), Some("<merge_tree><old_parts_lifetime>10</old_parts_lifetime></merge_tree>"), "the settings");
    assert_eq!(table.kind, TableKind::Table);
    assert_eq!(table.views, [ViewEdge { view: TableName::new("d", "daily_mv"), target: TableName::new("d", "daily") }]);
    assert!(table.parts.is_empty(), "parts come only when asked for");
    assert_eq!((described.tables[1].kind, described.tables[1].engine.clone()), (TableKind::MaterializedView, None));
}

#[tokio::test]
async fn a_description_lists_parts_when_asked() {
    let described = writer("").describe(true).await.unwrap();
    let parts = &described.tables[0].parts;
    assert_eq!(parts.len(), 2);
    for part in parts {
        let parsed = PartInfo::parse(&part.name).unwrap();
        assert_eq!(
            (parsed.partition_id.as_str(), parsed.min_block, parsed.max_block, parsed.level, parsed.mutation),
            (part.partition_id.as_str(), part.min_block, part.max_block, part.level, part.mutation)
        );
    }
    assert_eq!(parts.iter().map(|part| part.active).collect::<Vec<_>>(), [false, true]);
}

#[tokio::test]
async fn an_insert_streams_its_batch_in_frames() {
    // The stand-in names its first part for the bytes it received and its second for the frames they came in.
    for (size, frames) in [(0usize, 0i64), (1, 1), (1 << 20, 1), ((1 << 20) + 1, 2), (5 * (1 << 20) + 123, 6)] {
        let inserted = insert(&writer(""), size).await.unwrap().committed().expect("the insert commits");
        let parts = inserted.parts.expect("a worker names the parts it wrote");
        assert_eq!(parts[0].table, events());
        assert_eq!(parts[0].part, PartInfo::new("JSONEachRow", size as i64, size as i64, 0, 0), "{size} bytes arrive");
        assert_eq!(parts[1].part, PartInfo::new("batch42", frames, frames, 0, 0), "{size} bytes arrive in {frames} frames");
        assert_eq!((parts[0].rows, parts[0].bytes), (7, size as u64), "a part comes with its rows and its size");
        assert_eq!(inserted.counter_conflicts, 0);
    }

    // A writer that waives exactly-once says so in each insert.
    let waived = insert(&writer("").at_least_once(), 10).await.unwrap().committed().unwrap();
    assert_eq!(waived.counter_conflicts, 3);
}

#[tokio::test]
async fn plans_merges_and_housekeeping_round_trip() {
    let writer = writer("");
    let candidates = writer.plan(&events(), &Budget { max_merges: 2 }).await.unwrap();
    assert_eq!(candidates.len(), 2, "the budget reaches the worker");
    assert_eq!(
        candidates[1],
        PlannedMerge {
            candidate: MergeCandidate {
                partition_id: "events".into(),
                sources: vec![PartInfo::new("events", 3, 3, 0, 0), PartInfo::new("events", 4, 4, 0, 0)]
            },
            rows: 200,
            bytes: 8192,
        },
        "the sources and their size come back"
    );

    let candidate = MergeCandidate {
        partition_id: "202611".into(),
        sources: ["202611_4_4_0", "202611_5_6_1", "202611_9_9_0"].map(|name| PartInfo::parse(name).unwrap()).to_vec(),
    };
    let merged = writer.merge(&events(), &candidate).await.unwrap().committed().expect("the merge commits");
    assert_eq!(merged.part.name(), "202611_4_9_1", "the partition and the sources reach the worker in order");

    let housekept = writer.housekeep(&events()).await.unwrap().committed().expect("housekeeping reports");
    assert_eq!(housekept.removed.iter().map(PartInfo::name).collect::<Vec<_>>(), ["all_1_1_0", "all_2_2_0"]);
    assert_eq!(housekept.remaining[0].part.name(), "all_3_3_0");
    assert_eq!(housekept.remaining[0].reason, "covered 10 s ago in d.events");
    assert_eq!(housekept.temporary_directories, 2);
    assert_eq!((housekept.blobs_removed, housekept.blobs_remain), (5, true));
}

#[tokio::test]
async fn each_status_becomes_its_outcome() {
    assert_eq!(insert(&writer("duplicate"), 10).await.unwrap(), Outcome::Duplicate);
    assert_eq!(insert(&writer("schema-changed"), 10).await.unwrap(), Outcome::SchemaChanged);
    assert_eq!(insert(&writer("fenced"), 10).await.unwrap(), Outcome::Fenced);
    assert_eq!(insert(&writer("refused"), 10).await.unwrap(), Outcome::Refused("the table keeps no tokens".into()));

    let candidate = MergeCandidate { partition_id: "all".into(), sources: ["all_1_1_0", "all_2_2_0"].map(|name| PartInfo::parse(name).unwrap()).to_vec() };
    assert_eq!(writer("claim-lost").merge(&events(), &candidate).await.unwrap(), Outcome::ClaimLost);
    assert_eq!(writer("fenced").housekeep(&events()).await.unwrap(), Outcome::Fenced);
}

#[tokio::test]
async fn a_writer_adds_up_what_its_workers_spent() {
    let writer = writer("");
    let clone = writer.clone();
    assert_eq!(writer.usage().operations, 0);
    insert(&writer, 10).await.unwrap();
    clone.describe(false).await.unwrap();
    // An outcome that committed nothing was still a worker's work.
    assert!(insert(&writer.clone(), 10).await.unwrap().is_committed());

    let usage = writer.usage();
    assert_eq!((usage.operations, usage.inodedb_commits, usage.inodedb_reads, usage.inodedb_calls()), (3, 6, 9, 15));
    assert_eq!((usage.cpu, usage.peak_anonymous_memory_bytes, usage.peak_threads), (Duration::from_millis(3), 4_096, 1));
    assert_eq!(clone.usage(), usage, "clones share the figures");

    // A worker that reports nothing adds nothing, and one that fails outright answered nothing.
    let quiet = self::writer("silent-usage");
    quiet.describe(false).await.unwrap();
    assert_eq!(quiet.usage().operations, 0);
    let failing = self::writer("exit");
    assert!(insert(&failing, 10).await.is_err());
    assert_eq!(failing.usage().operations, 0);
    let refused = self::writer("refused");
    insert(&refused, 10).await.unwrap();
    assert_eq!(refused.usage().operations, 1, "a refusal is an answer");
}

#[tokio::test]
async fn a_failed_operation_is_an_error() {
    let exception = insert(&writer("exception"), 10).await.unwrap_err();
    assert_eq!(exception.clickhouse_code(), Some(60));
    assert!(exception.to_string().contains("d.absent"), "{exception}");

    let failed = insert(&writer("failed"), 10).await.unwrap_err();
    assert!(matches!(&failed, Error::Worker { message } if message.contains("memory limit")), "{failed}");
    // A plan has no outcome but its candidates, so anything else is an error too.
    assert!(writer("fenced").plan(&events(), &Budget::default()).await.is_err());
    assert!(writer("refused").describe(false).await.is_err());
}

#[tokio::test]
async fn a_worker_that_dies_is_an_error_with_its_log() {
    let error = insert(&writer("exit"), 10).await.unwrap_err();
    let message = error.to_string();
    assert!(matches!(error, Error::Worker { .. }), "{message}");
    assert!(message.contains("exit status: 3") && message.contains("the disk could not be attached"), "{message}");

    // An answer from a worker that then failed is no answer.
    let error = insert(&writer("exit-after-answer"), 10).await.unwrap_err();
    assert!(error.to_string().contains("exit status: 1"), "{error}");
}

#[tokio::test]
async fn an_answer_that_is_no_frame_is_an_error() {
    for (mode, expected) in [
        ("garbage", "no response frame"),
        ("truncated", "one response frame"),
        ("two-frames", "one response frame"),
        ("no-result", "without the operation's result"),
        ("unknown-status", "unknown status 99"),
        ("bad-part", "no part name"),
    ] {
        let error = insert(&writer(mode), 10).await.unwrap_err();
        assert!(matches!(&error, Error::Worker { message } if message.contains(expected)), "{mode}: {error}");
    }
}

#[tokio::test]
async fn a_worker_that_answers_before_reading_the_batch_is_heard() {
    // The stand-in refuses without reading a byte of a batch far larger than a pipe holds.
    let outcome = insert(&writer("deaf"), 64 << 20).await.unwrap();
    assert_eq!(outcome, Outcome::Refused("the table keeps no tokens".into()));
}

#[tokio::test]
async fn a_worker_past_its_deadline_is_killed() {
    let pid_file = std::env::temp_dir().join(format!("libch-fake-worker-{:08x}.pid", rand::random::<u32>()));
    let command = WorkerCommand::new(env!("CARGO_BIN_EXE_libch-fake-worker"))
        .env("LIBCH_FAKE_WORKER", "hang")
        .env("LIBCH_FAKE_WORKER_PID", pid_file.display().to_string());
    let writer = OpWriter::new(command, &tenant(), "17.key").with_deadline(Duration::from_millis(400));

    let started = Instant::now();
    let error = insert(&writer, 10).await.unwrap_err();
    assert!(started.elapsed() < Duration::from_secs(5), "the deadline ends the call: {:?}", started.elapsed());
    assert!(error.to_string().contains("did not finish within 400ms"), "{error}");

    let pid = std::fs::read_to_string(&pid_file).unwrap();
    std::fs::remove_file(&pid_file).unwrap();
    let gone = Instant::now();
    while Path::new(&format!("/proc/{pid}")).exists() {
        assert!(gone.elapsed() < Duration::from_secs(5), "the worker is killed");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn a_worker_that_cannot_start_is_an_error() {
    let writer = OpWriter::new(WorkerCommand::clickhouse("/nonexistent/clickhouse"), &tenant(), "17.key");
    let error = writer.describe(false).await.unwrap_err();
    assert!(matches!(&error, Error::Worker { message } if message.contains("/nonexistent/clickhouse") && message.contains("did not start")), "{error}");
}

#[tokio::test]
async fn many_operations_run_side_by_side() {
    let writer = writer("");
    let inserts = (0..48).map(|index| {
        let writer = &writer;
        async move { (index, insert(writer, 1_000 + index).await) }
    });
    for (index, outcome) in futures_util::future::join_all(inserts).await {
        let parts = outcome.unwrap().committed().unwrap().parts.unwrap();
        assert_eq!(parts[0].part.min_block as usize, 1_000 + index, "each call gets its own worker's answer");
    }
}
