//! An observer that records each operation through the [`metrics`] crate's facade, beside the pool's and key
//! caches' figures.
//!
//! A service installs the recorder it wants, as `metrics-exporter-prometheus` does, and gives its pool a
//! [`Metrics`]. The pool's and a key cache's figures are snapshots, which the service publishes as often as its
//! backend reads them:
//!
//! ```no_run
//! # async fn service(command: libch::worker::WorkerCommand, keys: libch::keys::KeyCache) -> Result<(), libch::Error> {
//! use std::sync::Arc;
//! use std::time::Duration;
//!
//! use libch::metrics::Metrics;
//! use libch::pool::{Pool, PoolOptions};
//!
//! let metrics = Arc::new(Metrics::new());
//! let pool = Pool::with_observer(command, PoolOptions { waiting: 8, concurrency: 16 }, metrics.clone())?;
//! loop {
//!     metrics.record_pool(&pool, "ingest");
//!     metrics.record_keys(&keys);
//!     tokio::time::sleep(Duration::from_secs(10)).await;
//! }
//! # }
//! ```
//!
//! Each operation's metrics carry its `kind` (`insert`, `plan`, `merge`, `housekeep`, `describe`, `share`), and the
//! busiest carry its `outcome` ([`Ended::name`]: `committed`, `fenced`, `deadline`, …) and its `store`, as a URL.
//! No metric names a tenant: a service with many tenants counts each one from the [`Report`]s.
//!
//! | Metric | Type | Labels | What it records |
//! | --- | --- | --- | --- |
//! | `libch_operations_total` | counter | kind, outcome, store | Operations ended. |
//! | `libch_operation_duration_seconds` | histogram | kind, outcome, store | From the call to its end. |
//! | `libch_operation_queued_seconds` | histogram | kind | The wait for a turn in the pool. |
//! | `libch_worker_cpu_seconds` | histogram | kind | The worker's CPU for the operation. |
//! | `libch_worker_memory_bytes` | histogram | kind | The worker's anonymous memory as it answered. |
//! | `libch_inodedb_calls_total` | counter | kind, call | Calls to the store: `commit`, `stat`, `read`, `list`. |
//! | `libch_s3_requests_total` | counter | kind, request | Requests to S3: `read`, `write`. |
//! | `libch_insert_batch_bytes_total` | counter | | The batches inserts sent. |
//! | `libch_rows_inserted_total` | counter | | Rows inserts committed to their tables. |
//! | `libch_parts_committed_total` | counter | kind | Parts inserts and merges committed. |
//! | `libch_part_bytes_committed_total` | counter | | The bytes on disk of the parts inserts committed. |
//! | `libch_parts_merged_total` | counter | | Parts merges took as sources. |
//! | `libch_merges_planned_total` | counter | | Merges plans proposed. |
//! | `libch_parts_removed_total`, `libch_blobs_removed_total` | counter | | What housekeeping removed. |
//! | `libch_pool_waiting_workers`, `libch_pool_running_operations` | gauge | pool | [`PoolStats`](crate::pool::PoolStats) now. |
//! | `libch_pool_workers_started_total`, `libch_pool_worker_start_failures_total`, `libch_pool_unpooled_operations_total`, `libch_pool_workers_lost_total` | counter | pool | [`PoolStats`](crate::pool::PoolStats) since the pool was made. |
//! | `libch_keys_live` | gauge | store | [`KeyStats::live`](crate::keys::KeyStats). |
//! | `libch_keys_minted_total`, `libch_keys_revoked_total` | counter | store | [`KeyStats`](crate::keys::KeyStats) since the cache was made. |
//!
//! The metrics from the worker's counts come from operations a worker answered: [`Report::usage`].

use metrics::{Unit, counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram};

use crate::keys::{KeyCache, normalize};
use crate::observe::{Kind, Observer, Report};
use crate::pool::Pool;

/// Records operations, and the snapshots a service publishes, through the recorder the service installed.
#[derive(Debug, Default)]
pub struct Metrics {
    _private: (),
}

impl Metrics {
    /// Describes the metric set to the installed recorder, so an exporter shows each metric's unit and meaning.
    pub fn new() -> Metrics {
        describe_counter!("libch_operations_total", Unit::Count, "Operations ended, by kind, outcome and store.");
        describe_histogram!("libch_operation_duration_seconds", Unit::Seconds, "Each operation's time from the call to its end.");
        describe_histogram!("libch_operation_queued_seconds", Unit::Seconds, "Each operation's wait for a turn in its pool.");
        describe_histogram!("libch_worker_cpu_seconds", Unit::Seconds, "The CPU a worker spent on its operation.");
        describe_histogram!("libch_worker_memory_bytes", Unit::Bytes, "A worker's anonymous memory as it answered.");
        describe_counter!("libch_inodedb_calls_total", Unit::Count, "Workers' calls to their tenants' InodeDB stores, by call.");
        describe_counter!("libch_s3_requests_total", Unit::Count, "Workers' requests to S3 that read and that write.");
        describe_counter!("libch_insert_batch_bytes_total", Unit::Bytes, "The batches inserts sent.");
        describe_counter!("libch_rows_inserted_total", Unit::Count, "Rows inserts committed to their tables.");
        describe_counter!("libch_parts_committed_total", Unit::Count, "Parts inserts and merges committed.");
        describe_counter!("libch_part_bytes_committed_total", Unit::Bytes, "The bytes on disk of the parts inserts committed.");
        describe_counter!("libch_parts_merged_total", Unit::Count, "Parts merges took as sources.");
        describe_counter!("libch_merges_planned_total", Unit::Count, "Merges plans proposed.");
        describe_counter!("libch_parts_removed_total", Unit::Count, "Covered parts housekeeping removed.");
        describe_counter!("libch_blobs_removed_total", Unit::Count, "Blobs housekeeping removed from S3.");
        describe_gauge!("libch_pool_waiting_workers", Unit::Count, "Workers started and waiting for a request.");
        describe_gauge!("libch_pool_running_operations", Unit::Count, "Operations running in the pool.");
        describe_counter!("libch_pool_workers_started_total", Unit::Count, "Workers the pool started.");
        describe_counter!("libch_pool_worker_start_failures_total", Unit::Count, "Attempts to start a worker that failed.");
        describe_counter!("libch_pool_unpooled_operations_total", Unit::Count, "Operations that found no worker waiting and started their own.");
        describe_counter!("libch_pool_workers_lost_total", Unit::Count, "Waiting workers that had exited when an operation came for them.");
        describe_gauge!("libch_keys_live", Unit::Count, "Keys a cache holds in its store.");
        describe_counter!("libch_keys_minted_total", Unit::Count, "Keys a cache minted.");
        describe_counter!("libch_keys_revoked_total", Unit::Count, "Keys a cache revoked.");
        Metrics { _private: () }
    }

    /// Publishes the pool's figures under the label `pool = name`.
    pub fn record_pool(&self, pool: &Pool, name: &str) {
        let stats = pool.stats();
        let pool = [("pool", name.to_string())];
        gauge!("libch_pool_waiting_workers", &pool).set(stats.waiting as f64);
        gauge!("libch_pool_running_operations", &pool).set(stats.running as f64);
        counter!("libch_pool_workers_started_total", &pool).absolute(stats.started);
        counter!("libch_pool_worker_start_failures_total", &pool).absolute(stats.failed_starts);
        counter!("libch_pool_unpooled_operations_total", &pool).absolute(stats.unpooled);
        counter!("libch_pool_workers_lost_total", &pool).absolute(stats.lost);
    }

    /// Publishes the key cache's figures under the label of its store.
    pub fn record_keys(&self, keys: &KeyCache) {
        let stats = keys.stats();
        let store = [("store", keys.store().to_string())];
        gauge!("libch_keys_live", &store).set(stats.live as f64);
        counter!("libch_keys_minted_total", &store).absolute(stats.minted);
        counter!("libch_keys_revoked_total", &store).absolute(stats.revoked);
    }
}

impl Observer for Metrics {
    fn operation(&self, report: &Report) {
        let kind = report.kind.name();
        let ended = [("kind", kind.to_string()), ("outcome", report.ended.name().to_string()), ("store", normalize(&report.store))];
        counter!("libch_operations_total", &ended).increment(1);
        histogram!("libch_operation_duration_seconds", &ended).record(report.elapsed.as_secs_f64());
        histogram!("libch_operation_queued_seconds", "kind" => kind).record(report.queued.as_secs_f64());

        if let Some(usage) = &report.usage {
            histogram!("libch_worker_cpu_seconds", "kind" => kind).record(usage.cpu.as_secs_f64());
            histogram!("libch_worker_memory_bytes", "kind" => kind).record(usage.peak_anonymous_memory_bytes as f64);
            let calls = [("commit", usage.inodedb_commits), ("stat", usage.inodedb_stats), ("read", usage.inodedb_reads), ("list", usage.inodedb_lists)];
            for (call, count) in calls {
                counter!("libch_inodedb_calls_total", "kind" => kind, "call" => call).increment(count);
            }
            counter!("libch_s3_requests_total", "kind" => kind, "request" => "read").increment(usage.s3_reads);
            counter!("libch_s3_requests_total", "kind" => kind, "request" => "write").increment(usage.s3_writes);
        }

        let work = &report.work;
        match report.kind {
            Kind::Insert => {
                counter!("libch_insert_batch_bytes_total").increment(work.batch_bytes);
                counter!("libch_rows_inserted_total").increment(work.rows_inserted);
                counter!("libch_parts_committed_total", "kind" => kind).increment(work.parts_committed);
                counter!("libch_part_bytes_committed_total").increment(work.part_bytes);
            }
            Kind::Merge => {
                counter!("libch_parts_committed_total", "kind" => kind).increment(work.parts_committed);
                counter!("libch_parts_merged_total").increment(work.parts_merged);
            }
            Kind::Plan => counter!("libch_merges_planned_total").increment(work.merges_planned),
            Kind::Housekeep => {
                counter!("libch_parts_removed_total").increment(work.parts_removed);
                counter!("libch_blobs_removed_total").increment(work.blobs_removed);
            }
            Kind::Describe | Kind::Share => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    use super::*;
    use crate::observe::{Ended, Failure, Work};
    use crate::worker::Usage;
    use crate::writer::TableName;

    fn report(kind: Kind, ended: Ended, usage: Option<Usage>, work: Work) -> Report {
        Report {
            kind,
            store: "127.0.0.1:7170".into(),
            tenant: "tenants/t1/disk".into(),
            table: Some(TableName::new("d", "events")),
            ended,
            elapsed: Duration::from_millis(30),
            queued: Duration::from_millis(5),
            waiting_worker: true,
            usage,
            work,
        }
    }

    /// Every metric recorded, as `name{label=value,…}` in label order, with its value.
    fn recorded(record: impl FnOnce()) -> Vec<(String, DebugValue)> {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, record);
        let mut metrics: Vec<(String, DebugValue)> = snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .map(|(key, _, _, value)| {
                let key = key.key();
                let mut labels: Vec<String> = key.labels().map(|label| format!("{}={}", label.key(), label.value())).collect();
                labels.sort();
                (format!("{}{{{}}}", key.name(), labels.join(",")), value)
            })
            .collect();
        metrics.sort_by(|a, b| a.0.cmp(&b.0));
        metrics
    }

    fn find<'a>(metrics: &'a [(String, DebugValue)], name: &str) -> &'a DebugValue {
        &metrics.iter().find(|(key, _)| key == name).unwrap_or_else(|| panic!("{name} among {:?}", metrics.iter().map(|(key, _)| key).collect::<Vec<_>>())).1
    }

    fn counted(metrics: &[(String, DebugValue)], name: &str) -> u64 {
        match find(metrics, name) {
            DebugValue::Counter(value) => *value,
            other => panic!("{name} is {other:?}"),
        }
    }

    #[test]
    fn an_insert_records_its_ending_its_worker_and_its_work() {
        let usage = Usage {
            operations: 1,
            inodedb_commits: 2,
            inodedb_stats: 11,
            inodedb_reads: 3,
            inodedb_lists: 6,
            s3_reads: 0,
            s3_writes: 1,
            cpu: Duration::from_millis(16),
            peak_anonymous_memory_bytes: 32 << 20,
            peak_threads: 33,
        };
        let work = Work { batch_bytes: 81_000, rows_inserted: 1_000, parts_committed: 2, part_bytes: 9_000, ..Work::default() };
        let metrics = recorded(|| Metrics::new().operation(&report(Kind::Insert, Ended::Committed, Some(usage), work)));

        let ended = "{kind=insert,outcome=committed,store=http://127.0.0.1:7170}";
        assert_eq!(counted(&metrics, &format!("libch_operations_total{ended}")), 1);
        assert_eq!(find(&metrics, &format!("libch_operation_duration_seconds{ended}")), &DebugValue::Histogram(vec![0.03.into()]));
        assert_eq!(find(&metrics, "libch_operation_queued_seconds{kind=insert}"), &DebugValue::Histogram(vec![0.005.into()]));
        assert_eq!(find(&metrics, "libch_worker_cpu_seconds{kind=insert}"), &DebugValue::Histogram(vec![0.016.into()]));
        assert_eq!(find(&metrics, "libch_worker_memory_bytes{kind=insert}"), &DebugValue::Histogram(vec![((32u64 << 20) as f64).into()]));
        let calls: Vec<u64> =
            ["commit", "stat", "read", "list"].map(|call| counted(&metrics, &format!("libch_inodedb_calls_total{{call={call},kind=insert}}"))).into();
        assert_eq!(calls, [2, 11, 3, 6]);
        assert_eq!(counted(&metrics, "libch_s3_requests_total{kind=insert,request=write}"), 1);
        assert_eq!(counted(&metrics, "libch_s3_requests_total{kind=insert,request=read}"), 0);
        assert_eq!(counted(&metrics, "libch_insert_batch_bytes_total{}"), 81_000);
        assert_eq!(counted(&metrics, "libch_rows_inserted_total{}"), 1_000);
        assert_eq!(counted(&metrics, "libch_parts_committed_total{kind=insert}"), 2);
        assert_eq!(counted(&metrics, "libch_part_bytes_committed_total{}"), 9_000);
        let names: Vec<&str> = metrics.iter().map(|(key, _)| key.split('{').next().unwrap()).collect();
        assert!(!names.contains(&"libch_parts_removed_total"), "an insert records only an insert's work");
        assert!(metrics.iter().all(|(key, _)| !key.contains("tenants/")), "no metric names the tenant");
    }

    #[test]
    fn a_failure_without_an_answer_records_its_ending_alone() {
        let failed = report(Kind::Merge, Ended::Failed(Failure::Deadline), None, Work::default());
        let fenced = report(Kind::Housekeep, Ended::Fenced, None, Work::default());
        let metrics = recorded(|| {
            let observer = Metrics::new();
            observer.operation(&failed);
            observer.operation(&failed);
            observer.operation(&fenced);
        });
        assert_eq!(counted(&metrics, "libch_operations_total{kind=merge,outcome=deadline,store=http://127.0.0.1:7170}"), 2);
        assert_eq!(counted(&metrics, "libch_operations_total{kind=housekeep,outcome=fenced,store=http://127.0.0.1:7170}"), 1);
        assert!(metrics.iter().all(|(key, _)| !key.starts_with("libch_worker_") && !key.starts_with("libch_inodedb_")), "no worker answered");
        assert_eq!(counted(&metrics, "libch_parts_merged_total{}"), 0);
        assert_eq!(counted(&metrics, "libch_parts_removed_total{}"), 0);
    }

    #[test]
    fn every_ending_is_a_label() {
        let endings = [
            Ended::Committed,
            Ended::Duplicate,
            Ended::SchemaChanged,
            Ended::Fenced,
            Ended::ClaimLost,
            Ended::Refused,
            Ended::Failed(Failure::Start),
            Ended::Failed(Failure::Closed),
            Ended::Failed(Failure::Deadline),
            Ended::Failed(Failure::Exited),
            Ended::Failed(Failure::Protocol),
            Ended::Failed(Failure::Exception { code: Some(241) }),
            Ended::Failed(Failure::Cancelled),
        ];
        let metrics = recorded(|| {
            let observer = Metrics::new();
            for ended in endings {
                observer.operation(&report(Kind::Insert, ended, None, Work::default()));
            }
        });
        let outcomes: Vec<&str> = metrics
            .iter()
            .filter(|(key, _)| key.starts_with("libch_operations_total"))
            .map(|(key, _)| key.split("outcome=").nth(1).unwrap().split(',').next().unwrap())
            .collect();
        assert_eq!(outcomes.len(), endings.len(), "{outcomes:?}");
        for ended in endings {
            assert!(outcomes.contains(&ended.name()), "{}", ended.name());
        }
    }
}
