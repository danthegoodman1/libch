//! Operation reports against the fork's worker: each operation of a tenant's round through a pool reports once,
//! with the usage its writer sums and the rows and parts it committed.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use libch::observe::{Ended, Kind, Observer, Report, Work};
use libch::pool::{Pool, PoolOptions};
use libch::worker::Usage;
use libch::writer::{Budget, MergeCandidate};
use libch::{Outcome, PartInfo, Writer};
use libch_testkit::ops::{active, insert, short_grace, stored, tenant_with_parts};
use libch_testkit::schema::{batch, events};
use libch_testkit::stack;

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
}

/// What the reports' usage adds up to, as a writer sums it.
fn summed(reports: &[Report]) -> Usage {
    let mut total = Usage::default();
    for usage in reports.iter().filter_map(|report| report.usage) {
        total += usage;
    }
    total
}

#[tokio::test]
async fn a_round_of_operations_reports_what_each_did() {
    let stack = stack().await;
    let (fixture, _) = tenant_with_parts(0).await;
    let reports = Arc::new(Reports::default());
    let pool = Pool::with_observer(stack.worker_command(), PoolOptions { waiting: 2, concurrency: 4 }, reports.clone()).unwrap();
    let writer = pool.writer(&fixture.tenant, &fixture.key).with_settings_xml(short_grace());

    // Each insert commits 100 rows to the events table, one part there and one in the view's target.
    let batches: Vec<_> = (0..4).map(|index| batch(index, 100)).collect();
    for batch in &batches {
        assert!(insert(&writer, batch).await.unwrap().is_committed());
    }
    assert_eq!(insert(&writer, &batches[0]).await.unwrap(), Outcome::Duplicate);
    let inserts = reports.take();
    assert_eq!(inserts.len(), 5);
    for (report, batch) in inserts.iter().zip(&batches) {
        assert_eq!((report.kind, report.ended, report.table.as_ref()), (Kind::Insert, Ended::Committed, Some(&events())));
        assert_eq!((report.store.as_str(), report.tenant.as_str()), (fixture.tenant.store.as_str(), fixture.tenant.root.as_str()));
        assert_eq!((report.work.rows_inserted, report.work.parts_committed, report.work.batch_bytes), (100, 2, batch.data.len() as u64));
        assert!(report.work.part_bytes > 0);
        assert!(report.work.part_rows >= 100, "the events table's rows, and the view's target's");
        let usage = report.usage.expect("the worker answered");
        assert!(usage.inodedb_commits >= 2 && usage.cpu > Duration::ZERO, "{usage:?}");
        // Two commits and some twenty calls take time, which the worker counts beside its CPU.
        assert!(usage.inodedb_time > Duration::ZERO && usage.inodedb_retries == 0, "{usage:?}");
    }
    assert_eq!((inserts[4].ended, inserts[4].work.rows_inserted, inserts[4].work.parts_committed), (Ended::Duplicate, 0, 0));
    assert!(inserts.iter().all(|report| report.elapsed >= report.queued));

    let planned = writer.plan(&events(), &Budget::default()).await.unwrap();
    let parts = active(&stored(&fixture, &events()).await);
    let sources: Vec<PartInfo> = parts[..2].iter().map(|name| PartInfo::parse(name).unwrap()).collect();
    let candidate = MergeCandidate { partition_id: sources[0].partition_id.clone(), sources };
    assert!(writer.merge(&events(), &candidate).await.unwrap().is_committed());
    // The merged parts outlive the one-second grace before housekeeping removes them.
    tokio::time::sleep(Duration::from_millis(2_100)).await;
    let housekept = writer.housekeep(&events()).await.unwrap().committed().expect("housekeeping commits");
    writer.describe(false).await.unwrap();
    writer.share().await.unwrap();

    let rest = reports.take();
    let kinds: Vec<(Kind, Ended)> = rest.iter().map(|report| (report.kind, report.ended)).collect();
    assert_eq!(kinds, [Kind::Plan, Kind::Merge, Kind::Housekeep, Kind::Describe, Kind::Share].map(|kind| (kind, Ended::Committed)));
    assert_eq!(rest[0].work, Work { merges_planned: planned.len() as u64, ..Work::default() });
    let merged = rest[1].work;
    assert_eq!((merged.parts_committed, merged.parts_merged, merged.part_rows), (1, 2, 200), "two parts of 100 rows");
    assert!(merged.part_bytes > 0);
    assert_eq!(rest[2].work.parts_removed, housekept.removed.len() as u64);
    assert_eq!(housekept.removed.len(), 2, "{housekept:?}");
    assert_eq!(rest[2].work.blobs_removed, housekept.blobs_removed);

    // The reports carry, between them, every figure the writer summed.
    let all: Vec<Report> = inserts.into_iter().chain(rest).collect();
    assert!(all.iter().all(|report| report.usage.is_some()), "every operation answered");
    assert_eq!(summed(&all), writer.usage());
    assert!(all.iter().filter(|report| report.waiting_worker).count() >= all.len() / 2, "the pool's workers served most operations");

    pool.close().await;
    fixture.remove().await.unwrap();
}
