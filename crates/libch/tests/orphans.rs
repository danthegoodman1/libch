//! The orphan count: blobs in S3 that no file of the disk names, and no freed file either.
//!
//! A batch of 60,000 events is large enough that its part keeps blobs in S3, where a small part's files live in
//! InodeDB whole.

use std::time::{Duration, Instant};

use libch::Writer;
use libch::check;
use libch::worker::OpWriter;
use libch_testkit::ops::{assert_clean, fail_point, insert, merge, short_grace, stored, tenant_with_parts};
use libch_testkit::schema::{batch, events};

const LARGE: u64 = 60_000;

/// A writer whose workers stop at `point` and die two seconds after their request.
fn doomed_at(writer: &OpWriter, point: &str) -> OpWriter {
    writer.clone().with_settings_xml(fail_point(point)).with_deadline(Duration::from_secs(2))
}

#[tokio::test]
async fn operations_that_finish_leave_no_orphan() {
    let (fixture, writer) = tenant_with_parts(0).await;
    for index in 0..3 {
        assert!(insert(&writer, &batch(index, LARGE)).await.unwrap().is_committed());
    }
    let written = check::orphans(&fixture.tenant, &fixture.key).await.unwrap();
    assert!(written.named > 0, "the parts keep blobs in S3: {written}");
    assert_eq!((written.freed, written.orphans.len()), (0, 0), "{written}");
    assert_eq!(written.stored, written.named, "{written}");

    // A merge writes its part's blobs, and housekeeping frees its sources' blobs and removes them from S3.
    let sources = ["202611_1_1_0", "202611_2_2_0", "202611_3_3_0"];
    assert!(merge(&writer, &events(), "202611", &sources).await.unwrap().is_committed());
    let merged = check::orphans(&fixture.tenant, &fixture.key).await.unwrap();
    assert!(merged.stored > written.stored, "the merged part's blobs joined the sources': {merged}");
    assert_eq!(merged.orphans.len(), 0, "{merged}");

    // Housekeeping removes the covered sources, which frees their blobs, and removes freed blobs from S3 once
    // every lease older than the removal has ended. A killed worker's lease lasts out its 30 seconds, and the
    // store's leases are shared by every tenant, so the blobs may take a later round.
    tokio::time::sleep(Duration::from_millis(1_100)).await;
    let housekeeper = writer.clone().with_settings_xml(short_grace());
    let housekept = housekeeper.housekeep(&events()).await.unwrap().committed().unwrap();
    assert_eq!(housekept.removed.len(), 3, "{housekept:?}");
    let started = Instant::now();
    let after = loop {
        let found = check::orphans(&fixture.tenant, &fixture.key).await.unwrap();
        assert_eq!(found.orphans.len(), 0, "{found}");
        if found.freed == 0 {
            break found;
        }
        assert!(started.elapsed() < Duration::from_secs(60), "freed blobs stay in S3: {found}");
        tokio::time::sleep(Duration::from_millis(500)).await;
        housekeeper.housekeep(&events()).await.unwrap();
    };
    assert_eq!(after.stored, after.named, "{after}");
    assert!(after.stored < merged.stored, "{after}");
    assert_clean(&fixture).await;
}

#[tokio::test]
async fn a_worker_killed_before_its_commit_leaves_its_uploads_as_orphans() {
    let (fixture, writer) = tenant_with_parts(0).await;
    assert!(insert(&writer, &batch(0, LARGE)).await.unwrap().is_committed());
    let one = check::orphans(&fixture.tenant, &fixture.key).await.unwrap();

    let killed = insert(&doomed_at(&writer, "inodedb_insert_pause_before_commit"), &batch(1, LARGE)).await;
    assert!(killed.unwrap_err().to_string().contains("was killed"));
    let left = check::orphans(&fixture.tenant, &fixture.key).await.unwrap();
    assert_eq!(left.named, one.named, "the dead worker committed nothing: {left}");
    assert!(!left.orphans.is_empty(), "its uploads stay in S3: {left}");
    assert!(left.bytes() > 0, "{left}");
    assert_eq!(left.stored, left.named + left.orphans.len(), "{left}");

    // The batch goes again under its token and lands once. The first attempt's uploads stay: housekeeping
    // removes what the disk freed, and nothing ever named these.
    assert!(insert(&writer, &batch(1, LARGE)).await.unwrap().is_committed());
    writer.housekeep(&events()).await.unwrap();
    let after = check::orphans(&fixture.tenant, &fixture.key).await.unwrap();
    assert_eq!(after.orphans, left.orphans, "{after}");
    assert!(after.named > one.named, "{after}");
    assert_clean(&fixture).await;
}

#[tokio::test]
async fn a_merge_killed_before_its_commit_leaves_its_part_as_orphans() {
    let (fixture, writer) = tenant_with_parts(0).await;
    for index in 0..2 {
        assert!(insert(&writer, &batch(index, LARGE)).await.unwrap().is_committed());
    }
    let before = check::orphans(&fixture.tenant, &fixture.key).await.unwrap();

    let sources = ["202611_1_1_0", "202611_2_2_0"];
    let killed = merge(&doomed_at(&writer, "inodedb_merge_pause_before_commit"), &events(), "202611", &sources).await;
    assert!(killed.unwrap_err().to_string().contains("was killed"));
    // The merged part's own files join the table in the merge's commit, so the dead worker leaves a temporary
    // directory that holds the projection's part alone, and the merged part's blobs in S3, where nothing names
    // them.
    let table = stored(&fixture, &events()).await;
    let temporary: Vec<_> = table.other.iter().filter(|name| name.starts_with("tmp_merge_202611_1_2_1_")).collect();
    assert_eq!(temporary.len(), 1, "{:?}", table.other);
    let catalog = fixture.catalog().await.unwrap();
    let definition = catalog.table(&events()).await.unwrap().unwrap();
    assert_eq!(catalog.part_files(&definition, temporary[0]).await.unwrap(), ["by_kind.proj"]);
    let left = check::orphans(&fixture.tenant, &fixture.key).await.unwrap();
    assert_eq!(left.named, before.named, "{left}");
    assert!(!left.orphans.is_empty(), "the merged part's blobs: {left}");

    // Housekeeping removes the directory once it is old enough. The blobs stay.
    tokio::time::sleep(Duration::from_millis(1_100)).await;
    let housekeeper = writer.clone().with_settings_xml("<merge_tree><temporary_directories_lifetime>1</temporary_directories_lifetime></merge_tree>");
    let housekept = housekeeper.housekeep(&events()).await.unwrap().committed().unwrap();
    assert_eq!(housekept.temporary_directories, 1, "{housekept:?}");
    let after = check::orphans(&fixture.tenant, &fixture.key).await.unwrap();
    assert_eq!((after.named, after.orphans.clone()), (before.named, left.orphans), "{after}");
    assert_clean(&fixture).await;
}
