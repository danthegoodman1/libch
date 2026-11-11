//! The checker passes a healthy disk and names each corruption planted into one.

use libch::catalog::{Catalog, Table};
use libch::check::{self, Violation};
use libch::disk::Value;
use libch::inodedb::{List, Op};
use libch::s3::S3Client;
use libch::stock::{StockWriter, StockWriterOptions};
use libch::writer::{Insert, TableName};
use libch::{Outcome, Writer};
use libch_testkit::conformance::tenant_with_events;
use libch_testkit::{Stack, TenantFixture, apply, schema, stack};

/// A tenant whose events table holds three small parts and one part large enough to keep blobs in S3, written
/// by a shared writer that has stopped.
async fn written_tenant(stack: &Stack) -> (TenantFixture, Catalog, Table) {
    let fixture = tenant_with_events(stack).await;
    let writer = StockWriter::start(&stack.stock_env(), &fixture.tenant, &fixture.key, &StockWriterOptions::default()).await.unwrap();
    let mut batches: Vec<schema::Batch> = (0..3).map(|index| schema::batch(index, 50)).collect();
    batches.push(schema::batch(1_000, 60_000));
    for batch in &batches {
        let outcome = writer.insert(Insert { table: &schema::events(), format: "JSONEachRow", token: &batch.token, data: batch.data.clone() }).await.unwrap();
        assert!(matches!(outcome, Outcome::Committed(_)));
    }
    writer.stop().await.unwrap();
    let catalog = fixture.catalog().await.unwrap();
    let events = catalog.table(&schema::events()).await.unwrap().unwrap();
    (fixture, catalog, events)
}

fn events() -> TableName {
    schema::events()
}

/// Asserts that a report names the violation, and prints the whole report when it does not.
fn assert_reported(report: &check::Report, expected: &Violation) {
    assert!(report.violations.contains(expected), "expected `{expected}` in: {report}");
}

#[tokio::test]
async fn a_healthy_disk_is_clean() {
    let stack = stack().await;
    let (fixture, _catalog, _events) = written_tenant(stack).await;
    let report = check::tenant(&fixture.tenant, &fixture.key).await.unwrap();
    assert!(report.is_clean(), "{report}");
    assert_eq!(report.tables, 2, "the events table and the view's target");
    assert_eq!(report.parts, 8, "four parts in each table");
    assert!(report.files > 40, "{report}");
    assert!(report.blobs > 0, "the large part keeps blobs in S3: {report}");
    assert!(report.to_string().ends_with("clean"));

    let rejected = check::stock_load(&stack.stock_env(), &fixture.tenant, &fixture.key).await.unwrap();
    assert!(rejected.is_empty(), "{rejected:?}");
}

#[tokio::test]
async fn intersecting_parts_are_named() {
    let stack = stack().await;
    let (fixture, catalog, events_table) = written_tenant(stack).await;
    let directory = format!("tree/{}", events_table.data_path());
    // Two merges that share block 2: neither holds the other.
    apply(catalog.client(), [Op::mkdir(format!("{directory}/202611_1_2_1")), Op::mkdir(format!("{directory}/202611_2_3_1"))]).await.unwrap();

    let report = check::tenant(&fixture.tenant, &fixture.key).await.unwrap();
    assert_reported(&report, &Violation::IntersectingParts { table: events(), first: "202611_1_2_1".into(), second: "202611_2_3_1".into() });
}

#[tokio::test]
async fn a_repeated_block_number_is_named() {
    let stack = stack().await;
    let (fixture, catalog, events_table) = written_tenant(stack).await;
    // Block 2 again, in another month's partition.
    apply(catalog.client(), [Op::mkdir(format!("tree/{}/202612_2_2_0", events_table.data_path()))]).await.unwrap();

    let report = check::tenant(&fixture.tenant, &fixture.key).await.unwrap();
    assert_reported(&report, &Violation::RepeatedBlock { table: events(), block: 2, parts: vec!["202611_2_2_0".into(), "202612_2_2_0".into()] });
}

#[tokio::test]
async fn a_missing_part_file_is_named_and_a_stock_writer_rejects_the_part() {
    let stack = stack().await;
    let (fixture, catalog, events_table) = written_tenant(stack).await;
    apply(catalog.client(), [Op::unlink(format!("tree/{}/202611_1_1_0/columns.txt", events_table.data_path()))]).await.unwrap();

    let report = check::tenant(&fixture.tenant, &fixture.key).await.unwrap();
    assert_reported(&report, &Violation::MissingPartFile { table: events(), part: "202611_1_1_0".into(), file: "columns.txt".into() });
    assert_eq!(report.violations.len(), 1, "{report}");

    let rejected = check::stock_load(&stack.stock_env(), &fixture.tenant, &fixture.key).await.unwrap();
    assert!(
        rejected.iter().any(|violation| matches!(violation, Violation::DetachedPart { table, part } if *table == events() && part.contains("202611_1_1_0"))),
        "a stock writer sets the part aside: {rejected:?}"
    );
}

/// The object key of one blob that a file of the table's large part names.
async fn a_blob_of(catalog: &Catalog, table: &Table, tenant: &libch::Tenant) -> (String, String, u64) {
    let directory = format!("tree/{}/202611_4_4_0", table.data_path());
    let listing = catalog.list_with(&directory, &List { values: true, ..List::default() }).await.unwrap().expect("the large part");
    for entry in &listing.entries {
        let path = format!("{directory}/{}", entry.name_lossy());
        let Some(bytes) = entry.value.as_deref() else { continue };
        let Ok(record) = Value::decode(bytes, &path).and_then(|value| value.record(&path)) else { continue };
        if let Some(blob) = record.blobs.first() {
            return (path, record.object_keys(&tenant.s3.key_prefix())[0].clone(), blob.size);
        }
    }
    panic!("no file of the large part keeps a blob");
}

#[tokio::test]
async fn a_missing_blob_is_named() {
    let stack = stack().await;
    let (fixture, catalog, events_table) = written_tenant(stack).await;
    let (path, key, _size) = a_blob_of(&catalog, &events_table, &fixture.tenant).await;
    S3Client::new(&fixture.tenant.s3).unwrap().delete(&key).await.unwrap();

    let report = check::tenant(&fixture.tenant, &fixture.key).await.unwrap();
    assert_reported(&report, &Violation::MissingBlob { path, key });
    assert_eq!(report.violations.len(), 1, "{report}");
}

#[tokio::test]
async fn a_blob_of_another_size_is_named() {
    let stack = stack().await;
    let (fixture, catalog, events_table) = written_tenant(stack).await;
    let (path, key, recorded) = a_blob_of(&catalog, &events_table, &fixture.tenant).await;
    S3Client::new(&fixture.tenant.s3).unwrap().put(&key, "cut short".into()).await.unwrap();

    let report = check::tenant(&fixture.tenant, &fixture.key).await.unwrap();
    assert_reported(&report, &Violation::BlobSize { path, key, recorded, found: 9 });
}

#[tokio::test]
async fn a_claim_with_no_table_is_named() {
    let stack = stack().await;
    let (fixture, catalog, _events) = written_tenant(stack).await;
    let orphan = "store/000/00000000-0000-4000-8000-000000000abc";
    let claims = format!("coord/{orphan}/claims");
    apply(catalog.client(), [Op::mkdir(claims.as_str()).parents(), Op::put(format!("{claims}/all_1_1_0"), "done 0a1b2c3d all_1_2_1")]).await.unwrap();

    let report = check::tenant(&fixture.tenant, &fixture.key).await.unwrap();
    assert_reported(&report, &Violation::OrphanCoordination { path: orphan.into() });
    assert_eq!(report.violations.len(), 1, "{report}");
}

#[tokio::test]
async fn a_counter_behind_its_parts_is_named() {
    let stack = stack().await;
    let (fixture, catalog, events_table) = written_tenant(stack).await;
    // The shared epoch is still the disk's epoch, and its counter says two where four blocks are committed.
    apply(catalog.client(), [Op::put(format!("coord/{}/block_number", events_table.data_path()), "2")]).await.unwrap();

    let report = check::tenant(&fixture.tenant, &fixture.key).await.unwrap();
    assert_reported(&report, &Violation::CounterBehind { table: events(), counter: 2, part: "202611_3_3_0".into() });
    assert_reported(&report, &Violation::CounterBehind { table: events(), counter: 2, part: "202611_4_4_0".into() });
    assert_eq!(report.violations.len(), 2, "{report}");
}

#[tokio::test]
async fn a_file_in_no_disk_format_is_named() {
    let stack = stack().await;
    let (fixture, catalog, events_table) = written_tenant(stack).await;
    let path = format!("tree/{}/202611_2_2_0/count.txt", events_table.data_path());
    apply(catalog.client(), [Op::put(path.as_str(), "not a disk value")]).await.unwrap();

    let report = check::tenant(&fixture.tenant, &fixture.key).await.unwrap();
    assert!(report.violations.iter().any(|violation| matches!(violation, Violation::UnreadableFile { path: found, .. } if *found == path)), "{report}");
}

#[tokio::test]
async fn a_spilled_record_is_followed_to_its_blobs() {
    use libch::disk::{Blob, Record, ValueKind};

    let stack = stack().await;
    let (fixture, catalog, events_table) = written_tenant(stack).await;
    let (_path, existing, size) = a_blob_of(&catalog, &events_table, &fixture.tenant).await;
    let s3 = S3Client::new(&fixture.tenant.s3).unwrap();
    let prefix = fixture.tenant.s3.key_prefix();

    // A record too large for an InodeDB value lives in a blob of its own, and the file's value names that
    // blob. This one names a blob that exists and a blob that does not.
    let absent = format!("{prefix}no/such/blob");
    let record = Record {
        version: 5,
        blobs: vec![Blob { size, key: existing }, Blob { size: 77, key: absent.clone() }],
        ref_count: 0,
        read_only: false,
        inline: Vec::new(),
    };
    let spill_key = format!("{prefix}spilled/record");
    s3.put(&spill_key, record.serialize().into()).await.unwrap();
    let value = Value { kind: ValueKind::Spilled, retained: false, ctime: 0, body: spill_key.clone().into_bytes() };
    let path = format!("tree/{}/spilled.bin", events_table.data_path());
    apply(catalog.client(), [Op::put(path.as_str(), value.encode())]).await.unwrap();

    let report = check::tenant(&fixture.tenant, &fixture.key).await.unwrap();
    assert_reported(&report, &Violation::MissingBlob { path: path.clone(), key: absent });
    assert_eq!(report.violations.len(), 1, "{report}");

    // With the spilled record itself gone, the file names a blob S3 does not hold.
    s3.delete(&spill_key).await.unwrap();
    let report = check::tenant(&fixture.tenant, &fixture.key).await.unwrap();
    assert_reported(&report, &Violation::MissingBlob { path, key: spill_key });
}

#[tokio::test]
async fn the_checker_stays_clean_beside_live_writers() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    let stack = stack().await;
    let fixture = tenant_with_events(stack).await;
    // The writer merges on its own schedule and removes merged parts after a second, so parts appear, are
    // covered and vanish while the checker reads.
    let options = StockWriterOptions {
        background_merges: true,
        merge_tree_settings_xml: "<old_parts_lifetime>1</old_parts_lifetime>".into(),
        ..StockWriterOptions::default()
    };
    let writer = Arc::new(StockWriter::start(&stack.stock_env(), &fixture.tenant, &fixture.key, &options).await.unwrap());
    let done = Arc::new(AtomicBool::new(false));
    let inserting = {
        let (writer, done) = (writer.clone(), done.clone());
        tokio::spawn(async move {
            let mut batches = Vec::new();
            for index in 0..250 {
                let batch = schema::batch(index, 30);
                let outcome =
                    writer.insert(Insert { table: &schema::events(), format: "JSONEachRow", token: &batch.token, data: batch.data.clone() }).await.unwrap();
                assert!(matches!(outcome, Outcome::Committed(_)));
                batches.push(batch);
            }
            done.store(true, Ordering::Relaxed);
            batches
        })
    };

    let mut checks = 0;
    while !done.load(Ordering::Relaxed) {
        let report = check::tenant(&fixture.tenant, &fixture.key).await.unwrap();
        assert!(report.is_clean(), "check {checks}: {report}");
        checks += 1;
    }
    let batches = inserting.await.unwrap();
    assert!(checks >= 3, "the checker ran while the writer wrote: {checks} checks");
    Arc::try_unwrap(writer).expect("the inserting task is done").stop().await.unwrap();

    let report = check::tenant(&fixture.tenant, &fixture.key).await.unwrap();
    assert!(report.is_clean(), "{report}");
    assert!(check::stock_load(&stack.stock_env(), &fixture.tenant, &fixture.key).await.unwrap().is_empty());
    let rows: u64 = batches.iter().map(|batch| batch.rows).sum();
    assert_eq!(libch_testkit::Reader::number_once(stack, &fixture, "SELECT count() FROM d.events").await.unwrap(), rows);
}
