//! What tests of the worker's operations share: a tenant with parts, a merge by part names, the settings that
//! switch on a failpoint or shorten a claim, and the checks a test ends with.

use std::time::{SystemTime, UNIX_EPOCH};

use libch::catalog::TableParts;
use libch::stock::Exclusive;
use libch::worker::OpWriter;
use libch::writer::{Insert, MergeCandidate, Merged, TableName};
use libch::{Error, Outcome, PartInfo, Writer, check};

use crate::schema::{Batch, EVENTS_SCHEMA, batch, events};
use crate::{Reader, TenantFixture, stack};

/// A tenant with the events schema and `batches` batches of 100 events, each inserted by a worker: parts
/// `202611_{n}_{n}_0` in `d.events` and `all_{n}_{n}_1` in `d.daily`.
pub async fn tenant_with_parts(batches: u64) -> (TenantFixture, OpWriter) {
    let stack = stack().await;
    let fixture = TenantFixture::create(stack).await.unwrap();
    Exclusive::run(&stack.stock_env(), &fixture.tenant, &fixture.key, &EVENTS_SCHEMA).await.unwrap();
    let writer = OpWriter::new(stack.worker_command(), &fixture.tenant, &fixture.key);
    for index in 0..batches {
        assert!(insert(&writer, &batch(index, 100)).await.unwrap().is_committed());
    }
    (fixture, writer)
}

/// Inserts a batch into `d.events`.
pub async fn insert(writer: &OpWriter, batch: &Batch) -> Result<Outcome<libch::writer::Inserted>, Error> {
    writer.insert(Insert { table: &events(), format: "JSONEachRow", token: &batch.token, data: batch.data.clone() }).await
}

pub fn parts(names: &[&str]) -> Vec<PartInfo> {
    names.iter().map(|name| PartInfo::parse(name).unwrap_or_else(|| panic!("`{name}` is a part name"))).collect()
}

/// Merges the named parts of one partition.
pub async fn merge(writer: &OpWriter, table: &TableName, partition: &str, sources: &[&str]) -> Result<Outcome<Merged>, Error> {
    writer.merge(table, &MergeCandidate { partition_id: partition.into(), sources: parts(sources) }).await
}

/// What the table's directory holds now.
pub async fn stored(fixture: &TenantFixture, table: &TableName) -> TableParts {
    let catalog = fixture.catalog().await.unwrap();
    let definition = catalog.table(table).await.unwrap().unwrap();
    catalog.parts(&definition).await.unwrap()
}

pub fn active(parts: &TableParts) -> Vec<String> {
    parts.active().iter().map(PartInfo::name).collect()
}

/// The totals a reader finds: rows and sum of the events, and the sum through the view's target.
pub async fn totals(fixture: &TenantFixture) -> (u64, u64, u64) {
    let reader = Reader::open(stack().await, fixture).await.unwrap();
    let totals = (
        reader.number("SELECT count() FROM d.events").await.unwrap(),
        reader.number("SELECT sum(v) FROM d.events").await.unwrap(),
        reader.number("SELECT sumMerge(total) FROM d.daily").await.unwrap(),
    );
    reader.close().await.unwrap();
    totals
}

/// Runs the invariant checker, which reads the disk and changes nothing.
pub async fn assert_consistent(fixture: &TenantFixture) {
    let report = check::tenant(&fixture.tenant, &fixture.key).await.unwrap();
    assert!(report.is_clean(), "{report}");
}

/// Runs the invariant checker, then has a stock exclusive writer load the disk. That writer's start removes
/// every temporary directory, as a server's does.
pub async fn assert_clean(fixture: &TenantFixture) {
    assert_consistent(fixture).await;
    let rejected = check::stock_load(&stack().await.stock_env(), &fixture.tenant, &fixture.key).await.unwrap();
    assert!(rejected.is_empty(), "a stock writer rejects: {rejected:?}");
}

pub fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
}

/// Settings that switch a failpoint on in a worker from its start. A worker stays at a pausing failpoint until
/// its deadline kills it.
pub fn fail_point(name: &str) -> String {
    format!("<fail_points_active><{name}>1</{name}></fail_points_active>")
}

/// Settings under which a worker's merge claims last `milliseconds` without renewal.
pub fn claim_ttl(milliseconds: u64) -> String {
    format!("<storage_configuration><disks><bighouse><inodedb_claim_ttl_ms>{milliseconds}</inodedb_claim_ttl_ms></bighouse></disks></storage_configuration>")
}

/// Settings under which a worker's tables keep covered parts for one second.
pub fn short_grace() -> String {
    "<merge_tree><old_parts_lifetime>1</old_parts_lifetime></merge_tree>".into()
}
