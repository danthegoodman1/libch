//! The tenant fixture leaves no disk, key or S3 object behind, however its test ends, and what its removal
//! freed in the store is collected then or by a later test process.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use libch::inodedb::{Client, FsError};
use libch::s3::S3Client;
use libch::stock::{Exclusive, StockWriter, StockWriterOptions};
use libch::tenant::store_url;
use libch::writer::{Insert, TableName};
use libch::{Outcome, Tenant, Writer};
use libch_testkit::{Stack, TenantFixture, collect_pending, stack};

/// What identifies a fixture after it is gone.
#[derive(Clone)]
struct Traces {
    home: String,
    tenant: Tenant,
    key: String,
}

/// Creates a tenant and writes a table with a part large enough to put a blob in S3.
async fn tenant_with_data(stack: &Stack) -> (TenantFixture, Traces) {
    let fixture = TenantFixture::create(stack).await.unwrap();
    let env = stack.stock_env();
    Exclusive::run(&env, &fixture.tenant, &fixture.key, &["CREATE DATABASE d", "CREATE TABLE d.t (k UInt64, s String) ENGINE = MergeTree ORDER BY k"])
        .await
        .unwrap();
    let writer = StockWriter::start(&env, &fixture.tenant, &fixture.key, &StockWriterOptions::default()).await.unwrap();
    let rows: String = (0..4_000).map(|k| format!("{{\"k\":{k},\"s\":\"row {k} of a part that outgrows an inline file\"}}\n")).collect();
    let table = TableName::new("d", "t");
    let outcome = writer.insert(Insert { table: &table, format: "JSONEachRow", token: "only", data: Bytes::from(rows) }).await.unwrap();
    assert!(matches!(outcome, Outcome::Committed(_)));
    writer.stop().await.unwrap();

    let traces = Traces { home: fixture.home().to_string(), tenant: fixture.tenant.clone(), key: fixture.key.clone() };
    let blobs = S3Client::new(&traces.tenant.s3).unwrap().list(&traces.tenant.s3.key_prefix()).await.unwrap();
    assert!(!blobs.is_empty(), "the tenant holds blobs in S3 before it is removed");
    assert!(stack.admin().await.unwrap().stat(traces.home.as_str()).await.is_ok());
    (fixture, traces)
}

/// Whether the store still holds a path.
async fn exists(stack: &Stack, path: &str) -> bool {
    match stack.admin().await.unwrap().stat(path).await {
        Ok(_) => true,
        Err(libch::inodedb::Error::Fs(FsError::NotFound)) => false,
        Err(error) => panic!("stat {path}: {error}"),
    }
}

/// The tenant is gone: its disk's directory, its blobs and its key. The directory that held the disk goes with
/// them when everything the removal freed could be collected at once, and with a later collection otherwise.
async fn assert_nothing_left(stack: &Stack, traces: &Traces) {
    assert!(!exists(stack, &format!("{}/disk", traces.home)).await, "the tenant's disk is gone");

    let blobs = S3Client::new(&traces.tenant.s3).unwrap().list(&traces.tenant.s3.key_prefix()).await.unwrap();
    assert!(blobs.is_empty(), "S3 holds nothing under the tenant's prefix: {blobs:?}");

    let revoked = match Client::connect(store_url(&stack.inodedb_endpoint), traces.key.as_str()).await {
        Ok(client) => client.list("").await.map(|_| ()),
        Err(error) => Err(error),
    };
    assert!(revoked.is_err(), "the tenant's key no longer works");

    // Leases of other tests' servers may hold the freed entries back for a while.
    let started = Instant::now();
    while exists(stack, &traces.home).await {
        assert!(started.elapsed() < Duration::from_secs(90), "the tenant's directory is still there after 90 s of collections");
        tokio::time::sleep(Duration::from_millis(500)).await;
        collect_pending(stack).await;
    }
}

#[tokio::test]
async fn a_passing_test_leaves_nothing_behind() {
    let stack = stack().await;
    let traces = {
        let (_fixture, traces) = tenant_with_data(stack).await;
        traces
    };
    assert_nothing_left(stack, &traces).await;
}

#[tokio::test]
async fn a_panicking_test_leaves_nothing_behind() {
    let stack = stack().await;
    let seen: Arc<Mutex<Option<Traces>>> = Arc::default();
    let record = seen.clone();
    let test = tokio::spawn(async move {
        let (_fixture, traces) = tenant_with_data(stack).await;
        *record.lock().unwrap() = Some(traces);
        panic!("the test body fails");
    });
    assert!(test.await.unwrap_err().is_panic());
    let traces = seen.lock().unwrap().clone().expect("the test body ran");
    assert_nothing_left(stack, &traces).await;
}

#[tokio::test]
async fn removing_a_tenant_reports_success_and_leaves_nothing_behind() {
    let stack = stack().await;
    let (fixture, traces) = tenant_with_data(stack).await;
    fixture.remove().await.unwrap();
    assert_nothing_left(stack, &traces).await;
}

/// A reader that began before a tenant was removed holds back what the removal freed. The fixture removes the
/// tenant and leaves its emptied directory with a note, and a later collection finishes once the reader is gone.
#[tokio::test]
async fn a_removal_that_a_lease_held_back_is_finished_later() {
    let stack = stack().await;
    let (fixture, traces) = tenant_with_data(stack).await;
    let lease = stack.admin().await.unwrap().lease_current().await.unwrap();
    fixture.remove().await.unwrap();
    assert!(!exists(stack, &format!("{}/disk", traces.home)).await);
    assert!(exists(stack, &traces.home).await, "the directory waits for what the lease holds");
    collect_pending(stack).await;
    assert!(exists(stack, &traces.home).await, "a collection under the lease leaves it");

    lease.release().await;
    assert_nothing_left(stack, &traces).await;
}

#[tokio::test]
async fn tenants_are_separate() {
    let stack = stack().await;
    let first = TenantFixture::create(stack).await.unwrap();
    let second = TenantFixture::create(stack).await.unwrap();
    assert_ne!(first.tenant.root, second.tenant.root);
    assert_ne!(first.tenant.s3.prefix, second.tenant.s3.prefix);
    assert_ne!(first.key, second.key);
    // A tenant's key sees its own directory as its whole tree.
    let listing = first.client().await.unwrap().list("").await.unwrap();
    assert!(listing.entries.is_empty(), "a new tenant's directory is empty");
}
