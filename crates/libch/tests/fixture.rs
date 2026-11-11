//! The tenant fixture leaves no InodeDB directory, key or S3 object behind, however its test ends.

use std::sync::{Arc, Mutex};

use bytes::Bytes;
use libch::inodedb::{Client, FsError};
use libch::s3::S3Client;
use libch::stock::{Exclusive, StockWriter, StockWriterOptions};
use libch::tenant::store_url;
use libch::writer::{Insert, TableName};
use libch::{Outcome, Tenant, Writer};
use libch_testkit::{Stack, TenantFixture, stack};

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

async fn assert_nothing_left(stack: &Stack, traces: &Traces) {
    let gone = stack.admin().await.unwrap().stat(traces.home.as_str()).await;
    assert!(matches!(gone, Err(libch::inodedb::Error::Fs(FsError::NotFound))), "the tenant's directory is gone: {gone:?}");

    let blobs = S3Client::new(&traces.tenant.s3).unwrap().list(&traces.tenant.s3.key_prefix()).await.unwrap();
    assert!(blobs.is_empty(), "S3 holds nothing under the tenant's prefix: {blobs:?}");

    let revoked = match Client::connect(store_url(&stack.inodedb_endpoint), traces.key.as_str()).await {
        Ok(client) => client.list("").await.map(|_| ()),
        Err(error) => Err(error),
    };
    assert!(revoked.is_err(), "the tenant's key no longer works");
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
