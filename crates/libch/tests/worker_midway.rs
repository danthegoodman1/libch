//! What happens to an operation when the tenant changes under it. Each test holds a worker at a failpoint
//! before its commit, changes something, lets the worker go, and checks that the operation committed nothing
//! and said why, and that the same request then lands once.

use std::time::Duration;

use libch::inodedb::{Grant, List, Op};
use libch::stock::Exclusive;
use libch::worker::OpWriter;
use libch::writer::{Inserted, Merged};
use libch::{Error, Outcome};
use libch_testkit::ops::{Pause, active, assert_clean, insert, merge, stored, tenant_with_parts, totals};
use libch_testkit::schema::{Batch, batch, daily, events};
use libch_testkit::{Reader, TenantFixture, apply, stack};
use tokio::task::JoinHandle;

const BEFORE_INSERT_COMMIT: &str = "inodedb_insert_pause_before_commit";
const BEFORE_MERGE_COMMIT: &str = "inodedb_merge_pause_before_commit";

/// Starts an insert of `batch` under `key` whose worker stops at `pause`, and waits until it has.
async fn held_insert(fixture: &TenantFixture, key: &str, pause: &Pause, batch: &Batch) -> JoinHandle<Result<Outcome<Inserted>, Error>> {
    let writer = OpWriter::new(pause.command().await, &fixture.tenant, key).with_settings_xml(pause.settings());
    let batch = batch.clone();
    let insert = tokio::spawn(async move { insert(&writer, &batch).await });
    pause.reached().await;
    insert
}

/// The events table's parts and the view target's: what an insert that committed nothing leaves as it found.
async fn parts(fixture: &TenantFixture) -> (Vec<String>, Vec<String>) {
    (active(&stored(fixture, &events()).await), active(&stored(fixture, &daily()).await))
}

/// An exclusive writer that starts while an insert is under way ends the insert's epoch: the insert commits
/// nothing and ends `Fenced`. Here the exclusive writer creates a view, which the held worker never loaded; sent
/// again, the batch reaches that view too.
#[tokio::test]
async fn an_exclusive_writer_that_starts_mid_insert_fences_it() {
    let stack = stack().await;
    let (fixture, writer) = tenant_with_parts(1).await;
    let before = parts(&fixture).await;
    let second = batch(1, 100);
    let pause = Pause::at(BEFORE_INSERT_COMMIT).await;
    let held = held_insert(&fixture, &fixture.key, &pause, &second).await;

    let ddl = [
        "CREATE TABLE d.kinds (kind LowCardinality(String), events AggregateFunction(count)) ENGINE = AggregatingMergeTree ORDER BY kind \
         SETTINGS non_replicated_deduplication_window = 10000",
        "CREATE MATERIALIZED VIEW d.kinds_mv TO d.kinds AS SELECT kind, countState() AS events FROM d.events GROUP BY kind",
    ];
    fixture.exclusively(&ddl).await.unwrap();
    pause.resume();
    assert_eq!(held.await.unwrap().unwrap(), Outcome::Fenced);
    assert_eq!(parts(&fixture).await, before, "the fenced insert committed nothing");

    assert!(insert(&writer, &second).await.unwrap().is_committed());
    assert_eq!(totals(&fixture).await, (200, 100 * 99 / 2 + second.sum, 100 * 99 / 2 + second.sum));
    let reader = Reader::open(stack, &fixture).await.unwrap();
    assert_eq!(reader.number("SELECT countMerge(events) FROM d.kinds").await.unwrap(), 100, "the batch reached the view made meanwhile");
    reader.close().await.unwrap();
    assert_clean(&fixture).await;
}

/// A key revoked while its insert is under way: the commit is refused, and the insert ends `Fenced`.
#[tokio::test]
async fn a_key_revoked_mid_insert_fences_it() {
    let stack = stack().await;
    let (fixture, writer) = tenant_with_parts(1).await;
    let before = parts(&fixture).await;
    let second = batch(1, 100);
    let key = fixture.mint(Grant::Write, Duration::ZERO).await.unwrap();
    let pause = Pause::at(BEFORE_INSERT_COMMIT).await;
    let held = held_insert(&fixture, &key, &pause, &second).await;

    stack.admin().await.unwrap().revoke_key(&key).await.unwrap();
    pause.resume();
    assert_eq!(held.await.unwrap().unwrap(), Outcome::Fenced);
    assert_eq!(parts(&fixture).await, before, "the insert committed nothing");

    assert!(insert(&writer, &second).await.unwrap().is_committed());
    assert_eq!(totals(&fixture).await.0, 200);
    assert_clean(&fixture).await;
}

/// A key that runs out while its insert is under way does as a revoked one does.
#[tokio::test]
async fn a_key_that_expires_mid_insert_fences_it() {
    let (fixture, writer) = tenant_with_parts(1).await;
    let before = parts(&fixture).await;
    let second = batch(1, 100);
    let lifetime = Duration::from_secs(3);
    let key = fixture.mint(Grant::Write, lifetime).await.unwrap();
    let pause = Pause::at(BEFORE_INSERT_COMMIT).await;
    let held = held_insert(&fixture, &key, &pause, &second).await;

    tokio::time::sleep(lifetime + Duration::from_secs(1)).await;
    pause.resume();
    assert_eq!(held.await.unwrap().unwrap(), Outcome::Fenced);
    assert_eq!(parts(&fixture).await, before, "the insert committed nothing");

    assert!(insert(&writer, &second).await.unwrap().is_committed());
    assert_eq!(totals(&fixture).await.0, 200);
    assert_clean(&fixture).await;
}

/// A table's definition that changes in InodeDB after a worker loaded it, within one epoch of shared writers:
/// the insert's commit requires the version it loaded, so it commits nothing and ends `SchemaChanged`.
///
/// Every DDL statement runs on an exclusive writer, whose epoch fences the insert first. The definition's
/// version is the second protection, and the test reaches it as a control plane could: it writes the same
/// definition again, which gives it a new version.
#[tokio::test]
async fn a_definition_rewritten_mid_insert_ends_schema_changed() {
    let (fixture, writer) = tenant_with_parts(1).await;
    let before = parts(&fixture).await;
    let second = batch(1, 100);
    let pause = Pause::at(BEFORE_INSERT_COMMIT).await;
    let held = held_insert(&fixture, &fixture.key, &pause, &second).await;

    let catalog = fixture.catalog().await.unwrap();
    let database = catalog.database("d").await.unwrap().unwrap();
    let directory = format!("tree/{}", database.path());
    let listing = catalog.list_with(&directory, &List { values: true, ..List::default() }).await.unwrap().unwrap();
    let definition = listing.entries.iter().find(|entry| entry.name_lossy() == "events.sql").expect("the events table's definition");
    let old_version = catalog.table(&events()).await.unwrap().unwrap().definition_version;
    apply(catalog.client(), [Op::put(format!("{directory}/events.sql"), definition.value.clone().expect("the definition's bytes")).replace()]).await.unwrap();
    assert!(catalog.table(&events()).await.unwrap().unwrap().definition_version > old_version);

    pause.resume();
    assert_eq!(held.await.unwrap().unwrap(), Outcome::SchemaChanged);
    assert_eq!(parts(&fixture).await, before, "the insert committed nothing");

    // A worker that loads the definition as it now is inserts the batch.
    assert!(insert(&writer, &second).await.unwrap().is_committed());
    assert_eq!(totals(&fixture).await.0, 200);
    assert_clean(&fixture).await;
}

/// Starts a merge of the events table's first three parts under `key` whose worker stops at `pause`.
async fn held_merge(fixture: &TenantFixture, key: &str, pause: &Pause) -> JoinHandle<Result<Outcome<Merged>, Error>> {
    let writer = OpWriter::new(pause.command().await, &fixture.tenant, key).with_settings_xml(pause.settings());
    let merge = tokio::spawn(async move { merge(&writer, &events(), "202611", &["202611_1_1_0", "202611_2_2_0", "202611_3_3_0"]).await });
    pause.reached().await;
    merge
}

/// A merge whose key is revoked before its commit commits nothing: its sources stay active, and another worker
/// merges them once the dead merge's claims have run out.
#[tokio::test]
async fn a_key_revoked_mid_merge_fences_it() {
    let stack = stack().await;
    let (fixture, _writer) = tenant_with_parts(3).await;
    let before = totals(&fixture).await;
    let key = fixture.mint(Grant::Write, Duration::ZERO).await.unwrap();
    let pause = Pause::at(BEFORE_MERGE_COMMIT).await;
    let held = held_merge(&fixture, &key, &pause).await;

    stack.admin().await.unwrap().revoke_key(&key).await.unwrap();
    pause.resume();
    assert_eq!(held.await.unwrap().unwrap(), Outcome::Fenced);
    assert_eq!(active(&stored(&fixture, &events()).await), ["202611_1_1_0", "202611_2_2_0", "202611_3_3_0"]);
    assert_eq!(totals(&fixture).await, before);
    assert_clean(&fixture).await;
}

/// An exclusive writer that starts while a merge is under way ends the merge's epoch, and the merge commits
/// nothing. The exclusive writer leaves the table as its own merges made it.
#[tokio::test]
async fn an_exclusive_writer_that_starts_mid_merge_fences_it() {
    let stack = stack().await;
    let (fixture, _writer) = tenant_with_parts(3).await;
    let before = totals(&fixture).await;
    let pause = Pause::at(BEFORE_MERGE_COMMIT).await;
    let held = held_merge(&fixture, &fixture.key, &pause).await;

    Exclusive::run(&stack.stock_env(), &fixture.tenant, &fixture.key, &["ALTER TABLE d.events ADD COLUMN note String DEFAULT ''"]).await.unwrap();
    pause.resume();
    assert_eq!(held.await.unwrap().unwrap(), Outcome::Fenced);
    assert_eq!(totals(&fixture).await, before);
    assert_clean(&fixture).await;
}
