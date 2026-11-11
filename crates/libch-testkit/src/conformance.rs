//! The conformance scenarios: what every [`Writer`] must do, whatever runs behind it.
//!
//! Each scenario writes a fresh tenant through writers a [`Harness`] supplies, reads the result through
//! read-only servers, and ends with the invariant checker and a stock exclusive load. A test file supplies the
//! harness and calls each scenario.

use std::future::Future;
use std::time::Duration;

use bytes::Bytes;
use futures_util::future::join_all;
use libch::catalog::Catalog;
use libch::inodedb::Grant;
use libch::stock::Exclusive;
use libch::writer::{Budget, Insert, Inserted, TableName};
use libch::{Error, Outcome, PartInfo, Writer, check};

use crate::schema::{self, Batch, EVENTS_SCHEMA};
use crate::{Reader, Stack, TenantFixture};

/// Supplies the writers a scenario runs against.
pub trait Harness: Sync {
    type Writer: Writer + Send + Sync;

    /// Starts a writer on the tenant's disk under `key`, which holds the `write` grant. A scenario may run
    /// several writers at once, each under a key of its own.
    fn start(&self, stack: &Stack, fixture: &TenantFixture, key: &str) -> impl Future<Output = Result<Self::Writer, Error>> + Send;

    /// Stops a writer cleanly.
    fn stop(&self, writer: Self::Writer) -> impl Future<Output = Result<(), Error>> + Send;

    /// Whether a writer stays in the epoch it started in, as a server does. A writer through workers joins the
    /// disk anew for each operation, so no epoch outlives it.
    fn writers_keep_their_epoch(&self) -> bool {
        true
    }
}

/// A tenant with the events schema.
pub async fn tenant_with_events(stack: &Stack) -> TenantFixture {
    let fixture = TenantFixture::create(stack).await.expect("a tenant");
    Exclusive::run(&stack.stock_env(), &fixture.tenant, &fixture.key, &EVENTS_SCHEMA).await.expect("the events schema");
    fixture
}

/// Starts a writer under a key minted for it.
async fn start<H: Harness>(stack: &Stack, harness: &H, fixture: &TenantFixture) -> (H::Writer, String) {
    let key = fixture.mint(Grant::Write, Duration::ZERO).await.expect("a key for the writer");
    let writer = harness.start(stack, fixture, &key).await.expect("a writer");
    (writer, key)
}

async fn insert<W: Writer>(writer: &W, table: &TableName, batch: &Batch) -> Outcome<Inserted> {
    writer.insert(Insert { table, format: "JSONEachRow", token: &batch.token, data: batch.data.clone() }).await.expect("the insert runs")
}

async fn insert_committed<W: Writer>(writer: &W, table: &TableName, batch: &Batch) {
    let outcome = insert(writer, table, batch).await;
    assert!(outcome.is_committed(), "batch {} ended {}", batch.token, outcome.name());
}

/// Checks that readers find exactly `batches` in the events table, its projection and the daily aggregate.
async fn assert_events(stack: &Stack, fixture: &TenantFixture, batches: &[Batch]) {
    let rows: u64 = batches.iter().map(|batch| batch.rows).sum();
    let sum: u64 = batches.iter().map(|batch| batch.sum).sum();
    let reader = Reader::open(stack, fixture).await.expect("a reader");
    assert_eq!(reader.number("SELECT count() FROM d.events").await.unwrap(), rows, "rows in d.events");
    assert_eq!(reader.number("SELECT sum(v) FROM d.events").await.unwrap(), sum, "sum of d.events");
    assert_eq!(reader.number("SELECT uniqExact(v) FROM d.events").await.unwrap(), rows, "each row once in d.events");
    let by_projection = "SELECT sum(total) FROM (SELECT kind, sum(v) AS total FROM d.events GROUP BY kind SETTINGS force_optimize_projection = 1)";
    assert_eq!(reader.number(by_projection).await.unwrap(), sum, "sum through the projection");
    assert_eq!(reader.number("SELECT sumMerge(total) FROM d.daily").await.unwrap(), sum, "sum of the view's target");
    reader.close().await.expect("the reader stops");
}

/// Runs the invariant checker, then has a fresh stock exclusive writer load the disk. Every writer of the
/// scenario has stopped by now.
async fn assert_healthy(stack: &Stack, fixture: &TenantFixture) {
    let report = check::tenant(&fixture.tenant, &fixture.key).await.expect("the checker runs");
    assert!(report.is_clean(), "{report}");
    assert!(report.tables > 0 && report.parts > 0);
    let rejected = check::stock_load(&stack.stock_env(), &fixture.tenant, &fixture.key).await.expect("a stock writer loads the disk");
    assert!(rejected.is_empty(), "a stock writer rejects: {rejected:?}");
}

fn names(parts: &[PartInfo]) -> Vec<String> {
    parts.iter().map(PartInfo::name).collect()
}

/// Batches land in the table, in its projection and in the view's target.
pub async fn inserts_reach_the_view_and_the_projection<H: Harness>(stack: &Stack, harness: &H) {
    let fixture = tenant_with_events(stack).await;
    let (writer, _key) = start(stack, harness, &fixture).await;
    let batches: Vec<Batch> = (0..5).map(|index| schema::batch(index, 200)).collect();
    for batch in &batches {
        insert_committed(&writer, &schema::events(), batch).await;
    }
    harness.stop(writer).await.expect("the writer stops");

    assert_events(stack, &fixture, &batches).await;
    let reader = Reader::open(stack, &fixture).await.unwrap();
    let projections = reader.number("SELECT count() FROM system.projection_parts WHERE database = 'd' AND table = 'events' AND active").await.unwrap();
    assert_eq!(projections, 5, "each part carries its projection");
    reader.close().await.unwrap();
    assert_healthy(stack, &fixture).await;
}

/// A batch sent again under its token lands once, on the same writer and on a later one.
pub async fn a_retried_batch_lands_once<H: Harness>(stack: &Stack, harness: &H) {
    let fixture = tenant_with_events(stack).await;
    let events = schema::events();
    let first = schema::batch(0, 150);
    let second = schema::batch(1, 150);

    let (writer, _key) = start(stack, harness, &fixture).await;
    insert_committed(&writer, &events, &first).await;
    assert_eq!(insert(&writer, &events, &first).await, Outcome::Duplicate, "the same writer sees the batch again");
    insert_committed(&writer, &events, &second).await;
    assert_eq!(insert(&writer, &events, &first).await, Outcome::Duplicate);
    harness.stop(writer).await.expect("the writer stops");

    let (later, _key) = start(stack, harness, &fixture).await;
    assert_eq!(insert(&later, &events, &first).await, Outcome::Duplicate, "a later writer sees the batch again");
    assert_eq!(insert(&later, &events, &second).await, Outcome::Duplicate);
    // The token names the batch: the same rows under another token are another batch.
    let renamed = Batch { token: "another-name".into(), ..schema::batch(2, 150) };
    insert_committed(&later, &events, &renamed).await;
    harness.stop(later).await.expect("the later writer stops");

    assert_events(stack, &fixture, &[first, second, renamed]).await;
    assert_healthy(stack, &fixture).await;
}

/// A view that reads a view's target gets each batch once, and a retry adds nothing at any level.
pub async fn a_chain_of_two_views_gets_each_batch_once<H: Harness>(stack: &Stack, harness: &H) {
    let fixture = TenantFixture::create(stack).await.expect("a tenant");
    let mut statements = EVENTS_SCHEMA.to_vec();
    statements.push("CREATE TABLE d.kinds (kind LowCardinality(String), groups UInt64) ENGINE = SummingMergeTree ORDER BY kind SETTINGS non_replicated_deduplication_window = 10000");
    statements.push("CREATE MATERIALIZED VIEW d.kinds_mv TO d.kinds AS SELECT kind, count() AS groups FROM d.daily GROUP BY kind");
    Exclusive::run(&stack.stock_env(), &fixture.tenant, &fixture.key, &statements).await.expect("the schema");

    let (writer, _key) = start(stack, harness, &fixture).await;
    let batches: Vec<Batch> = (0..4).map(|index| schema::batch(index, 120)).collect();
    for batch in &batches {
        insert_committed(&writer, &schema::events(), batch).await;
    }
    assert_eq!(insert(&writer, &schema::events(), &batches[2]).await, Outcome::Duplicate);
    harness.stop(writer).await.expect("the writer stops");

    assert_events(stack, &fixture, &batches).await;
    // Each batch falls in one day and holds three kinds, so it adds three groups to the daily table, and the
    // second view counts one group for each kind.
    let groups = Reader::number_once(stack, &fixture, "SELECT sum(groups) FROM d.kinds").await.unwrap();
    assert_eq!(groups, 3 * batches.len() as u64);
    assert_healthy(stack, &fixture).await;
}

/// Writers that insert into one table at the same time lose nothing, repeat nothing, and number each block
/// once.
pub async fn concurrent_inserts_keep_every_batch<H: Harness>(stack: &Stack, harness: &H) {
    const WRITERS: u64 = 2;
    const STREAMS: u64 = 4;
    const BATCHES: u64 = 8;
    let fixture = tenant_with_events(stack).await;
    let events = schema::events();
    let mut writers = Vec::new();
    for _ in 0..WRITERS {
        writers.push(start(stack, harness, &fixture).await.0);
    }

    let batch_of = |writer: u64, stream: u64, index: u64| schema::batch(writer * 1_000 + stream * 100 + index, 40);
    let mut streams = Vec::new();
    for (writer_index, writer) in writers.iter().enumerate() {
        for stream in 0..STREAMS {
            let events = &events;
            streams.push(async move {
                for index in 0..BATCHES {
                    insert_committed(writer, events, &batch_of(writer_index as u64, stream, index)).await;
                }
            });
        }
    }
    join_all(streams).await;
    for writer in writers {
        harness.stop(writer).await.expect("a writer stops");
    }

    let mut batches = Vec::new();
    for writer in 0..WRITERS {
        for stream in 0..STREAMS {
            batches.extend((0..BATCHES).map(|index| batch_of(writer, stream, index)));
        }
    }
    assert_events(stack, &fixture, &batches).await;

    let catalog = fixture.catalog().await.unwrap();
    let definition = catalog.table(&events).await.unwrap().expect("d.events");
    let parts = catalog.parts(&definition).await.unwrap();
    let mut blocks: Vec<i64> = parts.entries.values().filter(|entry| entry.info.level == 0).map(|entry| entry.info.min_block).collect();
    blocks.sort_unstable();
    let inserts = (WRITERS * STREAMS * BATCHES) as i64;
    assert_eq!(blocks, (1..=inserts).collect::<Vec<i64>>(), "each insert took the next block number");
    assert_eq!(parts.counter.map(|counter| counter.value), Some(inserts));
    assert_healthy(stack, &fixture).await;
}

/// A plan names each partition's parts, a merge leaves one part in their place, and readers find the same rows.
pub async fn merges_leave_one_part_and_the_same_rows<H: Harness>(stack: &Stack, harness: &H) {
    let fixture = tenant_with_events(stack).await;
    let events = schema::events();
    let (writer, _key) = start(stack, harness, &fixture).await;
    let mut batches: Vec<Batch> = (0..6).map(|index| schema::batch(index, 100)).collect();
    batches.extend((6..9).map(|index| schema::batch_at(index, 100, 1)));
    for batch in &batches {
        insert_committed(&writer, &events, batch).await;
    }

    let catalog = fixture.catalog().await.unwrap();
    let definition = catalog.table(&events).await.unwrap().expect("d.events");
    assert_eq!(catalog.parts(&definition).await.unwrap().active().len(), 9);

    let candidates = writer.plan(&events, &Budget::default()).await.expect("a plan");
    let proposed: Vec<(String, usize)> = candidates.iter().map(|candidate| (candidate.partition_id.clone(), candidate.sources.len())).collect();
    assert_eq!(proposed, [("202611".to_string(), 6), ("202612".to_string(), 3)]);
    assert_eq!(writer.plan(&events, &Budget { max_merges: 1 }).await.expect("a plan").len(), 1, "a plan keeps to its budget");
    for candidate in &candidates {
        assert!(candidate.sources.iter().all(|source| source.partition_id == candidate.partition_id));
        let merged = writer.merge(&events, candidate).await.expect("the merge runs").committed().expect("the merge commits");
        assert!(candidate.sources.iter().all(|source| merged.part.contains(source)), "{} holds its sources", merged.part);
    }

    let parts = catalog.parts(&definition).await.unwrap();
    assert_eq!(names(parts.active()), ["202611_1_6_1", "202612_7_9_1"]);
    assert!(parts.set.intersections.is_empty());
    assert!(writer.plan(&events, &Budget::default()).await.expect("a plan").is_empty(), "nothing is left to merge");
    let again = writer.merge(&events, &candidates[0]).await.expect("the merge runs");
    assert!(!again.is_committed(), "sources that a merge replaced merge no further: {}", again.name());

    let housekept = writer.housekeep(&events).await.expect("housekeeping runs").committed().expect("housekeeping reports");
    let covered: Vec<String> = parts.set.covered.iter().map(|covered| covered.part.name()).collect();
    for part in housekept.removed.iter().chain(housekept.remaining.iter().map(|remaining| &remaining.part)) {
        assert!(covered.contains(&part.name()), "housekeeping names only covered parts, and {part} is active");
    }

    // The daily table holds one part for each batch, all in one partition.
    let daily = schema::daily();
    let candidates = writer.plan(&daily, &Budget::default()).await.expect("a plan");
    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].sources.len(), 9);
    assert!(writer.merge(&daily, &candidates[0]).await.expect("the merge runs").is_committed());
    harness.stop(writer).await.expect("the writer stops");

    assert_events(stack, &fixture, &batches).await;
    assert_healthy(stack, &fixture).await;
}

/// An exclusive writer's `ALTER` fences the writers that were running, where writers keep their epoch, and a
/// writer started after it inserts under the new definition.
pub async fn an_exclusive_epoch_fences_running_writers<H: Harness>(stack: &Stack, harness: &H) {
    let fixture = tenant_with_events(stack).await;
    let events = schema::events();
    let before = schema::batch(0, 100);
    let during = schema::batch(1, 100);

    let (writer, _key) = start(stack, harness, &fixture).await;
    insert_committed(&writer, &events, &before).await;

    Exclusive::run(&stack.stock_env(), &fixture.tenant, &fixture.key, &["ALTER TABLE d.events ADD COLUMN source String DEFAULT 'unknown'"])
        .await
        .expect("the ALTER on an exclusive writer");

    if harness.writers_keep_their_epoch() {
        assert_eq!(insert(&writer, &events, &during).await, Outcome::Fenced, "the epoch that began fences the running writer");
        assert_eq!(insert(&writer, &events, &during).await, Outcome::Fenced, "and it stays fenced");
    }
    let _ = harness.stop(writer).await;

    let (later, _key) = start(stack, harness, &fixture).await;
    insert_committed(&later, &events, &during).await;
    let with_source = Batch {
        token: "with-source".into(),
        data: Bytes::from_static(b"{\"ts\":\"2026-11-02 00:00:00\",\"kind\":\"a\",\"v\":1000000,\"source\":\"web\"}\n"),
        rows: 1,
        sum: 1_000_000,
    };
    insert_committed(&later, &events, &with_source).await;
    harness.stop(later).await.expect("the later writer stops");

    assert_events(stack, &fixture, &[before, during, with_source]).await;
    let reader = Reader::open(stack, &fixture).await.unwrap();
    assert_eq!(reader.number("SELECT count() FROM d.events WHERE source = 'web'").await.unwrap(), 1);
    assert_eq!(reader.number("SELECT count() FROM d.events WHERE source = 'unknown'").await.unwrap(), 200);
    reader.close().await.unwrap();
    assert_healthy(stack, &fixture).await;
}

/// Revoking a writer's key fences that writer alone: it commits nothing more, and another writer carries on.
pub async fn a_revoked_key_fences_its_writer<H: Harness>(stack: &Stack, harness: &H) {
    let fixture = tenant_with_events(stack).await;
    let events = schema::events();
    let first = schema::batch(0, 100);
    let second = schema::batch(1, 100);

    let (revoked, revoked_key) = start(stack, harness, &fixture).await;
    let (other, _key) = start(stack, harness, &fixture).await;
    insert_committed(&revoked, &events, &first).await;
    stack.admin().await.unwrap().revoke_key(&revoked_key).await.expect("the key is revoked");

    assert_eq!(insert(&revoked, &events, &second).await, Outcome::Fenced, "a writer whose key is revoked commits nothing");
    insert_committed(&other, &events, &second).await;
    let _ = harness.stop(revoked).await;
    harness.stop(other).await.expect("the other writer stops");

    assert_events(stack, &fixture, &[first, second]).await;
    assert_healthy(stack, &fixture).await;
}

/// The catalog a scenario's tenant ends with, for a test that wants to look further.
pub async fn catalog(fixture: &TenantFixture) -> Catalog {
    fixture.catalog().await.expect("the tenant's catalog")
}
