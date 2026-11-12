//! A tenant with hundreds of tables: an operation pays for the tables it reaches.
//!
//! A worker reads every definition in one listing of each database, and builds a table when the operation first
//! reaches it, with the views that read from it and their targets.

use libch::Writer;
use libch::stock::Exclusive;
use libch::worker::{OpWriter, Usage};
use libch::writer::Budget;
use libch_testkit::ops::{assert_clean, insert, tenant_with_parts};
use libch_testkit::schema::{EVENTS_SCHEMA, batch, events};
use libch_testkit::{TenantFixture, stack};

const MORE_TABLES: usize = 300;

/// A tenant with the events schema and further tables, each made by one of `more`'s statements.
async fn tenant(more: &[String]) -> (TenantFixture, OpWriter) {
    let stack = stack().await;
    let fixture = TenantFixture::create(stack).await.unwrap();
    let writer = Exclusive::start(&stack.stock_env(), &fixture.tenant, &fixture.key).await.unwrap();
    for statement in EVENTS_SCHEMA.iter().copied().chain(more.iter().map(String::as_str)) {
        writer.execute(statement).await.unwrap();
    }
    writer.stop().await.unwrap();
    fixture.share().await.unwrap();
    let writer = OpWriter::new(stack.worker_command(), &fixture.tenant, &fixture.key);
    (fixture, writer)
}

/// The InodeDB calls between two readings of a writer's usage: commits, stats, reads and lists.
fn calls(before: Usage, after: Usage) -> (u64, u64, u64, u64) {
    (
        after.inodedb_commits - before.inodedb_commits,
        after.inodedb_stats - before.inodedb_stats,
        after.inodedb_reads - before.inodedb_reads,
        after.inodedb_lists - before.inodedb_lists,
    )
}

/// What each operation costs a tenant in InodeDB calls, after a first insert has made the tables' coordination
/// files: a describe, an insert, a plan and a housekeeping.
async fn costs(writer: &OpWriter) -> Vec<(u64, u64, u64, u64)> {
    assert!(insert(writer, &batch(0, 100)).await.unwrap().is_committed());
    let mut readings = vec![writer.usage()];
    writer.describe(false).await.unwrap();
    readings.push(writer.usage());
    assert!(insert(writer, &batch(1, 100)).await.unwrap().is_committed());
    readings.push(writer.usage());
    writer.plan(&events(), &Budget::default()).await.unwrap();
    readings.push(writer.usage());
    writer.housekeep(&events()).await.unwrap();
    readings.push(writer.usage());
    readings.windows(2).map(|pair| calls(pair[0], pair[1])).collect()
}

#[tokio::test]
async fn three_hundred_more_tables_cost_an_operation_no_call() {
    let more: Vec<String> = (0..MORE_TABLES)
        .map(|table| {
            format!("CREATE TABLE d.more_{table} (k UInt64, v String) ENGINE = MergeTree ORDER BY k SETTINGS non_replicated_deduplication_window = 10000")
        })
        .collect();
    let (narrow_fixture, narrow) = tenant(&[]).await;
    let (wide_fixture, wide) = tenant(&more).await;

    let (narrow_costs, wide_costs) = (costs(&narrow).await, costs(&wide).await);
    assert_eq!(wide_costs, narrow_costs, "a describe, an insert, a plan and a housekeeping make the same calls on both tenants");
    let (narrow_usage, wide_usage) = (narrow.usage(), wide.usage());
    assert!(wide_usage.peak_threads <= narrow_usage.peak_threads + 1, "{wide_usage:?} against {narrow_usage:?}");

    // The worker's engine does see every table when asked.
    assert_eq!(wide.describe(false).await.unwrap().tables.len(), MORE_TABLES + 3);
    assert_clean(&narrow_fixture).await;
    assert_clean(&wide_fixture).await;
}

/// A table the worker cannot build fails the operations that reach it, and no other: here a `URL` table, an
/// engine the worker leaves out. An insert into the events table commits, and a describe, which reaches every
/// table, fails with ClickHouse's unknown-engine error.
#[tokio::test]
async fn a_table_the_worker_cannot_build_fails_only_what_reaches_it() {
    let (fixture, writer) = tenant(&["CREATE TABLE d.outside (x UInt8) ENGINE = URL('http://127.0.0.1:9/x', 'CSV')".to_string()]).await;
    assert!(insert(&writer, &batch(0, 100)).await.unwrap().is_committed());
    writer.plan(&events(), &Budget::default()).await.unwrap();
    assert!(writer.housekeep(&events()).await.unwrap().is_committed());

    let described = writer.describe(false).await.unwrap_err();
    assert!(described.to_string().contains("Unknown table engine URL"), "{described}");
    assert_clean(&fixture).await;
}

/// A view that reads through a table the worker cannot build fails the insert that reaches it, before anything
/// commits.
#[tokio::test]
async fn a_view_over_a_table_the_worker_cannot_build_fails_its_inserts() {
    let more = [
        "CREATE TABLE d.outside (kind String, weight UInt64) ENGINE = URL('http://127.0.0.1:9/x', 'CSV')".to_string(),
        "CREATE TABLE d.weighted (kind LowCardinality(String), total UInt64) ENGINE = SummingMergeTree ORDER BY kind SETTINGS non_replicated_deduplication_window = 10000".to_string(),
        "CREATE MATERIALIZED VIEW d.weighted_mv TO d.weighted AS SELECT kind, v * weight AS total FROM d.events AS e INNER JOIN d.outside AS o ON e.kind = o.kind".to_string(),
    ];
    let (fixture, writer) = tenant(&more).await;
    let error = insert(&writer, &batch(0, 100)).await.unwrap_err();
    assert!(error.to_string().contains("Unknown table engine URL"), "{error}");
    let (_other, healthy) = tenant_with_parts(0).await;
    assert!(insert(&healthy, &batch(0, 100)).await.unwrap().is_committed());

    let catalog = fixture.catalog().await.unwrap();
    for table in catalog.all_tables().await.unwrap().iter().filter(|table| table.is_merge_tree()) {
        assert!(catalog.parts(table).await.unwrap().entries.is_empty(), "{} holds no part", table.name);
    }
    assert_clean(&fixture).await;
}
