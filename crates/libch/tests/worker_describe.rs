//! The worker's `describe`, against the fork's binary: the tenant's catalog as an engine in operation mode
//! attaches it.

use std::collections::BTreeMap;

use libch::catalog::{Catalog, TableKind};
use libch::stock::{Exclusive, StockWriter, StockWriterOptions};
use libch::worker::{Described, DescribedTable, OpWriter, Usage, ViewEdge};
use libch::writer::{Budget, Insert, TableName};
use libch::{Error, Outcome, PartInfo, Writer};
use libch_testkit::schema::{EVENTS_SCHEMA, batch, batch_at, daily, events};
use libch_testkit::{TenantFixture, stack};

/// A tenant with the events schema. A stock shared writer has inserted `batches` batches into it, and merged
/// November's parts of `d.events` when there are several.
async fn tenant_with_events(batches: u64) -> TenantFixture {
    let stack = stack().await;
    let env = stack.stock_env();
    let fixture = TenantFixture::create(stack).await.unwrap();
    Exclusive::run(&env, &fixture.tenant, &fixture.key, &EVENTS_SCHEMA).await.unwrap();
    let writer = StockWriter::start(&env, &fixture.tenant, &fixture.key, &StockWriterOptions::default()).await.unwrap();
    for index in 0..batches {
        let batch = if index % 4 == 3 { batch_at(index, 10, 1) } else { batch(index, 10) };
        let outcome = writer.insert(Insert { table: &events(), format: "JSONEachRow", token: &batch.token, data: batch.data }).await.unwrap();
        assert!(matches!(outcome, Outcome::Committed(_)));
    }
    if batches > 2 {
        let november = writer.plan(&events(), &Budget::default()).await.unwrap().into_iter().find(|candidate| candidate.partition_id == "202611").unwrap();
        writer.merge(&events(), &november).await.unwrap().committed().expect("the merge commits");
    }
    writer.stop().await.unwrap();
    fixture
}

async fn worker(fixture: &TenantFixture) -> OpWriter {
    OpWriter::new(stack().await.worker_command(), &fixture.tenant, &fixture.key)
}

fn table<'a>(described: &'a Described, name: &TableName) -> &'a DescribedTable {
    described.tables.iter().find(|table| table.name == *name).unwrap_or_else(|| panic!("{name:?} is described"))
}

#[tokio::test]
async fn a_description_agrees_with_the_catalog() {
    let fixture = tenant_with_events(6).await;
    let described = worker(&fixture).await.describe(false).await.unwrap();

    let catalog = fixture.catalog().await.unwrap();
    let stored = catalog.all_tables().await.unwrap();
    let names = |tables: Vec<&TableName>| tables.into_iter().map(|name| format!("{}.{}", name.database, name.table)).collect::<Vec<_>>();
    assert_eq!(names(described.tables.iter().map(|table| &table.name).collect()), ["d.daily", "d.daily_mv", "d.events"]);
    assert_eq!(names(stored.iter().map(|table| &table.name).collect()), ["d.daily", "d.daily_mv", "d.events"]);
    for stored in &stored {
        let table = table(&described, &stored.name);
        assert_eq!((table.uuid.as_str(), table.kind), (stored.uuid.as_str(), stored.kind), "{:?}", stored.name);
        assert!(table.parts.is_empty(), "parts come only when asked for");
    }

    let daily_mv = TableName::new("d", "daily_mv");
    assert_eq!(table(&described, &events()).engine.as_deref(), Some("MergeTree"));
    assert_eq!(table(&described, &daily()).engine.as_deref(), Some("AggregatingMergeTree"));
    assert_eq!((table(&described, &daily_mv).kind, table(&described, &daily_mv).engine.clone()), (TableKind::MaterializedView, None));
    assert_eq!(table(&described, &events()).views, [ViewEdge { view: daily_mv.clone(), target: daily() }], "the view reads from the events");
    assert!(table(&described, &daily()).views.is_empty() && table(&described, &daily_mv).views.is_empty());
}

/// Each MergeTree table's parts by name, with whether each is active.
async fn stored_parts(catalog: &Catalog) -> BTreeMap<String, BTreeMap<String, bool>> {
    let mut tables = BTreeMap::new();
    for table in catalog.all_tables().await.unwrap().into_iter().filter(|table| table.is_merge_tree()) {
        let parts = catalog.parts(&table).await.unwrap();
        let active: Vec<String> = parts.active().iter().map(PartInfo::name).collect();
        tables.insert(table.name.table.clone(), parts.entries.keys().map(|name| (name.clone(), active.contains(name))).collect());
    }
    tables
}

#[tokio::test]
async fn a_description_names_parts_as_the_engine_parses_them() {
    let fixture = tenant_with_events(6).await;
    let described = worker(&fixture).await.describe(true).await.unwrap();

    let mut by_table = BTreeMap::new();
    for table in described.tables.iter().filter(|table| table.kind == TableKind::Table) {
        for part in &table.parts {
            let parsed = PartInfo::parse(&part.name).unwrap_or_else(|| panic!("`{}` is a part name", part.name));
            assert_eq!(
                (parsed.partition_id.as_str(), parsed.min_block, parsed.max_block, parsed.level, parsed.mutation),
                (part.partition_id.as_str(), part.min_block, part.max_block, part.level, part.mutation),
                "the crate and the engine read `{}` alike",
                part.name
            );
        }
        by_table.insert(table.name.table.clone(), table.parts.iter().map(|part| (part.name.clone(), part.active)).collect::<BTreeMap<_, _>>());
    }
    assert_eq!(by_table, stored_parts(&fixture.catalog().await.unwrap()).await);

    // Five of the six batches fell in November and were merged: one active part covers five.
    let november: Vec<(&String, &bool)> = by_table["events"].iter().filter(|(name, _)| name.starts_with("202611_")).collect();
    assert_eq!((november.len(), november.iter().filter(|(_, active)| **active).count()), (6, 1), "{november:?}");
    assert!(table(&described, &TableName::new("d", "daily_mv")).parts.is_empty(), "a view holds no parts");
}

async fn usage_of_a_description(fixture: &TenantFixture) -> Usage {
    let worker = worker(fixture).await;
    worker.describe(false).await.unwrap();
    worker.usage()
}

#[tokio::test]
async fn attaching_a_tenant_costs_the_same_whatever_its_parts() {
    let small = tenant_with_events(2).await;
    let large = tenant_with_events(120).await;
    let (few, many) = (usage_of_a_description(&small).await, usage_of_a_description(&large).await);
    let calls = |usage: &Usage| (usage.inodedb_commits, usage.inodedb_stats, usage.inodedb_reads, usage.inodedb_lists);
    assert_eq!(calls(&few), calls(&many), "a table in operation mode reads nothing under its directory");
    assert_eq!((few.s3_reads + few.s3_writes, many.s3_reads + many.s3_writes), (0, 0), "an attach reads no blob");

    // The disk is in a shared epoch already, so attaching it commits nothing.
    assert_eq!(many.inodedb_commits, 0, "{many:?}");
    assert!(many.peak_threads > 0 && many.peak_anonymous_memory_bytes > 0 && many.cpu.as_micros() > 0, "{many:?}");
}

#[tokio::test]
async fn describing_leaves_the_disk_to_other_writers() {
    let stack = stack().await;
    let fixture = tenant_with_events(3).await;
    let before = stored_parts(&fixture.catalog().await.unwrap()).await;
    worker(&fixture).await.describe(true).await.unwrap();
    assert_eq!(stored_parts(&fixture.catalog().await.unwrap()).await, before);

    let report = libch::check::tenant(&fixture.tenant, &fixture.key).await.unwrap();
    assert!(report.violations.is_empty(), "{:?}", report.violations);

    // A stock shared writer inserts beside workers, before and after.
    let writer = StockWriter::start(&stack.stock_env(), &fixture.tenant, &fixture.key, &StockWriterOptions::default()).await.unwrap();
    let batch = batch(100, 10);
    let outcome = writer.insert(Insert { table: &events(), format: "JSONEachRow", token: &batch.token, data: batch.data }).await.unwrap();
    assert!(matches!(outcome, Outcome::Committed(_)));
    worker(&fixture).await.describe(true).await.unwrap();
    writer.stop().await.unwrap();
}

#[tokio::test]
async fn a_worker_joins_the_shared_writers_of_a_disk_an_exclusive_writer_left() {
    let stack = stack().await;
    let fixture = TenantFixture::create(stack).await.unwrap();
    Exclusive::run(&stack.stock_env(), &fixture.tenant, &fixture.key, &EVENTS_SCHEMA).await.unwrap();
    let catalog = fixture.catalog().await.unwrap();
    assert_eq!(catalog.owner().await.unwrap().map(|owner| owner.mode), Some(libch::catalog::OwnerMode::Exclusive));

    let worker = worker(&fixture).await;
    assert_eq!(worker.describe(false).await.unwrap().tables.len(), 3);
    assert_eq!(catalog.owner().await.unwrap().map(|owner| owner.mode), Some(libch::catalog::OwnerMode::Shared));
    assert_eq!(worker.usage().inodedb_commits, 1, "the worker began the shared epoch, and wrote nothing else");
}

#[tokio::test]
async fn a_tenant_without_databases_describes_as_empty() {
    let fixture = TenantFixture::create(stack().await).await.unwrap();
    assert!(worker(&fixture).await.describe(true).await.unwrap().tables.is_empty());
}

#[tokio::test]
async fn external_engines_are_unknown_to_the_worker() {
    let stack = stack().await;
    for (engine, definition) in [
        ("URL", "CREATE TABLE d.outside (k UInt64) ENGINE = URL('http://127.0.0.1:1/rows', 'CSV')"),
        ("File", "CREATE TABLE d.outside (k UInt64) ENGINE = File('CSV')"),
        ("Memory", "CREATE TABLE d.outside (k UInt64) ENGINE = Memory"),
    ] {
        let fixture = TenantFixture::create(stack).await.unwrap();
        Exclusive::run(&stack.stock_env(), &fixture.tenant, &fixture.key, &["CREATE DATABASE d", definition]).await.unwrap();
        // The loader wraps the engine's error in its own, so the engine's shows in the message.
        let error = worker(&fixture).await.describe(false).await.unwrap_err().to_string();
        assert!(error.contains(&format!("Unknown table engine {engine}")) && error.contains("(UNKNOWN_STORAGE)"), "{definition}: {error}");
    }
}

#[tokio::test]
async fn a_refused_key_ends_as_an_error_that_names_it() {
    let fixture = tenant_with_events(1).await;
    let worker = OpWriter::new(stack().await.worker_command(), &fixture.tenant, "0000000000000000.bm90LWEta2V5");
    let error = worker.describe(false).await.unwrap_err();
    assert!(matches!(&error, Error::Worker { message } if message.contains("fenced")), "{error}");
}
