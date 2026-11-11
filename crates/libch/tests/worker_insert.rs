//! The worker's `insert`, against the fork's binary. The conformance scenarios cover what every writer does;
//! these cover what is the worker's own: its report, its cost, and the tables it attaches without their parts.

use bytes::Bytes;
use libch::stock::{Exclusive, StockWriter, StockWriterOptions};
use libch::worker::{OpWriter, Usage};
use libch::writer::{Insert, Inserted, TableName};
use libch::{Error, Outcome, PartInfo, Writer, check};
use libch_testkit::schema::{Batch, EVENTS_SCHEMA, batch, daily, events};
use libch_testkit::{Reader, TenantFixture, stack};

async fn tenant(statements: &[&str]) -> TenantFixture {
    let stack = stack().await;
    let fixture = TenantFixture::create(stack).await.unwrap();
    Exclusive::run(&stack.stock_env(), &fixture.tenant, &fixture.key, statements).await.unwrap();
    fixture
}

async fn worker(fixture: &TenantFixture) -> OpWriter {
    OpWriter::new(stack().await.worker_command(), &fixture.tenant, &fixture.key)
}

async fn insert(writer: &OpWriter, table: &TableName, batch: &Batch) -> Result<Outcome<Inserted>, Error> {
    writer.insert(Insert { table, format: "JSONEachRow", token: &batch.token, data: batch.data.clone() }).await
}

fn rows(token: &str, keys: std::ops::Range<u64>) -> Batch {
    let data: String = keys.clone().map(|k| format!("{{\"k\":{k}}}\n")).collect();
    Batch { token: token.into(), data: Bytes::from(data), rows: keys.end - keys.start, sum: keys.sum() }
}

async fn assert_clean(fixture: &TenantFixture) {
    let report = check::tenant(&fixture.tenant, &fixture.key).await.unwrap();
    assert!(report.is_clean(), "{report}");
    let rejected = check::stock_load(&stack().await.stock_env(), &fixture.tenant, &fixture.key).await.unwrap();
    assert!(rejected.is_empty(), "a stock writer rejects: {rejected:?}");
}

#[tokio::test]
async fn an_insert_reports_the_parts_it_committed() {
    let fixture = tenant(&EVENTS_SCHEMA).await;
    let writer = worker(&fixture).await;

    let first = insert(&writer, &events(), &batch(0, 1_000)).await.unwrap().committed().expect("the insert commits");
    let parts = first.parts.expect("a worker names its parts");
    let named: Vec<(String, String, u64)> = parts.iter().map(|part| (part.table.table.clone(), part.part.name(), part.rows)).collect();
    // The view groups a thousand events of one day by three kinds.
    // An aggregating table merges a block as it writes it, so its new part is at level one, as on a server.
    assert_eq!(named, [("events".to_string(), "202611_1_1_0".to_string(), 1_000), ("daily".to_string(), "all_1_1_1".to_string(), 3)]);
    assert!(parts.iter().all(|part| part.bytes > 0));
    assert_eq!(first.counter_conflicts, 0);

    let second = insert(&writer, &events(), &batch(1, 10)).await.unwrap().committed().unwrap().parts.unwrap();
    assert_eq!(second.iter().map(|part| part.part.name()).collect::<Vec<_>>(), ["202611_2_2_0", "all_2_2_1"], "the next insert takes the next numbers");

    let catalog = fixture.catalog().await.unwrap();
    for table in [events(), daily()] {
        let definition = catalog.table(&table).await.unwrap().unwrap();
        let stored = catalog.parts(&definition).await.unwrap();
        assert_eq!(stored.active().len(), 2, "{table:?}");
        assert_eq!(stored.counter.map(|counter| counter.value), Some(2), "each table's counter follows its own parts");
    }
    assert_clean(&fixture).await;
}

#[tokio::test]
async fn a_batch_sent_again_ends_as_a_duplicate_and_writes_nothing() {
    let fixture = tenant(&EVENTS_SCHEMA).await;
    let writer = worker(&fixture).await;
    let batch = batch(0, 50);
    assert!(insert(&writer, &events(), &batch).await.unwrap().is_committed());
    let before = fixture.catalog().await.unwrap().parts(&fixture.catalog().await.unwrap().table(&events()).await.unwrap().unwrap()).await.unwrap();

    assert_eq!(insert(&writer, &events(), &batch).await.unwrap(), Outcome::Duplicate);
    assert_eq!(insert(&worker(&fixture).await, &events(), &batch).await.unwrap(), Outcome::Duplicate, "on any worker");

    let catalog = fixture.catalog().await.unwrap();
    let after = catalog.parts(&catalog.table(&events()).await.unwrap().unwrap()).await.unwrap();
    assert_eq!(after.entries.keys().collect::<Vec<_>>(), before.entries.keys().collect::<Vec<_>>());
    assert_eq!(after.other.iter().filter(|name| name.starts_with("tmp")).count(), 0, "a duplicate leaves no temporary directory");
    assert_eq!(Reader::number_once(stack().await, &fixture, "SELECT count() FROM d.events").await.unwrap(), 50);
    assert_clean(&fixture).await;
}

async fn usage_of_an_insert(fixture: &TenantFixture, index: u64) -> Usage {
    let writer = worker(fixture).await;
    assert!(insert(&writer, &events(), &batch(index, 1_000)).await.unwrap().is_committed());
    writer.usage()
}

#[tokio::test]
async fn an_insert_costs_the_same_whatever_the_parts() {
    let stack = stack().await;
    let small = tenant(&EVENTS_SCHEMA).await;
    let large = tenant(&EVENTS_SCHEMA).await;
    // A stock shared writer fills one tenant with parts, and gives both a counter of the shared epoch.
    for (fixture, batches) in [(&small, 1), (&large, 150)] {
        let stock = StockWriter::start(&stack.stock_env(), &fixture.tenant, &fixture.key, &StockWriterOptions::default()).await.unwrap();
        for index in 0..batches {
            let batch = batch(index, 5);
            assert!(stock.insert(Insert { table: &events(), format: "JSONEachRow", token: &batch.token, data: batch.data }).await.unwrap().is_committed());
        }
        stock.stop().await.unwrap();
    }

    let (few, many) = (usage_of_an_insert(&small, 1_000).await, usage_of_an_insert(&large, 1_000).await);
    let calls = |usage: &Usage| (usage.inodedb_commits, usage.inodedb_stats, usage.inodedb_reads, usage.inodedb_lists);
    assert_eq!(calls(&few), calls(&many), "an insert reads nothing that grows with the table's parts: {few:?} and {many:?}");
    assert_eq!(many.inodedb_commits, 2, "one commit for the table and one for the view's target");
    assert_eq!(many.s3_reads + many.s3_writes, 0, "a thousand rows fit in the part's inline files");
    println!("an insert of 1,000 rows through a projection and a view: {many:?}");
}

#[tokio::test]
async fn a_table_without_tokens_is_refused_unless_the_writer_waives_exactly_once() {
    let fixture = tenant(&["CREATE DATABASE d", "CREATE TABLE d.plain (k UInt64) ENGINE = MergeTree ORDER BY k"]).await;
    let table = TableName::new("d", "plain");
    let batch = rows("only", 0..10);

    let refused = insert(&worker(&fixture).await, &table, &batch).await.unwrap();
    assert!(matches!(&refused, Outcome::Refused(reason) if reason.contains("d.plain") && reason.contains("keeps no deduplication tokens")), "{refused:?}");
    assert_eq!(Reader::number_once(stack().await, &fixture, "SELECT count() FROM d.plain").await.unwrap(), 0, "a refusal writes nothing");

    let waiving = worker(&fixture).await.at_least_once();
    assert!(insert(&waiving, &table, &batch).await.unwrap().is_committed());
    assert!(insert(&waiving, &table, &batch).await.unwrap().is_committed(), "without tokens a second insert lands again");
    assert_eq!(Reader::number_once(stack().await, &fixture, "SELECT count() FROM d.plain").await.unwrap(), 20);
    assert_clean(&fixture).await;
}

#[tokio::test]
async fn a_view_target_without_tokens_is_refused_too() {
    let fixture = tenant(&[
        "CREATE DATABASE d",
        "CREATE TABLE d.source (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS non_replicated_deduplication_window = 100",
        "CREATE TABLE d.target (k UInt64) ENGINE = MergeTree ORDER BY k",
        "CREATE MATERIALIZED VIEW d.copy TO d.target AS SELECT k FROM d.source",
    ])
    .await;
    let refused = insert(&worker(&fixture).await, &TableName::new("d", "source"), &rows("only", 0..10)).await.unwrap();
    assert!(matches!(&refused, Outcome::Refused(reason) if reason.contains("d.target")), "{refused:?}");
    assert_eq!(Reader::number_once(stack().await, &fixture, "SELECT count() FROM d.source").await.unwrap(), 0);
}

#[tokio::test]
async fn a_view_that_reads_stored_rows_is_refused() {
    let dimension = "CREATE TABLE d.dimension (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS non_replicated_deduplication_window = 100";
    let target = "CREATE TABLE d.known (k UInt64, known UInt8) ENGINE = MergeTree ORDER BY k SETTINGS non_replicated_deduplication_window = 100";
    for (reads, view) in [
        ("d.dimension", "CREATE MATERIALIZED VIEW d.known_mv TO d.known AS SELECT k, k IN (SELECT k FROM d.dimension) AS known FROM d.keyed"),
        ("d.keyed", "CREATE MATERIALIZED VIEW d.known_mv TO d.known AS SELECT k, k IN (SELECT k FROM d.keyed) AS known FROM d.keyed"),
    ] {
        let fixture = tenant(&["CREATE DATABASE d", KEYED, dimension, target, "INSERT INTO d.dimension VALUES (1)", view]).await;
        let table = TableName::new("d", "keyed");
        let refused = insert(&worker(&fixture).await, &table, &rows("only", 0..10)).await.unwrap();
        assert!(matches!(&refused, Outcome::Refused(reason) if reason.contains("d.known_mv") && reason.contains(&format!("reads {reads}"))), "{refused:?}");
        // The refusal comes before the table's own part, so the table and the view's target stay in step.
        assert_eq!(Reader::number_once(stack().await, &fixture, "SELECT count() FROM d.keyed").await.unwrap(), 0);

        // A table no view reads stored rows of takes inserts as ever.
        assert!(insert(&worker(&fixture).await, &TableName::new("d", "dimension"), &rows("more", 5..6)).await.unwrap().is_committed());
    }
}

#[tokio::test]
async fn a_view_that_names_a_table_function_fails_the_insert() {
    let target = "CREATE TABLE d.known (k UInt64, known UInt8) ENGINE = MergeTree ORDER BY k SETTINGS non_replicated_deduplication_window = 100";
    for (function, source) in [("numbers", "numbers(5)"), ("url", "url('http://127.0.0.1:1/rows', 'CSV', 'number UInt64')")] {
        let view = format!("CREATE MATERIALIZED VIEW d.known_mv TO d.known AS SELECT k, k IN (SELECT number FROM {source}) AS known FROM d.keyed");
        let fixture = tenant(&["CREATE DATABASE d", KEYED, target, &view]).await;
        let table = TableName::new("d", "keyed");
        // The worker registers no table function, so a view that names one reaches nothing outside the tenant.
        // Such a view would fail as it runs, after the table's own part is in, so the worker refuses the insert.
        let refused = insert(&worker(&fixture).await, &table, &rows("only", 0..10)).await.unwrap();
        assert!(
            matches!(&refused, Outcome::Refused(reason) if reason.contains("d.known_mv") && reason.contains(&format!("table function `{function}`"))),
            "{refused:?}"
        );
        assert_eq!(parts_and_counter(&fixture, &table).await.0, Vec::<String>::new(), "the insert wrote no part");
    }
}

/// A server holds an insert back, or refuses it, by what the table's parts add up to: `parts_to_delay_insert`,
/// `parts_to_throw_insert`, `max_parts_in_total`, the two `inactive_parts_to_*_insert`,
/// `max_avg_part_size_for_too_many_parts` and the two `dead_blobs_to_*_insert`. A worker loads no part, so none of
/// them fires, and the caller reads the counts from the catalog and holds a tenant back itself. The limits on
/// the batch itself stay: here the partitions one batch may write.
#[tokio::test]
async fn limits_on_a_tables_parts_move_to_the_caller_and_limits_on_the_batch_stay() {
    let limited = "CREATE TABLE d.limited (k UInt64) ENGINE = MergeTree PARTITION BY k % 10 ORDER BY k \
                   SETTINGS parts_to_delay_insert = 1, parts_to_throw_insert = 2, max_parts_in_total = 3, non_replicated_deduplication_window = 100";
    let fixture = tenant(&["CREATE DATABASE d", limited]).await;
    let table = TableName::new("d", "limited");
    let writer = worker(&fixture)
        .await
        .with_settings_xml("<profiles><default><max_partitions_per_insert_block>3</max_partitions_per_insert_block></default></profiles>");

    // Five parts in one partition, where a server refuses the third insert.
    for index in 0..5 {
        let batch = Batch { token: format!("tens-{index}"), data: Bytes::from(format!("{{\"k\":{}}}\n", index * 10)), rows: 1, sum: 0 };
        assert!(insert(&writer, &table, &batch).await.unwrap().is_committed(), "insert {index}");
    }
    let catalog = fixture.catalog().await.unwrap();
    let crowded = catalog.crowded_tables(2).await.unwrap();
    assert_eq!(crowded.len(), 1, "{crowded:?}");
    assert_eq!((crowded[0].table.name.clone(), crowded[0].partitions[0].partition_id.as_str(), crowded[0].partitions[0].active), (table.clone(), "0", 5));

    // A batch across four partitions passes the cap of three, fails as it does on a server, and writes nothing.
    let wide = insert(&writer, &table, &rows("wide", 1..5)).await.unwrap_err();
    assert_eq!(wide.clickhouse_code(), Some(252), "{wide}");
    assert_eq!(parts_and_counter(&fixture, &table).await.0.len(), 5);
    assert!(insert(&writer, &table, &rows("narrow", 1..4)).await.unwrap().is_committed(), "three partitions are within the cap");
    assert_clean(&fixture).await;
}

/// A view that reads a dictionary would reach whatever the dictionary's source names: here an HTTP address. The
/// worker loads no dictionary, and refuses the insert before the table's own part is in.
#[tokio::test]
async fn a_view_that_reads_a_dictionary_fails_the_insert() {
    let dictionary = "CREATE DICTIONARY d.names (k UInt64, name String) PRIMARY KEY k SOURCE(HTTP(url 'http://127.0.0.1:1/names' format 'CSV')) \
                      LAYOUT(FLAT()) LIFETIME(0)";
    let target = "CREATE TABLE d.named (k UInt64, name String) ENGINE = MergeTree ORDER BY k SETTINGS non_replicated_deduplication_window = 100";
    let view = "CREATE MATERIALIZED VIEW d.named_mv TO d.named AS SELECT k, dictGetString('d.names', 'name', k) AS name FROM d.keyed";
    let fixture = tenant(&["CREATE DATABASE d", KEYED, dictionary, target, view]).await;
    let table = TableName::new("d", "keyed");
    let refused = insert(&worker(&fixture).await, &table, &rows("only", 0..10)).await.unwrap();
    assert!(matches!(&refused, Outcome::Refused(reason) if reason.contains("d.named_mv") && reason.contains("`dictGetString`")), "{refused:?}");
    assert_eq!(parts_and_counter(&fixture, &table).await.0, Vec::<String>::new(), "the insert wrote no part");
}

const KEYED: &str = "CREATE TABLE d.keyed (k UInt64, v UInt64 DEFAULT 0) ENGINE = MergeTree ORDER BY k SETTINGS non_replicated_deduplication_window = 100";

/// The names of a table's part directories and its counter's value.
async fn parts_and_counter(fixture: &TenantFixture, table: &TableName) -> (Vec<String>, Option<i64>) {
    let catalog = fixture.catalog().await.unwrap();
    let definition = catalog.table(table).await.unwrap().unwrap();
    let parts = catalog.parts(&definition).await.unwrap();
    (parts.active().iter().map(PartInfo::name).collect(), parts.counter.map(|counter| counter.value))
}

#[tokio::test]
async fn an_insert_numbers_its_part_above_what_an_exclusive_writer_left() {
    let stack = stack().await;
    let table = TableName::new("d", "keyed");
    // An exclusive writer numbers its parts itself and keeps no counter.
    let fixture = tenant(&["CREATE DATABASE d", KEYED, "INSERT INTO d.keyed (k) VALUES (1)", "INSERT INTO d.keyed (k) VALUES (2)"]).await;
    assert_eq!(parts_and_counter(&fixture, &table).await, (vec!["all_1_1_0".to_string(), "all_2_2_0".to_string()], None));

    let writer = worker(&fixture).await;
    let inserted = insert(&writer, &table, &rows("three", 3..4)).await.unwrap().committed().unwrap().parts.unwrap();
    assert_eq!(inserted[0].part.name(), "all_3_3_0", "the part takes the number after the exclusive writer's last");
    assert_eq!(parts_and_counter(&fixture, &table).await.1, Some(3));

    // The exclusive writer returns, inserts, merges and mutates. Its mutation takes a block number too.
    Exclusive::run(
        &stack.stock_env(),
        &fixture.tenant,
        &fixture.key,
        &["INSERT INTO d.keyed (k) VALUES (4)", "OPTIMIZE TABLE d.keyed FINAL", "ALTER TABLE d.keyed UPDATE v = k WHERE 1 SETTINGS mutations_sync = 2"],
    )
    .await
    .unwrap();
    let (parts, counter) = parts_and_counter(&fixture, &table).await;
    assert_eq!((parts, counter), (vec!["all_1_4_1_5".to_string()], Some(3)), "the counter trails the exclusive writer's numbers");

    let inserted = insert(&writer, &table, &rows("six", 6..7)).await.unwrap().committed().unwrap().parts.unwrap();
    assert_eq!(inserted[0].part.name(), "all_6_6_0", "above the merged part's blocks and the mutation's version");
    let inserted = insert(&writer, &table, &rows("seven", 7..8)).await.unwrap().committed().unwrap().parts.unwrap();
    assert_eq!(inserted[0].part.name(), "all_7_7_0", "the counter is the epoch's own from here on");

    let reader = Reader::open(stack, &fixture).await.unwrap();
    assert_eq!(reader.value("SELECT groupArray(k) FROM (SELECT k FROM d.keyed ORDER BY k)").await.unwrap().trim(), "[1,2,3,4,6,7]");
    reader.close().await.unwrap();
    assert_clean(&fixture).await;
}

#[tokio::test]
async fn an_insert_that_cannot_run_fails_with_the_engines_exception() {
    const UNKNOWN_TABLE: i32 = 60;
    const UNKNOWN_FORMAT: i32 = 73;
    let fixture = tenant(&["CREATE DATABASE d", KEYED]).await;
    let writer = worker(&fixture).await;

    let absent = insert(&writer, &TableName::new("d", "absent"), &rows("only", 0..1)).await.unwrap_err();
    assert_eq!(absent.clickhouse_code(), Some(UNKNOWN_TABLE), "{absent}");

    let table = TableName::new("d", "keyed");
    let unknown = writer.insert(Insert { table: &table, format: "NoSuchFormat", token: "only", data: Bytes::from_static(b"1\n") }).await.unwrap_err();
    assert_eq!(unknown.clickhouse_code(), Some(UNKNOWN_FORMAT), "{unknown}");

    let malformed =
        writer.insert(Insert { table: &table, format: "JSONEachRow", token: "only", data: Bytes::from_static(b"{\"k\": \"not a number\"}\n") }).await;
    assert!(malformed.unwrap_err().clickhouse_code().is_some());
    assert_eq!(Reader::number_once(stack().await, &fixture, "SELECT count() FROM d.keyed").await.unwrap(), 0, "a failed insert writes nothing");
}

#[tokio::test]
async fn a_batch_of_no_rows_commits_nothing() {
    let fixture = tenant(&["CREATE DATABASE d", KEYED]).await;
    let table = TableName::new("d", "keyed");
    let inserted = worker(&fixture).await.insert(Insert { table: &table, format: "JSONEachRow", token: "empty", data: Bytes::new() }).await.unwrap();
    assert_eq!(inserted.committed().unwrap().parts.unwrap().len(), 0);
}

#[tokio::test]
async fn a_large_batch_arrives_whole() {
    let fixture = tenant(&["CREATE DATABASE d", KEYED]).await;
    let table = TableName::new("d", "keyed");
    // About four megabytes, so the batch spans several frames and the part's files leave the inline limit.
    let batch = rows("large", 0..400_000);
    assert!(batch.data.len() > 3 << 20);
    let writer = worker(&fixture).await;
    let parts = insert(&writer, &table, &batch).await.unwrap().committed().unwrap().parts.unwrap();
    assert_eq!(parts.iter().map(|part| part.rows).sum::<u64>(), 400_000);
    assert!(writer.usage().s3_writes > 0, "a part this size keeps its files in S3");

    let reader = Reader::open(stack().await, &fixture).await.unwrap();
    assert_eq!(reader.number("SELECT count() FROM d.keyed").await.unwrap(), 400_000);
    assert_eq!(reader.number("SELECT sum(k) FROM d.keyed").await.unwrap(), batch.sum);
    reader.close().await.unwrap();
    assert_clean(&fixture).await;
}

#[tokio::test]
async fn an_insert_over_its_memory_cap_fails_and_leaves_nothing() {
    let wide = "CREATE TABLE d.wide (k UInt64, text String) ENGINE = MergeTree ORDER BY k SETTINGS non_replicated_deduplication_window = 100";
    let fixture = tenant(&["CREATE DATABASE d", wide]).await;
    let table = TableName::new("d", "wide");
    let texts = |token: &str, keys: std::ops::Range<u64>, width: usize| {
        let text = "x".repeat(width);
        let data: String = keys.clone().map(|k| format!("{{\"k\":{k},\"text\":\"{text}\"}}\n")).collect();
        Batch { token: token.into(), data: Bytes::from(data), rows: keys.end - keys.start, sum: 0 }
    };
    let capped = worker(&fixture).await.with_memory_limit(MEMORY_CAP);

    // A small batch fits under the cap.
    assert!(insert(&capped, &table, &texts("small", 0..1_000, 100)).await.unwrap().is_committed());
    let before = parts_and_counter(&fixture, &table).await;

    // A block gathers rows up to 256 MB before it is written, and 160 MB of text passes the cap on the way. The
    // worker names the limit, and the table gains nothing.
    let large = texts("large", 1_000..21_000, 8_000);
    let failed = insert(&capped, &table, &large).await.unwrap_err();
    assert_eq!(failed.clickhouse_code(), Some(241), "{failed}");
    assert!(failed.to_string().contains("memory limit") || failed.to_string().contains("Memory limit"), "{failed}");
    assert_eq!(parts_and_counter(&fixture, &table).await, before);
    assert_clean(&fixture).await;

    // The same batch through a worker with room commits, under the same token.
    assert!(insert(&worker(&fixture).await, &table, &large).await.unwrap().is_committed());
    assert_eq!(Reader::number_once(stack().await, &fixture, "SELECT count() FROM d.wide").await.unwrap(), 21_000);
}

/// A cap the engine and a small insert fit under, and a block of 160 MB does not.
const MEMORY_CAP: u64 = 96 << 20;
