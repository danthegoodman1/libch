//! The differential matrix: each `MergeTree` engine and the features around a table, written through the fork's
//! worker and through the reference writer, must hold the same rows before and after merging.

use libch::writer::TableName;
use libch_testkit::differential::{Case, Format, run};
use libch_testkit::stack;

const TOKENS: &str = "non_replicated_deduplication_window = 10000";

/// A sequence of numbers that is the same on every run.
struct Numbers(u64);

impl Numbers {
    fn next(&mut self, below: u64) -> u64 {
        self.0 = self.0.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1_442_695_040_888_963_407);
        (self.0 >> 33) % below
    }
}

fn table(name: &str) -> TableName {
    TableName::new("d", name)
}

/// Queries over every row of a table, as stored and as `FINAL` reads it.
fn rows_of(table: &str, columns: &str) -> Vec<String> {
    vec![
        format!("SELECT count(), sum(cityHash64(toString(({columns})))) FROM d.{table}"),
        format!("SELECT count(), sum(cityHash64(toString(({columns})))) FROM d.{table} FINAL"),
    ]
}

/// A plain table with the features an insert and a merge carry: a projection, a skip index, columns with
/// defaults, a constraint, a TTL, and nullable, low-cardinality, array and map columns; a view into an
/// aggregating table and a second view from that table into a monthly one. Batches span two partitions.
fn plain() -> Case {
    let mut numbers = Numbers(7);
    let batches = (0..6u64)
        .map(|batch| {
            (0..400u64)
                .map(|row| {
                    let v = numbers.next(100_000);
                    let month = 11 + (row % 2);
                    let day = 1 + numbers.next(28);
                    let kind = ["a", "b", "c", "d"][numbers.next(4) as usize];
                    let note = if row % 5 == 0 { "null".to_string() } else { format!("\"n{}\"", numbers.next(50)) };
                    let tags: Vec<String> = (0..numbers.next(4)).map(|tag| format!("\"t{tag}\"")).collect();
                    format!(
                        "{{\"ts\":\"2026-{month}-{day:02} 10:{:02}:{:02}\",\"k\":{},\"kind\":\"{kind}\",\"v\":{v},\"note\":{note},\"tags\":[{}],\"attrs\":{{\"a\":{},\"b\":{}}}}}\n",
                        row % 60,
                        batch,
                        batch * 1_000 + row,
                        tags.join(","),
                        numbers.next(10),
                        numbers.next(1_000)
                    )
                })
                .collect()
        })
        .collect();
    // A plain table has no `FINAL`: its rows are the ones stored.
    let mut queries = rows_of("t", "ts, k, kind, v, note, tags, attrs, doubled, day")[..1].to_vec();
    queries.extend([
        "SELECT sum(cityHash64(kind, s, c)) FROM (SELECT kind, sum(v) AS s, count() AS c FROM d.t GROUP BY kind SETTINGS force_optimize_projection = 1)"
            .to_string(),
        "SELECT count() FROM d.t WHERE v BETWEEN 1000 AND 2000".to_string(),
        "SELECT countIf(note IS NULL), sum(length(tags)), sum(attrs['a']), sum(doubled) FROM d.t".to_string(),
        "SELECT sum(cityHash64(day, kind, s)) FROM (SELECT day, kind, sumMerge(total) AS s FROM d.daily GROUP BY day, kind)".to_string(),
        "SELECT sum(cityHash64(month, kind, s)) FROM (SELECT month, kind, sumMerge(total) AS s FROM d.monthly GROUP BY month, kind)".to_string(),
        "SELECT sumMerge(total) FROM d.monthly".to_string(),
    ]);
    Case {
        schema: vec![
            format!(
                "CREATE TABLE d.t (ts DateTime, k UInt64, kind LowCardinality(String), v UInt64, note Nullable(String), tags Array(String), \
                 attrs Map(String, UInt64), doubled UInt64 DEFAULT v * 2, day Date MATERIALIZED toDate(ts), \
                 CONSTRAINT v_is_small CHECK v < 1000000000, INDEX v_range v TYPE minmax GRANULARITY 1, \
                 PROJECTION by_kind (SELECT kind, sum(v), count() GROUP BY kind)) \
                 ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY (kind, k) TTL ts + INTERVAL 50 YEAR SETTINGS index_granularity = 64, {TOKENS}"
            ),
            format!("CREATE TABLE d.daily (day Date, kind LowCardinality(String), total AggregateFunction(sum, UInt64)) ENGINE = AggregatingMergeTree ORDER BY (day, kind) SETTINGS {TOKENS}"),
            "CREATE MATERIALIZED VIEW d.daily_mv TO d.daily AS SELECT toDate(ts) AS day, kind, sumState(v) AS total FROM d.t GROUP BY day, kind".to_string(),
            format!("CREATE TABLE d.monthly (month Date, kind LowCardinality(String), total AggregateFunction(sum, UInt64)) ENGINE = AggregatingMergeTree ORDER BY (month, kind) SETTINGS {TOKENS}"),
            "CREATE MATERIALIZED VIEW d.monthly_mv TO d.monthly AS SELECT toStartOfMonth(day) AS month, kind, sumMergeState(total) AS total FROM d.daily GROUP BY month, kind".to_string(),
        ],
        table: table("t"),
        structure: "ts DateTime, k UInt64, kind LowCardinality(String), v UInt64, note Nullable(String), tags Array(String), attrs Map(String, UInt64), doubled UInt64".to_string(),
        tables: vec![table("t"), table("daily"), table("monthly")],
        batches,
        queries,
        insert_settings: Vec::new(),
    }
}

/// A case of one table that batches of `rows` fill, each row from its batch and row numbers.
fn engine(engine: &str, columns: &str, order_by: &str, names: &str, row: impl Fn(u64, u64, &mut Numbers) -> String) -> Case {
    let mut numbers = Numbers(11);
    let batches = (0..5u64).map(|batch| (0..300u64).map(|index| row(batch, index, &mut numbers) + "\n").collect()).collect();
    Case {
        schema: vec![format!("CREATE TABLE d.t ({columns}) ENGINE = {engine} ORDER BY {order_by} SETTINGS index_granularity = 64, {TOKENS}")],
        table: table("t"),
        structure: columns.to_string(),
        tables: vec![table("t")],
        batches,
        queries: rows_of("t", names),
        insert_settings: Vec::new(),
    }
}

#[tokio::test]
async fn merge_tree_with_its_features_in_json() {
    let parts = run(stack().await, &plain(), Format::JsonEachRow).await;
    assert_eq!(parts, 12, "six batches, each over two partitions");
}

#[tokio::test]
async fn merge_tree_with_its_features_in_row_binary() {
    run(stack().await, &plain(), Format::RowBinary).await;
}

#[tokio::test]
async fn merge_tree_with_its_features_in_native() {
    run(stack().await, &plain(), Format::Native).await;
}

#[tokio::test]
async fn a_batch_of_several_blocks() {
    // Blocks of 100 rows: each batch of 400 becomes four parts in a partition, each under a token of its own.
    let mut case = plain();
    case.insert_settings = vec![("max_insert_block_size", "100"), ("min_insert_block_size_rows", "100"), ("min_insert_block_size_bytes", "0")];
    let parts = run(stack().await, &case, Format::JsonEachRow).await;
    assert_eq!(parts, 48, "four blocks a batch, each over two partitions");
}

#[tokio::test]
async fn replacing_merge_tree() {
    let case = engine("ReplacingMergeTree(version)", "k UInt64, version UInt64, v String", "k", "k, version, v", |batch, _, numbers| {
        format!("{{\"k\":{},\"version\":{},\"v\":\"b{batch}-{}\"}}", numbers.next(400), batch * 10 + numbers.next(10), numbers.next(1_000))
    });
    run(stack().await, &case, Format::JsonEachRow).await;
}

#[tokio::test]
async fn summing_merge_tree() {
    let case = engine("SummingMergeTree", "k UInt64, kind LowCardinality(String), a UInt64, b Int64", "(k, kind)", "k, kind, a, b", |_, _, numbers| {
        format!(
            "{{\"k\":{},\"kind\":\"{}\",\"a\":{},\"b\":{}}}",
            numbers.next(200),
            ["x", "y"][numbers.next(2) as usize],
            numbers.next(100),
            numbers.next(100) as i64 - 50
        )
    });
    run(stack().await, &case, Format::Native).await;
}

#[tokio::test]
async fn aggregating_merge_tree() {
    let columns =
        "k UInt64, hi SimpleAggregateFunction(max, UInt64), total SimpleAggregateFunction(sum, UInt64), last SimpleAggregateFunction(anyLast, String)";
    let case = engine("AggregatingMergeTree", columns, "k", "k, hi, total, last", |batch, index, numbers| {
        format!("{{\"k\":{},\"hi\":{},\"total\":{},\"last\":\"b{batch}r{index}\"}}", numbers.next(200), numbers.next(10_000), numbers.next(100))
    });
    run(stack().await, &case, Format::RowBinary).await;
}

/// Rows of a collapsing table: each batch cancels the state its key got from the batch before and writes a new
/// one.
fn collapsing_row(batch: u64, index: u64, versioned: bool) -> String {
    let (k, state) = (index % 150, |batch: u64| (index % 150) * 7 + batch);
    let version = |batch: u64| if versioned { format!(",\"version\":{batch}") } else { String::new() };
    let write = format!("{{\"k\":{k},\"v\":{},\"sign\":1{}}}", state(batch), version(batch));
    // The first half of a batch's rows cancel, and the second half write.
    if batch > 0 && index < 150 { format!("{{\"k\":{k},\"v\":{},\"sign\":-1{}}}", state(batch - 1), version(batch - 1)) } else { write }
}

#[tokio::test]
async fn collapsing_merge_tree() {
    let case = engine("CollapsingMergeTree(sign)", "k UInt64, v UInt64, sign Int8", "k", "k, v, sign", |batch, index, _| collapsing_row(batch, index, false));
    run(stack().await, &case, Format::JsonEachRow).await;
}

#[tokio::test]
async fn versioned_collapsing_merge_tree() {
    let columns = "k UInt64, v UInt64, sign Int8, version UInt32";
    let case = engine("VersionedCollapsingMergeTree(sign, version)", columns, "k", "k, v, sign, version", |batch, index, _| collapsing_row(batch, index, true));
    run(stack().await, &case, Format::JsonEachRow).await;
}
