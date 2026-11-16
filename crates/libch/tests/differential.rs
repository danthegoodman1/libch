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
        merge_tree_settings: String::new(),
        one_merge: false,
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
        merge_tree_settings: String::new(),
        one_merge: false,
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
async fn server_settings_shape_the_parts_as_on_the_reference() {
    // Wide parts from the first row, and a merge that writes columns one at a time: settings a writer takes from
    // its configuration, where a table's definition leaves them out.
    let mut case = plain();
    case.merge_tree_settings = "<min_bytes_for_wide_part>0</min_bytes_for_wide_part><min_rows_for_wide_part>0</min_rows_for_wide_part>\
                                <vertical_merge_algorithm_min_rows_to_activate>1</vertical_merge_algorithm_min_rows_to_activate>\
                                <vertical_merge_algorithm_min_columns_to_activate>1</vertical_merge_algorithm_min_columns_to_activate>"
        .to_string();
    case.one_merge = true;
    case.queries.push("SELECT throwIf(countIf(part_type != 'Wide') > 0) FROM system.parts WHERE database = 'd' AND active".to_string());
    run(stack().await, &case, Format::JsonEachRow).await;
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

/// A table with a `JSON` column whose rows bring new paths batch by batch: paths under an object, a path that one
/// batch alone writes, thirty that rows take in turn, one whose type changes between batches, an array, an array
/// of objects, and a path the rows sometimes leave out. A part holds the paths its own rows carry, so each
/// insert and merge writes them with no change to the table's definition.
fn json(data_type: &str) -> Case {
    let mut numbers = Numbers(13);
    let batches = (0..5u64)
        .map(|batch| {
            (0..300u64)
                .map(|index| {
                    let id = numbers.next(10_000);
                    let name = if index % 7 == 0 { String::new() } else { format!(",\"name\":\"u{id}\"") };
                    let mixed = if batch % 2 == 0 { numbers.next(1_000).to_string() } else { format!("\"s{}\"", numbers.next(1_000)) };
                    let tags: Vec<String> = (0..numbers.next(4)).map(|tag| format!("\"t{tag}\"")).collect();
                    let items: Vec<String> =
                        (0..numbers.next(3)).map(|_| format!("{{\"sku\":\"s{}\",\"qty\":{}}}", numbers.next(50), numbers.next(9))).collect();
                    format!(
                        "{{\"k\":{},\"data\":{{\"user\":{{\"id\":{id}{name}}},\"mixed\":{mixed},\"b{batch}\":{{\"only\":{}}},\"p{}\":{},\"noise\":{index},\"tags\":[{}],\"items\":[{}]}}}}\n",
                        batch * 1_000 + index,
                        numbers.next(100),
                        index % 30,
                        numbers.next(100),
                        tags.join(","),
                        items.join(",")
                    )
                })
                .collect()
        })
        .collect();
    Case {
        schema: vec![format!("CREATE TABLE d.t (k UInt64, data {data_type}) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 64, {TOKENS}")],
        table: table("t"),
        structure: format!("k UInt64, data {data_type}"),
        tables: vec![table("t")],
        batches,
        queries: vec![
            "SELECT count(), sum(cityHash64(k, toJSONString(data))) FROM d.t".to_string(),
            "SELECT sum(data.user.id::Nullable(UInt64)), countIf(data.user.name IS NULL), countIf(dynamicType(data.mixed) = 'Int64'), \
             countIf(dynamicType(data.mixed) = 'String'), sum(data.mixed.:Int64), sum(length(data.mixed.:String)), sum(data.b3.only::Nullable(UInt64)) FROM d.t"
                .to_string(),
            "SELECT arrayStringConcat(arraySort(groupUniqArrayArray(JSONAllPaths(data))), ',') FROM d.t".to_string(),
            "SELECT sum(cityHash64(toString(JSONAllPathsWithTypes(data)))) FROM d.t".to_string(),
            "SELECT sum(length(data.tags::Array(String))), sum(length(data.items[])), sum(cityHash64(toString(data.items[].qty))) FROM d.t".to_string(),
        ],
        insert_settings: Vec::new(),
        merge_tree_settings: String::new(),
        one_merge: false,
    }
}

#[tokio::test]
async fn json_paths_arrive_with_the_batches_in_json() {
    let mut case = json("JSON");
    // 41 paths, each in a column of its own: the two under `user`, `mixed`, `noise`, `tags`, `items`, one for each
    // of five batches and 30 that rows take in turn.
    case.queries.push(
        "SELECT throwIf(length(groupUniqArrayArray(JSONAllPaths(data))) != 41), throwIf(length(groupUniqArrayArray(JSONDynamicPaths(data))) != 41) FROM d.t"
            .to_string(),
    );
    let parts = run(stack().await, &case, Format::JsonEachRow).await;
    assert_eq!(parts, 5);
}

#[tokio::test]
async fn json_paths_arrive_with_the_batches_in_native() {
    run(stack().await, &json("JSON"), Format::Native).await;
}

#[tokio::test]
async fn json_paths_arrive_with_the_batches_in_row_binary() {
    run(stack().await, &json("JSON"), Format::RowBinary).await;
}

#[tokio::test]
async fn json_with_a_typed_path_a_skipped_path_and_more_paths_than_columns() {
    // Eight paths get columns of their own in a part and the rest share one; `user.id` is a typed column and
    // `noise` is dropped as a row is read. Wide parts and a vertical merge write each path's files apart.
    let mut case = json("JSON(max_dynamic_paths = 8, user.id UInt64, SKIP noise)");
    case.merge_tree_settings = "<min_bytes_for_wide_part>0</min_bytes_for_wide_part><min_rows_for_wide_part>0</min_rows_for_wide_part>\
                                <vertical_merge_algorithm_min_rows_to_activate>1</vertical_merge_algorithm_min_rows_to_activate>\
                                <vertical_merge_algorithm_min_columns_to_activate>1</vertical_merge_algorithm_min_columns_to_activate>"
        .to_string();
    case.one_merge = true;
    case.queries.push("SELECT throwIf(countIf(part_type != 'Wide') > 0) FROM system.parts WHERE database = 'd' AND active".to_string());
    case.queries.push(
        "SELECT throwIf(has(groupUniqArrayArray(JSONAllPaths(data)), 'noise')), throwIf(max(length(JSONSharedDataPaths(data))) = 0), \
         length(groupUniqArrayArray(JSONDynamicPaths(data))), toTypeName(any(data.user.id)) FROM d.t"
            .to_string(),
    );
    // A part keeps eight paths in columns of their own.
    case.queries
        .push("SELECT throwIf(max(paths) != 8) FROM (SELECT length(groupUniqArrayArray(JSONDynamicPaths(data))) AS paths FROM d.t GROUP BY _part)".to_string());
    run(stack().await, &case, Format::JsonEachRow).await;
}

/// A table with a `Variant` column and two `Dynamic` ones, the types a `JSON` path is stored in. Each batch brings
/// values of a type the batches before it left out, so parts hold different sets of types and a merge joins
/// them. `small` keeps two types in columns of their own and the rest in its shared variant.
fn variants() -> Case {
    let mut numbers = Numbers(17);
    let batches = (0..5u64)
        .map(|batch| {
            (0..300u64)
                .map(|index| {
                    let n = numbers.next(1_000);
                    let variant = match (batch + index) % 4 {
                        0 => n.to_string(),
                        1 => format!("\"s{n}\""),
                        2 => format!("[{n},{}]", n + 1),
                        _ => "null".to_string(),
                    };
                    // A batch writes the types up to its own number: integers, then strings, floats, arrays and booleans.
                    let dynamic = match index % (batch + 1) {
                        0 => n.to_string(),
                        1 => format!("\"d{n}\""),
                        2 => format!("{n}.5"),
                        3 => format!("[{n}]"),
                        _ => n.is_multiple_of(2).to_string(),
                    };
                    format!("{{\"k\":{},\"v\":{variant},\"d\":{dynamic},\"small\":{dynamic}}}\n", batch * 1_000 + index)
                })
                .collect()
        })
        .collect();
    let columns = "k UInt64, v Variant(UInt64, String, Array(UInt64)), d Dynamic, small Dynamic(max_types = 2)";
    let types = |column: &str, kind: &str| {
        format!("SELECT toString(arraySort(groupArray((t, c)))) FROM (SELECT {kind} AS t, count() AS c FROM d.t GROUP BY t) -- {column}")
    };
    Case {
        schema: vec![format!("CREATE TABLE d.t ({columns}) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 64, {TOKENS}")],
        table: table("t"),
        structure: columns.to_string(),
        tables: vec![table("t")],
        batches,
        queries: vec![
            "SELECT count(), sum(cityHash64(k, toString(v), toString(d), toString(small))) FROM d.t".to_string(),
            types("v", "toString(variantType(v))"),
            types("d", "dynamicType(d)"),
            types("small", "dynamicType(small)"),
            "SELECT sum(v.UInt64), sum(length(v.String)), sum(arraySum(v.`Array(UInt64)`)), sum(d.Int64), sum(length(d.String)), sum(d.Float64), \
             sum(small.Int64), throwIf(countIf(isDynamicElementInSharedData(small)) = 0) FROM d.t"
                .to_string(),
            "SELECT throwIf(uniqExact(dynamicType(d)) != 5) FROM d.t".to_string(),
        ],
        insert_settings: Vec::new(),
        merge_tree_settings: String::new(),
        one_merge: false,
    }
}

#[tokio::test]
async fn variant_and_dynamic_columns_in_json() {
    run(stack().await, &variants(), Format::JsonEachRow).await;
}

#[tokio::test]
async fn variant_and_dynamic_columns_in_native() {
    run(stack().await, &variants(), Format::Native).await;
}

#[tokio::test]
async fn variant_and_dynamic_columns_in_row_binary() {
    run(stack().await, &variants(), Format::RowBinary).await;
}

#[tokio::test]
async fn variant_and_dynamic_columns_in_wide_parts_merged_column_by_column() {
    let mut case = variants();
    case.merge_tree_settings = "<min_bytes_for_wide_part>0</min_bytes_for_wide_part><min_rows_for_wide_part>0</min_rows_for_wide_part>\
                                <vertical_merge_algorithm_min_rows_to_activate>1</vertical_merge_algorithm_min_rows_to_activate>\
                                <vertical_merge_algorithm_min_columns_to_activate>1</vertical_merge_algorithm_min_columns_to_activate>"
        .to_string();
    case.one_merge = true;
    case.queries.push("SELECT throwIf(countIf(part_type != 'Wide') > 0) FROM system.parts WHERE database = 'd' AND active".to_string());
    run(stack().await, &case, Format::JsonEachRow).await;
}
