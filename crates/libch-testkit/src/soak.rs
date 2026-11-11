//! Workloads for the differential soak: from a seed, a schema drawn from the engines and features of the
//! differential matrix, batches of random sizes and rows, and the settings and format they are written under.
//!
//! [`workload`] is a pure function of its seed, so a difference a soak finds is reproduced by running that seed
//! again.

use libch::writer::TableName;

use crate::differential::{Case, Format};

const TOKENS: &str = "non_replicated_deduplication_window = 10000";

/// Numbers that are the same for a seed on every run.
pub struct Numbers(u64);

impl Numbers {
    pub fn seeded(seed: u64) -> Numbers {
        // A seed of zero would leave the generator's first outputs small.
        Numbers(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ 0x2545_f491_4f6c_dd1d)
    }

    /// A number below `below`.
    pub fn next(&mut self, below: u64) -> u64 {
        self.0 = self.0.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1_442_695_040_888_963_407);
        (self.0 >> 33) % below
    }

    fn chance(&mut self, one_in: u64) -> bool {
        self.next(one_in) == 0
    }

    fn pick<'a>(&mut self, of: &[&'a str]) -> &'a str {
        of[self.next(of.len() as u64) as usize]
    }
}

/// A workload, the format its batches go in, and a line that says what was drawn.
pub struct Workload {
    pub case: Case,
    pub format: Format,
    pub summary: String,
}

fn table(name: &str) -> TableName {
    TableName::new("d", name)
}

/// A query over every row of a table, as stored or as `FINAL` reads it.
fn rows_of(table: &str, columns: &str, final_too: bool) -> Vec<String> {
    let stored = format!("SELECT count(), sum(cityHash64(toString(({columns})))) FROM d.{table}");
    if final_too { vec![stored.clone(), format!("{stored} FINAL")] } else { vec![stored] }
}

/// The workload of a seed.
pub fn workload(seed: u64) -> Workload {
    let mut numbers = Numbers::seeded(seed);
    let engine = numbers.next(6);
    let batches = 2 + numbers.next(6);
    let partition = numbers.pick(&["", "", "PARTITION BY k % 3"]);
    let mut drawn = Vec::new();

    let mut case = match engine {
        0 => plain(&mut numbers, batches, &mut drawn),
        1 => one_table(
            "ReplacingMergeTree(version)",
            "k UInt64, version UInt64, v String",
            "k",
            "k, version, v",
            partition,
            &mut numbers,
            batches,
            |batch, _, numbers| {
                format!("{{\"k\":{},\"version\":{},\"v\":\"b{batch}-{}\"}}", numbers.next(400), batch * 10 + numbers.next(10), numbers.next(1_000))
            },
        ),
        2 => one_table(
            "SummingMergeTree",
            "k UInt64, kind LowCardinality(String), a UInt64, b Int64",
            "(k, kind)",
            "k, kind, a, b",
            partition,
            &mut numbers,
            batches,
            |_, _, numbers| {
                let (k, kind, a) = (numbers.next(200), ["x", "y"][numbers.next(2) as usize], numbers.next(100));
                format!("{{\"k\":{k},\"kind\":\"{kind}\",\"a\":{a},\"b\":{}}}", numbers.next(100) as i64 - 50)
            },
        ),
        3 => one_table(
            "AggregatingMergeTree",
            "k UInt64, hi SimpleAggregateFunction(max, UInt64), total SimpleAggregateFunction(sum, UInt64), last SimpleAggregateFunction(anyLast, String)",
            "k",
            "k, hi, total, last",
            partition,
            &mut numbers,
            batches,
            |batch, index, numbers| {
                format!("{{\"k\":{},\"hi\":{},\"total\":{},\"last\":\"b{batch}r{index}\"}}", numbers.next(200), numbers.next(10_000), numbers.next(100))
            },
        ),
        4 => one_table("CollapsingMergeTree(sign)", "k UInt64, v UInt64, sign Int8", "k", "k, v, sign", partition, &mut numbers, batches, |batch, index, _| {
            collapsing_row(batch, index, false)
        }),
        _ => one_table(
            "VersionedCollapsingMergeTree(sign, version)",
            "k UInt64, v UInt64, sign Int8, version UInt32",
            "k",
            "k, v, sign, version",
            partition,
            &mut numbers,
            batches,
            |batch, index, _| collapsing_row(batch, index, true),
        ),
    };
    drawn.insert(0, ["MergeTree", "Replacing", "Summing", "Aggregating", "Collapsing", "VersionedCollapsing"][engine as usize].to_string());
    if engine != 0 && !partition.is_empty() {
        drawn.push("partitioned".into());
    }

    if numbers.chance(4) {
        // Blocks smaller than most batches, so a batch becomes several parts under tokens of their own.
        case.insert_settings = vec![("max_insert_block_size", "200"), ("min_insert_block_size_rows", "200"), ("min_insert_block_size_bytes", "0")];
        drawn.push("blocks of 200".into());
    }
    if numbers.chance(4) {
        case.merge_tree_settings = "<min_bytes_for_wide_part>0</min_bytes_for_wide_part><min_rows_for_wide_part>0</min_rows_for_wide_part>\
                                    <vertical_merge_algorithm_min_rows_to_activate>1</vertical_merge_algorithm_min_rows_to_activate>\
                                    <vertical_merge_algorithm_min_columns_to_activate>1</vertical_merge_algorithm_min_columns_to_activate>"
            .to_string();
        drawn.push("wide parts, vertical merges".into());
    }
    // A vertical merge writes its columns in the blocks its sources had, so only a merge of the same sources as
    // the reference's writes the same bytes.
    case.one_merge = !case.merge_tree_settings.is_empty() || numbers.chance(2);
    drawn.push(if case.one_merge { "one merge".into() } else { "two merges".into() });

    let format = [Format::JsonEachRow, Format::RowBinary, Format::Native][numbers.next(3) as usize];
    let rows: usize = case.batches.iter().map(|batch| batch.lines().count()).sum();
    let summary = format!("seed {seed}: {}; {} batches, {rows} rows, {}", drawn.join(", "), case.batches.len(), format.name());
    Workload { case, format, summary }
}

/// One table of an engine that folds rows as it merges. Each batch holds between 1 and 1,500 rows, each from
/// its batch and row numbers.
#[allow(clippy::too_many_arguments)]
fn one_table(
    engine: &str,
    columns: &str,
    order_by: &str,
    names: &str,
    partition: &str,
    numbers: &mut Numbers,
    batches: u64,
    row: impl Fn(u64, u64, &mut Numbers) -> String,
) -> Case {
    let batches = (0..batches)
        .map(|batch| {
            let rows = 1 + numbers.next(1_500);
            (0..rows).map(|index| row(batch, index, numbers) + "\n").collect()
        })
        .collect();
    Case {
        schema: vec![format!("CREATE TABLE d.t ({columns}) ENGINE = {engine} {partition} ORDER BY {order_by} SETTINGS index_granularity = 64, {TOKENS}")],
        table: table("t"),
        structure: columns.to_string(),
        tables: vec![table("t")],
        batches,
        queries: rows_of("t", names, true),
        insert_settings: Vec::new(),
        merge_tree_settings: String::new(),
        one_merge: false,
    }
}

/// Rows of a collapsing table: each batch cancels the state its key got from the batch before and writes a new
/// one.
fn collapsing_row(batch: u64, index: u64, versioned: bool) -> String {
    let (k, state) = (index % 150, |batch: u64| (index % 150) * 7 + batch);
    let version = |batch: u64| if versioned { format!(",\"version\":{batch}") } else { String::new() };
    if batch > 0 && index < 150 {
        format!("{{\"k\":{k},\"v\":{},\"sign\":-1{}}}", state(batch - 1), version(batch - 1))
    } else {
        format!("{{\"k\":{k},\"v\":{},\"sign\":1{}}}", state(batch), version(batch))
    }
}

/// A plain table with a draw of the features an insert and a merge carry, and up to two views behind it.
fn plain(numbers: &mut Numbers, batches: u64, drawn: &mut Vec<String>) -> Case {
    let mut columns =
        vec!["ts DateTime", "k UInt64", "kind LowCardinality(String)", "v UInt64", "note Nullable(String)", "tags Array(String)", "attrs Map(String, UInt64)"];
    let mut names = vec!["ts", "k", "kind", "v", "note", "tags", "attrs"];
    let mut clauses: Vec<&str> = Vec::new();
    let mut structure = columns.clone();
    let mut feature = |numbers: &mut Numbers, name: &str| {
        let on = numbers.chance(2);
        if on {
            drawn.push(name.to_string());
        }
        on
    };
    if feature(numbers, "default column") {
        columns.push("doubled UInt64 DEFAULT v * 2");
        structure.push("doubled UInt64");
        names.push("doubled");
    }
    if feature(numbers, "materialized column") {
        columns.push("day Date MATERIALIZED toDate(ts)");
        names.push("day");
    }
    if feature(numbers, "constraint") {
        clauses.push("CONSTRAINT v_is_small CHECK v < 1000000000");
    }
    if feature(numbers, "skip index") {
        clauses.push("INDEX v_range v TYPE minmax GRANULARITY 1");
    }
    let projection = feature(numbers, "projection");
    if projection {
        clauses.push("PROJECTION by_kind (SELECT kind, sum(v), count() GROUP BY kind)");
    }
    let ttl = if feature(numbers, "TTL") { "TTL ts + INTERVAL 50 YEAR" } else { "" };
    let partition = numbers.pick(&["", "PARTITION BY toYYYYMM(ts)", "PARTITION BY k % 3"]);
    if !partition.is_empty() {
        drawn.push(partition.to_lowercase());
    }
    let views = numbers.next(3);
    drawn.push(format!("{views} views"));

    let definitions: Vec<&str> = columns.iter().chain(&clauses).copied().collect();
    let mut schema = vec![format!(
        "CREATE TABLE d.t ({}) ENGINE = MergeTree {partition} ORDER BY (kind, k) {ttl} SETTINGS index_granularity = 64, {TOKENS}",
        definitions.join(", ")
    )];
    let mut tables = vec![table("t")];
    let mut queries = rows_of("t", &names.join(", "), false);
    queries.push("SELECT count() FROM d.t WHERE v BETWEEN 1000 AND 2000".to_string());
    queries.push("SELECT countIf(note IS NULL), sum(length(tags)), sum(attrs['a']) FROM d.t".to_string());
    if projection {
        let grouped = "SELECT kind, sum(v) AS s, count() AS c FROM d.t GROUP BY kind SETTINGS force_optimize_projection = 1";
        queries.push(format!("SELECT sum(cityHash64(kind, s, c)) FROM ({grouped})"));
    }
    if views >= 1 {
        schema.push(format!(
            "CREATE TABLE d.daily (day Date, kind LowCardinality(String), total AggregateFunction(sum, UInt64)) ENGINE = AggregatingMergeTree ORDER BY (day, kind) SETTINGS {TOKENS}"
        ));
        schema.push(
            "CREATE MATERIALIZED VIEW d.daily_mv TO d.daily AS SELECT toDate(ts) AS day, kind, sumState(v) AS total FROM d.t GROUP BY day, kind".to_string(),
        );
        tables.push(table("daily"));
        queries.push("SELECT sum(cityHash64(day, kind, s)) FROM (SELECT day, kind, sumMerge(total) AS s FROM d.daily GROUP BY day, kind)".to_string());
    }
    if views == 2 {
        schema.push(format!(
            "CREATE TABLE d.monthly (month Date, kind LowCardinality(String), total AggregateFunction(sum, UInt64)) ENGINE = AggregatingMergeTree ORDER BY (month, kind) SETTINGS {TOKENS}"
        ));
        schema.push(
            "CREATE MATERIALIZED VIEW d.monthly_mv TO d.monthly AS SELECT toStartOfMonth(day) AS month, kind, sumMergeState(total) AS total FROM d.daily GROUP BY month, kind"
                .to_string(),
        );
        tables.push(table("monthly"));
        queries.push("SELECT sumMerge(total) FROM d.monthly".to_string());
    }

    let batches = (0..batches)
        .map(|batch| {
            let rows = 1 + numbers.next(1_500);
            (0..rows)
                .map(|row| {
                    let (month, day) = (11 + numbers.next(2), 1 + numbers.next(28));
                    let kind = ["a", "b", "c", "d"][numbers.next(4) as usize];
                    let note = if numbers.chance(5) { "null".to_string() } else { format!("\"n{}\"", numbers.next(50)) };
                    let tags: Vec<String> = (0..numbers.next(4)).map(|tag| format!("\"t{tag}\"")).collect();
                    format!(
                        "{{\"ts\":\"2026-{month}-{day:02} 10:{:02}:{:02}\",\"k\":{},\"kind\":\"{kind}\",\"v\":{},\"note\":{note},\"tags\":[{}],\"attrs\":{{\"a\":{},\"b\":{}}}}}\n",
                        row % 60,
                        batch % 60,
                        batch * 10_000 + row,
                        numbers.next(100_000),
                        tags.join(","),
                        numbers.next(10),
                        numbers.next(1_000)
                    )
                })
                .collect()
        })
        .collect();

    Case {
        schema,
        table: table("t"),
        structure: structure.join(", "),
        tables,
        batches,
        queries,
        insert_settings: Vec::new(),
        merge_tree_settings: String::new(),
        one_merge: false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_seed_gives_the_same_workload_every_time() {
        for seed in [0, 1, 7, 1_000_003] {
            let (first, second) = (workload(seed), workload(seed));
            assert_eq!(first.summary, second.summary);
            assert_eq!(first.case.schema, second.case.schema);
            assert_eq!(first.case.batches, second.case.batches);
            assert_eq!(first.format, second.format);
        }
    }

    #[test]
    fn seeds_reach_every_engine_and_format() {
        let workloads: Vec<Workload> = (0..200).map(workload).collect();
        for engine in ["MergeTree", "Replacing", "Summing", "Aggregating", "Collapsing", "VersionedCollapsing"] {
            assert!(workloads.iter().any(|workload| workload.summary.contains(&format!(": {engine}, "))), "{engine}");
        }
        for format in [Format::JsonEachRow, Format::RowBinary, Format::Native] {
            assert!(workloads.iter().any(|workload| workload.format == format), "{}", format.name());
        }
        for feature in ["projection", "TTL", "2 views", "blocks of 200", "wide parts", "partition by k % 3", "one merge"] {
            assert!(workloads.iter().any(|workload| workload.summary.contains(feature)), "{feature}");
        }
        assert!(workloads.iter().all(|workload| (2..=7).contains(&workload.case.batches.len())));
    }

    #[test]
    fn a_plain_workload_names_each_table_its_batches_reach() {
        let plain: Vec<Workload> = (0..200).map(workload).filter(|workload| workload.summary.contains(": MergeTree, ")).collect();
        for workload in &plain {
            let views = workload.case.schema.iter().filter(|statement| statement.starts_with("CREATE MATERIALIZED VIEW")).count();
            assert_eq!(workload.case.tables.len(), 1 + views, "{}", workload.summary);
            assert!(!workload.case.structure.contains("MATERIALIZED") && !workload.case.structure.contains("DEFAULT"), "{}", workload.case.structure);
        }
    }
}
