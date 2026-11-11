//! The differential harness: one workload goes through the reference writer and through workers, into two
//! tenants, and a fixed set of queries must answer alike on both, before and after each tenant merges its tables
//! to one part a partition.

use std::process::Stdio;

use bytes::Bytes;
use libch::stock::{Exclusive, StockWriter, StockWriterOptions};
use libch::worker::OpWriter;
use libch::writer::{Insert, MergeCandidate, TableName};
use libch::{PartInfo, Writer};
use tokio::io::AsyncWriteExt;
use tokio::process::Command;

use crate::ops::{assert_clean, stored};
use crate::{Reader, Stack, TenantFixture};

/// The format a case's batches are sent in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Format {
    JsonEachRow,
    RowBinary,
    Native,
}

impl Format {
    pub fn name(self) -> &'static str {
        match self {
            Format::JsonEachRow => "JSONEachRow",
            Format::RowBinary => "RowBinary",
            Format::Native => "Native",
        }
    }
}

/// A schema, a workload for it, and the queries that compare two tenants holding it.
#[derive(Clone, Debug)]
pub struct Case {
    /// The statements that create the case's tables in database `d`.
    pub schema: Vec<String>,
    /// The table the batches go to.
    pub table: TableName,
    /// That table's insertable columns in order, as `k UInt64, v String`. A binary format carries exactly these.
    pub structure: String,
    /// Every table the batches reach: the table and each view's target.
    pub tables: Vec<TableName>,
    /// The batches, each as `JSONEachRow` text.
    pub batches: Vec<String>,
    /// The queries whose answers both tenants must give alike.
    pub queries: Vec<String>,
    /// Query settings every insert runs under.
    pub insert_settings: Vec<(&'static str, &'static str)>,
    /// MergeTree settings from the server's configuration, as `<min_bytes_for_wide_part>0</min_bytes_for_wide_part>`:
    /// the reference writer boots with them, and the workers get them in their settings document.
    pub merge_tree_settings: String,
    /// Whether the workers merge a partition in one merge, as the reference's `OPTIMIZE ... FINAL` does, where
    /// they otherwise take two so that a merged part is itself a source. One merge writes the bytes the
    /// reference writes; two write the same rows in other compression blocks.
    pub one_merge: bool,
}

/// What both tenants' parts must agree on, beside the case's own queries: each table's parts, rows, kinds and
/// bytes, compressed and not. Equal bytes mean the worker wrote and merged under the reference's settings.
const PARTS: &str = "SELECT table, count(), sum(rows), groupUniqArray(part_type), sum(data_compressed_bytes), sum(data_uncompressed_bytes) \
                     FROM system.parts WHERE database = 'd' AND active GROUP BY table ORDER BY table";

/// The same without the bytes, for parts that a different sequence of merges made. A part merged in two steps
/// holds the rows of one merged in one, in blocks cut at other places: its compression blocks differ, and so do
/// the dictionaries a low-cardinality column writes for each.
const PARTS_AFTER_OTHER_MERGES: &str = "SELECT table, count(), sum(rows), groupUniqArray(part_type) \
                                        FROM system.parts WHERE database = 'd' AND active GROUP BY table ORDER BY table";

/// A batch in the case's format: `clickhouse local` converts the `JSONEachRow` text, so both tenants get the
/// same bytes.
async fn convert(stack: &Stack, case: &Case, json: &str, format: Format) -> Bytes {
    if format == Format::JsonEachRow {
        return Bytes::from(json.to_string());
    }
    let mut child = Command::new(&stack.clickhouse)
        .args(["local", "--structure", &case.structure, "--input-format", "JSONEachRow", "--output-format", format.name(), "--query", "SELECT * FROM table"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("clickhouse local starts");
    let mut stdin = child.stdin.take().expect("stdin is piped");
    let text = json.to_string();
    let feed = tokio::spawn(async move {
        stdin.write_all(text.as_bytes()).await.expect("clickhouse local reads the batch");
    });
    let output = child.wait_with_output().await.expect("clickhouse local runs");
    feed.await.unwrap();
    assert!(output.status.success(), "clickhouse local failed to convert a batch: {}", String::from_utf8_lossy(&output.stderr));
    Bytes::from(output.stdout)
}

async fn tenant(stack: &Stack, case: &Case) -> TenantFixture {
    let fixture = TenantFixture::create(stack).await.expect("a tenant");
    let mut statements = vec!["CREATE DATABASE d"];
    statements.extend(case.schema.iter().map(String::as_str));
    Exclusive::run(&stack.stock_env(), &fixture.tenant, &fixture.key, &statements).await.expect("the case's schema");
    fixture
}

/// Each query's answer on a tenant.
async fn answers(stack: &Stack, fixture: &TenantFixture, queries: &[String]) -> Vec<String> {
    let reader = Reader::open(stack, fixture).await.expect("a reader");
    let mut answers = Vec::new();
    for query in queries {
        answers.push(reader.value(query).await.unwrap_or_else(|error| panic!("`{query}` failed: {error}")));
    }
    reader.close().await.expect("the reader stops");
    answers
}

fn assert_alike(queries: &[String], ours: &[String], reference: &[String], when: &str) {
    for ((query, ours), reference) in queries.iter().zip(ours).zip(reference) {
        assert_eq!(ours, reference, "{when}, the worker's tenant and the reference answer `{query}` differently");
    }
}

/// Merges every partition of a table to one part through workers: in two merges where the partition holds three
/// parts or more, so a merged part is itself a source, or in one where the case asks.
async fn merge_to_one_part(writer: &OpWriter, fixture: &TenantFixture, table: &TableName, one_merge: bool) {
    let parts = stored(fixture, table).await;
    for partition in parts.set.partitions() {
        let active: Vec<PartInfo> = parts.set.active_in(partition).cloned().collect();
        if active.len() < 2 {
            continue;
        }
        let first = if active.len() >= 3 && !one_merge { active.len().div_ceil(2) } else { active.len() };
        let mut sources = active[..first].to_vec();
        loop {
            let candidate = MergeCandidate { partition_id: partition.to_string(), sources: sources.clone() };
            let merged =
                writer.merge(table, &candidate).await.expect("the merge runs").committed().unwrap_or_else(|| panic!("the merge of {sources:?} commits"));
            if merged.part.contains(active.last().expect("the partition holds parts")) {
                break;
            }
            sources = std::iter::once(merged.part).chain(active[first..].iter().cloned()).collect();
        }
    }
    let after = stored(fixture, table).await;
    for (partition, count) in after.set.active_counts() {
        assert_eq!(count, 1, "partition {partition} of {table} holds one part after its merges");
    }
}

/// Runs a case in a format: the same batches through workers and through the reference writer, the queries
/// compared, each table merged to one part a partition on both tenants, and the queries compared again. Returns
/// how many parts the batches made in the case's table.
pub async fn run(stack: &Stack, case: &Case, format: Format) -> usize {
    let ours = tenant(stack, case).await;
    let reference = tenant(stack, case).await;

    let profile: String = case.insert_settings.iter().map(|(name, value)| format!("<{name}>{value}</{name}>")).collect();
    let settings = format!("<profiles><default>{profile}</default></profiles><merge_tree>{}</merge_tree>", case.merge_tree_settings);
    let writer = OpWriter::new(stack.worker_command(), &ours.tenant, &ours.key).with_settings_xml(settings);
    let options = StockWriterOptions { merge_tree_settings_xml: case.merge_tree_settings.clone(), ..StockWriterOptions::default() };
    let stock = StockWriter::start(&stack.stock_env(), &reference.tenant, &reference.key, &options).await.expect("the reference writer");
    let queries: Vec<String> = case.queries.iter().cloned().chain([PARTS.to_string()]).collect();
    let statement = format!("INSERT INTO {} FORMAT {}", case.table.quoted(), format.name());
    for (index, batch) in case.batches.iter().enumerate() {
        let data = convert(stack, case, batch, format).await;
        let token = format!("batch-{index}");
        let inserted = writer.insert(Insert { table: &case.table, format: format.name(), token: &token, data: data.clone() }).await;
        assert!(inserted.unwrap_or_else(|error| panic!("batch {index} through a worker: {error}")).is_committed(), "batch {index} commits through a worker");
        // One thread, as an operation writes: a server that writes a batch's blocks on several threads numbers
        // them in the order they finish, and an engine that keeps a key's last row then answers by that order.
        let mut settings = vec![("insert_deduplication_token", token.as_str()), ("async_insert", "0"), ("max_threads", "1"), ("max_insert_threads", "1")];
        settings.extend(case.insert_settings.iter().copied());
        stock.server().run(&statement, &settings, data).await.unwrap_or_else(|error| panic!("batch {index} through the reference: {error}"));
    }

    // The same blocks went into both tenants, so they hold the same parts.
    let parts = stored(&ours, &case.table).await.active().len();
    for table in &case.tables {
        let (theirs, mine) = (stored(&reference, table).await, stored(&ours, table).await);
        assert_eq!(mine.set.active_counts(), theirs.set.active_counts(), "the parts of {table}");
    }
    let before = answers(stack, &reference, &queries).await;
    assert_alike(&queries, &answers(stack, &ours, &queries).await, &before, "before any merge");

    for table in &case.tables {
        merge_to_one_part(&writer, &ours, table, case.one_merge).await;
        stock.server().query(&format!("OPTIMIZE TABLE {} FINAL", table.quoted())).await.expect("the reference merges");
    }
    stock.stop().await.expect("the reference writer stops");

    let merged_parts = if case.one_merge { PARTS } else { PARTS_AFTER_OTHER_MERGES };
    let queries: Vec<String> = case.queries.iter().cloned().chain([merged_parts.to_string()]).collect();
    let after = answers(stack, &reference, &queries).await;
    assert_alike(&queries, &answers(stack, &ours, &queries).await, &after, "after the merges");
    assert_clean(&ours).await;
    parts
}
