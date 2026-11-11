//! The invariant checker: what must hold of a tenant's disk whoever wrote it.
//!
//! [`tenant`] reads InodeDB and S3 alone, so it runs beside live writers and readers. [`stock_load`] then has a
//! fresh stock exclusive writer load the disk, which is the final word on whether stock ClickHouse accepts it;
//! starting that writer fences every other writer, so it runs once they have stopped. [`orphans`] counts the
//! blobs in S3 that the disk no longer names, or never did.
//!
//! ```no_run
//! use libch::check;
//!
//! # async fn checked(tenant: libch::Tenant, key: &str) -> Result<(), libch::Error> {
//! let report = check::tenant(&tenant, key).await?;
//! if !report.is_clean() {
//!     // The report lists each violation: intersecting parts, a block number used twice, a missing blob.
//!     eprintln!("{report}");
//! }
//!
//! // What a quiet tenant keeps in S3 beyond its parts: blobs awaiting removal, and blobs nothing names.
//! let left = check::orphans(&tenant, key).await?;
//! println!("{} orphan blobs of {} bytes", left.orphans.len(), left.bytes());
//! # Ok(())
//! # }
//! ```

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures_util::future::try_join_all;
use inodedb::{Kind, List, Version};

use crate::catalog::{Catalog, Table, TableParts, is_not_found};
use crate::disk::{Record, Value, ValueKind};
use crate::s3::{Object, S3Client};
use crate::stock::{Exclusive, StockEnv, quote_string};
use crate::writer::TableName;
use crate::{Error, Tenant};

/// The files every part's directory holds.
const PART_FILES: [&str; 3] = ["checksums.txt", "columns.txt", "count.txt"];

/// How many S3 requests run at once.
const S3_CONCURRENCY: usize = 32;

/// One way a disk breaks an invariant.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Violation {
    /// Two active parts of a partition share blocks, and neither contains the other. ClickHouse refuses to
    /// load such a table.
    IntersectingParts { table: TableName, first: String, second: String },
    /// One block number names parts in two partitions. A table numbers its blocks once.
    RepeatedBlock { table: TableName, block: i64, parts: Vec<String> },
    /// A part committed in the current writer epoch has a block number above the table's counter.
    CounterBehind { table: TableName, counter: i64, part: String },
    /// A part's directory lacks a file every part has.
    MissingPartFile { table: TableName, part: String, file: String },
    /// A file's record names a blob that S3 does not hold.
    MissingBlob { path: String, key: String },
    /// A blob's size in S3 differs from the size its record gives.
    BlobSize { path: String, key: String, recorded: u64, found: u64 },
    /// A file's value is in no format the disk writes.
    UnreadableFile { path: String, message: String },
    /// Coordination files exist for a table path that holds no table.
    OrphanCoordination { path: String },
    /// A stock writer that loaded the table set a part aside as broken.
    DetachedPart { table: TableName, part: String },
    /// `CHECK TABLE` on a stock writer found the table damaged.
    CheckTableFailed { table: TableName, message: String },
}

impl fmt::Display for Violation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Violation::IntersectingParts { table, first, second } => write!(f, "{table}: parts {first} and {second} intersect"),
            Violation::RepeatedBlock { table, block, parts } => write!(f, "{table}: block {block} names parts in two partitions: {}", parts.join(", ")),
            Violation::CounterBehind { table, counter, part } => write!(f, "{table}: part {part} is numbered above the block counter, {counter}"),
            Violation::MissingPartFile { table, part, file } => write!(f, "{table}: part {part} has no {file}"),
            Violation::MissingBlob { path, key } => write!(f, "{path}: S3 holds no blob {key}"),
            Violation::BlobSize { path, key, recorded, found } => write!(f, "{path}: blob {key} holds {found} bytes, and its record says {recorded}"),
            Violation::UnreadableFile { path, message } => write!(f, "{path}: {message}"),
            Violation::OrphanCoordination { path } => write!(f, "coord/{path}: coordination files for a path that holds no table"),
            Violation::DetachedPart { table, part } => write!(f, "{table}: a stock writer detached {part}"),
            Violation::CheckTableFailed { table, message } => write!(f, "{table}: CHECK TABLE failed: {message}"),
        }
    }
}

/// What a check found, and how much it read.
#[derive(Clone, Debug, Default)]
pub struct Report {
    pub violations: Vec<Violation>,
    /// The MergeTree tables checked.
    pub tables: usize,
    /// The part directories seen, active and covered.
    pub parts: usize,
    /// The files under the disk's tree.
    pub files: usize,
    /// The distinct blobs those files name.
    pub blobs: usize,
}

impl Report {
    pub fn is_clean(&self) -> bool {
        self.violations.is_empty()
    }
}

impl fmt::Display for Report {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} tables, {} parts, {} files, {} blobs: ", self.tables, self.parts, self.files, self.blobs)?;
        if self.is_clean() {
            return write!(f, "clean");
        }
        write!(f, "{} violations", self.violations.len())?;
        for violation in &self.violations {
            write!(f, "\n  {violation}")?;
        }
        Ok(())
    }
}

/// Checks a tenant's disk from InodeDB and S3. `key` needs the `read` grant on the tenant's directory.
pub async fn tenant(tenant: &Tenant, key: &str) -> Result<Report, Error> {
    let catalog = Catalog::connect(tenant, key).await?;
    let mut report = catalog_invariants(&catalog).await?;
    blobs(&catalog, tenant, &mut report).await?;
    Ok(report)
}

/// Checks what InodeDB alone shows: parts, counters and coordination files.
pub async fn catalog_invariants(catalog: &Catalog) -> Result<Report, Error> {
    let mut report = Report::default();
    let owner = catalog.owner().await?;
    let tables = catalog.all_tables().await?;
    for table in tables.iter().filter(|table| table.is_merge_tree()) {
        let parts = catalog.parts(table).await?;
        report.tables += 1;
        report.parts += parts.entries.len();
        part_invariants(table, &parts, owner.as_ref().map(|owner| owner.version), &mut report.violations);
        part_files(catalog, table, &parts, &mut report.violations).await?;
    }

    let table_paths: Vec<String> = tables.iter().map(Table::data_path).collect();
    for path in catalog.coordinated_paths().await? {
        if table_paths.contains(&path) || exists(catalog, &format!("tree/{path}")).await? {
            continue;
        }
        // Dropping a table removes its coordination files in the same commit, so a path whose files are gone
        // by now was dropped while this check ran.
        if exists(catalog, &format!("coord/{path}")).await? {
            report.violations.push(Violation::OrphanCoordination { path });
        }
    }
    Ok(report)
}

fn part_invariants(table: &Table, parts: &TableParts, owner_version: Option<u64>, violations: &mut Vec<Violation>) {
    for (first, second) in &parts.set.intersections {
        violations.push(Violation::IntersectingParts { table: table.name.clone(), first: first.name(), second: second.name() });
    }

    // A block number belongs to one insert, so the single-block parts that carry it share a partition.
    let mut by_block: BTreeMap<i64, Vec<&str>> = BTreeMap::new();
    for (name, entry) in &parts.entries {
        if entry.info.min_block == entry.info.max_block {
            by_block.entry(entry.info.min_block).or_default().push(name);
        }
    }
    for (block, names) in by_block {
        let partition = |name: &str| parts.entries[name].info.partition_id.clone();
        if names.iter().any(|name| partition(name) != partition(names[0])) {
            violations.push(Violation::RepeatedBlock { table: table.name.clone(), block, parts: names.iter().map(|name| name.to_string()).collect() });
        }
    }

    // An exclusive writer leaves the counter alone, so the counter binds only parts of an epoch that began
    // before it was last written. A part committed after the counter was read is newer than the reading.
    if let (Some(counter), Some(owner_version)) = (&parts.counter, owner_version)
        && counter.version > owner_version
    {
        for (name, entry) in &parts.entries {
            if entry.info.max_block > counter.value && entry.entry_version <= counter.version {
                violations.push(Violation::CounterBehind { table: table.name.clone(), counter: counter.value, part: name.clone() });
            }
        }
    }
}

async fn part_files(catalog: &Catalog, table: &Table, parts: &TableParts, violations: &mut Vec<Violation>) -> Result<(), Error> {
    for part in parts.active() {
        let name = part.name();
        let files = catalog.part_files(table, &name).await?;
        // A part with no directory left was removed while this check ran.
        if files.is_empty() && !exists(catalog, &format!("tree/{}/{name}", table.data_path())).await? {
            continue;
        }
        for file in PART_FILES {
            if !files.iter().any(|found| found == file) {
                violations.push(Violation::MissingPartFile { table: table.name.clone(), part: name.clone(), file: file.to_string() });
            }
        }
    }
    Ok(())
}

async fn exists(catalog: &Catalog, path: &str) -> Result<bool, Error> {
    match catalog.client().stat(path).await {
        Ok(_) => Ok(true),
        Err(error) if is_not_found(&error) => Ok(false),
        Err(error) => Err(error.into()),
    }
}

/// The blobs a disk's files name.
#[derive(Default)]
struct Named {
    /// Each blob's key, with the size its record gives and one file that names it. Hard links share blobs.
    blobs: HashMap<String, (u64, String)>,
    /// The blobs that hold a record too large for its file's value.
    records: Vec<String>,
    /// The newest version a listing of the walk reflects.
    version: Version,
}

/// Walks the disk's tree and collects the blobs its files name.
async fn named_blobs(catalog: &Catalog, s3: &S3Client, key_prefix: &str, report: &mut Report) -> Result<Named, Error> {
    let mut named = Named::default();
    let mut spilled: Vec<(String, String)> = Vec::new();

    let mut directories = vec!["tree".to_string()];
    while let Some(directory) = directories.pop() {
        let Some(listing) = catalog.list_with(&directory, &List { values: true, ..List::default() }).await? else { continue };
        named.version = named.version.max(listing.newest);
        for entry in &listing.entries {
            let path = format!("{directory}/{}", entry.name_lossy());
            if entry.kind == Kind::Dir {
                directories.push(path);
                continue;
            }
            report.files += 1;
            let Some(bytes) = entry.value.as_deref() else { continue };
            let record = Value::decode(bytes, &path).and_then(|value| match value.kind {
                ValueKind::Record => value.record(&path).map(Some),
                ValueKind::Spilled => {
                    spilled.push((path.clone(), String::from_utf8_lossy(&value.body).into_owned()));
                    Ok(None)
                }
                ValueKind::String | ValueKind::Tombstone => Ok(None),
            });
            match record {
                Ok(Some(record)) => expect_blobs(&record, key_prefix, &path, &mut named.blobs),
                Ok(None) => {}
                Err(error) => report.violations.push(Violation::UnreadableFile { path, message: error.to_string() }),
            }
        }
    }

    // A spilled record lives in a blob of its own, and names the file's blobs from there.
    for (path, key) in spilled {
        match s3.get(&key).await? {
            None => report.violations.push(Violation::MissingBlob { path, key }),
            Some(body) => {
                match Record::parse(&body, &path) {
                    Ok(record) => expect_blobs(&record, key_prefix, &path, &mut named.blobs),
                    Err(error) => report.violations.push(Violation::UnreadableFile { path, message: error.to_string() }),
                }
                named.records.push(key);
            }
        }
    }
    Ok(named)
}

/// Checks that S3 holds every blob the disk's files name, at the size their records give.
async fn blobs(catalog: &Catalog, tenant: &Tenant, report: &mut Report) -> Result<(), Error> {
    let s3 = S3Client::new(&tenant.s3)?;
    let named = named_blobs(catalog, &s3, &tenant.s3.key_prefix(), report).await?;

    report.blobs = named.blobs.len();
    let expected: Vec<(String, (u64, String))> = named.blobs.into_iter().collect();
    for chunk in expected.chunks(S3_CONCURRENCY) {
        let sizes = try_join_all(chunk.iter().map(|(key, _)| s3.size(key))).await?;
        for ((key, (recorded, path)), found) in chunk.iter().zip(sizes) {
            match found {
                None => report.violations.push(Violation::MissingBlob { path: path.clone(), key: key.clone() }),
                Some(found) if found != *recorded => {
                    report.violations.push(Violation::BlobSize { path: path.clone(), key: key.clone(), recorded: *recorded, found });
                }
                Some(_) => {}
            }
        }
    }
    report.violations.sort_by_key(|violation| violation.to_string());
    Ok(())
}

/// What S3 holds under a tenant's prefix, sorted by what still names it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Orphans {
    /// The objects under the tenant's prefix.
    pub stored: usize,
    /// Of those, the blobs a file of the disk names.
    pub named: usize,
    /// The blobs a freed file names. The disk's blob removal deletes them: a stock writer's on its own
    /// schedule, a worker's in `housekeep`.
    pub freed: usize,
    /// The objects nothing names: what a writer uploaded and never committed, because it died, lost a merge
    /// claim or was fenced. No writer removes them.
    pub orphans: Vec<Object>,
}

impl Orphans {
    /// The orphans' bytes.
    pub fn bytes(&self) -> u64 {
        self.orphans.iter().map(|object| object.size).sum()
    }
}

impl fmt::Display for Orphans {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} objects: {} named, {} freed, {} orphans of {} bytes", self.stored, self.named, self.freed, self.orphans.len(), self.bytes())
    }
}

/// Counts the blobs under a tenant's prefix that neither a file nor a freed file names.
///
/// It reads S3's listing, then the disk's tree, then InodeDB's free queue once the queue holds everything freed
/// up to the tree's version: a removed directory's files join the queue a moment after the removal. A blob that
/// has left S3 by the end was freed and deleted meanwhile, and is left out. The count is exact on a tenant no
/// writer is working on; beside a writer, a blob uploaded and not yet committed counts as an orphan. `key` needs
/// the `read` grant on the tenant's directory.
pub async fn orphans(tenant: &Tenant, key: &str) -> Result<Orphans, Error> {
    let catalog = Catalog::connect(tenant, key).await?;
    let s3 = S3Client::new(&tenant.s3)?;
    let key_prefix = tenant.s3.key_prefix();
    let stored = s3.list(&key_prefix).await?;

    let named = named_blobs(&catalog, &s3, &key_prefix, &mut Report::default()).await?;
    let freed = freed_blobs(&catalog, &s3, &key_prefix, named.version).await?;
    let named: HashSet<String> = named.blobs.into_keys().chain(named.records).collect();

    let mut found = Orphans::default();
    for object in stored {
        if named.contains(&object.key) {
            found.named += 1;
        } else if freed.contains(&object.key) {
            found.freed += 1;
        } else if s3.size(&object.key).await?.is_some() {
            found.orphans.push(object);
        } else {
            continue;
        }
        found.stored += 1;
    }
    found.orphans.sort_by(|a, b| a.key.cmp(&b.key));
    Ok(found)
}

/// The blobs that the files in InodeDB's free queue name, once the queue holds every file freed at or before
/// the version `through`.
async fn freed_blobs(catalog: &Catalog, s3: &S3Client, key_prefix: &str, through: Version) -> Result<HashSet<String>, Error> {
    const PAGE: u32 = 1_000;
    const QUEUE: &str = "the free queue";
    const CATCH_UP: Duration = Duration::from_secs(30);
    let started = Instant::now();
    loop {
        let mut keys = HashSet::new();
        let mut after: Option<Bytes> = None;
        let freed_through = loop {
            let page = catalog.client().list_freed(after.as_deref(), PAGE).await?;
            for entry in &page.entries {
                // A freed directory or string value names no blob.
                let Some(Ok(value)) = entry.value.as_deref().map(|bytes| Value::decode(bytes, QUEUE)) else { continue };
                match value.kind {
                    ValueKind::Record => {
                        if let Ok(record) = value.record(QUEUE) {
                            keys.extend(record.object_keys(key_prefix));
                        }
                    }
                    ValueKind::Spilled => {
                        let key = String::from_utf8_lossy(&value.body).into_owned();
                        if let Some(Ok(record)) = s3.get(&key).await?.map(|body| Record::parse(&body, QUEUE)) {
                            keys.extend(record.object_keys(key_prefix));
                        }
                        keys.insert(key);
                    }
                    ValueKind::String | ValueKind::Tombstone => {}
                }
            }
            match page.next {
                Some(next) => after = Some(next),
                None => break page.freed_through,
            }
        };
        if freed_through >= through {
            return Ok(keys);
        }
        if started.elapsed() > CATCH_UP {
            return Err(Error::Other(format!("the free queue holds what was freed through version {freed_through}, short of {through}, after {CATCH_UP:?}")));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn expect_blobs(record: &Record, key_prefix: &str, path: &str, expected: &mut HashMap<String, (u64, String)>) {
    for (blob, key) in record.blobs.iter().zip(record.object_keys(key_prefix)) {
        expected.entry(key).or_insert_with(|| (blob.size, path.to_string()));
    }
}

/// Has a fresh stock exclusive writer load the disk, and reports what it rejects: parts it sets aside as
/// broken, and tables that fail `CHECK TABLE`.
///
/// Starting the writer fences every other writer of the disk. `key` needs the `write` grant.
pub async fn stock_load(env: &StockEnv, tenant: &Tenant, key: &str) -> Result<Vec<Violation>, Error> {
    let tables = Catalog::connect(tenant, key).await?.all_tables().await?;
    let writer = Exclusive::start(env, tenant, key).await?;
    let mut violations = Vec::new();
    let mut failure = None;
    for table in tables.iter().filter(|table| table.is_merge_tree()) {
        if let Err(error) = stock_check(&writer, &table.name, &mut violations).await {
            failure = Some(error);
            break;
        }
    }
    let stopped = writer.stop().await;
    match failure {
        Some(error) => Err(error),
        None => stopped.map(|()| violations),
    }
}

async fn stock_check(writer: &Exclusive, table: &TableName, violations: &mut Vec<Violation>) -> Result<(), Error> {
    let detached = writer
        .execute(&format!(
            "SELECT name FROM system.detached_parts WHERE database = {} AND table = {} ORDER BY name FORMAT TSV",
            quote_string(&table.database),
            quote_string(&table.table)
        ))
        .await?;
    for part in detached.lines().filter(|line| !line.is_empty()) {
        violations.push(Violation::DetachedPart { table: table.clone(), part: part.to_string() });
    }
    match writer.server().query_with(&format!("CHECK TABLE {}", table.quoted()), &[("check_query_single_value_result", "1")]).await {
        Ok(answer) if answer.trim() == "1" => {}
        Ok(answer) => violations.push(Violation::CheckTableFailed { table: table.clone(), message: format!("answered `{}`", answer.trim()) }),
        Err(Error::ClickHouse { message, .. }) => violations.push(Violation::CheckTableFailed { table: table.clone(), message }),
        Err(error) => return Err(error),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{Counter, PartEntry, TableKind};
    use crate::part::{PartInfo, PartSet};

    fn table() -> Table {
        Table {
            name: TableName::new("d", "t"),
            uuid: "7f59e238-897d-4481-9ad5-b21a0d56af5e".into(),
            kind: TableKind::Table,
            engine: Some("MergeTree".into()),
            definition: String::new(),
            definition_version: 1,
        }
    }

    /// Parts by name, each committed at the version given.
    fn parts(names: &[(&str, u64)], counter: Option<Counter>) -> TableParts {
        let entries: BTreeMap<String, PartEntry> = names
            .iter()
            .map(|(name, version)| (name.to_string(), PartEntry { info: PartInfo::parse(name).unwrap(), mtime_ms: 0, entry_version: *version }))
            .collect();
        let set = PartSet::of(entries.values().map(|entry| entry.info.clone()));
        TableParts { set, entries, counter, ..TableParts::default() }
    }

    fn violations(parts: &TableParts, owner_version: Option<u64>) -> Vec<String> {
        let mut found = Vec::new();
        part_invariants(&table(), parts, owner_version, &mut found);
        found.iter().map(Violation::to_string).collect()
    }

    #[test]
    fn a_healthy_table_breaks_nothing() {
        let healthy = parts(
            &[("a_1_1_0", 10), ("a_2_2_0", 11), ("a_1_2_1", 12), ("b_3_3_0", 13), ("a_4_4_0", 14), ("a_4_4_0_9", 20), ("a_4_4_1_9", 21)],
            Some(Counter { value: 4, version: 14 }),
        );
        assert_eq!(violations(&healthy, Some(5)), Vec::<String>::new());
    }

    #[test]
    fn intersecting_parts_are_reported() {
        let found = violations(&parts(&[("a_1_3_1", 1), ("a_3_4_1", 2)], None), None);
        assert_eq!(found, ["d.t: parts a_1_3_1 and a_3_4_1 intersect"]);
    }

    #[test]
    fn a_block_number_in_two_partitions_is_reported() {
        let found = violations(&parts(&[("a_5_5_0", 1), ("b_5_5_0", 2), ("a_5_5_0_7", 3), ("a_6_6_0", 4)], None), None);
        assert_eq!(found, ["d.t: block 5 names parts in two partitions: a_5_5_0, a_5_5_0_7, b_5_5_0"]);
    }

    #[test]
    fn a_counter_behind_its_parts_is_reported_within_one_epoch() {
        let behind = parts(&[("a_1_1_0", 10), ("a_7_7_0", 11)], Some(Counter { value: 3, version: 12 }));
        assert_eq!(violations(&behind, Some(5)), ["d.t: part a_7_7_0 is numbered above the block counter, 3"]);
        // An exclusive epoch began after the counter was written, so the counter may trail.
        assert!(violations(&behind, Some(15)).is_empty());
        // The part was committed after the counter was read.
        let newer = parts(&[("a_1_1_0", 10), ("a_7_7_0", 13)], Some(Counter { value: 3, version: 12 }));
        assert!(violations(&newer, Some(5)).is_empty());
    }

    #[test]
    fn a_report_lists_its_violations() {
        let mut report = Report { tables: 1, parts: 2, files: 9, blobs: 3, ..Report::default() };
        assert!(report.is_clean());
        assert_eq!(report.to_string(), "1 tables, 2 parts, 9 files, 3 blobs: clean");
        report.violations.push(Violation::MissingBlob { path: "tree/p/data.bin".into(), key: "t/abc".into() });
        assert!(!report.is_clean());
        assert_eq!(report.to_string(), "1 tables, 2 parts, 9 files, 3 blobs: 1 violations\n  tree/p/data.bin: S3 holds no blob t/abc");
    }
}
