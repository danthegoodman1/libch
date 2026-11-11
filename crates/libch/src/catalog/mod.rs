//! A tenant's catalog, read from InodeDB: databases, tables, parts and coordination files.
//!
//! A writer keeps its database definitions on the tenant's disk, so InodeDB holds everything a reader of the
//! catalog needs, and reading it asks nothing of any ClickHouse server:
//!
//! ```text
//! owner                                    the writer epoch: `exclusive {id}` or `shared {id}`
//! tree/metadata/{database}.sql             a database's definition, with its UUID
//! tree/store/{uuid[..3]}/{uuid}/{table}.sql   a table's definition, in its database's directory
//! tree/store/{uuid[..3]}/{uuid}/{part}/    a table's parts, in the table's own directory
//! coord/store/{uuid[..3]}/{uuid}/          the table's coordination files
//! ```

mod coord;

use std::collections::BTreeMap;

use inodedb::{Client, Entry, FsError, Kind, List, Listing, Version};

pub use coord::{Claim, ClaimState, Coordination, Counter, Owner, OwnerMode, Token};

use crate::disk::Value;
use crate::part::{PartInfo, PartSet};
use crate::writer::TableName;
use crate::{Error, Tenant};

/// The databases every server creates for itself. A tenant's catalog leaves them out.
const SERVER_DATABASES: [&str; 3] = ["system", "INFORMATION_SCHEMA", "information_schema"];

/// How many times a listing that changed underfoot is read again.
const LISTING_ATTEMPTS: usize = 8;

/// A database's definition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Database {
    pub name: String,
    pub uuid: String,
    /// The `ATTACH DATABASE` statement the writer stored.
    pub definition: String,
}

impl Database {
    /// The directory that holds the database's table definitions, relative to the disk.
    pub fn path(&self) -> String {
        store_path(&self.uuid)
    }
}

/// What kind of object a table definition creates.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TableKind {
    Table,
    MaterializedView,
    View,
    Dictionary,
    Other,
}

/// A table's definition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Table {
    pub name: TableName,
    pub uuid: String,
    pub kind: TableKind,
    /// The storage engine the definition names, as `MergeTree`.
    pub engine: Option<String>,
    /// The `ATTACH` statement the writer stored.
    pub definition: String,
    /// The version of the definition's file. A shared writer's commits require the version it loaded.
    pub definition_version: Version,
}

impl Table {
    /// The directory that holds the table's parts, relative to the disk.
    pub fn data_path(&self) -> String {
        store_path(&self.uuid)
    }

    /// Whether the table keeps parts: any engine of the MergeTree family.
    pub fn is_merge_tree(&self) -> bool {
        self.kind == TableKind::Table && self.engine.as_deref().is_some_and(|engine| engine.ends_with("MergeTree"))
    }
}

fn store_path(uuid: &str) -> String {
    format!("store/{}/{uuid}", &uuid[..3.min(uuid.len())])
}

/// One part's directory.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartEntry {
    pub info: PartInfo,
    /// When the part was committed, in milliseconds since the epoch.
    pub mtime_ms: u64,
    /// The version of the commit that put the directory under its name.
    pub entry_version: Version,
}

/// What a table's directory holds.
#[derive(Clone, Debug, Default)]
pub struct TableParts {
    /// The parts, sorted into active and covered.
    pub set: PartSet,
    /// Every part directory by name.
    pub entries: BTreeMap<String, PartEntry>,
    /// The versions of the mutation entries, ascending.
    pub mutations: Vec<i64>,
    /// Every other name in the directory, such as `detached` and temporary directories.
    pub other: Vec<String>,
    /// The block counter as read before the listing. The listing holds every part numbered at or below it, or
    /// a part that covers it.
    pub counter: Option<Counter>,
    /// The version the listing reflects.
    pub version: Version,
}

impl TableParts {
    pub fn active(&self) -> &[PartInfo] {
        &self.set.active
    }

    /// The highest block number any part or mutation entry holds.
    pub fn max_block(&self) -> Option<i64> {
        let parts = self.entries.values().map(|entry| entry.info.max_block);
        parts.chain(self.mutations.iter().copied()).max()
    }
}

/// Reads a tenant's catalog under one key.
#[derive(Clone)]
pub struct Catalog {
    client: Client,
}

impl std::fmt::Debug for Catalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Catalog").field("store", &self.client.endpoint()).finish()
    }
}

impl Catalog {
    /// A catalog over a client whose key is rooted at the tenant's directory.
    pub fn new(client: Client) -> Catalog {
        Catalog { client }
    }

    pub async fn connect(tenant: &Tenant, key: &str) -> Result<Catalog, Error> {
        Ok(Catalog::new(tenant.connect(key).await?))
    }

    pub fn client(&self) -> &Client {
        &self.client
    }

    /// The disk's writer epoch, or `None` before any writer has started on it.
    pub async fn owner(&self) -> Result<Option<Owner>, Error> {
        match self.client.read("owner").await {
            Ok(value) => Ok(Some(Owner::parse(&value.value, value.inode.version)?)),
            Err(error) if is_not_found(&error) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    /// The tenant's databases, by name.
    pub async fn databases(&self) -> Result<Vec<Database>, Error> {
        let Some(listing) = self.list("tree/metadata", true).await? else { return Ok(Vec::new()) };
        let mut databases = Vec::new();
        for entry in &listing.entries {
            let file_name = entry.name_lossy();
            let Some(escaped) = file_name.strip_suffix(".sql") else { continue };
            if entry.kind != Kind::File {
                continue;
            }
            let name = unescape_file_name(escaped);
            if SERVER_DATABASES.contains(&name.as_str()) {
                continue;
            }
            let path = format!("tree/metadata/{file_name}");
            let definition = definition_text(entry, &path)?;
            let uuid = definition_uuid(&definition).ok_or_else(|| Error::format(&path, "holds a database definition without a UUID"))?;
            databases.push(Database { name, uuid, definition });
        }
        databases.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(databases)
    }

    pub async fn database(&self, name: &str) -> Result<Option<Database>, Error> {
        Ok(self.databases().await?.into_iter().find(|database| database.name == name))
    }

    /// A database's tables, views and dictionaries, by name.
    pub async fn tables(&self, database: &Database) -> Result<Vec<Table>, Error> {
        let directory = format!("tree/{}", database.path());
        let Some(listing) = self.list(&directory, true).await? else { return Ok(Vec::new()) };
        let mut tables = Vec::new();
        for entry in &listing.entries {
            let file_name = entry.name_lossy();
            let Some(escaped) = file_name.strip_suffix(".sql") else { continue };
            if entry.kind != Kind::File {
                continue;
            }
            let path = format!("{directory}/{file_name}");
            let definition = definition_text(entry, &path)?;
            let uuid = definition_uuid(&definition).ok_or_else(|| Error::format(&path, "holds a table definition without a UUID"))?;
            let inode = entry.inode.as_ref().ok_or_else(|| Error::format(&path, "came back from the listing without its attributes"))?;
            tables.push(Table {
                name: TableName::new(database.name.clone(), unescape_file_name(escaped)),
                uuid,
                kind: definition_kind(&definition),
                engine: definition_engine(&definition),
                definition,
                definition_version: inode.version,
            });
        }
        tables.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(tables)
    }

    pub async fn table(&self, name: &TableName) -> Result<Option<Table>, Error> {
        let Some(database) = self.database(&name.database).await? else { return Ok(None) };
        Ok(self.tables(&database).await?.into_iter().find(|table| table.name == *name))
    }

    /// Every table of every database.
    pub async fn all_tables(&self) -> Result<Vec<Table>, Error> {
        let mut tables = Vec::new();
        for database in self.databases().await? {
            tables.extend(self.tables(&database).await?);
        }
        Ok(tables)
    }

    /// What a table's directory holds: its parts, mutation entries and everything else.
    ///
    /// The block counter is read first, as a shared writer's refresh reads it, so the listing is complete for
    /// every block at or below the counter.
    pub async fn parts(&self, table: &Table) -> Result<TableParts, Error> {
        let counter = self.counter(table).await?;
        let directory = format!("tree/{}", table.data_path());
        let Some(listing) = self.list_with(&directory, &List { attrs: true, ..List::default() }).await? else {
            return Ok(TableParts { counter, ..TableParts::default() });
        };
        let mut parts = TableParts { counter, version: listing.version, ..TableParts::default() };
        for entry in &listing.entries {
            let name = entry.name_lossy();
            if entry.kind == Kind::Dir {
                if let Some(info) = PartInfo::parse(&name) {
                    let mtime_ms = entry.inode.as_ref().map_or(0, |inode| inode.mtime);
                    parts.entries.insert(name, PartEntry { info, mtime_ms, entry_version: entry.entry_version });
                    continue;
                }
            } else if let Some(version) = name.strip_prefix("mutation_").and_then(|rest| rest.strip_suffix(".txt")).and_then(|version| version.parse().ok()) {
                parts.mutations.push(version);
                continue;
            }
            parts.other.push(name);
        }
        parts.mutations.sort_unstable();
        parts.set = PartSet::of(parts.entries.values().map(|entry| entry.info.clone()));
        Ok(parts)
    }

    /// The names in one part's directory.
    pub async fn part_files(&self, table: &Table, part: &str) -> Result<Vec<String>, Error> {
        let directory = format!("tree/{}/{part}", table.data_path());
        Ok(self.list(&directory, false).await?.map(|listing| listing.entries.iter().map(Entry::name_lossy).collect()).unwrap_or_default())
    }

    /// The table's block counter, or `None` before any shared writer has loaded the table.
    pub async fn counter(&self, table: &Table) -> Result<Option<Counter>, Error> {
        let path = format!("coord/{}/block_number", table.data_path());
        match self.client.read(path.as_str()).await {
            Ok(value) => Ok(Some(Counter::parse(&value.value, value.inode.version, &path)?)),
            Err(error) if is_not_found(&error) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    /// The table's coordination files: its counter, merge claims and deduplication tokens.
    pub async fn coordination(&self, table: &Table) -> Result<Coordination, Error> {
        let directory = format!("coord/{}", table.data_path());
        let mut coordination = Coordination { counter: self.counter(table).await?, ..Coordination::default() };
        if let Some(listing) = self.list(&format!("{directory}/claims"), true).await? {
            for entry in listing.entries.iter().filter(|entry| entry.kind == Kind::File) {
                let name = entry.name_lossy();
                let path = format!("{directory}/claims/{name}");
                coordination.claims.push(Claim::parse(&name, entry_value(entry, &path)?, entry_version(entry), entry_mtime(entry), &path)?);
            }
        }
        if let Some(listing) = self.list(&format!("{directory}/tokens"), true).await? {
            for entry in listing.entries.iter().filter(|entry| entry.kind == Kind::File) {
                let name = entry.name_lossy();
                let path = format!("{directory}/tokens/{name}");
                let part = String::from_utf8_lossy(entry_value(entry, &path)?).into_owned();
                coordination.tokens.push(Token { block_id: name, part, version: entry_version(entry), mtime_ms: entry_mtime(entry) });
            }
        }
        Ok(coordination)
    }

    /// The table paths that have a directory under `coord/`, relative to the disk, as `store/abc/{uuid}`.
    pub async fn coordinated_paths(&self) -> Result<Vec<String>, Error> {
        let mut paths = Vec::new();
        let Some(prefixes) = self.list("coord/store", false).await? else { return Ok(paths) };
        for prefix in prefixes.entries.iter().filter(|entry| entry.kind == Kind::Dir) {
            let prefix = prefix.name_lossy();
            let Some(tables) = self.list(&format!("coord/store/{prefix}"), false).await? else { continue };
            paths.extend(tables.entries.iter().filter(|entry| entry.kind == Kind::Dir).map(|entry| format!("store/{prefix}/{}", entry.name_lossy())));
        }
        Ok(paths)
    }

    /// Lists a directory, with values when asked. `None` when the directory is absent.
    pub async fn list(&self, path: &str, values: bool) -> Result<Option<Listing>, Error> {
        self.list_with(path, &List { attrs: values, values, ..List::default() }).await
    }

    /// Lists a directory at one version. `None` when the directory is absent.
    ///
    /// InodeDB pages a long listing, and later pages can reflect later versions. A directory's version changes
    /// whenever an entry is added, removed or renamed, so a listing whose directory still has the version of
    /// its first page reflects one moment. A listing that spans a change is read again.
    pub async fn list_with(&self, path: &str, options: &List) -> Result<Option<Listing>, Error> {
        let mut last = None;
        for _ in 0..LISTING_ATTEMPTS {
            let listing = match self.client.list_with(path, options).await {
                Ok(listing) => listing,
                Err(error) if is_not_found(&error) => return Ok(None),
                Err(error) => return Err(error.into()),
            };
            if listing.newest == listing.version {
                return Ok(Some(listing));
            }
            match self.client.stat(listing.dir.clone()).await {
                Ok(inode) if inode.version <= listing.version => return Ok(Some(listing)),
                Ok(_) => last = Some(listing),
                Err(error) if is_not_found(&error) => return Ok(None),
                Err(error) => return Err(error.into()),
            }
        }
        match last {
            Some(_) => Err(Error::Other(format!("`{path}` kept changing through {LISTING_ATTEMPTS} listings"))),
            None => Ok(None),
        }
    }
}

pub(crate) fn is_not_found(error: &inodedb::Error) -> bool {
    matches!(error, inodedb::Error::Fs(FsError::NotFound | FsError::NotDir))
}

fn entry_value<'a>(entry: &'a Entry, path: &str) -> Result<&'a [u8], Error> {
    entry.value.as_deref().ok_or_else(|| Error::format(path, "came back from the listing without its value"))
}

fn entry_version(entry: &Entry) -> Version {
    entry.inode.as_ref().map_or(entry.entry_version, |inode| inode.version)
}

fn entry_mtime(entry: &Entry) -> u64 {
    entry.inode.as_ref().map_or(0, |inode| inode.mtime)
}

/// The text of a definition file, which the disk keeps inline in the file's value.
fn definition_text(entry: &Entry, path: &str) -> Result<String, Error> {
    let value = Value::decode(entry_value(entry, path)?, path)?;
    let bytes = value.inline_bytes(path)?.ok_or_else(|| Error::format(path, "keeps its bytes in blobs, where a definition is expected inline"))?;
    String::from_utf8(bytes).map_err(|_| Error::format(path, "holds a definition that is not UTF-8"))
}

/// The UUID a definition gives its object: the one after the first `UUID` keyword.
fn definition_uuid(definition: &str) -> Option<String> {
    let after = definition.split_once(" UUID '")?.1;
    let uuid = after.get(..36)?;
    let well_formed = uuid.bytes().enumerate().all(|(at, byte)| if matches!(at, 8 | 13 | 18 | 23) { byte == b'-' } else { byte.is_ascii_hexdigit() });
    (well_formed && after[36..].starts_with('\'')).then(|| uuid.to_string())
}

fn definition_kind(definition: &str) -> TableKind {
    let Some(rest) = definition.strip_prefix("ATTACH ") else { return TableKind::Other };
    if rest.starts_with("TABLE ") {
        TableKind::Table
    } else if rest.starts_with("MATERIALIZED VIEW ") {
        TableKind::MaterializedView
    } else if rest.starts_with("VIEW ") {
        TableKind::View
    } else if rest.starts_with("DICTIONARY ") {
        TableKind::Dictionary
    } else {
        TableKind::Other
    }
}

/// The engine a definition names on its `ENGINE =` line.
fn definition_engine(definition: &str) -> Option<String> {
    let after = definition.split("\nENGINE = ").nth(1)?;
    // ClickHouse quotes an engine whose name is a keyword, as `Null`.
    let after = after.strip_prefix('`').unwrap_or(after);
    let engine: String = after.chars().take_while(|c| c.is_ascii_alphanumeric() || *c == '_').collect();
    (!engine.is_empty()).then_some(engine)
}

/// Reverses ClickHouse's `escapeForFileName`, which writes every byte outside `[A-Za-z0-9_]` as `%XX`.
fn unescape_file_name(escaped: &str) -> String {
    let bytes = escaped.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut at = 0;
    while at < bytes.len() {
        if bytes[at] == b'%' && at + 2 < bytes.len() {
            let hex = std::str::from_utf8(&bytes[at + 1..at + 3]).ok().and_then(|hex| u8::from_str_radix(hex, 16).ok());
            if let Some(byte) = hex {
                out.push(byte);
                at += 3;
                continue;
            }
        }
        out.push(bytes[at]);
        at += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    const EVENTS: &str = "ATTACH TABLE _ UUID '7f59e238-897d-4481-9ad5-b21a0d56af5e'\n(\n    `ts` DateTime,\n    `v` UInt64\n)\nENGINE = MergeTree\nPARTITION BY toYYYYMM(ts)\nORDER BY ts\nSETTINGS index_granularity = 8192\n";
    const VIEW: &str =
        "ATTACH MATERIALIZED VIEW _ UUID '5f7ed82b-2877-4202-8cb0-571051391452' TO d.agg\n(\n    `day` Date\n)\nAS SELECT toDate(ts) AS day\nFROM d.events\n";

    #[test]
    fn definitions_give_their_uuid_kind_and_engine() {
        assert_eq!(definition_uuid(EVENTS).as_deref(), Some("7f59e238-897d-4481-9ad5-b21a0d56af5e"));
        assert_eq!(definition_kind(EVENTS), TableKind::Table);
        assert_eq!(definition_engine(EVENTS).as_deref(), Some("MergeTree"));

        assert_eq!(definition_uuid(VIEW).as_deref(), Some("5f7ed82b-2877-4202-8cb0-571051391452"));
        assert_eq!(definition_kind(VIEW), TableKind::MaterializedView);
        assert_eq!(definition_engine(VIEW), None);

        let database = "ATTACH DATABASE _ UUID 'b3412cbc-447f-4b3f-8e2c-d96919acf4db'\nENGINE = Atomic\n";
        assert_eq!(definition_uuid(database).as_deref(), Some("b3412cbc-447f-4b3f-8e2c-d96919acf4db"));
        assert_eq!(definition_engine(database).as_deref(), Some("Atomic"));

        assert_eq!(definition_engine("ATTACH TABLE _ UUID 'x'\n(\n    `v` UInt64\n)\nENGINE = `Null`\n").as_deref(), Some("Null"));
        assert_eq!(definition_uuid("ATTACH TABLE _ UUID 'not-a-uuid'"), None);
        assert_eq!(definition_uuid("ATTACH TABLE _ (a UInt8) ENGINE = Memory"), None);
        assert_eq!(definition_kind("CREATE TABLE t"), TableKind::Other);
        assert_eq!(definition_kind("ATTACH DICTIONARY _ UUID"), TableKind::Dictionary);
        assert_eq!(definition_kind("ATTACH VIEW _ UUID"), TableKind::View);
    }

    #[test]
    fn engines_of_the_merge_tree_family_keep_parts() {
        let table = |kind, engine: Option<&str>| Table {
            name: TableName::new("d", "t"),
            uuid: "7f59e238-897d-4481-9ad5-b21a0d56af5e".into(),
            kind,
            engine: engine.map(str::to_string),
            definition: String::new(),
            definition_version: 1,
        };
        assert!(table(TableKind::Table, Some("MergeTree")).is_merge_tree());
        assert!(table(TableKind::Table, Some("AggregatingMergeTree")).is_merge_tree());
        assert!(!table(TableKind::Table, Some("Null")).is_merge_tree());
        assert!(!table(TableKind::MaterializedView, None).is_merge_tree());
        assert_eq!(table(TableKind::Table, None).data_path(), "store/7f5/7f59e238-897d-4481-9ad5-b21a0d56af5e");
    }

    #[test]
    fn file_names_unescape() {
        assert_eq!(unescape_file_name("events"), "events");
        assert_eq!(unescape_file_name("my%2Dtable%20one"), "my-table one");
        assert_eq!(unescape_file_name("caf%C3%A9"), "café");
        assert_eq!(unescape_file_name("100%"), "100%");
        assert_eq!(unescape_file_name("a%zzb"), "a%zzb");
    }
}
