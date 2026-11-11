//! The [`Writer`] trait: what every way of writing a tenant's disk offers.

use std::fmt;
use std::future::Future;

use bytes::Bytes;

use crate::{Error, PartInfo};

/// A table, by database and name.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TableName {
    pub database: String,
    pub table: String,
}

impl TableName {
    pub fn new(database: impl Into<String>, table: impl Into<String>) -> TableName {
        TableName { database: database.into(), table: table.into() }
    }

    /// The name as a ClickHouse statement spells it: both parts quoted as identifiers.
    pub fn quoted(&self) -> String {
        format!("{}.{}", crate::stock::quote_identifier(&self.database), crate::stock::quote_identifier(&self.table))
    }
}

impl fmt::Display for TableName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.database, self.table)
    }
}

/// How an operation ended. `Err` from the operation means it failed outright, and the caller cannot tell what,
/// if anything, it committed; every outcome here is certain.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome<T> {
    /// The operation committed its work.
    Committed(T),
    /// An insert found every block of its batch already committed under the same token, and wrote nothing.
    Duplicate,
    /// A table's definition changed since the writer loaded it. Nothing was committed; a writer that loads the
    /// new definition can retry.
    SchemaChanged,
    /// An exclusive writer took the disk, or the writer's key was revoked. Nothing was committed.
    Fenced,
    /// A merge lost a claim on a source part, or a source part is gone. Nothing was committed.
    ClaimLost,
    /// The writer declined the operation, for the reason given. Nothing was committed.
    Refused(String),
}

impl<T> Outcome<T> {
    /// What the operation committed, when it did.
    pub fn committed(self) -> Option<T> {
        match self {
            Outcome::Committed(value) => Some(value),
            _ => None,
        }
    }

    pub fn is_committed(&self) -> bool {
        matches!(self, Outcome::Committed(_))
    }

    /// The outcome's name, for logs and reports.
    pub fn name(&self) -> &'static str {
        match self {
            Outcome::Committed(_) => "committed",
            Outcome::Duplicate => "duplicate",
            Outcome::SchemaChanged => "schema changed",
            Outcome::Fenced => "fenced",
            Outcome::ClaimLost => "claim lost",
            Outcome::Refused(_) => "refused",
        }
    }
}

/// One batch of rows for a table.
#[derive(Clone, Debug)]
pub struct Insert<'a> {
    pub table: &'a TableName,
    /// A ClickHouse input format, as `JSONEachRow`.
    pub format: &'a str,
    /// Names the batch. A second insert of the batch under the same token writes nothing.
    pub token: &'a str,
    pub data: Bytes,
}

/// A part an operation committed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommittedPart {
    pub table: TableName,
    pub part: PartInfo,
    pub rows: u64,
    /// The part's size on the disk.
    pub bytes: u64,
}

/// What an insert committed.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Inserted {
    /// The parts written, for the table and for each view's target, when the writer reports them. A stock
    /// server names none.
    pub parts: Option<Vec<CommittedPart>>,
    /// How many commits found their block number taken by another writer's part and went again with the next.
    pub counter_conflicts: u64,
}

/// How much a plan may propose.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Budget {
    /// The most merges to propose.
    pub max_merges: usize,
}

impl Default for Budget {
    fn default() -> Budget {
        Budget { max_merges: 16 }
    }
}

/// A merge by its sources: parts of one partition, adjacent in block order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MergeCandidate {
    pub partition_id: String,
    pub sources: Vec<PartInfo>,
}

/// A merge a plan proposes, with the size of its sources: what a scheduler reads to choose the worker that
/// runs it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlannedMerge {
    pub candidate: MergeCandidate,
    /// The sources' rows.
    pub rows: u64,
    /// The sources' bytes on disk.
    pub bytes: u64,
}

/// What a merge committed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Merged {
    /// The part that now holds the sources' rows.
    pub part: PartInfo,
}

/// A covered part that housekeeping left in place.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Remaining {
    pub part: PartInfo,
    pub reason: String,
}

/// What housekeeping did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Housekept {
    /// The covered parts removed.
    pub removed: Vec<PartInfo>,
    /// The covered parts still on the disk, each with the reason it stays.
    pub remaining: Vec<Remaining>,
    /// How many temporary directories went: what inserts and merges whose workers died left in the table, once
    /// older than the table's `temporary_directories_lifetime`.
    pub temporary_directories: u64,
    /// How many blobs went from S3: those of files the tenant's disk had freed, in this operation or before it.
    /// InodeDB holds a freed file's blobs back while a reader that started before the file was freed still
    /// holds its lease, so the blobs of the parts this call removed may go in a later call.
    pub blobs_removed: u64,
    /// Whether the call stopped removing blobs while it still found freed files, as after a backlog of some
    /// thousands of removed parts. Another call takes up the rest.
    pub blobs_remain: bool,
}

/// Writes one tenant's disk.
///
/// Every method is safe to call again after an `Err` or a lost answer: a token makes an insert exact, claims
/// make a merge exact, and housekeeping removes only what a committed part covers.
pub trait Writer {
    /// Inserts a batch into a table and into every view that reads from it.
    fn insert(&self, insert: Insert<'_>) -> impl Future<Output = Result<Outcome<Inserted>, Error>> + Send;

    /// Proposes merges for a table, and runs none.
    fn plan(&self, table: &TableName, budget: &Budget) -> impl Future<Output = Result<Vec<PlannedMerge>, Error>> + Send;

    /// Merges a candidate's sources into one part.
    fn merge(&self, table: &TableName, candidate: &MergeCandidate) -> impl Future<Output = Result<Outcome<Merged>, Error>> + Send;

    /// Removes what merges left behind: covered parts past the grace period, with their claims.
    fn housekeep(&self, table: &TableName) -> impl Future<Output = Result<Outcome<Housekept>, Error>> + Send;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_names_quote_each_part() {
        let name = TableName::new("d", "ev`ents");
        assert_eq!(name.quoted(), "`d`.`ev\\`ents`");
        assert_eq!(name.to_string(), "d.ev`ents");
    }

    #[test]
    fn outcomes_name_themselves_and_yield_what_they_committed() {
        assert_eq!(Outcome::Committed(7).committed(), Some(7));
        assert_eq!(Outcome::<u8>::Duplicate.committed(), None);
        let all: [Outcome<u8>; 6] =
            [Outcome::Committed(1), Outcome::Duplicate, Outcome::SchemaChanged, Outcome::Fenced, Outcome::ClaimLost, Outcome::Refused("why".into())];
        let names: Vec<&str> = all.iter().map(Outcome::name).collect();
        assert_eq!(names, ["committed", "duplicate", "schema changed", "fenced", "claim lost", "refused"]);
        assert_eq!(all.iter().filter(|outcome| outcome.is_committed()).count(), 1);
    }
}
