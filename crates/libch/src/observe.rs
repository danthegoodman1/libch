//! What libch reports about each operation as it ends, for a service to count, bill and alert on.
//!
//! An [`Observer`] given to a [`Pool`](crate::pool::Pool) or an [`OpWriter`](crate::worker::OpWriter) hears of
//! every operation once, when it ends, however it ends: committed, turned back, failed in the worker, failed
//! before a worker answered, or dropped by its caller. Its [`Report`] names the tenant, so a service sums a tenant's usage from
//! the reports; the `metrics` feature adds `metrics::Metrics`, an observer that records the
//! reports as metrics with labels a metrics backend can hold.

use std::time::Duration;

use crate::worker::Usage;
use crate::writer::TableName;

/// Hears of each operation as it ends.
///
/// The writer calls it on the task that ran the operation, after the worker's answer and before the caller
/// gets the result, so it returns quickly: a service that ships reports elsewhere queues them.
pub trait Observer: Send + Sync {
    fn operation(&self, report: &Report);
}

/// One operation, as it ended.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Report {
    pub kind: Kind,
    /// The tenant's InodeDB store, as its [`Tenant`](crate::Tenant) names it.
    pub store: String,
    /// The tenant's directory in the store.
    pub tenant: String,
    /// The table the operation names. `describe` and `share` name none.
    pub table: Option<TableName>,
    pub ended: Ended,
    /// From the call to its end.
    pub elapsed: Duration,
    /// How long the operation waited for a turn in its pool.
    pub queued: Duration,
    /// Whether a worker the pool started ahead of the operation served it. An operation without one started its
    /// own worker, and its time includes the engine's start.
    pub waiting_worker: bool,
    /// What the worker counted for this operation, when it answered. [`Usage::operations`] is 1.
    pub usage: Option<Usage>,
    pub work: Work,
}

/// What an operation is.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Kind {
    Insert,
    Plan,
    Merge,
    Housekeep,
    Describe,
    Share,
}

impl Kind {
    pub const ALL: [Kind; 6] = [Kind::Insert, Kind::Plan, Kind::Merge, Kind::Housekeep, Kind::Describe, Kind::Share];

    /// The operation's name in the protocol, as `insert`.
    pub fn name(self) -> &'static str {
        match self {
            Kind::Insert => "insert",
            Kind::Plan => "plan",
            Kind::Merge => "merge",
            Kind::Housekeep => "housekeep",
            Kind::Describe => "describe",
            Kind::Share => "share",
        }
    }
}

/// How an operation ended: one of the [`Outcome`](crate::Outcome)s, or a failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Ended {
    /// The operation did its work: committed it, or for `plan` and `describe`, answered.
    Committed,
    Duplicate,
    SchemaChanged,
    Fenced,
    ClaimLost,
    Refused,
    /// The call returned an error, and the caller cannot tell what, if anything, the operation committed.
    Failed(Failure),
}

impl Ended {
    /// A name in lower case with underscores, as a metric label takes it: `committed`, `fenced`, or the failure's
    /// name, as `deadline`.
    pub fn name(self) -> &'static str {
        match self {
            Ended::Committed => "committed",
            Ended::Duplicate => "duplicate",
            Ended::SchemaChanged => "schema_changed",
            Ended::Fenced => "fenced",
            Ended::ClaimLost => "claim_lost",
            Ended::Refused => "refused",
            Ended::Failed(failure) => failure.name(),
        }
    }

    pub fn is_failed(self) -> bool {
        matches!(self, Ended::Failed(_))
    }
}

/// Why an operation's call returned an error.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Failure {
    /// No worker started for it: its directory or its process could not be made.
    Start,
    /// Its pool was closed.
    Closed,
    /// It passed its deadline, and its worker was killed.
    Deadline,
    /// Its worker, or a forking worker's child, ended without an answer: killed, or exited.
    Exited,
    /// The worker answered with something the protocol does not allow.
    Protocol,
    /// The worker answered that the operation failed, with ClickHouse's exception code when there was one.
    Exception { code: Option<i32> },
    /// The caller dropped the call before it ended. The call's worker, when it had one, was killed.
    Cancelled,
}

impl Failure {
    pub fn name(self) -> &'static str {
        match self {
            Failure::Start => "start_failed",
            Failure::Closed => "closed",
            Failure::Deadline => "deadline",
            Failure::Exited => "exited",
            Failure::Protocol => "protocol",
            Failure::Exception { .. } => "exception",
            Failure::Cancelled => "cancelled",
        }
    }
}

/// What an operation did, by the worker's answer. Each figure counts for the operations that do that work, and
/// is zero for the rest and for an operation that committed nothing.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Work {
    /// The batch an insert sent, in bytes, whatever became of it.
    pub batch_bytes: u64,
    /// The rows an insert committed to its table.
    pub rows_inserted: u64,
    /// The parts an insert committed, to its table and to its views' targets, or the part a merge committed.
    pub parts_committed: u64,
    /// The rows of those parts, and their bytes on disk.
    pub part_rows: u64,
    pub part_bytes: u64,
    /// The parts a merge took as its sources.
    pub parts_merged: u64,
    /// The merges a plan proposed.
    pub merges_planned: u64,
    /// The covered parts housekeeping removed, and the blobs it removed from S3.
    pub parts_removed: u64,
    pub blobs_removed: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endings_name_themselves_as_labels() {
        let endings = [
            Ended::Committed,
            Ended::Duplicate,
            Ended::SchemaChanged,
            Ended::Fenced,
            Ended::ClaimLost,
            Ended::Refused,
            Ended::Failed(Failure::Start),
            Ended::Failed(Failure::Closed),
            Ended::Failed(Failure::Deadline),
            Ended::Failed(Failure::Exited),
            Ended::Failed(Failure::Protocol),
            Ended::Failed(Failure::Exception { code: Some(60) }),
            Ended::Failed(Failure::Cancelled),
        ];
        let names: Vec<&str> = endings.iter().map(|ended| ended.name()).collect();
        assert_eq!(
            names,
            [
                "committed",
                "duplicate",
                "schema_changed",
                "fenced",
                "claim_lost",
                "refused",
                "start_failed",
                "closed",
                "deadline",
                "exited",
                "protocol",
                "exception",
                "cancelled"
            ]
        );
        assert_eq!(endings.iter().filter(|ended| ended.is_failed()).count(), 7);
        assert_eq!(Kind::ALL.map(Kind::name), ["insert", "plan", "merge", "housekeep", "describe", "share"]);
    }
}
