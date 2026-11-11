//! The reference writer: a stock shared writer behind the [`Writer`] trait.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use tokio::sync::Mutex;

use super::{DiskMode, Server, ServerConfig, StockEnv, database_disk_xml, disk_xml, merge_tree_xml, policy_xml, quote_string};
use crate::catalog::{Catalog, Table};
use crate::writer::{Budget, Housekept, Insert, Inserted, MergeCandidate, Merged, Outcome, Remaining, TableName, Writer};
use crate::{Error, Tenant};

/// How a [`StockWriter`] boots.
#[derive(Clone, Debug, Default)]
pub struct StockWriterOptions {
    /// Further MergeTree settings, as `<old_parts_lifetime>10</old_parts_lifetime>`.
    pub merge_tree_settings_xml: String,
    /// How long a merge claim lasts without renewal. `None` keeps the fork's default.
    pub claim_ttl: Option<Duration>,
    /// Whether the server merges on its own schedule. Off by default, so the writer merges only what
    /// [`Writer::merge`] asks for.
    pub background_merges: bool,
}

/// A stock ClickHouse server in shared-writer mode, booted with the tenant's config.
///
/// It is what libch's operations are tested against: the same batches through this writer and through an
/// operation must leave tables that answer every query alike. It maps onto the [`Writer`] trait as far as stock
/// SQL reaches:
///
/// - `insert` sends one synchronous `INSERT` with the batch's token;
/// - `plan` proposes one merge for each partition that holds two or more active parts: all of them, since
///   `OPTIMIZE ... PARTITION ... FINAL` is the only statement that merges a set of parts the caller can name;
/// - `merge` runs that statement when the partition still holds exactly the candidate's sources;
/// - `housekeep` reports the covered parts and removes none, because a stock shared writer removes the sources
///   of its own merges on its own schedule.
#[derive(Debug)]
pub struct StockWriter {
    server: Server,
    catalog: Catalog,
    /// Serializes inserts, so the server's counters before and after one insert belong to that insert.
    inserting: Mutex<()>,
}

impl StockWriter {
    /// Boots the writer and waits until it has loaded the tenant's databases.
    pub async fn start(env: &StockEnv, tenant: &Tenant, key: &str, options: &StockWriterOptions) -> Result<StockWriter, Error> {
        let mut merge_tree = options.merge_tree_settings_xml.clone();
        if !options.background_merges {
            // The background selector merges nothing above this size, and an explicit merge of a whole
            // partition ignores it.
            merge_tree.push_str("<max_bytes_to_merge_at_max_space_in_pool>1</max_bytes_to_merge_at_max_space_in_pool>");
        }
        let config = ServerConfig {
            name: "shared".into(),
            disks_xml: disk_xml(tenant, key, &DiskMode::Shared { claim_ttl: options.claim_ttl }),
            policies_xml: policy_xml(),
            extra_xml: merge_tree_xml(&merge_tree) + &database_disk_xml(),
        };
        let server = Server::start(env, &config).await?;
        Ok(StockWriter { server, catalog: Catalog::connect(tenant, key).await?, inserting: Mutex::new(()) })
    }

    pub fn server(&self) -> &Server {
        &self.server
    }

    pub fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    /// Stops the writer, which lets its disk shut down cleanly.
    pub async fn stop(mut self) -> Result<(), Error> {
        self.server.stop().await
    }

    /// Kills the writer with SIGKILL.
    pub fn kill(mut self) {
        self.server.kill();
    }

    async fn table(&self, name: &TableName) -> Result<Table, Error> {
        self.catalog.table(name).await?.ok_or_else(|| Error::Other(format!("the tenant has no table {name}")))
    }

    /// How many blocks this server has written as parts, and how many it found already committed.
    async fn block_counters(&self) -> Result<(u64, u64), Error> {
        let answer = self
            .server
            .query("SELECT sumIf(value, event = 'MergeTreeDataWriterBlocks'), sumIf(value, event = 'DuplicatedInsertedBlocks') FROM system.events FORMAT TSV")
            .await?;
        let mut fields = answer.split_whitespace().map(str::parse::<u64>);
        match (fields.next(), fields.next()) {
            (Some(Ok(written)), Some(Ok(duplicates))) => Ok((written, duplicates)),
            _ => Err(Error::Other(format!("system.events answered `{}`", answer.trim()))),
        }
    }
}

impl Writer for StockWriter {
    async fn insert(&self, insert: Insert<'_>) -> Result<Outcome<Inserted>, Error> {
        let sql = format!("INSERT INTO {} FORMAT {}", insert.table.quoted(), insert.format);
        let settings = [("insert_deduplication_token", insert.token), ("async_insert", "0")];
        let _serialized = self.inserting.lock().await;
        let (written_before, duplicates_before) = self.block_counters().await?;
        if let Err(error) = self.server.run(&sql, &settings, insert.data.clone()).await {
            return refusal(error);
        }
        let (written_after, duplicates_after) = self.block_counters().await?;
        let written = written_after - written_before;
        let duplicates = duplicates_after - duplicates_before;
        // The server writes each block as a part before it learns the block is a duplicate, so an insert that
        // committed nothing counts as many duplicates as blocks.
        if written > 0 && duplicates >= written { Ok(Outcome::Duplicate) } else { Ok(Outcome::Committed(Inserted { parts: None })) }
    }

    async fn plan(&self, table: &TableName, budget: &Budget) -> Result<Vec<MergeCandidate>, Error> {
        let table = self.table(table).await?;
        let parts = self.catalog.parts(&table).await?;
        let coordination = self.catalog.coordination(&table).await?;
        let now = now_ms();
        let mut candidates = Vec::new();
        for partition in parts.set.partitions() {
            let sources: Vec<_> = parts.set.active_in(partition).cloned().collect();
            let claimed = sources.iter().any(|source| coordination.claim(&source.name()).is_some_and(|claim| claim.is_live(now)));
            if sources.len() < 2 || sources[0].is_patch() || claimed {
                continue;
            }
            candidates.push(MergeCandidate { partition_id: partition.to_string(), sources });
            if candidates.len() >= budget.max_merges {
                break;
            }
        }
        Ok(candidates)
    }

    async fn merge(&self, table: &TableName, candidate: &MergeCandidate) -> Result<Outcome<Merged>, Error> {
        if candidate.sources.len() < 2 {
            return Ok(Outcome::Refused("a merge takes two or more source parts".into()));
        }
        let definition = self.table(table).await?;
        let before = self.catalog.parts(&definition).await?;
        let active: Vec<_> = before.set.active_in(&candidate.partition_id).cloned().collect();
        if active != candidate.sources {
            return Ok(Outcome::Refused(format!(
                "a stock writer merges a whole partition, and partition {} no longer holds exactly the candidate's sources",
                candidate.partition_id
            )));
        }

        let sql = format!("OPTIMIZE TABLE {} PARTITION ID {} FINAL", table.quoted(), quote_string(&candidate.partition_id));
        let settings = [("optimize_throw_if_noop", "1"), ("optimize_skip_merged_partitions", "1")];
        if let Err(error) = self.server.run(&sql, &settings, Bytes::new()).await {
            return refusal(error);
        }

        let after = self.catalog.parts(&definition).await?;
        let result = after.set.active.iter().find(|part| candidate.sources.iter().all(|source| part.contains(source) && *part != source));
        match result {
            Some(part) => Ok(Outcome::Committed(Merged { part: part.clone() })),
            None => Ok(Outcome::ClaimLost),
        }
    }

    async fn housekeep(&self, table: &TableName) -> Result<Outcome<Housekept>, Error> {
        let table = self.table(table).await?;
        let parts = self.catalog.parts(&table).await?;
        let remaining = parts
            .set
            .covered
            .iter()
            .map(|covered| Remaining {
                part: covered.part.clone(),
                reason: format!("covered by {}; a stock writer removes the sources of its own merges", covered.by),
            })
            .collect();
        Ok(Outcome::Committed(Housekept { removed: Vec::new(), remaining }))
    }
}

/// Sorts a server's exception into the outcome it stands for, or passes it on.
fn refusal<T>(error: Error) -> Result<Outcome<T>, Error> {
    let Error::ClickHouse { code, message } = &error else { return Err(error) };
    const ABORTED: i32 = 236;
    const TABLE_IS_READ_ONLY: i32 = 242;
    const SUPPORT_IS_DISABLED: i32 = 344;
    const CANNOT_ASSIGN_OPTIMIZE: i32 = 388;
    let lost_ownership = *code == TABLE_IS_READ_ONLY && message.contains("lost ownership");
    let key_refused = message.contains("revoked key") || message.contains("Unauthenticated");
    let needs_exclusive = *code == SUPPORT_IS_DISABLED && message.contains("needs an exclusive writer");
    if lost_ownership || key_refused {
        Ok(Outcome::Fenced)
    } else if *code == ABORTED && message.contains("definition of") {
        Ok(Outcome::SchemaChanged)
    } else if message.contains("merge claim") || message.contains("lost its claims") {
        Ok(Outcome::ClaimLost)
    } else if needs_exclusive || *code == CANNOT_ASSIGN_OPTIMIZE {
        Ok(Outcome::Refused(message.clone()))
    } else {
        Err(error)
    }
}

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |elapsed| elapsed.as_millis() as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn exception(code: i32, message: &str) -> Error {
        Error::ClickHouse { code, message: message.into() }
    }

    #[test]
    fn exceptions_sort_into_outcomes() {
        let outcome = |code, message| refusal::<()>(exception(code, message)).unwrap();
        assert_eq!(outcome(242, "Code: 242. DB::Exception: disk lost ownership: another server took over the disk"), Outcome::Fenced);
        assert_eq!(outcome(210, "InodeDB: Unauthenticated: unknown or revoked key"), Outcome::Fenced);
        assert_eq!(outcome(236, "The definition of d.t changed since this writer loaded it, so the insert committed nothing"), Outcome::SchemaChanged);
        assert_eq!(outcome(49, "Merge all_1_2_1 of `store/x` lost its claims to another writer"), Outcome::ClaimLost);
        assert!(matches!(outcome(344, "ALTER needs an exclusive writer: this server shares the disk"), Outcome::Refused(_)));
        assert!(matches!(outcome(388, "Cannot OPTIMIZE table: There is only one part inside partition"), Outcome::Refused(_)));
    }

    #[test]
    fn other_exceptions_stay_errors() {
        assert_eq!(refusal::<()>(exception(60, "Table d.missing does not exist")).unwrap_err().clickhouse_code(), Some(60));
        assert_eq!(refusal::<()>(exception(242, "Table is in readonly mode")).unwrap_err().clickhouse_code(), Some(242));
        assert!(matches!(refusal::<()>(Error::Other("boom".into())), Err(Error::Other(_))));
    }
}
