//! The exclusive writer: one stock server that holds the tenant's disk alone.

use super::{DiskMode, Server, ServerConfig, StockEnv, database_disk_xml, disk_xml, merge_tree_xml, policy_xml};
use crate::{Error, Tenant};

/// A stock server that is the disk's only writer. It runs what shared writers and libch's operations leave to
/// stock ClickHouse: DDL, mutations, lightweight updates and deletes, and partition commands.
///
/// Starting one begins a new owner epoch, which fences every other writer of the disk: their next commits
/// apply nothing. Stop every shared writer first to spare them the wasted work.
#[derive(Debug)]
pub struct Exclusive {
    server: Server,
}

impl Exclusive {
    /// Boots the writer and waits until it has loaded the tenant's databases.
    pub async fn start(env: &StockEnv, tenant: &Tenant, key: &str) -> Result<Exclusive, Error> {
        Exclusive::start_with(env, tenant, key, "").await
    }

    /// Boots the writer with further MergeTree settings, as `<old_parts_lifetime>1</old_parts_lifetime>`.
    pub async fn start_with(env: &StockEnv, tenant: &Tenant, key: &str, merge_tree_settings_xml: &str) -> Result<Exclusive, Error> {
        let config = ServerConfig {
            name: "exclusive".into(),
            disks_xml: disk_xml(tenant, key, &DiskMode::Exclusive),
            policies_xml: policy_xml(),
            extra_xml: merge_tree_xml(merge_tree_settings_xml) + &database_disk_xml(),
        };
        Ok(Exclusive { server: Server::start(env, &config).await? })
    }

    /// Runs one statement and returns its output.
    pub async fn execute(&self, sql: &str) -> Result<String, Error> {
        self.server.query(sql).await
    }

    pub fn server(&self) -> &Server {
        &self.server
    }

    /// Stops the writer, which lets its disk shut down cleanly.
    pub async fn stop(mut self) -> Result<(), Error> {
        self.server.stop().await
    }

    /// Boots a writer, runs the statements in order, and stops it. The writer stops even when a statement
    /// fails, and the statement's error comes back.
    pub async fn run(env: &StockEnv, tenant: &Tenant, key: &str, statements: &[&str]) -> Result<(), Error> {
        let writer = Exclusive::start(env, tenant, key).await?;
        let mut result = Ok(());
        for statement in statements {
            if let Err(error) = writer.execute(statement).await {
                result = Err(error);
                break;
            }
        }
        let stopped = writer.stop().await;
        result.and(stopped)
    }
}
