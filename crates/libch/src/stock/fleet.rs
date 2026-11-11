//! Read-only servers that answer one tenant's queries as parallel replicas, the way BigHouse's fleets do.

use futures_util::future::try_join_all;

use super::{DISK, DiskMode, Server, ServerConfig, StockEnv, disk_xml, merge_tree_xml, policy_xml, quote_identifier};
use crate::catalog::{Catalog, Database};
use crate::{Error, Tenant};

/// The cluster name every fleet's servers know themselves by.
const CLUSTER: &str = "bighouse";

/// A blank server: one that holds no tenant's disk, cluster or credentials yet.
pub struct Blank;

impl Blank {
    /// Boots a blank server.
    ///
    /// MergeTree reads its default settings only at boot, so the boot config names the tenant disk's storage
    /// policy. The server looks the name up when a table attaches, by which time assignment has added the
    /// policy.
    pub async fn boot(env: &StockEnv) -> Result<Server, Error> {
        let config = ServerConfig { name: "blank".into(), extra_xml: merge_tree_xml(""), ..ServerConfig::default() };
        Server::start(env, &config).await
    }

    /// Boots `count` blank servers at once.
    pub async fn boot_many(env: &StockEnv, count: usize) -> Result<Vec<Server>, Error> {
        try_join_all((0..count).map(|_| Blank::boot(env))).await
    }
}

/// Blank servers assigned to one tenant's disk under a read key.
///
/// Assignment gives each server the disk, its storage policy and a cluster that names exactly the fleet's
/// servers, with one config reload, and attaches the tenant's databases from their definitions in InodeDB. A
/// query then runs on the first server, and the rest read their share of its marks. A reload can add a disk
/// and can neither remove one nor change its key, so a server serves one tenant and is then replaced.
#[derive(Debug)]
pub struct Fleet {
    servers: Vec<Server>,
    databases: Vec<Database>,
    left_out: Vec<(String, String)>,
}

impl Fleet {
    /// Assigns blank servers to a tenant. `read_key` needs the `read` grant on the tenant's directory.
    pub async fn assign(servers: Vec<Server>, tenant: &Tenant, read_key: &str) -> Result<Fleet, Error> {
        if servers.is_empty() {
            return Err(Error::Other("a fleet takes one server or more".into()));
        }
        let replicas: String = servers.iter().map(|server| format!("<replica><host>127.0.0.1</host><port>{}</port></replica>", server.tcp_port())).collect();
        let config = format!(
            "<clickhouse>
    <storage_configuration>
        <disks>{disk}</disks>
        <policies>{policy}</policies>
    </storage_configuration>
    <remote_servers><{CLUSTER}><shard>{replicas}</shard></{CLUSTER}></remote_servers>
</clickhouse>
",
            disk = disk_xml(tenant, read_key, &DiskMode::ReadOnly),
            policy = policy_xml(),
        );
        try_join_all(servers.iter().map(|server| server.add_config("tenant.xml", &config))).await?;

        let catalog = Catalog::connect(tenant, read_key).await?;
        let mut databases = Vec::new();
        let mut left_out = Vec::new();
        for database in catalog.databases().await? {
            if database.definition.contains("\nENGINE = Atomic") {
                databases.push(database);
            } else {
                left_out.push((database.name.clone(), "its definition is not a plain Atomic database".to_string()));
            }
        }
        try_join_all(servers.iter().map(|server| attach(server, &databases))).await?;
        Ok(Fleet { servers, databases, left_out })
    }

    pub fn servers(&self) -> &[Server] {
        &self.servers
    }

    /// The tenant's databases the fleet serves.
    pub fn databases(&self) -> &[Database] {
        &self.databases
    }

    /// Each database the fleet could not serve, with the reason.
    pub fn left_out(&self) -> &[(String, String)] {
        &self.left_out
    }

    /// Runs a query on the first server, with every server of the fleet reading as a parallel replica.
    pub async fn query(&self, sql: &str) -> Result<String, Error> {
        self.query_with(sql, &[]).await
    }

    /// Runs a query as [`Fleet::query`] does, under further settings.
    pub async fn query_with(&self, sql: &str, settings: &[(&str, &str)]) -> Result<String, Error> {
        let replicas = self.servers.len().to_string();
        let mut all = vec![
            ("enable_parallel_replicas", "1"),
            ("max_parallel_replicas", replicas.as_str()),
            ("cluster_for_parallel_replicas", CLUSTER),
            ("parallel_replicas_for_non_replicated_merge_tree", "1"),
            ("parallel_replicas_min_number_of_rows_per_replica", "0"),
        ];
        all.extend_from_slice(settings);
        self.servers[0].query_with(sql, &all).await
    }

    /// The marks each server has read as a parallel replica since it booted: its share of the fleet's queries.
    pub async fn marks_read(&self) -> Result<Vec<u64>, Error> {
        let read = |server| async move {
            let answer: String = Server::query(server, "SELECT sum(value) FROM system.events WHERE event = 'ParallelReplicasReadMarks'").await?;
            Ok::<u64, Error>(answer.trim().parse().unwrap_or(0))
        };
        try_join_all(self.servers.iter().map(read)).await
    }

    /// Stops every server with SIGTERM, so each disk shuts down and releases its InodeDB lease.
    pub async fn stop(mut self) -> Result<(), Error> {
        try_join_all(self.servers.iter_mut().map(Server::stop)).await?;
        Ok(())
    }
}

async fn attach(server: &Server, databases: &[Database]) -> Result<(), Error> {
    for database in databases {
        // A server starts with an empty `default` of its own; the tenant's takes its place.
        if database.name == "default" {
            server.query_with("DROP DATABASE default SYNC", &[("database", "system")]).await?;
        }
        let sql = format!("ATTACH DATABASE {} UUID '{}' ENGINE = Atomic SETTINGS disk = '{DISK}'", quote_identifier(&database.name), database.uuid);
        server.query_with(&sql, &[("database", "system")]).await?;
    }
    Ok(())
}
