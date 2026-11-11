//! Stock ClickHouse servers from the BigHouse fork.
//!
//! libch's own operations replace these for inserts and merges. They stay in the system for what an operation
//! leaves to stock ClickHouse, and as the reference every operation is tested against:
//!
//! - [`StockWriter`]: a shared writer, the reference implementation of [`Writer`](crate::Writer);
//! - [`Exclusive`]: the one writer that runs DDL and mutations;
//! - [`Fleet`]: read-only servers that answer a query as parallel replicas.

use std::time::Duration;

mod exclusive;
mod fleet;
pub mod ports;
mod server;
mod writer;

pub use exclusive::Exclusive;
pub use fleet::{Blank, Fleet};
pub use server::{Answer, Server, ServerConfig, StockEnv, Summary, xml_escape};
pub use writer::{StockWriter, StockWriterOptions};

use crate::Tenant;

/// The name every server gives the tenant's disk and its storage policy, as BigHouse does.
pub const DISK: &str = "bighouse";

/// How a server holds the tenant's disk.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DiskMode {
    /// The one writer of the disk. Starting it fences every other writer.
    Exclusive,
    /// One of several writers that insert and merge side by side.
    Shared {
        /// How long a merge claim of this writer lasts without renewal. `None` keeps the fork's default.
        claim_ttl: Option<Duration>,
    },
    /// A reader: the disk and its S3 storage refuse writes, so MergeTree treats the tables as static.
    ReadOnly,
}

/// The `<disks>` entry for a tenant's disk under `key`.
pub fn disk_xml(tenant: &Tenant, key: &str, mode: &DiskMode) -> String {
    let mode_xml = match mode {
        DiskMode::Exclusive => String::new(),
        DiskMode::Shared { claim_ttl } => {
            let ttl = claim_ttl.map(|ttl| format!("<inodedb_claim_ttl_ms>{}</inodedb_claim_ttl_ms>", ttl.as_millis())).unwrap_or_default();
            format!("<inodedb_writer_mode>shared</inodedb_writer_mode>{ttl}")
        }
        DiskMode::ReadOnly => "<readonly>1</readonly>".to_string(),
    };
    format!(
        "
        <{DISK}>
            <type>object_storage</type>
            <object_storage_type>s3</object_storage_type>
            <metadata_type>inodedb</metadata_type>
            <endpoint>{endpoint}</endpoint>
            <access_key_id>{access_key}</access_key_id>
            <secret_access_key>{secret_key}</secret_access_key>
            <inodedb_endpoint>{store}</inodedb_endpoint>
            <inodedb_key>{key}</inodedb_key>
            <inodedb_read_only>{read_only}</inodedb_read_only>
            {mode_xml}
        </{DISK}>",
        endpoint = xml_escape(&tenant.s3.disk_endpoint()),
        access_key = xml_escape(&tenant.s3.access_key),
        secret_key = xml_escape(&tenant.s3.secret_key),
        store = xml_escape(&tenant.store),
        key = xml_escape(key),
        read_only = u8::from(*mode == DiskMode::ReadOnly),
    )
}

/// The `<policies>` entry that puts tables on the tenant's disk.
pub fn policy_xml() -> String {
    format!("<{DISK}><volumes><main><disk>{DISK}</disk></main></volumes></{DISK}>")
}

/// The `<merge_tree>` section that makes the tenant's disk every MergeTree table's default, with `settings_xml`
/// as further MergeTree settings.
pub fn merge_tree_xml(settings_xml: &str) -> String {
    format!("<merge_tree><storage_policy>{DISK}</storage_policy>{settings_xml}</merge_tree>")
}

/// Keeps a writer's database definitions on the tenant's disk, where every other server finds them.
pub fn database_disk_xml() -> String {
    format!("<database_disk><disk>{DISK}</disk></database_disk>")
}

/// A name as a ClickHouse identifier.
pub fn quote_identifier(name: &str) -> String {
    format!("`{}`", name.replace('\\', "\\\\").replace('`', "\\`"))
}

/// Text as a ClickHouse string literal.
pub fn quote_string(text: &str) -> String {
    format!("'{}'", text.replace('\\', "\\\\").replace('\'', "\\'"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::S3Location;

    fn tenant() -> Tenant {
        Tenant {
            store: "127.0.0.1:7170".into(),
            root: "tenants/t1".into(),
            s3: S3Location {
                endpoint: "http://127.0.0.1:11211".into(),
                bucket: "libch".into(),
                prefix: "tenants/t1".into(),
                region: "us-east-1".into(),
                access_key: "libch".into(),
                secret_key: "se<cret".into(),
            },
        }
    }

    #[test]
    fn disk_xml_sets_the_mode() {
        let exclusive = disk_xml(&tenant(), "1.abc", &DiskMode::Exclusive);
        assert!(exclusive.contains("<endpoint>http://127.0.0.1:11211/libch/tenants/t1/</endpoint>"));
        assert!(exclusive.contains("<inodedb_read_only>0</inodedb_read_only>"));
        assert!(exclusive.contains("<secret_access_key>se&lt;cret</secret_access_key>"));
        assert!(!exclusive.contains("inodedb_writer_mode") && !exclusive.contains("<readonly>"));

        let shared = disk_xml(&tenant(), "1.abc", &DiskMode::Shared { claim_ttl: Some(Duration::from_secs(4)) });
        assert!(shared.contains("<inodedb_writer_mode>shared</inodedb_writer_mode><inodedb_claim_ttl_ms>4000</inodedb_claim_ttl_ms>"));

        let read_only = disk_xml(&tenant(), "1.abc", &DiskMode::ReadOnly);
        assert!(read_only.contains("<inodedb_read_only>1</inodedb_read_only>") && read_only.contains("<readonly>1</readonly>"));
    }

    #[test]
    fn quoting_escapes_the_quote_and_the_backslash() {
        assert_eq!(quote_identifier("a`b\\c"), "`a\\`b\\\\c`");
        assert_eq!(quote_string("it's"), "'it\\'s'");
    }
}
