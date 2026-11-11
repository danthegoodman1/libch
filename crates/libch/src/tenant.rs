//! Where a tenant's disk lives.

use std::time::Duration;

use inodedb::{Client, Grant};

use crate::Error;

/// The S3 prefix that holds a disk's blobs, and the credentials that reach it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct S3Location {
    /// The service's URL, as `http://127.0.0.1:11211`.
    pub endpoint: String,
    pub bucket: String,
    /// The disk's key prefix inside the bucket, without a leading or trailing slash.
    pub prefix: String,
    pub region: String,
    pub access_key: String,
    pub secret_key: String,
}

impl S3Location {
    /// The endpoint a ClickHouse disk takes: the service, the bucket and the prefix, ending in a slash.
    pub fn disk_endpoint(&self) -> String {
        format!("{}/{}/{}/", self.endpoint.trim_end_matches('/'), self.bucket, self.prefix)
    }

    /// What every blob key of the disk starts with.
    pub fn key_prefix(&self) -> String {
        format!("{}/", self.prefix)
    }
}

/// One tenant's disk: a directory in an InodeDB store, and an S3 prefix.
///
/// A key rooted at `root` sees the disk's `tree/`, `coord/` and `owner` at the top of its own tree.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Tenant {
    /// The InodeDB store's address, as `127.0.0.1:7170`.
    pub store: String,
    /// The tenant's directory in the store, as a key rooted above it names it.
    pub root: String,
    pub s3: S3Location,
}

impl Tenant {
    /// The store's address as the Rust client takes it.
    pub fn store_url(&self) -> String {
        store_url(&self.store)
    }

    /// Connects to the tenant's store under `key`.
    pub async fn connect(&self, key: &str) -> Result<Client, Error> {
        Ok(Client::connect(self.store_url(), key).await?)
    }

    /// Mints a key rooted at the tenant's directory. `minter` holds an `admin` key rooted at or above it, and
    /// names the directory as `self.root`. A zero `ttl` lasts as long as the minter's key.
    pub async fn mint_key(&self, minter: &Client, grant: Grant, ttl: Duration) -> Result<String, Error> {
        Ok(minter.mint_key(self.root.as_bytes(), grant, ttl).await?)
    }
}

/// A store's address as the Rust client takes it.
pub fn store_url(store: &str) -> String {
    if store.contains("://") { store.to_string() } else { format!("http://{store}") }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disk_endpoint_joins_service_bucket_and_prefix() {
        let s3 = S3Location {
            endpoint: "http://127.0.0.1:11211/".into(),
            bucket: "libch".into(),
            prefix: "tenants/t1".into(),
            region: "us-east-1".into(),
            access_key: "a".into(),
            secret_key: "s".into(),
        };
        assert_eq!(s3.disk_endpoint(), "http://127.0.0.1:11211/libch/tenants/t1/");
        assert_eq!(s3.key_prefix(), "tenants/t1/");
    }

    #[test]
    fn store_url_adds_a_scheme_once() {
        assert_eq!(store_url("127.0.0.1:7170"), "http://127.0.0.1:7170");
        assert_eq!(store_url("http://127.0.0.1:7170"), "http://127.0.0.1:7170");
    }
}
