//! Where the dev stack and the BigHouse checkout are.

use std::path::{Path, PathBuf};

use libch::inodedb::Client;
use libch::stock::StockEnv;
use libch::tenant::store_url;
use libch::{Error, S3Location};

/// The environment libch's tests run in. Every field has a default that matches `scripts/dev-up` and a
/// BigHouse checkout beside this repository; an environment variable overrides each.
#[derive(Clone, Debug)]
pub struct Stack {
    /// The BigHouse checkout. `BIGHOUSE_DIR`, default `../BigHouse` beside this repository.
    pub bighouse_dir: PathBuf,
    /// The fork's binary. `LIBCH_CLICKHOUSE`, default `build/programs/clickhouse` in the checkout.
    pub clickhouse: PathBuf,
    /// The InodeDB store. `LIBCH_INODEDB_ENDPOINT`, default `127.0.0.1:7170`.
    pub inodedb_endpoint: String,
    /// The file holding the store's admin key. `LIBCH_INODEDB_ADMIN_KEY_FILE`, default in the dev directory.
    pub admin_key_file: PathBuf,
    /// The S3 service. `LIBCH_S3_ENDPOINT`, default `http://127.0.0.1:11211`.
    pub s3_endpoint: String,
    /// `LIBCH_S3_BUCKET`, default `libch`.
    pub s3_bucket: String,
    /// `LIBCH_S3_ACCESS_KEY`, default `libch`.
    pub s3_access_key: String,
    /// `LIBCH_S3_SECRET_KEY`, default `libch`.
    pub s3_secret_key: String,
    /// Where servers keep their directories. `LIBCH_WORK_DIR`, default `libch-work` in the system's temporary
    /// directory.
    pub work_dir: PathBuf,
}

impl Stack {
    pub fn from_env() -> Stack {
        let root = Stack::repo_root();
        let var = |name: &str| std::env::var(name).ok().filter(|value| !value.is_empty());
        let bighouse_dir = var("BIGHOUSE_DIR").map(PathBuf::from).unwrap_or_else(|| root.join("../BigHouse"));
        let dev_dir = var("LIBCH_DEV_DIR").map(PathBuf::from).unwrap_or_else(|| root.join(".dev"));
        Stack {
            clickhouse: var("LIBCH_CLICKHOUSE").map(PathBuf::from).unwrap_or_else(|| bighouse_dir.join("build/programs/clickhouse")),
            bighouse_dir,
            inodedb_endpoint: var("LIBCH_INODEDB_ENDPOINT").unwrap_or_else(|| "127.0.0.1:7170".into()),
            admin_key_file: var("LIBCH_INODEDB_ADMIN_KEY_FILE").map(PathBuf::from).unwrap_or_else(|| dev_dir.join("inodedb/admin.key")),
            s3_endpoint: var("LIBCH_S3_ENDPOINT").unwrap_or_else(|| "http://127.0.0.1:11211".into()),
            s3_bucket: var("LIBCH_S3_BUCKET").unwrap_or_else(|| "libch".into()),
            s3_access_key: var("LIBCH_S3_ACCESS_KEY").unwrap_or_else(|| "libch".into()),
            s3_secret_key: var("LIBCH_S3_SECRET_KEY").unwrap_or_else(|| "libch".into()),
            work_dir: var("LIBCH_WORK_DIR").map(PathBuf::from).unwrap_or_else(|| std::env::temp_dir().join("libch-work")),
        }
    }

    /// This repository's root.
    pub fn repo_root() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
    }

    /// Where stock servers come from and keep their directories.
    pub fn stock_env(&self) -> StockEnv {
        StockEnv { clickhouse: self.clickhouse.clone(), work_dir: self.work_dir.clone() }
    }

    /// The S3 location of a disk whose keys start with `prefix`.
    pub fn s3(&self, prefix: &str) -> S3Location {
        S3Location {
            endpoint: self.s3_endpoint.clone(),
            bucket: self.s3_bucket.clone(),
            prefix: prefix.to_string(),
            region: "us-east-1".into(),
            access_key: self.s3_access_key.clone(),
            secret_key: self.s3_secret_key.clone(),
        }
    }

    pub fn admin_key(&self) -> std::io::Result<String> {
        Ok(std::fs::read_to_string(&self.admin_key_file)?.trim().to_string())
    }

    /// A client under the store's admin key, which is rooted at the store's top.
    pub async fn admin(&self) -> Result<Client, Error> {
        Ok(Client::connect(store_url(&self.inodedb_endpoint), self.admin_key()?).await?)
    }
}
