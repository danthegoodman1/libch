//! libch runs ClickHouse's insert and merge code as explicit operations on a tenant's BigHouse `inodedb` disk.
//!
//! Its parts:
//!
//! - [`tenant`]: where a tenant's disk lives, in InodeDB and in S3.
//! - [`keys`]: [`keys::KeyCache`], which mints, caches and revokes the keys operations run under.
//! - [`catalog`]: a tenant's databases, tables, parts and coordination files, read from InodeDB.
//! - [`part`]: part names as ClickHouse parses them, and the active set they form.
//! - [`writer`]: the [`Writer`] trait, which every way of writing a tenant's disk implements.
//! - [`stock`]: stock ClickHouse servers from the BigHouse fork: the reference [`stock::StockWriter`], the
//!   [`stock::Exclusive`] writer for DDL and mutations, and the read-only [`stock::Fleet`].
//! - [`worker`]: [`worker::OpWriter`], which runs each operation in a worker process of its own.
//! - [`pool`]: [`pool::Pool`], which keeps workers started ahead of demand and bounds how many operations run at once.
//! - [`check`]: the invariant checker.
//!
//! # An insert, a plan and its merges
//!
//! Each call runs in a worker process that attaches the tenant, does its work, answers and exits. The pool
//! starts workers before they are needed, and the key cache mints each tenant a short-lived key.
//!
//! ```no_run
//! use bytes::Bytes;
//! use libch::keys::{KeyCache, KeyPolicy};
//! use libch::pool::{Pool, PoolOptions};
//! use libch::worker::WorkerCommand;
//! use libch::writer::{Budget, Insert, TableName};
//! use libch::{Outcome, S3Location, Tenant, Writer};
//!
//! # async fn recipe(admin: libch::inodedb::Client) -> Result<(), libch::Error> {
//! let pool = Pool::new(WorkerCommand::clickhouse("/usr/bin/clickhouse"), PoolOptions { waiting: 8, concurrency: 16 })?;
//! let keys = KeyCache::new(admin, KeyPolicy::default())?;
//! let tenant = Tenant {
//!     store: "127.0.0.1:7170".into(),
//!     root: "tenants/acme/disk".into(),
//!     s3: S3Location {
//!         endpoint: "http://127.0.0.1:11211".into(),
//!         bucket: "analytics".into(),
//!         prefix: "tenants/acme".into(),
//!         region: "us-east-1".into(),
//!         access_key: "…".into(),
//!         secret_key: "…".into(),
//!     },
//! };
//! let writer = pool.writer(&tenant, keys.key(&tenant).await?);
//! let events = TableName::new("d", "events");
//!
//! // A batch lands once, however often it is sent under its token.
//! let batch = Bytes::from_static(b"{\"ts\":\"2026-11-01 00:00:00\",\"kind\":\"view\",\"v\":1}\n");
//! match writer.insert(Insert { table: &events, format: "JSONEachRow", token: "batch-1", data: batch }).await? {
//!     Outcome::Committed(_) | Outcome::Duplicate => {}
//!     other => return Err(libch::Error::Other(format!("the batch ended {}", other.name()))),
//! }
//!
//! // ClickHouse's selector proposes merges, and each runs as an operation of its own.
//! for planned in writer.plan(&events, &Budget::default()).await? {
//!     writer.merge(&events, &planned.candidate).await?;
//! }
//! // Housekeeping removes the merged parts once queries that began before the merge have had their time.
//! writer.housekeep(&events).await?;
//! # Ok(())
//! # }
//! ```

pub mod catalog;
pub mod check;
pub mod disk;
pub mod error;
pub mod keys;
pub mod part;
pub mod pool;
pub mod s3;
pub mod stock;
pub mod tenant;
pub mod worker;
pub mod writer;

pub use error::Error;
pub use part::PartInfo;
pub use tenant::{S3Location, Tenant, TenantKey};
pub use writer::{Outcome, Writer};

pub use inodedb;
