//! libch runs ClickHouse's insert and merge code as explicit operations on a tenant's BigHouse `inodedb` disk.
//!
//! Its parts:
//!
//! - [`tenant`]: where a tenant's disk lives, in InodeDB and in S3.
//! - [`catalog`]: a tenant's databases, tables, parts and coordination files, read from InodeDB.
//! - [`part`]: part names as ClickHouse parses them, and the active set they form.
//! - [`writer`]: the [`Writer`] trait, which every way of writing a tenant's disk implements.
//! - [`stock`]: stock ClickHouse servers from the BigHouse fork: the reference [`stock::StockWriter`], the
//!   [`stock::Exclusive`] writer for DDL and mutations, and the read-only [`stock::Fleet`].
//! - [`worker`]: [`worker::OpWriter`], which runs each operation in a worker process of its own.
//! - [`check`]: the invariant checker.

pub mod catalog;
pub mod check;
pub mod disk;
pub mod error;
pub mod part;
pub mod s3;
pub mod stock;
pub mod tenant;
pub mod worker;
pub mod writer;

pub use error::Error;
pub use part::PartInfo;
pub use tenant::{S3Location, Tenant};
pub use writer::{Outcome, Writer};

pub use inodedb;
