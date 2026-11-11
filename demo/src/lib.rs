//! The high-tenant recipe on libch, end to end.
//!
//! Tenants are set up once. Their load is batched per tenant, and every batch becomes an `insert` operation
//! through one pool of workers, under a token that makes a retry safe. A tenant's tables stay compact through
//! `plan`, `merge` and `housekeep` operations, scheduled from the count of batches it has taken. No process
//! outlives an operation. Queries run on read-only servers booted blank and assigned to a tenant under a read
//! key, as BigHouse assigns its fleets.

pub mod load;
mod report;
mod run;

pub use report::Report;
pub use run::{Config, run};
