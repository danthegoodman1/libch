//! Reads a tenant's tables the way a query does: through read-only servers under a read key.

use std::time::Duration;

use libch::Error;
use libch::inodedb::Grant;
use libch::stock::{Blank, Fleet};

use crate::{Stack, TenantFixture};

/// A fleet of one server for a test's queries. It sees the tenant's parts as of the moment it opened, so a
/// test opens a new one after each change it wants to observe.
pub struct Reader {
    fleet: Fleet,
}

impl Reader {
    pub async fn open(stack: &Stack, fixture: &TenantFixture) -> Result<Reader, Error> {
        Reader::open_fleet(stack, fixture, 1).await
    }

    /// A reader over `servers` parallel replicas.
    pub async fn open_fleet(stack: &Stack, fixture: &TenantFixture, servers: usize) -> Result<Reader, Error> {
        let key = fixture.mint(Grant::Read, Duration::from_secs(900)).await?;
        let blank = Blank::boot_many(&stack.stock_env(), servers).await?;
        Ok(Reader { fleet: Fleet::assign(blank, &fixture.tenant, &key).await? })
    }

    pub fn fleet(&self) -> &Fleet {
        &self.fleet
    }

    /// A query's output, trimmed.
    pub async fn value(&self, sql: &str) -> Result<String, Error> {
        Ok(self.fleet.query(sql).await?.trim().to_string())
    }

    /// A query's single number.
    pub async fn number(&self, sql: &str) -> Result<u64, Error> {
        let value = self.value(sql).await?;
        value.parse().map_err(|_| Error::Other(format!("`{sql}` answered `{value}`, where a number is expected")))
    }

    pub async fn close(self) -> Result<(), Error> {
        self.fleet.stop().await
    }

    /// Opens a reader, runs one query for a number, and closes it.
    pub async fn number_once(stack: &Stack, fixture: &TenantFixture, sql: &str) -> Result<u64, Error> {
        let reader = Reader::open(stack, fixture).await?;
        let number = reader.number(sql).await;
        reader.close().await?;
        number
    }
}
