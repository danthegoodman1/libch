//! A tenant for one test, removed when the test ends.

use std::time::{Duration, Instant};

use libch::catalog::Catalog;
use libch::inodedb::{Client, Grant, Op};
use libch::s3::S3Client;
use libch::tenant::store_url;
use libch::{Error, Tenant};

use crate::Stack;

/// A tenant with a directory in the dev stack's InodeDB store, a write key and an S3 prefix of its own.
///
/// Dropping the fixture removes all three, on a passing test and on a panicking one alike.
/// [`TenantFixture::remove`] does the same and reports a failure.
pub struct TenantFixture {
    pub tenant: Tenant,
    /// A key with the `write` grant, rooted at the tenant's directory.
    pub key: String,
    /// The fixture's own directory, which holds the tenant's.
    home: String,
    /// A key with the `write` grant rooted at `home`: it outlives the tenant's directory, so it can collect
    /// what removing that directory frees.
    home_key: String,
    stack: Stack,
    removed: bool,
}

impl TenantFixture {
    pub async fn create(stack: &Stack) -> Result<TenantFixture, Error> {
        let id = format!("{:016x}", rand::random::<u64>());
        let home = format!("tenants/{id}");
        let root = format!("{home}/disk");
        let admin = stack.admin().await?;
        apply(&admin, [Op::mkdir(root.as_bytes()).parents()]).await?;
        let home_key = admin.mint_key(home.as_bytes(), Grant::Write, Duration::ZERO).await?;
        let key = admin.mint_key(root.as_bytes(), Grant::Write, Duration::ZERO).await?;
        let tenant = Tenant { store: stack.inodedb_endpoint.clone(), root, s3: stack.s3(&home) };
        Ok(TenantFixture { tenant, key, home, home_key, stack: stack.clone(), removed: false })
    }

    /// Mints another key rooted at the tenant's directory.
    pub async fn mint(&self, grant: Grant, ttl: Duration) -> Result<String, Error> {
        self.tenant.mint_key(&self.stack.admin().await?, grant, ttl).await
    }

    /// The tenant's catalog under its write key.
    pub async fn catalog(&self) -> Result<Catalog, Error> {
        Catalog::connect(&self.tenant, &self.key).await
    }

    /// A client under the tenant's write key.
    pub async fn client(&self) -> Result<Client, Error> {
        self.tenant.connect(&self.key).await
    }

    /// The tenant's directory as the store's admin key names it.
    pub fn home(&self) -> &str {
        &self.home
    }

    /// Removes the tenant: its S3 objects, its directory, and every key rooted there.
    pub async fn remove(mut self) -> Result<(), Error> {
        self.removed = true;
        cleanup(&self.stack, &self.tenant, &self.home, &self.home_key).await
    }
}

impl Drop for TenantFixture {
    fn drop(&mut self) {
        if self.removed {
            return;
        }
        let (stack, tenant, home, home_key) = (self.stack.clone(), self.tenant.clone(), self.home.clone(), self.home_key.clone());
        // Cleanup is async, and a drop may run inside a runtime that is unwinding. A thread with a runtime of
        // its own finishes the work wherever the drop happens.
        let cleaner = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().expect("a runtime for cleanup");
            runtime.block_on(cleanup(&stack, &tenant, &home, &home_key))
        });
        if let Ok(Err(error)) = cleaner.join() {
            eprintln!("removing tenant {} failed: {error}", self.home);
        }
    }
}

/// Commits ops as one all-or-nothing batch, and fails when InodeDB skipped any of them: a commit that applies
/// nothing still answers without an error.
pub async fn apply(client: &Client, ops: impl IntoIterator<Item = Op>) -> Result<(), Error> {
    let mut batch = client.batch().all_or_nothing();
    for op in ops {
        batch.push(op);
    }
    let commit = batch.commit().await?;
    if commit.all_applied() { Ok(()) } else { Err(Error::Other(format!("InodeDB skipped a batch: {:?}", commit.outcomes))) }
}

async fn cleanup(stack: &Stack, tenant: &Tenant, home: &str, home_key: &str) -> Result<(), Error> {
    S3Client::new(&tenant.s3)?.delete_prefix(&tenant.s3.key_prefix()).await?;

    // Removing the tenant's directory frees every inode and value under it, and revokes the tenant's keys. The
    // freed entries wait in InodeDB's free queue for a consumer; the fixture is that consumer, so the store
    // stays small across test runs.
    let home_client = Client::connect(store_url(&stack.inodedb_endpoint), home_key).await?;
    home_client.commit([Op::remove_all("disk").if_exists()]).await?;
    let deadline = Instant::now() + Duration::from_secs(2);
    let mut idle_rounds = 0;
    while idle_rounds < 3 && Instant::now() < deadline {
        match home_client.claim_freed(1_000).await {
            Ok(claim) if !claim.entries.is_empty() => {
                idle_rounds = 0;
                home_client.ack_freed(claim.id, &claim.entries).await?;
            }
            _ => {
                idle_rounds += 1;
                tokio::time::sleep(Duration::from_millis(40)).await;
            }
        }
    }

    stack.admin().await?.commit([Op::remove_all(home.as_bytes()).if_exists()]).await?;
    Ok(())
}
