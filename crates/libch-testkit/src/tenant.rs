//! A tenant for one test, removed when the test ends.

use std::time::{Duration, Instant};

use futures_util::StreamExt;

use libch::catalog::Catalog;
use libch::inodedb::{Client, Grant, Op};
use libch::s3::S3Client;
use libch::stock::Exclusive;
use libch::tenant::store_url;
use libch::worker::OpWriter;
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

    /// Runs statements in a stock exclusive writer, then hands the disk to shared writers, as a service does
    /// around a tenant's DDL. The writer has stopped either way, so the disk goes back when a statement fails too.
    pub async fn exclusively(&self, statements: &[&str]) -> Result<(), Error> {
        let ran = Exclusive::run(&self.stack.stock_env(), &self.tenant, &self.key, statements).await;
        let shared = self.share().await;
        ran.and(shared)
    }

    /// Hands the tenant's disk to shared writers once an exclusive writer has stopped. Until then every
    /// operation of a worker ends fenced.
    pub async fn share(&self) -> Result<(), Error> {
        OpWriter::new(self.stack.worker_command(), &self.tenant, &self.key).share().await.map(drop)
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

    /// Lets the tenant stay in the store when the fixture goes, for a stack that is wiped afterwards.
    pub fn leave(&mut self) {
        self.removed = true;
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
    // freed entries wait in InodeDB's free queue for a consumer, and come free to claim once every lease older
    // than the removal has ended, which a killed server's may take half a minute to do. The fixture collects
    // what is free now. What is left it notes down, and a later test process collects it: entries that nobody
    // claims stay in the queue, and a long queue slows every call that reads it.
    //
    // The queue is one list for the whole store, so a consumer cannot ask whether its own part of it is empty.
    // A marker file freed after the removal answers instead: the queue hands entries out in the order they
    // were freed, so once a claim returns the marker, everything the removal freed was free to claim too.
    let home_client = Client::connect(store_url(&stack.inodedb_endpoint), home_key).await?;
    home_client.commit([Op::remove_all("disk").if_exists()]).await?;
    apply(&home_client, [Op::put(MARKER_FILE, MARKER)]).await?;
    apply(&home_client, [Op::unlink(MARKER_FILE)]).await?;
    if collect(&home_client).await? {
        stack.admin().await?.commit([Op::remove_all(home.as_bytes()).if_exists()]).await?;
    } else {
        let notes = pending_dir(stack);
        std::fs::create_dir_all(&notes)?;
        std::fs::write(notes.join(home.replace('/', "_")), format!("{home}\n{home_key}\n"))?;
    }
    Ok(())
}

const MARKER_FILE: &str = "collected-through";
const MARKER: &[u8] = b"libch-testkit: what was freed before this file is free to claim";

/// Where the fixtures of this stack note the directories they could not finish removing.
fn pending_dir(stack: &Stack) -> std::path::PathBuf {
    stack.work_dir.join("pending-tenants")
}

/// Claims and acknowledges the freed entries under a key's root that are free to claim. Returns whether the
/// marker came with them and nothing is left after it: the root's part of the queue is then empty.
async fn collect(client: &Client) -> Result<bool, Error> {
    let mut marker_seen = false;
    // A pass takes up to 64,000 entries before the marker, and goes on to the end once it has seen it.
    for round in 0.. {
        let claim = client.claim_freed(1_000).await?;
        if claim.entries.is_empty() {
            return Ok(marker_seen);
        }
        marker_seen |= claim.entries.iter().any(|entry| entry.value.as_deref() == Some(MARKER));
        client.ack_freed(claim.id, &claim.entries).await?;
        if !marker_seen && round >= 64 {
            break;
        }
    }
    Ok(false)
}

/// Finishes the removals earlier test processes noted down: collects what has come free under each directory
/// since, and removes the directory once its queue is empty. A note whose key the store no longer knows, as
/// after the store was wiped, is dropped.
pub async fn collect_pending(stack: &Stack) {
    let Ok(notes) = std::fs::read_dir(pending_dir(stack)) else { return };
    let Ok(admin) = stack.admin().await else { return };
    let notes: Vec<_> = notes.flatten().take(4_000).collect();
    let finished = notes.iter().map(|note| {
        let admin = &admin;
        async move {
            let Ok(text) = std::fs::read_to_string(note.path()) else { return };
            let mut lines = text.lines();
            let (Some(home), Some(home_key)) = (lines.next(), lines.next()) else { return };
            let emptied = match Client::connect(store_url(&stack.inodedb_endpoint), home_key).await {
                Ok(client) => collect(&client).await,
                Err(error) => Err(error.into()),
            };
            match emptied {
                Ok(true) => {
                    if admin.commit([Op::remove_all(home.as_bytes()).if_exists()]).await.is_ok() {
                        let _ = std::fs::remove_file(note.path());
                    }
                }
                // Entries that an older lease still holds: a later process looks again.
                Ok(false) => {}
                Err(_) => {
                    let _ = std::fs::remove_file(note.path());
                }
            }
        }
    });
    futures_util::stream::iter(finished).buffer_unordered(32).collect::<Vec<()>>().await;
}

/// Waits until no lease from before now holds the store's free queue. `writer` holds a `write` key.
///
/// The queue's release horizon is the store's, so a reader of any tenant holds it back. A server that was
/// killed, or whose key was revoked, cannot release its lease, and the lease then lasts out its 30 seconds.
pub async fn wait_for_a_clear_horizon(writer: &Client) {
    apply(writer, [Op::put("horizon-probe", "x")]).await.expect("a probe file");
    apply(writer, [Op::unlink("horizon-probe")]).await.expect("the probe file freed");
    let started = Instant::now();
    loop {
        let claim = writer.claim_freed(100).await.expect("the free queue answers");
        if !claim.entries.is_empty() {
            writer.ack_freed(claim.id, &claim.entries).await.expect("the claim is acknowledged");
            return;
        }
        assert!(started.elapsed() < Duration::from_secs(45), "an older lease still holds the free queue after 45 s");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
