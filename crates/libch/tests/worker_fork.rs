//! Workers that fork a child for each operation: a parent starts the engine once, and the pool gives it one
//! operation after another.

use std::path::{Path, PathBuf};
use std::time::Duration;

use libch::pool::{Pool, PoolOptions};
use libch::s3::S3Client;
use libch::worker::OpWriter;
use libch::writer::Budget;
use libch::{Outcome, Writer};
use libch_testkit::ops::{assert_clean, insert, tenant_with_parts, totals};
use libch_testkit::schema::{batch, events};
use libch_testkit::{Stack, stack};

/// Rows enough that a batch's part keeps its data in S3.
const LARGE: u64 = 60_000;

/// The directory a test's forking workers keep theirs in, removed when the test ends.
struct Scratch(PathBuf);

impl std::ops::Deref for Scratch {
    type Target = Path;

    fn deref(&self) -> &Path {
        &self.0
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// A pool of forking workers, and the directory that holds theirs: the tests of this file run at the same time,
/// and each tells its own workers by where they keep their files.
fn forking_pool(stack: &Stack, waiting: usize, concurrency: usize) -> (Pool, Scratch) {
    let scratch = Scratch(stack.work_dir.join(format!("forking-{:016x}", rand::random::<u64>())));
    let pool = Pool::new(stack.worker_command().scratch(&*scratch).forking(), PoolOptions { waiting, concurrency }).unwrap();
    (pool, scratch)
}

/// The forking workers this test has started under `scratch`: each one's process id.
fn parents(scratch: &Path) -> Vec<u32> {
    let me = std::process::id().to_string();
    let under = format!("--path={}", scratch.display());
    let mut found = Vec::new();
    for entry in std::fs::read_dir("/proc").unwrap().flatten() {
        let Some(pid) = entry.file_name().to_str().and_then(|name| name.parse::<u32>().ok()) else { continue };
        let Ok(stat) = std::fs::read_to_string(format!("/proc/{pid}/stat")) else { continue };
        // The parent's id is the second field after the command's closing parenthesis.
        let parent = stat.rsplit_once(") ").and_then(|(_, rest)| rest.split(' ').nth(1).map(str::to_string));
        let command = std::fs::read(format!("/proc/{pid}/cmdline")).unwrap_or_default();
        let arguments: Vec<&[u8]> = command.split(|&byte| byte == 0).collect();
        let ours = arguments.iter().any(|argument| argument.starts_with(under.as_bytes()));
        if parent.as_deref() == Some(me.as_str()) && ours && arguments.contains(&b"--fork".as_slice()) {
            found.push(pid);
        }
    }
    found
}

fn threads_of(pid: u32) -> usize {
    std::fs::read_dir(format!("/proc/{pid}/task")).map(|tasks| tasks.count()).unwrap_or(0)
}

/// Whether a process's memory holds `needle`, in any mapping it can write.
fn memory_holds(pid: u32, needle: &[u8]) -> bool {
    use std::io::{Read, Seek, SeekFrom};
    let maps = std::fs::read_to_string(format!("/proc/{pid}/maps")).unwrap();
    let mut memory = std::fs::File::open(format!("/proc/{pid}/mem")).unwrap();
    for line in maps.lines() {
        let mut fields = line.split_whitespace();
        let (Some(range), Some(permissions)) = (fields.next(), fields.next()) else { continue };
        if !permissions.starts_with("rw") {
            continue;
        }
        let Some((start, end)) = range.split_once('-') else { continue };
        let (start, end) = (u64::from_str_radix(start, 16).unwrap(), u64::from_str_radix(end, 16).unwrap());
        let mut region = vec![0u8; (end - start) as usize];
        if memory.seek(SeekFrom::Start(start)).is_err() || memory.read_exact(&mut region).is_err() {
            continue;
        }
        if region.windows(needle.len()).any(|window| window == needle) {
            return true;
        }
    }
    false
}

/// One parent serves operation after operation for two tenants. It stays one process with one thread, and no
/// tenant's key is in its memory: each child reads its request itself.
///
/// `LIBCH_FORK_OPERATIONS` sets the operations, 200 by default; the plan's check runs 10,000.
#[tokio::test]
async fn a_parent_serves_many_operations_and_keeps_nothing_of_them() {
    let operations: u64 = std::env::var("LIBCH_FORK_OPERATIONS").ok().and_then(|count| count.parse().ok()).unwrap_or(200);
    let stack = stack().await;
    let (first, _) = tenant_with_parts(0).await;
    let (second, _) = tenant_with_parts(0).await;
    let (pool, scratch) = forking_pool(stack, 1, 1);
    let writers = [pool.writer(&first.tenant, &first.key), pool.writer(&second.tenant, &second.key)];

    let (mut sums, mut rows) = ([0u64; 2], [0u64; 2]);
    for operation in 0..operations {
        let tenant = (operation % 2) as usize;
        // Mostly describes, which are quick, and an insert every tenth operation for each tenant. The first
        // inserts are large enough to keep their data in S3, under names their children draw at random.
        if operation % 20 < 2 {
            let batch = batch(operation, if operation < 40 { LARGE } else { 100 });
            assert!(insert(&writers[tenant], &batch).await.unwrap().is_committed(), "operation {operation}");
            sums[tenant] += batch.sum;
            rows[tenant] += batch.rows;
        } else {
            assert_eq!(writers[tenant].describe(false).await.unwrap().tables.len(), 3, "operation {operation}");
        }
    }

    let stats = pool.stats();
    assert!(stats.started <= 2, "the pool's one parent served every operation: {stats:?}");
    let parents = parents(&scratch);
    assert_eq!(parents.len(), 1, "{parents:?}");
    assert_eq!(threads_of(parents[0]), 1, "the parent forks from its only thread");
    for fixture in [&first, &second] {
        assert!(!memory_holds(parents[0], fixture.key.as_bytes()), "the parent's memory holds a tenant's key");
    }
    // The scan finds what is there: the parent's memory does hold its own arguments.
    assert!(memory_holds(parents[0], b"--fork"));

    // Each child draws its own random numbers: no two blobs of the two tenants share a name. Children that
    // carried their parent's generator would each name their first blob alike.
    let mut names = Vec::new();
    for fixture in [&first, &second] {
        let prefix = fixture.tenant.s3.key_prefix();
        let objects = S3Client::new(&fixture.tenant.s3).unwrap().list(&prefix).await.unwrap();
        assert!(objects.len() >= 2, "the large batches keep blobs in S3: {objects:?}");
        names.extend(objects.into_iter().map(|object| object.key[prefix.len()..].to_string()));
    }
    let distinct: std::collections::HashSet<&String> = names.iter().collect();
    assert_eq!(distinct.len(), names.len(), "{names:?}");

    for (tenant, fixture) in [&first, &second].into_iter().enumerate() {
        assert_eq!(totals(fixture).await, (rows[tenant], sums[tenant], sums[tenant]), "tenant {tenant}");
        assert_clean(fixture).await;
    }
    println!("{operations} operations through one parent: {stats:?}");
}

/// Every operation runs through forking workers, several at a time.
#[tokio::test(flavor = "multi_thread")]
async fn operations_run_through_forking_workers() {
    let stack = stack().await;
    let (fixture, _) = tenant_with_parts(0).await;
    let (pool, _scratch) = forking_pool(stack, 4, 4);
    let writer = pool.writer(&fixture.tenant, &fixture.key);
    let batches: Vec<_> = (0..12).map(|index| batch(index, if index == 11 { 60_000 } else { 100 })).collect();
    let inserted = futures_util::future::join_all(batches.iter().map(|batch| insert(&writer, batch))).await;
    assert!(inserted.into_iter().all(|outcome| outcome.unwrap().is_committed()));

    let planned = writer.plan(&events(), &Budget::default()).await.unwrap();
    assert!(!planned.is_empty());
    for merge in &planned {
        assert!(writer.merge(&events(), &merge.candidate).await.unwrap().is_committed());
    }
    assert!(writer.housekeep(&events()).await.unwrap().is_committed());
    assert_eq!(insert(&writer, &batches[0]).await.unwrap(), Outcome::Duplicate, "a batch sent again finds its token");
    let sum = batches.iter().map(|batch| batch.sum).sum();
    assert_eq!(totals(&fixture).await, (60_000 + 11 * 100, sum, sum));
    assert!(pool.stats().started <= 8, "{:?}", pool.stats());
    assert_clean(&fixture).await;
}

/// An operation that passes its deadline takes its parent with it, and the child with the parent. The pool starts
/// another parent, and the batch goes again under its token.
#[tokio::test]
async fn a_deadline_kills_the_parent_and_its_child() {
    let stack = stack().await;
    let (fixture, _) = tenant_with_parts(1).await;
    let (pool, scratch) = forking_pool(stack, 1, 1);
    let writer = pool.writer(&fixture.tenant, &fixture.key);
    writer.describe(false).await.unwrap();
    let before = parents(&scratch);
    assert_eq!(before.len(), 1);

    let second = batch(1, 100);
    let held = writer.clone().with_settings_xml(libch_testkit::ops::fail_point("inodedb_insert_pause_before_commit")).with_deadline(Duration::from_secs(2));
    let killed = insert(&held, &second).await.unwrap_err();
    assert!(killed.to_string().contains("was killed"), "{killed}");

    // The parent is gone, and so is every process that carried its name: the child died with it.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!std::path::Path::new(&format!("/proc/{}", before[0])).exists() || threads_of(before[0]) == 0);
    assert!(insert(&writer, &second).await.unwrap().is_committed());
    let after = parents(&scratch);
    assert!(after.len() == 1 && after != before, "{before:?} then {after:?}");
    assert_eq!(totals(&fixture).await.0, 200);
    assert_clean(&fixture).await;
}

/// A forking worker in the sandbox: each child confines itself before it reads its request, and operations run
/// as they do outside.
#[tokio::test]
async fn forking_workers_run_in_the_sandbox() {
    let stack = stack().await;
    let (fixture, _) = tenant_with_parts(0).await;
    let pool = Pool::new(stack.sandboxed_worker_command().forking(), PoolOptions { waiting: 1, concurrency: 1 }).unwrap();
    let writer: OpWriter = pool.writer(&fixture.tenant, &fixture.key);
    let mut sum = 0;
    for index in 0..6 {
        let batch = batch(index, if index == 5 { 60_000 } else { 100 });
        sum += batch.sum;
        assert!(insert(&writer, &batch).await.unwrap().is_committed());
    }
    for merge in &writer.plan(&events(), &Budget::default()).await.unwrap() {
        assert!(writer.merge(&events(), &merge.candidate).await.unwrap().is_committed());
    }
    assert!(writer.housekeep(&events()).await.unwrap().is_committed());
    assert_eq!(totals(&fixture).await, (60_500, sum, sum));
    assert!(pool.stats().started <= 2, "{:?}", pool.stats());
    assert_clean(&fixture).await;
}
