//! What tests of the worker's operations share: a tenant with parts, a merge by part names, the settings that
//! switch on a failpoint or shorten a claim, and the checks a test ends with.

use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use libch::catalog::TableParts;
use libch::worker::{OpWriter, WorkerCommand};
use libch::writer::{Insert, MergeCandidate, Merged, TableName};
use libch::{Error, Outcome, PartInfo, Writer, check};

use crate::schema::{Batch, EVENTS_SCHEMA, batch, events};
use crate::{Reader, TenantFixture, stack};

/// A tenant with the events schema and `batches` batches of 100 events, each inserted by a worker: parts
/// `202611_{n}_{n}_0` in `d.events` and `all_{n}_{n}_1` in `d.daily`.
pub async fn tenant_with_parts(batches: u64) -> (TenantFixture, OpWriter) {
    let stack = stack().await;
    let fixture = TenantFixture::create(stack).await.unwrap();
    fixture.exclusively(&EVENTS_SCHEMA).await.unwrap();
    let writer = OpWriter::new(stack.worker_command(), &fixture.tenant, &fixture.key);
    for index in 0..batches {
        assert!(insert(&writer, &batch(index, 100)).await.unwrap().is_committed());
    }
    (fixture, writer)
}

/// Inserts a batch into `d.events`.
pub async fn insert(writer: &OpWriter, batch: &Batch) -> Result<Outcome<libch::writer::Inserted>, Error> {
    writer.insert(Insert { table: &events(), format: "JSONEachRow", token: &batch.token, data: batch.data.clone() }).await
}

pub fn parts(names: &[&str]) -> Vec<PartInfo> {
    names.iter().map(|name| PartInfo::parse(name).unwrap_or_else(|| panic!("`{name}` is a part name"))).collect()
}

/// Merges the named parts of one partition.
pub async fn merge(writer: &OpWriter, table: &TableName, partition: &str, sources: &[&str]) -> Result<Outcome<Merged>, Error> {
    writer.merge(table, &MergeCandidate { partition_id: partition.into(), sources: parts(sources) }).await
}

/// What the table's directory holds now.
pub async fn stored(fixture: &TenantFixture, table: &TableName) -> TableParts {
    let catalog = fixture.catalog().await.unwrap();
    let definition = catalog.table(table).await.unwrap().unwrap();
    catalog.parts(&definition).await.unwrap()
}

pub fn active(parts: &TableParts) -> Vec<String> {
    parts.active().iter().map(PartInfo::name).collect()
}

/// The totals a reader finds: rows and sum of the events, and the sum through the view's target.
pub async fn totals(fixture: &TenantFixture) -> (u64, u64, u64) {
    let reader = Reader::open(stack().await, fixture).await.unwrap();
    let totals = (
        reader.number("SELECT count() FROM d.events").await.unwrap(),
        reader.number("SELECT sum(v) FROM d.events").await.unwrap(),
        reader.number("SELECT sumMerge(total) FROM d.daily").await.unwrap(),
    );
    reader.close().await.unwrap();
    totals
}

/// Runs the invariant checker, which reads the disk and changes nothing.
pub async fn assert_consistent(fixture: &TenantFixture) {
    let report = check::tenant(&fixture.tenant, &fixture.key).await.unwrap();
    assert!(report.is_clean(), "{report}");
}

/// Runs the invariant checker, then has a stock exclusive writer load the disk. That writer's start removes
/// every temporary directory, as a server's does. The disk goes back to shared writers afterwards.
pub async fn assert_clean(fixture: &TenantFixture) {
    assert_consistent(fixture).await;
    let rejected = check::stock_load(&stack().await.stock_env(), &fixture.tenant, &fixture.key).await.unwrap();
    fixture.share().await.unwrap();
    assert!(rejected.is_empty(), "a stock writer rejects: {rejected:?}");
}

pub fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
}

/// Settings that switch a failpoint on in a worker from its start. A worker stays at a pausing failpoint until
/// its deadline kills it; [`Pause`] holds one and lets it go.
pub fn fail_point(name: &str) -> String {
    format!("<fail_points_active><{name}>1</{name}></fail_points_active>")
}

/// A pausing failpoint a test holds a worker at and then lets go.
///
/// The worker's settings name a directory under the worker's own. Once the worker reaches the failpoint it
/// creates a file there, and it goes on when the test creates another. The test starts the worker from
/// [`Pause::command`], which keeps the worker's directory where the pause can find it.
pub struct Pause {
    name: String,
    /// Where the held worker's own directory is made.
    scratch: PathBuf,
}

/// The directory of the pause's files, under the worker's own.
const SIGNALS: &str = "signals";

impl Pause {
    /// A pause at the failpoint `name`.
    pub async fn at(name: &str) -> Pause {
        let scratch = stack().await.work_dir.join(format!("pause-{:016x}", rand::random::<u64>()));
        Pause { name: name.to_string(), scratch }
    }

    /// The stack's worker, with its directory where this pause looks for it.
    pub async fn command(&self) -> WorkerCommand {
        stack().await.worker_command().scratch(&self.scratch)
    }

    /// The settings that switch the failpoint on in a worker and tell it where to say it paused.
    pub fn settings(&self) -> String {
        format!("{}<fail_point_signals>{SIGNALS}</fail_point_signals>", fail_point(&self.name))
    }

    /// The pause's directory in the worker that holds one: under the worker's own directory, or under the
    /// directory of the child a forking worker serves in.
    fn signals(&self) -> Option<PathBuf> {
        let workers = std::fs::read_dir(&self.scratch).ok()?.flatten().map(|worker| worker.path());
        workers
            .flat_map(|worker| {
                let children = std::fs::read_dir(&worker).into_iter().flatten().flatten().map(|child| child.path().join(SIGNALS));
                std::iter::once(worker.join(SIGNALS)).chain(children).collect::<Vec<_>>()
            })
            .find(|signals| signals.is_dir())
    }

    /// Waits until a worker under these settings has stopped at the failpoint.
    pub async fn reached(&self) {
        let started = Instant::now();
        loop {
            if self.signals().is_some_and(|signals| signals.join(format!("{}.paused", self.name)).exists()) {
                return;
            }
            assert!(started.elapsed() < Duration::from_secs(60), "no worker reached {} within a minute", self.name);
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// Lets the held worker go on.
    pub fn resume(&self) {
        let signals = self.signals().expect("a worker holds the pause");
        std::fs::write(signals.join(format!("{}.resume", self.name)), "").expect("the resume file");
    }
}

impl Drop for Pause {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.scratch);
    }
}

/// Settings under which a worker's merge claims last `milliseconds` without renewal.
pub fn claim_ttl(milliseconds: u64) -> String {
    format!("<storage_configuration><disks><bighouse><inodedb_claim_ttl_ms>{milliseconds}</inodedb_claim_ttl_ms></bighouse></disks></storage_configuration>")
}

/// Settings under which a worker's tables keep covered parts for one second.
pub fn short_grace() -> String {
    "<merge_tree><old_parts_lifetime>1</old_parts_lifetime></merge_tree>".into()
}
