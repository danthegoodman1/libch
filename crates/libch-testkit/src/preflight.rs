//! Checks that the environment can run libch's tests, and says what to do when it cannot.

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

use bytes::Bytes;
use libch::catalog::OwnerMode;
use libch::s3::S3Client;
use libch::stock::{Exclusive, StockWriter, StockWriterOptions};
use libch::writer::{Insert, TableName};
use libch::{Outcome, Writer};

use crate::{Stack, TenantFixture};

/// The BigHouse commit libch runs against, from `bighouse.rev`. A checkout at this commit or a later one passes.
pub const BIGHOUSE_REV: &str = include_str!("../../../bighouse.rev");

/// The commit of the fork's worker branch this code runs against, from `fork.rev`. A worktree at this commit or a
/// later one passes.
pub const FORK_REV: &str = include_str!("../../../fork.rev");

/// The protocol as this repository defines it. The fork compiles a copy.
pub const PROTOCOL: &str = include_str!("../../../proto/libch.proto");

/// The InodeDB commit the workspace builds its client from, read from the workspace's `Cargo.toml`.
pub fn inodedb_client_rev() -> &'static str {
    let manifest = include_str!("../../../Cargo.toml");
    let line = manifest.lines().find(|line| line.starts_with("inodedb = ")).expect("the workspace depends on inodedb");
    let after = line.split_once("rev = \"").expect("the inodedb dependency pins a rev").1;
    after.split_once('"').expect("the rev is quoted").0
}

#[derive(Debug, thiserror::Error)]
pub enum PreflightError {
    #[error("no BigHouse checkout at {0}: clone BigHouse there, or point BIGHOUSE_DIR at one")]
    NoCheckout(PathBuf),

    #[error("the BigHouse checkout at {dir} is at {head}, which does not include the pinned commit {pin}: update the checkout")]
    WrongCommit { dir: PathBuf, head: String, pin: String },

    #[error("the InodeDB client is built from {client}, and the checkout's inodedb submodule is at {server}: set the `inodedb` rev in Cargo.toml to match")]
    InodeDbMismatch { client: String, server: String },

    #[error("no worktree of the fork at {0}: make it with scripts/fork-setup")]
    NoFork(PathBuf),

    #[error("the fork at {dir} is at {head}, which does not include {bighouse}, the fork commit BigHouse has checked out: merge it into the worker's branch")]
    ForkBehind { dir: PathBuf, head: String, bighouse: String },

    #[error(
        "the fork at {dir} is at {head}, which does not include the pinned worker commit {pin}: update the worker's branch and rebuild with scripts/fork-build"
    )]
    ForkWithoutPin { dir: PathBuf, head: String, pin: String },

    #[error("{0} differs from proto/libch.proto: copy the protocol there and rebuild with scripts/fork-build")]
    ProtocolMismatch(PathBuf),

    #[error("no ClickHouse binary at {0}: build it with scripts/fork-setup and scripts/fork-build, or point LIBCH_CLICKHOUSE at one")]
    NoBinary(PathBuf),

    #[error("the ClickHouse binary at {path} does not run: {message}")]
    BrokenBinary { path: PathBuf, message: String },

    #[error("the ClickHouse binary at {path} does not start `libch-worker`: build the worker's branch with scripts/fork-build ({message})")]
    NoWorker { path: PathBuf, message: String },

    #[error("no InodeDB admin key at {0}: start the dev stack with scripts/dev-up")]
    NoAdminKey(PathBuf),

    #[error("InodeDB does not answer at {endpoint}: start the dev stack with scripts/dev-up ({message})")]
    NoInodeDb { endpoint: String, message: String },

    #[error("S3 does not answer at {endpoint}: start the dev stack with scripts/dev-up ({message})")]
    NoS3 { endpoint: String, message: String },

    #[error("the fork's shared writers do not behave as BigHouse documents: {0}")]
    SharedMode(String),
}

/// Checks the checkout, its commit, the fork's worktree, the binary and the dev stack. `pin` is the commit the
/// checkout must include.
pub async fn environment(stack: &Stack, pin: &str) -> Result<(), PreflightError> {
    checkout(&stack.bighouse_dir, pin)?;
    if let Some(fork_dir) = &stack.fork_dir {
        fork(fork_dir, &stack.bighouse_dir, FORK_REV.trim())?;
    }
    binary(&stack.clickhouse)?;
    worker(&stack.clickhouse)?;
    dev_stack(stack).await
}

/// Checks that the fork's worktree exists, includes the fork commit BigHouse has checked out and the worker commit
/// `pin`, and compiles this repository's protocol.
pub fn fork(dir: &Path, bighouse_dir: &Path, pin: &str) -> Result<(), PreflightError> {
    if !dir.join(".git").exists() {
        return Err(PreflightError::NoFork(dir.to_path_buf()));
    }
    let includes = |commit: &str| {
        let ancestor = Command::new("git").arg("-C").arg(dir).args(["merge-base", "--is-ancestor", commit, "HEAD"]).output();
        ancestor.is_ok_and(|output| output.status.success())
    };
    let head = || git(dir, &["rev-parse", "HEAD"]).unwrap_or_else(|| "an unknown commit".into());
    if let Some(bighouse) = git(bighouse_dir, &["rev-parse", "HEAD:ClickHouse"])
        && !includes(&bighouse)
    {
        return Err(PreflightError::ForkBehind { dir: dir.to_path_buf(), head: head(), bighouse });
    }
    if !pin.is_empty() && !includes(pin) {
        return Err(PreflightError::ForkWithoutPin { dir: dir.to_path_buf(), head: head(), pin: pin.to_string() });
    }
    let copy = dir.join("programs/libch-worker/libch.proto");
    if std::fs::read_to_string(&copy).ok().as_deref() != Some(PROTOCOL) {
        return Err(PreflightError::ProtocolMismatch(copy));
    }
    Ok(())
}

/// Checks that the checkout exists, includes the pinned commit, and pins the InodeDB commit the client is
/// built from.
pub fn checkout(dir: &Path, pin: &str) -> Result<(), PreflightError> {
    if !dir.join(".git").exists() {
        return Err(PreflightError::NoCheckout(dir.to_path_buf()));
    }
    let head = git(dir, &["rev-parse", "HEAD"]).unwrap_or_else(|| "an unknown commit".into());
    let includes_pin = Command::new("git").arg("-C").arg(dir).args(["merge-base", "--is-ancestor", pin, "HEAD"]).output();
    if !includes_pin.is_ok_and(|output| output.status.success()) {
        return Err(PreflightError::WrongCommit { dir: dir.to_path_buf(), head, pin: pin.to_string() });
    }
    if let Some(server) = git(dir, &["rev-parse", "HEAD:inodedb"])
        && server != inodedb_client_rev()
    {
        return Err(PreflightError::InodeDbMismatch { client: inodedb_client_rev().to_string(), server });
    }
    Ok(())
}

fn git(dir: &Path, args: &[&str]) -> Option<String> {
    let output = Command::new("git").arg("-C").arg(dir).args(args).output().ok()?;
    output.status.success().then(|| String::from_utf8_lossy(&output.stdout).trim().to_string())
}

/// Checks that the fork's binary exists and runs.
pub fn binary(path: &Path) -> Result<(), PreflightError> {
    if !path.is_file() {
        return Err(PreflightError::NoBinary(path.to_path_buf()));
    }
    let broken = |message: String| PreflightError::BrokenBinary { path: path.to_path_buf(), message };
    let output = Command::new(path).args(["local", "--query", "SELECT version()"]).output().map_err(|error| broken(error.to_string()))?;
    if output.status.success() { Ok(()) } else { Err(broken(String::from_utf8_lossy(&output.stderr).trim().to_string())) }
}

/// Checks that the binary holds the worker: one started with no request finishes its startup and leaves cleanly.
pub fn worker(path: &Path) -> Result<(), PreflightError> {
    let missing = |message: String| PreflightError::NoWorker { path: path.to_path_buf(), message };
    let output = Command::new(path).arg("libch-worker").stdin(Stdio::null()).output().map_err(|error| missing(error.to_string()))?;
    if output.status.success() {
        Ok(())
    } else {
        let log = String::from_utf8_lossy(&output.stderr);
        Err(missing(log.lines().last().unwrap_or("it exited without a word").trim().to_string()))
    }
}

/// Checks that InodeDB and S3 answer.
pub async fn dev_stack(stack: &Stack) -> Result<(), PreflightError> {
    if !stack.admin_key_file.is_file() {
        return Err(PreflightError::NoAdminKey(stack.admin_key_file.clone()));
    }
    let no_inodedb = |message: String| PreflightError::NoInodeDb { endpoint: stack.inodedb_endpoint.clone(), message };
    let admin = tokio::time::timeout(Duration::from_secs(10), stack.admin())
        .await
        .map_err(|_| no_inodedb("no answer in 10 s".into()))?
        .map_err(|error| no_inodedb(error.to_string()))?;
    tokio::time::timeout(Duration::from_secs(10), admin.list(""))
        .await
        .map_err(|_| no_inodedb("no answer in 10 s".into()))?
        .map_err(|error| no_inodedb(error.to_string()))?;

    let no_s3 = |message: String| PreflightError::NoS3 { endpoint: stack.s3_endpoint.clone(), message };
    let s3 = S3Client::new(&stack.s3("preflight")).map_err(|error| no_s3(error.to_string()))?;
    tokio::time::timeout(Duration::from_secs(10), s3.list("preflight/"))
        .await
        .map_err(|_| no_s3("no answer in 10 s".into()))?
        .map_err(|error| no_s3(error.to_string()))?;
    Ok(())
}

/// Checks that a shared writer from this binary leaves the coordination files BigHouse documents: a shared
/// `owner`, and for a table it inserted into, a block counter and one deduplication token for each block.
pub async fn shared_mode(stack: &Stack) -> Result<(), PreflightError> {
    let failed = |what: &str, error: &dyn std::fmt::Display| PreflightError::SharedMode(format!("{what}: {error}"));
    let env = stack.stock_env();
    let fixture = TenantFixture::create(stack).await.map_err(|error| failed("creating a tenant", &error))?;
    let table = TableName::new("preflight", "t");
    Exclusive::run(
        &env,
        &fixture.tenant,
        &fixture.key,
        &["CREATE DATABASE preflight", "CREATE TABLE preflight.t (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS non_replicated_deduplication_window = 100"],
    )
    .await
    .map_err(|error| failed("creating a table on an exclusive writer", &error))?;

    let writer = StockWriter::start(&env, &fixture.tenant, &fixture.key, &StockWriterOptions::default())
        .await
        .map_err(|error| failed("starting a writer with inodedb_writer_mode = shared", &error))?;
    for token in ["one", "two"] {
        let insert = Insert { table: &table, format: "JSONEachRow", token, data: Bytes::from_static(b"{\"k\": 1}\n") };
        match writer.insert(insert).await {
            Ok(Outcome::Committed(_)) => {}
            Ok(other) => return Err(PreflightError::SharedMode(format!("an insert on a shared writer ended {}", other.name()))),
            Err(error) => return Err(failed("an insert on a shared writer", &error)),
        }
    }
    writer.stop().await.map_err(|error| failed("stopping the shared writer", &error))?;

    let catalog = fixture.catalog().await.map_err(|error| failed("connecting to InodeDB", &error))?;
    let owner = catalog.owner().await.map_err(|error| failed("reading owner", &error))?;
    if owner.as_ref().map(|owner| owner.mode) != Some(OwnerMode::Shared) {
        return Err(PreflightError::SharedMode(format!("`owner` holds {owner:?}, where a shared epoch is expected")));
    }
    let definition = catalog.table(&table).await.map_err(|error| failed("reading the catalog", &error))?;
    let definition = definition.ok_or_else(|| PreflightError::SharedMode("the catalog has no table preflight.t".into()))?;
    let coordination = catalog.coordination(&definition).await.map_err(|error| failed("reading coord/", &error))?;
    let counter = coordination.counter.as_ref().map(|counter| counter.value);
    if counter != Some(2) {
        return Err(PreflightError::SharedMode(format!("coord/{}/block_number holds {counter:?} after two inserts", definition.data_path())));
    }
    let mut parts: Vec<&str> = coordination.tokens.iter().map(|token| token.part.as_str()).collect();
    parts.sort_unstable();
    if parts != ["all_1_1_0", "all_2_2_0"] {
        return Err(PreflightError::SharedMode(format!("coord/{}/tokens name the parts {parts:?} after two inserts", definition.data_path())));
    }
    fixture.remove().await.map_err(|error| failed("removing the tenant", &error))
}
