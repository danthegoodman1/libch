//! The crate's side of a worker: one process for each operation.
//!
//! A worker is a program from the BigHouse fork that attaches one tenant's catalog, runs one operation and
//! exits. [`OpWriter`] gives each call a worker of its own, sends it the request, streams an insert's batch
//! after it, and reads back one response. The worker is started for the call, or taken from a
//! [`Pool`](crate::pool::Pool) that started it ahead of time. `proto/libch.proto` defines the messages and the
//! frames that carry them.

use std::net::SocketAddrV4;
use std::os::unix::fs::DirBuilderExt;
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use libch_proto as pb;
use libch_proto::Message;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command};
use tokio::task::JoinHandle;

use crate::catalog::TableKind;
use crate::pool::Pool;
use crate::writer::{Budget, CommittedPart, Housekept, Insert, Inserted, MergeCandidate, Merged, Outcome, PlannedMerge, Remaining, TableName, Writer};
use crate::{Error, PartInfo, Tenant, TenantKey};

/// How much of a worker's log the crate keeps for an error message.
const LOG_KEPT: usize = 16 << 10;

/// What every worker starts with in its environment: the AWS SDK finds no credentials file, no configuration
/// file and no metadata service.
const NO_AMBIENT_CREDENTIALS: [(&str, &str); 3] =
    [("AWS_SHARED_CREDENTIALS_FILE", "/dev/null"), ("AWS_CONFIG_FILE", "/dev/null"), ("AWS_EC2_METADATA_DISABLED", "true")];

/// How long an operation may run before its worker is killed, unless the writer says otherwise.
pub const DEFAULT_DEADLINE: Duration = Duration::from_secs(600);

/// The program that serves an operation.
///
/// A worker's environment holds the variables the command names and no other, so nothing of the caller's own
/// environment, a cloud credential among it, reaches a process that runs a tenant's SQL. Three more keep the
/// AWS SDK inside the worker from looking for credentials of its own, in the caller's `~/.aws` files and at
/// the instance's metadata service: a worker's S3 credentials are the ones its request names.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerCommand {
    pub program: PathBuf,
    pub args: Vec<String>,
    pub envs: Vec<(String, String)>,
    /// Where workers keep their files: each gets a directory of its own under this one, which the crate makes
    /// before the worker starts and removes once it is gone.
    pub scratch: PathBuf,
    /// What confines each worker, when something does.
    pub sandbox: Option<Sandbox>,
    /// Whether a worker serves its operations one after another, each in a child it forks once its engine has
    /// started.
    pub forking: bool,
}

impl WorkerCommand {
    /// The worker inside the BigHouse fork's `clickhouse` binary.
    pub fn clickhouse(binary: impl Into<PathBuf>) -> WorkerCommand {
        WorkerCommand::new(binary).arg("libch-worker")
    }

    pub fn new(program: impl Into<PathBuf>) -> WorkerCommand {
        WorkerCommand {
            program: program.into(),
            args: Vec::new(),
            envs: Vec::new(),
            scratch: std::env::temp_dir().join("libch-workers"),
            sandbox: None,
            forking: false,
        }
    }

    pub fn arg(mut self, arg: impl Into<String>) -> WorkerCommand {
        self.args.push(arg.into());
        self
    }

    pub fn env(mut self, name: impl Into<String>, value: impl Into<String>) -> WorkerCommand {
        self.envs.push((name.into(), value.into()));
        self
    }

    /// Keeps each worker's directory under `directory`.
    pub fn scratch(mut self, directory: impl Into<PathBuf>) -> WorkerCommand {
        self.scratch = directory.into();
        self
    }

    /// Confines each worker before it does anything else. A worker that cannot enter its sandbox, on a kernel
    /// without Landlock ABI 6 (Linux 6.12) for one, fails where it starts.
    pub fn sandboxed(mut self, sandbox: Sandbox) -> WorkerCommand {
        self.sandbox = Some(sandbox);
        self
    }

    /// Has each worker serve its operations in children it forks. A fork hands the child the engine already
    /// started, so an operation pays for the fork where a worker of its own pays for the engine's start. A
    /// [`Pool`](crate::pool::Pool) keeps such workers and gives each one operation at a time; a writer without
    /// a pool starts one for each call. The child reads the request, so the parent never holds a tenant's key.
    pub fn forking(mut self) -> WorkerCommand {
        self.forking = true;
        self
    }

    /// The arguments a worker with the directory `scratch` starts with.
    fn arguments(&self, scratch: &Path) -> Vec<String> {
        let mut arguments = self.args.clone();
        arguments.push(format!("--path={}", scratch.display()));
        if self.forking {
            arguments.push("--fork".into());
        }
        if let Some(sandbox) = &self.sandbox {
            arguments.push("--sandbox".into());
            if !sandbox.connect_ports.is_empty() {
                let ports: Vec<String> = sandbox.connect_ports.iter().map(u16::to_string).collect();
                arguments.push(format!("--allow-connect={}", ports.join(",")));
            }
            arguments.extend(sandbox.read_paths.iter().map(|path| format!("--allow-read={}", path.display())));
        }
        arguments
    }

    /// Starts a sandboxed worker that serves no request: it tries each probe from inside its sandbox and says
    /// what happened. A deployment runs it once to see that the sandbox holds on its kernel.
    pub async fn probe(&self, probes: &[Probe]) -> Result<Vec<Probed>, Error> {
        let scratch = Scratch::make(&self.scratch)?;
        let mut command = Command::new(&self.program);
        command.args(self.arguments(scratch.path())).args(probes.iter().map(Probe::argument)).env_clear().envs(NO_AMBIENT_CREDENTIALS);
        command.envs(self.envs.iter().map(|(name, value)| (name.as_str(), value.as_str()))).stdin(Stdio::null()).kill_on_drop(true);
        let program = self.program.display();
        let output = command.output().await.map_err(|error| Error::Worker { message: format!("{program} did not start: {error}") })?;
        let failed =
            || Error::Worker { message: format!("{program} failed its probes with {}: {}", output.status, String::from_utf8_lossy(&output.stderr).trim()) };
        if !output.status.success() {
            return Err(failed());
        }
        let answers = String::from_utf8_lossy(&output.stdout);
        let errors: Vec<Option<i32>> =
            answers.lines().map(|line| line.rsplit_once(": ").and_then(|(_, outcome)| probe_outcome(outcome))).collect::<Option<_>>().ok_or_else(failed)?;
        if errors.len() != probes.len() {
            return Err(failed());
        }
        Ok(probes.iter().cloned().zip(errors).map(|(probe, error)| Probed { probe, error }).collect())
    }
}

/// A probe's outcome as the worker prints it: `ok`, or an error number and its name.
fn probe_outcome(outcome: &str) -> Option<Option<i32>> {
    if outcome == "ok" {
        return Some(None);
    }
    outcome.split(' ').next().and_then(|number| number.parse().ok()).map(Some)
}

/// What confines a worker: Landlock rules and a system call filter that the worker puts on itself before it
/// starts its engine.
///
/// A sandboxed worker reads the few system files an engine needs and `read_paths`, writes under its own
/// directory alone, executes nothing, connects by TCP to `connect_ports` alone and listens on none, opens no
/// Unix socket, signals and traces no other process, and makes none of the system calls that reach around
/// those rules. Landlock allows connections by port, so which hosts a worker reaches on those ports is for the
/// network the pool runs in to say.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Sandbox {
    /// The TCP ports a worker may connect to: its InodeDB store's and its S3 endpoint's.
    pub connect_ports: Vec<u16>,
    /// Files and directories a worker may read beside the system's own, such as a private CA bundle.
    pub read_paths: Vec<PathBuf>,
}

impl Sandbox {
    /// A sandbox that lets a worker reach an InodeDB store, as `10.0.0.5:7170`, and an S3 endpoint, as
    /// `https://s3.us-east-1.amazonaws.com`.
    pub fn reaching(store: &str, s3_endpoint: &str) -> Result<Sandbox, Error> {
        let store_port = store.rsplit_once(':').and_then(|(_, port)| port.parse().ok());
        let store_port = store_port.ok_or_else(|| Error::Other(format!("the store `{store}` names no port")))?;
        let endpoint = url::Url::parse(s3_endpoint).map_err(|error| Error::S3(format!("endpoint `{s3_endpoint}`: {error}")))?;
        let s3_port = endpoint.port_or_known_default().ok_or_else(|| Error::S3(format!("endpoint `{s3_endpoint}` names no port")))?;
        let mut connect_ports = vec![store_port, s3_port];
        connect_ports.dedup();
        Ok(Sandbox { connect_ports, read_paths: Vec::new() })
    }
}

/// Something a sandboxed worker tries, to show what its sandbox lets through.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Probe {
    /// Opens a file to read it.
    Read(PathBuf),
    /// Opens a file to write it, creating it when it is absent.
    Write(PathBuf),
    /// Runs a program.
    Execute(PathBuf),
    /// Connects by TCP.
    Connect(SocketAddrV4),
    /// Opens a Unix socket.
    UnixSocket,
    /// Asks whether it may signal a process.
    Signal(u32),
    /// Attaches to a process as a debugger does.
    Trace(u32),
}

impl Probe {
    fn argument(&self) -> String {
        match self {
            Probe::Read(path) => format!("--probe-read={}", path.display()),
            Probe::Write(path) => format!("--probe-write={}", path.display()),
            Probe::Execute(path) => format!("--probe-execute={}", path.display()),
            Probe::Connect(address) => format!("--probe-connect={address}"),
            Probe::UnixSocket => "--probe-unix=stream".into(),
            Probe::Signal(pid) => format!("--probe-signal={pid}"),
            Probe::Trace(pid) => format!("--probe-trace={pid}"),
        }
    }
}

/// What became of a probe.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Probed {
    pub probe: Probe,
    /// The error number the attempt met, as `EACCES`, or `None` when it went through.
    pub error: Option<i32>,
}

/// A worker's own directory, removed when the value is dropped.
#[derive(Debug)]
struct Scratch(PathBuf);

impl Scratch {
    /// Makes a directory under `base` that this user alone reaches.
    fn make(base: &Path) -> Result<Scratch, Error> {
        let directory = base.join(format!("worker-{:016x}", rand::random::<u64>()));
        let made = std::fs::create_dir_all(base).and_then(|()| std::fs::DirBuilder::new().mode(0o700).create(&directory));
        made.map_err(|error| Error::Worker { message: format!("a worker's directory {} could not be made: {error}", directory.display()) })?;
        Ok(Scratch(directory))
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// A tenant's catalog as a worker's engine sees it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Described {
    pub tables: Vec<DescribedTable>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DescribedTable {
    pub name: TableName,
    pub uuid: String,
    pub kind: TableKind,
    /// The storage engine, as `MergeTree`. `None` for a view.
    pub engine: Option<String>,
    /// The materialized views that read from this table, each with the table it writes to.
    pub views: Vec<ViewEdge>,
    /// The table's parts, when the call asked for them.
    pub parts: Vec<DescribedPart>,
}

/// A materialized view and the table it writes to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ViewEdge {
    pub view: TableName,
    pub target: TableName,
}

/// A part's directory name, and what the engine reads from it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DescribedPart {
    pub name: String,
    pub partition_id: String,
    pub min_block: i64,
    pub max_block: i64,
    pub level: u32,
    pub mutation: i64,
    pub active: bool,
}

/// What workers spent, as they counted it themselves, from each worker's start to its response.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Usage {
    /// How many operations the figures cover: every one that a worker answered, whatever the outcome.
    pub operations: u64,
    /// Calls to the tenant's InodeDB store, by kind.
    pub inodedb_commits: u64,
    pub inodedb_stats: u64,
    pub inodedb_reads: u64,
    pub inodedb_lists: u64,
    /// Requests to S3 that read, and requests that write.
    pub s3_reads: u64,
    pub s3_writes: u64,
    /// The workers' CPU time, user and system.
    pub cpu: Duration,
    /// The most anonymous memory, and the most threads, a worker held as it answered.
    pub peak_anonymous_memory_bytes: u64,
    pub peak_threads: u32,
}

impl Usage {
    /// Every call to the tenant's InodeDB store.
    pub fn inodedb_calls(&self) -> u64 {
        self.inodedb_commits + self.inodedb_stats + self.inodedb_reads + self.inodedb_lists
    }

    fn add(&mut self, worker: &pb::Usage) {
        self.operations += 1;
        self.inodedb_commits += u64::from(worker.inodedb_commits);
        self.inodedb_stats += u64::from(worker.inodedb_stats);
        self.inodedb_reads += u64::from(worker.inodedb_reads);
        self.inodedb_lists += u64::from(worker.inodedb_lists);
        self.s3_reads += u64::from(worker.s3_reads);
        self.s3_writes += u64::from(worker.s3_writes);
        self.cpu += Duration::from_micros(worker.cpu_microseconds);
        self.peak_anonymous_memory_bytes = self.peak_anonymous_memory_bytes.max(worker.anonymous_memory_bytes);
        self.peak_threads = self.peak_threads.max(worker.threads);
    }
}

/// Where a writer's workers come from.
#[derive(Clone, Debug)]
enum Workers {
    /// Each call starts its own.
    Started(WorkerCommand),
    /// Each call takes one a pool started ahead of time.
    Pooled(Pool),
}

/// Writes a tenant's disk through workers: one process for each operation.
#[derive(Clone, Debug)]
pub struct OpWriter {
    workers: Workers,
    tenant: Tenant,
    key: TenantKey,
    settings_xml: String,
    memory_limit: Option<u64>,
    deadline: Duration,
    at_least_once: bool,
    /// What this writer's workers have spent. Clones of the writer add to the same figures.
    usage: Arc<Mutex<Usage>>,
}

impl OpWriter {
    /// A writer for one tenant that starts a worker for each call. `key` holds the `write` grant on the
    /// tenant's directory.
    pub fn new(command: WorkerCommand, tenant: &Tenant, key: impl Into<TenantKey>) -> OpWriter {
        OpWriter::over(Workers::Started(command), tenant, key.into())
    }

    /// A writer for one tenant whose calls take their workers from `pool`.
    pub(crate) fn pooled(pool: Pool, tenant: &Tenant, key: TenantKey) -> OpWriter {
        OpWriter::over(Workers::Pooled(pool), tenant, key)
    }

    fn over(workers: Workers, tenant: &Tenant, key: TenantKey) -> OpWriter {
        OpWriter {
            workers,
            tenant: tenant.clone(),
            key,
            settings_xml: String::new(),
            memory_limit: None,
            deadline: DEFAULT_DEADLINE,
            at_least_once: false,
            usage: Arc::default(),
        }
    }

    /// Server settings every operation runs under, as the XML a stock writer boots with.
    pub fn with_settings_xml(mut self, settings_xml: impl Into<String>) -> OpWriter {
        self.settings_xml = settings_xml.into();
        self
    }

    /// The settings a worker boots with: the caller's document, and the writer's memory cap after it.
    fn settings_document(&self) -> String {
        match self.memory_limit {
            Some(bytes) => format!("{}<max_server_memory_usage>{bytes}</max_server_memory_usage>", self.settings_xml),
            None => self.settings_xml.clone(),
        }
    }

    /// Caps the memory each operation's worker may allocate, on top of the settings document. An operation that
    /// would pass the cap fails with ClickHouse's `MEMORY_LIMIT_EXCEEDED` (code 241) before its commit.
    pub fn with_memory_limit(mut self, bytes: u64) -> OpWriter {
        self.memory_limit = Some(bytes);
        self
    }

    /// How long an operation may run, from the moment its request goes out, before its worker is killed.
    pub fn with_deadline(mut self, deadline: Duration) -> OpWriter {
        self.deadline = deadline;
        self
    }

    /// Lets this writer insert into a table, or a view's target, that keeps no deduplication tokens, where a
    /// retried batch lands twice. Without it a worker refuses such an insert.
    pub fn at_least_once(mut self) -> OpWriter {
        self.at_least_once = true;
        self
    }

    /// What this writer's workers have spent so far, over every operation a worker answered.
    pub fn usage(&self) -> Usage {
        *self.usage.lock().expect("no panics holding the usage")
    }

    /// The tenant's catalog as the worker's engine sees it, with each table's parts when `parts` is set.
    pub async fn describe(&self, parts: bool) -> Result<Described, Error> {
        let response = self.call(pb::request::Operation::Describe(pb::Describe { parts }), None).await?;
        let outcome = outcome(response, |result| match result {
            pb::response::Result::Described(described) => Some(described),
            _ => None,
        })?;
        match outcome {
            Outcome::Committed(described) => described.tables.into_iter().map(described_table).collect::<Result<_, _>>().map(|tables| Described { tables }),
            other => Err(protocol(format!("answered a describe with `{}`", other.name()))),
        }
    }

    /// Runs one operation in a worker of its own, and returns the worker's response.
    async fn call(&self, operation: pb::request::Operation, data: Option<&Bytes>) -> Result<pb::Response, Error> {
        let request = pb::Request {
            tenant: Some(pb::Tenant {
                inodedb_endpoint: self.tenant.store.clone(),
                inodedb_key: self.key.to_string(),
                s3: Some(pb::S3 {
                    endpoint: self.tenant.s3.endpoint.clone(),
                    bucket: self.tenant.s3.bucket.clone(),
                    prefix: self.tenant.s3.prefix.clone(),
                    region: self.tenant.s3.region.clone(),
                    access_key: self.tenant.s3.access_key.clone(),
                    secret_key: self.tenant.s3.secret_key.clone(),
                }),
            }),
            settings_xml: self.settings_document(),
            operation: Some(operation),
        };
        let response = match &self.workers {
            Workers::Started(command) if command.forking => Worker::start(command)?.serve(&request, data, self.deadline).await.0,
            Workers::Started(command) => Worker::start(command)?.run(&request, data, self.deadline).await,
            Workers::Pooled(pool) => pool.run(&request, data, self.deadline).await,
        }?;
        if let Some(usage) = &response.usage {
            self.usage.lock().expect("no panics holding the usage").add(usage);
        }
        Ok(response)
    }
}

/// A worker process that has not been given its request yet.
///
/// Dropping it kills the process.
pub(crate) struct Worker {
    program: String,
    child: Child,
    stdin: ChildStdin,
    stdout: ChildStdout,
    /// The end of the worker's log, collected from the moment it starts, so a worker that logs while it waits
    /// for a request never fills its pipe.
    log: Arc<Log>,
    /// The task that collects it, which ends when the worker's stderr does.
    logging: JoinHandle<()>,
    /// The worker's directory, which goes after the process: the fields drop in this order, and dropping the
    /// child kills it.
    scratch: Scratch,
}

impl Worker {
    /// Starts a worker. The caller is inside a Tokio runtime.
    pub(crate) fn start(command: &WorkerCommand) -> Result<Worker, Error> {
        let program = command.program.display().to_string();
        let scratch = Scratch::make(&command.scratch)?;
        let mut child = Command::new(&command.program)
            .args(command.arguments(scratch.path()))
            .env_clear()
            .envs(NO_AMBIENT_CREDENTIALS)
            .envs(command.envs.iter().map(|(name, value)| (name.as_str(), value.as_str())))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .map_err(|error| Error::Worker { message: format!("{program} did not start: {error}") })?;
        let stdin = child.stdin.take().expect("stdin is piped");
        let stdout = child.stdout.take().expect("stdout is piped");
        let log = Arc::new(Log::default());
        let logging = tokio::spawn(keep_log(child.stderr.take().expect("stderr is piped"), log.clone()));
        Ok(Worker { program, child, stdin, stdout, log, logging, scratch })
    }

    /// Whether the process has ended, as a worker that failed to start up has.
    pub(crate) fn has_exited(&mut self) -> bool {
        !matches!(self.child.try_wait(), Ok(None))
    }

    /// Kills the process and waits for it to go.
    pub(crate) async fn kill(mut self) {
        let _ = self.child.kill().await;
    }

    /// Gives the worker its request, and the batch after it, and returns its response. The worker serves this one
    /// operation and exits.
    pub(crate) async fn run(self, request: &pb::Request, data: Option<&Bytes>, deadline: Duration) -> Result<pb::Response, Error> {
        // The directory is bound first, so it is dropped last: after the child, which a return may kill.
        let Worker { scratch: _scratch, program, mut child, mut stdin, stdout, log, logging } = self;
        let mut stdout = stdout.take((pb::FRAME_HEADER + pb::MAX_FRAME + 1) as u64);

        // The worker may answer before it has read everything, and it may fill a pipe while the request is
        // still going out, so the streams move together.
        let send = async move {
            send_request(&mut stdin, request, data).await?;
            stdin.shutdown().await
        };
        let receive = async {
            let mut answer = Vec::new();
            stdout.read_to_end(&mut answer).await.map(|_| answer)
        };
        let exchange = async {
            let (sent, answer) = tokio::join!(send, receive);
            let status = child.wait().await;
            let _ = logging.await;
            (sent, answer, status)
        };

        let Ok((sent, answer, status)) = tokio::time::timeout(deadline, exchange).await else {
            // Returning drops the child, which kills it.
            return Err(Error::Worker { message: format!("{program} did not finish within {deadline:?} and was killed") });
        };
        let log = String::from_utf8_lossy(&log.take()).into_owned();
        let status = status.map_err(|error| Error::Worker { message: format!("{program} could not be waited for: {error}") })?;
        let answer = answer.map_err(|error| Error::Worker { message: format!("{program}'s answer could not be read: {error}") })?;
        decode(status, &answer).map_err(|problem| {
            let unsent = sent.err().map(|error| format!("; the request did not all go out: {error}")).unwrap_or_default();
            Error::Worker { message: format!("{program} {problem}{unsent}{}", log_tail(&log)) }
        })
    }

    /// Gives a forking worker one request, and the batch after it, and returns the response of the child that
    /// served it, with whether the worker can serve another. It can once the child's exit has been read, whatever
    /// became of the operation; after a deadline or a broken stream the caller drops the worker, which kills it
    /// and the child with it.
    pub(crate) async fn serve(&mut self, request: &pb::Request, data: Option<&Bytes>, deadline: Duration) -> (Result<pb::Response, Error>, bool) {
        let program = self.program.clone();
        self.log.take();
        let (stdin, stdout) = (&mut self.stdin, &mut self.stdout);
        let send = async move {
            send_request(stdin, request, data).await?;
            stdin.flush().await
        };
        // The child's response frame, when it wrote one, and then the parent's word on how the child ended.
        let receive = async move {
            let mut answer = Vec::new();
            loop {
                let mut header = [0u8; pb::FRAME_HEADER];
                stdout.read_exact(&mut header).await?;
                if header == pb::FORK_TRAILER {
                    let mut status = [0u8; 4];
                    stdout.read_exact(&mut status).await?;
                    return Ok::<_, std::io::Error>((answer, ExitStatus::from_raw(i32::from_le_bytes(status))));
                }
                let length = pb::frame_length(header)?;
                let start = answer.len();
                answer.extend_from_slice(&header);
                answer.resize(start + pb::FRAME_HEADER + length, 0);
                stdout.read_exact(&mut answer[start + pb::FRAME_HEADER..]).await?;
            }
        };

        let Ok((sent, received)) = tokio::time::timeout(deadline, async { tokio::join!(send, receive) }).await else {
            return (Err(Error::Worker { message: format!("{program} did not finish within {deadline:?} and was killed") }), false);
        };
        let (answer, status) = match received {
            Ok(received) => received,
            Err(error) => {
                let log = String::from_utf8_lossy(&self.log.take()).into_owned();
                return (Err(Error::Worker { message: format!("{program}'s answer could not be read: {error}{}", log_tail(&log)) }), false);
            }
        };
        let log = self.log.take_operation().await;
        let response = decode(status, &answer).map_err(|problem| {
            let unsent = sent.err().map(|error| format!("; the request did not all go out: {error}")).unwrap_or_default();
            Error::Worker { message: format!("{program}'s child {problem}{unsent}{}", log_tail(&String::from_utf8_lossy(&log))) }
        });
        (response, true)
    }
}

/// Writes a request's frame, and after it the batch in data frames with the empty frame that ends them.
async fn send_request(stdin: &mut ChildStdin, request: &pb::Request, data: Option<&Bytes>) -> std::io::Result<()> {
    stdin.write_all(&pb::message_frame(request)).await?;
    if let Some(data) = data {
        for chunk in data.chunks(pb::DATA_FRAME) {
            stdin.write_all(&(chunk.len() as u32).to_le_bytes()).await?;
            stdin.write_all(chunk).await?;
        }
        stdin.write_all(&0u32.to_le_bytes()).await?;
    }
    Ok(())
}

/// The end of a worker's log as it arrives.
#[derive(Default)]
struct Log {
    kept: Mutex<Vec<u8>>,
    grew: tokio::sync::Notify,
}

impl Log {
    fn kept(&self) -> std::sync::MutexGuard<'_, Vec<u8>> {
        self.kept.lock().expect("no panics holding the log")
    }

    /// Takes what has arrived.
    fn take(&self) -> Vec<u8> {
        std::mem::take(&mut *self.kept())
    }

    /// Takes the log of the operation a forking worker just served: what arrived up to the mark the worker
    /// writes after each child. The mark is in the pipe by the time the child's exit has been read, so the wait
    /// for it is short, and bounded.
    async fn take_operation(&self) -> Vec<u8> {
        let marked = async {
            loop {
                let grew = self.grew.notified();
                if self.kept().contains(&pb::FORK_LOG_END) {
                    return;
                }
                grew.await;
            }
        };
        let _ = tokio::time::timeout(Duration::from_millis(200), marked).await;
        let mut kept = self.kept();
        match kept.iter().position(|&byte| byte == pb::FORK_LOG_END) {
            Some(mark) => {
                let rest = kept.split_off(mark + 1);
                let mut operation = std::mem::replace(&mut *kept, rest);
                operation.pop();
                operation
            }
            None => std::mem::take(&mut *kept),
        }
    }
}

/// Reads a worker's log to its end, and keeps the last of it.
async fn keep_log(mut stderr: ChildStderr, log: Arc<Log>) {
    let mut buffer = [0u8; 4096];
    while let Ok(read) = stderr.read(&mut buffer).await {
        if read == 0 {
            break;
        }
        {
            let mut kept = log.kept();
            kept.extend_from_slice(&buffer[..read]);
            if kept.len() > 2 * LOG_KEPT {
                let excess = kept.len() - LOG_KEPT;
                kept.drain(..excess);
            }
        }
        log.grew.notify_waiters();
    }
    let mut kept = log.kept();
    let excess = kept.len().saturating_sub(LOG_KEPT);
    kept.drain(..excess);
}

/// The response a finished worker left on its stdout, or what is wrong with what it left.
fn decode(status: ExitStatus, answer: &[u8]) -> Result<pb::Response, String> {
    if !status.success() {
        return Err(format!("failed with {status}"));
    }
    if answer.len() < pb::FRAME_HEADER {
        return Err(format!("exited without a response: it wrote {} bytes", answer.len()));
    }
    let header: [u8; pb::FRAME_HEADER] = answer[..pb::FRAME_HEADER].try_into().expect("the slice has the header's length");
    let length = pb::frame_length(header).map_err(|error| format!("wrote no response frame: {error}"))?;
    if answer.len() != pb::FRAME_HEADER + length {
        return Err(format!("wrote {} bytes where one response frame of {} is expected", answer.len(), pb::FRAME_HEADER + length));
    }
    pb::Response::decode(&answer[pb::FRAME_HEADER..]).map_err(|error| format!("wrote a response that does not decode: {error}"))
}

/// The end of a worker's log, for an error message.
fn log_tail(log: &str) -> String {
    let log = log.trim();
    if log.is_empty() {
        return String::new();
    }
    let start = log.len().saturating_sub(2_000);
    let start = (start..=log.len()).find(|&at| log.is_char_boundary(at)).unwrap_or(log.len());
    format!("; its log ends: {}", &log[start..])
}

fn protocol(problem: String) -> Error {
    Error::Worker { message: format!("the worker {problem}") }
}

/// Turns a response into the outcome it reports. `take` picks the result this operation expects.
fn outcome<T>(response: pb::Response, take: impl FnOnce(pb::response::Result) -> Option<T>) -> Result<Outcome<T>, Error> {
    match pb::Status::try_from(response.status) {
        Ok(pb::Status::Ok) => match response.result.and_then(take) {
            Some(result) => Ok(Outcome::Committed(result)),
            None => Err(protocol("answered OK without the operation's result".into())),
        },
        Ok(pb::Status::Duplicate) => Ok(Outcome::Duplicate),
        Ok(pb::Status::SchemaChanged) => Ok(Outcome::SchemaChanged),
        Ok(pb::Status::Fenced) => Ok(Outcome::Fenced),
        Ok(pb::Status::ClaimLost) => Ok(Outcome::ClaimLost),
        Ok(pb::Status::Refused) => Ok(Outcome::Refused(response.message)),
        Ok(pb::Status::Failed) if response.exception_code != 0 => Err(Error::ClickHouse { code: response.exception_code, message: response.message }),
        Ok(pb::Status::Failed) => Err(Error::Worker { message: format!("the operation failed: {}", response.message) }),
        Ok(pb::Status::Unspecified) | Err(_) => Err(protocol(format!("answered with the unknown status {}", response.status))),
    }
}

fn part(name: &str) -> Result<PartInfo, Error> {
    PartInfo::parse(name).ok_or_else(|| protocol(format!("named `{name}`, which is no part name")))
}

fn table_name(name: Option<pb::TableName>) -> Result<TableName, Error> {
    name.map(|name| TableName::new(name.database, name.table)).ok_or_else(|| protocol("left a table's name out of its answer".into()))
}

fn pb_table(name: &TableName) -> Option<pb::TableName> {
    Some(pb::TableName { database: name.database.clone(), table: name.table.clone() })
}

fn described_table(table: pb::Table) -> Result<DescribedTable, Error> {
    let kind = match pb::table::Kind::try_from(table.kind) {
        Ok(pb::table::Kind::Table) => TableKind::Table,
        Ok(pb::table::Kind::MaterializedView) => TableKind::MaterializedView,
        Ok(pb::table::Kind::View) => TableKind::View,
        Ok(pb::table::Kind::Dictionary) => TableKind::Dictionary,
        Ok(pb::table::Kind::Unspecified) | Err(_) => TableKind::Other,
    };
    let views =
        table.views.into_iter().map(|edge| Ok(ViewEdge { view: table_name(edge.view)?, target: table_name(edge.target)? })).collect::<Result<_, Error>>()?;
    let parts = table
        .parts
        .into_iter()
        .map(|part| DescribedPart {
            name: part.name,
            partition_id: part.partition_id,
            min_block: part.min_block,
            max_block: part.max_block,
            level: part.level,
            mutation: part.mutation,
            active: part.active,
        })
        .collect();
    Ok(DescribedTable { name: table_name(table.name)?, uuid: table.uuid, kind, engine: (!table.engine.is_empty()).then_some(table.engine), views, parts })
}

impl Writer for OpWriter {
    async fn insert(&self, insert: Insert<'_>) -> Result<Outcome<Inserted>, Error> {
        let operation =
            pb::Insert { table: pb_table(insert.table), format: insert.format.to_string(), token: insert.token.to_string(), at_least_once: self.at_least_once };
        let response = self.call(pb::request::Operation::Insert(operation), Some(&insert.data)).await?;
        let outcome = outcome(response, |result| match result {
            pb::response::Result::Inserted(inserted) => Some(inserted),
            _ => None,
        })?;
        let Outcome::Committed(inserted) = outcome else { return Ok(retype(outcome)) };
        let counter_conflicts = u64::from(inserted.counter_conflicts);
        let parts = inserted.parts.into_iter().map(|committed| {
            Ok(CommittedPart { table: table_name(committed.table)?, part: part(&committed.part)?, rows: committed.rows, bytes: committed.bytes })
        });
        Ok(Outcome::Committed(Inserted { parts: Some(parts.collect::<Result<_, Error>>()?), counter_conflicts }))
    }

    async fn plan(&self, table: &TableName, budget: &Budget) -> Result<Vec<PlannedMerge>, Error> {
        let operation = pb::Plan { table: pb_table(table), max_merges: u32::try_from(budget.max_merges).unwrap_or(u32::MAX) };
        let response = self.call(pb::request::Operation::Plan(operation), None).await?;
        let outcome = outcome(response, |result| match result {
            pb::response::Result::Planned(planned) => Some(planned),
            _ => None,
        })?;
        let Outcome::Committed(planned) = outcome else { return Err(protocol(format!("answered a plan with `{}`", outcome.name()))) };
        planned
            .candidates
            .into_iter()
            .map(|candidate| {
                let sources = candidate.sources.iter().map(|source| part(source)).collect::<Result<_, _>>()?;
                Ok(PlannedMerge { candidate: MergeCandidate { partition_id: candidate.partition_id, sources }, rows: candidate.rows, bytes: candidate.bytes })
            })
            .collect()
    }

    async fn merge(&self, table: &TableName, candidate: &MergeCandidate) -> Result<Outcome<Merged>, Error> {
        let operation =
            pb::Merge { table: pb_table(table), partition_id: candidate.partition_id.clone(), sources: candidate.sources.iter().map(PartInfo::name).collect() };
        let response = self.call(pb::request::Operation::Merge(operation), None).await?;
        let outcome = outcome(response, |result| match result {
            pb::response::Result::Merged(merged) => Some(merged),
            _ => None,
        })?;
        let Outcome::Committed(merged) = outcome else { return Ok(retype(outcome)) };
        Ok(Outcome::Committed(Merged { part: part(&merged.part)? }))
    }

    async fn housekeep(&self, table: &TableName) -> Result<Outcome<Housekept>, Error> {
        let response = self.call(pb::request::Operation::Housekeep(pb::Housekeep { table: pb_table(table) }), None).await?;
        let outcome = outcome(response, |result| match result {
            pb::response::Result::Housekept(housekept) => Some(housekept),
            _ => None,
        })?;
        let Outcome::Committed(housekept) = outcome else { return Ok(retype(outcome)) };
        let temporary_directories = u64::from(housekept.temporary_directories_removed);
        let removed = housekept.removed.iter().map(|name| part(name)).collect::<Result<_, _>>()?;
        let remaining =
            housekept.remaining.into_iter().map(|left| Ok(Remaining { part: part(&left.part)?, reason: left.reason })).collect::<Result<_, Error>>()?;
        let (blobs_removed, blobs_remain) = (housekept.blobs_removed, housekept.blobs_remain);
        Ok(Outcome::Committed(Housekept { removed, remaining, temporary_directories, blobs_removed, blobs_remain }))
    }
}

/// Carries an outcome that committed nothing over to another operation's result type.
fn retype<T, U>(outcome: Outcome<T>) -> Outcome<U> {
    match outcome {
        Outcome::Committed(_) => unreachable!("a committed outcome carries its own result"),
        Outcome::Duplicate => Outcome::Duplicate,
        Outcome::SchemaChanged => Outcome::SchemaChanged,
        Outcome::Fenced => Outcome::Fenced,
        Outcome::ClaimLost => Outcome::ClaimLost,
        Outcome::Refused(reason) => Outcome::Refused(reason),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn response(status: pb::Status) -> pb::Response {
        pb::Response { status: status as i32, ..pb::Response::default() }
    }

    fn inserted(result: pb::response::Result) -> Option<pb::Inserted> {
        match result {
            pb::response::Result::Inserted(inserted) => Some(inserted),
            _ => None,
        }
    }

    #[test]
    fn statuses_become_outcomes() {
        let ok = pb::Response { result: Some(pb::response::Result::Inserted(pb::Inserted::default())), ..response(pb::Status::Ok) };
        assert!(outcome(ok, inserted).unwrap().is_committed());
        assert_eq!(outcome(response(pb::Status::Duplicate), inserted).unwrap(), Outcome::Duplicate);
        assert_eq!(outcome(response(pb::Status::SchemaChanged), inserted).unwrap(), Outcome::SchemaChanged);
        assert_eq!(outcome(response(pb::Status::Fenced), inserted).unwrap(), Outcome::Fenced);
        assert_eq!(outcome(response(pb::Status::ClaimLost), inserted).unwrap(), Outcome::ClaimLost);
        let refused = pb::Response { message: "no tokens".into(), ..response(pb::Status::Refused) };
        assert_eq!(outcome(refused, inserted).unwrap(), Outcome::Refused("no tokens".into()));
    }

    #[test]
    fn failures_and_malformed_answers_become_errors() {
        let exception = pb::Response { message: "Table d.t does not exist".into(), exception_code: 60, ..response(pb::Status::Failed) };
        assert_eq!(outcome(exception, inserted).unwrap_err().clickhouse_code(), Some(60));
        let failed = pb::Response { message: "out of memory".into(), ..response(pb::Status::Failed) };
        assert!(matches!(outcome(failed, inserted).unwrap_err(), Error::Worker { message } if message.contains("out of memory")));

        assert!(outcome(response(pb::Status::Ok), inserted).is_err(), "OK needs a result");
        let wrong = pb::Response { result: Some(pb::response::Result::Merged(pb::Merged::default())), ..response(pb::Status::Ok) };
        assert!(outcome(wrong, inserted).is_err(), "OK needs this operation's result");
        assert!(outcome(response(pb::Status::Unspecified), inserted).is_err());
        assert!(outcome(pb::Response { status: 99, ..pb::Response::default() }, inserted).is_err());
    }

    #[test]
    fn an_answer_is_one_whole_frame_from_a_clean_exit() {
        let ok = ExitStatus::from_raw(0);
        let frame = pb::message_frame(&response(pb::Status::Duplicate));
        assert_eq!(decode(ok, &frame).unwrap().status, pb::Status::Duplicate as i32);

        assert!(decode(ExitStatus::from_raw(3 << 8), &frame).unwrap_err().contains("failed with"));
        assert!(decode(ok, b"").unwrap_err().contains("without a response"));
        assert!(decode(ok, &frame[..frame.len() - 1]).unwrap_err().contains("one response frame"));
        let mut twice = frame.clone();
        twice.extend_from_slice(&frame);
        assert!(decode(ok, &twice).unwrap_err().contains("one response frame"));
        assert!(decode(ok, b"Code: 60. DB::Exception: no such table").unwrap_err().contains("no response frame"));
        let mut garbled = 3u32.to_le_bytes().to_vec();
        garbled.extend_from_slice(&[0xff, 0xff, 0xff]);
        assert!(decode(ok, &garbled).unwrap_err().contains("does not decode"));
    }

    #[test]
    fn usage_sums_calls_and_keeps_peaks() {
        let mut usage = Usage::default();
        let worker = |commits, memory, threads| pb::Usage {
            inodedb_commits: commits,
            inodedb_stats: 2,
            inodedb_reads: 3,
            inodedb_lists: 1,
            s3_reads: 4,
            s3_writes: 5,
            cpu_microseconds: 1_500,
            anonymous_memory_bytes: memory,
            threads,
        };
        usage.add(&worker(2, 40 << 20, 9));
        usage.add(&worker(1, 30 << 20, 12));
        assert_eq!((usage.operations, usage.inodedb_commits, usage.inodedb_calls()), (2, 3, 15));
        assert_eq!((usage.s3_reads, usage.s3_writes, usage.cpu), (8, 10, Duration::from_millis(3)));
        assert_eq!((usage.peak_anonymous_memory_bytes, usage.peak_threads), (40 << 20, 12));
    }

    #[test]
    fn the_command_names_the_fork_worker() {
        let command = WorkerCommand::clickhouse("/opt/bighouse/clickhouse").env("TZ", "UTC");
        assert_eq!(command.program, PathBuf::from("/opt/bighouse/clickhouse"));
        assert_eq!(command.args, ["libch-worker"]);
        assert_eq!(command.envs, [("TZ".to_string(), "UTC".to_string())]);
        assert_eq!(command.arguments(Path::new("/scratch/w1")), ["libch-worker", "--path=/scratch/w1"]);
    }

    #[test]
    fn a_sandbox_names_its_ports_and_paths_to_the_worker() {
        let sandbox = Sandbox::reaching("10.0.0.5:7170", "https://s3.us-east-1.amazonaws.com").unwrap();
        assert_eq!(sandbox.connect_ports, [7170, 443]);
        assert_eq!(Sandbox::reaching("127.0.0.1:7170", "http://127.0.0.1:11211").unwrap().connect_ports, [7170, 11211]);
        assert!(Sandbox::reaching("inodedb", "http://127.0.0.1:11211").is_err(), "a store without a port");

        let sandbox = Sandbox { read_paths: vec!["/etc/corp-ca.pem".into()], ..sandbox };
        let command = WorkerCommand::clickhouse("/opt/bighouse/clickhouse").sandboxed(sandbox);
        assert_eq!(
            command.arguments(Path::new("/scratch/w1")),
            ["libch-worker", "--path=/scratch/w1", "--sandbox", "--allow-connect=7170,443", "--allow-read=/etc/corp-ca.pem"]
        );
    }

    #[test]
    fn probes_name_themselves_and_read_their_outcomes() {
        assert_eq!(Probe::Read("/etc/passwd".into()).argument(), "--probe-read=/etc/passwd");
        assert_eq!(Probe::Connect("127.0.0.1:9000".parse().unwrap()).argument(), "--probe-connect=127.0.0.1:9000");
        assert_eq!(Probe::Signal(1).argument(), "--probe-signal=1");
        assert_eq!(probe_outcome("ok"), Some(None));
        assert_eq!(probe_outcome("13 Permission denied"), Some(Some(13)));
        assert_eq!(probe_outcome("denied"), None);
    }

    #[test]
    fn a_scratch_directory_goes_with_its_value() {
        let base = std::env::temp_dir().join(format!("libch-scratch-test-{:08x}", rand::random::<u32>()));
        let scratch = Scratch::make(&base).unwrap();
        let path = scratch.path().to_path_buf();
        assert!(path.is_dir() && path.starts_with(&base));
        drop(scratch);
        assert!(!path.exists());
        std::fs::remove_dir(&base).unwrap();
    }

    #[test]
    fn a_log_tail_is_empty_for_a_quiet_worker() {
        assert_eq!(log_tail("  \n"), "");
        assert_eq!(log_tail("one line\n"), "; its log ends: one line");
        assert!(log_tail(&"x".repeat(5_000)).len() < 2_100);
    }
}
