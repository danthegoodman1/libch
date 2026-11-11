//! The crate's side of a worker: one process for each operation.
//!
//! A worker is a program from the BigHouse fork that attaches one tenant's catalog, runs one operation and
//! exits. [`OpWriter`] gives each call a worker of its own, sends it the request, streams an insert's batch
//! after it, and reads back one response. The worker is started for the call, or taken from a
//! [`Pool`](crate::pool::Pool) that started it ahead of time. `proto/libch.proto` defines the messages and the
//! frames that carry them.

use std::path::PathBuf;
use std::process::{ExitStatus, Stdio};
use std::time::Duration;

use bytes::Bytes;
use libch_proto as pb;
use libch_proto::Message;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command};
use tokio::task::JoinHandle;

use crate::catalog::TableKind;
use crate::pool::Pool;
use crate::writer::{Budget, CommittedPart, Housekept, Insert, Inserted, MergeCandidate, Merged, Outcome, Remaining, TableName, Writer};
use crate::{Error, PartInfo, Tenant, TenantKey};

/// How much of a worker's log the crate keeps for an error message.
const LOG_KEPT: usize = 16 << 10;

/// How long an operation may run before its worker is killed, unless the writer says otherwise.
pub const DEFAULT_DEADLINE: Duration = Duration::from_secs(600);

/// The program that serves an operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerCommand {
    pub program: PathBuf,
    pub args: Vec<String>,
    pub envs: Vec<(String, String)>,
}

impl WorkerCommand {
    /// The worker inside the BigHouse fork's `clickhouse` binary.
    pub fn clickhouse(binary: impl Into<PathBuf>) -> WorkerCommand {
        WorkerCommand::new(binary).arg("libch-worker")
    }

    pub fn new(program: impl Into<PathBuf>) -> WorkerCommand {
        WorkerCommand { program: program.into(), args: Vec::new(), envs: Vec::new() }
    }

    pub fn arg(mut self, arg: impl Into<String>) -> WorkerCommand {
        self.args.push(arg.into());
        self
    }

    pub fn env(mut self, name: impl Into<String>, value: impl Into<String>) -> WorkerCommand {
        self.envs.push((name.into(), value.into()));
        self
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
    deadline: Duration,
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
        OpWriter { workers, tenant: tenant.clone(), key, settings_xml: String::new(), deadline: DEFAULT_DEADLINE }
    }

    /// Server settings every operation runs under, as the XML a stock writer boots with.
    pub fn with_settings_xml(mut self, settings_xml: impl Into<String>) -> OpWriter {
        self.settings_xml = settings_xml.into();
        self
    }

    /// How long an operation may run, from the moment its request goes out, before its worker is killed.
    pub fn with_deadline(mut self, deadline: Duration) -> OpWriter {
        self.deadline = deadline;
        self
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
            settings_xml: self.settings_xml.clone(),
            operation: Some(operation),
        };
        match &self.workers {
            Workers::Started(command) => Worker::start(command)?.run(&request, data, self.deadline).await,
            Workers::Pooled(pool) => pool.run(&request, data, self.deadline).await,
        }
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
    /// Collects the end of the worker's log from the moment it starts, so a worker that logs while it waits
    /// for a request never fills its pipe.
    log: JoinHandle<Vec<u8>>,
}

impl Worker {
    /// Starts a worker. The caller is inside a Tokio runtime.
    pub(crate) fn start(command: &WorkerCommand) -> Result<Worker, Error> {
        let program = command.program.display().to_string();
        let mut child = Command::new(&command.program)
            .args(&command.args)
            .envs(command.envs.iter().map(|(name, value)| (name.as_str(), value.as_str())))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .map_err(|error| Error::Worker { message: format!("{program} did not start: {error}") })?;
        let stdin = child.stdin.take().expect("stdin is piped");
        let stdout = child.stdout.take().expect("stdout is piped");
        let log = tokio::spawn(keep_log(child.stderr.take().expect("stderr is piped")));
        Ok(Worker { program, child, stdin, stdout, log })
    }

    /// Whether the process has ended, as a worker that failed to start up has.
    pub(crate) fn has_exited(&mut self) -> bool {
        !matches!(self.child.try_wait(), Ok(None))
    }

    /// Kills the process and waits for it to go.
    pub(crate) async fn kill(mut self) {
        let _ = self.child.kill().await;
    }

    /// Gives the worker its request, and the batch after it, and returns its response.
    pub(crate) async fn run(self, request: &pb::Request, data: Option<&Bytes>, deadline: Duration) -> Result<pb::Response, Error> {
        let Worker { program, mut child, mut stdin, stdout, mut log } = self;
        let mut stdout = stdout.take((pb::FRAME_HEADER + pb::MAX_FRAME + 1) as u64);

        // The worker may answer before it has read everything, and it may fill a pipe while the request is
        // still going out, so the streams move together.
        let send = async move {
            stdin.write_all(&pb::message_frame(request)).await?;
            if let Some(data) = data {
                for chunk in data.chunks(pb::DATA_FRAME) {
                    stdin.write_all(&(chunk.len() as u32).to_le_bytes()).await?;
                    stdin.write_all(chunk).await?;
                }
                stdin.write_all(&0u32.to_le_bytes()).await?;
            }
            stdin.shutdown().await
        };
        let receive = async {
            let mut answer = Vec::new();
            stdout.read_to_end(&mut answer).await.map(|_| answer)
        };
        let exchange = async {
            let (sent, answer) = tokio::join!(send, receive);
            let status = child.wait().await;
            (sent, answer, status, (&mut log).await.unwrap_or_default())
        };

        let Ok((sent, answer, status, log)) = tokio::time::timeout(deadline, exchange).await else {
            // Returning drops the child, which kills it.
            log.abort();
            return Err(Error::Worker { message: format!("{program} did not finish within {deadline:?} and was killed") });
        };
        let log = String::from_utf8_lossy(&log).into_owned();
        let status = status.map_err(|error| Error::Worker { message: format!("{program} could not be waited for: {error}") })?;
        let answer = answer.map_err(|error| Error::Worker { message: format!("{program}'s answer could not be read: {error}") })?;
        decode(status, &answer).map_err(|problem| {
            let unsent = sent.err().map(|error| format!("; the request did not all go out: {error}")).unwrap_or_default();
            Error::Worker { message: format!("{program} {problem}{unsent}{}", log_tail(&log)) }
        })
    }
}

/// Reads a worker's log to its end, and returns the last of it.
async fn keep_log(mut stderr: ChildStderr) -> Vec<u8> {
    let mut kept = Vec::new();
    let mut buffer = [0u8; 4096];
    while let Ok(read) = stderr.read(&mut buffer).await {
        if read == 0 {
            break;
        }
        kept.extend_from_slice(&buffer[..read]);
        if kept.len() > 2 * LOG_KEPT {
            kept.drain(..kept.len() - LOG_KEPT);
        }
    }
    let excess = kept.len().saturating_sub(LOG_KEPT);
    kept.drain(..excess);
    kept
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
        let operation = pb::Insert { table: pb_table(insert.table), format: insert.format.to_string(), token: insert.token.to_string() };
        let response = self.call(pb::request::Operation::Insert(operation), Some(&insert.data)).await?;
        let outcome = outcome(response, |result| match result {
            pb::response::Result::Inserted(inserted) => Some(inserted),
            _ => None,
        })?;
        let Outcome::Committed(inserted) = outcome else { return Ok(retype(outcome)) };
        let parts = inserted.parts.into_iter().map(|committed| Ok(CommittedPart { table: table_name(committed.table)?, part: part(&committed.part)? }));
        Ok(Outcome::Committed(Inserted { parts: Some(parts.collect::<Result<_, Error>>()?) }))
    }

    async fn plan(&self, table: &TableName, budget: &Budget) -> Result<Vec<MergeCandidate>, Error> {
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
                Ok(MergeCandidate { partition_id: candidate.partition_id, sources })
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
        let removed = housekept.removed.iter().map(|name| part(name)).collect::<Result<_, _>>()?;
        let remaining =
            housekept.remaining.into_iter().map(|left| Ok(Remaining { part: part(&left.part)?, reason: left.reason })).collect::<Result<_, Error>>()?;
        Ok(Outcome::Committed(Housekept { removed, remaining }))
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
    use std::os::unix::process::ExitStatusExt;

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
    fn the_command_names_the_fork_worker() {
        let command = WorkerCommand::clickhouse("/opt/bighouse/clickhouse").env("TZ", "UTC");
        assert_eq!(command.program, PathBuf::from("/opt/bighouse/clickhouse"));
        assert_eq!(command.args, ["libch-worker"]);
        assert_eq!(command.envs, [("TZ".to_string(), "UTC".to_string())]);
    }

    #[test]
    fn a_log_tail_is_empty_for_a_quiet_worker() {
        assert_eq!(log_tail("  \n"), "");
        assert_eq!(log_tail("one line\n"), "; its log ends: one line");
        assert!(log_tail(&"x".repeat(5_000)).len() < 2_100);
    }
}
