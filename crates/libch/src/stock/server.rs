//! One ClickHouse server process from the BigHouse fork, with a directory and ports of its own.

use std::fs;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::OnceLock;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::sync::oneshot;

use super::ports::{self, PortLease};
use crate::Error;

/// Where stock servers come from and where they keep their directories.
#[derive(Clone, Debug)]
pub struct StockEnv {
    /// The BigHouse fork's `clickhouse` binary.
    pub clickhouse: PathBuf,
    /// The directory under which each server gets one of its own.
    pub work_dir: PathBuf,
}

/// What a server boots with.
#[derive(Clone, Debug, Default)]
pub struct ServerConfig {
    /// A name for logs and for the server's directory.
    pub name: String,
    /// The `<disks>` section's content.
    pub disks_xml: String,
    /// The `<policies>` section's content.
    pub policies_xml: String,
    /// More top-level config, such as `<merge_tree>` and `<database_disk>`.
    pub extra_xml: String,
}

/// How a statement's result came back.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Summary {
    pub read_rows: u64,
    pub written_rows: u64,
    pub written_bytes: u64,
    pub result_rows: u64,
}

/// A statement's answer.
#[derive(Clone, Debug)]
pub struct Answer {
    pub body: String,
    pub summary: Summary,
}

/// A running ClickHouse server. Dropping it kills the process and removes its directory; [`Server::stop`] lets
/// it shut down first, which releases its InodeDB lease.
#[derive(Debug)]
pub struct Server {
    name: String,
    dir: PathBuf,
    http: PortLease,
    tcp: PortLease,
    child: Option<Child>,
    client: reqwest::Client,
    keep_dir: bool,
}

const BOOT_TIMEOUT: Duration = Duration::from_secs(60);
const STOP_TIMEOUT: Duration = Duration::from_secs(60);

impl Server {
    /// Boots a server and waits for it to answer.
    pub async fn start(env: &StockEnv, config: &ServerConfig) -> Result<Server, Error> {
        let http = ports::lease()?;
        let tcp = ports::lease()?;
        let dir = env.work_dir.join(format!("{}-{:08x}", config.name, rand::random::<u32>()));
        for sub in ["data", "tmp", "user_files", "format_schemas", "log", "config.d"] {
            fs::create_dir_all(dir.join(sub))?;
        }
        fs::write(dir.join("users.xml"), USERS_XML)?;
        fs::write(dir.join("config.xml"), config_xml(&dir, http.port(), tcp.port(), config))?;

        let mut command = Command::new(&env.clickhouse);
        command
            .arg("server")
            .arg("--config-file")
            .arg(dir.join("config.xml"))
            .current_dir(&dir)
            // The process started is then the server itself, with no watchdog parent in between.
            .env("CLICKHOUSE_WATCHDOG_ENABLE", "0")
            .stdin(Stdio::null())
            .stdout(fs::File::create(dir.join("log/stdout.log"))?)
            .stderr(fs::File::create(dir.join("log/stderr.log"))?);
        let child = spawn(command)
            .await
            .map_err(|error| Error::Server { name: config.name.clone(), message: format!("{} did not start: {error}", env.clickhouse.display()) })?;

        // A connection kept open would hold the server's shutdown back until its keep-alive runs out, so each
        // statement takes a connection of its own.
        let client = reqwest::Client::builder().pool_max_idle_per_host(0).build()?;
        let mut server =
            Server { name: config.name.clone(), dir, http, tcp, child: Some(child), client, keep_dir: std::env::var_os("LIBCH_KEEP_SERVER_DIRS").is_some() };

        let started = Instant::now();
        loop {
            if let Some(status) = server.child.as_mut().expect("the child was just set").try_wait()? {
                let message = format!("exited with {status} while booting: {}", server.log_tail());
                server.child = None;
                return Err(server.error(message));
            }
            if server.query("SELECT 1").await.is_ok_and(|answer| answer.trim() == "1") {
                return Ok(server);
            }
            if started.elapsed() > BOOT_TIMEOUT {
                let message = format!("did not answer within {BOOT_TIMEOUT:?}: {}", server.log_tail());
                return Err(server.error(message));
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn http_port(&self) -> u16 {
        self.http.port()
    }

    pub fn tcp_port(&self) -> u16 {
        self.tcp.port()
    }

    /// The server's process id, while it runs.
    pub fn pid(&self) -> Option<u32> {
        self.child.as_ref().map(Child::id)
    }

    /// Runs a statement and returns its output.
    pub async fn query(&self, sql: &str) -> Result<String, Error> {
        Ok(self.run(sql, &[], Bytes::new()).await?.body)
    }

    /// Runs a statement under settings and returns its output.
    pub async fn query_with(&self, sql: &str, settings: &[(&str, &str)]) -> Result<String, Error> {
        Ok(self.run(sql, settings, Bytes::new()).await?.body)
    }

    /// Runs a statement under settings, with `body` as its data.
    pub async fn run(&self, sql: &str, settings: &[(&str, &str)], body: Bytes) -> Result<Answer, Error> {
        let mut url = url::Url::parse(&format!("http://127.0.0.1:{}/", self.http.port())).expect("a valid URL");
        url.query_pairs_mut().append_pair("query", sql).extend_pairs(settings);
        // ClickHouse refuses a POST without a length, and an empty body goes out without one unless it is set.
        let response = self.client.post(url).header(reqwest::header::CONTENT_LENGTH, body.len()).body(body).send().await?;
        let status = response.status();
        let code = response.headers().get("X-ClickHouse-Exception-Code").and_then(|value| value.to_str().ok()).and_then(|value| value.parse().ok());
        let summary = response.headers().get("X-ClickHouse-Summary").and_then(|value| value.to_str().ok()).map(parse_summary).unwrap_or_default();
        let body = response.text().await?;
        if status.is_success() && code.is_none() {
            Ok(Answer { body, summary })
        } else {
            Err(Error::ClickHouse { code: code.unwrap_or(0), message: body.trim().to_string() })
        }
    }

    /// Writes a file into the server's `config.d` and has the server reload its config.
    pub async fn add_config(&self, file_name: &str, xml: &str) -> Result<(), Error> {
        // The server also rereads `config.d` on its own, so the file appears there whole.
        let staged = self.dir.join(format!("{file_name}.staged"));
        fs::write(&staged, xml)?;
        fs::rename(&staged, self.dir.join("config.d").join(file_name))?;
        self.query("SYSTEM RELOAD CONFIG").await?;
        Ok(())
    }

    /// Stops the server with SIGTERM and waits for it, so its disks shut down and release their InodeDB leases.
    pub async fn stop(&mut self) -> Result<(), Error> {
        let Some(mut child) = self.child.take() else { return Ok(()) };
        signal(&child, libc::SIGTERM);
        let started = Instant::now();
        loop {
            if child.try_wait()?.is_some() {
                return Ok(());
            }
            if started.elapsed() > STOP_TIMEOUT {
                signal(&child, libc::SIGKILL);
                child.wait()?;
                return Err(self.error(format!("ignored SIGTERM for {STOP_TIMEOUT:?} and was killed")));
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }

    /// Kills the server with SIGKILL: it gets no chance to finish anything.
    pub fn kill(&mut self) {
        if let Some(mut child) = self.child.take() {
            signal(&child, libc::SIGKILL);
            let _ = child.wait();
        }
    }

    /// What the server's logs say went wrong: the exceptions it logged last, without their stack traces, or
    /// the end of its output when it logged none.
    pub fn log_tail(&self) -> String {
        let mut logs = String::new();
        for file in ["log/server.err.log", "log/stderr.log"] {
            if let Ok(text) = fs::read_to_string(self.dir.join(file)) {
                logs.push_str(&text);
                logs.push('\n');
            }
        }
        let exceptions: Vec<&str> = logs.lines().filter(|line| line.contains("Exception") && !is_stack_frame(line)).collect();
        if exceptions.is_empty() {
            let start = logs.len().saturating_sub(1_000);
            let start = (start..=logs.len()).find(|&at| logs.is_char_boundary(at)).unwrap_or(logs.len());
            return logs[start..].trim().to_string();
        }
        let last = &exceptions[exceptions.len().saturating_sub(3)..];
        last.iter().map(|line| line.chars().take(600).collect::<String>()).collect::<Vec<_>>().join("\n")
    }

    fn error(&self, message: String) -> Error {
        Error::Server { name: self.name.clone(), message }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.kill();
        if !self.keep_dir {
            let _ = fs::remove_dir_all(&self.dir);
        }
    }
}

/// Whether a log line is one frame of a stack trace, as `3. src/Common/Exception.h:57:54: DB::Exception::...`.
fn is_stack_frame(line: &str) -> bool {
    let digits = line.bytes().take_while(u8::is_ascii_digit).count();
    digits > 0 && line[digits..].starts_with(". ")
}

fn signal(child: &Child, signal: libc::c_int) {
    // SAFETY: `kill` takes a pid and a signal number and touches no memory. The child has not been waited for,
    // so its pid still names it.
    unsafe {
        libc::kill(child.id() as libc::pid_t, signal);
    }
}

type SpawnRequest = (Command, oneshot::Sender<std::io::Result<Child>>);

/// Starts a process from one thread that lives as long as this process does.
///
/// Each child asks the kernel to kill it when its parent thread exits, so a crash of this process leaves no
/// server behind. The request is tied to the thread that forked, and async runtimes retire their threads, so
/// every spawn goes through this one.
async fn spawn(mut command: Command) -> std::io::Result<Child> {
    static SPAWNER: OnceLock<mpsc::Sender<SpawnRequest>> = OnceLock::new();
    let spawner = SPAWNER.get_or_init(|| {
        let (requests, queue) = mpsc::channel::<SpawnRequest>();
        std::thread::Builder::new()
            .name("libch-spawner".into())
            .spawn(move || {
                for (mut command, reply) in queue {
                    let _ = reply.send(command.spawn());
                }
            })
            .expect("the spawner thread starts");
        requests
    });
    // SAFETY: the closure runs in the forked child before exec and calls only `prctl`, which is
    // async-signal-safe and allocates nothing.
    unsafe {
        command.pre_exec(|| {
            if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let (reply, answer) = oneshot::channel();
    spawner.send((command, reply)).map_err(|_| std::io::Error::other("the spawner thread is gone"))?;
    answer.await.map_err(|_| std::io::Error::other("the spawner thread dropped a request"))?
}

fn parse_summary(header: &str) -> Summary {
    let Ok(serde_json::Value::Object(fields)) = serde_json::from_str::<serde_json::Value>(header) else {
        return Summary::default();
    };
    let field = |name: &str| fields.get(name).and_then(|value| value.as_str()).and_then(|value| value.parse().ok()).unwrap_or(0);
    Summary { read_rows: field("read_rows"), written_rows: field("written_rows"), written_bytes: field("written_bytes"), result_rows: field("result_rows") }
}

const USERS_XML: &str = "<clickhouse>
    <profiles><default/></profiles>
    <users><default><password/><networks><ip>::/0</ip></networks><profile>default</profile><quota>default</quota><access_management>1</access_management></default></users>
    <quotas><default/></quotas>
</clickhouse>
";

fn config_xml(dir: &Path, http_port: u16, tcp_port: u16, config: &ServerConfig) -> String {
    let dir = xml_escape(&dir.display().to_string());
    format!(
        "<clickhouse>
    <logger>
        <level>information</level>
        <log>{dir}/log/server.log</log>
        <errorlog>{dir}/log/server.err.log</errorlog>
        <size>200M</size>
        <count>2</count>
    </logger>
    <listen_host>127.0.0.1</listen_host>
    <http_port>{http_port}</http_port>
    <tcp_port>{tcp_port}</tcp_port>
    <path>{dir}/data/</path>
    <tmp_path>{dir}/tmp/</tmp_path>
    <user_files_path>{dir}/user_files/</user_files_path>
    <format_schema_path>{dir}/format_schemas/</format_schema_path>
    <users_config>users.xml</users_config>
    <mark_cache_size>1073741824</mark_cache_size>
    <storage_configuration>
        <disks>{disks}</disks>
        <policies>{policies}</policies>
    </storage_configuration>
    {extra}
</clickhouse>
",
        disks = config.disks_xml,
        policies = config.policies_xml,
        extra = config.extra_xml,
    )
}

/// Escapes text for an XML element's content.
pub fn xml_escape(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for c in text.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            other => out.push(other),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn summary_reads_the_header_clickhouse_sends() {
        let header = r#"{"read_rows":"1000","read_bytes":"8000","written_rows":"1000","written_bytes":"12345","total_rows_to_read":"0","result_rows":"1000","result_bytes":"1","elapsed_ns":"5"}"#;
        assert_eq!(parse_summary(header), Summary { read_rows: 1000, written_rows: 1000, written_bytes: 12345, result_rows: 1000 });
        assert_eq!(parse_summary("not json"), Summary::default());
    }

    #[test]
    fn stack_frames_are_told_from_messages() {
        assert!(is_stack_frame("3. src/Common/Exception.h:57:54: DB::Exception::Exception(PreformattedMessage&&, int) @ 0x0a"));
        assert!(is_stack_frame("12. __GI___clone3 @ 0x00000000001375cc"));
        assert!(!is_stack_frame("2026.11.10 17:28:50.311522 [ 1 ] {} <Error> Application: Code: 115. DB::Exception: Unknown setting"));
        assert!(!is_stack_frame(""));
    }

    #[test]
    fn xml_escape_covers_the_five_characters() {
        assert_eq!(xml_escape(r#"a<b>&"c'"#), "a&lt;b&gt;&amp;&quot;c&apos;");
    }
}
