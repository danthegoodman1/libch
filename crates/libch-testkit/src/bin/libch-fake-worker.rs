//! A stand-in for the worker that speaks the protocol and touches no disk, for testing the crate's side of it.
//!
//! `LIBCH_FAKE_WORKER` picks what it does. Unset, it answers each operation with a result that echoes the
//! request, so a test can see that every field arrived. Three more variables shape any mode:
//!
//! - `LIBCH_FAKE_WORKER_DIR`: a directory where the worker leaves `started-{pid}` when it starts, and
//!   `running-{pid}` for as long as it holds a request in the `slow` mode.
//! - `LIBCH_FAKE_WORKER_MARKER`: a file whose presence makes the worker exit as it starts.
//! - `LIBCH_FAKE_WORKER_CHATTER`: how many bytes of log the worker writes before it reads its request.

use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use libch_proto as pb;
use libch_proto::Message;

/// What the worker tells a test about itself, in the last table of a description.
struct Report {
    /// How long the worker waited for its request.
    waited: Duration,
    /// The most workers that held a request at once while this one did, itself among them.
    together: usize,
}

fn main() {
    let started = Instant::now();
    let mode = std::env::var("LIBCH_FAKE_WORKER").unwrap_or_default();
    let dir = std::env::var_os("LIBCH_FAKE_WORKER_DIR").map(PathBuf::from);
    if let Some(dir) = &dir {
        std::fs::write(dir.join(format!("started-{}", std::process::id())), b"").unwrap();
    }
    if std::env::var_os("LIBCH_FAKE_WORKER_MARKER").is_some_and(|marker| Path::new(&marker).exists()) {
        eprintln!("fatal: the engine could not start");
        std::process::exit(3);
    }
    if let Ok(chatter) = std::env::var("LIBCH_FAKE_WORKER_CHATTER") {
        let line = format!("{}\n", "starting up ".repeat(8));
        let mut stderr = io::stderr().lock();
        for _ in 0..chatter.parse::<usize>().expect("a byte count") / line.len() {
            stderr.write_all(line.as_bytes()).unwrap();
        }
        stderr.write_all(b"ready for a request\n").unwrap();
    }

    let mut stdin = io::stdin().lock();
    let frame = pb::read_frame(&mut stdin).expect("a request frame").expect("a request");
    let request = pb::Request::decode(frame.as_slice()).expect("a request that decodes");
    let operation = request.operation.clone().expect("an operation");
    let mut report = Report { waited: started.elapsed(), together: 1 };
    if mode == "slow" {
        let dir = dir.as_deref().expect("the slow mode counts workers in LIBCH_FAKE_WORKER_DIR");
        let mine = dir.join(format!("running-{}", std::process::id()));
        std::fs::write(&mine, b"").unwrap();
        for _ in 0..6 {
            report.together = report.together.max(running(dir));
            std::thread::sleep(Duration::from_millis(50));
        }
        std::fs::remove_file(&mine).unwrap();
    }

    let status = |status: pb::Status| pb::Response { status: status as i32, ..pb::Response::default() };
    let response = match mode.as_str() {
        "duplicate" => status(pb::Status::Duplicate),
        "schema-changed" => status(pb::Status::SchemaChanged),
        "fenced" => status(pb::Status::Fenced),
        "claim-lost" => status(pb::Status::ClaimLost),
        "refused" | "deaf" => pb::Response { message: "the table keeps no tokens".into(), ..status(pb::Status::Refused) },
        "exception" => pb::Response { message: "Table d.absent does not exist".into(), exception_code: 60, ..status(pb::Status::Failed) },
        "failed" => pb::Response { message: "the memory limit was reached".into(), ..status(pb::Status::Failed) },
        "no-result" => status(pb::Status::Ok),
        "unknown-status" => pb::Response { status: 99, ..pb::Response::default() },
        "bad-part" => {
            let parts = vec![pb::CommittedPart { table: table_of(&operation), part: "not_a_part".into(), ..pb::CommittedPart::default() }];
            pb::Response { result: Some(pb::response::Result::Inserted(pb::Inserted { parts, counter_conflicts: 0 })), ..status(pb::Status::Ok) }
        }
        "exit" => {
            eprintln!("fatal: the disk could not be attached");
            std::process::exit(3);
        }
        "garbage" => {
            io::stdout().write_all(b"Code: 60. DB::Exception: this is no frame").unwrap();
            return;
        }
        "hang" => {
            if let Ok(path) = std::env::var("LIBCH_FAKE_WORKER_PID") {
                std::fs::write(path, std::process::id().to_string()).unwrap();
            }
            std::thread::sleep(Duration::from_secs(600));
            return;
        }
        _ => pb::Response { result: Some(echo(&request, operation, &report, &mut stdin)), ..status(pb::Status::Ok) },
    };

    // What a worker would have counted, in figures a test can tell apart from zero.
    let usage = pb::Usage { inodedb_commits: 2, inodedb_reads: 3, cpu_microseconds: 1_000, anonymous_memory_bytes: 4_096, threads: 1, ..pb::Usage::default() };
    let response = pb::Response { usage: (response.status != 0 && mode != "silent-usage").then_some(usage), ..response };

    let frame = pb::message_frame(&response);
    let mut stdout = io::stdout().lock();
    match mode.as_str() {
        "truncated" => stdout.write_all(&frame[..frame.len() - 1]).unwrap(),
        "two-frames" => {
            stdout.write_all(&frame).unwrap();
            stdout.write_all(&frame).unwrap();
        }
        _ => stdout.write_all(&frame).unwrap(),
    }
    stdout.flush().unwrap();
    if mode == "exit-after-answer" {
        std::process::exit(1);
    }
}

fn table_of(operation: &pb::request::Operation) -> Option<pb::TableName> {
    match operation {
        pb::request::Operation::Insert(insert) => insert.table.clone(),
        pb::request::Operation::Plan(plan) => plan.table.clone(),
        pb::request::Operation::Merge(merge) => merge.table.clone(),
        pb::request::Operation::Housekeep(housekeep) => housekeep.table.clone(),
        pb::request::Operation::Describe(_) | pb::request::Operation::Share(_) => None,
    }
}

/// How many workers hold a request now.
fn running(dir: &Path) -> usize {
    let entries = std::fs::read_dir(dir).expect("the directory lists");
    entries.filter(|entry| entry.as_ref().is_ok_and(|entry| entry.file_name().to_string_lossy().starts_with("running-"))).count()
}

/// A name with only the characters a partition id may hold.
fn partition(text: &str) -> String {
    text.chars().filter(char::is_ascii_alphanumeric).collect()
}

/// A result that carries the request's fields back.
fn echo(request: &pb::Request, operation: pb::request::Operation, report: &Report, stdin: &mut impl Read) -> pb::response::Result {
    let name = |database: &str, table: &str| Some(pb::TableName { database: database.into(), table: table.into() });
    match operation {
        pb::request::Operation::Describe(describe) => {
            let tenant = request.tenant.clone().unwrap_or_default();
            let s3 = tenant.s3.unwrap_or_default();
            let parts = if describe.parts {
                vec![
                    pb::Part { name: "all_1_1_0".into(), partition_id: "all".into(), min_block: 1, max_block: 1, level: 0, mutation: 0, active: false },
                    pb::Part { name: "all_1_2_1_7".into(), partition_id: "all".into(), min_block: 1, max_block: 2, level: 1, mutation: 7, active: true },
                ]
            } else {
                Vec::new()
            };
            let tables = vec![
                pb::Table {
                    name: name(&tenant.inodedb_endpoint, &tenant.inodedb_key),
                    uuid: [s3.endpoint, s3.bucket, s3.prefix, s3.region, s3.access_key, s3.secret_key].join("|"),
                    kind: pb::table::Kind::Table as i32,
                    engine: request.settings_xml.clone(),
                    views: vec![pb::View { view: name("d", "daily_mv"), target: name("d", "daily") }],
                    parts,
                },
                pb::Table { name: name("d", "daily_mv"), kind: pb::table::Kind::MaterializedView as i32, ..pb::Table::default() },
                // The worker itself: its process, how long it waited, and how many ran beside it.
                pb::Table {
                    name: name("worker", &std::process::id().to_string()),
                    uuid: report.waited.as_millis().to_string(),
                    engine: report.together.to_string(),
                    ..pb::Table::default()
                },
            ];
            pb::response::Result::Described(pb::Described { tables })
        }
        pb::request::Operation::Insert(insert) => {
            let mut bytes = 0usize;
            let mut frames = 0usize;
            loop {
                let frame = pb::read_frame(stdin).expect("a data frame").expect("the batch ends with an empty frame");
                if frame.is_empty() {
                    break;
                }
                assert!(frame.iter().all(|&byte| byte == b'x'), "the batch arrives as it was sent");
                bytes += frame.len();
                frames += 1;
            }
            assert_eq!(pb::read_frame(stdin).expect("the end of the stream"), None, "nothing follows the batch");
            let parts = vec![
                pb::CommittedPart {
                    table: insert.table.clone(),
                    part: format!("{}_{bytes}_{bytes}_0", partition(&insert.format)),
                    rows: 7,
                    bytes: bytes as u64,
                },
                pb::CommittedPart { table: name("frames", "count"), part: format!("{}_{frames}_{frames}_0", partition(&insert.token)), rows: 0, bytes: 0 },
            ];
            // The waiver travels back as a count a test can read.
            pb::response::Result::Inserted(pb::Inserted { parts, counter_conflicts: u32::from(insert.at_least_once) * 3 })
        }
        pb::request::Operation::Plan(plan) => {
            let table = plan.table.unwrap_or_default();
            let candidates = (0..plan.max_merges.min(3))
                .map(|index| pb::MergeCandidate {
                    partition_id: partition(&table.table),
                    sources: vec![
                        format!("{}_{}_{}_0", partition(&table.table), index * 2 + 1, index * 2 + 1),
                        format!("{}_{}_{}_0", partition(&table.table), index * 2 + 2, index * 2 + 2),
                    ],
                    rows: 100 * (u64::from(index) + 1),
                    bytes: 4096 * (u64::from(index) + 1),
                })
                .collect();
            pb::response::Result::Planned(pb::Planned { candidates })
        }
        pb::request::Operation::Merge(merge) => {
            let block = |name: &String, field: usize| name.split('_').nth(field).unwrap_or("0").to_string();
            let part = format!("{}_{}_{}_1", merge.partition_id, block(&merge.sources[0], 1), block(merge.sources.last().unwrap(), 2));
            pb::response::Result::Merged(pb::Merged { part })
        }
        pb::request::Operation::Housekeep(housekeep) => {
            let table = housekeep.table.unwrap_or_default();
            pb::response::Result::Housekept(pb::Housekept {
                removed: vec!["all_1_1_0".into(), "all_2_2_0".into()],
                remaining: vec![pb::Remaining { part: "all_3_3_0".into(), reason: format!("covered 10 s ago in {}.{}", table.database, table.table) }],
                temporary_directories_removed: 2,
                blobs_removed: 5,
                blobs_remain: true,
            })
        }
        pb::request::Operation::Share(_) => pb::response::Result::Shared(pb::Shared { owner_version: 42 }),
    }
}
