//! The sandbox a worker puts on itself: what it lets an operation do, and what it refuses a worker that tries
//! to leave it.

use std::net::{Ipv4Addr, SocketAddrV4, TcpListener};
use std::os::unix::fs::symlink;
use std::time::Duration;

use libch::Writer;
use libch::worker::{OpWriter, Probe, Sandbox};
use libch::writer::Budget;
use libch_testkit::ops::{assert_clean, insert, tenant_with_parts, totals};
use libch_testkit::schema::{batch, events};
use libch_testkit::stack;

const EPERM: i32 = 1;
const EACCES: i32 = 13;
const EAFNOSUPPORT: i32 = 97;

fn local(port: u16) -> SocketAddrV4 {
    SocketAddrV4::new(Ipv4Addr::LOCALHOST, port)
}

/// The port of an address as `127.0.0.1:7170` or `http://127.0.0.1:11211`.
fn port_of(endpoint: &str) -> u16 {
    endpoint.rsplit(':').next().and_then(|port| port.trim_end_matches('/').parse().ok()).expect("the endpoint names a port")
}

/// A sandboxed worker that tries to leave: each attempt fails, and the same attempts go through for a worker
/// without the sandbox, so it is the sandbox that stops them.
#[tokio::test]
async fn a_sandboxed_worker_reaches_its_store_and_nothing_else() {
    let stack = stack().await;
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let off_the_list = listener.local_addr().unwrap().port();
    let created = std::env::temp_dir().join(format!("libch-escape-{:08x}", rand::random::<u32>()));
    let (store, s3) = (port_of(&stack.inodedb_endpoint), port_of(&stack.s3_endpoint));

    let attempts = [
        Probe::Read("/etc/passwd".into()),
        Probe::Read(stack.admin_key_file.clone()),
        Probe::Write(created.clone()),
        Probe::Execute("/bin/true".into()),
        Probe::Connect(local(off_the_list)),
        Probe::UnixSocket,
        Probe::Signal(std::process::id()),
    ];
    let allowed = [Probe::Connect(local(store)), Probe::Connect(local(s3)), Probe::Read("/proc/self/status".into())];

    let sandboxed = stack.sandboxed_worker_command();
    let refused = sandboxed.probe(&attempts).await.unwrap();
    let errors: Vec<Option<i32>> = refused.iter().map(|probed| probed.error).collect();
    // Running a program is refused twice: the filter refuses the new process, and Landlock the program's file.
    assert_eq!(errors, [Some(EACCES), Some(EACCES), Some(EACCES), Some(EACCES), Some(EACCES), Some(EAFNOSUPPORT), Some(EPERM)], "{refused:?}");
    assert!(!created.exists(), "the refused write made no file");
    // Attaching to another process as a debugger does is refused twice over, by the filter and by Landlock.
    assert_eq!(sandboxed.probe(&[Probe::Trace(std::process::id())]).await.unwrap()[0].error, Some(EPERM));
    for probed in sandboxed.probe(&allowed).await.unwrap() {
        assert_eq!(probed.error, None, "{probed:?}");
    }

    let open = stack.worker_command();
    if open.sandbox.is_none() {
        for probed in open.probe(&attempts).await.unwrap() {
            assert_eq!(probed.error, None, "without the sandbox: {probed:?}");
        }
        std::fs::remove_file(&created).unwrap();
    }
}

/// A directory the sandbox lets a worker read holds a link to a file outside it. The link is no way out: the
/// worker reads the directory's own file and is refused the link's target.
#[tokio::test]
async fn a_link_out_of_an_allowed_directory_is_refused() {
    let stack = stack().await;
    let allowed = stack.work_dir.join(format!("sandbox-allowed-{:08x}", rand::random::<u32>()));
    std::fs::create_dir_all(&allowed).unwrap();
    std::fs::write(allowed.join("inside"), "readable").unwrap();
    symlink("/etc/passwd", allowed.join("outside")).unwrap();

    let sandbox = Sandbox { read_paths: vec![allowed.clone()], ..stack.sandbox() };
    let probed = stack.worker_command().sandboxed(sandbox).probe(&[Probe::Read(allowed.join("inside")), Probe::Read(allowed.join("outside"))]).await.unwrap();
    assert_eq!((probed[0].error, probed[1].error), (None, Some(EACCES)), "{probed:?}");
    std::fs::remove_dir_all(&allowed).unwrap();
}

/// Every operation runs in the sandbox as it does outside it.
#[tokio::test]
async fn operations_run_in_the_sandbox() {
    let stack = stack().await;
    let (fixture, _open) = tenant_with_parts(0).await;
    let writer = OpWriter::new(stack.sandboxed_worker_command(), &fixture.tenant, &fixture.key);
    let mut sum = 0;
    for index in 0..6 {
        // One batch is large enough to keep its data in S3.
        let batch = batch(index, if index == 5 { 60_000 } else { 100 });
        sum += batch.sum;
        assert!(insert(&writer, &batch).await.unwrap().is_committed());
    }
    assert_eq!(writer.describe(true).await.unwrap().tables.len(), 3);
    let planned = writer.plan(&events(), &Budget::default()).await.unwrap();
    assert!(!planned.is_empty(), "six small parts are worth a merge");
    for merge in &planned {
        assert!(writer.merge(&events(), &merge.candidate).await.unwrap().is_committed());
    }
    assert!(writer.housekeep(&events()).await.unwrap().is_committed());
    assert_eq!(totals(&fixture).await, (60_500, sum, sum));
    assert_clean(&fixture).await;
}

/// A sandbox that leaves the store's port out keeps the worker from its tenant: the operation fails and commits
/// nothing.
#[tokio::test]
async fn a_worker_whose_sandbox_leaves_the_store_out_commits_nothing() {
    let stack = stack().await;
    let (fixture, open) = tenant_with_parts(1).await;
    let sandbox = Sandbox { connect_ports: vec![port_of(&stack.s3_endpoint)], read_paths: Vec::new() };
    let confined = OpWriter::new(stack.worker_command().sandboxed(sandbox), &fixture.tenant, &fixture.key).with_deadline(Duration::from_secs(10));
    assert!(insert(&confined, &batch(1, 100)).await.is_err());
    assert_eq!(totals(&fixture).await.0, 100);
    assert!(insert(&open, &batch(1, 100)).await.unwrap().is_committed());
    assert_clean(&fixture).await;
}
