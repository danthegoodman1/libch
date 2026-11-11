//! The preflight passes on a ready environment, and names what is missing on any other.

use std::path::PathBuf;

use libch_testkit::preflight::{self, PreflightError};
use libch_testkit::{Stack, stack};

#[tokio::test]
async fn the_environment_is_ready() {
    stack().await;
}

#[tokio::test]
async fn shared_writers_leave_the_layout_bighouse_documents() {
    preflight::shared_mode(stack().await).await.unwrap();
}

#[test]
fn the_client_rev_comes_from_the_workspace_manifest() {
    let rev = preflight::inodedb_client_rev();
    assert_eq!(rev.len(), 40);
    assert!(rev.bytes().all(|byte| byte.is_ascii_hexdigit()));
    assert_eq!(preflight::BIGHOUSE_REV.trim().len(), 40);
}

#[tokio::test]
async fn a_missing_checkout_is_named() {
    let stack = Stack { bighouse_dir: PathBuf::from("/nonexistent/BigHouse"), ..Stack::from_env() };
    let error = preflight::environment(&stack, preflight::BIGHOUSE_REV.trim()).await.unwrap_err();
    assert!(matches!(error, PreflightError::NoCheckout(_)), "{error}");
    let message = error.to_string();
    assert!(message.contains("/nonexistent/BigHouse") && message.contains("BIGHOUSE_DIR"), "{message}");
}

#[tokio::test]
async fn a_checkout_without_the_pinned_commit_is_named() {
    let stack = Stack::from_env();
    let absent = "0123456789abcdef0123456789abcdef01234567";
    let error = preflight::environment(&stack, absent).await.unwrap_err();
    assert!(matches!(error, PreflightError::WrongCommit { .. }), "{error}");
    let message = error.to_string();
    assert!(message.contains(absent) && message.contains("update the checkout"), "{message}");
}

#[tokio::test]
async fn a_missing_binary_is_named() {
    let stack = Stack { clickhouse: PathBuf::from("/nonexistent/clickhouse"), ..Stack::from_env() };
    let error = preflight::environment(&stack, preflight::BIGHOUSE_REV.trim()).await.unwrap_err();
    assert!(matches!(error, PreflightError::NoBinary(_)), "{error}");
    assert!(error.to_string().contains("scripts/fork-build"), "{error}");
}

/// A directory of this test's own under the system's temporary directory.
fn scratch(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("libch-preflight-{name}-{:08x}", rand::random::<u32>()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[tokio::test]
async fn a_binary_without_the_worker_is_named() {
    // A program that answers `local` and knows no `libch-worker`.
    let dir = scratch("binary");
    let program = dir.join("clickhouse");
    std::fs::write(&program, "#!/bin/sh\n[ \"$1\" = local ] && exit 0\necho \"no such program: $1\" >&2\nexit 2\n").unwrap();
    std::fs::set_permissions(&program, std::os::unix::fs::PermissionsExt::from_mode(0o755)).unwrap();

    let stack = Stack { clickhouse: program, ..Stack::from_env() };
    let error = preflight::environment(&stack, preflight::BIGHOUSE_REV.trim()).await.unwrap_err();
    assert!(matches!(error, PreflightError::NoWorker { .. }), "{error}");
    let message = error.to_string();
    assert!(message.contains("scripts/fork-build") && message.contains("no such program: libch-worker"), "{message}");
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn a_missing_fork_is_named() {
    let stack = Stack { fork_dir: Some(PathBuf::from("/nonexistent/fork")), ..Stack::from_env() };
    let error = preflight::environment(&stack, preflight::BIGHOUSE_REV.trim()).await.unwrap_err();
    assert!(matches!(error, PreflightError::NoFork(_)), "{error}");
    assert!(error.to_string().contains("scripts/fork-setup"), "{error}");
}

#[test]
fn a_fork_without_bighouses_commit_is_named() {
    // A repository with no commit at all includes none of BigHouse's.
    let dir = scratch("behind");
    assert!(std::process::Command::new("git").arg("init").arg("--quiet").arg(&dir).status().unwrap().success());
    let error = preflight::fork(&dir, &Stack::from_env().bighouse_dir, "").unwrap_err();
    assert!(matches!(error, PreflightError::ForkBehind { .. }), "{error}");
    assert!(error.to_string().contains("merge it into the worker's branch"), "{error}");
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn a_fork_without_the_pinned_worker_commit_is_named() {
    let stack = Stack::from_env();
    let fork_dir = stack.fork_dir.expect("the tests run the fork's own build");
    let absent = "0123456789abcdef0123456789abcdef01234567";
    let error = preflight::fork(&fork_dir, &stack.bighouse_dir, absent).unwrap_err();
    assert!(matches!(error, PreflightError::ForkWithoutPin { .. }), "{error}");
    assert!(error.to_string().contains(absent) && error.to_string().contains("scripts/fork-build"), "{error}");
    assert_eq!(preflight::FORK_REV.trim().len(), 40);
}

#[test]
fn a_fork_with_another_protocol_is_named() {
    let dir = scratch("protocol");
    std::fs::create_dir_all(dir.join(".git")).unwrap();
    std::fs::create_dir_all(dir.join("programs/libch-worker")).unwrap();
    let copy = dir.join("programs/libch-worker/libch.proto");
    // With no BigHouse checkout to compare commits with, the protocol is what is left to check.
    let no_checkout = dir.join("no-bighouse");

    let error = preflight::fork(&dir, &no_checkout, "").unwrap_err();
    assert!(matches!(error, PreflightError::ProtocolMismatch(_)), "a fork without the protocol: {error}");
    std::fs::write(&copy, format!("{}// One more line.\n", preflight::PROTOCOL)).unwrap();
    let error = preflight::fork(&dir, &no_checkout, "").unwrap_err();
    assert!(error.to_string().contains("differs from proto/libch.proto"), "{error}");
    std::fs::write(&copy, preflight::PROTOCOL).unwrap();
    preflight::fork(&dir, &no_checkout, "").unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn a_binary_that_does_not_run_is_named() {
    let stack = Stack { clickhouse: PathBuf::from("/bin/false"), ..Stack::from_env() };
    let error = preflight::environment(&stack, preflight::BIGHOUSE_REV.trim()).await.unwrap_err();
    assert!(matches!(error, PreflightError::BrokenBinary { .. }), "{error}");
}

#[tokio::test]
async fn a_stopped_inodedb_is_named() {
    // Nothing listens on port 1.
    let stack = Stack { inodedb_endpoint: "127.0.0.1:1".into(), ..Stack::from_env() };
    let error = preflight::environment(&stack, preflight::BIGHOUSE_REV.trim()).await.unwrap_err();
    assert!(matches!(error, PreflightError::NoInodeDb { .. }), "{error}");
    assert!(error.to_string().contains("scripts/dev-up"), "{error}");
}

#[tokio::test]
async fn a_stopped_s3_is_named() {
    let stack = Stack { s3_endpoint: "http://127.0.0.1:1".into(), ..Stack::from_env() };
    let error = preflight::environment(&stack, preflight::BIGHOUSE_REV.trim()).await.unwrap_err();
    assert!(matches!(error, PreflightError::NoS3 { .. }), "{error}");
    assert!(error.to_string().contains("scripts/dev-up"), "{error}");
}

#[tokio::test]
async fn a_missing_admin_key_is_named() {
    let stack = Stack { admin_key_file: PathBuf::from("/nonexistent/admin.key"), ..Stack::from_env() };
    let error = preflight::environment(&stack, preflight::BIGHOUSE_REV.trim()).await.unwrap_err();
    assert!(matches!(error, PreflightError::NoAdminKey(_)), "{error}");
}
