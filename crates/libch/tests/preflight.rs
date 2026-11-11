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
    assert!(error.to_string().contains("scripts/clickhouse-build"), "{error}");
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
