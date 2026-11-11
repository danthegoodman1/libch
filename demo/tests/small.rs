//! A small run of the recipe: every step, every tenant checked and queried.

use libch_demo::{Config, run};
use libch_testkit::stack;

#[tokio::test(flavor = "multi_thread")]
async fn a_small_run_ends_exact() {
    // Every insert carries one batch of 200 rows, so the busiest tenants take dozens and compact as they go.
    let config = Config {
        tenants: 20,
        batches: 400,
        rows_per_batch: 200,
        max_batch_rows: 200,
        zipf_exponent: 0.6,
        queried_tenants: 20,
        checked_tenants: 20,
        ..Config::default()
    };
    let report = run(stack().await, &config).await.expect("the run finishes");
    println!("{report}");
    assert!(report.is_exact(), "{report}");
    assert_eq!(report.rows, 400 * 200);
    assert!(report.batches >= 400, "{} batches", report.batches);
    assert_eq!((report.tenants_checked, report.tenants_queried), (20, 20));
    assert!(report.merges > 0 && report.parts_removed > 0, "the busiest tenants merged, and housekeeping removed the sources");
    assert_eq!(report.failed_attempts, 0);
    assert_eq!(report.orphan_blobs, 0, "no worker died, so S3 holds nothing that no disk names");
    assert_eq!(report.stock_inserts.len(), 3);
    assert!(report.stock_inserts.iter().all(|insert| insert.cpu > std::time::Duration::from_millis(50)), "{:?}", report.stock_inserts);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_run_that_kills_workers_ends_exact() {
    let config = Config {
        tenants: 8,
        batches: 200,
        rows_per_batch: 200,
        max_batch_rows: 200,
        zipf_exponent: 0.3,
        queried_tenants: 8,
        checked_tenants: 8,
        kill_one_in: Some(5),
        ..Config::default()
    };
    let report = run(stack().await, &config).await.expect("the run finishes");
    println!("{report}");
    assert!(report.is_exact(), "{report}");
    assert!(report.kills > 10 && report.failed_attempts > 0, "{} kills, {} failed attempts", report.kills, report.failed_attempts);
}
