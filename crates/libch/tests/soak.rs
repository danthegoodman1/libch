//! The differential soak: workloads drawn from seeds go through the fork's worker and through the reference
//! writer, and both tenants must answer alike before and after their merges.
//!
//! `cargo test` runs three seeds. `scripts/soak` runs seeds for as long as it is told, several at a time, and
//! keeps the log; a workload that differs fails the run, and its line in the log names the seed that draws it.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use futures_util::future::join_all;
use libch_testkit::differential::run;
use libch_testkit::soak::workload;
use libch_testkit::stack;

fn from_env(name: &str, default: u64) -> u64 {
    std::env::var(name).ok().and_then(|value| value.parse().ok()).unwrap_or(default)
}

#[tokio::test]
async fn three_seeds_answer_alike() {
    let stack = stack().await;
    for seed in [1, 2, 3] {
        let workload = workload(seed);
        println!("{}", workload.summary);
        run(stack, &workload.case, workload.format).await;
    }
}

/// `LIBCH_SOAK_MINUTES` sets how long new workloads start, 60 by default; `LIBCH_SOAK_SEED` the first seed, 1 by
/// default; and `LIBCH_SOAK_STREAMS` how many workloads run at a time, 4 by default.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "runs for an hour; scripts/soak starts it"]
async fn soak() {
    let stack = stack().await;
    let lasting = Duration::from_secs(60 * from_env("LIBCH_SOAK_MINUTES", 60));
    let first = from_env("LIBCH_SOAK_SEED", 1);
    let next = AtomicU64::new(first);
    let started = Instant::now();

    let streams = (0..from_env("LIBCH_SOAK_STREAMS", 4)).map(|_| async {
        while started.elapsed() < lasting {
            let workload = workload(next.fetch_add(1, Ordering::SeqCst));
            println!("{}", workload.summary);
            run(stack, &workload.case, workload.format).await;
        }
    });
    join_all(streams).await;

    let after = next.load(Ordering::SeqCst);
    println!("{} workloads answered alike in {:.0?}: seeds {first} to {}", after - first, started.elapsed(), after - 1);
}
