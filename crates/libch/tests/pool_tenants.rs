//! One pool serving 64 tenants at once, against the fork's worker.
//!
//! Both tests set up 64 tenants through stock servers and take minutes, so `cargo test` leaves them out:
//! `cargo test -p libch --test pool_tenants -- --ignored` runs them.

use futures_util::future::join_all;
use libch::pool::{Pool, PoolOptions};
use libch::worker::OpWriter;
use libch::{Error, check};
use libch_testkit::conformance::{self, Harness, tenant_with_events};
use libch_testkit::ops::insert;
use libch_testkit::schema::{batch, events};
use libch_testkit::{Stack, TenantFixture, stack};

const TENANTS: usize = 64;

/// Writers whose operations all run in one pool.
struct Pooled(Pool);

impl Harness for Pooled {
    type Writer = OpWriter;

    async fn start(&self, _stack: &Stack, fixture: &TenantFixture, key: &str) -> Result<OpWriter, Error> {
        Ok(self.0.writer(&fixture.tenant, key))
    }

    async fn stop(&self, _writer: OpWriter) -> Result<(), Error> {
        Ok(())
    }

    fn writers_keep_their_epoch(&self) -> bool {
        false
    }
}

fn pool(stack: &Stack) -> Pool {
    Pool::new(stack.worker_command(), PoolOptions { waiting: 16, concurrency: TENANTS }).unwrap()
}

/// Sixty-four tenants each run a conformance scenario at the same time, through one pool. The scenarios boot
/// stock servers of their own to read and to check, which is most of what the machine does meanwhile.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "six minutes of stock servers; run with --ignored"]
async fn the_conformance_scenarios_pass_on_64_tenants_at_once() {
    let stack = stack().await;
    let harness = Pooled(pool(stack));
    let scenarios = (0..TENANTS).map(|tenant| {
        let harness = &harness;
        async move {
            match tenant % 7 {
                0 => conformance::inserts_reach_the_view_and_the_projection(stack, harness).await,
                1 => conformance::a_retried_batch_lands_once(stack, harness).await,
                2 => conformance::a_chain_of_two_views_gets_each_batch_once(stack, harness).await,
                3 => conformance::concurrent_inserts_keep_every_batch(stack, harness).await,
                4 => conformance::merges_leave_one_part_and_the_same_rows(stack, harness).await,
                5 => conformance::an_exclusive_epoch_fences_running_writers(stack, harness).await,
                _ => conformance::a_revoked_key_fences_its_writer(stack, harness).await,
            }
        }
    });
    join_all(scenarios).await;

    let stats = harness.0.stats();
    assert_eq!(stats.running, 0, "{stats:?}");
    assert!(stats.started > TENANTS as u64, "{stats:?}");
    assert_eq!(stats.failed_starts, 0, "{stats:?}");
    println!("{stats:?}");
}

/// Sixty-four tenants insert at the same time, so the pool holds 64 operations in flight, each with one tenant's
/// key. Every tenant ends with its own batches and no other's.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "three minutes of tenant setup; run with --ignored"]
async fn sixty_four_operations_run_at_a_time() {
    const BATCHES: u64 = 12;
    let stack = stack().await;
    let pool = pool(stack);
    let fixtures: Vec<TenantFixture> = join_all((0..TENANTS).map(|_| tenant_with_events(stack))).await;

    let inserts = fixtures.iter().enumerate().map(|(tenant, fixture)| {
        let writer = pool.writer(&fixture.tenant, &fixture.key);
        async move {
            // Tenant `n` sends batches of `n + 1` rows, so a batch that landed in another tenant would show.
            for index in 0..BATCHES {
                let batch = batch(tenant as u64 * 1_000 + index, tenant as u64 + 1);
                assert!(insert(&writer, &batch).await.unwrap().is_committed(), "tenant {tenant}, batch {index}");
            }
            writer.usage()
        }
    });
    let usage = join_all(inserts).await;

    let stats = pool.stats();
    assert_eq!(stats.most_running, TENANTS, "{stats:?}");
    assert!(usage.iter().all(|usage| usage.operations == BATCHES), "each writer counts its own tenant's operations");
    for (tenant, fixture) in fixtures.iter().enumerate() {
        let catalog = fixture.catalog().await.unwrap();
        let table = catalog.table(&events()).await.unwrap().unwrap();
        assert_eq!(catalog.rows(&table).await.unwrap(), BATCHES * (tenant as u64 + 1), "tenant {tenant}");
        assert_eq!(catalog.parts(&table).await.unwrap().active().len() as u64, BATCHES, "tenant {tenant}");
        let report = check::tenant(&fixture.tenant, &fixture.key).await.unwrap();
        assert!(report.is_clean(), "tenant {tenant}: {report}");
    }
    println!("{stats:?}");
    join_all(fixtures.into_iter().map(TenantFixture::remove)).await;
}
