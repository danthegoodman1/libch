//! The conformance scenarios, run against the reference: stock shared writers.

use libch::Error;
use libch::stock::{StockWriter, StockWriterOptions};
use libch_testkit::conformance::{self, Harness};
use libch_testkit::{Stack, TenantFixture, stack};

struct Stock;

impl Harness for Stock {
    type Writer = StockWriter;

    async fn start(&self, stack: &Stack, fixture: &TenantFixture, key: &str) -> Result<StockWriter, Error> {
        StockWriter::start(&stack.stock_env(), &fixture.tenant, key, &StockWriterOptions::default()).await
    }

    async fn stop(&self, writer: StockWriter) -> Result<(), Error> {
        writer.stop().await
    }
}

#[tokio::test]
async fn inserts_reach_the_view_and_the_projection() {
    conformance::inserts_reach_the_view_and_the_projection(stack().await, &Stock).await;
}

#[tokio::test]
async fn a_retried_batch_lands_once() {
    conformance::a_retried_batch_lands_once(stack().await, &Stock).await;
}

#[tokio::test]
async fn a_chain_of_two_views_gets_each_batch_once() {
    conformance::a_chain_of_two_views_gets_each_batch_once(stack().await, &Stock).await;
}

#[tokio::test]
async fn concurrent_inserts_keep_every_batch() {
    conformance::concurrent_inserts_keep_every_batch(stack().await, &Stock).await;
}

#[tokio::test]
async fn merges_leave_one_part_and_the_same_rows() {
    conformance::merges_leave_one_part_and_the_same_rows(stack().await, &Stock).await;
}

#[tokio::test]
async fn an_exclusive_epoch_fences_running_writers() {
    conformance::an_exclusive_epoch_fences_running_writers(stack().await, &Stock).await;
}

#[tokio::test]
async fn a_revoked_key_fences_its_writer() {
    conformance::a_revoked_key_fences_its_writer(stack().await, &Stock).await;
}
