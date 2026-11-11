//! The conformance scenarios, run against the worker: each operation in a process of its own.

use libch::Error;
use libch::worker::OpWriter;
use libch_testkit::conformance::{self, Harness};
use libch_testkit::{Stack, TenantFixture, stack};

struct Worker;

impl Harness for Worker {
    type Writer = OpWriter;

    async fn start(&self, stack: &Stack, fixture: &TenantFixture, key: &str) -> Result<OpWriter, Error> {
        Ok(OpWriter::new(stack.worker_command(), &fixture.tenant, key))
    }

    /// A writer through workers holds nothing between operations.
    async fn stop(&self, _writer: OpWriter) -> Result<(), Error> {
        Ok(())
    }

    /// Each operation's worker joins the disk's writers itself, which ends an exclusive epoch it finds.
    fn writers_keep_their_epoch(&self) -> bool {
        false
    }
}

#[tokio::test]
async fn inserts_reach_the_view_and_the_projection() {
    conformance::inserts_reach_the_view_and_the_projection(stack().await, &Worker).await;
}

#[tokio::test]
async fn a_retried_batch_lands_once() {
    conformance::a_retried_batch_lands_once(stack().await, &Worker).await;
}

#[tokio::test]
async fn a_chain_of_two_views_gets_each_batch_once() {
    conformance::a_chain_of_two_views_gets_each_batch_once(stack().await, &Worker).await;
}

#[tokio::test]
async fn concurrent_inserts_keep_every_batch() {
    conformance::concurrent_inserts_keep_every_batch(stack().await, &Worker).await;
}

#[tokio::test]
async fn merges_leave_one_part_and_the_same_rows() {
    conformance::merges_leave_one_part_and_the_same_rows(stack().await, &Worker).await;
}

#[tokio::test]
async fn an_exclusive_epoch_fences_running_writers() {
    conformance::an_exclusive_epoch_fences_running_writers(stack().await, &Worker).await;
}

#[tokio::test]
async fn a_revoked_key_fences_its_writer() {
    conformance::a_revoked_key_fences_its_writer(stack().await, &Worker).await;
}
