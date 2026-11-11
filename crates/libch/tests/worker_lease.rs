//! A worker releases its InodeDB reader lease when it finishes, so nothing waits out the lease's 30 seconds.
//!
//! The test stands alone in its file: a lease of any reader holds back the store's whole free queue, and the
//! test measures when that queue moves.

use std::time::{Duration, Instant};

use libch::Writer;
use libch::inodedb::Op;
use libch::stock::Exclusive;
use libch::worker::OpWriter;
use libch::writer::Insert;
use libch_testkit::schema::{EVENTS_SCHEMA, batch, events};
use libch_testkit::{TenantFixture, apply, stack, wait_for_a_clear_horizon};

#[tokio::test]
async fn a_finished_worker_holds_no_lease() {
    let stack = stack().await;
    let fixture = TenantFixture::create(stack).await.unwrap();
    Exclusive::run(&stack.stock_env(), &fixture.tenant, &fixture.key, &EVENTS_SCHEMA).await.unwrap();
    let client = fixture.client().await.unwrap();
    wait_for_a_clear_horizon(&client).await;

    // Each worker takes a lease as it attaches the disk. A file freed after they have gone comes free at once
    // when their leases are gone too, and only after 30 seconds when one is left to run out.
    let worker = OpWriter::new(stack.worker_command(), &fixture.tenant, &fixture.key);
    worker.describe(true).await.unwrap();
    let batch = batch(0, 100);
    assert!(worker.insert(Insert { table: &events(), format: "JSONEachRow", token: &batch.token, data: batch.data }).await.unwrap().is_committed());

    apply(&client, [Op::put("probe", "freed after the workers left")]).await.unwrap();
    apply(&client, [Op::unlink("probe")]).await.unwrap();
    let freed = Instant::now();
    loop {
        let claim = client.claim_freed(100).await.unwrap();
        if !claim.entries.is_empty() {
            client.ack_freed(claim.id, &claim.entries).await.unwrap();
            break;
        }
        assert!(freed.elapsed() < Duration::from_secs(10), "a worker's lease still holds the free queue after 10 s");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
