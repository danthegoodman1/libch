//! Operations whose connection to the store fails. A proxy before the store loses the answer of one call, or
//! answers it as a standby that names the writer, and each test moves that fault through every call an
//! operation makes. The store's client sends a commit again under the request id it first carried, so the
//! operation commits once whichever call the fault meets.

use std::time::Duration;

use libch::worker::{OpWriter, Sandbox};
use libch::{Outcome, Tenant, Writer};
use libch_testkit::faults::{Fault, FaultyStore};
use libch_testkit::ops::{active, assert_clean, insert, merge, short_grace, stored, tenant_with_parts};
use libch_testkit::schema::{batch, events};
use libch_testkit::{Reader, Stack, TenantFixture, stack};

/// A writer whose workers reach the tenant's store through the proxy.
fn through(stack: &Stack, proxy: &FaultyStore, fixture: &TenantFixture) -> OpWriter {
    let tenant = Tenant { store: proxy.endpoint.clone(), ..fixture.tenant.clone() };
    let mut command = stack.worker_command();
    if command.sandbox.is_some() {
        // A sandboxed worker reaches the proxy's port, and the store's own once the proxy names it as the writer.
        let mut sandbox = Sandbox::reaching(&proxy.endpoint, &stack.s3_endpoint).unwrap();
        sandbox.connect_ports.extend(stack.sandbox().connect_ports);
        command = command.sandboxed(sandbox);
    }
    OpWriter::new(command, &tenant, &fixture.key)
}

/// How many calls to the store `operation` makes through the proxy.
async fn calls_of<T>(proxy: &FaultyStore, operation: impl Future<Output = T>) -> (T, u64) {
    proxy.disarm();
    let result = operation.await;
    (result, proxy.calls())
}

async fn assert_events(fixture: &TenantFixture, rows: u64, sum: u64) {
    let reader = Reader::open(stack().await, fixture).await.unwrap();
    assert_eq!(reader.value("SELECT count(), uniqExact(v), sum(v) FROM d.events").await.unwrap(), format!("{rows}\t{rows}\t{sum}"));
    assert_eq!(reader.number("SELECT sumMerge(total) FROM d.daily").await.unwrap(), sum, "each batch reached the view once");
    reader.close().await.unwrap();
}

#[tokio::test]
async fn an_insert_lands_once_whichever_call_fails() {
    let stack = stack().await;
    let (fixture, _direct) = tenant_with_parts(0).await;
    let proxy = FaultyStore::before(&stack.inodedb_endpoint).await;
    let writer = through(stack, &proxy, &fixture);

    // The first insert into a table makes its coordination directories, so the second shows what an insert costs.
    let mut sent = vec![batch(0, 1_000), batch(1, 1_000)];
    assert!(insert(&writer, &sent[0]).await.unwrap().is_committed());
    let (inserted, calls) = calls_of(&proxy, insert(&writer, &sent[1])).await;
    assert!(inserted.unwrap().is_committed());
    assert!(calls >= 10, "an insert through a view makes more than a handful of calls: {calls}");

    for fault in [Fault::LostAnswer, Fault::NotWriter] {
        for call in 1..=calls {
            let batch = batch(sent.len() as u64, 1_000);
            proxy.arm(call, fault);
            let outcome = insert(&writer, &batch).await.unwrap_or_else(|error| panic!("{fault:?} at call {call} of {calls}: {error}"));
            assert!(outcome.is_committed(), "{fault:?} at call {call} of {calls}: the insert ended {}", outcome.name());
            sent.push(batch);
        }
    }
    // An insert's calls vary by one or two, so a fault armed for the last of them may meet none.
    assert!(proxy.delivered() + 4 >= 2 * calls, "{} of {} armed faults met a call", proxy.delivered(), 2 * calls);

    assert_events(&fixture, sent.iter().map(|batch| batch.rows).sum(), sent.iter().map(|batch| batch.sum).sum()).await;
    assert_eq!(active(&stored(&fixture, &events()).await).len(), sent.len(), "one part for each batch");
    assert_clean(&fixture).await;
}

#[tokio::test]
async fn a_merge_commits_once_whichever_call_fails() {
    let stack = stack().await;
    let (fixture, direct) = tenant_with_parts(2).await;
    let proxy = FaultyStore::before(&stack.inodedb_endpoint).await;
    let writer = through(stack, &proxy, &fixture);
    let (merged, calls) = calls_of(&proxy, merge(&writer, &events(), "202611", &["202611_1_1_0", "202611_2_2_0"])).await;
    assert!(merged.unwrap().is_committed());
    assert!(calls >= 10, "{calls}");

    let mut batches = 2;
    for fault in [Fault::LostAnswer, Fault::NotWriter] {
        for call in 1..=calls {
            // Two more parts, numbered after every block before them, and their merge under the fault.
            for _ in 0..2 {
                assert!(insert(&direct, &batch(batches, 100)).await.unwrap().is_committed());
                batches += 1;
            }
            let sources = [format!("202611_{}_{}_0", batches - 1, batches - 1), format!("202611_{batches}_{batches}_0")];
            proxy.arm(call, fault);
            let outcome = merge(&writer, &events(), "202611", &[&sources[0], &sources[1]]).await;
            let outcome = outcome.unwrap_or_else(|error| panic!("{fault:?} at call {call} of {calls}: {error}"));
            assert!(outcome.is_committed(), "{fault:?} at call {call} of {calls}: the merge ended {}", outcome.name());
        }
    }
    assert!(proxy.delivered() + 4 >= 2 * calls, "{} of {} armed faults met a call", proxy.delivered(), 2 * calls);

    let parts = stored(&fixture, &events()).await;
    assert_eq!(active(&parts).len() as u64, batches / 2, "each pair of parts is one merged part");
    assert!(parts.active().iter().all(|part| part.level == 1), "{:?}", active(&parts));
    let sent: Vec<_> = (0..batches).map(|index| batch(index, 100)).collect();
    assert_events(&fixture, sent.iter().map(|batch| batch.rows).sum(), sent.iter().map(|batch| batch.sum).sum()).await;
    assert_clean(&fixture).await;
}

#[tokio::test]
async fn housekeeping_ends_whichever_call_fails() {
    let stack = stack().await;
    let (fixture, direct) = tenant_with_parts(0).await;
    let proxy = FaultyStore::before(&stack.inodedb_endpoint).await;
    // Covered parts go after a second, so each housekeeping finds the sources of the merge before it.
    let writer = through(stack, &proxy, &fixture).with_settings_xml(short_grace());

    let mut batches = 0;
    let mut covered_pair = async || {
        for _ in 0..2 {
            assert!(insert(&direct, &batch(batches, 100)).await.unwrap().is_committed());
            batches += 1;
        }
        let sources = [format!("202611_{}_{}_0", batches - 1, batches - 1), format!("202611_{batches}_{batches}_0")];
        assert!(merge(&direct, &events(), "202611", &[&sources[0], &sources[1]]).await.unwrap().is_committed());
        tokio::time::sleep(Duration::from_millis(1_100)).await;
    };

    covered_pair().await;
    let (housekept, calls) = calls_of(&proxy, writer.housekeep(&events())).await;
    assert_eq!(housekept.unwrap().committed().unwrap().removed.len(), 2);
    assert!(calls >= 5, "{calls}");

    // The two faults take turns through the calls.
    for call in 1..=calls {
        let fault = if call % 2 == 1 { Fault::LostAnswer } else { Fault::NotWriter };
        covered_pair().await;
        proxy.arm(call, fault);
        let outcome = writer.housekeep(&events()).await.unwrap_or_else(|error| panic!("{fault:?} at call {call} of {calls}: {error}"));
        let housekept = match outcome {
            Outcome::Committed(housekept) => housekept,
            other => panic!("{fault:?} at call {call} of {calls}: housekeeping ended {}", other.name()),
        };
        assert!(housekept.remaining.is_empty(), "{fault:?} at call {call} of {calls}: {housekept:?}");
    }
    assert!(proxy.delivered() + 4 >= calls, "{} of {calls} armed faults met a call", proxy.delivered());

    let parts = stored(&fixture, &events()).await;
    assert!(parts.set.covered.is_empty(), "{:?}", parts.set.covered);
    assert_eq!(parts.entries.len(), active(&parts).len());
    assert_clean(&fixture).await;
}
