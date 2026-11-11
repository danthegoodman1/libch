//! The hazards of merging named parts, against the fork's worker. Each runs with its protection, where the worker
//! commits nothing, and with the protection switched off by a failpoint, where the damage shows.

use std::time::Duration;

use libch::stock::Exclusive;
use libch::worker::OpWriter;
use libch::writer::Budget;
use libch::{Outcome, PartInfo, Writer};
use libch_testkit::ops::{active, assert_clean, fail_point, insert, merge, stored, tenant_with_parts, totals};
use libch_testkit::schema::{batch, events};
use libch_testkit::{Reader, TenantFixture, apply, stack};

/// A writer whose merges skip the checks of their named sources.
fn unchecked(writer: &OpWriter) -> OpWriter {
    writer.clone().with_settings_xml(fail_point("libch_merge_unchecked_sources"))
}

#[tokio::test]
async fn sources_around_an_active_part_would_take_its_rows() {
    let (fixture, writer) = tenant_with_parts(3).await;
    let around = ["202611_1_1_0", "202611_3_3_0"];
    assert!(matches!(merge(&writer, &events(), "202611", &around).await.unwrap(), Outcome::Refused(_)));
    assert_eq!(totals(&fixture).await.0, 300);

    // Without the check the merged part spans the part between its sources, and readers lose that part's rows.
    let merged = merge(&unchecked(&writer), &events(), "202611", &around).await.unwrap().committed().expect("the unchecked merge commits");
    assert_eq!(merged.part.name(), "202611_1_3_1");
    assert_eq!(active(&stored(&fixture, &events()).await), ["202611_1_3_1"]);
    assert_eq!(totals(&fixture).await.0, 200, "the rows of 202611_2_2_0 are gone");
}

/// Registers an `UPDATE` of every event as a mutation and stops before the server runs it.
async fn register_an_update(fixture: &TenantFixture) {
    let statements = ["SYSTEM STOP MERGES d.events", "ALTER TABLE d.events UPDATE v = v + 1000000 WHERE 1"];
    Exclusive::run(&stack().await.stock_env(), &fixture.tenant, &fixture.key, &statements).await.unwrap();
}

/// Has an exclusive writer finish the table's mutations.
async fn finish_mutations(fixture: &TenantFixture) {
    let exclusive = Exclusive::start(&stack().await.stock_env(), &fixture.tenant, &fixture.key).await.unwrap();
    for _ in 0..400 {
        if exclusive.execute("SELECT count() FROM system.mutations WHERE NOT is_done").await.unwrap().trim() == "0" {
            return exclusive.stop().await.unwrap();
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("the exclusive writer left a mutation unfinished");
}

async fn sum_of_events(fixture: &TenantFixture) -> u64 {
    Reader::number_once(stack().await, fixture, "SELECT sum(v) FROM d.events").await.unwrap()
}

#[tokio::test]
async fn a_pending_mutation_keeps_its_parts_out_of_plans_and_merges() {
    let (fixture, writer) = tenant_with_parts(2).await;
    register_an_update(&fixture).await;
    assert_eq!(stored(&fixture, &events()).await.mutations, [3], "the mutation took the block after the two parts");

    // Batches inserted after the statement take later blocks, and the mutation leaves their rows alone.
    for index in 2..8 {
        assert!(insert(&writer, &batch(index, 100)).await.unwrap().is_committed());
    }
    let after: Vec<String> = (4..10).map(|block| format!("202611_{block}_{block}_0")).collect();
    assert_eq!(active(&stored(&fixture, &events()).await)[2..], after[..]);

    // The parts the mutation applies to wait for the exclusive writer that finishes it. The later parts merge.
    let across = merge(&writer, &events(), "202611", &["202611_2_2_0", "202611_4_4_0"]).await.unwrap();
    assert!(matches!(&across, Outcome::Refused(reason) if reason.contains("Mutation 3 is pending for part 202611_2_2_0")), "{across:?}");
    let before = merge(&writer, &events(), "202611", &["202611_1_1_0", "202611_2_2_0"]).await.unwrap();
    assert!(matches!(&before, Outcome::Refused(reason) if reason.contains("Mutation 3 is pending")), "{before:?}");
    let planned = writer.plan(&events(), &Budget::default()).await.unwrap();
    assert_eq!(planned.len(), 1, "{planned:?}");
    assert_eq!(planned[0].candidate.sources.iter().map(PartInfo::name).collect::<Vec<_>>(), after, "a plan proposes the parts no mutation is pending for");
    assert_eq!(planned[0].rows, 600);
    assert!(writer.merge(&events(), &planned[0].candidate).await.unwrap().is_committed());

    let expected: u64 = (0..8).map(|index| batch(index, 100).sum).sum::<u64>() + 200 * 1_000_000;
    finish_mutations(&fixture).await;
    assert_eq!(sum_of_events(&fixture).await, expected, "the update reached the two batches before it, and no other");
    assert_eq!(active(&stored(&fixture, &events()).await), ["202611_1_1_0_3", "202611_2_2_0_3", "202611_4_9_1"]);
    assert_clean(&fixture).await;
}

#[tokio::test]
async fn a_merge_across_a_pending_mutation_would_rewrite_later_rows() {
    let (fixture, writer) = tenant_with_parts(2).await;
    register_an_update(&fixture).await;
    assert!(insert(&writer, &batch(2, 100)).await.unwrap().is_committed());

    // Without the check the merged part takes the older source's place in the mutation's order, so the update
    // rewrites the batch inserted after it.
    let merged =
        merge(&unchecked(&writer), &events(), "202611", &["202611_2_2_0", "202611_4_4_0"]).await.unwrap().committed().expect("the unchecked merge commits");
    assert_eq!(merged.part.name(), "202611_2_4_1");
    let expected: u64 = (0..3).map(|index| batch(index, 100).sum).sum::<u64>() + 200 * 1_000_000;
    finish_mutations(&fixture).await;
    assert_eq!(sum_of_events(&fixture).await, expected + 100 * 1_000_000, "the update reached the later batch too");
}

#[tokio::test]
async fn overlapping_merges_commit_once() {
    let (fixture, writer) = tenant_with_parts(6).await;
    let before = totals(&fixture).await;
    let table = events();
    let name = |block: u64| format!("202611_{block}_{block}_0");
    let ranges: Vec<Vec<String>> = vec![(1..=4).map(name).collect(), (3..=6).map(name).collect(), (2..=3).map(name).collect(), (1..=6).map(name).collect()];
    let merges = ranges.iter().map(|range| {
        let sources: Vec<&str> = range.iter().map(String::as_str).collect();
        let (writer, table) = (&writer, &table);
        async move { merge(writer, table, "202611", &sources).await.unwrap() }
    });
    let outcomes = futures_util::future::join_all(merges).await;

    // Every pair of ranges shares a part, so one merge holds its sources and the others find them taken.
    let committed: Vec<_> = outcomes.iter().filter(|outcome| outcome.is_committed()).collect();
    assert_eq!(committed.len(), 1, "{outcomes:?}");
    assert!(outcomes.iter().all(|outcome| outcome.is_committed() || *outcome == Outcome::ClaimLost), "{outcomes:?}");
    let after = stored(&fixture, &events()).await;
    assert!(after.set.intersections.is_empty(), "{:?}", after.set.intersections);
    assert_eq!(totals(&fixture).await, before);
    assert_clean(&fixture).await;
}

#[tokio::test]
async fn a_part_above_the_counter_waits() {
    let (fixture, writer) = tenant_with_parts(2).await;
    let sources = ["202611_1_1_0", "202611_2_2_0"];
    let catalog = fixture.catalog().await.unwrap();
    let definition = catalog.table(&events()).await.unwrap().unwrap();
    let counter = format!("coord/{}/block_number", definition.data_path());

    // A listing read in pages can show a part and miss a lower-numbered one committed meanwhile. The counter a
    // merge read before its listing bounds what the listing shows in full, and here it stands below a source.
    let client = fixture.client().await.unwrap();
    apply(&client, [libch::inodedb::Op::put(counter.as_bytes(), "1")]).await.unwrap();
    let above = merge(&writer, &events(), "202611", &sources).await.unwrap();
    assert!(matches!(&above, Outcome::Refused(reason) if reason.contains("202611_2_2_0 ends above block 1")), "{above:?}");
    assert_eq!(active(&stored(&fixture, &events()).await), sources);

    apply(&client, [libch::inodedb::Op::put(counter.as_bytes(), "2")]).await.unwrap();
    assert!(merge(&writer, &events(), "202611", &sources).await.unwrap().is_committed());
    assert_clean(&fixture).await;
}
