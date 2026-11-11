//! The hazards of merging named parts and of numbering a part, against the fork's worker. Each runs with its
//! protection, where the worker does the safe thing, and with the protection switched off by a failpoint, where
//! the damage shows.

use std::time::Duration;

use libch::check::Violation;
use libch::inodedb::Op;
use libch::stock::Exclusive;
use libch::worker::OpWriter;
use libch::writer::{Budget, Insert, TableName};
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
    // Six parts, which the selector merges at once when nothing stands in the way.
    let (fixture, writer) = tenant_with_parts(6).await;
    register_an_update(&fixture).await;
    assert_eq!(stored(&fixture, &events()).await.mutations, [7], "the mutation took the block after the six parts");

    // Batches inserted after the statement take later blocks, and the mutation leaves their rows alone.
    for index in 6..12 {
        assert!(insert(&writer, &batch(index, 100)).await.unwrap().is_committed());
    }
    let after: Vec<String> = (8..14).map(|block| format!("202611_{block}_{block}_0")).collect();
    assert_eq!(active(&stored(&fixture, &events()).await)[6..], after[..]);

    // The parts the mutation applies to wait for the exclusive writer that finishes it. The later parts merge.
    let across = merge(&writer, &events(), "202611", &["202611_6_6_0", "202611_8_8_0"]).await.unwrap();
    assert!(matches!(&across, Outcome::Refused(reason) if reason.contains("Mutation 7 is pending for part 202611_6_6_0")), "{across:?}");
    let before = merge(&writer, &events(), "202611", &["202611_1_1_0", "202611_2_2_0"]).await.unwrap();
    assert!(matches!(&before, Outcome::Refused(reason) if reason.contains("Mutation 7 is pending")), "{before:?}");
    // The selector alone would propose the six earlier parts as well: they share the mutation, and a server
    // merges such parts and mutates the result.
    let planned = writer.plan(&events(), &Budget::default()).await.unwrap();
    assert_eq!(planned.len(), 1, "{planned:?}");
    assert_eq!(planned[0].candidate.sources.iter().map(PartInfo::name).collect::<Vec<_>>(), after, "a plan proposes the parts no mutation is pending for");
    assert_eq!(planned[0].rows, 600);
    assert!(writer.merge(&events(), &planned[0].candidate).await.unwrap().is_committed());

    let expected: u64 = (0..12).map(|index| batch(index, 100).sum).sum::<u64>() + 600 * 1_000_000;
    finish_mutations(&fixture).await;
    assert_eq!(sum_of_events(&fixture).await, expected, "the update reached the six batches before it, and no other");
    // The exclusive writer mutated the six earlier parts, and may have merged them since. The later rows are in
    // the part the worker merged, at the data version of their inserts.
    let parts = stored(&fixture, &events()).await;
    assert!(parts.active().iter().all(|part| part.min_block >= 8 || part.mutation == 7), "{:?}", active(&parts));
    assert!(active(&parts).contains(&"202611_8_13_1".to_string()), "{:?}", active(&parts));
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

/// A covered source has two protections. The worker's check of its sources finds the part that covers it, and
/// the source's claim, which the merge that covered it left `done`, refuses the next merge's claim. With the
/// check off the claim still holds. With the claim gone too, the merge commits a part that intersects the one
/// that covers its source.
#[tokio::test]
async fn a_covered_source_would_be_merged_twice() {
    let (fixture, writer) = tenant_with_parts(3).await;
    assert!(merge(&writer, &events(), "202611", &["202611_1_1_0", "202611_2_2_0"]).await.unwrap().is_committed());
    let again = ["202611_2_2_0", "202611_3_3_0"];

    assert_eq!(merge(&writer, &events(), "202611", &again).await.unwrap(), Outcome::ClaimLost, "the check finds 202611_2_2_0 covered");
    assert_eq!(merge(&unchecked(&writer), &events(), "202611", &again).await.unwrap(), Outcome::ClaimLost, "the done claim refuses the merge");
    assert_eq!(active(&stored(&fixture, &events()).await), ["202611_1_2_1", "202611_3_3_0"]);
    assert_eq!(totals(&fixture).await.0, 300);

    // Housekeeping removes a covered part and its claim in one commit, so no writer finds the part without the
    // claim. The test removes the claim alone.
    let catalog = fixture.catalog().await.unwrap();
    let definition = catalog.table(&events()).await.unwrap().unwrap();
    apply(catalog.client(), [Op::unlink(format!("coord/{}/claims/202611_2_2_0", definition.data_path()))]).await.unwrap();
    let merged = merge(&unchecked(&writer), &events(), "202611", &again).await.unwrap().committed().expect("the unprotected merge commits");
    assert_eq!(merged.part.name(), "202611_2_3_1");
    let report = libch::check::tenant(&fixture.tenant, &fixture.key).await.unwrap();
    let intersecting = Violation::IntersectingParts { table: events(), first: "202611_1_2_1".into(), second: "202611_2_3_1".into() };
    assert!(report.violations.contains(&intersecting), "{report}");
}

const PLAIN: &str = "CREATE TABLE d.plain (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS non_replicated_deduplication_window = 10000";

fn plain() -> TableName {
    TableName::new("d", "plain")
}

/// Inserts rows `k` of `keys` into `d.plain`.
async fn insert_plain(writer: &OpWriter, token: &str, keys: std::ops::Range<u64>) -> Outcome<libch::writer::Inserted> {
    let data: String = keys.map(|k| format!("{{\"k\":{k}}}\n")).collect();
    writer.insert(Insert { table: &plain(), format: "JSONEachRow", token, data: data.into() }).await.unwrap()
}

/// A tenant whose table two workers and then an exclusive writer have written. The workers' inserts took blocks
/// 1 and 2 and left the counter at 2. The exclusive writer numbered its own inserts 3 and 4, merged all four
/// parts into `all_1_4_1`, and left the counter alone, as exclusive writers do. The merge's sources are gone, as
/// they are once the writer's cleanup has run.
async fn after_an_exclusive_epoch() -> (TenantFixture, OpWriter) {
    let stack = stack().await;
    let fixture = TenantFixture::create(stack).await.unwrap();
    Exclusive::run(&stack.stock_env(), &fixture.tenant, &fixture.key, &["CREATE DATABASE d", PLAIN]).await.unwrap();
    let writer = OpWriter::new(stack.worker_command(), &fixture.tenant, &fixture.key);
    assert!(insert_plain(&writer, "first", 0..10).await.is_committed());
    assert!(insert_plain(&writer, "second", 10..20).await.is_committed());

    let statements =
        ["INSERT INTO d.plain SELECT 20 + number FROM numbers(10)", "INSERT INTO d.plain SELECT 30 + number FROM numbers(10)", "OPTIMIZE TABLE d.plain FINAL"];
    Exclusive::run(&stack.stock_env(), &fixture.tenant, &fixture.key, &statements).await.unwrap();
    let parts = stored(&fixture, &plain()).await;
    assert_eq!(active(&parts), ["all_1_4_1"]);
    assert_eq!(parts.counter.as_ref().map(|counter| counter.value), Some(2), "the counter is the shared epoch's");

    let catalog = fixture.catalog().await.unwrap();
    let definition = catalog.table(&plain()).await.unwrap().unwrap();
    let covered = parts.set.covered.iter().map(|covered| Op::remove_all(format!("tree/{}/{}", definition.data_path(), covered.part.name())));
    apply(catalog.client(), covered).await.unwrap();
    (fixture, writer)
}

async fn rows_of_plain(fixture: &TenantFixture) -> u64 {
    Reader::number_once(stack().await, fixture, "SELECT count() FROM d.plain").await.unwrap()
}

/// An insert trusts the block counter only when a writer of its own epoch wrote it. After an exclusive epoch the
/// counter trails the parts, and the insert raises it from the names in the table's directory first.
#[tokio::test]
async fn a_counter_from_before_an_exclusive_epoch_would_hide_an_insert() {
    let (fixture, writer) = after_an_exclusive_epoch().await;
    let inserted = insert_plain(&writer, "third", 40..50).await.committed().unwrap().parts.unwrap();
    assert_eq!(inserted[0].part.name(), "all_5_5_0", "the insert numbers its part above the exclusive writer's");
    assert_eq!(rows_of_plain(&fixture).await, 50);
    assert_clean(&fixture).await;

    // Trusting the counter as it stands, the insert takes block 3 again. Its part commits, and the part the
    // exclusive writer merged covers it, so readers never see its rows and the next cleanup removes them.
    let (fixture, writer) = after_an_exclusive_epoch().await;
    let trusting = writer.clone().with_settings_xml(fail_point("libch_insert_trust_counter"));
    let inserted = insert_plain(&trusting, "third", 40..50).await.committed().expect("the insert answers that it committed").parts.unwrap();
    assert_eq!(inserted[0].part.name(), "all_3_3_0");
    assert_eq!(active(&stored(&fixture, &plain()).await), ["all_1_4_1"], "the new part is covered");
    assert_eq!(rows_of_plain(&fixture).await, 40, "the insert's ten rows are lost");
}
