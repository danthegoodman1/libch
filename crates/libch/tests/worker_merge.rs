//! The worker's `merge`, against the fork's binary: named source parts of one partition become one part, after
//! the worker has checked the names against the table's directory.

use libch::catalog::ClaimState;
use libch::inodedb::Op;
use libch::worker::OpWriter;
use libch::writer::{Budget, Insert, PlannedMerge, TableName};
use libch::{Outcome, PartInfo, Writer};
use libch_testkit::apply;
use libch_testkit::ops::{active, assert_clean, merge, now_ms, short_grace, stored, tenant_with_parts, totals};
use libch_testkit::schema::{batch, daily, events};

#[tokio::test]
async fn named_parts_merge_into_one_and_keep_their_rows() {
    let (fixture, writer) = tenant_with_parts(4).await;
    let before = totals(&fixture).await;
    assert_eq!(before.0, 400);

    let all = ["202611_1_1_0", "202611_2_2_0", "202611_3_3_0", "202611_4_4_0"];
    let merged = merge(&writer, &events(), "202611", &all).await.unwrap().committed().expect("the merge commits");
    assert_eq!(merged.part.name(), "202611_1_4_1");
    let after = stored(&fixture, &events()).await;
    assert_eq!(active(&after), ["202611_1_4_1"]);
    assert_eq!(after.set.covered.len(), 4, "the sources stay until housekeeping removes them");
    assert_eq!(after.counter.map(|counter| counter.value), Some(4), "a merge takes no block number");

    // The sources keep claims that say the merge is done, so a writer with an older view merges nothing over it.
    let catalog = fixture.catalog().await.unwrap();
    let coordination = catalog.coordination(&catalog.table(&events()).await.unwrap().unwrap()).await.unwrap();
    for source in all {
        let claim = coordination.claim(source).unwrap_or_else(|| panic!("{source} keeps its claim"));
        assert_eq!((claim.state, claim.result.as_str()), (ClaimState::Done, "202611_1_4_1"));
    }

    // The view's target is an aggregating table: its parts merge by its own rule.
    let merged = merge(&writer, &daily(), "all", &["all_1_1_1", "all_2_2_1", "all_3_3_1", "all_4_4_1"]).await.unwrap().committed().unwrap();
    assert_eq!(merged.part.name(), "all_1_4_2");
    assert_eq!(totals(&fixture).await, before);
    assert_clean(&fixture).await;
}

#[tokio::test]
async fn a_merged_part_takes_part_in_the_next_merge() {
    let (fixture, writer) = tenant_with_parts(3).await;
    let before = totals(&fixture).await;
    let first = merge(&writer, &events(), "202611", &["202611_1_1_0", "202611_2_2_0"]).await.unwrap().committed().unwrap();
    assert_eq!(first.part.name(), "202611_1_2_1");
    let second = merge(&writer, &events(), "202611", &["202611_1_2_1", "202611_3_3_0"]).await.unwrap().committed().unwrap();
    assert_eq!(second.part.name(), "202611_1_3_2");
    assert_eq!(active(&stored(&fixture, &events()).await), ["202611_1_3_2"]);
    assert_eq!(totals(&fixture).await, before);
    assert_clean(&fixture).await;
}

#[tokio::test]
async fn sources_around_an_active_part_are_refused() {
    let (fixture, writer) = tenant_with_parts(3).await;
    let before = active(&stored(&fixture, &events()).await);

    let around = merge(&writer, &events(), "202611", &["202611_1_1_0", "202611_3_3_0"]).await.unwrap();
    assert!(matches!(&around, Outcome::Refused(reason) if reason.contains("not adjacent") && reason.contains("202611_2_2_0")), "{around:?}");
    let reversed = merge(&writer, &events(), "202611", &["202611_2_2_0", "202611_1_1_0"]).await.unwrap();
    assert!(matches!(&reversed, Outcome::Refused(reason) if reason.contains("not adjacent")), "{reversed:?}");
    let elsewhere = merge(&writer, &events(), "202612", &["202611_1_1_0", "202611_2_2_0"]).await.unwrap();
    assert!(matches!(&elsewhere, Outcome::Refused(reason) if reason.contains("outside partition 202612")), "{elsewhere:?}");
    let single = merge(&writer, &events(), "202611", &["202611_1_1_0"]).await.unwrap();
    assert!(matches!(&single, Outcome::Refused(reason) if reason.contains("two or more")), "{single:?}");

    let after = stored(&fixture, &events()).await;
    assert_eq!(active(&after), before, "a refused merge changes nothing");
    assert!(after.other.iter().all(|name| !name.starts_with("tmp")), "and leaves no temporary directory: {:?}", after.other);
    let catalog = fixture.catalog().await.unwrap();
    assert!(catalog.coordination(&catalog.table(&events()).await.unwrap().unwrap()).await.unwrap().claims.is_empty(), "and takes no claim");
}

#[tokio::test]
async fn a_source_that_is_covered_or_gone_loses_the_claim() {
    let (fixture, writer) = tenant_with_parts(3).await;
    assert!(merge(&writer, &events(), "202611", &["202611_1_1_0", "202611_2_2_0"]).await.unwrap().is_committed());

    // A plan made before that merge names its sources again, alone and with a neighbor.
    assert_eq!(merge(&writer, &events(), "202611", &["202611_1_1_0", "202611_2_2_0"]).await.unwrap(), Outcome::ClaimLost);
    assert_eq!(merge(&writer, &events(), "202611", &["202611_2_2_0", "202611_3_3_0"]).await.unwrap(), Outcome::ClaimLost);
    assert_eq!(merge(&writer, &events(), "202611", &["202611_3_3_0", "202611_9_9_0"]).await.unwrap(), Outcome::ClaimLost, "a part that never was");

    assert_eq!(active(&stored(&fixture, &events()).await), ["202611_1_2_1", "202611_3_3_0"]);
    assert_clean(&fixture).await;
}

#[tokio::test]
async fn another_writers_claim_holds_a_merge_off_until_it_expires() {
    let (fixture, writer) = tenant_with_parts(2).await;
    let sources = ["202611_1_1_0", "202611_2_2_0"];
    let catalog = fixture.catalog().await.unwrap();
    let definition = catalog.table(&events()).await.unwrap().unwrap();
    let claim = format!("coord/{}/claims/202611_2_2_0", definition.data_path());

    // Another writer holds one source for a merge of its own.
    let client = fixture.client().await.unwrap();
    let held = format!("held 0a0b0c0d {} 202611_2_3_1", now_ms() + 60_000);
    apply(&client, [Op::mkdir(format!("coord/{}/claims", definition.data_path()).as_bytes()).parents(), Op::put(claim.as_bytes(), held.into_bytes())])
        .await
        .unwrap();
    assert_eq!(merge(&writer, &events(), "202611", &sources).await.unwrap(), Outcome::ClaimLost);
    assert_eq!(active(&stored(&fixture, &events()).await), sources);
    let coordination = catalog.coordination(&definition).await.unwrap();
    assert_eq!(coordination.claims.len(), 1, "a merge that loses one claim takes none: {:?}", coordination.claims);

    // That writer died: its claim runs out, and the merge takes it over.
    let expired = format!("held 0a0b0c0d {} 202611_2_3_1", now_ms() - 1_000);
    apply(&client, [Op::put(claim.as_bytes(), expired.into_bytes())]).await.unwrap();
    assert!(merge(&writer, &events(), "202611", &sources).await.unwrap().is_committed());
    assert_eq!(active(&stored(&fixture, &events()).await), ["202611_1_2_1"]);
    assert_clean(&fixture).await;
}

#[tokio::test]
async fn a_pending_mutation_keeps_its_parts_out_of_merges() {
    let (fixture, writer) = tenant_with_parts(2).await;
    let sources = ["202611_1_1_0", "202611_2_2_0"];
    let catalog = fixture.catalog().await.unwrap();
    let definition = catalog.table(&events()).await.unwrap().unwrap();

    // An exclusive writer registered a mutation as block 3 and has yet to run it. The worker reads the entry's
    // name alone, so any file under that name stands for it here.
    let entry = format!("tree/{}/mutation_3.txt", definition.data_path());
    let client = fixture.client().await.unwrap();
    apply(&client, [Op::put(entry.as_bytes(), "a mutation entry")]).await.unwrap();
    let pending = merge(&writer, &events(), "202611", &sources).await.unwrap();
    assert!(matches!(&pending, Outcome::Refused(reason) if reason.contains("Mutation 3 is pending") && reason.contains("exclusive writer")), "{pending:?}");
    assert_eq!(active(&stored(&fixture, &events()).await), sources);

    // Once nothing is pending for the parts, they merge.
    apply(&client, [Op::unlink(entry.as_bytes())]).await.unwrap();
    assert!(merge(&writer, &events(), "202611", &sources).await.unwrap().is_committed());
    assert_clean(&fixture).await;
}

#[tokio::test]
async fn a_merge_names_a_merge_tree_table() {
    let (fixture, writer) = tenant_with_parts(1).await;
    let view = merge(&writer, &TableName::new("d", "daily_mv"), "all", &["all_1_1_0", "all_2_2_0"]).await.unwrap_err();
    assert!(view.to_string().contains("takes a `MergeTree` table"), "{view}");
    let absent = merge(&writer, &TableName::new("d", "absent"), "all", &["all_1_1_0", "all_2_2_0"]).await.unwrap_err();
    assert_eq!(absent.clickhouse_code(), Some(60), "{absent}");
    drop(fixture);
}

#[tokio::test]
async fn inserts_land_beside_a_merge() {
    let (fixture, writer) = tenant_with_parts(4).await;
    let sources = ["202611_1_1_0", "202611_2_2_0", "202611_3_3_0", "202611_4_4_0"];
    let late = batch(50, 100);
    let table = events();
    let (merged, inserted) = tokio::join!(
        merge(&writer, &table, "202611", &sources),
        writer.insert(Insert { table: &table, format: "JSONEachRow", token: &late.token, data: late.data.clone() })
    );
    assert_eq!(merged.unwrap().committed().unwrap().part.name(), "202611_1_4_1");
    assert!(inserted.unwrap().is_committed());

    assert_eq!(active(&stored(&fixture, &events()).await), ["202611_1_4_1", "202611_5_5_0"], "the insert takes a number past the merged range");
    assert_eq!(totals(&fixture).await.0, 500);
    assert_clean(&fixture).await;
}

/// A writer whose tables keep covered parts for one second.
fn with_a_short_grace(writer: &OpWriter) -> OpWriter {
    writer.clone().with_settings_xml(short_grace())
}

#[tokio::test]
async fn housekeeping_removes_covered_parts_once_their_grace_has_passed() {
    let (fixture, writer) = tenant_with_parts(3).await;
    let before = totals(&fixture).await;
    let sources = ["202611_1_1_0", "202611_2_2_0", "202611_3_3_0"];
    assert!(merge(&writer, &events(), "202611", &sources).await.unwrap().is_committed());

    // Under the default grace of eight minutes the sources stay, and housekeeping says why.
    let early = writer.housekeep(&events()).await.unwrap().committed().expect("housekeeping reports");
    assert!(early.removed.is_empty());
    assert_eq!(early.remaining.iter().map(|left| left.part.name()).collect::<Vec<_>>(), sources);
    assert!(
        early.remaining.iter().all(|left| left.reason.contains("202611_1_3_1 covered it") && left.reason.contains("stay for 480 s")),
        "{:?}",
        early.remaining
    );
    assert_eq!(stored(&fixture, &events()).await.set.covered.len(), 3);

    tokio::time::sleep(std::time::Duration::from_millis(2_100)).await;
    let late = with_a_short_grace(&writer).housekeep(&events()).await.unwrap().committed().unwrap();
    assert_eq!(late.removed.iter().map(PartInfo::name).collect::<Vec<_>>(), sources);
    assert!(late.remaining.is_empty(), "{:?}", late.remaining);

    let after = stored(&fixture, &events()).await;
    assert_eq!(after.entries.keys().collect::<Vec<_>>(), ["202611_1_3_1"], "the merged part alone is left");
    let catalog = fixture.catalog().await.unwrap();
    let coordination = catalog.coordination(&catalog.table(&events()).await.unwrap().unwrap()).await.unwrap();
    assert!(coordination.claims.is_empty(), "the sources' claims went with them: {:?}", coordination.claims);
    assert_eq!(totals(&fixture).await, before);

    // A second run finds nothing to do, and the table of the view is untouched by either.
    let again = with_a_short_grace(&writer).housekeep(&events()).await.unwrap().committed().unwrap();
    assert!(again.removed.is_empty() && again.remaining.is_empty());
    assert_eq!(stored(&fixture, &daily()).await.entries.len(), 3);
    assert_clean(&fixture).await;
}

#[tokio::test]
async fn housekeeping_leaves_a_part_another_writer_holds() {
    let (fixture, writer) = tenant_with_parts(2).await;
    assert!(merge(&writer, &events(), "202611", &["202611_1_1_0", "202611_2_2_0"]).await.unwrap().is_committed());
    let catalog = fixture.catalog().await.unwrap();
    let definition = catalog.table(&events()).await.unwrap().unwrap();

    // A writer with an old view of the table claimed one source for a merge of its own.
    let claim = format!("coord/{}/claims/202611_2_2_0", definition.data_path());
    let held = format!("held 0a0b0c0d {} 202611_2_3_1", now_ms() + 60_000);
    apply(&fixture.client().await.unwrap(), [Op::put(claim.as_bytes(), held.into_bytes())]).await.unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(2_100)).await;
    let housekept = with_a_short_grace(&writer).housekeep(&events()).await.unwrap().committed().unwrap();
    assert_eq!(housekept.removed.iter().map(PartInfo::name).collect::<Vec<_>>(), ["202611_1_1_0"]);
    assert_eq!(housekept.remaining.len(), 1);
    assert_eq!(housekept.remaining[0].part.name(), "202611_2_2_0");
    assert!(housekept.remaining[0].reason.contains("holds a claim"), "{:?}", housekept.remaining);
    assert_eq!(stored(&fixture, &events()).await.entries.len(), 2);
}

#[tokio::test]
async fn housekeeping_keeps_the_tokens_of_recent_batches() {
    let (fixture, writer) = tenant_with_parts(2).await;
    with_a_short_grace(&writer).housekeep(&events()).await.unwrap().committed().unwrap();
    let again = batch(0, 100);
    let retried = writer.insert(Insert { table: &events(), format: "JSONEachRow", token: &again.token, data: again.data }).await.unwrap();
    assert_eq!(retried, Outcome::Duplicate, "a batch sent again after housekeeping is still known");
    assert_eq!(totals(&fixture).await.0, 200);
}

#[tokio::test]
async fn a_plan_proposes_merges_the_worker_can_run_and_claims_nothing() {
    let (fixture, writer) = tenant_with_parts(8).await;
    let before = totals(&fixture).await;
    let catalog = fixture.catalog().await.unwrap();
    let definition = catalog.table(&events()).await.unwrap().unwrap();

    let planned = writer.plan(&events(), &Budget::default()).await.unwrap();
    assert!(!planned.is_empty(), "eight small parts of one partition are worth a merge");
    assert!(catalog.coordination(&definition).await.unwrap().claims.is_empty(), "a plan claims nothing");
    assert_eq!(active(&stored(&fixture, &events()).await).len(), 8, "and merges nothing");
    for PlannedMerge { candidate, rows, bytes } in &planned {
        assert!(candidate.sources.len() >= 2 && candidate.sources.iter().all(|source| source.partition_id == candidate.partition_id), "{candidate:?}");
        assert_eq!(*rows, 100 * candidate.sources.len() as u64, "a plan counts its sources' rows");
        assert!(*bytes > 0);
    }
    assert!(writer.plan(&events(), &Budget { max_merges: 1 }).await.unwrap().len() <= 1, "a plan keeps to its budget");

    for PlannedMerge { candidate, .. } in &planned {
        let merged = writer.merge(&events(), candidate).await.unwrap().committed().expect("a planned merge commits");
        assert!(candidate.sources.iter().all(|source| merged.part.contains(source)));
    }
    assert!(active(&stored(&fixture, &events()).await).len() < 8);
    assert_eq!(totals(&fixture).await, before);
    assert_clean(&fixture).await;
}

#[tokio::test]
async fn a_plan_leaves_out_the_parts_another_writer_holds() {
    let (fixture, writer) = tenant_with_parts(14).await;
    let catalog = fixture.catalog().await.unwrap();
    let definition = catalog.table(&events()).await.unwrap().unwrap();
    let claims = format!("coord/{}/claims", definition.data_path());
    let held = format!("held 0a0b0c0d {} 202611_7_8_1", now_ms() + 60_000);
    let client = fixture.client().await.unwrap();
    apply(&client, [Op::mkdir(claims.as_bytes()).parents()]).await.unwrap();
    for part in ["202611_7_7_0", "202611_8_8_0"] {
        apply(&client, [Op::put(format!("{claims}/{part}").as_bytes(), held.clone().into_bytes())]).await.unwrap();
    }

    // Six small parts lie on each side of the held ones, and six are worth a merge.
    let planned = writer.plan(&events(), &Budget::default()).await.unwrap();
    assert!(!planned.is_empty(), "the parts beside the held ones are worth a merge");
    for PlannedMerge { candidate, .. } in &planned {
        let below = candidate.sources.iter().all(|source| source.max_block < 7);
        let above = candidate.sources.iter().all(|source| source.min_block > 8);
        assert!(below || above, "a candidate lies on one side of the held parts: {candidate:?}");
        assert!(writer.merge(&events(), candidate).await.unwrap().is_committed(), "{candidate:?}");
    }
    let after = active(&stored(&fixture, &events()).await);
    assert!(after.contains(&"202611_7_7_0".to_string()) && after.contains(&"202611_8_8_0".to_string()), "{after:?}");
    let coordination = catalog.coordination(&definition).await.unwrap();
    let claim = coordination.claim("202611_7_7_0").expect("the other writer's claim stays");
    assert!(matches!(claim.state, ClaimState::Held { .. }) && claim.result == "202611_7_8_1", "{claim:?}");
}
