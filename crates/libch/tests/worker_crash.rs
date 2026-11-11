//! Workers killed mid-operation, against the fork's binary. Each test holds a worker at a failpoint until its
//! deadline kills it, then checks what the tenant's disk holds and runs the operation again.

use std::time::Duration;

use libch::catalog::ClaimState;
use libch::worker::OpWriter;
use libch::{Error, Outcome, PartInfo, Writer};
use libch_testkit::TenantFixture;
use libch_testkit::ops::{
    active, assert_clean, assert_consistent, claim_ttl, fail_point, insert, merge, now_ms, short_grace, stored, tenant_with_parts, totals,
};
use libch_testkit::schema::{batch, daily, events};

const SOURCES: [&str; 3] = ["202611_1_1_0", "202611_2_2_0", "202611_3_3_0"];

/// A writer whose workers stop at `point` and die two seconds after their request, under `settings`.
fn doomed_at(writer: &OpWriter, point: &str, settings: &str) -> OpWriter {
    writer.clone().with_settings_xml(format!("{settings}{}", fail_point(point))).with_deadline(Duration::from_secs(2))
}

fn assert_killed<T: std::fmt::Debug>(result: Result<T, Error>) {
    let error = result.expect_err("the worker dies at its failpoint");
    assert!(error.to_string().contains("was killed"), "{error}");
}

/// The claims on the events table's parts, as `(part, held, result)`.
async fn claims(fixture: &TenantFixture) -> Vec<(String, bool, String)> {
    let catalog = fixture.catalog().await.unwrap();
    let coordination = catalog.coordination(&catalog.table(&events()).await.unwrap().unwrap()).await.unwrap();
    let mut claims: Vec<_> =
        coordination.claims.iter().map(|claim| (claim.part.clone(), matches!(claim.state, ClaimState::Held { .. }), claim.result.clone())).collect();
    claims.sort();
    claims
}

/// Waits until every held claim on the events table has run out.
async fn wait_for_claims_to_expire(fixture: &TenantFixture) {
    let catalog = fixture.catalog().await.unwrap();
    let coordination = catalog.coordination(&catalog.table(&events()).await.unwrap().unwrap()).await.unwrap();
    let latest = coordination.claims.iter().filter_map(|claim| if let ClaimState::Held { expiry_ms } = claim.state { Some(expiry_ms) } else { None }).max();
    if let Some(expiry) = latest {
        tokio::time::sleep(Duration::from_millis(expiry.saturating_sub(now_ms()) + 100)).await;
    }
}

/// The names in the events table's directory that start with `tmp`.
async fn temporary_directories(fixture: &TenantFixture) -> Vec<String> {
    stored(fixture, &events()).await.other.into_iter().filter(|name| name.starts_with("tmp")).collect()
}

/// A writer whose tables keep covered parts and temporary directories for one second.
fn with_short_lifetimes(writer: &OpWriter) -> OpWriter {
    writer.clone().with_settings_xml(
        "<merge_tree><old_parts_lifetime>1</old_parts_lifetime><temporary_directories_lifetime>1</temporary_directories_lifetime></merge_tree>",
    )
}

/// A merge killed while it holds its claims commits nothing. Its claims hold the sources until they run out, and
/// then another merge takes them over. `temporary` says whether the kill came after the merged part was written,
/// which leaves that part's temporary directory for housekeeping.
async fn a_merge_killed_before_its_commit(point: &str, temporary: usize) {
    let (fixture, writer) = tenant_with_parts(3).await;
    let before = totals(&fixture).await;
    let short = claim_ttl(1_500);
    assert_killed(merge(&doomed_at(&writer, point, &short), &events(), "202611", &SOURCES).await);

    // The worker renewed its claims while it was held: taken for a second and a half, they are live two seconds on.
    let catalog = fixture.catalog().await.unwrap();
    let coordination = catalog.coordination(&catalog.table(&events()).await.unwrap().unwrap()).await.unwrap();
    assert!(coordination.claims.iter().all(|claim| claim.is_live(now_ms())), "{:?}", coordination.claims);

    let held: Vec<_> = SOURCES.iter().map(|source| (source.to_string(), true, "202611_1_3_1".to_string())).collect();
    assert_eq!(claims(&fixture).await, held, "the dead worker's claims stay");
    let after = stored(&fixture, &events()).await;
    assert_eq!(active(&after), SOURCES);
    assert_eq!(after.entries.len(), 3, "no part joined the table: {:?}", after.entries.keys());
    let left = temporary_directories(&fixture).await;
    assert_eq!(left.len(), temporary, "{left:?}");
    assert!(left.iter().all(|name| name.starts_with("tmp_merge_202611_1_3_1_")), "{left:?}");
    assert_consistent(&fixture).await;

    wait_for_claims_to_expire(&fixture).await;
    let again = writer.clone().with_settings_xml(short);
    assert_eq!(merge(&again, &events(), "202611", &SOURCES).await.unwrap().committed().map(|merged| merged.part.name()), Some("202611_1_3_1".to_string()));
    assert_eq!(totals(&fixture).await, before);

    // Housekeeping under the default lifetimes leaves the dead worker's directory, and removes it once it is old.
    assert_eq!(writer.housekeep(&events()).await.unwrap().committed().unwrap().temporary_directories, 0);
    assert_eq!(temporary_directories(&fixture).await.len(), temporary);
    tokio::time::sleep(Duration::from_millis(2_100)).await;
    let housekept = with_short_lifetimes(&writer).housekeep(&events()).await.unwrap().committed().unwrap();
    assert_eq!(housekept.temporary_directories as usize, temporary);
    assert_eq!(housekept.removed.len(), 3, "the merge's sources went too: {housekept:?}");
    let end = stored(&fixture, &events()).await;
    assert_eq!(end.entries.keys().collect::<Vec<_>>(), ["202611_1_3_1"]);
    assert!(temporary_directories(&fixture).await.is_empty());
    assert_eq!(totals(&fixture).await, before);
    assert_clean(&fixture).await;
}

#[tokio::test]
async fn a_merge_killed_after_its_claims_leaves_them_to_run_out() {
    a_merge_killed_before_its_commit("libch_merge_pause_after_claims", 0).await;
}

#[tokio::test]
async fn a_merge_killed_before_its_commit_leaves_a_directory_for_housekeeping() {
    a_merge_killed_before_its_commit("inodedb_merge_pause_before_commit", 1).await;
}

#[tokio::test]
async fn a_merge_killed_after_its_commit_has_merged() {
    let (fixture, writer) = tenant_with_parts(3).await;
    let before = totals(&fixture).await;
    assert_killed(merge(&doomed_at(&writer, "libch_pause_before_response", ""), &events(), "202611", &SOURCES).await);

    // The caller got no answer. The table holds the merged part, and the same merge again finds its sources taken.
    assert_eq!(active(&stored(&fixture, &events()).await), ["202611_1_3_1"]);
    let done: Vec<_> = SOURCES.iter().map(|source| (source.to_string(), false, "202611_1_3_1".to_string())).collect();
    assert_eq!(claims(&fixture).await, done);
    assert_eq!(merge(&writer, &events(), "202611", &SOURCES).await.unwrap(), Outcome::ClaimLost);
    assert_eq!(totals(&fixture).await, before);
    assert_clean(&fixture).await;
}

#[tokio::test]
async fn housekeeping_killed_between_batches_is_finished_by_the_next_run() {
    let (fixture, writer) = tenant_with_parts(4).await;
    let before = totals(&fixture).await;
    assert!(merge(&writer, &events(), "202611", &["202611_1_1_0", "202611_2_2_0"]).await.unwrap().is_committed());
    assert!(merge(&writer, &events(), "202611", &["202611_3_3_0", "202611_4_4_0"]).await.unwrap().is_committed());
    tokio::time::sleep(Duration::from_millis(2_100)).await;

    // Housekeeping removes the parts each merged part covers in a batch of its own, and dies after the first.
    let doomed = doomed_at(&writer, "libch_housekeep_pause_between_batches", &short_grace());
    assert_killed(doomed.housekeep(&events()).await);
    let left = stored(&fixture, &events()).await;
    assert_eq!(left.entries.keys().collect::<Vec<_>>(), ["202611_1_2_1", "202611_3_3_0", "202611_3_4_1", "202611_4_4_0"]);
    assert_eq!(
        claims(&fixture).await.iter().map(|claim| claim.0.as_str()).collect::<Vec<_>>(),
        ["202611_3_3_0", "202611_4_4_0"],
        "a part's claim goes with it"
    );
    assert_clean(&fixture).await;

    let rest = writer.clone().with_settings_xml(short_grace()).housekeep(&events()).await.unwrap().committed().unwrap();
    assert_eq!(rest.removed.iter().map(PartInfo::name).collect::<Vec<_>>(), ["202611_3_3_0", "202611_4_4_0"]);
    assert_eq!(stored(&fixture, &events()).await.entries.keys().collect::<Vec<_>>(), ["202611_1_2_1", "202611_3_4_1"]);
    assert!(claims(&fixture).await.is_empty());
    assert_eq!(totals(&fixture).await, before);
    assert_clean(&fixture).await;
}

/// How many batches the events table and the view's target hold.
async fn batches_in(fixture: &TenantFixture) -> (usize, usize) {
    (stored(fixture, &events()).await.entries.len(), stored(fixture, &daily()).await.entries.len())
}

#[tokio::test]
async fn an_insert_killed_before_its_commit_lands_once_when_sent_again() {
    let (fixture, writer) = tenant_with_parts(1).await;
    let late = batch(1, 100);
    assert_killed(insert(&doomed_at(&writer, "inodedb_insert_pause_before_commit", ""), &late).await);
    assert_eq!(batches_in(&fixture).await, (1, 1), "no part of the batch joined a table");
    assert!(temporary_directories(&fixture).await.is_empty(), "an insert's part reaches the table's directory with its commit");
    assert_clean(&fixture).await;

    assert!(insert(&writer, &late).await.unwrap().is_committed());
    assert_eq!(insert(&writer, &late).await.unwrap(), Outcome::Duplicate);
    let expected = batch(0, 100).sum + late.sum;
    assert_eq!(totals(&fixture).await, (200, expected, expected));
    assert_clean(&fixture).await;
}

#[tokio::test]
async fn an_insert_killed_between_its_tables_reaches_the_rest_when_sent_again() {
    let (fixture, writer) = tenant_with_parts(1).await;
    let late = batch(1, 100);
    assert_killed(insert(&doomed_at(&writer, "inodedb_insert_pause_after_commit", ""), &late).await);

    // One of the insert's two tables holds the batch, and the other has yet to get it.
    let held = batches_in(&fixture).await;
    assert!(held == (2, 1) || held == (1, 2), "{held:?}");
    assert_clean(&fixture).await;

    // The batch sent again skips the table that holds it and lands in the other.
    let again = insert(&writer, &late).await.unwrap().committed().expect("the rest of the batch commits");
    assert_eq!(again.parts.as_ref().map(Vec::len), Some(1), "{:?}", again.parts);
    assert_eq!(batches_in(&fixture).await, (2, 2));
    assert_eq!(insert(&writer, &late).await.unwrap(), Outcome::Duplicate);
    let expected = batch(0, 100).sum + late.sum;
    assert_eq!(totals(&fixture).await, (200, expected, expected));
    assert_clean(&fixture).await;
}

#[tokio::test]
async fn an_insert_killed_after_its_commits_is_a_duplicate_when_sent_again() {
    let (fixture, writer) = tenant_with_parts(1).await;
    let late = batch(1, 100);
    assert_killed(insert(&doomed_at(&writer, "libch_pause_before_response", ""), &late).await);
    assert_eq!(batches_in(&fixture).await, (2, 2), "the batch is in both tables");

    assert_eq!(insert(&writer, &late).await.unwrap(), Outcome::Duplicate);
    let expected = batch(0, 100).sum + late.sum;
    assert_eq!(totals(&fixture).await, (200, expected, expected));
    assert_clean(&fixture).await;
}
