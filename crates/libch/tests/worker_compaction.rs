//! Compaction through operations, against the fork's worker: rounds of `plan` and `merge` on a table many inserts
//! filled, alone and beside a stock shared writer and querying fleets.

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use futures_util::StreamExt;
use futures_util::stream;
use libch::stock::{StockWriter, StockWriterOptions};
use libch::worker::OpWriter;
use libch::writer::{Budget, Insert, PlannedMerge, TableName};
use libch::{Outcome, Writer};
use libch_testkit::ops::{active, assert_clean, insert, short_grace, stored, tenant_with_parts};
use libch_testkit::schema::{Batch, batch, daily, events};
use libch_testkit::{Reader, TenantFixture, stack};

/// What a fixed set of queries answers on a tenant: counts, sums and content hashes of the events, of their
/// projection and of the view's target.
async fn answers(fixture: &TenantFixture) -> Vec<u64> {
    let reader = Reader::open(stack().await, fixture).await.unwrap();
    let mut answers = Vec::new();
    for sql in [
        "SELECT count() FROM d.events",
        "SELECT sum(v) FROM d.events",
        "SELECT sum(cityHash64(ts, kind, v)) FROM d.events",
        "SELECT sum(cityHash64(kind, total)) FROM (SELECT kind, sum(v) AS total FROM d.events GROUP BY kind SETTINGS force_optimize_projection = 1)",
        "SELECT sum(cityHash64(day, kind, total)) FROM (SELECT day, kind, sumMerge(total) AS total FROM d.daily GROUP BY day, kind)",
        "SELECT count() FROM (SELECT kind, toStartOfHour(ts) AS hour, max(v) FROM d.events WHERE v % 7 = 3 GROUP BY kind, hour)",
    ] {
        answers.push(reader.number(sql).await.unwrap());
    }
    reader.close().await.unwrap();
    answers
}

/// Inserts the batches through workers, `at_once` at a time, and returns how many counter conflicts the workers
/// retried.
async fn insert_all(writer: &OpWriter, batches: &[Batch], at_once: usize) -> u64 {
    let inserts = batches.iter().map(|batch| async move { insert(writer, batch).await.unwrap().committed().expect("the batch commits").counter_conflicts });
    stream::iter(inserts).buffer_unordered(at_once).collect::<Vec<_>>().await.into_iter().sum()
}

/// Runs rounds of `plan` and `merge` on a table until a plan proposes nothing, and returns how many merges
/// committed and how many found their sources taken.
async fn compact(writer: &OpWriter, table: &TableName) -> (usize, usize) {
    let (mut committed, mut lost) = (0, 0);
    loop {
        let planned = writer.plan(table, &Budget::default()).await.unwrap();
        if planned.is_empty() {
            return (committed, lost);
        }
        let merges = planned.iter().map(|PlannedMerge { candidate, .. }| async move { writer.merge(table, candidate).await.unwrap() });
        for outcome in stream::iter(merges).buffer_unordered(4).collect::<Vec<_>>().await {
            match outcome {
                Outcome::Committed(_) => committed += 1,
                Outcome::ClaimLost => lost += 1,
                other => panic!("a planned merge ended {other:?}"),
            }
        }
    }
}

#[tokio::test]
async fn three_hundred_parts_compact_to_one_with_the_reference_writers_answers() {
    const BATCHES: u64 = 300;
    let batches: Vec<Batch> = (0..BATCHES).map(|index| batch(index, 20)).collect();
    let (fixture, writer) = tenant_with_parts(0).await;
    insert_all(&writer, &batches, 16).await;
    assert_eq!(active(&stored(&fixture, &events()).await).len(), 300);

    // The selector proposes many small parts at once, and two or three only once they are ten seconds old. Each
    // round merges what a plan proposes; a round that leaves several parts waits for them to age.
    let mut merges = 0;
    for round in 0.. {
        merges += compact(&writer, &events()).await.0 + compact(&writer, &daily()).await.0;
        let left = (active(&stored(&fixture, &events()).await).len(), active(&stored(&fixture, &daily()).await).len());
        if left.0 <= 2 && left.1 <= 2 {
            break;
        }
        assert!(round < 4, "{left:?} parts are left after {round} rounds");
        tokio::time::sleep(Duration::from_secs(11)).await;
    }
    assert!(merges >= 2, "both tables merged");
    let housekeeper = writer.clone().with_settings_xml(short_grace());
    tokio::time::sleep(Duration::from_millis(1_100)).await;
    for table in [events(), daily()] {
        let housekept = housekeeper.housekeep(&table).await.unwrap().committed().unwrap();
        assert!(housekept.remaining.is_empty(), "{:?}", housekept.remaining);
        assert!(stored(&fixture, &table).await.set.covered.is_empty());
    }
    assert_clean(&fixture).await;

    // The same batches through a stock writer, merged by `OPTIMIZE ... FINAL`.
    let stack = stack().await;
    let (reference, _) = tenant_with_parts(0).await;
    let stock = StockWriter::start(&stack.stock_env(), &reference.tenant, &reference.key, &StockWriterOptions::default()).await.unwrap();
    for batch in &batches {
        assert!(stock.insert(Insert { table: &events(), format: "JSONEachRow", token: &batch.token, data: batch.data.clone() }).await.unwrap().is_committed());
    }
    for table in ["d.events", "d.daily"] {
        stock.server().query(&format!("OPTIMIZE TABLE {table} FINAL")).await.unwrap();
    }
    stock.stop().await.unwrap();

    let expected = answers(&reference).await;
    assert_eq!(expected[0], BATCHES * 20);
    assert_eq!(answers(&fixture).await, expected);
}

#[tokio::test]
async fn merges_run_beside_inserts_a_stock_writer_and_querying_fleets() {
    const BATCHES: u64 = 160;
    let stack = stack().await;
    let batches: Vec<Batch> = (0..BATCHES).map(|index| batch(index, 100)).collect();
    let (fixture, writer) = tenant_with_parts(0).await;
    // The stock writer merges on its own schedule, from a view of the table it refreshes every three seconds.
    let options = StockWriterOptions {
        background_merges: true,
        merge_tree_settings_xml: "<refresh_parts_interval>3</refresh_parts_interval>".into(),
        ..StockWriterOptions::default()
    };
    let stock = StockWriter::start(&stack.stock_env(), &fixture.tenant, &fixture.key, &options).await.unwrap();

    let inserting = AtomicBool::new(true);
    let inserts = async {
        let conflicts = insert_all(&writer, &batches, 16).await;
        inserting.store(false, Ordering::SeqCst);
        conflicts
    };
    // Rounds of plans and merges while the batches arrive, and one more once they are all in.
    let merges = async {
        let (mut committed, mut lost) = (0, 0);
        loop {
            let last = !inserting.load(Ordering::SeqCst);
            let round = compact(&writer, &events()).await;
            (committed, lost) = (committed + round.0, lost + round.1);
            if last {
                return (committed, lost);
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    };
    // A fleet for each query, as BigHouse assigns one: every answer holds whole batches, each row once.
    let queries = async {
        let mut seen = Vec::new();
        while inserting.load(Ordering::SeqCst) {
            let reader = Reader::open_fleet(stack, &fixture, 2).await.unwrap();
            let answer = reader.value("SELECT count(), uniqExact(v), sum(v) FROM d.events").await.unwrap();
            reader.close().await.unwrap();
            let numbers: Vec<u64> = answer.split_whitespace().map(|number| number.parse().unwrap()).collect();
            assert!(numbers[0] % 100 == 0 && numbers[0] == numbers[1], "a fleet answered {answer}");
            seen.push(numbers[0]);
        }
        seen
    };
    let (conflicts, (committed, lost), seen) = tokio::join!(inserts, merges, queries);

    let stock_merges: u64 = stock.server().query("SELECT value FROM system.events WHERE event = 'Merge'").await.unwrap().trim().parse().unwrap_or(0);
    stock.stop().await.unwrap();
    println!(
        "{conflicts} counter conflicts retried; workers merged {committed} times and lost {lost} claims; the stock writer merged {stock_merges} times; fleets saw {seen:?} rows"
    );
    assert!(committed >= 1 && stock_merges >= 1, "the workers and the stock writer both merged");

    let parts = stored(&fixture, &events()).await;
    assert!(parts.set.intersections.is_empty(), "{:?}", parts.set.intersections);
    assert_eq!(parts.counter.map(|counter| counter.value), Some(BATCHES as i64), "each insert took one block number");
    let total: u64 = batches.iter().map(|batch| batch.sum).sum();
    let reader = Reader::open_fleet(stack, &fixture, 2).await.unwrap();
    assert_eq!(reader.value("SELECT count(), uniqExact(v), sum(v) FROM d.events").await.unwrap(), format!("{}\t{}\t{total}", BATCHES * 100, BATCHES * 100));
    assert_eq!(reader.number("SELECT sumMerge(total) FROM d.daily").await.unwrap(), total);
    reader.close().await.unwrap();
    assert_clean(&fixture).await;
}
