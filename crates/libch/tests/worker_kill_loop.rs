//! A kill loop against the fork's worker: rounds of inserts, plans, merges and housekeeping on one tenant, with
//! workers killed at moments of their own. One round in seven runs beside a stock shared writer that merges on
//! its own schedule and dies by `SIGKILL` too, and one in ten ends with an exclusive writer's DDL. After every
//! round the invariant checker passes and the table holds each row once. The loop ends with a count of the blobs
//! its kills left in S3.
//!
//! `LIBCH_KILL_ROUNDS` sets the rounds, 30 by default; the plan's gate runs 1,000.

use std::time::{Duration, Instant};

use libch::stock::{Exclusive, StockWriter, StockWriterOptions};
use libch::worker::OpWriter;
use libch::writer::{Budget, Insert};
use libch::{Outcome, Writer, check};
use libch_testkit::ops::{assert_clean, assert_consistent, claim_ttl, insert, short_grace, tenant_with_parts};
use libch_testkit::schema::{Batch, batch, events};
use libch_testkit::{Reader, stack};

/// Numbers that are the same on every run.
struct Dice(u64);

impl Dice {
    fn roll(&mut self, sides: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % sides
    }

    /// One writer in two dies between 3 and 90 ms after its request goes out: before its commit, in it, or after.
    fn doom(&mut self, writer: &OpWriter) -> OpWriter {
        if self.roll(2) == 0 { writer.clone().with_deadline(Duration::from_millis(3 + self.roll(88))) } else { writer.clone() }
    }
}

/// What the loop counts.
#[derive(Default)]
struct Counts {
    batches: u64,
    worker_kills: u64,
    stock_kills: u64,
    stock_inserts: u64,
    merges: u64,
    removed: u64,
}

/// Sends a batch through the stock writer. One insert in two is cut short: the writer dies between 1 and 40 ms
/// after the statement goes out. Returns whether the batch is known to have landed.
async fn insert_through_stock(stock: &mut Option<StockWriter>, batch: &Batch, dice: &mut Dice, counts: &mut Counts) -> bool {
    let Some(writer) = stock.as_ref() else { return false };
    let table = events();
    let request = writer.insert(Insert { table: &table, format: "JSONEachRow", token: &batch.token, data: batch.data.clone() });
    if dice.roll(2) == 0 {
        let outcome = request.await.unwrap_or_else(|error| panic!("the stock writer's insert of {} failed: {error}", batch.token));
        assert!(matches!(outcome, Outcome::Committed(_) | Outcome::Duplicate), "the stock writer's insert of {} ended {outcome:?}", batch.token);
        counts.stock_inserts += 1;
        return true;
    }
    let answered = tokio::time::timeout(Duration::from_millis(1 + dice.roll(40)), request).await;
    stock.take().expect("the writer was just used").kill();
    counts.stock_kills += 1;
    matches!(answered, Ok(Ok(Outcome::Committed(_) | Outcome::Duplicate)))
}

#[tokio::test(flavor = "multi_thread")]
async fn killed_writers_leave_every_row_once() {
    let rounds: u64 = std::env::var("LIBCH_KILL_ROUNDS").ok().and_then(|rounds| rounds.parse().ok()).unwrap_or(30);
    let stack = stack().await;
    let (fixture, plain) = tenant_with_parts(0).await;
    // Claims that last a second, so a dead merge's sources come free within a round or two, and covered parts
    // that go after a second.
    let writer = plain.with_settings_xml(format!("{}{}", claim_ttl(1_000), short_grace()));
    let stock_options = StockWriterOptions {
        merge_tree_settings_xml: "<old_parts_lifetime>1</old_parts_lifetime>".into(),
        claim_ttl: Some(Duration::from_secs(1)),
        background_merges: true,
    };
    let mut dice = Dice(0x9e37_79b9_7f4a_7c15);
    let (mut expected_rows, mut expected_sum) = (0u64, 0u64);
    let mut counts = Counts::default();

    for round in 0..rounds {
        let mut stock =
            if round % 7 == 3 { Some(StockWriter::start(&stack.stock_env(), &fixture.tenant, &fixture.key, &stock_options).await.unwrap()) } else { None };

        // Inserts. One batch in four is large enough to keep its data in S3, where a kill can leave it. A batch
        // whose writer died goes again through workers, under its token, until a worker answers.
        for _ in 0..4 + dice.roll(5) {
            let rows = if dice.roll(4) == 0 { 5_000 } else { 50 };
            let batch = batch(counts.batches, rows);
            counts.batches += 1;
            let mut landed = dice.roll(2) == 0 && insert_through_stock(&mut stock, &batch, &mut dice, &mut counts).await;
            while !landed {
                match insert(&dice.doom(&writer), &batch).await {
                    Ok(Outcome::Committed(_)) | Ok(Outcome::Duplicate) => landed = true,
                    Ok(other) => panic!("round {round}: batch {} ended {other:?}", batch.token),
                    Err(error) if error.to_string().contains("was killed") => counts.worker_kills += 1,
                    Err(error) => panic!("round {round}: batch {} failed: {error}", batch.token),
                }
            }
            expected_rows += batch.rows;
            expected_sum += batch.sum;
        }

        // A plan and its merges. A killed plan proposes nothing, and a killed merge leaves its claims to run out.
        // Beside a stock writer a planned merge may find its sources merged already.
        let planned = match dice.doom(&writer).plan(&events(), &Budget { max_merges: 2 }).await {
            Ok(planned) => planned,
            Err(error) if error.to_string().contains("was killed") => {
                counts.worker_kills += 1;
                Vec::new()
            }
            Err(error) => panic!("round {round}: the plan failed: {error}"),
        };
        for candidate in planned {
            match dice.doom(&writer).merge(&events(), &candidate.candidate).await {
                Ok(Outcome::Committed(_)) => counts.merges += 1,
                Ok(Outcome::ClaimLost) => {}
                Ok(other) => panic!("round {round}: a planned merge ended {other:?}"),
                Err(error) if error.to_string().contains("was killed") => counts.worker_kills += 1,
                Err(error) => panic!("round {round}: a merge failed: {error}"),
            }
        }

        // Housekeeping, which a kill may stop between two batches of removals.
        match dice.doom(&writer).housekeep(&events()).await {
            Ok(housekept) => counts.removed += housekept.committed().map_or(0, |housekept| housekept.removed.len() as u64),
            Err(error) if error.to_string().contains("was killed") => counts.worker_kills += 1,
            Err(error) => panic!("round {round}: housekeeping failed: {error}"),
        }

        // A stock writer that outlived its inserts dies here: idle, or in a merge of its own.
        if let Some(stock) = stock.take() {
            stock.kill();
            counts.stock_kills += 1;
        }

        // Now and then an exclusive writer changes the table, which starts a new epoch under the next worker.
        if round % 10 == 9 {
            let add = format!("ALTER TABLE d.events ADD COLUMN extra_{round} UInt8 DEFAULT 0");
            Exclusive::run(&stack.stock_env(), &fixture.tenant, &fixture.key, &[&add]).await.unwrap();
        }

        assert_consistent(&fixture).await;
        let catalog = fixture.catalog().await.unwrap();
        let table = catalog.table(&events()).await.unwrap().unwrap();
        let rows = loop {
            // Housekeeping in a later round may remove a part between a listing and its read.
            if let Ok(rows) = catalog.rows(&table).await {
                break rows;
            }
        };
        assert_eq!(rows, expected_rows, "round {round}: the table's parts hold each row once");
    }

    println!(
        "{rounds} rounds: {} batches, {} workers killed, {} stock writers killed, {} stock inserts, {} merges, {} covered parts removed",
        counts.batches, counts.worker_kills, counts.stock_kills, counts.stock_inserts, counts.merges, counts.removed
    );
    assert!(counts.worker_kills > rounds, "the loop killed workers: {}", counts.worker_kills);
    assert!(rounds < 7 || counts.stock_kills > 0, "the loop killed stock writers");
    let reader = Reader::open(stack, &fixture).await.unwrap();
    assert_eq!(reader.value("SELECT count(), uniqExact(v), sum(v) FROM d.events").await.unwrap(), format!("{expected_rows}\t{expected_rows}\t{expected_sum}"));
    assert_eq!(reader.number("SELECT sumMerge(total) FROM d.daily").await.unwrap(), expected_sum);
    reader.close().await.unwrap();
    assert_clean(&fixture).await;

    // What the kills left in S3. Housekeeping first removes every blob the disk freed, which takes until the
    // dead writers' leases have run out.
    let started = Instant::now();
    let left = loop {
        let housekept = writer.housekeep(&events()).await.unwrap().committed().unwrap();
        let left = check::orphans(&fixture.tenant, &fixture.key).await.unwrap();
        println!("{:.0?}: housekeeping removed {} blobs; {left}", started.elapsed(), housekept.blobs_removed);
        if left.freed == 0 {
            break left;
        }
        assert!(started.elapsed() < Duration::from_secs(180), "freed blobs stay in S3: {left}");
        tokio::time::sleep(Duration::from_secs(5)).await;
    };
    let kills = counts.worker_kills + counts.stock_kills;
    println!(
        "{left}; {:.0} orphans and {:.1} MB for each 1,000 kills",
        left.orphans.len() as f64 * 1e3 / kills as f64,
        left.bytes() as f64 / kills as f64 / 1e3
    );
}
