//! A kill loop against the fork's worker: rounds of inserts, plans, merges and housekeeping on one tenant, with
//! workers killed at moments of their own, and now and then an exclusive writer's DDL. After every round the
//! invariant checker passes and the table holds each row once.
//!
//! `LIBCH_KILL_ROUNDS` sets the rounds, 30 by default; the plan's gate runs 1,000.

use std::time::Duration;

use libch::stock::Exclusive;
use libch::worker::OpWriter;
use libch::writer::Budget;
use libch::{Outcome, Writer};
use libch_testkit::ops::{assert_clean, assert_consistent, claim_ttl, insert, short_grace, tenant_with_parts};
use libch_testkit::schema::{batch, events};
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

#[tokio::test(flavor = "multi_thread")]
async fn killed_workers_leave_every_row_once() {
    let rounds: u64 = std::env::var("LIBCH_KILL_ROUNDS").ok().and_then(|rounds| rounds.parse().ok()).unwrap_or(30);
    let stack = stack().await;
    let (fixture, plain) = tenant_with_parts(0).await;
    // Claims that last a second, so a dead merge's sources come free within a round or two, and covered parts
    // that go after a second.
    let writer = plain.with_settings_xml(format!("{}{}", claim_ttl(1_000), short_grace()));
    let mut dice = Dice(0x9e37_79b9_7f4a_7c15);
    let (mut next_batch, mut expected_rows, mut expected_sum) = (0u64, 0u64, 0u64);
    let (mut kills, mut merges, mut removed) = (0u64, 0u64, 0u64);

    for round in 0..rounds {
        // Inserts: a batch whose worker died goes again under its token until a worker answers.
        for _ in 0..4 + dice.roll(5) {
            let batch = batch(next_batch, 50);
            next_batch += 1;
            loop {
                match insert(&dice.doom(&writer), &batch).await {
                    Ok(Outcome::Committed(_)) | Ok(Outcome::Duplicate) => break,
                    Ok(other) => panic!("round {round}: batch {} ended {other:?}", batch.token),
                    Err(error) if error.to_string().contains("was killed") => kills += 1,
                    Err(error) => panic!("round {round}: batch {} failed: {error}", batch.token),
                }
            }
            expected_rows += batch.rows;
            expected_sum += batch.sum;
        }

        // A plan and its merges. A killed plan proposes nothing, and a killed merge leaves its claims to run out.
        let planned = match dice.doom(&writer).plan(&events(), &Budget { max_merges: 2 }).await {
            Ok(planned) => planned,
            Err(error) if error.to_string().contains("was killed") => {
                kills += 1;
                Vec::new()
            }
            Err(error) => panic!("round {round}: the plan failed: {error}"),
        };
        for candidate in planned {
            match dice.doom(&writer).merge(&events(), &candidate.candidate).await {
                Ok(Outcome::Committed(_)) => merges += 1,
                Ok(Outcome::ClaimLost) => {}
                Ok(other) => panic!("round {round}: a planned merge ended {other:?}"),
                Err(error) if error.to_string().contains("was killed") => kills += 1,
                Err(error) => panic!("round {round}: a merge failed: {error}"),
            }
        }

        // Housekeeping, which a kill may stop between two batches of removals.
        match dice.doom(&writer).housekeep(&events()).await {
            Ok(housekept) => removed += housekept.committed().map_or(0, |housekept| housekept.removed.len() as u64),
            Err(error) if error.to_string().contains("was killed") => kills += 1,
            Err(error) => panic!("round {round}: housekeeping failed: {error}"),
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

    println!("{rounds} rounds: {next_batch} batches, {kills} workers killed, {merges} merges, {removed} covered parts removed");
    assert!(kills > rounds, "the loop killed workers: {kills}");
    let reader = Reader::open(stack, &fixture).await.unwrap();
    assert_eq!(reader.value("SELECT count(), uniqExact(v), sum(v) FROM d.events").await.unwrap(), format!("{expected_rows}\t{expected_rows}\t{expected_sum}"));
    assert_eq!(reader.number("SELECT sumMerge(total) FROM d.daily").await.unwrap(), expected_sum);
    reader.close().await.unwrap();
    assert_clean(&fixture).await;
}
