//! Read-only servers, assigned to a tenant after boot and clustered, answer as parallel replicas and release
//! their InodeDB leases when they stop.

use std::time::{Duration, Instant};

use libch::inodedb::{Grant, Op};
use libch::s3::S3Client;
use libch::stock::{Blank, Exclusive, Fleet};
use libch_testkit::{TenantFixture, apply, stack, wait_for_a_clear_horizon};
use tokio::sync::Mutex;

/// One fleet at a time: a reader's lease holds back the store's whole free queue, and the lease test measures
/// when that queue moves.
static ONE_FLEET: Mutex<()> = Mutex::const_new(());

/// A tenant with a table of `parts` parts of 20,000 rows each, in many small granules, so a scan has marks to share.
///
/// Both columns count up evenly and are stored as their second differences, so every file of every part is
/// small enough for the disk to keep inline in InodeDB. The fleet tests then read nothing from S3: the dev
/// stack's S3 service drops connections under the burst that four servers scanning at once make, and
/// ClickHouse's retries of those would stall a query for tens of seconds.
async fn tenant_with_a_large_table(stack: &libch_testkit::Stack, parts: u64) -> (TenantFixture, String) {
    let fixture = TenantFixture::create(stack).await.unwrap();
    let writer = Exclusive::start(&stack.stock_env(), &fixture.tenant, &fixture.key).await.unwrap();
    writer.execute("CREATE DATABASE d").await.unwrap();
    writer
        .execute("CREATE TABLE d.big (k UInt64 CODEC(DoubleDelta, ZSTD), v UInt64 CODEC(DoubleDelta, ZSTD)) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 256")
        .await
        .unwrap();
    // The parts stay as inserted: a merged part would outgrow an inline file.
    writer.execute("SYSTEM STOP MERGES").await.unwrap();
    for part in 0..parts {
        writer.execute(&format!("INSERT INTO d.big SELECT number, number * 3 FROM numbers({}, 20000)", part * 20_000)).await.unwrap();
    }
    let expected = writer.execute("SELECT count(), sum(v), uniqExact(k) FROM d.big FORMAT TSV").await.unwrap();
    writer.stop().await.unwrap();
    let blobs = S3Client::new(&fixture.tenant.s3).unwrap().list(&fixture.tenant.s3.key_prefix()).await.unwrap();
    assert!(blobs.is_empty(), "every file of the table lives inline in InodeDB: {blobs:?}");
    (fixture, expected)
}

#[tokio::test]
async fn four_servers_answer_as_parallel_replicas() {
    let _one = ONE_FLEET.lock().await;
    let stack = stack().await;
    let (fixture, expected) = tenant_with_a_large_table(stack, 150).await;
    assert_eq!(expected.trim(), "3000000\t13499995500000\t3000000");

    let key = fixture.mint(Grant::Read, Duration::from_secs(600)).await.unwrap();
    let blank = Blank::boot_many(&stack.stock_env(), 4).await.unwrap();
    for server in &blank {
        let disks = server.query("SELECT count() FROM system.disks WHERE name = 'bighouse'").await.unwrap();
        assert_eq!(disks.trim(), "0", "a blank server holds no tenant's disk");
    }

    let fleet = Fleet::assign(blank, &fixture.tenant, &key).await.unwrap();
    assert_eq!(fleet.servers().len(), 4);
    let served: Vec<&str> = fleet.databases().iter().map(|database| database.name.as_str()).collect();
    assert_eq!(served, ["d", "default"]);
    assert!(fleet.left_out().is_empty());

    let answer = fleet.query("SELECT count(), sum(v), uniqExact(k) FROM d.big FORMAT TSV").await.unwrap();
    assert_eq!(answer, expected, "the fleet answers as the writer did");
    let marks = fleet.marks_read().await.unwrap();
    assert!(marks.iter().all(|&read| read > 0), "each server read a share of the marks: {marks:?}");

    // Every server holds the tenant's tables, and refuses to write them.
    for server in fleet.servers() {
        assert_eq!(server.query("SELECT count() FROM d.big WHERE k < 1000").await.unwrap().trim(), "1000");
        let refused = server.query("INSERT INTO d.big VALUES (1, 1)").await.unwrap_err();
        assert!(refused.clickhouse_code().is_some(), "{refused}");
    }
    fleet.stop().await.unwrap();
}

#[tokio::test]
async fn a_fleet_reads_under_a_key_that_cannot_write() {
    let _one = ONE_FLEET.lock().await;
    let stack = stack().await;
    let (fixture, expected) = tenant_with_a_large_table(stack, 20).await;
    let key = fixture.mint(Grant::Read, Duration::from_secs(600)).await.unwrap();
    let reader = fixture.tenant.connect(&key).await.unwrap();
    assert!(reader.commit([Op::put("probe", "x")]).await.is_err(), "a read key commits nothing");

    let fleet = Fleet::assign(Blank::boot_many(&stack.stock_env(), 1).await.unwrap(), &fixture.tenant, &key).await.unwrap();
    assert_eq!(fleet.query("SELECT count(), sum(v), uniqExact(k) FROM d.big FORMAT TSV").await.unwrap(), expected);
    fleet.stop().await.unwrap();
}

#[tokio::test]
async fn stopping_a_fleet_releases_its_leases() {
    let _one = ONE_FLEET.lock().await;
    let stack = stack().await;
    let (fixture, _expected) = tenant_with_a_large_table(stack, 20).await;
    let writer = fixture.client().await.unwrap();
    wait_for_a_clear_horizon(&writer).await;
    let key = fixture.mint(Grant::Read, Duration::from_secs(600)).await.unwrap();
    let fleet = Fleet::assign(Blank::boot_many(&stack.stock_env(), 4).await.unwrap(), &fixture.tenant, &key).await.unwrap();
    assert_eq!(fleet.query("SELECT count() FROM d.big").await.unwrap().trim(), "400000");

    // Free a file after the fleet's servers opened their leases. InodeDB holds the freed entry back while a
    // lease that began before it might still read it.
    apply(&writer, [Op::put("probe", "freed after the fleet's leases began")]).await.unwrap();
    apply(&writer, [Op::unlink("probe")]).await.unwrap();
    let held_until = Instant::now() + Duration::from_millis(1_500);
    while Instant::now() < held_until {
        let claim = writer.claim_freed(100).await.unwrap();
        assert!(claim.entries.is_empty(), "the fleet's leases hold the freed entry back");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // A lease runs out on its own after 30 seconds. A stopped fleet has released its leases, so the entry
    // comes free well before that.
    fleet.stop().await.unwrap();
    let stopped = Instant::now();
    let released = loop {
        let claim = writer.claim_freed(100).await.unwrap();
        if !claim.entries.is_empty() {
            writer.ack_freed(claim.id, &claim.entries).await.unwrap();
            break stopped.elapsed();
        }
        assert!(stopped.elapsed() < Duration::from_secs(10), "the freed entry stays held after the fleet stopped");
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    assert!(released < Duration::from_secs(10), "released after {released:?}");
}
