//! What loading a table's parts costs a stock server at boot, beside a worker's `plan` of the same table. Both
//! load every part from the tenant's disk; the server does it once and then selects merges from memory.
//!
//! `PARTS=10000 cargo test -p libch --test plan_beside_stock_boot -- --ignored --nocapture` prints the figures.

use std::time::Instant;

use futures_util::{StreamExt, stream};
use libch::Writer;
use libch::stock::Exclusive;
use libch::worker::OpWriter;
use libch::writer::Budget;
use libch_testkit::measure::process_cpu;
use libch_testkit::schema::{EVENTS_SCHEMA, events};
use libch_testkit::{TenantFixture, stack};

const MANY_PARTS: &str = "<parts_to_throw_insert>1000000</parts_to_throw_insert><parts_to_delay_insert>1000000</parts_to_delay_insert><max_parts_in_total>10000000</max_parts_in_total>";
/// The same, with merges too small to select, so a booted server's CPU is its loading alone.
const NO_MERGES: &str = "<parts_to_throw_insert>1000000</parts_to_throw_insert><parts_to_delay_insert>1000000</parts_to_delay_insert><max_parts_in_total>10000000</max_parts_in_total><max_bytes_to_merge_at_max_space_in_pool>1</max_bytes_to_merge_at_max_space_in_pool>";

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn a_plan_beside_a_stock_boot() {
    let parts: usize = std::env::var("PARTS").ok().and_then(|value| value.parse().ok()).unwrap_or(10_000);
    let stack = stack().await;
    let fixture = TenantFixture::create(stack).await.unwrap();
    let writer = Exclusive::start_with(&stack.stock_env(), &fixture.tenant, &fixture.key, MANY_PARTS).await.unwrap();
    for statement in EVENTS_SCHEMA {
        writer.execute(statement).await.unwrap();
    }
    writer.execute("SYSTEM STOP MERGES").await.unwrap();
    let built = Instant::now();
    let inserts = (0..parts).map(|part| {
        let writer = &writer;
        async move {
            let rows = format!("SELECT toDateTime('2026-11-01 00:00:00') + number, ['a','b','c'][number % 3 + 1], {part} * 100 + number FROM numbers(100)");
            writer.execute(&format!("INSERT INTO d.events {rows}")).await
        }
    });
    for inserted in stream::iter(inserts).buffer_unordered(8).collect::<Vec<_>>().await {
        inserted.unwrap();
    }
    writer.stop().await.unwrap();
    println!("built {parts} parts a table in {:?}", built.elapsed());

    for run in 0..3 {
        let started = Instant::now();
        let server = Exclusive::start_with(&stack.stock_env(), &fixture.tenant, &fixture.key, NO_MERGES).await.unwrap();
        let ready = started.elapsed();
        let active = server.execute("SELECT count() FROM system.parts WHERE active AND database = 'd' GROUP BY table ORDER BY table FORMAT TSV").await.unwrap();
        let loaded = started.elapsed();
        let cpu = server.server().pid().and_then(process_cpu).unwrap_or_default();
        let again = Instant::now();
        server.execute("SELECT count(), sum(rows), sum(bytes_on_disk) FROM system.parts WHERE active AND database = 'd' AND table = 'events'").await.unwrap();
        let in_memory = again.elapsed();
        server.stop().await.unwrap();
        println!(
            "stock boot {run}: ready after {ready:?}, parts answered after {loaded:?}, CPU {cpu:?}, active parts by table {:?}, every part's size from memory in {in_memory:?}",
            active.split_whitespace().collect::<Vec<_>>()
        );
    }

    fixture.share().await.unwrap();
    let worker = OpWriter::new(stack.worker_command(), &fixture.tenant, &fixture.key);
    for run in 0..3 {
        let before = worker.usage();
        let started = Instant::now();
        let planned = worker.plan(&events(), &Budget::default()).await.unwrap();
        let wall = started.elapsed();
        let after = worker.usage();
        println!(
            "plan {run}: wall {wall:?}, CPU {:?}, {} InodeDB calls, {} merges proposed",
            after.cpu - before.cpu,
            after.inodedb_calls() - before.inodedb_calls(),
            planned.len()
        );
    }
}
