//! Stock servers: their processes, their answers, and the reference writer's corners.

use std::path::Path;
use std::time::{Duration, Instant};

use bytes::Bytes;
use libch::stock::{Blank, Exclusive, Server, ServerConfig, StockWriter, StockWriterOptions};
use libch::writer::{Budget, Insert, MergeCandidate, TableName};
use libch::{Error, Outcome, PartInfo, Writer};
use libch_testkit::conformance::tenant_with_events;
use libch_testkit::{TenantFixture, schema, stack};

fn process_runs(pid: u32) -> bool {
    Path::new(&format!("/proc/{pid}")).exists()
}

#[tokio::test]
async fn a_server_boots_answers_and_stops() {
    let stack = stack().await;
    let mut server = Blank::boot(&stack.stock_env()).await.unwrap();
    let pid = server.pid().expect("a running server has a pid");
    assert!(process_runs(pid));
    assert!(server.dir().join("config.xml").is_file());
    assert_ne!(server.http_port(), server.tcp_port());
    assert_eq!(server.query("SELECT count() FROM numbers(10)").await.unwrap().trim(), "10");
    assert_eq!(server.query_with("SELECT getSetting('max_threads')", &[("max_threads", "3")]).await.unwrap().trim(), "3");

    let started = Instant::now();
    server.stop().await.unwrap();
    assert!(started.elapsed() < Duration::from_secs(10), "a server stops promptly: {:?}", started.elapsed());
    assert!(!process_runs(pid));
    assert_eq!(server.pid(), None);
    server.stop().await.unwrap();

    let dir = server.dir().to_path_buf();
    drop(server);
    assert!(!dir.exists(), "a dropped server's directory is removed");
}

#[tokio::test]
async fn dropping_a_server_kills_it() {
    let stack = stack().await;
    let server = Blank::boot(&stack.stock_env()).await.unwrap();
    let (pid, dir) = (server.pid().unwrap(), server.dir().to_path_buf());
    drop(server);
    assert!(!process_runs(pid), "a dropped server's process is gone");
    assert!(!dir.exists());
}

#[tokio::test]
async fn a_killed_server_is_gone_at_once() {
    let stack = stack().await;
    let mut server = Blank::boot(&stack.stock_env()).await.unwrap();
    let pid = server.pid().unwrap();
    server.kill();
    assert!(!process_runs(pid));
    assert!(server.query("SELECT 1").await.is_err());
}

#[tokio::test]
async fn an_exception_carries_its_code_and_message() {
    let stack = stack().await;
    let server = Blank::boot(&stack.stock_env()).await.unwrap();
    let error = server.query("SELECT * FROM absent_database.t").await.unwrap_err();
    assert_eq!(error.clickhouse_code(), Some(81), "UNKNOWN_DATABASE: {error}");
    assert!(error.to_string().contains("absent_database"), "{error}");
    let syntax = server.query("SELEC 1").await.unwrap_err();
    assert_eq!(syntax.clickhouse_code(), Some(62), "SYNTAX_ERROR: {syntax}");
}

#[tokio::test]
async fn a_body_and_a_summary_travel_with_a_statement() {
    let stack = stack().await;
    let server = Blank::boot(&stack.stock_env()).await.unwrap();
    server.query("CREATE TABLE default.m (k UInt64) ENGINE = Memory").await.unwrap();
    let answer = server
        .run("INSERT INTO default.m FORMAT JSONEachRow", &[("async_insert", "0")], Bytes::from_static(b"{\"k\":1}\n{\"k\":2}\n{\"k\":3}\n"))
        .await
        .unwrap();
    assert_eq!(answer.summary.written_rows, 3);
    assert_eq!(server.query("SELECT sum(k) FROM default.m").await.unwrap().trim(), "6");
}

#[tokio::test]
async fn added_config_reaches_a_running_server() {
    let stack = stack().await;
    let server = Blank::boot(&stack.stock_env()).await.unwrap();
    let setting = "SELECT value FROM system.server_settings WHERE name = 'max_table_size_to_drop'";
    assert_ne!(server.query(setting).await.unwrap().trim(), "12345");
    server.add_config("limits.xml", "<clickhouse><max_table_size_to_drop>12345</max_table_size_to_drop></clickhouse>").await.unwrap();
    assert_eq!(server.query(setting).await.unwrap().trim(), "12345");
}

#[tokio::test]
async fn a_server_that_cannot_boot_says_why() {
    let stack = stack().await;
    let config = ServerConfig {
        name: "broken".into(),
        extra_xml: "<merge_tree><no_such_merge_tree_setting>1</no_such_merge_tree_setting></merge_tree>".into(),
        ..ServerConfig::default()
    };
    let error = Server::start(&stack.stock_env(), &config).await.unwrap_err();
    assert!(matches!(&error, Error::Server { name, message } if name == "broken" && message.contains("while booting")), "{error}");
    assert!(error.to_string().contains("no_such_merge_tree_setting"), "the error carries the server's log: {error}");
}

#[tokio::test]
async fn an_exclusive_run_stops_at_the_first_failure() {
    let stack = stack().await;
    let fixture = TenantFixture::create(stack).await.unwrap();
    let error =
        Exclusive::run(&stack.stock_env(), &fixture.tenant, &fixture.key, &["CREATE DATABASE first", "CREATE TABLE first.bad (", "CREATE DATABASE second"])
            .await
            .unwrap_err();
    assert_eq!(error.clickhouse_code(), Some(62), "the failing statement's error comes back: {error}");
    let databases: Vec<String> = fixture.catalog().await.unwrap().databases().await.unwrap().into_iter().map(|database| database.name).collect();
    assert_eq!(databases, ["default", "first"], "statements after the failure did not run");
}

#[tokio::test]
async fn a_shared_writer_refuses_what_needs_an_exclusive_one() {
    let stack = stack().await;
    let fixture = tenant_with_events(stack).await;
    let writer = StockWriter::start(&stack.stock_env(), &fixture.tenant, &fixture.key, &StockWriterOptions::default()).await.unwrap();
    for statement in [
        "ALTER TABLE d.events ADD COLUMN extra UInt8",
        "ALTER TABLE d.events UPDATE v = 0 WHERE 1",
        "CREATE TABLE d.more (k UInt64) ENGINE = MergeTree ORDER BY k",
        "DROP TABLE d.events",
        "TRUNCATE TABLE d.events",
    ] {
        let error = writer.server().query(statement).await.unwrap_err();
        assert_eq!(error.clickhouse_code(), Some(344), "`{statement}`: {error}");
        assert!(error.to_string().contains("exclusive writer"), "`{statement}`: {error}");
    }
    writer.stop().await.unwrap();
}

#[tokio::test]
async fn the_reference_declines_what_it_cannot_do() {
    let stack = stack().await;
    let fixture = tenant_with_events(stack).await;
    let writer = StockWriter::start(&stack.stock_env(), &fixture.tenant, &fixture.key, &StockWriterOptions::default()).await.unwrap();
    let events = schema::events();

    // A table the tenant lacks.
    let absent = TableName::new("d", "absent");
    let batch = schema::batch(0, 10);
    let error = writer.insert(Insert { table: &absent, format: "JSONEachRow", token: &batch.token, data: batch.data.clone() }).await.unwrap_err();
    assert_eq!(error.clickhouse_code(), Some(60), "UNKNOWN_TABLE: {error}");
    assert!(writer.plan(&absent, &Budget::default()).await.is_err());

    // Rows that do not parse commit nothing.
    let garbage =
        writer.insert(Insert { table: &events, format: "JSONEachRow", token: "garbage", data: Bytes::from_static(b"{\"ts\": \"not a time\"}\n") }).await;
    assert!(garbage.is_err(), "{garbage:?}");

    // One source is no merge, and neither is a partition that has moved on.
    let one = MergeCandidate { partition_id: "202611".into(), sources: vec![PartInfo::parse("202611_1_1_0").unwrap()] };
    assert!(matches!(writer.merge(&events, &one).await.unwrap(), Outcome::Refused(_)));
    for index in 0..3 {
        let batch = schema::batch(index, 10);
        assert!(writer.insert(Insert { table: &events, format: "JSONEachRow", token: &batch.token, data: batch.data.clone() }).await.unwrap().is_committed());
    }
    let stale = MergeCandidate { partition_id: "202611".into(), sources: ["202611_1_1_0", "202611_2_2_0"].map(|name| PartInfo::parse(name).unwrap()).to_vec() };
    assert!(matches!(writer.merge(&events, &stale).await.unwrap(), Outcome::Refused(reason) if reason.contains("whole partition")));

    let report = writer.housekeep(&events).await.unwrap().committed().unwrap();
    assert!(report.removed.is_empty() && report.remaining.is_empty(), "nothing is covered yet: {report:?}");
    assert_eq!(active_parts(&fixture, &events).await, 3);
    writer.stop().await.unwrap();
}

async fn active_parts(fixture: &TenantFixture, table: &TableName) -> usize {
    let catalog = fixture.catalog().await.unwrap();
    let definition = catalog.table(table).await.unwrap().unwrap();
    catalog.parts(&definition).await.unwrap().active().len()
}

#[tokio::test]
async fn background_merges_run_only_when_asked_for() {
    let stack = stack().await;
    let events = schema::events();

    let quiet = tenant_with_events(stack).await;
    let writer = StockWriter::start(&stack.stock_env(), &quiet.tenant, &quiet.key, &StockWriterOptions::default()).await.unwrap();
    for index in 0..12 {
        let batch = schema::batch(index, 20);
        writer.insert(Insert { table: &events, format: "JSONEachRow", token: &batch.token, data: batch.data.clone() }).await.unwrap();
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(active_parts(&quiet, &events).await, 12, "a reference writer merges only on request");
    writer.stop().await.unwrap();

    let merging = tenant_with_events(stack).await;
    let options = StockWriterOptions { background_merges: true, ..StockWriterOptions::default() };
    let writer = StockWriter::start(&stack.stock_env(), &merging.tenant, &merging.key, &options).await.unwrap();
    for index in 0..12 {
        let batch = schema::batch(index, 20);
        writer.insert(Insert { table: &events, format: "JSONEachRow", token: &batch.token, data: batch.data.clone() }).await.unwrap();
    }
    let deadline = Instant::now() + Duration::from_secs(20);
    while active_parts(&merging, &events).await >= 12 {
        assert!(Instant::now() < deadline, "a writer with background merges on merges its parts");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    writer.stop().await.unwrap();
}
