//! The catalog read from InodeDB agrees with what a stock server reports about the same disk.

use std::collections::BTreeSet;

use bytes::Bytes;
use libch::catalog::{ClaimState, OwnerMode, TableKind};
use libch::stock::{Exclusive, StockWriter, StockWriterOptions};
use libch::writer::{Budget, Insert, TableName};
use libch::{Outcome, PartInfo, Writer};
use libch_testkit::{TenantFixture, stack};

#[tokio::test]
async fn definitions_agree_with_the_writer() {
    let stack = stack().await;
    let fixture = TenantFixture::create(stack).await.unwrap();
    let writer = Exclusive::start(&stack.stock_env(), &fixture.tenant, &fixture.key).await.unwrap();
    for statement in [
        "CREATE DATABASE d",
        "CREATE DATABASE `my-db 1`",
        "CREATE TABLE d.events (ts DateTime, v UInt64) ENGINE = MergeTree ORDER BY ts",
        "CREATE TABLE d.totals (day Date, v UInt64) ENGINE = SummingMergeTree ORDER BY day",
        "CREATE TABLE d.sink (v UInt64) ENGINE = Null",
        "CREATE MATERIALIZED VIEW d.totals_mv TO d.totals AS SELECT toDate(ts) AS day, sum(v) AS v FROM d.events GROUP BY day",
        "CREATE VIEW d.recent AS SELECT * FROM d.events WHERE ts > now() - 3600",
        "CREATE TABLE `my-db 1`.`odd name%` (k UInt64) ENGINE = ReplacingMergeTree ORDER BY k",
    ] {
        writer.execute(statement).await.unwrap();
    }
    let reported = writer
        .execute("SELECT database, name, toString(uuid), engine FROM system.tables WHERE database IN ('d', 'my-db 1') ORDER BY database, name FORMAT TSV")
        .await
        .unwrap();
    let database_uuids =
        writer.execute("SELECT name, toString(uuid) FROM system.databases WHERE name IN ('d', 'my-db 1', 'default') ORDER BY name FORMAT TSV").await.unwrap();
    writer.stop().await.unwrap();

    let catalog = fixture.catalog().await.unwrap();
    let owner = catalog.owner().await.unwrap().expect("a writer has started on the disk");
    assert_eq!(owner.mode, OwnerMode::Exclusive);

    let databases = catalog.databases().await.unwrap();
    let found: String = databases.iter().map(|database| format!("{}\t{}\n", database.name, database.uuid)).collect();
    assert_eq!(found, database_uuids, "the tenant's databases, without the server's own");

    let tables = catalog.all_tables().await.unwrap();
    let found: String = tables
        .iter()
        .map(|table| {
            // The server reports a view's engine; its definition names none.
            let engine = match table.kind {
                TableKind::MaterializedView => "MaterializedView".to_string(),
                TableKind::View => "View".to_string(),
                _ => table.engine.clone().unwrap_or_default(),
            };
            format!("{}\t{}\t{}\t{engine}\n", table.name.database, table.name.table, table.uuid)
        })
        .collect();
    assert_eq!(found, reported);

    let kinds: Vec<(String, TableKind, bool)> = tables.iter().map(|table| (table.name.to_string(), table.kind, table.is_merge_tree())).collect();
    assert_eq!(
        kinds,
        [
            ("d.events".to_string(), TableKind::Table, true),
            ("d.recent".to_string(), TableKind::View, false),
            ("d.sink".to_string(), TableKind::Table, false),
            ("d.totals".to_string(), TableKind::Table, true),
            ("d.totals_mv".to_string(), TableKind::MaterializedView, false),
            ("my-db 1.odd name%".to_string(), TableKind::Table, true),
        ]
    );

    let events = catalog.table(&TableName::new("d", "events")).await.unwrap().unwrap();
    assert!(events.definition.starts_with("ATTACH TABLE _ UUID '") && events.definition.contains("ENGINE = MergeTree"));
    assert!(events.definition_version > 0);
    assert_eq!(catalog.table(&TableName::new("d", "absent")).await.unwrap(), None);
    assert_eq!(catalog.table(&TableName::new("absent", "events")).await.unwrap(), None);
}

/// One generated table: its partition key and the rows that fill it.
struct Generated {
    name: &'static str,
    partition_by: &'static str,
    rows: &'static str,
}

const GENERATED: [Generated; 6] = [
    Generated { name: "unpartitioned", partition_by: "tuple()", rows: "number, toDate('2026-11-01') + number % 3, toString(number % 2), number" },
    Generated { name: "by_integer", partition_by: "k % 3", rows: "number, toDate('2026-11-01'), 'x', number" },
    Generated { name: "by_negative", partition_by: "toInt64(k % 3) - 2", rows: "number, toDate('2026-11-01'), 'x', number" },
    Generated { name: "by_date", partition_by: "d", rows: "number, toDate('2026-11-01') + number % 3, 'x', number" },
    Generated { name: "by_string", partition_by: "s", rows: "number, toDate('2026-11-01'), ['north_east', 'south-west', 'a b'][number % 3 + 1], number" },
    Generated { name: "by_tuple", partition_by: "(k % 2, s)", rows: "number, toDate('2026-11-01'), toString(number % 2), number" },
];

#[tokio::test]
async fn part_names_agree_with_system_parts() {
    let stack = stack().await;
    let fixture = TenantFixture::create(stack).await.unwrap();
    let writer = Exclusive::start(&stack.stock_env(), &fixture.tenant, &fixture.key).await.unwrap();
    writer.execute("CREATE DATABASE g").await.unwrap();
    for table in &GENERATED {
        let name = table.name;
        writer
            .execute(&format!(
                "CREATE TABLE g.{name} (k UInt64, d Date, s String, v UInt64) ENGINE = MergeTree PARTITION BY {} ORDER BY k \
                 SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1",
                table.partition_by
            ))
            .await
            .unwrap();
        // Three inserts, a merge, two more inserts, a mutation, and a lightweight update, so the table holds
        // merged, mutated and patch parts beside the ones they replaced.
        for round in 0..3 {
            writer.execute(&format!("INSERT INTO g.{name} SELECT {} FROM numbers({}, 30)", table.rows, round * 30)).await.unwrap();
        }
        writer.execute(&format!("OPTIMIZE TABLE g.{name} FINAL")).await.unwrap();
        for round in 3..5 {
            writer.execute(&format!("INSERT INTO g.{name} SELECT {} FROM numbers({}, 30)", table.rows, round * 30)).await.unwrap();
        }
        writer.server().query_with(&format!("ALTER TABLE g.{name} UPDATE v = v + 1 WHERE k % 2 = 0"), &[("mutations_sync", "2")]).await.unwrap();
        writer
            .server()
            .query_with(
                &format!("UPDATE g.{name} SET v = 0 WHERE k = 7"),
                &[("enable_lightweight_update", "1"), ("allow_experimental_lightweight_update", "1")],
            )
            .await
            .unwrap();
    }

    let mut kinds_seen = BTreeSet::new();
    let catalog = fixture.catalog().await.unwrap();
    for table in &GENERATED {
        let name = table.name;
        let reported = writer
            .execute(&format!(
                "SELECT name, partition_id, min_block_number, max_block_number, level, active FROM system.parts WHERE database = 'g' AND table = '{name}' ORDER BY name FORMAT TSV"
            ))
            .await
            .unwrap();
        let definition = catalog.table(&TableName::new("g", name)).await.unwrap().unwrap();
        let parts = catalog.parts(&definition).await.unwrap();

        let mut found = String::new();
        for (part_name, entry) in &parts.entries {
            let info = &entry.info;
            assert_eq!(&info.name(), part_name, "a parsed name spells itself again");
            let active = u8::from(parts.set.active.contains(info));
            found.push_str(&format!("{part_name}\t{}\t{}\t{}\t{}\t{active}\n", info.partition_id, info.min_block, info.max_block, info.level));
            if info.is_patch() {
                kinds_seen.insert("patch");
            }
            if info.mutation != 0 {
                kinds_seen.insert("mutated");
            }
            if info.level > 0 {
                kinds_seen.insert("merged");
            }
            if info.partition_id.starts_with('-') {
                kinds_seen.insert("negative");
            }
        }
        // system.parts lists names in its own collation; compare as sets of lines.
        let lines = |text: &str| text.lines().map(str::to_string).collect::<BTreeSet<String>>();
        assert_eq!(lines(&found), lines(&reported), "table g.{name}");
        assert!(parts.set.intersections.is_empty(), "table g.{name}");
        assert!(parts.other.iter().any(|other| other == "format_version.txt"), "table g.{name}: {:?}", parts.other);
        assert!(!parts.mutations.is_empty(), "table g.{name} keeps its mutation entry");
    }
    assert_eq!(kinds_seen.into_iter().collect::<Vec<_>>(), ["merged", "mutated", "negative", "patch"]);
    writer.stop().await.unwrap();
}

#[tokio::test]
async fn coordination_files_follow_a_shared_writer() {
    let stack = stack().await;
    let env = stack.stock_env();
    let fixture = TenantFixture::create(stack).await.unwrap();
    Exclusive::run(
        &env,
        &fixture.tenant,
        &fixture.key,
        &["CREATE DATABASE d", "CREATE TABLE d.t (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS non_replicated_deduplication_window = 100"],
    )
    .await
    .unwrap();
    let catalog = fixture.catalog().await.unwrap();
    let table = TableName::new("d", "t");
    let definition = catalog.table(&table).await.unwrap().unwrap();
    assert_eq!(catalog.counter(&definition).await.unwrap(), None, "an exclusive writer keeps no counter");
    assert!(catalog.coordination(&definition).await.unwrap().tokens.is_empty());

    let writer = StockWriter::start(&env, &fixture.tenant, &fixture.key, &StockWriterOptions::default()).await.unwrap();
    for (token, row) in [("one", "{\"k\":1}\n"), ("two", "{\"k\":2}\n"), ("three", "{\"k\":3}\n")] {
        let outcome = writer.insert(Insert { table: &table, format: "JSONEachRow", token, data: Bytes::from_static(row.as_bytes()) }).await.unwrap();
        assert!(matches!(outcome, Outcome::Committed(_)));
    }

    let owner = catalog.owner().await.unwrap().unwrap();
    assert_eq!(owner.mode, OwnerMode::Shared);
    let coordination = catalog.coordination(&definition).await.unwrap();
    assert_eq!(coordination.counter.as_ref().map(|counter| counter.value), Some(3));
    let mut token_parts: Vec<&str> = coordination.tokens.iter().map(|token| token.part.as_str()).collect();
    token_parts.sort_unstable();
    assert_eq!(token_parts, ["all_1_1_0", "all_2_2_0", "all_3_3_0"]);
    assert!(coordination.tokens.iter().all(|token| token.block_id.starts_with("all_") && token.mtime_ms > 0));
    assert!(coordination.claims.is_empty());

    let parts = catalog.parts(&definition).await.unwrap();
    assert_eq!(parts.counter.as_ref().map(|counter| counter.value), Some(3));
    assert_eq!(parts.max_block(), Some(3));
    assert_eq!(parts.active().iter().map(PartInfo::name).collect::<Vec<_>>(), ["all_1_1_0", "all_2_2_0", "all_3_3_0"]);
    assert!(parts.entries.values().all(|entry| entry.mtime_ms > 0 && entry.entry_version > 0));
    assert_eq!(
        catalog
            .part_files(&definition, "all_1_1_0")
            .await
            .unwrap()
            .iter()
            .filter(|file| ["checksums.txt", "columns.txt", "count.txt"].contains(&file.as_str()))
            .count(),
        3
    );

    let candidates = writer.plan(&table, &Budget::default()).await.unwrap();
    assert_eq!(candidates.len(), 1);
    let merged = writer.merge(&table, &candidates[0]).await.unwrap().committed().expect("the merge commits");
    assert_eq!(merged.part.name(), "all_1_3_1");

    let coordination = catalog.coordination(&definition).await.unwrap();
    let mut claims: Vec<(&str, ClaimState, &str, &str)> =
        coordination.claims.iter().map(|claim| (claim.part.as_str(), claim.state, claim.result.as_str(), claim.writer.as_str())).collect();
    claims.sort_by_key(|claim| claim.0);
    let writer_id = claims[0].3;
    assert!(!writer_id.is_empty());
    assert_eq!(
        claims,
        [
            ("all_1_1_0", ClaimState::Done, "all_1_3_1", writer_id),
            ("all_2_2_0", ClaimState::Done, "all_1_3_1", writer_id),
            ("all_3_3_0", ClaimState::Done, "all_1_3_1", writer_id),
        ]
    );
    assert_eq!(coordination.claim("all_2_2_0").map(|claim| claim.is_live(u64::MAX)), Some(true));
    assert_eq!(coordination.counter.as_ref().map(|counter| counter.value), Some(3), "a merge takes no block number");
    assert_eq!(catalog.coordinated_paths().await.unwrap(), [definition.data_path()]);

    let parts = catalog.parts(&definition).await.unwrap();
    assert_eq!(parts.active().iter().map(PartInfo::name).collect::<Vec<_>>(), ["all_1_3_1"]);
    assert_eq!(parts.set.covered.len(), 3);
    writer.stop().await.unwrap();
}

/// Fills a directory with `count` empty directories named `{prefix}{index:05}`.
async fn fill(client: &libch::inodedb::Client, directory: &str, prefix: &str, count: usize) {
    use libch::inodedb::Op;
    libch_testkit::apply(client, [Op::mkdir(directory).parents().if_absent()]).await.unwrap();
    for chunk in (0..count).collect::<Vec<usize>>().chunks(500) {
        libch_testkit::apply(client, chunk.iter().map(|index| Op::mkdir(format!("{directory}/{prefix}{index:05}")))).await.unwrap();
    }
}

#[tokio::test]
async fn a_listing_longer_than_a_page_is_complete() {
    let stack = stack().await;
    let fixture = TenantFixture::create(stack).await.unwrap();
    let catalog = fixture.catalog().await.unwrap();
    assert!(catalog.list("absent", false).await.unwrap().is_none(), "an absent directory lists as none");

    // InodeDB pages a listing at 1,024 entries.
    fill(catalog.client(), "long", "entry-", 2_600).await;
    let listing = catalog.list("long", false).await.unwrap().expect("the directory");
    let names: Vec<String> = listing.entries.iter().map(|entry| entry.name_lossy()).collect();
    assert_eq!(names.len(), 2_600);
    assert!(names.windows(2).all(|pair| pair[0] < pair[1]), "names come in order, each once");
    assert_eq!((names[0].as_str(), names[2_599].as_str()), ("entry-00000", "entry-02599"));
}

#[tokio::test]
async fn a_listing_reflects_one_moment_while_the_directory_changes() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use libch::inodedb::Op;

    let stack = stack().await;
    let fixture = TenantFixture::create(stack).await.unwrap();
    let catalog = fixture.catalog().await.unwrap();
    // Enough entries between `a-` and `z-` names that the two fall on different pages of a listing.
    fill(catalog.client(), "pairs", "m-", 2_600).await;

    // One writer adds a pair of names in each commit: `a-{n}` on the listing's first page and `z-{n}` on its
    // last. A listing that mixed two moments would show a `z-` name without its `a-` name.
    let done = Arc::new(AtomicBool::new(false));
    let writer = {
        let (client, done) = (fixture.client().await.unwrap(), done.clone());
        tokio::spawn(async move {
            let mut pair = 0u32;
            while !done.load(Ordering::Relaxed) {
                libch_testkit::apply(&client, [Op::mkdir(format!("pairs/a-{pair:06}")), Op::mkdir(format!("pairs/z-{pair:06}"))]).await.unwrap();
                pair += 1;
            }
            pair
        })
    };

    let mut most_pairs = 0;
    let mut listings = 0;
    let until = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while std::time::Instant::now() < until {
        listings += 1;
        let listing = catalog.list("pairs", false).await.unwrap().expect("the directory");
        let names: std::collections::BTreeSet<String> = listing.entries.iter().map(|entry| entry.name_lossy()).collect();
        let firsts: Vec<&str> = names.iter().filter_map(|name| name.strip_prefix("a-")).collect();
        let lasts: Vec<&str> = names.iter().filter_map(|name| name.strip_prefix("z-")).collect();
        assert_eq!(firsts, lasts, "every pair is whole in a listing");
        assert_eq!(names.len(), 2_600 + 2 * firsts.len());
        most_pairs = most_pairs.max(firsts.len());
    }
    done.store(true, Ordering::Relaxed);
    let written = writer.await.unwrap();
    assert!(
        written > 20 && listings > 20 && most_pairs > 0,
        "the directory changed while it was listed: {written} pairs written, {listings} listings, {most_pairs} pairs seen"
    );
}
