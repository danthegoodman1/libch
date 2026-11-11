# libch Development Plan

## Overarching Goal

libch runs ClickHouse's insert and merge code as explicit, short-lived operations on a tenant's `inodedb` disk, so one pool of workers serves a very large number of tenants. A Rust crate drives a worker program built from the BigHouse ClickHouse fork. Each operation starts from the tenant's key and the state in InodeDB, does one thing (insert this batch, merge these parts, remove these covered parts), commits through BigHouse's shared-writer protocol, and exits. A disk that libch writes stays a stock BigHouse disk: stock read-only servers attach it and answer as parallel replicas, and a stock writer can take it over, at any time.

The plan delivers the library and a Rust demo of the recipe: thousands of tenants ingested and merged by operations alone, each queried by read-only servers the demo boots and clusters the way BigHouse's fleets work.

BigHouse Phase 7 (shared writers and merge leases) is complete before this plan's work starts. libch builds on its writer modes, `coord/` layout, block counter, deduplication tokens, schema condition, part-set refresh and merge claims.

Outside this plan: the product's control plane (events API, durable buffer, tenant directory); a query service, and compatibility with BigHouse's query API and demo; and DDL, mutations, partition commands and lightweight updates and deletes as operations, which run in a stock exclusive writer.

## Layout

| Path | Contents |
| --- | --- |
| `crates/libch/` | The crate: `catalog`, `part`, `disk`, the `Writer` trait, `stock` servers, `check`, a small S3 client, the `worker` client, the worker `pool` and the `keys` cache. |
| `crates/libch-testkit/` | What tests, benchmarks and the demo share: the dev stack, the preflight, the tenant fixture, the conformance scenarios, a reader, process measurements, and a stand-in worker that speaks the protocol and touches no disk. |
| `crates/libch/tests/` | Integration tests against the dev stack. |
| `crates/libch/benches/` | Benchmarks that write `BENCHMARKS.md`. |
| `proto/libch.proto`, `crates/libch-proto/` | The worker's request and response messages and the frames that carry them, shared with the fork. |
| `demo/` | From Phase 6: `libch-demo`, with tenant setup, the load generator, the ingest and merge loops, and the fleet query step. |
| `scripts/dev-up`, `scripts/dev-down` | libch's own dev stack: InodeDB on port 7170 and s3proxy on port 11211, beside BigHouse's on other ports. |
| `scripts/fork-setup`, `scripts/fork-build`, `fork/` | The worker's branch of the fork: `fork/ClickHouse` is a git worktree of the BigHouse checkout's fork on the branch `libch-worker`, and `fork/build` its build. Tests run the binary there, as stock server and as worker. |
| `bighouse.rev` | The BigHouse commit this plan's code runs against. A checkout at that commit or a later one passes the preflight. |
| `fork.rev` | The commit of the fork's `libch-worker` branch this plan's code runs against. A worktree at that commit or a later one passes the preflight. |
| `$BIGHOUSE_DIR` (default `../BigHouse`) | The BigHouse checkout: the fork, its build, and the built InodeDB server. |
| `fork/ClickHouse/programs/libch-worker/` | The worker: its entry point, the engine's start and the tenant's attach, the frames, and each operation. `libch.proto` there is a copy of this repository's, which the preflight compares. |

The InodeDB client is a git dependency at the commit BigHouse pins its `inodedb` submodule to, and the preflight compares the two.

## Implementation Principles

- **Reuse ClickHouse's code paths whole.** An operation calls the sink, merge task and selector a server uses. libch adds entry points and removes lifecycle: startup, part loading, background threads.
- **State lives in InodeDB.** An operation needs its request and the tenant's key. A worker keeps nothing between operations.
- **One tenant per process.** A worker holds one tenant's InodeDB key and S3 credentials, runs that tenant's SQL (view SELECTs, defaults, TTL expressions), and exits. The engine runs in a worker process, and the crate talks to it over a pipe: ClickHouse allows one engine per process, and the process boundary keeps tenant SQL away from other tenants' credentials.
- **Every operation is safe to retry.** A token makes an insert exact, claims and commit conditions make a merge exact, and containment in a complete listing makes a removal safe.
- **The caller schedules.** An operation runs no background thread and no timer, and it ends when its work commits.
- **A libch-written disk is a stock disk.** The differential suite against stock writers decides compatibility, and a difference is a libch bug.
- **The stock writer stays in the system.** DDL and mutations go through a stock exclusive writer, and the stock shared writer is the reference in every test.
- **Fork changes follow BigHouse's rules.** Each change outside `programs/libch-worker/` gets a row in BigHouse's files-changed table and a `BigHouse:` notice, the new directory joins `LICENSE-BIGHOUSE`, and BigHouse's own suites pass on the fork commit that carries the worker.
- **Gates name numbers measured on a quiet machine.** The benchmark harness records CPU as the children's `rusage`, memory as the worker's own anonymous memory, and refuses to record while the load average is above 2.

## Testing Strategy

- **Conformance.** `libch-testkit`'s `conformance` module holds one set of scenarios over the `Writer` trait. A test file supplies a harness and runs them: against the stock reference writer now, and against the worker from Phase 3.
- **Differential.** The same seeded workload goes through a stock writer and through operations, into two tenants. A fixed query set through a stock read-only server must give equal answers, across the engine and feature matrix in Phase 3.
- **Invariant checker.** `libch::check` reads a tenant from InodeDB alone: active parts of a partition never intersect, block numbers are unique and at or below the counter, every claim and token belongs to a table, and every blob a record names exists in S3. A fresh stock exclusive writer then loads the disk and `CHECK TABLE` passes.
- **Hazards.** Each hazard has a scenario that passes, and that fails once more with its protection switched off through a failpoint, as BigHouse's `tests/shared.py` does. A worker enables the failpoints its settings name under `fail_points_active`, as a server does.
- **Crash.** Workers die at named failpoints and under random `SIGKILL`. A worker stays at a pausing failpoint until its deadline kills it. After each kill the checker passes and a retry with the same token leaves each row once.
- **Benchmarks.** `benches/` records CPU, wall time, own memory, threads and InodeDB calls per operation in `BENCHMARKS.md`, beside the stock numbers from the same machine and build.
- **Unit.** Rust tests cover part names, the value format and the protocol. Operation mode is tested from the crate: each response carries the worker's own counts of InodeDB and S3 calls, its CPU, memory and threads, so a test compares what two tenants cost a worker.
- **Query.** A fleet of stock read-only servers, assigned to a tenant and clustered as parallel replicas the way BigHouse's fleets are, answers on libch-written tenants and matches expected totals.

`BENCHMARKS.md` holds the stock baselines that later gates compare with. Three of its findings shape the gates:

- **A stock process's cost grows with the tenant's parts.** Booting a server, giving it the tenant's disk, inserting 1,000 rows and stopping costs 219 ms of CPU with 10 parts in each table, 420 ms with 100, and 1,177 ms with 500: about 2 ms a part. One `clickhouse local` process for the same insert costs 138, 306 and 901 ms.
- **The insert itself costs about 6 ms of CPU** in a running writer, through a projection and a materialized view, and 2 ms into a plain table on one thread.
- **The engine alone is small, and its start is the largest fixed cost.** `clickhouse local` idle inside a query holds 35 MB of its own memory in 27 threads, where a server holds 147 MB in about 200. A process that only answers `SELECT 1` costs 49 ms of CPU; attaching an empty tenant's disk adds 14 ms, four tables without parts add 21 ms, and 10 parts a table add 44 ms.

An operation loads no parts, so its cost stays flat where the stock figures climb. The running-writer figures use `async_insert = 0`; at this build's default, `async_insert = 1`, an insert with a request body waits at least 50 ms in the asynchronous queue.

## Fork Changes

The worker's branch, `libch-worker`, starts at the fork commit BigHouse pins and adds `programs/libch-worker/`. It changes these files of the fork besides, each at a comment that starts `BigHouse:`. BigHouse's plan lists its own changes in the same form, and these rows join that table when the branch merges.

| File | Change | Reason |
| --- | --- | --- |
| `programs/CMakeLists.txt`, `programs/main.cpp` | Add the `libch-worker` program. | Registration. |
| `src/Storages/StorageMergeTree.{h,cpp}` | Operation mode: `setOperationMode`, `attachInOperationMode` in place of the constructor's loading, a `startup` that starts nothing, `read` and the totals refusing a table without its parts; `prepareOperationCommits`, which reads the definition's version and validates the block counter before a table's first commit; a record of the parts committed. `mergeNamedParts` loads, claims, merges and commits parts by name. `planMerges` runs the selector without claiming, and under it `sharedWriterCannotMerge` leaves out the parts a mutation is pending for. `startRenewingClaims` is `startup`'s claim task, shared with `mergeNamedParts`. | A worker attaches tables without reading their directories, reports what it wrote, and merges and plans on the caller's schedule. |
| `src/Storages/MergeTree/MergeTreeSink.cpp` | In operation mode `commitPartOnInodeDB` prepares the table's commits, creates the token directory in the part's batch, and records the committed part. | Inserts from a table that loaded nothing. |
| `src/Storages/MergeTree/MergeTreeData.{h,cpp}` | `loadNamedParts` loads parts by name and adds them as active, as a refresh adds the parts it lists. | A merge loads its sources alone. |
| `src/Disks/DiskObjectStorage/MetadataStorages/InodeDB/InodeDBCoordination.h`, `MetadataStorageFromInodeDB.h` | `ownerVersion`. | A writer tells a counter of its own epoch from an older one. |
| `src/Disks/DiskObjectStorage/MetadataStorages/InodeDB/InodeDBTableCoordinator.{h,cpp}` | `readCounterOfThisEpoch`, `addDirectories`, a public `ensureDirectories`, `partsHeldByOthers`. | A counter of this epoch, the token directory in a part's batch, the claims directory before a merge's claims, and the claims a plan reads where a server learns of each from a failed claim. |
| `src/Disks/DiskObjectStorage/MetadataStorages/InodeDB/MetadataStorageFromInodeDB.{h,cpp}` | A shared writer sends no batch for a transaction that only creates directories that exist or removes a path that is absent. | Attaching a tenant costs a shared writer reads alone: 12 commits fewer for two databases. |
| `src/Storages/MergeTree/DataPartStorageOnDiskBase.cpp` | `isCaseInsensitive` answers false on `inodedb` metadata without the probe file. | Two commits fewer in a process's first insert. |
| `src/Common/FailPoint.cpp` | `libch_merge_unchecked_sources`, `libch_merge_pause_after_claims`, `libch_housekeep_pause_between_batches`, `libch_pause_before_response`. | The worker's hazard and crash tests. |
| `LICENSE-BIGHOUSE` | Names `programs/libch-worker/` and the branch's commits. | The worker is covered as BigHouse's own files are. |

## Phase 0: Foundations

Goal:
A workspace that builds, finds a BigHouse checkout with Phase 7, runs against a dev stack of its own, and records the stock baselines.

Scope:
- 0A: A Cargo workspace with `crates/libch` and `crates/libch-testkit`, taking `inodedb-client` from InodeDB's repository at the commit BigHouse pins.
- 0B: The BigHouse pin and a preflight. `bighouse.rev` names the commit. The preflight checks that the checkout includes it, that the checkout's `inodedb` submodule is at the commit the client is built from, that the fork's binary runs, that InodeDB and S3 answer, and that a server with `inodedb_writer_mode` set to `shared` writes the `coord/` layout BigHouse documents.
- 0C: Stock baselines. `benches/stock.rs` measures a stock process's whole life for one insert at three tenant sizes, a running writer's inserts, and the idle engine, and refuses to record on a busy machine.
- 0D: The test kit and the dev stack: `scripts/dev-up` starts an InodeDB store and an S3 service for libch alone; a tenant fixture creates an InodeDB directory, mints keys and takes an S3 prefix, and removes all three on drop; `libch::stock::Server` boots, assigns and stops stock servers; and measurement helpers read a process's CPU, own memory and threads.

Completion gate:
From a clean checkout, `cargo test` passes against a running dev stack, and `BENCHMARKS.md` holds the stock baselines from the pinned build, recorded on a quiet machine.

Testing plan:
- The preflight fails with a clear message for a missing checkout, a checkout without the pinned commit, a missing binary, a binary that does not run, a stopped InodeDB, a stopped S3 service and a missing admin key.
- The tenant fixture leaves no InodeDB directory, key or S3 object behind, checked after a passing and a panicking test.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 0A: Cargo workspace | `Cargo.toml` with `crates/libch` and `crates/libch-testkit`; `inodedb` is a git dependency at `be669f1`, and `cargo build --workspace --all-targets` and `cargo clippy --workspace --all-targets` pass clean. |
| Complete | Work | 0B: BigHouse pin and preflight | `bighouse.rev` pins `3e11a9c`. `libch-testkit/src/preflight.rs`; `tests/preflight.rs` passes ten cases: the environment is ready, `shared_writers_leave_the_layout_bighouse_documents`, and one case for each failure in the testing plan. |
| Complete | Work | 0C: Stock baselines | `crates/libch/benches/stock.rs` wrote `BENCHMARKS.md` at a load average of 1.6, on BigHouse `f218bef`; with the load above 2 it exits with "CPU figures taken now would mislead". Another session's disk activity ran during that recording, so its wall times stand above an idle machine's while its CPU times agree with an earlier run within 10%. |
| Complete | Work | 0D: Test kit and dev stack | `scripts/dev-up`, `scripts/dev-down`; `libch-testkit` (`Stack`, `TenantFixture`, `Reader`, `measure`); `libch::stock::Server` with `tests/stock.rs` (boot, stop, kill, drop, exceptions, a body and its summary, added config, a server that cannot boot). Servers lease their ports through file locks (`stock/ports.rs`) and die with the test process. |
| Complete | Decision | libch runs a dev stack of its own | BigHouse's session wipes and kills its stack in its own tests, and both sessions share one machine. `scripts/dev-up` uses ports and a container name of its own, so the two stacks run side by side. |
| Complete | Gate | Clean checkout passes and baselines are recorded | A fresh clone of the repository passes `cargo test` against the running stack; `BENCHMARKS.md`. |
| Complete | Test | Fixture cleanup after pass and panic | `tests/fixture.rs`: `a_passing_test_leaves_nothing_behind`, `a_panicking_test_leaves_nothing_behind`, `removing_a_tenant_reports_success_and_leaves_nothing_behind`. |

## Phase 1: Catalog and Reference Writer

Goal:
Rust reads a tenant's catalog and part state from InodeDB, and a stock writer behind the `Writer` trait is the reference for every later phase. This phase changes nothing in the fork.

Scope:
- 1A: `libch::catalog` definitions: a tenant's databases and tables with their UUIDs and definition text, read from `tree/metadata` and `tree/store` through the disk's value format (`libch::disk`).
- 1B: `libch::catalog` parts: part names parsed as `MergeTreePartInfo` parses them (`libch::part`), containment, and the active and covered sets of each partition; the writer epoch from `owner`; the counter, claims with their state, holder and expiry, and tokens from `coord/`. A listing longer than one page is read at one version.
- 1C: The `Writer` trait: `insert`, `plan`, `merge` and `housekeep`. Each returns an `Outcome` that tells apart committed, duplicate, schema changed, fenced, claim lost and refused; a failure is the call's `Err`.
- 1D: `StockWriter`, the reference: a stock shared server booted with the tenant's config, which inserts with a token and `async_insert = 0`, and merges a whole partition with `OPTIMIZE`. `Exclusive`: one stock server with the tenant's config, for DDL and mutations.
- 1E: `Fleet`: blank stock servers assigned to a tenant under a read key with one config reload, attached with `ATTACH DATABASE ... SETTINGS disk`, and clustered as parallel replicas. Tests and the demo query through it.
- 1F: `libch::check`, the invariant checker: `check::tenant` from InodeDB and S3, and `check::stock_load` through a fresh stock exclusive writer.
- 1G: Conformance scenarios for the reference: inserts through a view and a projection, a retry with its token, a chain of two views, concurrent inserts into one table, merging to one part, an exclusive epoch for `ALTER`, and a revoked key.

Out of scope:
- Parsing SQL in Rust. The catalog reads a definition's kind, UUID and engine, and the view dependency graph comes from the worker's `describe` in Phase 2.

Completion gate:
The conformance scenarios pass on `StockWriter`, and the checker reports each planted corruption: intersecting parts, a repeated block number, a missing part file, a missing blob, and a claim with no table.

Testing plan:
- The part-name parser agrees with `system.parts` of a stock server over generated tables: integer, date, string and tuple partition keys, negative values, merged parts, mutated parts and patch parts.
- Each planted corruption is written into a tenant through InodeDB or S3, and the checker names it.
- A fleet of four servers answers a query on a stock-written tenant, each server reads a share of the marks, and stopping the fleet releases its InodeDB leases.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 1A: Catalog definitions | `libch/src/disk.rs` (values and records, 9 unit tests) and `catalog/mod.rs`. `tests/catalog.rs::definitions_agree_with_the_writer` compares databases, tables, UUIDs and engines with a stock writer's `system.databases` and `system.tables`, names that need escaping among them. |
| Complete | Work | 1B: Catalog parts and coordination files | `libch/src/part.rs` (10 unit tests, one against the slow definition of an active set on 2,000 random part sets) and `catalog/coord.rs`. `tests/catalog.rs`: `part_names_agree_with_system_parts`, `coordination_files_follow_a_shared_writer`, `a_listing_longer_than_a_page_is_complete`, `a_listing_reflects_one_moment_while_the_directory_changes`. |
| Complete | Work | 1C: `Writer` trait and outcomes | `libch/src/writer.rs`. |
| Complete | Work | 1D: `StockWriter` and `Exclusive` | `stock/writer.rs`, `stock/exclusive.rs`; `tests/stock.rs` covers what the reference declines, what a shared writer refuses, background merges on and off, and an exclusive run that stops at its first failure. |
| Complete | Work | 1E: `Fleet` | `stock/fleet.rs`; `tests/fleet.rs`. |
| Complete | Work | 1F: `libch::check` | `libch/src/check.rs`; `tests/check.rs` (11 tests). |
| Complete | Work | 1G: Conformance scenarios | `libch-testkit/src/conformance.rs`; `tests/conformance_stock.rs` runs the seven scenarios on `StockWriter`. Each ends with `check::tenant` clean and `check::stock_load` empty. |
| Complete | Decision | The reference boots with the tenant's config | A shared writer's commit requires the version of the table's definition file on the disk, and a table attached into a server's own database has none there. Booting with `database_disk` is also how BigHouse's tests start shared writers. |
| Complete | Decision | The reference tells a duplicate from the server's counters | ClickHouse reports a deduplicated insert as rows written, in its answer and in `X-ClickHouse-Summary`. `StockWriter` serializes its inserts and reads `MergeTreeDataWriterBlocks` and `DuplicatedInsertedBlocks` from `system.events` around each: an insert whose every block was a duplicate ends `Duplicate`. `a_retried_batch_lands_once` checks it on the same writer and on a later one. |
| Complete | Decision | The reference merges whole partitions, and its `housekeep` only reports | `OPTIMIZE ... PARTITION ID ... FINAL` is the one statement that merges a set of parts the caller can name, so `plan` proposes each partition's active parts and `merge` refuses any other set. A stock shared writer removes the sources of its own merges on its own schedule. |
| Complete | Decision | The fleet tests keep their table inline in InodeDB | The dev stack's S3 service drops connections under the burst that four servers scanning at once make, and ClickHouse's S3 retries then stall a query for 10 to 40 seconds. The table's columns compress to files under 16 KiB, and `tests/fleet.rs` asserts that S3 holds none of them. |
| Complete | Risk | The Rust part-name parser diverges from ClickHouse's | `part_names_agree_with_system_parts` compares name, partition, blocks, level and active flag for every part of six generated tables, with merged, mutated, negative-partition and patch parts all present. Phase 2 adds the comparison with the worker's own parse. |
| Complete | Risk | A listing mixes two moments | `Catalog::list_with` reads a listing again when its directory's version passed its first page's. With that check removed, `a_listing_reflects_one_moment_while_the_directory_changes` fails in each of three runs. |
| Complete | Gate | Conformance passes on the reference | `cargo test --test conformance_stock`: 7 passed. |
| Complete | Gate | The checker reports each planted corruption | `tests/check.rs`: `intersecting_parts_are_named`, `a_repeated_block_number_is_named`, `a_missing_part_file_is_named_and_a_stock_writer_rejects_the_part`, `a_missing_blob_is_named`, `a_claim_with_no_table_is_named`; also a blob of another size, a counter behind its parts, a file in no disk format and a spilled record. `the_checker_stays_clean_beside_live_writers` runs it against a writer that inserts, merges and removes parts. |
| Complete | Test | A four-server fleet answers, shares the marks, and releases its leases | `tests/fleet.rs`: `four_servers_answer_as_parallel_replicas`, `stopping_a_fleet_releases_its_leases`, `a_fleet_reads_under_a_key_that_cannot_write`. |

## Phase 2: Worker and Operation Mode

Goal:
The fork builds a worker that attaches a tenant's catalog in operation mode, definitions only, at a cost independent of how many parts the tenant holds.

Scope:
- 2A: The program `libch-worker` in the fork's `programs/`, linked on `dbms`, with a minimal global context. It registers the MergeTree family, `Null`, views, formats, functions and aggregate functions, and leaves table functions, dictionaries and external engines unregistered.
- 2B: The protocol in `proto/libch.proto`, with its Rust types in `crates/libch-proto`: one request on stdin with the tenant's disk (InodeDB endpoint and key, S3 endpoint, prefix and credentials), a settings document and the operation; framed data after it; one response on stdout; logs on stderr. `libch::worker::OpWriter` is the crate's side: it starts one worker for each call and kills one that passes its deadline.
- 2C: Operation mode for `StorageMergeTree`: a table built from its definition loads no parts, starts no background assignee or cleanup thread, and runs no startup cleanup. The switch is off by default.
- 2D: Catalog load: the worker joins the shared owner, takes a writer id and a reader lease, and attaches the tenant's databases from the disk with every table in operation mode.
- 2E: The `describe` operation: tables with engines and UUIDs, the view dependency graph, and each part name as the engine parses it.
- 2F: A startup profile in `BENCHMARKS.md`: where one `clickhouse local` process spends its time, step by step, and the worker's own budget by the same steps (the engine, the disk, the catalog, the operation).

Out of scope:
- Operations that write.

Completion gate:
On the three-table tenant, the worker answers `describe` within 80 ms of CPU from exec, with at most 60 MB of its own memory and 40 threads. A tenant whose table holds 10,000 parts costs within 10% of that, and a 300-table tenant at most 150 ms. `url`, `file`, `s3`, `remote`, `executable`, dictionaries and the `URL`, `File`, `S3` and `Kafka` engines are unknown to the worker. BigHouse's earlier suites pass on the fork commit.

Testing plan:
- gtests: a table built in operation mode makes no InodeDB list call under its directory, and starts no thread.
- `describe` agrees with a stock server's `system.tables` and with 1B's part parser on the Phase 1 corpus.
- A request for each unregistered source fails with ClickHouse's unknown-function or unknown-engine error.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 2A: `libch-worker` program | Fork commit `0b6ad7bd4`: `programs/libch-worker/` (`LibchWorker.cpp`, `Engine.cpp`, `Frames.cpp`, `Describe.cpp`, `Insert.cpp`, `Usage.cpp`), registered in `programs/CMakeLists.txt` and `programs/main.cpp`. `Engine.cpp` registers interpreters, parsers, functions, aggregate functions, the `Atomic` database, the `MergeTree` family, `Null`, `View`, `MaterializedView`, disks and formats, and nothing else. A worker started with no request finishes its startup in 20 ms of CPU and exits cleanly, which the preflight checks. |
| Complete | Work | 2B: Protocol | `proto/libch.proto`; `crates/libch-proto` (frames, 4 unit tests); `libch/src/worker.rs` (`OpWriter`); `libch-testkit/tests/worker.rs` runs 13 cases against the stand-in worker. The fork compiles a copy of the protocol (`programs/libch-worker/CMakeLists.txt`) and reads frames in `Frames.cpp`, with an insert's batch as a stream; `preflight::fork` fails when the copy differs. Every response carries a `Usage`, which `OpWriter::usage` adds up. |
| In Progress | Work | 2C: Operation mode | `StorageMergeTree::setOperationMode`, which the worker alone calls: the constructor takes `attachInOperationMode` in place of the directory, part, mutation and token loading, `startup` starts nothing, and `read` throws. `worker_describe::attaching_a_tenant_costs_the_same_whatever_its_parts` finds equal InodeDB calls of every kind for a tenant of 2 batches and one of 120, with no commit and no S3 request; `conformance_stock` and the other stock suites pass on the same binary with the switch off. The fork's changed files are listed under Fork Changes. Missing: a run that shows threads and calls flat in the number of tables. |
| Complete | Work | 2D: Catalog load | `Engine::attachTenant` builds the configuration from the request (the tenant's disk as a shared writer, the request's settings document, pools of one thread) and runs `loadMetadata`. `worker_describe::a_worker_joins_the_shared_writers_of_a_disk_an_exclusive_writer_left` sees one commit, the new epoch, and `attaching_a_tenant_costs_the_same_whatever_its_parts` sees none on a disk already shared. `Engine::detachTenant` shuts the disk down before the response, which releases the reader lease: `worker_lease::a_finished_worker_holds_no_lease` frees a file after a `describe` and an insert and claims it at once, and fails at its 10-second limit against a worker built to skip the release. |
| Complete | Work | 2E: `describe` | `Describe.cpp`. `worker_describe::a_description_agrees_with_the_catalog` compares names, UUIDs, kinds, engines and the view edge with `Catalog`; `a_description_names_parts_as_the_engine_parses_them` compares every part's fields with `PartInfo::parse` and the active set with `Catalog::parts`, after a merge. |
| In Progress | Work | 2F: Startup profile | `benches/stock.rs` measures one `clickhouse local` process with one more step in each row. `BENCHMARKS.md` records 49 ms of CPU for the engine alone, 63 with an empty tenant's disk, 84 with four tables, 128 with 10 parts a table and 146 with the insert. The worker, measured by its own `Usage` in test runs on a busy machine: 20 ms to start the engine, about 50 ms for a `describe` and 55 ms for a 1,000-row insert through a projection and a view. Missing: the worker's steps in `BENCHMARKS.md`, recorded on a quiet machine. |
| Complete | Decision | Frames of protobuf on stdin and stdout | A frame is a length and its bytes, so the same stream works over a pipe now and over a socket when Phase 5's pool or Phase 8's forking parent needs one. Protobuf is already in the fork's build for the InodeDB client. A batch travels as raw data frames after the request, so no message holds it whole. |
| Complete | Decision | How operation mode is switched | A process-wide flag on `StorageMergeTree` that the worker sets before it attaches anything. A table setting could be turned on from SQL, where it would make a server load no parts for a table. A flag on the context would change `Context.h`, which every file of the fork includes. |
| In Progress | Risk | Client initialization sets a floor above the budget | A `describe` costs about 50 ms of CPU from exec with the S3 and gRPC clients in it, inside the gate's 80 ms. Protection: Phase 5's pre-spawned workers and Phase 8's fork move the engine's 20 ms off each operation. Missing: the figure in `BENCHMARKS.md`. |
| Incomplete | Risk | Operation mode skips a step inserts depend on | Protection: Phase 3's differential matrix. Missing: the matrix. |
| Incomplete | Risk | The worker's settings drift from a stock writer's | Compression and merge settings come from server config. Protection: the worker takes the same settings document a stock writer boots with; the differential suite compares part contents. Missing: both. |
| In Progress | Gate | `describe` within 80 ms of CPU, 60 MB, 40 threads; flat in part count; 150 ms at 300 tables | Test runs report about 50 ms of CPU, 33 MB of anonymous memory and 33 threads on the three-table tenant, and equal InodeDB calls for 2 and 120 batches' parts. Missing: the benchmark rows, with a 10,000-part table and a 300-table tenant. |
| In Progress | Gate | External sources are unknown to the worker | `worker_describe::external_engines_are_unknown_to_the_worker`: tenants with a `URL`, a `File` and a `Memory` table fail to attach with `UNKNOWN_STORAGE`. Missing: table functions in a view's query, dictionaries, and the `S3` and `Kafka` engines. |
| In Progress | Gate | BigHouse's suites pass on the fork commit | On `0b6ad7bd4`, with `BIGHOUSE_CLICKHOUSE` naming the worktree's binary: `shared.py` 120 passed, `features.py` 21, `fence.py` 4, `claims.py` 3, `crash.py 100` 100, `replicas.py` 10, `discovery.py` 21, none failed. Missing: the result of `parity.py`, and the same runs on `c8000603b`. |

## Phase 3: Insert Operation

Goal:
One operation turns a batch of rows into committed parts for a table and every view that reads from it, exactly once, with none of the table's parts loaded.

Scope:
- 3A: `insert`: a table, an input format, a token and the batch. It runs ClickHouse's `INSERT` interpreter and sink, so defaults, constraints, projections and the view chain behave as on a server, and it commits through BigHouse's shared-writer commit. It inserts synchronously (`async_insert = 0`), and on one thread for a batch under a size threshold, which costs 30% to 60% less CPU for 1,000 rows than the default thread fan-out.
- 3B: Results: for each table written, the committed part names, rows and bytes; the outcomes duplicate, schema changed and fenced; and the count of counter conflicts retried.
- 3C: Counter validity. Operation mode loads no parts, and an exclusive writer leaves the counter alone, so an insert trusts the counter only when the counter's version is newer than `owner`'s. Otherwise it raises the counter, under its version, to the highest block among the names of the table's parts and mutation entries before it takes a number.
- 3D: Limits the part set enforced. Part-count backpressure moves to the caller, which reads counts from `catalog`. Limits the sink enforces on the batch itself stay.
- 3E: Exactly-once preconditions. The operation refuses a table, or a view's target, whose settings leave tokens off, unless the request waives exactly-once.
- 3F: `OpWriter::insert` in the crate, streaming the batch to the worker.

Out of scope:
- One atomic commit for a source part and its views' parts. A retry with the token completes a partial insert, as BigHouse Phase 7 specifies.

Completion gate:
The conformance scenarios and the differential matrix pass on the worker. The crash matrix leaves each row once. Sixteen insert operations at a time on one table, beside a stock shared writer that merges, end with exact counts and unique block numbers. One 1,000-row insert through a projection and a view costs at most 100 ms of CPU and 100 ms of wall time from exec to exit, makes two InodeDB commits, and costs within 10% of that on a table of 10,000 parts.

Testing plan:
- Differential matrix: `MergeTree`, `ReplacingMergeTree`, `SummingMergeTree`, `AggregatingMergeTree`, `CollapsingMergeTree` and `VersionedCollapsingMergeTree`; with a projection, a view into an aggregating table, a chain of two views, `DEFAULT` and `MATERIALIZED` columns, a constraint, a TTL, `Nullable`, `LowCardinality`, `Array` and `Map` columns, a skip index, a batch across several partitions, and a batch larger than `max_insert_block_size`; in `JSONEachRow`, `RowBinary` and `Native`.
- Crash matrix: a kill before the source's commit, between the source's and the view's, after both and before the response; then a retry with the same token.
- Hazards, each with its protection switched off once: a stale counter after an exclusive writer's inserts; a schema change between load and commit; an exclusive writer starting mid-operation; a key revoked mid-operation.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 3A: `insert` | `Insert.cpp` runs `executeQuery` over the query's text and the batch's frames as one stream, with the token as `insert_deduplication_token`, `async_insert = 0` and one thread unless a profile says otherwise. The six insert scenarios of `conformance_worker` pass, through a projection, a view and a chain of two views, each ending with the checker and a stock load. `worker_insert::a_large_batch_arrives_whole` sends 400,000 rows over several frames into a part that keeps its files in S3. |
| In Progress | Work | 3B: Results and outcomes | A response names each committed part with its table, rows and bytes, from a record `MergeTreeSink` keeps in operation mode (`worker_insert::an_insert_reports_the_parts_it_committed`). A batch that commits nothing and finds its tokens taken ends `DUPLICATE` (`a_batch_sent_again_ends_as_a_duplicate_and_writes_nothing`). `LibchWorker.cpp` maps a lost epoch and a refused key to `FENCED`, a changed definition to `SCHEMA_CHANGED`, and a need for an exclusive writer to `REFUSED`; `conformance_worker::a_revoked_key_fences_its_writer` covers the key. Missing: tests that see a counter conflict counted and a definition change reported. |
| Complete | Work | 3C: Counter validity | `InodeDBTableCoordinator::readCounterOfThisEpoch` compares the counter's version with the `owner` version the disk joined, and `StorageMergeTree::prepareOperationCommits` raises an older counter to `highestBlockNumberInDirectory`, which reads part names and mutation entries and aborts a listing slower than half of `old_parts_lifetime`. `worker_insert::an_insert_numbers_its_part_above_what_an_exclusive_writer_left` inserts after an exclusive writer's inserts, and again after its merge and its mutation, and gets blocks 3, 6 and 7. |
| Incomplete | Work | 3D: Limits | The checks that read the part set see none in operation mode, so `parts_to_delay_insert`, `parts_to_throw_insert`, `max_parts_in_total` and the inactive-part limits never fire, and the caller reads counts from `Catalog::crowded_tables`. Missing: the list of each check with where it now lives, and a test of the limits the sink keeps on the batch itself. |
| Complete | Work | 3E: Exactly-once preconditions | `Insert.cpp` walks the table and every view's target and refuses when one keeps no tokens, unless the request sets `at_least_once`, which `OpWriter::at_least_once` sends (`worker_insert::a_table_without_tokens_is_refused_unless_the_writer_waives_exactly_once`, `a_view_target_without_tokens_is_refused_too`). Both refusals come before any part is written. |
| Complete | Work | 3F: `OpWriter::insert` | The call streams a batch in frames and maps each status, tested against the stand-in worker (`libch-testkit/tests/worker.rs`) and against the fork's worker (`conformance_worker`, `worker_insert`). |
| In Progress | Risk | A stale counter repeats a block number | After an exclusive epoch the counter trails the parts, and a repeated number makes a part that another covers or collides with. Protection: 3C, with its scenario passing. Missing: the scenario's failure with 3C switched off. |
| Complete | Risk | A view reads a table whose parts are not loaded | A view's query that names a table besides the one it is fed from, or its source a second time, would read that table as empty. Protection: `Insert.cpp` refuses such an insert before it writes anything, and `StorageMergeTree::read` throws in operation mode. `worker_insert::a_view_that_reads_stored_rows_is_refused` covers a subquery on another table and one on the source. |
| Incomplete | Risk | An operation ends an exclusive writer's epoch | A worker joins the disk as a shared writer, which replaces an exclusive `owner` and fences the exclusive writer, as a stock shared writer's start does. Protection: the caller runs no operation on a tenant while its exclusive writer works. Needs: a decision on whether a worker should refuse a disk whose owner is exclusive unless the request says the exclusive writer is done. |
| Incomplete | Risk | A view created after the worker loaded the catalog is skipped | Every DDL runs in an exclusive epoch, which fences the worker before it commits. Missing: a scenario that creates a view mid-operation and finds the insert fenced. |
| Incomplete | Risk | A check that read the part set is lost | Protection: 3D's list and the differential matrix. Missing: both. |
| Complete | Gate | Conformance and the differential matrix pass on the worker | `cargo test --test conformance_worker`: all seven scenarios pass, the merge scenario among them. `cargo test --test differential`: nine cases through `libch-testkit`'s `differential::run`, which sends the same bytes through workers and through the reference writer and compares the parts and a query set. `MergeTree` with a projection, a skip index, `DEFAULT` and `MATERIALIZED` columns, a constraint, a TTL, `Nullable`, `LowCardinality`, `Array` and `Map` columns, a view into an aggregating table and a second view from it, in batches over two partitions, runs in `JSONEachRow`, `RowBinary` and `Native`, and once with blocks of 100 rows, where a batch makes eight parts. `ReplacingMergeTree`, `SummingMergeTree`, `AggregatingMergeTree`, `CollapsingMergeTree` and `VersionedCollapsingMergeTree` each run once. |
| Complete | Gate | Exactly once under the crash matrix | `tests/worker_crash.rs` kills an insert before its first commit (`inodedb_insert_pause_before_commit`), between the source's commit and the view target's (`inodedb_insert_pause_after_commit`), and after both, before the response (`libch_pause_before_response`). The batch sent again commits whole, commits the one missing part, and ends as a duplicate; each time the events table and the view's target hold every row once and the checker and a stock load pass. An insert killed before its commit leaves nothing in the table's directory. |
| Complete | Gate | Sixteen concurrent inserts beside a merging stock writer | `worker_compaction::merges_run_beside_inserts_a_stock_writer_and_querying_fleets`: 160 batches, sixteen insert operations at a time on one table, beside a stock shared writer that merges and workers that plan and merge. The counter ends at 160, no parts intersect, the totals are exact through the view, and the checker and a stock load pass. The workers retried about 220 counter conflicts, so one table takes about 13 inserts a second at that concurrency on the dev machine: a caller gains by batching a tenant's rows, not by running its inserts side by side. |
| In Progress | Gate | 100 ms of CPU and wall, two commits, flat in part count | `worker_insert::an_insert_costs_the_same_whatever_the_parts`: a 1,000-row insert through a projection and a view makes 2 commits, 23 stats, 3 reads and 2 lists on a tenant of 150 parts and the same on a tenant of one, with no S3 request, in 55 ms of CPU. Missing: wall time, a 10,000-part table, and the rows in `BENCHMARKS.md`. |
| Incomplete | Test | Hazard scenarios with protections off | Missing: four scenarios, each run both ways. |

## Phase 4: Merge, Plan and Housekeeping Operations

Goal:
The caller compacts a table through explicit operations that end when their work commits: `plan` names merges, `merge` runs one, and `housekeep` removes what merges leave behind.

Scope:
- 4A: `plan`: refresh as BigHouse Phase 7 defines (the counter, then the listing), load part properties and mutation entries, and run ClickHouse's selector with stock settings over the parts no live claim holds and no mutation is pending for. It returns candidate merges with their sources, rows and bytes, up to a budget, and runs none.
- 4B: `merge`: sources by name. It validates them against a fresh complete view: each is active, they are adjacent in block order with no active part between them, no mutation is pending for any of them, and they sit at or below the counter it read. It takes their claims, loads only the sources, runs the merge task to completion in the calling thread, and commits through BigHouse's merge commit. A merge reads no mutation entries, so it would read a source a pending `RENAME COLUMN` applies to under the wrong names; the parts of a pending mutation wait for the exclusive writer that finishes it.
- 4C: `housekeep`: from one complete listing, remove each covered part whose covering part committed more than `old_parts_lifetime` ago, then its claim, skipping any part under a live claim; remove expired tokens; remove temporary directories older than `temporary_directories_lifetime`, which a merge leaves when its worker dies before its commit; and report what stays and why. A merge operation exits at its commit, so this operation is the one that removes its sources.
- 4D: Claims for short-lived holders: a merge renews its claims while it runs, a killed merge's claims expire, `plan` skips live claims, and `merge` takes over expired ones.
- 4E: `OpWriter::plan`, `merge` and `housekeep`, and catalog helpers that name the tables whose partitions hold more active parts than a threshold.

Out of scope:
- Applying mutations. A stock exclusive writer finishes them, and `plan` and `merge` keep parts a pending mutation separates apart, as the selector does.

Completion gate:
Three hundred small parts reach at most two through rounds of `plan` and `merge`, with answers equal to the reference. Merges beside insert operations and a stock shared writer leave no intersecting parts and exact answers, and a fleet querying throughout answers exactly. A merge of four sources in a table of 10,000 parts spends at most 100 ms of CPU before it reads its first source, and the worker exits within 50 ms of its commit's answer. After the grace period `housekeep` leaves no covered part.

Testing plan:
- Differential: for each engine in the matrix, a table merged to one part by operations and one merged by the reference's `OPTIMIZE ... FINAL` hold equal rows.
- Hazards, each with its protection switched off once: sources around an active part; a source that is covered or gone; two merges racing for overlapping sources; a plan gone stale before its merge; `housekeep` reading a listing cut short; `housekeep` beside a merge taking over an expired claim; a pending mutation between two sources.
- Crash matrix: a kill after the claims, mid-merge, after the commit and before exit, and mid-`housekeep`; then the checker, and a second merge of the same range.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Complete | Work | 4A: `plan` | `Plan.cpp` calls `StorageMergeTree::planMerges`, which refreshes the parts, reads the mutation entries and other writers' claims, and runs `selectPartsToMerge` up to the budget with nothing claimed; the worker keeps a server's merge pool size for a plan, since the selector bounds merges by it. Each candidate carries its sources' rows and bytes (`PlannedMerge`). `worker_merge::a_plan_proposes_merges_the_worker_can_run_and_claims_nothing`, `a_plan_leaves_out_the_parts_another_writer_holds`, and `worker_hazards::a_pending_mutation_keeps_its_parts_out_of_plans_and_merges`, where a plan proposes the six parts after a pending mutation and neither part before it. A table with patch parts gets an empty plan. |
| Complete | Work | 4B: `merge` | `Merge.cpp` reads the counter, lists the directory by name and refuses sources that are fewer than two, outside the partition, not adjacent, above the counter, under a pending mutation, or in a table with patch parts; sources that are covered or gone lose the claim. `StorageMergeTree::mergeNamedParts` loads those parts with `MergeTreeData::loadNamedParts`, claims them, runs `MergePlainMergeTreeTask` in the calling thread and commits through `commitSharedMergedPart`. `tests/worker_merge.rs`: eight merge tests, among them a merge of a merged part, a merge of the aggregating target, and an insert beside a merge. `conformance_worker::merges_leave_one_part_and_the_same_rows` passes. |
| Complete | Work | 4C: `housekeep` | `Housekeep.cpp` removes covered parts in batches of 50 for each covering part older than `old_parts_lifetime`, each batch requiring the covering part's directory and each claim's version, and leaving parts under a live claim; then expires tokens, sweeps dead claims, and calls `clearOldTemporaryDirectories` with `temporary_directories_lifetime`. A shared writer may remove any path, so no fork change was needed for parts another process merged. `worker_merge::housekeeping_removes_covered_parts_once_their_grace_has_passed`, `housekeeping_leaves_a_part_another_writer_holds`, `housekeeping_keeps_the_tokens_of_recent_batches`; `worker_crash::a_merge_killed_before_its_commit_leaves_a_directory_for_housekeeping`. |
| Complete | Work | 4D: Claims for short-lived holders | `mergeNamedParts` starts the claim renewal task before it claims. `worker_crash` holds a merge for two seconds under a claim lifetime of 1.5 s and finds its claims live at the kill, then expired, then taken over by the next merge of the range. `worker_merge::another_writers_claim_holds_a_merge_off_until_it_expires` covers a live and an expired claim of another writer, and `a_plan_leaves_out_the_parts_another_writer_holds` the plan. |
| Complete | Work | 4E: Crate calls and catalog helpers | `OpWriter::plan`, `merge` and `housekeep` run against the fork's worker (`tests/worker_merge.rs`, `conformance_worker`) and round-trip against the stand-in. `Writer::plan` returns `PlannedMerge`: a `MergeCandidate` with its sources' rows and bytes, which the reference writer reads from `system.parts`. `Housekept` counts the temporary directories removed. `Catalog::crowded_tables` names the tables with a partition over a part threshold (`tests/catalog.rs::crowded_tables_are_the_ones_over_the_part_threshold`). |
| Complete | Risk | A merge of named sources covers a live part | Sources around an active part produce a part that covers it, and its rows vanish. Protection: 4B's adjacency check against a complete view. `worker_merge::sources_around_an_active_part_are_refused` shows the refusal, and `worker_hazards::sources_around_an_active_part_would_take_its_rows` the loss with `libch_merge_unchecked_sources` on: 200 of 300 rows remain. `worker_hazards::a_part_above_the_counter_waits` covers the counter bound that keeps a listing read in pages complete. |
| Complete | Risk | `housekeep` removes a live part | A part is covered when a listed name contains its name, so a listing cut short shows fewer covered parts, never more. A covering part leaves the directory only when an exclusive writer drops it, whose epoch fences the housekeeper's commit, or when a later merge covers it too. Each batch still requires the covering part's directory, and leaves a part whose claim another writer holds or changed (`worker_merge::housekeeping_leaves_a_part_another_writer_holds`). |
| Complete | Risk | Covered parts stay forever | BigHouse's shared writers remove only the sources they merged. Protection: 4C. `worker_merge::housekeeping_removes_covered_parts_once_their_grace_has_passed` ends with the merged part alone in the directory and no claim, with no stock writer involved. |
| Complete | Risk | Overlapping merges | Protection: BigHouse's claims. `worker_hazards::overlapping_merges_commit_once` runs four merges at once whose ranges all share a part: one commits, three lose the claim, no parts intersect and the totals hold. |
| Complete | Risk | A plan proposes a merge that `merge` refuses | The selector merges parts that share a pending mutation, and `merge` refuses any source a mutation is pending for. Protection: under a plan `sharedWriterCannotMerge` leaves those parts out. `worker_hazards::a_pending_mutation_keeps_its_parts_out_of_plans_and_merges` registers a real `UPDATE`, merges what the plan proposes, and has an exclusive writer finish the mutation: the update reaches the two batches before it and no other. `a_merge_across_a_pending_mutation_would_rewrite_later_rows` shows the damage with the check off: the update rewrites a batch inserted after it. |
| Complete | Risk | A dead worker's files stay in the table | A merge killed after it wrote its part leaves `tmp_merge_…` in the table's directory; an insert leaves nothing there. Protection: 4C removes temporary directories past `temporary_directories_lifetime`, one day by default, as a server's background cleanup does, and an exclusive writer's start removes them all. `worker_crash::a_merge_killed_before_its_commit_leaves_a_directory_for_housekeeping`. |
| Complete | Gate | 300 parts to at most two, equal to the reference | `worker_compaction::three_hundred_parts_compact_to_one_with_the_reference_writers_answers`: 300 inserts through workers, then rounds of `plan` and `merge` on the table and the view's target until each holds at most two parts, then `housekeep` to no covered part. Six queries answer as they do on a tenant a stock writer filled with the same batches and merged with `OPTIMIZE ... FINAL`. The stock selector proposes two or three small parts only once they are ten seconds old, so the rounds wait once. |
| Complete | Gate | Merges beside inserts, a stock writer and a querying fleet | `worker_compaction::merges_run_beside_inserts_a_stock_writer_and_querying_fleets`: rounds of `plan` and `merge` beside sixteen inserts at a time and a stock shared writer merging from a view it refreshes every three seconds. In three runs the workers committed 4 to 6 merges and lost 1 to 3 claims to the stock writer, which merged 7 or 8 times. A fleet of two servers assigned for each query, about twenty over the run, answered whole batches with each row once every time. No parts intersect at the end, and the checker and a stock load pass. |
| Incomplete | Gate | 100 ms of CPU before the first source read; exit within 50 ms of the commit | Missing: the benchmark rows. |
| Complete | Gate | `housekeep` leaves no covered part after the grace period | `worker_merge::housekeeping_removes_covered_parts_once_their_grace_has_passed`: under the default grace the three sources stay and the report says why; a second past a one-second grace they are gone with their claims, and a further run finds nothing. |
| Complete | Test | Differential merge per engine | `cargo test --test differential`: after the inserts each case merges every partition of every table to one part, through workers in two merges, so a merged part is a source, and on the reference with `OPTIMIZE ... FINAL`. The stored rows and the `FINAL` rows answer alike for the six engines, as do the projection, the skip index and both views' targets. |
| In Progress | Test | Hazard scenarios with protections off | `tests/worker_hazards.rs` runs two both ways: sources around an active part, and a pending mutation between two sources. Protected runs alone cover a source that is covered or gone and a plan gone stale (`worker_merge::a_source_that_is_covered_or_gone_loses_the_claim`), racing merges (`overlapping_merges_commit_once`), and `housekeep` beside a live claim. BigHouse's `claims.py` switches the claims off. Missing: the covered-source scenario with the worker's check off, where the source's `done` claim is the second protection. |
| Complete | Test | Merge and housekeep crash matrix | `tests/worker_crash.rs`: a merge killed after its claims, after it wrote its part and before its commit, and after its commit before the response; and `housekeep` killed between two batches. After each the checker passes; a second merge of the range commits once the claims run out, or loses the claim where the first had committed; and a second `housekeep` removes the rest. |

## Phase 5: Pool, Limits and Isolation

Goal:
The crate runs operations for many tenants at once from a bounded pool, and each worker is confined to its tenant.

Scope:
- 5A: `Pool`: workers spawned ahead of demand that finish static initialization and wait for a request. Each serves one operation and is replaced.
- 5B: `Tenant`: an InodeDB store endpoint, a root and an S3 prefix. Write keys are minted with a TTL, cached per tenant and revoked on demand.
- 5C: Limits per operation: a memory cap, a thread cap and a deadline. An operation over a limit commits nothing, and its outcome names the limit.
- 5D: The sandbox: a private temporary directory, no other writable path, network egress to the tenant's InodeDB store and S3 endpoint only, and no credentials but the tenant's.
- 5E: The public API and its documentation: `Pool`, `Tenant`, `Writer`, `catalog` and the outcomes, with examples that compile.

Completion gate:
Sixty-four operations at a time across 64 tenants pass the conformance scenarios. A sandboxed worker fails to open a file outside its directory, to connect to any other host, and to read another tenant's prefix. An operation over its memory cap fails with nothing visible. A 1,000-row insert takes at most 40 ms from request to commit through a waiting worker.

Testing plan:
- An escape suite: a view whose SELECT names a table function, a dictionary source, a `URL` engine table, a symlink out of the temporary directory, and a connection to a local port that is off the allowlist.
- A waiting worker holds no tenant state: its memory and open files are equal before any request, whatever tenants ran earlier.
- Limits: a batch that exceeds the memory cap, a merge that exceeds the deadline, and the checker after each.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| In Progress | Work | 5A: `Pool` | `libch/src/pool.rs`: a pool keeps `waiting` workers started, gives each operation the one started first and starts its replacement, runs at most `concurrency` operations, passes over a worker that died waiting, retries failed starts at growing intervals, and kills its waiting workers when closed or dropped. `worker.rs` drains a worker's log from its start, so a waiting worker never blocks in it. `libch-testkit/tests/pool.rs` runs 13 cases against the stand-in worker, among them 64 tenants at once, the bound of three observed from inside the workers, and a turn given back after a deadline and after a dropped call. Missing: runs with the fork's worker. |
| Complete | Work | 5B: `Tenant` and key cache | `libch/src/keys.rs`: `KeyCache` mints a tenant's key on first use, shares one mint among callers that arrive together, replaces a key that has less than `min_remaining` left while the old one lasts its term, revokes on demand, and keeps its count of keys in the store at or under `capacity` by revoking the key of the tenant served longest ago, leaving any key an operation holds. A mint whose answer was lost keeps its place in the count until it would expire. `TenantKey` and `S3Location` hide their secrets from `Debug`. `tests/keys.rs` runs 11 cases against InodeDB, each checking what the store itself accepts and refuses; 4 unit tests cover eviction order, expiry and lost answers. The held-key case fails with the protection removed. |
| Incomplete | Work | 5C: Limits | Missing: the three limits and their outcomes. |
| Incomplete | Work | 5D: Sandbox | Missing: the profile and the escape suite. |
| Incomplete | Work | 5E: Public API and documentation | Missing: rustdoc with compiling examples. |
| Incomplete | Decision | Sandbox mechanism | Needs: a choice among user namespaces with seccomp and Landlock, bubblewrap, and a container runtime, by start cost per worker (at most 5 ms) and support for an egress allowlist. |
| Incomplete | Risk | Waiting workers hold memory | Each holds the engine's baseline, 35 MB by the reference number. Protection: the pool's size is a setting, and `BENCHMARKS.md` records memory per waiting worker. Missing: the measurement. |
| Incomplete | Gate | 64 concurrent operations across 64 tenants | Missing: the run. |
| Incomplete | Gate | The escape suite fails every attempt | Missing: five cases. |
| Incomplete | Gate | An operation over its memory cap leaves nothing visible | Missing: the scenario and the checker's pass. |
| Incomplete | Gate | 40 ms from request to commit through a waiting worker | Missing: the benchmark row. |

## Phase 6: Demo of the High-Tenant Recipe

Goal:
A Rust program shows the recipe end to end: thousands of tenants ingested and merged by operations alone, with no long-lived process per tenant, each queried by read-only servers the program boots and clusters.

Scope:
- 6A: `libch-demo setup`: N tenants, each with an InodeDB root, an S3 prefix and the demo schema (an events table with a projection, and a view into an `AggregatingMergeTree` table), created through `Exclusive`.
- 6B: A load generator and batcher: events for tenants drawn from a Zipf distribution, batched per tenant and table by size and age, each batch with an id that is its token. The generator keeps expected totals per tenant.
- 6C: The ingest loop: batches become `insert` operations through the pool, and a failed or timed-out batch goes again with its token.
- 6D: The merge loop: a catalog scan finds tables over the part threshold, then `plan`, `merge` with bounded concurrency, and `housekeep` after the grace period.
- 6E: The query step: for a sampled tenant the program takes blank servers, mints a read key, assigns and attaches them, runs the query on them as parallel replicas, and replaces them. The answers equal the generator's row counts and view sums.
- 6F: The report: tenants, batches, rows, operations by kind and outcome, worker CPU per operation, InodeDB commits per insert, parts per partition at the end, and blobs orphaned by kills, beside the stock recipe measured on a sample of the same tenants.
- 6G: `demo/README.md`: the recipe in steps.

Out of scope:
- A durable buffer, an HTTP API, a query API with slot reservation, and routing tenants across several InodeDB stores. `Tenant` carries its store's endpoint, so the demo's single store is a choice of the demo.

Completion gate:
On the dev machine, 1,000 tenants take 100,000 batches of 1,000 rows. The checker finds every tenant's totals equal to the generator's, and 100 sampled tenants answer equally through fleets of four servers. Every partition ends at or under the part threshold with no covered part. Mean worker CPU per insert is at most 100 ms, the same for a tenant of 10 parts and one of 500, and the report sets it beside the stock recipe on the same sample. A run that kills workers at random ends equally exact. A 10,000-tenant run completes, and `BENCHMARKS.md` holds both reports.

Testing plan:
- A small run (20 tenants, 2,000 batches) in `cargo test`, with the query step on every tenant.
- The gate's runs, started by `demo/run`, with the InodeDB store on the drive and on tmpfs.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Work | 6A: Tenant setup | Missing: `libch-demo setup` and the schema. |
| Incomplete | Work | 6B: Load generator and batcher | Missing: the generator, batching by size and age, and expected totals. |
| Incomplete | Work | 6C: Ingest loop | Missing: the loop and its retry rule. |
| Incomplete | Work | 6D: Merge loop | Missing: the scan, the threshold, and the three operations in order. |
| Incomplete | Work | 6E: Query step | Missing: the fleet step in `libch-demo` and the comparison with expected totals. |
| Incomplete | Work | 6F: Report | Missing: the report and the stock sample beside it. |
| Incomplete | Doc | 6G: `demo/README.md` | Missing: the document. |
| Incomplete | Risk | The dev stack's S3 service drops connections under bursts | Several servers scanning blobs at once made ClickHouse's S3 retries stall a query for 10 to 40 seconds in Phase 1. Protection: none yet. Needs: a dev S3 service that holds up, or fleet queries sized to it, before the demo's query step is trusted. |
| Incomplete | Risk | Tenant setup dominates the demo | Each tenant's schema costs one stock process. Protection: setup runs in parallel and its cost is in the report. Missing: the measurement. |
| Incomplete | Gate | 1,000 tenants and 100,000 batches end exact | Missing: the run, the checker's pass, and 100 fleet answers. |
| Incomplete | Gate | Parts at or under the threshold, no covered parts | Missing: the report's part distribution. |
| Incomplete | Gate | CPU per insert at most 100 ms, flat in the tenant's parts | Missing: the report beside the stock sample. |
| Incomplete | Gate | The run with random kills ends exact | Missing: the run. |
| Incomplete | Gate | A 10,000-tenant run completes | Missing: its report in `BENCHMARKS.md`. |
| Incomplete | Test | Small demo run in `cargo test` | Missing: the test. |

## Phase 7: Hardening

Goal:
The library holds its invariants under faults for hours, and each protection has a test that fails without it.

Scope:
- 7A: A kill loop: random `SIGKILL` of workers and stock writers under a mix of inserts, plans, merges, housekeeping and exclusive DDL, with the checker and an exactly-once count after each round.
- 7B: Faults between the worker and InodeDB: a lost commit answer, a `NotWriter` redirect, and a key revoked or expired mid-operation.
- 7C: A differential soak: seeded random workloads over the Phase 3 matrix, through operations and through the reference.
- 7D: Planted bugs: `scripts/mutants` removes each hazard's protection in turn, and the suites must fail for each.
- 7E: Orphans: the blobs that killed operations leave, counted per 1,000 kills and described for BigHouse's orphan sweep.
- 7F: Documents: `README.md` with what libch is, the recipe and its limits, and operating notes on keys, pools, limits and what goes to a stock exclusive writer.

Completion gate:
One thousand kill rounds and a one-hour soak end with no invariant broken and no difference from the reference, every planted bug is caught, and the documents exist.

Testing plan:
- `tests/kill --rounds 1000`, `tests/soak --minutes 60 --seed S`, and `scripts/mutants`, each with its log kept under `build/`.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Work | 7A: Kill loop | Missing: the loop and a 1,000-round log. |
| Incomplete | Work | 7B: InodeDB faults | Missing: the three faults and their scenarios. |
| Incomplete | Work | 7C: Differential soak | Missing: the workload generator and a one-hour log. |
| Incomplete | Work | 7D: Planted bugs | Missing: `scripts/mutants` and a list that maps each hazard in Phases 3 and 4 to its mutant. |
| Incomplete | Work | 7E: Orphan count | Missing: the count and the note for BigHouse. |
| Incomplete | Doc | 7F: `README.md` and operating notes | Missing: both documents. |
| Incomplete | Gate | 1,000 kill rounds and a one-hour soak end clean | Missing: both logs. |
| Incomplete | Gate | Every planted bug is caught | Missing: the mutants run. |

## Phase 8: Fork per Operation

Goal:
A tenant switch costs a fork, so a small insert's CPU approaches the insert's own work. This phase starts when Phase 6's report shows operation startup as the largest share of worker CPU.

Scope:
- 8A: A parent worker that finishes static initialization on one thread and forks a child for each request.
- 8B: Fork safety: the parent starts no thread, and the child creates the allocator's background work and the gRPC and S3 clients.
- 8C: `Pool` runs operations through the parent, behind a setting.

Completion gate:
One 1,000-row insert through a projection and a view costs at most 15 ms of CPU and 20 ms of wall time, `BENCHMARKS.md` records private memory per running operation, and every earlier suite passes with the setting on.

Testing plan:
- The conformance, differential and kill suites with the setting on.
- A check that the parent holds one thread and no tenant's key after 10,000 operations.

Status ledger:

| Status | Type | Item | Evidence / Gap |
| --- | --- | --- | --- |
| Incomplete | Work | 8A: Forking parent | Missing: the parent and its request loop. |
| Incomplete | Work | 8B: Fork safety | Missing: an audit of threads and clients created before the fork, and the fixes. |
| Incomplete | Work | 8C: `Pool` setting | Missing: the setting and the suites run with it. |
| Incomplete | Risk | ClickHouse starts threads during static initialization | A fork copies one thread, so a lock another thread held stays locked in the child. Protection: 8B's audit, and the parent's thread count asserted before every fork. Missing: both. |
| Incomplete | Gate | 15 ms of CPU and 20 ms of wall per small insert | Missing: the benchmark row. |
| Incomplete | Gate | Every earlier suite passes with the setting on | Missing: the runs. |
| Incomplete | Test | Parent holds one thread and no key after 10,000 operations | Missing: the test. |
