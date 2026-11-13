# libch

libch runs ClickHouse's insert and merge code as explicit, short-lived operations on a tenant's disk, so one pool of workers serves a very large number of tenants. The disks are [BigHouse](https://github.com/danthegoodman1/BigHouse) `inodedb` disks: MergeTree tables whose blobs live in S3 and whose file tree lives in [InodeDB](https://github.com/danthegoodman1/inodedb). A disk libch writes is a stock BigHouse disk: read-only ClickHouse servers query it, and a stock writer can take it over, at any time.

A Rust crate drives a worker program built from the BigHouse fork of ClickHouse. Each operation starts from the tenant's key and the state in InodeDB, does one thing, commits and exits:

| Operation | What it does |
| --- | --- |
| `insert` | Commits a batch to a table and to every materialized view's target, once, however often it is sent under its token. |
| `plan` | Proposes merges with ClickHouse's own selector. |
| `merge` | Merges named parts into one. |
| `housekeep` | Removes what merges and dead workers leave behind. |
| `describe` | Lists a tenant's tables and parts as the worker's engine sees them. |
| `share` | Hands a disk back to operations once the tenant's exclusive writer has stopped. |

## Why operations

Writing a tenant through stock ClickHouse means booting a process for each batch or keeping one running for every tenant. The boot costs many times the insert, and grows with the parts the tenant holds. An operation adds a small, fixed cost to the insert:

| Parts a table | Stock server: boot, insert, stop | `clickhouse local` | libch, a worker per operation | libch, a forking worker |
| --- | --: | --: | --: | --: |
| 10 | 205 ms (37×) | 126 ms (23×) | 40 ms (7×) | 15.7 ms (2.8×) |
| 100 | 389 ms (69×) | 296 ms (53×) | 41 ms (7×) | 16.7 ms (3.0×) |
| 500 | 1,002 ms (179×) | 908 ms (162×) | 41 ms (7×) | 16.4 ms (2.9×) |

Each figure is the CPU for one 1,000-row insert through a projection and a materialized view, beside its multiple of the same insert into a running writer (5.6 ms). The stock processes run with background merges off, as a process that serves one batch would. A forking worker starts the engine once and serves each operation in a child it forks. A stock server holds 145 to 200 MB and over 200 threads and answers in 238 to 341 ms; a worker holds 32 MB and 33 threads and answers in 23 to 28 ms.

A worker loads none of a table's parts, so an insert costs the same on a tenant of ten thousand parts, and builds only the tables its operation reaches. [BENCHMARKS.md](BENCHMARKS.md) has the method and every operation's cost.

## The recipe

```rust
let pool = Pool::new(WorkerCommand::clickhouse("/usr/bin/clickhouse"), PoolOptions { waiting: 8, concurrency: 16 })?;
let keys = KeyCache::new(admin, KeyPolicy::default())?;
let writer = pool.writer(&tenant, keys.key(&tenant).await?);

writer.insert(Insert { table: &events, format: "JSONEachRow", token: "batch-1", data: batch }).await?;
for planned in writer.plan(&events, &Budget::default()).await? {
    writer.merge(&events, &planned.candidate).await?;
}
writer.housekeep(&events).await?;
```

[`demo/`](demo/README.md) runs the whole recipe for many tenants: batching, one insert in flight for each tenant, retries under a token, compaction from the catalog, housekeeping, and queries through read-only servers that belong to no tenant. [OPERATING.md](OPERATING.md) says what each call can answer and what a service around libch decides.

## Limits

- **DDL and mutations run in a stock exclusive writer.** `stock::Exclusive::run` boots one for a tenant, runs statements and stops. From its start the tenant's operations end `Fenced`, so none interrupts it, and they run again once `share` hands the disk back.
- **A view reads the rows it is fed.** A worker refuses an insert whose view also reads stored rows, names a table function or reads a dictionary.
- **Exactly-once needs tokens.** Every table an insert reaches keeps deduplication tokens (`non_replicated_deduplication_window`), or the writer waives exactly-once.
- **A source part and its views' parts commit one after another.** A retry under the token completes an insert that died between them.
- **`plan` loads every part of its table.** That costs about half a millisecond of CPU a part on every call, where a stock server pays it once at boot, so a caller plans the tables the catalog names as crowded and keeps them compacted.
- **A part holds up to about 7,400 files**, which is a wide part of about 3,700 columns. InodeDB bounds the commit that makes a part.
- **The sandbox needs Linux 6.12.** It confines a worker by file, port, signal and system call; which hosts a worker reaches is for the network to say.

[PLAN.md](PLAN.md) holds the goal, the phases, and the evidence for each.

## What is here

| Path | Contents |
| --- | --- |
| `crates/libch` | The crate: `catalog`, `part`, `disk`, the `Writer` trait, `stock` servers, `check`, a small S3 client, `worker` (the client that runs each operation in a worker process, and the sandbox), `pool` (workers started ahead of demand) and `keys` (tenants' keys, minted, cached and revoked). |
| `proto/libch.proto`, `crates/libch-proto` | The protocol between the crate and a worker. |
| `crates/libch-testkit` | What tests, benchmarks and the demo share: the dev stack, the preflight, a tenant fixture, the conformance scenarios, the differential harness and its workload generator, a proxy that fails calls to the store, process measurements, and a stand-in worker for protocol tests. |
| `crates/libch/tests` | Integration tests against the dev stack. |
| `crates/libch/benches` | The benchmarks that write [BENCHMARKS.md](BENCHMARKS.md). |
| `demo` | `libch-demo`, the high-tenant recipe end to end, and `demo/run`, which starts its large runs. |
| `scripts/dev-up`, `scripts/dev-down` | libch's own dev stack: InodeDB on port 7170 and s3proxy on port 11211. |
| `scripts/fork-setup`, `scripts/fork-build` | The worker's branch of the fork: a git worktree at `fork/ClickHouse` and its build at `fork/build`. |
| `scripts/soak`, `scripts/mutants` | The differential soak, and planted bugs that the tests must catch. |
| `bighouse.rev`, `fork.rev` | The BigHouse commit, and the commit of the fork's `libch-worker` branch, this code runs against. |

## Running the tests

The tests need:

- a BigHouse checkout beside this repository (or at `BIGHOUSE_DIR`), at the commit in `bighouse.rev` or later, with the fork's ClickHouse built (`scripts/clickhouse-build`) and InodeDB built (`scripts/inodedb-build`);
- the worker's branch of the fork, `libch-worker`, in that checkout's `ClickHouse` repository, built here;
- Docker, for s3proxy;
- `protoc`, which the InodeDB client's build runs.

```sh
scripts/fork-setup   # makes fork/ClickHouse, a worktree of the fork on the worker's branch
scripts/fork-build   # builds fork/build/programs/clickhouse, the stock server and the worker in one binary
scripts/dev-up       # starts InodeDB and s3proxy, with their state in .dev/
cargo test
scripts/dev-down     # add --wipe to delete the state
```

The first `scripts/fork-build` compiles all of ClickHouse, about an hour on 24 cores. `LIBCH_CLICKHOUSE` points the tests at a binary built elsewhere.

Every test that needs the stack runs a preflight first and fails with what to start or build. Each test writes a tenant of its own and removes it, so tests run side by side and leave the stack empty.

| Command | What it runs |
| --- | --- |
| `cargo test` | The suite: about six minutes on a freshly wiped stack. |
| `LIBCH_SANDBOX=1 cargo test` | The suite with every worker in the sandbox. |
| `LIBCH_FORK=1 cargo test` | The suite with every worker forking a child for each operation. |
| `cargo test -p libch --test pool_tenants -- --ignored` | Sixty-four tenants at once through one pool. |
| `LIBCH_KILL_ROUNDS=1000 cargo test -p libch --test worker_kill_loop` | The kill loop at the plan's length. |
| `LIBCH_FORK_OPERATIONS=10000 cargo test -p libch --test worker_fork a_parent_serves` | Ten thousand operations through one forking worker. |
| `PARTS=10000 cargo test -p libch --test plan_beside_stock_boot -- --ignored --nocapture` | A `plan` beside a stock server's boot on a table of 10,000 parts. |
| `scripts/soak --minutes 60` | The differential soak. |
| `scripts/mutants` | Each planted bug in turn, about an hour of builds. |
| `demo/run gate`, `demo/run kills`, `demo/run wide` | The demo's large runs, each from an empty stack: 1,000 tenants and 100,000,000 rows, the same with insert workers killed mid-operation, and 10,000 tenants. `--tmpfs` keeps the InodeDB store in memory. `wide` needs an InodeDB server at `3f4ff2d` or later, which `LIBCH_INODEDB_BIN` names while BigHouse pins an older one. |
| `cargo bench --bench stock`, `cargo bench --bench operations` | The benchmarks. They refuse to record on a busy machine. |
