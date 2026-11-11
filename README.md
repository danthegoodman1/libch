# libch

libch runs ClickHouse's insert and merge code as explicit, short-lived operations on a tenant's disk, so one pool of workers can serve a very large number of tenants. The disks are [BigHouse](https://github.com/danthegoodman1/BigHouse) `inodedb` disks: MergeTree tables whose blobs live in S3 and whose file tree lives in [InodeDB](https://github.com/danthegoodman1/inodedb).

[PLAN.md](PLAN.md) holds the goal, the phases and what each has delivered. Today the crate reads a tenant's catalog from InodeDB, drives stock ClickHouse servers as the reference writer and as read-only fleets, checks a disk's invariants, and runs five operations through the worker: `describe`, `insert`, `plan`, `merge` and `housekeep`. An insert of 1,000 rows through a projection and a materialized view costs the worker 54 ms of CPU and two InodeDB commits on tenants of 10 to 10,000 parts, and returns in 36 ms through a waiting worker; a stock server's whole life for the same insert costs 219 ms at 10 parts and 1,177 ms at 500 ([BENCHMARKS.md](BENCHMARKS.md)). A plan proposes merges with the stock selector, a merge runs one by its parts' names, and housekeeping removes what merges and dead workers leave behind. [`demo/`](demo/README.md) runs the whole recipe: 100 tenants took two million rows at 50,000 a second through 2,948 inserts and 415 merges and ended exact, checked from InodeDB and through read-only fleets. Limits for each operation, the sandbox and the large demo runs are next.

## What is here

| Path | Contents |
| --- | --- |
| `crates/libch` | The crate: `catalog`, `part`, `disk`, the `Writer` trait, `stock` servers, `check`, a small S3 client, `worker` (the client that runs each operation in a worker process), `pool` (workers started ahead of demand) and `keys` (tenants' keys, minted, cached and revoked). |
| `proto/libch.proto`, `crates/libch-proto` | The protocol between the crate and a worker. |
| `crates/libch-testkit` | What tests, benchmarks and the demo share: the dev stack, the preflight, a tenant fixture, the conformance scenarios, process measurements, and a stand-in worker for protocol tests. |
| `crates/libch/tests` | Integration tests against the dev stack. |
| `crates/libch/benches/stock.rs` | The stock baselines, which write [BENCHMARKS.md](BENCHMARKS.md). |
| `scripts/dev-up`, `scripts/dev-down` | libch's own dev stack: InodeDB on port 7170 and s3proxy on port 11211. |
| `scripts/fork-setup`, `scripts/fork-build` | The worker's branch of the fork: a git worktree at `fork/ClickHouse` and its build at `fork/build`. |
| `bighouse.rev` | The BigHouse commit this code runs against. |
| `fork.rev` | The commit of the fork's `libch-worker` branch this code runs against. |

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

`cargo bench --bench stock` records the stock baselines. It refuses to record on a busy machine.
