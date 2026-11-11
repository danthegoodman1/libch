# libch

libch runs ClickHouse's insert and merge code as explicit, short-lived operations on a tenant's disk, so one pool of workers can serve a very large number of tenants. The disks are [BigHouse](https://github.com/danthegoodman1/BigHouse) `inodedb` disks: MergeTree tables whose blobs live in S3 and whose file tree lives in [InodeDB](https://github.com/danthegoodman1/inodedb).

[PLAN.md](PLAN.md) holds the goal, the phases and what each has delivered. Phases 0 and 1 are complete: the crate reads a tenant's catalog from InodeDB, drives stock ClickHouse servers as the reference writer and as read-only fleets, and checks a disk's invariants. The crate's side of the worker is in place too: the protocol, the client that runs each operation in a worker process, the pool that keeps workers started ahead of demand, and the cache of tenants' keys. The worker itself, which runs the operations inside the BigHouse fork, is next.

## What is here

| Path | Contents |
| --- | --- |
| `crates/libch` | The crate: `catalog`, `part`, `disk`, the `Writer` trait, `stock` servers, `check`, a small S3 client, `worker` (the client that runs each operation in a worker process), `pool` (workers started ahead of demand) and `keys` (tenants' keys, minted, cached and revoked). |
| `proto/libch.proto`, `crates/libch-proto` | The protocol between the crate and a worker. |
| `crates/libch-testkit` | What tests, benchmarks and the demo share: the dev stack, the preflight, a tenant fixture, the conformance scenarios, process measurements, and a stand-in worker for protocol tests. |
| `crates/libch/tests` | Integration tests against the dev stack. |
| `crates/libch/benches/stock.rs` | The stock baselines, which write [BENCHMARKS.md](BENCHMARKS.md). |
| `scripts/dev-up`, `scripts/dev-down` | libch's own dev stack: InodeDB on port 7170 and s3proxy on port 11211. |
| `bighouse.rev` | The BigHouse commit this code runs against. |

## Running the tests

The tests need:

- a BigHouse checkout beside this repository (or at `BIGHOUSE_DIR`), at the commit in `bighouse.rev` or later, with the fork's ClickHouse built (`scripts/clickhouse-build`) and InodeDB built (`scripts/inodedb-build`);
- Docker, for s3proxy;
- `protoc`, which the InodeDB client's build runs.

```sh
scripts/dev-up     # starts InodeDB and s3proxy, with their state in .dev/
cargo test
scripts/dev-down   # add --wipe to delete the state
```

Every test that needs the stack runs a preflight first and fails with what to start or build. Each test writes a tenant of its own and removes it, so tests run side by side and leave the stack empty.

`cargo bench --bench stock` records the stock baselines. It refuses to record on a busy machine.
