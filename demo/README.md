# libch-demo: the high-tenant recipe

`libch-demo` ingests and compacts many tenants' analytics tables with short-lived operations alone. No process outlives an operation, so one pool of workers serves every tenant, and a tenant that sends nothing costs nothing. The tables are stock MergeTree tables on [BigHouse](https://github.com/danthegoodman1/BigHouse) disks: read-only ClickHouse servers query them as they query any BigHouse tenant.

## Run it

Start libch's dev stack and build the fork's binary as the [repository README](../README.md) describes. Then:

```sh
cargo run --release -p libch-demo -- --tenants 100 --batches 2000 --events-per-second 50000
```

The run sets its tenants up, ingests, compacts, checks every tenant, queries a sample, prints a report, and removes the tenants. It says on stderr how far each step has come. It exits with status zero when every tenant ends with exactly the rows the generator made. `cargo test -p libch-demo` runs two small runs, one of which sets a kill timer on one insert in five.

Two environment variables change how the run's workers work, as they do for the tests: `LIBCH_FORK=1` serves each operation in a child of a worker that started the engine once, and `LIBCH_SANDBOX=1` confines every worker to its own directory and the stack's two ports.

| Flag | Meaning | Default |
| --- | --- | --- |
| `--tenants` | Tenants to set up. | 20 |
| `--batches`, `--rows-per-batch` | The load: their product in events. | 400, 1,000 |
| `--events-per-second` | The rate the load arrives at. Without it the whole load is offered at once. | none |
| `--zipf` | The skew of the load across tenants. | 1.0 |
| `--max-batch-rows`, `--batch-age-ms` | How large a batch grows while its tenant's last insert runs, and how long an event waits for its batch. | 20,000, 2,000 |
| `--operations`, `--waiting-workers` | Operations at a time, and workers kept started ahead of them. | 16, 8 |
| `--compactions`, `--part-threshold`, `--grace` | Tenants compacted at a time, the active parts a partition may hold, and the seconds a covered part stays. | 4, 8, 3 |
| `--queried-tenants`, `--fleet-servers`, `--checked-tenants` | Tenants queried through a fleet, each fleet's servers, and tenants the invariant checker reads. | 4, 2, 20 |
| `--stock-sample` | Tenants on which the run ends by measuring the stock recipe: boot a shared writer, insert one batch, stop. | 3 |
| `--setups` | Tenants set up at a time. | 8 |
| `--kill-one-in` | Sets a kill timer of 20 to 320 ms on one insert in this many. A worker that has yet to answer when its timer runs out is killed, and its batch goes again. | none |
| `--free-queue-wait` | Seconds the run waits for the store's free queue before it counts the checked tenants' blobs. | 600 |
| `--skip-last-housekeeping` | Ends without the last housekeeping pass and counts the covered parts that remain, for a store on which that pass cannot finish. | off |
| `--leave-tenants` | Ends without removing the tenants, for a stack that is wiped afterwards. | off |

## The recipe

Each step names the libch call that does it. [`src/run.rs`](src/run.rs) holds them in this order.

1. **Set a tenant up once.** A tenant is a directory in an InodeDB store and an S3 prefix (`Tenant`). Its schema is DDL, which takes an exclusive writer: `stock::Exclusive::run` boots a stock server on the tenant's disk, runs the statements and stops. The demo's schema is an events table with a projection and a materialized view into an `AggregatingMergeTree` table. Every table keeps deduplication tokens (`non_replicated_deduplication_window`), which is what makes a retried batch land once.

2. **Batch per tenant, and give each batch an id.** Events gather in a buffer for each tenant (`load::Buffer`). A buffer becomes a batch when it holds `rows_per_batch` rows or its oldest event is `max_batch_age` old. The batch's id is its deduplication token.

3. **Insert each batch as an operation.** `keys::KeyCache` mints the tenant a short-lived write key, `pool::Pool` hands the batch to a worker that started before the batch existed, and `Writer::insert` returns once the batch's parts are committed in the table and in every view's target. The worker attaches the tenant's tables without loading their parts, so an insert costs the same on a tenant of one part as on a tenant of a thousand.

4. **Keep one insert in flight for each tenant.** Rows that arrive while a tenant's insert runs go out together as its next batch, up to `max_batch_rows`. A busy tenant's batches grow with its load, and its inserts never contend for a block number. When more tenants are due than operations are free, the largest backlog goes first: the rows a tenant holds, plus a batch's worth for each batch age its oldest row has waited. A busy tenant then keeps up with its load, and a quiet tenant's rows rise to the front as they wait.

5. **Send a failed batch again under the same token.** An insert whose worker died, or outlived its deadline, may or may not have committed. The same bytes under the same token commit what is missing and nothing twice: the retry ends `Committed`, or `Duplicate` when the first attempt had finished.

6. **Compact from a count of batches.** The run counts the batches each tenant has taken. At `part_threshold` it reads the tenant's catalog from InodeDB (`Catalog::crowded_tables`), which names the tables with a partition over the threshold at the cost of one listing a table. For each, `Writer::plan` proposes merges with ClickHouse's own selector, with each merge's rows and bytes, and `Writer::merge` runs one. A merge that finds its sources taken ends `ClaimLost`, and the next plan proposes what is left. Compactions run a few at a time, and the tenant that has taken the most batches since its last one goes first, so the busiest tenant's parts merge as fast as it makes them.

7. **Housekeep after the grace period.** A merge leaves its sources in place for the queries that started before it. `Writer::housekeep` removes covered parts older than `old_parts_lifetime`, and whatever a killed worker left in the table's directory. The run housekeeps a tenant's merged tables one grace period after each compaction, so covered parts and their blobs go while ingest continues, and housekeeps a table again when a call reports covered parts or freed blobs left.

8. **Query through servers that belong to no tenant.** `stock::Blank::boot_many` boots read-only servers with no disk. For a query the run mints a read key rooted at the tenant's directory, `stock::Fleet::assign` gives the servers the tenant's disk and attaches its tables, the query runs across them as parallel replicas, and the servers are discarded. This is how BigHouse serves a query, and a tenant written by operations needs nothing else.

9. **Check.** `Catalog::rows` totals a table's rows from its parts' own counts, with no ClickHouse process, and `check::tenant` reads a disk's invariants from InodeDB and S3: no two active parts intersect, block numbers are unique and at or below the counter, and every blob a file names exists.

## A run

`demo/run gate --tmpfs` on the dev machine: 1,000 tenants take 100,000,000 rows at 100,000 a second, with a Zipf skew of 1.0, 16 operations at a time, and the InodeDB store on tmpfs.

```
1000 tenants, 94542 batches of 1058 rows on average and 20000 at most, 100000000 rows (100000000 expected): exact
steps: setup 23.3 s, ingest 1028.5 s (92 batches/s, 97233 rows/s), compaction 148.9 s, verification 1.5 s, queries 19.5 s, removal 0.0 s
inserts: 94542 answered; worker CPU each: mean 50.2 ms, median 49.7 ms, p99 61.4 ms; InodeDB commits each 2.05, calls each 22.1; S3 writes each 0.14; peak worker memory 36 MB
retries: 0 attempts failed, 0 batches found committed when sent again, 0 counter conflicts
compaction: 9699 plans, 11491 merges, 0 claims lost; worker CPU for each plan or merge 61.4 ms; housekeeping removed 194658 covered parts and left 0 (50.7 ms each run)
partitions by active parts at the end: 826 with 1, 265 with 2, 292 with 3, 106 with 4, 129 with 5, 152 with 6, 136 with 7, 94 with 8
checked: every tenant's rows; 100 tenants by the invariant checker; 100 tenants through fleets
blobs of the checked tenants: 0 that nothing names (0.0 MB), 21 of freed files awaiting the next housekeeping
stock recipe on 3 of these tenants (boot a shared writer, insert one batch, stop): CPU 153 ms and 393 ms from start to exit on average, with 6 parts to load; an insert through a worker: CPU 50.2 ms
pool: 125309 workers started, 684 operations found none waiting, 0 waiting workers lost; keys: 1000 minted, 0 revoked
```

- **The run kept pace with its load.** The busiest tenant draws 13% of the rows and sent batches of up to 20,000; a quiet tenant sent a few rows when its turn came. Both cost a worker about 50 ms of CPU for each insert.
- **Compaction ran beside ingest.** 11,491 merges and 194,658 covered parts removed left every partition at or under the threshold of 8 parts.
- **The stock recipe costs three times the CPU on a tenant of six parts,** and its cost grows with the parts a tenant holds, where an operation's stays flat.
- **The store sets the insert rate.** The machine was mostly idle during the run, and InodeDB's single writer was busy.

[BENCHMARKS.md](../BENCHMARKS.md) sets this run beside the same load with the store on the drive, the same with insert workers killed mid-operation, and a run of 10,000 tenants.
