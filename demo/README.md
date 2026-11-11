# libch-demo: the high-tenant recipe

`libch-demo` ingests and compacts many tenants' analytics tables with short-lived operations alone. No process outlives an operation, so one pool of workers serves every tenant, and a tenant that sends nothing costs nothing. The tables are stock MergeTree tables on [BigHouse](https://github.com/danthegoodman1/BigHouse) disks: read-only ClickHouse servers query them as they query any BigHouse tenant.

## Run it

Start libch's dev stack and build the fork's binary as the [repository README](../README.md) describes. Then:

```sh
cargo run --release -p libch-demo -- --tenants 100 --batches 2000 --events-per-second 50000
```

The run sets its tenants up, ingests, compacts, checks every tenant, queries a sample, prints a report, and removes the tenants. It exits with status zero when every tenant ends with exactly the rows the generator made. `cargo test -p libch-demo` runs two small runs, one of which kills a fifth of its insert workers.

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
| `--kill-one-in` | Kills the worker of one insert in this many, mid-operation. | none |

## The recipe

Each step names the libch call that does it. [`src/run.rs`](src/run.rs) holds them in this order.

1. **Set a tenant up once.** A tenant is a directory in an InodeDB store and an S3 prefix (`Tenant`). Its schema is DDL, which takes an exclusive writer: `stock::Exclusive::run` boots a stock server on the tenant's disk, runs the statements and stops. The demo's schema is an events table with a projection and a materialized view into an `AggregatingMergeTree` table. Every table keeps deduplication tokens (`non_replicated_deduplication_window`), which is what makes a retried batch land once.

2. **Batch per tenant, and give each batch an id.** Events gather in a buffer for each tenant (`load::Buffer`). A buffer becomes a batch when it holds `rows_per_batch` rows or its oldest event is `max_batch_age` old. The batch's id is its deduplication token.

3. **Insert each batch as an operation.** `keys::KeyCache` mints the tenant a short-lived write key, `pool::Pool` hands the batch to a worker that started before the batch existed, and `Writer::insert` returns once the batch's parts are committed in the table and in every view's target. The worker attaches the tenant's tables without loading their parts, so an insert costs the same on a tenant of one part as on a tenant of a thousand.

4. **Keep one insert in flight for each tenant.** Rows that arrive while a tenant's insert runs go out together as its next batch, up to `max_batch_rows`. A busy tenant's batches grow with its load, its inserts never contend for a block number, and a quiet tenant's rows wait no longer than the batch age.

5. **Send a failed batch again under the same token.** An insert whose worker died, or outlived its deadline, may or may not have committed. The same bytes under the same token commit what is missing and nothing twice: the retry ends `Committed`, or `Duplicate` when the first attempt had finished.

6. **Compact from a count of batches.** The run counts the batches each tenant has taken. At `part_threshold` it reads the tenant's catalog from InodeDB (`Catalog::crowded_tables`), which names the tables with a partition over the threshold at the cost of one listing a table. For each, `Writer::plan` proposes merges with ClickHouse's own selector, with each merge's rows and bytes, and `Writer::merge` runs one. A merge that finds its sources taken ends `ClaimLost`, and the next plan proposes what is left.

7. **Housekeep after the grace period.** A merge leaves its sources in place for the queries that started before it. `Writer::housekeep` removes covered parts older than `old_parts_lifetime`, and whatever a killed worker left in the table's directory.

8. **Query through servers that belong to no tenant.** `stock::Blank::boot_many` boots read-only servers with no disk. For a query the run mints a read key rooted at the tenant's directory, `stock::Fleet::assign` gives the servers the tenant's disk and attaches its tables, the query runs across them as parallel replicas, and the servers are discarded. This is how BigHouse serves a query, and a tenant written by operations needs nothing else.

9. **Check.** `Catalog::rows` totals a table's rows from its parts' own counts, with no ClickHouse process, and `check::tenant` reads a disk's invariants from InodeDB and S3: no two active parts intersect, block numbers are unique and at or below the counter, and every blob a file names exists.

## A run

One hundred tenants and two million rows, arriving at 50,000 events a second with a Zipf skew of 1.0, on the dev machine while it ran other work:

```
100 tenants, 894 batches of 2237 rows on average and 20000 at most, 2000000 rows (2000000 expected): exact
steps: setup 51.5 s, ingest 50.4 s (18 batches/s, 39665 rows/s), compaction 32.1 s, verification 0.1 s, queries 1.7 s
inserts: 894 answered; worker CPU each: mean 50.6 ms, median 50.2 ms, p99 59.2 ms; InodeDB commits each 2.56, calls each 30.8; S3 writes each 0.24; peak worker memory 38 MB
retries: 0 attempts failed (0 workers killed by the run), 0 batches found committed when sent again, 0 counter conflicts
compaction: 115 plans, 115 merges, 0 claims lost; worker CPU for each plan or merge 47.8 ms; housekeeping removed 1143 covered parts and left 0 (46.4 ms each run)
partitions by active parts at the end: 111 with 1, 2 with 2, 2 with 5, 8 with 6, 29 with 7, 48 with 8
checked: every tenant's rows; 100 tenants by the invariant checker; 10 tenants through fleets
pool: 1250 workers started, 6 operations found none waiting, 0 waiting workers lost; keys: 100 minted, 0 revoked
```

The busiest tenant took a tenth of the load in batches of up to 20,000 rows, and the quietest sent a few hundred rows every two seconds. Both cost a worker about 50 ms of CPU for each insert. Setup is the slowest step for each tenant: one stock process for its DDL.
