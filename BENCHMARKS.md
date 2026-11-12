# Benchmarks

## Stock baselines

What it costs to write a tenant's disk through stock ClickHouse processes. libch's operations are measured against these. `cargo bench --bench stock` writes this section.

- **Machine:** Intel(R) Core(TM) Ultra 9 285, 24 threads, load average 1.6 at the start.
- **Build:** BigHouse `f218bef`, fork `eb69bcc43`, exclusive mode, each server's watchdog off.
- **Date:** 10 November 2026.
- **Method:** the median of 5 runs. CPU is user plus system time of the ClickHouse process. Memory is the process's own anonymous memory.

| Measurement | CPU | Wall | Own memory, threads |
| --- | --- | --- | --- |
| Stock server: boot, assign, one 1,000-row insert, stop; 10 parts a table | 219 ms | 389 ms | 147 MB, 202 |
| Stock server: boot, assign, one 1,000-row insert, stop; 100 parts a table | 420 ms | 400 ms | 183 MB, 255 |
| Stock server: boot, assign, one 1,000-row insert, stop; 500 parts a table | 1177 ms | 603 ms | 220 MB, 288 |
| `clickhouse local`: one process for the same insert; 10 parts a table | 138 ms | 298 ms |  |
| `clickhouse local`: one process for the same insert; 100 parts a table | 306 ms | 293 ms |  |
| `clickhouse local`: one process for the same insert; 500 parts a table | 901 ms | 322 ms |  |
| A blank server already running: assignment through the first insert; 10 parts a table |  | 153 ms |  |
| 1,000 JSON rows into a running writer, through the projection and the view | 6.1 ms | 16.4 ms |  |
| The same on one thread | 4.2 ms | 14.5 ms |  |
| 1,000 JSON rows into a running writer's plain `MergeTree` table | 5.7 ms | 47 ms |  |
| The same on one thread | 1.9 ms | 7.3 ms |  |
| `clickhouse local` idle inside a query |  |  | 35 MB, 27 |

The 1,000-row insert goes through a projection and a materialized view into an `AggregatingMergeTree` table, except in the plain-table rows. A tenant of N parts a table holds N unmerged parts in the events table and N in the view's target. The running-writer rows have merges stopped, start from empty tables, and leave out the writer's idle CPU. Every statement takes a new HTTP connection.

## Where a stock process spends its time

One `clickhouse local` process, start to exit, with one more step in each row. The difference between two rows is what the step costs.

| Process | CPU | Wall |
| --- | --- | --- |
| The engine alone: `SELECT 1` | 49 ms | 44 ms |
| With an empty tenant's disk attached | 63 ms | 168 ms |
| With the schema's four tables and no parts | 84 ms | 181 ms |
| With 10 parts a table | 128 ms | 195 ms |
| With 10 parts a table and one 1,000-row insert | 146 ms | 279 ms |

## Operations

What libch's operations cost through the fork's worker. Each operation is a process of its own that attaches the tenant, does its work, answers and exits. `cargo bench --bench operations` writes this section.

- **Build:** fork `eef32095f`; load average 1.5 at the start, on the machine of the stock baselines.
- **Date:** 11 November 2026.
- **Method:** the median of 10 runs through workers that each serve one operation, and of 5 through a forking worker. CPU, memory, threads and InodeDB calls are the worker's own counts, from its start to its answer. Wall time runs from the call to its answer: with a worker the call starts, and through a worker that was started and waiting. Through a forking worker, CPU is the child's from its fork to its answer and its parent's for the fork, and wall time is from the call to its answer.

| Operation | Worker CPU | Wall, own worker | Wall, waiting worker | CPU, wall through a forking worker | Worker memory, threads | InodeDB commits, calls |
| --- | --- | --- | --- | --- | --- | --- |
| A worker started and waiting for a request | 30 ms |  |  |  | 24 MB, 3 |  |
| A forking worker started and waiting for a request | 30 ms |  |  |  | 24 MB, 1 |  |
| `describe`; 10 parts a table | 34 ms | 37 ms | 22 ms | 11.2 ms, 12.9 ms | 30 MB, 33 | 0, 11 |
| `insert` of 1,000 rows through the projection and the view; 10 parts a table | 41 ms | 55 ms | 32 ms | 16.7 ms, 28 ms | 32 MB, 33 | 2, 22 |
| `plan`; 10 parts a table | 50 ms | 54 ms | 31 ms | 28 ms, 29 ms | 34 MB, 48 | 0, 92 |
| `describe`; 100 parts a table | 35 ms | 37 ms | 19.5 ms | 10.3 ms, 12.5 ms | 30 MB, 33 | 0, 11 |
| `insert` of 1,000 rows through the projection and the view; 100 parts a table | 41 ms | 55 ms | 32 ms | 16.1 ms, 27 ms | 32 MB, 33 | 2, 22 |
| `merge` of four parts; 100 parts a table | 41 ms | 59 ms | 42 ms | 17.3 ms, 35 ms | 31 MB, 33 | 4, 35 |
| `plan`; 100 parts a table | 71 ms | 76 ms | 49 ms | 48 ms, 53 ms | 37 MB, 48 | 0, 227 |
| `describe`; 500 parts a table | 35 ms | 38 ms | 23 ms | 10.6 ms, 12.3 ms | 30 MB, 33 | 0, 11 |
| `insert` of 1,000 rows through the projection and the view; 500 parts a table | 41 ms | 55 ms | 35 ms | 16.9 ms, 28 ms | 32 MB, 33 | 2, 22 |
| `merge` of four parts; 500 parts a table | 42 ms | 60 ms | 42 ms | 17.1 ms, 32 ms | 32 MB, 33 | 4, 35 |
| `plan`; 500 parts a table | 273 ms | 285 ms | 261 ms | 241 ms, 251 ms | 58 MB, 48 | 0, 1427 |
| `housekeep` with 40 covered parts inside their grace period; 500 parts a table | 40 ms | 52 ms | 33 ms | 16.1 ms, 25 ms | 31 MB, 33 | 1, 92 |
| `describe`; 10,000 parts a table | 35 ms | 38 ms | 23 ms | 10.1 ms, 13.3 ms | 30 MB, 33 | 0, 11 |
| `insert` of 1,000 rows through the projection and the view; 10,000 parts a table | 41 ms | 56 ms | 34 ms | 17.2 ms, 31 ms | 39 MB, 33 | 2, 22 |
| `merge` of four parts; 10,000 parts a table | 52 ms | 75 ms | 52 ms | 30 ms, 50 ms | 37 MB, 33 | 4, 35 |
| `plan`; 10,000 parts a table | 4835 ms | 4993 ms | 4811 ms | 4889 ms, 5020 ms | 389 MB, 48 | 0, 29927 |
| `describe`; 300 tables and one part a table | 108 ms | 113 ms | 90 ms | 84 ms, 89 ms | 46 MB, 33 | 0, 11 |
| `insert` of 1,000 rows through the projection and the view; 300 tables | 49 ms | 64 ms | 40 ms | 26 ms, 40 ms | 34 MB, 33 | 2, 22 |

A tenant of N parts a table holds N unmerged parts of 100 rows in the events table and N in the view's target. An insert writes both tables. A merge takes four of the events table's parts by name. A plan loads every part of its table, as a server's selector needs them, so its cost grows with the table where the other operations' costs stay level. A worker reads every table's definition and builds the tables its operation reaches, so 300 further tables cost an insert the reading of their definitions.

## Demo

`libch-demo`'s large runs, each started by `demo/run` from an empty stack. A run sets its tenants up, takes a load with a Zipf skew of 1.0 across them, compacts and housekeeps beside ingest, checks every tenant's rows against the generator, and queries a sample through read-only fleets of four servers.

- **Build:** fork `eef32095f`, on the machine of the stock baselines: 24 cores, shared by the load generator, the workers, InodeDB and s3proxy.
- **Date:** 11 November 2026.
- **Settings:** 16 operations at a time through one pool of workers that each serve one operation and exit; 8 compactions at a time; a part threshold of 8; a grace period of 3 s; batches of up to 20,000 rows, sent when a tenant holds 1,000 rows or a row has waited 2 s.
- **Store:** one InodeDB server on SQLite. Its file is on the drive, or on tmpfs where the column says so.
- **The 10,000-tenant run** ends without its last housekeeping pass, and so counts the covered parts that remain.

| | `gate` | `kills` | `gate --tmpfs` | `wide --tmpfs` |
| --- | --- | --- | --- | --- |
| Tenants | 1,000 | 1,000 | 1,000 | 10,000 |
| Rows offered, and their rate | 100,000,000 at 100,000 a second | the same | the same | 20,000,000 at 20,000 a second |
| Rows at the end against the generator's | exact | exact | exact | exact |
| Setup | 75.8 s | 75.5 s | 23.3 s | 240.4 s |
| Ingest | 1,012.8 s, 98,741 rows a second | 1,013.5 s, 98,668 rows a second | 1,028.5 s, 97,233 rows a second | 1,318.8 s, 15,166 rows a second |
| Inserts answered | 151,163, 149 a second | 114,897, 113 a second | 94,542, 92 a second | 162,747, 123 a second |
| Rows an insert: mean, largest | 662, 15,907 | 870, 20,000 | 1,058, 20,000 | 123, 20,000 |
| Worker CPU an insert: mean, median, 99th percentile | 49.5, 49.4, 57.0 ms | 49.0, 48.8, 57.2 ms | 50.2, 49.7, 61.4 ms | 50.9, 50.5, 62.4 ms |
| InodeDB commits, calls and S3 writes an insert | 2.03, 22.0, 0.10 | 2.04, 22.1, 0.13 | 2.05, 22.1, 0.14 | 2.31, 22.4, 0.01 |
| Inserts under a kill timer; attempts that died; batches found committed when sent again | none | 11,345; 1,440; 826 | none | none |
| Plans, merges, claims lost | 15,548, 19,342, 0 | 12,135, 14,480, 0 | 9,699, 11,491, 0 | 20,084, 20,085, 0 |
| Worker CPU a plan or merge | 59.7 ms | 59.5 ms | 61.4 ms | 58.5 ms |
| Covered parts housekeeping removed, and left | 316,040, 0 | 238,061, 0 | 194,658, 0 | 53,366, and 268,750 in 8,406 tenants |
| Partitions at the end: in all, with one part, over the threshold | 2,000, 939, 0 | 2,000, 724, 0 | 2,000, 826, 0 | 20,000, 17,912, 0 |
| Last compaction and housekeeping pass | 65.0 s | 151.6 s | 148.9 s | 158.4 s, compaction alone |
| Tenants clean under the invariant checker | 100 of 100 | 100 of 100 | 100 of 100 | 100 of 100 |
| Tenants equal through fleets of four, and the time for all | 100 of 100, 20.6 s | 20 of 20, 4.5 s | 100 of 100, 19.5 s | 20 of 20, 4.9 s |
| Blobs of the checked tenants that nothing names | uncounted | 3, of 0.2 MB | 0 | 0 |
| Store's free queue caught up with the run's removals | behind still after 611 s | after 982 s | at once | at once |
| Stock recipe on three of the tenants: CPU, start to exit | 183 ms, 661 ms | 167 ms, 708 ms | 153 ms, 393 ms | 210 ms, 1,349 ms |
| Workers started | 201,467 | 154,975 | 125,309 | 206,196 |

What the runs show:

- **Rows keep pace with the load in every run.** A tenant has one insert in flight and its rows gather meanwhile, so when operations slow down, batches grow: 662 rows an insert on the drive, 870 with kills, 1,058 on tmpfs.
- **An insert costs a worker 49 to 50 ms of CPU**, with two commits and 22 calls to the store, for the busiest tenant and the quietest alike. The stock recipe on the same tenants, a shared writer booted for one batch, costs 153 to 210 ms of CPU with three to six parts to load, and its cost grows with the tenant's parts.
- **The store's writer sets the insert rate.** With 16 operations in flight an operation takes about 85 ms, against 32 to 36 ms alone. During the tmpfs run the machine was 73% idle and one InodeDB thread was at 90% of a core.
- **A store on tmpfs answers fewer inserts a second than one on the drive, and keeps its free queue caught up.** InodeDB frees removed directories when its writer is idle, and at most ten a second otherwise. A run removes about 300 part directories a second, so on the drive the queue falls many minutes behind and the blobs of removed parts wait in S3 that long.
- **Ten thousand tenants in one store reach the store's limits.** Inserts run at 250 a second until housekeeping begins to remove parts, and at 33 to 42 a second from then on, beside 62 part directories removed a second: InodeDB's writer visits every live key for each removed directory, and the demo holds two keys a tenant. All 20,000,000 rows land, in 22 minutes for a load offered in 17, and every partition ends at or under the threshold. The last housekeeping pass does not finish at this size, on the drive or on tmpfs: each tenant's claim scans the whole store's free queue, and 16 at a time each outlast a deadline of 120 s. `../inodedb/PERF-NOTES-FROM-LIBCH.md` has both observations for InodeDB's own work.
- **Kills leave little.** 1,440 insert workers died mid-operation, 826 of them after their commit. Every batch landed once, and the 100 checked tenants hold 3 blobs that nothing names.

The demo gives turns by need: the tenant with the largest backlog inserts first, and the tenant that has taken the most batches since its last compaction compacts first. Turns in rotation fail this load. The busiest tenant draws 13% of the rows; with one batch a rotation it fell behind until its backlog filled the generator's buffer, and with compactions in arrival order its table ended at 1,921 parts.
