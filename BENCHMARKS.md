# Benchmarks

## Stock baselines

What it costs to write a tenant's disk through stock ClickHouse processes. libch's operations are measured against these. `cargo bench --bench stock` writes this section.

- **Machine:** Intel(R) Core(TM) Ultra 9 285, 24 threads, load average 0.3 at the start.
- **Build:** BigHouse `84ce515`, fork `0bfade026`, exclusive mode, each server's watchdog off.
- **Date:** 13 November 2026.
- **Method:** the median of 5 runs. CPU is user plus system time of the ClickHouse process. Memory is the process's own anonymous memory. Every measured process runs with background merges off (`max_bytes_to_merge_at_max_space_in_pool` = 1), as a process that serves one batch and exits would, and each tenant holds its parts unmerged to the end.

| Measurement | CPU | Wall | Own memory, threads |
| --- | --- | --- | --- |
| Stock server: boot, assign, one 1,000-row insert, stop; 10 parts a table | 205 ms | 238 ms | 145 MB, 204 |
| Stock server: boot, assign, one 1,000-row insert, stop; 100 parts a table | 389 ms | 269 ms | 163 MB, 255 |
| Stock server: boot, assign, one 1,000-row insert, stop; 500 parts a table | 1002 ms | 341 ms | 200 MB, 255 |
| `clickhouse local`: one process for the same insert; 10 parts a table | 126 ms | 106 ms |  |
| `clickhouse local`: one process for the same insert; 100 parts a table | 296 ms | 122 ms |  |
| `clickhouse local`: one process for the same insert; 500 parts a table | 908 ms | 206 ms |  |
| A blank server already running: assignment through the first insert; 10 parts a table |  | 69 ms |  |
| 1,000 JSON rows into a running writer, through the projection and the view | 5.6 ms | 9.9 ms |  |
| The same on one thread | 4.0 ms | 9.3 ms |  |
| 1,000 JSON rows into a running writer's plain `MergeTree` table | 5.1 ms | 5.7 ms |  |
| The same on one thread | 2.0 ms | 4.5 ms |  |
| `clickhouse local` idle inside a query |  |  | 34 MB, 26 |

The 1,000-row insert goes through a projection and a materialized view into an `AggregatingMergeTree` table, except in the plain-table rows. A tenant of N parts a table holds N unmerged parts in the events table and N in the view's target. The running-writer rows have merges stopped, start from empty tables, and leave out the writer's idle CPU. Every statement takes a new HTTP connection.

## Where a stock process spends its time

One `clickhouse local` process, start to exit. Each row adds a step to the one above and answers `SELECT 1`, with background merges off; the insert row runs the insert in place of that query. The last row turns merges on: the process starts merges of the tenant's parts as it attaches them and cancels them as it stops.

| Process | CPU | Wall |
| --- | --- | --- |
| The engine alone: `SELECT 1` | 52 ms | 47 ms |
| With an empty tenant's disk attached | 62 ms | 69 ms |
| With the schema's four tables and no parts | 83 ms | 76 ms |
| With 10 parts a table | 102 ms | 81 ms |
| With 10 parts a table and one 1,000-row insert | 116 ms | 101 ms |
| With 10 parts a table and background merges on | 169 ms | 143 ms |

## Operations

What libch's operations cost through the fork's worker. Each operation is a process of its own that attaches the tenant, does its work, answers and exits. `cargo bench --bench operations` writes this section.

- **Build:** fork `80c26f588`; load average 0.7 at the start, on the machine of the stock baselines.
- **Date:** 12 November 2026.
- **Method:** the median of 10 runs through workers that each serve one operation, and of 5 through a forking worker. CPU, memory, threads and InodeDB calls are the worker's own counts, from its start to its answer. Wall time runs from the call to its answer: with a worker the call starts, and through a worker that was started and waiting. Through a forking worker, CPU is the child's from its fork to its answer and its parent's for the fork, and wall time is from the call to its answer.

| Operation | Worker CPU | Wall, own worker | Wall, waiting worker | CPU, wall through a forking worker | Worker memory, threads | InodeDB commits, calls |
| --- | --- | --- | --- | --- | --- | --- |
| A worker started and waiting for a request | 20 ms |  |  |  | 24 MB, 2 |  |
| A forking worker started and waiting for a request | 20 ms |  |  |  | 24 MB, 1 |  |
| `describe`; 10 parts a table | 34 ms | 35 ms | 16.8 ms | 10.7 ms, 12.6 ms | 30 MB, 33 | 0, 11 |
| `insert` of 1,000 rows through the projection and the view; 10 parts a table | 40 ms | 50 ms | 27 ms | 15.7 ms, 23 ms | 32 MB, 33 | 2, 22 |
| `plan`; 10 parts a table | 50 ms | 54 ms | 30 ms | 26 ms, 28 ms | 34 MB, 48 | 0, 92 |
| `describe`; 100 parts a table | 34 ms | 35 ms | 16.9 ms | 10.3 ms, 11.6 ms | 30 MB, 33 | 0, 11 |
| `insert` of 1,000 rows through the projection and the view; 100 parts a table | 41 ms | 49 ms | 27 ms | 16.7 ms, 24 ms | 32 MB, 33 | 2, 22 |
| `merge` of four parts; 100 parts a table | 41 ms | 53 ms | 31 ms | 16.2 ms, 26 ms | 31 MB, 33 | 4, 35 |
| `plan`; 100 parts a table | 72 ms | 77 ms | 53 ms | 49 ms, 53 ms | 37 MB, 48 | 0, 227 |
| `describe`; 500 parts a table | 34 ms | 35 ms | 17.1 ms | 10.4 ms, 12.3 ms | 30 MB, 33 | 0, 11 |
| `insert` of 1,000 rows through the projection and the view; 500 parts a table | 41 ms | 50 ms | 27 ms | 16.4 ms, 24 ms | 32 MB, 33 | 2, 22 |
| `merge` of four parts; 500 parts a table | 42 ms | 54 ms | 32 ms | 16.3 ms, 26 ms | 31 MB, 33 | 4, 35 |
| `plan`; 500 parts a table | 268 ms | 272 ms | 257 ms | 249 ms, 256 ms | 59 MB, 48 | 0, 1427 |
| `housekeep` with 40 covered parts inside their grace period; 500 parts a table | 40 ms | 47 ms | 24 ms | 16.5 ms, 21 ms | 31 MB, 33 | 1, 92 |
| `describe`; 10,000 parts a table | 34 ms | 35 ms | 17.5 ms | 10.0 ms, 11.8 ms | 30 MB, 33 | 0, 11 |
| `insert` of 1,000 rows through the projection and the view; 10,000 parts a table | 41 ms | 50 ms | 28 ms | 16.0 ms, 23 ms | 39 MB, 33 | 2, 22 |
| `merge` of four parts; 10,000 parts a table | 53 ms | 69 ms | 46 ms | 29 ms, 45 ms | 38 MB, 33 | 4, 35 |
| `plan`; 10,000 parts a table | 4773 ms | 4880 ms | 4948 ms | 4668 ms, 4853 ms | 386 MB, 48 | 0, 29927 |
| `describe`; 300 tables and one part a table | 107 ms | 112 ms | 90 ms | 84 ms, 88 ms | 46 MB, 33 | 0, 11 |
| `insert` of 1,000 rows through the projection and the view; 300 tables | 49 ms | 60 ms | 36 ms | 24 ms, 33 ms | 34 MB, 33 | 2, 22 |

A tenant of N parts a table holds N unmerged parts of 100 rows in the events table and N in the view's target. An insert writes both tables. A merge takes four of the events table's parts by name. A plan loads every part of its table, as a server's selector needs them, so its cost grows with the table where the other operations' costs stay level. A worker reads every table's definition and builds the tables its operation reaches, so 300 further tables cost an insert the reading of their definitions.

## Demo

`libch-demo`'s large runs, each started by `demo/run` from an empty stack. A run sets its tenants up, takes a load with a Zipf skew of 1.0 across them, compacts and housekeeps beside ingest, checks every tenant's rows against the generator, and queries a sample through read-only fleets of four servers.

- **Build:** fork `80c26f588`, on the machine of the stock baselines: 24 cores, shared by the load generator, the workers, InodeDB and s3proxy. Each run had the machine to itself.
- **Date:** 12 November 2026.
- **Settings:** 16 operations at a time through one pool of workers that each serve one operation and exit; 8 compactions at a time; a part threshold of 8; a grace period of 3 s; batches of up to 20,000 rows, sent when a tenant holds 1,000 rows or a row has waited 2 s.
- **Store:** one InodeDB server on SQLite at `eeca4a0`, built inside InodeDB's own repository, where its settings make SQLite sync with `fdatasync`. Its file is on the drive, or on tmpfs where the column says so.

| | `gate` | `wide` | `kills` | `gate --tmpfs` |
| --- | --- | --- | --- | --- |
| Tenants | 1,000 | 10,000 | 1,000 | 1,000 |
| Rows offered, and their rate | 100,000,000 at 100,000 a second | 20,000,000 at 20,000 a second | 100,000,000 at 100,000 a second | the same |
| Rows at the end against the generator's | exact | exact | exact | exact |
| Setup | 56.1 s | 573.8 s | 57.5 s | 27.2 s |
| Ingest | 1,012.0 s, 98,814 rows a second | 1,069.4 s, 18,701 rows a second | 1,011.9 s, 98,829 rows a second | 1,011.0 s, 98,915 rows a second |
| Inserts answered | 152,788, 151 a second | 213,489, 200 a second | 154,502, 153 a second | 173,770, 172 a second |
| Rows an insert: mean, largest | 655, 20,000 | 94, 20,000 | 647, 20,000 | 575, 6,811 |
| Worker CPU an insert: mean, median, 99th percentile | 49.2, 48.9, 58.0 ms | 50.4, 50.0, 60.1 ms | 49.5, 49.2, 58.5 ms | 51.2, 50.8, 61.6 ms |
| InodeDB commits, calls and S3 writes an insert | 2.03, 22.0, 0.10 | 2.19, 22.3, 0.01 | 2.03, 22.1, 0.10 | 2.02, 22.0, 0.09 |
| Inserts under a kill timer; attempts that died; batches found committed when sent again | none | none | 15,419; 2,307; 1,376 | none |
| Plans, merges, claims lost | 16,347, 19,578, 0 | 20,441, 20,442, 0 | 16,572, 19,869, 0 | 17,542, 21,610, 0 |
| Worker CPU a plan or merge | 58.4 ms | 60.0 ms | 58.6 ms | 61.7 ms |
| Covered parts housekeeping removed, and left | 319,242, 0 | 396,871, 0 | 322,475, 0 | 363,285, 0 |
| Partitions at the end: in all, with one part, over the threshold | 2,000, 863, 0 | 20,000, 12,083, 0 | 2,000, 813, 0 | 2,000, 894, 0 |
| Last compaction and housekeeping pass | 40.9 s | 213.8 s | 52.9 s | 35.8 s |
| Tenants clean under the invariant checker | 100 of 100 | 100 of 100 | 100 of 100 | 100 of 100 |
| Tenants equal through fleets of four, and the time for all | 100 of 100, 22.2 s | 20 of 20, 4.8 s | 20 of 20, 4.1 s | 100 of 100, 20.7 s |
| Blobs of the checked tenants that nothing names | 0 | 0 | 3, of 0.1 MB | 0 |
| Store's free queue caught up with the run's removals | at once | after 81 s | at once | at once |
| Stock recipe on three of the tenants: CPU, start to exit | 167 ms, 249 ms | 163 ms, 262 ms | 160 ms, 241 ms | 157 ms, 236 ms |
| Workers started | 204,906 | 274,821 | 209,691 | 230,347 |

What the runs show:

- **Rows keep pace with the load in every run.** A tenant has one insert in flight and its rows gather meanwhile, so batches grow as operations slow: 575 rows an insert on tmpfs at 172 inserts a second, and 655 on the drive at 151.
- **An insert costs a worker 49 to 51 ms of CPU**, with two commits and 22 calls to the store, for the busiest tenant and the quietest alike. The stock recipe on the same tenants, a shared writer booted for one batch, costs 157 to 167 ms of CPU with four to eleven parts to load, and grows with the tenant's parts (section Stock baselines).
- **A housekeep costs a worker 44 to 50 ms of CPU and 21 to 22 calls to the store**: 2 commits, 11 to 12 stats, 2 reads and 6 lists, for the 20 covered parts it removes on average.
- **The store sets the insert rate.** The gate run completes about 200 operations a second through its 16 slots, 80 ms each, where an insert alone takes 27 ms through a waiting worker (section Operations). With the store on tmpfs the same load is answered in 172 inserts a second against 151.
- **The store's sync call is worth a tenth of the insert rate.** A server built outside InodeDB's repository misses its settings and syncs with `fsync`. BigHouse's build script made such a server until 12 November 2026, and on it the gate run answers 135 inserts a second against 151 and sets its tenants up in 82 s against 56 s; the 10,000-tenant run sets up in 831 s against 574 s. On tmpfs the two servers agree.
- **InodeDB keeps its free queue caught up at 1,000 tenants.** The gate run removes about 310 part directories a second beside 151 inserts, and every checked tenant's blobs count at once when it ends, with kills and without. `be669f1` freed at most ten removed directories a second while its writer was busy, and on the drive its queue fell 10 to 16 minutes behind.
- **Ten thousand tenants in one store complete in 33 minutes.** Inserts run at 215 to 250 a second until housekeeping begins to remove parts, and at 140 to 165 a second from then on, beside 310 to 380 part directories removed a second. All 20,000,000 rows land in 18 minutes for a load offered in 17. The last pass takes under 4 minutes: it brings every tenant to the threshold, then housekeeps for under 2 minutes at 56 tenants and 2,300 part directories a second, with the InodeDB server using 1.6 to 2.5 cores. The free queue ends that pass behind: 79 of the 100 checked tenants count within 32 s and the last after 81 s, where an earlier run of the day took 54 s.
- **A removal's cost in the store set the last pass on earlier commits.** On `3f4ff2d` each directory removed set off a walk of every key's root: the pass's housekeeping took 45 minutes at 95 part directories a second with 10,000 keys alive, and inserts beside housekeeping ran at 111 to 128 a second. `1191cde` searches only the key roots that cover the directory: its housekeeping took 15 minutes at 267 a second and left the free queue 411 s behind, which InodeDB traced to freeing the projection's directory inside each `events` part one part at a time. `eeca4a0` frees those directories in shared rounds. On `be669f1` the pass could not finish, and inserts fell to 13 to 26 a second once housekeeping began.
- **Thirty-two operations in flight carry more than 16 from InodeDB `eeca4a0` on.** A shorter run of 10,000 tenants, 8,000,000 rows over 400 s with a part threshold of 2, housekeeps its last pass at 54 tenants and 807 part directories a second with 16 operations in flight, and at 79 tenants and 1,371 part directories a second with `--operations 32`, while inserts rise from 180 a second to 209. The store's free queue is level when the pass ends at 16 and at most 30 s behind at 32, where the pass detaches inodes faster than the store frees them and the queue drains once it ends. On `1191cde` the same pass ran at 494 part directories a second at 16 and at 242 at 32, with the queue 300 s behind. One run each on fork `7fe3ea745`, on a build of InodeDB that counts its own work.
- **Kills leave little.** 2,307 insert workers died mid-operation, 1,376 of them after their commit. Every batch landed once, and the 100 checked tenants hold 3 blobs that nothing names.

The demo gives turns by need: the tenant with the largest backlog inserts first, and the tenant that has taken the most batches since its last compaction compacts first. Turns in rotation fail this load. The busiest tenant draws 13% of the rows; with one batch a rotation it fell behind until its backlog filled the generator's buffer, and with compactions in arrival order its table ended at 1,921 parts.
