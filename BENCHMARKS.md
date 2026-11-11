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
