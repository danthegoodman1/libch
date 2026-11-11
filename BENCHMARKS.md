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

- **Build:** fork `5b2688fab`; load average 1.6 at the start, on the machine of the stock baselines.
- **Date:** 11 November 2026.
- **Method:** the median of 10 runs. CPU, memory, threads and InodeDB calls are the worker's own counts, from its start to its answer. Wall time runs from the call to its answer: with a worker the call starts, and through a worker that was started and waiting.

| Operation | Worker CPU | Wall, own worker | Wall, waiting worker | Worker memory, threads | InodeDB commits, calls |
| --- | --- | --- | --- | --- | --- |
| A worker started and waiting for a request | 20 ms |  |  | 19 MB, 2 |  |
| `describe`; 10 parts a table | 47 ms | 49 ms | 22 ms | 30 MB, 33 | 0, 19 |
| `insert` of 1,000 rows through the projection and the view; 10 parts a table | 54 ms | 67 ms | 37 ms | 33 MB, 34 | 2, 30 |
| `plan`; 10 parts a table | 59 ms | 63 ms | 32 ms | 34 MB, 48 | 0, 85 |
| `describe`; 100 parts a table | 47 ms | 47 ms | 22 ms | 31 MB, 33 | 0, 19 |
| `insert` of 1,000 rows through the projection and the view; 100 parts a table | 54 ms | 67 ms | 36 ms | 33 MB, 34 | 2, 30 |
| `merge` of four parts; 100 parts a table | 53 ms | 71 ms | 39 ms | 32 MB, 33 | 4, 43 |
| `plan`; 100 parts a table | 87 ms | 90 ms | 60 ms | 38 MB, 48 | 0, 265 |
| `describe`; 500 parts a table | 47 ms | 48 ms | 22 ms | 31 MB, 33 | 0, 19 |
| `insert` of 1,000 rows through the projection and the view; 500 parts a table | 54 ms | 67 ms | 36 ms | 33 MB, 34 | 2, 30 |
| `merge` of four parts; 500 parts a table | 54 ms | 70 ms | 41 ms | 32 MB, 33 | 4, 43 |
| `plan`; 500 parts a table | 289 ms | 269 ms | 268 ms | 57 MB, 48 | 0, 1465 |
| `housekeep` with 40 covered parts inside their grace period; 500 parts a table | 52 ms | 117 ms | 87 ms | 31 MB, 33 | 1, 75 |
| `describe`; 10,000 parts a table | 47 ms | 47 ms | 26 ms | 31 MB, 34 | 0, 19 |
| `insert` of 1,000 rows through the projection and the view; 10,000 parts a table | 54 ms | 81 ms | 48 ms | 39 MB, 34 | 2, 30 |
| `merge` of four parts; 10,000 parts a table | 65 ms | 141 ms | 100 ms | 37 MB, 33 | 4, 43 |
| `plan`; 10,000 parts a table | 4720 ms | 4835 ms | 4874 ms | 394 MB, 48 | 0, 29965 |
| `describe`; 300 tables and one part a table | 212 ms | 197 ms | 166 ms | 57 MB, 51 | 0, 1219 |

A tenant of N parts a table holds N unmerged parts of 100 rows in the events table and N in the view's target. An insert writes both tables. A merge takes four of the events table's parts by name. A plan loads every part of its table, as a server's selector needs them, so its cost grows with the table where the other operations' costs stay level.
