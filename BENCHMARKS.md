# Benchmarks

## Stock baselines

What it costs to write a tenant's disk through stock ClickHouse processes. libch's operations are measured against these. `cargo bench --bench stock` writes this section.

- **Machine:** Intel(R) Core(TM) Ultra 9 285, 24 threads, load average 1.5 at the start.
- **Build:** BigHouse `3e11a9c`, fork `5c449f16e`, exclusive mode, each server's watchdog off.
- **Date:** 10 November 2026.
- **Method:** the median of 5 runs. CPU is user plus system time of the ClickHouse process. Memory is the process's own anonymous memory.

| Measurement | CPU | Wall | Own memory, threads |
| --- | --- | --- | --- |
| Stock server: boot, assign, one 1,000-row insert, stop; 10 parts a table | 206 ms | 270 ms | 143 MB, 202 |
| Stock server: boot, assign, one 1,000-row insert, stop; 100 parts a table | 432 ms | 340 ms | 184 MB, 255 |
| Stock server: boot, assign, one 1,000-row insert, stop; 500 parts a table | 1146 ms | 438 ms | 224 MB, 287 |
| `clickhouse local`: one process for the same insert; 10 parts a table | 132 ms | 151 ms |  |
| `clickhouse local`: one process for the same insert; 100 parts a table | 359 ms | 197 ms |  |
| `clickhouse local`: one process for the same insert; 500 parts a table | 1141 ms | 301 ms |  |
| A blank server already running: assignment through the first insert; 10 parts a table |  | 91 ms |  |
| 1,000 JSON rows into a running writer, through the projection and the view | 5.9 ms | 14.5 ms |  |
| The same on one thread | 5.4 ms | 17.5 ms |  |
| 1,000 JSON rows into a running writer's plain `MergeTree` table | 4.9 ms | 8.0 ms |  |
| The same on one thread | 2.2 ms | 7.8 ms |  |
| `clickhouse local` idle inside a query |  |  | 35 MB, 27 |

The 1,000-row insert goes through a projection and a materialized view into an `AggregatingMergeTree` table, except in the plain-table rows. A tenant of N parts a table holds N unmerged parts in the events table and N in the view's target. The running-writer rows have merges stopped, start from empty tables, and leave out the writer's idle CPU. Every statement takes a new HTTP connection.
