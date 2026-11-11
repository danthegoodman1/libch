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
