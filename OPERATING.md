# Operating libch

What a service built on libch runs, what each call can answer, and what stays with a stock ClickHouse writer.

## The parts

| Part | What it is | Who runs it |
| --- | --- | --- |
| An InodeDB store | The file tree of every tenant's disk, and the coordination files writers share. | You. |
| An S3 bucket | The blobs of every file too large for InodeDB, under a prefix for each tenant. | You. |
| The fork's `clickhouse` binary | The worker (`clickhouse libch-worker`) and the stock server, in one program. | libch starts both. |
| A pool of workers | Processes that each serve one operation on one tenant. | `libch::pool::Pool`. |
| A stock exclusive writer | A ClickHouse server that owns a tenant's disk while it runs DDL or a mutation. | `libch::stock::Exclusive`, when you call it. |
| Read-only servers | ClickHouse servers that attach a tenant under a read key and answer queries. | `libch::stock::Fleet`, or BigHouse's fleets. |

A tenant is a directory in a store and a prefix in a bucket (`libch::Tenant`). Nothing runs for a tenant between operations.

## Keys

Every operation runs under an InodeDB key rooted at its tenant's directory, so the worker that holds it reaches that tenant alone. `KeyCache` mints a tenant's key on first use, hands the same key out until it nears its expiry, and keeps the number of keys it holds in the store under `KeyPolicy::capacity`.

- Set `KeyPolicy::min_remaining` to at least your longest operation's deadline: an operation holds its key from start to end.
- `KeyCache::revoke` fences every operation that still holds the tenant's key. Its next commit ends `Fenced`.
- A key that expires mid-operation fences it the same way.
- Give each tenant S3 credentials scoped to its prefix. A worker holds the credentials its request names and looks for no others: libch starts it with an empty environment and points the AWS SDK away from credential files and the metadata service.

## The pool

`PoolOptions::waiting` workers stay started and wait for a request. A waiting worker holds about 19 MB of its own memory in two threads, and has seen no tenant. `PoolOptions::concurrency` bounds the operations that run at once; further calls wait their turn.

A worker serves one operation and exits. With `WorkerCommand::forking`, a worker is a parent that started the engine once and serves each operation in a child it forks, so an operation pays for a fork where a worker of its own pays for the engine's start. A forking parent serves one operation at a time, so keep as many waiting as operations run at once. The parent leaves each request on its pipe for the child to read, and holds no tenant's key however many operations it has served.

Each worker gets a directory of its own under `WorkerCommand::scratch`, which libch makes and removes.

## Operations and their outcomes

| Operation | What it does | Safe to send again because |
| --- | --- | --- |
| `insert` | Commits a batch to a table and to the target of every materialized view that reads from it. | The batch's token: what is committed stays, and what is missing commits. |
| `plan` | Proposes merges with ClickHouse's selector, each with its sources, rows and bytes. Runs none and claims nothing. | It changes nothing. |
| `merge` | Merges named parts of one partition into one. | Claims on the sources and conditions on the commit: one merge of a source commits. |
| `housekeep` | Removes covered parts past the grace period, expired tokens, dead writers' claims, old temporary directories, and the blobs of what the disk freed. | It removes only what a committed part covers or nothing names. |
| `describe` | Lists the tenant's tables, views and parts as the worker's engine sees them. | It changes nothing. |

| Outcome | Meaning | What to do |
| --- | --- | --- |
| `Committed` | The work is durable. | Go on. |
| `Duplicate` | An insert found every part of its batch committed. | Go on: an earlier attempt landed. |
| `Fenced` | An exclusive writer took the disk, or the key was revoked or ran out. Nothing committed. | Send it again with a fresh key once the exclusive writer is done. |
| `SchemaChanged` | The table's definition changed after the worker loaded it. Nothing committed. | Send it again. |
| `ClaimLost` | A merge found a source gone, covered, or held by another merge. Nothing committed. | Plan again. |
| `Refused` | The request asks for something an operation does not do. The message says what. | Fix the request, or use a stock writer. |
| `Err` | The worker failed, died, or passed its deadline. The work may or may not have committed. | Send it again: every operation is safe to repeat. |

An insert through a view may commit the table's part and die before the view's. Sent again under its token, it commits the view's part alone.

## Compaction

The caller schedules. A recipe that works:

1. Count the batches each tenant takes. Every few batches, call `Catalog::crowded_tables(threshold)`, which costs one listing a table and no ClickHouse process.
2. For each crowded table, `plan`, then `merge` each candidate. A plan's cost grows with the table's parts (87 ms of CPU at 100, 289 ms at 500), so plan the tables the catalog names and keep them compacted.
3. After the grace period (`old_parts_lifetime`), `housekeep` each table that merged. Covered parts stay that long for queries that began before the merge.
4. When `Housekept::blobs_remain` is set, housekeep again: the call stopped with freed files still coming.

`plan` runs ClickHouse's selector under the settings a stock writer has. It merges a range of parts that holds about five times its largest part, and brings that bar down over an hour as the parts age, so it takes hundreds of small parts to a handful at once and leaves a few parts of like size for later. To bring a partition to one part sooner, give the writer `min_age_to_force_merge_seconds` in its settings (`<merge_tree>`): parts older than that merge whatever their sizes.

A killed merge holds its sources until its claims run out (`inodedb_claim_ttl_ms`); the next plan leaves them out, and a later one proposes them again. Housekeeping removes the temporary directory it left.

InodeDB holds a freed file's blobs back while any reader that started before the file was freed still holds its lease. A lease of a killed process lasts 30 seconds and a dead consumer's claim on freed files 60, so blobs freed around a kill go a minute or two later, on a later `housekeep`.

## Limits

- **Deadline.** `OpWriter::with_deadline`: the worker is killed when it passes. The outcome is an `Err`.
- **Memory.** `OpWriter::with_memory_limit`: an operation that would pass it fails with ClickHouse's `MEMORY_LIMIT_EXCEEDED` (code 241) before its commit.
- **Limits on a table's parts move to you.** A server delays or refuses an insert by the parts a table holds: `parts_to_delay_insert`, `parts_to_throw_insert`, `max_parts_in_total`, `inactive_parts_to_delay_insert`, `inactive_parts_to_throw_insert`, `max_avg_part_size_for_too_many_parts`, `dead_blobs_to_delay_insert` and `dead_blobs_to_throw_insert`. A worker loads no part, so none fires. Read the counts from `Catalog::crowded_tables` and hold a tenant back yourself.
- **Limits on the batch stay.** `max_partitions_per_insert_block`, constraints, `max_insert_block_size` and the format's own limits apply as on a server.
- **One insert in flight for each table is cheapest.** Inserts into one table take block numbers from one counter, and concurrent ones retry on conflict. Gather a busy tenant's rows into its next batch.

## What goes to a stock exclusive writer

`Exclusive::run` boots a stock server on the tenant's disk as its only writer, runs statements and stops. Use it for:

- every DDL statement: `CREATE`, `ALTER`, `DROP`, `RENAME`;
- mutations (`ALTER ... UPDATE`, `ALTER ... DELETE`), lightweight updates and deletes, and partition commands;
- finishing a mutation that is pending: until it is done, `plan` and `merge` leave the parts it applies to alone;
- tables with patch parts.

An exclusive writer's start fences every running operation of the tenant, and an operation's start ends the exclusive writer's epoch. Run no operation on a tenant while its exclusive writer works, and send again what it fenced.

## What a worker refuses

- A view that reads rows from the disk beside the rows it is fed: a join or `IN` over a table of the tenant. A worker loads no parts, so the view would find none.
- A view whose query names a table function or reads a dictionary. A worker provides neither, so no view reaches outside the tenant's disk.
- A table, or a view's target, that keeps no deduplication tokens (`non_replicated_deduplication_window = 0`), unless the writer was built with `at_least_once`.
- A table whose engine reaches outside the disk (`URL`, `S3`, `File`, `Kafka` and the like) fails the operations that reach it, and no other.

## The sandbox

`WorkerCommand::sandboxed` has each worker confine itself before it starts its engine, with Landlock and a seccomp filter. A sandboxed worker:

- reads the few system files an engine needs and the paths you add, writes under its own directory alone, and executes nothing;
- connects by TCP to the ports you name alone (the store's and S3's), listens on none, and opens no Unix socket;
- signals and traces no other process;
- starts threads and no process;
- makes none of the system calls that reach around these rules.

It needs Linux 6.12 (Landlock ABI 6). A worker asked for a sandbox on an older kernel fails where it starts. Entering the sandbox costs under a millisecond and needs no privilege, no user namespace and no helper program.

Landlock allows connections by port. Which hosts a worker reaches on those ports is for the network the pool runs in to say: give the pool's hosts an egress policy that names the store and S3.

`WorkerCommand::probe` starts a sandboxed worker that serves no request and reports what it could reach. Run it once where you deploy.

Serve tenants who do not trust each other from sandboxed workers. Without the sandbox, a worker that a bug in the engine let a tenant take over could do what its user can do: read other workers' files, or, in a forking pool, leave a process behind that reads the next request on the parent's pipe.

## What kills leave behind

A worker killed mid-operation commits nothing or everything, and the same request sent again finishes the work. What it can leave:

- **Blobs nothing names.** An insert killed after it uploaded a part's blobs and before its commit, a merge killed before its commit, and a merge that lost its claims or was fenced each leave what they uploaded in S3. Small parts keep their files in InodeDB (up to 16 KB a file) and leave nothing.
- **A temporary directory** in the table, from a merge killed before its commit. `housekeep` removes it after `temporary_directories_lifetime`.
- **Claims** on a dead merge's sources, which run out.

`check::orphans` counts a tenant's blobs that neither a file nor a freed file names. No writer removes them; a sweep that deletes them must first see the tenant's tree and its free queue at one version, as BigHouse's plan for its orphan sweep describes, because a blob uploaded and not yet committed looks the same.

## Checking a tenant

`check::tenant` reads InodeDB and S3 and runs beside live writers: no two active parts intersect, block numbers are unique and at or below the counter, every part has its files, and every blob a file names exists at its size. `check::stock_load` has a fresh stock exclusive writer load the disk and run `CHECK TABLE`; it fences other writers, so run it when they have stopped.
