//! What a run did, in numbers.

use std::collections::BTreeMap;
use std::fmt;
use std::time::Duration;

use libch::keys::KeyStats;
use libch::pool::PoolStats;
use libch::worker::Usage;

/// One insert by the stock recipe: a stock shared writer booted for a tenant, one batch inserted, the writer
/// stopped.
#[derive(Clone, Copy, Debug)]
pub struct StockInsert {
    /// The active parts the writer loaded.
    pub parts: usize,
    /// The writer's CPU from its start to the insert's answer.
    pub cpu: Duration,
    /// From the writer's start to its exit.
    pub wall: Duration,
}

/// A run's counts, costs and findings.
#[derive(Clone, Debug, Default)]
pub struct Report {
    pub tenants: usize,
    /// How long each step took.
    pub setup: Duration,
    pub ingest: Duration,
    pub compaction: Duration,
    pub verification: Duration,
    pub queries: Duration,
    pub removal: Duration,

    /// Batches that landed, their rows, and the rows of the largest.
    pub batches: u64,
    pub rows: u64,
    pub largest_batch: u64,
    /// Batches whose last attempt found them already committed: an earlier attempt's worker died after its commit.
    pub duplicates: u64,
    /// Attempts whose worker failed or was killed, and the inserts the run set a kill timer on: an insert that
    /// answers before its timer runs out is no failed attempt.
    pub failed_attempts: u64,
    pub kills: u64,
    /// Commits the workers repeated because another insert took the block number first.
    pub counter_conflicts: u64,
    /// What the workers of answered inserts spent, and each insert's CPU in microseconds.
    pub inserts: Usage,
    pub insert_cpu_micros: Vec<u64>,

    pub plans: u64,
    pub merges: u64,
    pub claims_lost: u64,
    /// Plans, merges and housekeepings that ended in an error and went again.
    pub failed_steps: u64,
    /// Covered parts that a run without its last housekeeping pass leaves, and the tenants that hold them.
    pub covered_left: usize,
    pub tenants_with_covered: usize,
    /// What the workers of plans and merges spent.
    pub compactions: Usage,

    /// Covered parts housekeeping removed, and left.
    pub parts_removed: u64,
    pub parts_remaining: u64,
    pub housekeeping: Usage,

    /// The rows the generator made.
    pub rows_expected: u64,
    /// How many partitions end with each count of active parts.
    pub partitions_by_parts: BTreeMap<usize, usize>,
    pub tenants_checked: usize,
    pub tenants_queried: usize,
    /// What S3 holds for the checked tenants beyond the blobs their disks name: blobs nothing names, which
    /// killed workers leave, and blobs of freed files that the next housekeeping removes.
    pub orphan_blobs: usize,
    pub orphan_bytes: u64,
    pub freed_blobs: usize,
    /// Checked tenants whose blobs went uncounted because the store's free queue had yet to catch up.
    pub blobs_uncounted: usize,
    /// How long the run waited for the store's free queue before it counted blobs.
    pub free_queue_wait: Duration,
    /// The stock recipe, measured on a sample of the tenants at the end of the run.
    pub stock_inserts: Vec<StockInsert>,
    /// Everything the run found wrong at its end. An exact run has none.
    pub problems: Vec<String>,

    pub pool: PoolStats,
    pub keys: KeyStats,
}

impl Report {
    pub fn record_insert(&mut self, rows: usize, duplicate: bool, failed: u32, counter_conflicts: u64, usage: Usage) {
        self.batches += 1;
        self.rows += rows as u64;
        self.largest_batch = self.largest_batch.max(rows as u64);
        self.duplicates += u64::from(duplicate);
        self.failed_attempts += u64::from(failed);
        self.counter_conflicts += counter_conflicts;
        self.insert_cpu_micros.extend((usage.cpu.as_micros() as u64).checked_div(usage.operations));
        add(&mut self.inserts, usage);
    }

    pub fn record_compaction(&mut self, plans: u64, merges: u64, claims_lost: u64, usage: Usage) {
        self.plans += plans;
        self.merges += merges;
        self.claims_lost += claims_lost;
        add(&mut self.compactions, usage);
    }

    pub fn record_housekeeping(&mut self, removed: u64, remaining: u64, usage: Usage) {
        self.parts_removed += removed;
        self.parts_remaining += remaining;
        add(&mut self.housekeeping, usage);
    }

    /// Whether every tenant ended with the generator's totals and nothing out of place.
    pub fn is_exact(&self) -> bool {
        self.problems.is_empty() && self.rows == self.rows_expected
    }

    /// The CPU of the insert at `quantile` of the answered inserts, from 0 to 1.
    pub fn insert_cpu(&self, quantile: f64) -> Duration {
        let mut sorted = self.insert_cpu_micros.clone();
        sorted.sort_unstable();
        let at = ((sorted.len().saturating_sub(1)) as f64 * quantile).round() as usize;
        Duration::from_micros(sorted.get(at).copied().unwrap_or(0))
    }
}

fn add(total: &mut Usage, more: Usage) {
    total.operations += more.operations;
    total.inodedb_commits += more.inodedb_commits;
    total.inodedb_stats += more.inodedb_stats;
    total.inodedb_reads += more.inodedb_reads;
    total.inodedb_lists += more.inodedb_lists;
    total.s3_reads += more.s3_reads;
    total.s3_writes += more.s3_writes;
    total.cpu += more.cpu;
    total.peak_anonymous_memory_bytes = total.peak_anonymous_memory_bytes.max(more.peak_anonymous_memory_bytes);
    total.peak_threads = total.peak_threads.max(more.peak_threads);
}

fn each(usage: &Usage, total: u64) -> f64 {
    if usage.operations == 0 { 0.0 } else { total as f64 / usage.operations as f64 }
}

fn millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

impl fmt::Display for Report {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "{} tenants, {} batches of {:.0} rows on average and {} at most, {} rows ({} expected): {}",
            self.tenants,
            self.batches,
            self.rows as f64 / self.batches.max(1) as f64,
            self.largest_batch,
            self.rows,
            self.rows_expected,
            if self.is_exact() { "exact" } else { "NOT exact" }
        )?;
        writeln!(
            f,
            "steps: setup {:.1} s, ingest {:.1} s ({:.0} batches/s, {:.0} rows/s), compaction {:.1} s, verification {:.1} s, queries {:.1} s, removal {:.1} s",
            self.setup.as_secs_f64(),
            self.ingest.as_secs_f64(),
            self.batches as f64 / self.ingest.as_secs_f64().max(0.001),
            self.rows as f64 / self.ingest.as_secs_f64().max(0.001),
            self.compaction.as_secs_f64(),
            self.verification.as_secs_f64(),
            self.queries.as_secs_f64(),
            self.removal.as_secs_f64()
        )?;
        writeln!(
            f,
            "inserts: {} answered; worker CPU each: mean {:.1} ms, median {:.1} ms, p99 {:.1} ms; InodeDB commits each {:.2}, calls each {:.1}; S3 writes each {:.2}; peak worker memory {} MB",
            self.inserts.operations,
            millis(self.inserts.cpu) / self.inserts.operations.max(1) as f64,
            millis(self.insert_cpu(0.5)),
            millis(self.insert_cpu(0.99)),
            each(&self.inserts, self.inserts.inodedb_commits),
            each(&self.inserts, self.inserts.inodedb_calls()),
            each(&self.inserts, self.inserts.s3_writes),
            self.inserts.peak_anonymous_memory_bytes >> 20
        )?;
        write!(f, "retries: {} attempts failed", self.failed_attempts)?;
        if self.kills > 0 {
            write!(f, " (the run set a kill timer on {} inserts, and the others answered first)", self.kills)?;
        }
        writeln!(f, ", {} batches found committed when sent again, {} counter conflicts", self.duplicates, self.counter_conflicts)?;
        write!(
            f,
            "compaction: {} plans, {} merges, {} claims lost; worker CPU for each plan or merge {:.1} ms; housekeeping removed {} covered parts and left {} ({:.1} ms each run)",
            self.plans,
            self.merges,
            self.claims_lost,
            millis(self.compactions.cpu) / self.compactions.operations.max(1) as f64,
            self.parts_removed,
            self.parts_remaining,
            millis(self.housekeeping.cpu) / self.housekeeping.operations.max(1) as f64
        )?;
        if self.failed_steps > 0 {
            write!(f, "; {} plans, merges or housekeepings failed and went again", self.failed_steps)?;
        }
        writeln!(f)?;
        if self.covered_left > 0 {
            writeln!(f, "covered parts left without the last housekeeping pass: {} in {} tenants", self.covered_left, self.tenants_with_covered)?;
        }
        let parts: Vec<String> = self.partitions_by_parts.iter().map(|(parts, partitions)| format!("{partitions} with {parts}")).collect();
        writeln!(f, "partitions by active parts at the end: {}", parts.join(", "))?;
        writeln!(
            f,
            "checked: every tenant's rows; {} tenants by the invariant checker; {} tenants through fleets",
            self.tenants_checked, self.tenants_queried
        )?;
        write!(
            f,
            "blobs of the checked tenants: {} that nothing names ({:.1} MB), {} of freed files awaiting the next housekeeping",
            self.orphan_blobs,
            self.orphan_bytes as f64 / 1e6,
            self.freed_blobs
        )?;
        let waited = self.free_queue_wait.as_secs_f64();
        if self.blobs_uncounted > 0 {
            write!(f, "; {} tenants uncounted, the store's free queue being behind still after {waited:.0} s", self.blobs_uncounted)?;
        } else if waited >= 1.0 {
            write!(f, "; the store's free queue took {waited:.0} s to catch up")?;
        }
        writeln!(f)?;
        if !self.stock_inserts.is_empty() {
            let count = self.stock_inserts.len() as f64;
            let mean = |of: fn(&StockInsert) -> f64| self.stock_inserts.iter().map(of).sum::<f64>() / count;
            writeln!(
                f,
                "stock recipe on {} of these tenants (boot a shared writer, insert one batch, stop): CPU {:.0} ms and {:.0} ms from start to exit on average, with {:.0} parts to load; an insert through a worker: CPU {:.1} ms",
                self.stock_inserts.len(),
                mean(|insert| millis(insert.cpu)),
                mean(|insert| millis(insert.wall)),
                mean(|insert| insert.parts as f64),
                millis(self.inserts.cpu) / self.inserts.operations.max(1) as f64
            )?;
        }
        writeln!(
            f,
            "pool: {} workers started, {} operations found none waiting, {} waiting workers lost; keys: {} minted, {} revoked",
            self.pool.started, self.pool.unpooled, self.pool.lost, self.keys.minted, self.keys.revoked
        )?;
        for problem in &self.problems {
            writeln!(f, "problem: {problem}")?;
        }
        Ok(())
    }
}
