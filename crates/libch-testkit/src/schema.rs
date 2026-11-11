//! The schema and batches the conformance scenarios use.

use bytes::Bytes;
use libch::writer::TableName;

/// The events table: partitioned by month, with a projection, and a materialized view into a daily aggregate.
///
/// Every table keeps deduplication tokens, so a retried batch lands once in the table and once in the view's
/// target.
pub const EVENTS_SCHEMA: [&str; 4] = [
    "CREATE DATABASE d",
    "CREATE TABLE d.events (ts DateTime, kind LowCardinality(String), v UInt64, PROJECTION by_kind (SELECT kind, sum(v) GROUP BY kind)) \
     ENGINE = MergeTree PARTITION BY toYYYYMM(ts) ORDER BY (kind, ts) SETTINGS non_replicated_deduplication_window = 10000",
    "CREATE TABLE d.daily (day Date, kind LowCardinality(String), total AggregateFunction(sum, UInt64)) \
     ENGINE = AggregatingMergeTree ORDER BY (day, kind) SETTINGS non_replicated_deduplication_window = 10000",
    "CREATE MATERIALIZED VIEW d.daily_mv TO d.daily AS SELECT toDate(ts) AS day, kind, sumState(v) AS total FROM d.events GROUP BY day, kind",
];

pub fn events() -> TableName {
    TableName::new("d", "events")
}

pub fn daily() -> TableName {
    TableName::new("d", "daily")
}

/// One batch of events in `JSONEachRow`, with what a reader must find of it.
#[derive(Clone, Debug)]
pub struct Batch {
    pub token: String,
    pub data: Bytes,
    pub rows: u64,
    /// The sum of the batch's `v` column.
    pub sum: u64,
}

/// Batch number `index` of `rows` events. Every row of every batch has a `v` of its own, all rows fall in
/// November 2026, and `kind` cycles through `a`, `b` and `c`.
pub fn batch(index: u64, rows: u64) -> Batch {
    batch_at(index, rows, 0)
}

/// As [`batch`], with every row moved `months` months on, into a partition of its own month.
pub fn batch_at(index: u64, rows: u64, months: u32) -> Batch {
    let month = 11 + months;
    let (year, month) = (2026 + (month - 1) / 12, (month - 1) % 12 + 1);
    let mut data = String::with_capacity(rows as usize * 64);
    let mut sum = 0;
    for row in 0..rows {
        let v = index * rows + row;
        let second = v % 86_400;
        let day = 1 + (v / 86_400) % 28;
        let kind = ["a", "b", "c"][(row % 3) as usize];
        data.push_str(&format!(
            "{{\"ts\":\"{year}-{month:02}-{day:02} {:02}:{:02}:{:02}\",\"kind\":\"{kind}\",\"v\":{v}}}\n",
            second / 3600,
            second / 60 % 60,
            second % 60
        ));
        sum += v;
    }
    Batch { token: format!("batch-{index}-{months}"), data: Bytes::from(data), rows, sum }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batches_are_distinct_and_sum_their_rows() {
        let first = batch(0, 4);
        let second = batch(1, 4);
        assert_eq!((first.rows, first.sum), (4, 6));
        assert_eq!((second.rows, second.sum), (4, 4 + 5 + 6 + 7));
        assert_ne!(first.token, second.token);
        let text = std::str::from_utf8(&second.data).unwrap();
        assert_eq!(text.lines().count(), 4);
        assert_eq!(text.lines().next().unwrap(), r#"{"ts":"2026-11-01 00:00:04","kind":"a","v":4}"#);
    }

    #[test]
    fn later_months_roll_into_the_next_year() {
        let january = batch_at(0, 1, 2);
        assert!(std::str::from_utf8(&january.data).unwrap().contains("2027-01-01"));
        assert_ne!(january.token, batch(0, 1).token);
    }
}
