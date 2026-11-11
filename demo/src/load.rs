//! The demo's load: events for tenants drawn from a Zipf distribution, buffered per tenant, and cut into batches
//! that each carry the token a retry repeats.

use std::collections::VecDeque;
use std::time::Instant;

use bytes::Bytes;

/// Draws tenants: tenant `i` in proportion to `1 / (i + 1)^exponent`, the same sequence for the same seed.
pub struct Zipf {
    cumulative: Vec<f64>,
    state: u64,
}

impl Zipf {
    pub fn new(tenants: usize, exponent: f64, seed: u64) -> Zipf {
        let mut total = 0.0;
        let cumulative = (0..tenants)
            .map(|tenant| {
                total += 1.0 / ((tenant + 1) as f64).powf(exponent);
                total
            })
            .collect();
        Zipf { cumulative, state: seed | 1 }
    }

    /// A number in `[0, 1)`.
    pub fn unit(&mut self) -> f64 {
        self.state ^= self.state << 13;
        self.state ^= self.state >> 7;
        self.state ^= self.state << 17;
        (self.state >> 11) as f64 / (1u64 << 53) as f64
    }

    pub fn tenant(&mut self) -> usize {
        let total = *self.cumulative.last().expect("at least one tenant");
        let point = self.unit() * total;
        self.cumulative.partition_point(|&reached| reached <= point).min(self.cumulative.len() - 1)
    }
}

/// A batch of one tenant's events in `JSONEachRow`.
#[derive(Clone, Debug)]
pub struct Batch {
    /// The batch's id, which is its deduplication token: a batch sent again lands once.
    pub token: String,
    pub data: Bytes,
    pub rows: usize,
}

/// One tenant's events that have yet to be sent, and what the tenant holds once every batch has landed.
#[derive(Debug, Default)]
pub struct Buffer {
    /// Events in chunks of at most a batch's rows, oldest first.
    chunks: VecDeque<(String, usize)>,
    rows: usize,
    since: Option<Instant>,
    next_value: u64,
    next_batch: u64,
    /// The rows generated for the tenant, and the sum of their `v` column.
    pub expected_rows: u64,
    pub expected_sum: u64,
}

impl Buffer {
    pub fn rows(&self) -> usize {
        self.rows
    }

    /// When the oldest buffered event arrived.
    pub fn since(&self) -> Option<Instant> {
        self.since
    }

    /// Adds the tenant's next event. Every event of a tenant has a `v` of its own, and all fall in one month.
    pub fn push(&mut self, chunk_rows: usize) {
        let v = self.next_value;
        self.next_value += 1;
        let second = v % 2_592_000;
        let kind = ["view", "click", "buy"][(v % 3) as usize];
        if self.chunks.back().is_none_or(|(_, rows)| *rows >= chunk_rows) {
            self.chunks.push_back((String::with_capacity(chunk_rows * 60), 0));
        }
        let (text, rows) = self.chunks.back_mut().expect("a chunk to write to");
        text.push_str(&format!(
            "{{\"ts\":\"2026-11-{:02} {:02}:{:02}:{:02}\",\"kind\":\"{kind}\",\"v\":{v}}}\n",
            1 + second / 86_400,
            second / 3_600 % 24,
            second / 60 % 60,
            second % 60
        ));
        *rows += 1;
        self.rows += 1;
        self.since.get_or_insert_with(Instant::now);
        self.expected_rows += 1;
        self.expected_sum += v;
    }

    /// Cuts a batch of the oldest events, at most `max_rows` of them and at least one chunk.
    pub fn take(&mut self, tenant: usize, max_rows: usize) -> Option<Batch> {
        let (mut data, mut rows) = self.chunks.pop_front()?;
        while self.chunks.front().is_some_and(|(_, next)| rows + next <= max_rows) {
            let (text, more) = self.chunks.pop_front().expect("a chunk was seen");
            data.push_str(&text);
            rows += more;
        }
        self.rows -= rows;
        self.since = if self.chunks.is_empty() { None } else { Some(Instant::now()) };
        let token = format!("tenant-{tenant}-batch-{}", self.next_batch);
        self.next_batch += 1;
        Some(Batch { token, data: Bytes::from(data), rows })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_first_tenants_draw_most_of_the_load() {
        let mut zipf = Zipf::new(100, 1.0, 42);
        let mut drawn = vec![0u32; 100];
        for _ in 0..100_000 {
            drawn[zipf.tenant()] += 1;
        }
        assert!(drawn[0] > drawn[9] && drawn[9] > drawn[99], "{:?}", &drawn[..10]);
        assert!(drawn[0] > 15_000 && drawn[99] > 50, "tenant 0 drew {}, tenant 99 drew {}", drawn[0], drawn[99]);
        assert_eq!(drawn.iter().sum::<u32>(), 100_000);
    }

    #[test]
    fn batches_carry_every_event_once() {
        let mut buffer = Buffer::default();
        for _ in 0..2_500 {
            buffer.push(1_000);
        }
        let first = buffer.take(7, 2_000).unwrap();
        assert_eq!((first.rows, first.token.as_str()), (2_000, "tenant-7-batch-0"));
        let second = buffer.take(7, 2_000).unwrap();
        assert_eq!((second.rows, second.token.as_str()), (500, "tenant-7-batch-1"));
        assert!(buffer.take(7, 2_000).is_none() && buffer.rows() == 0 && buffer.since().is_none());
        let lines = first.data.iter().chain(second.data.iter()).filter(|byte| **byte == b'\n').count();
        assert_eq!((lines, buffer.expected_rows, buffer.expected_sum), (2_500, 2_500, 2_499 * 2_500 / 2));
    }
}
