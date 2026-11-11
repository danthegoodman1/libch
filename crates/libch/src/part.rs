//! Part names as ClickHouse's `MergeTreePartInfo` parses them, and the active set a table's parts form.

use std::cmp::Ordering;
use std::fmt;

/// What a part's directory name says: `{partition}_{min block}_{max block}_{level}`, with `_{mutation}` after it
/// once a mutation has rewritten the part.
///
/// The partition id runs up to the name's first underscore. A patch part, which a lightweight update writes,
/// has a partition id that starts with `patch-`, so it shares a partition with no regular part.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PartInfo {
    pub partition_id: String,
    pub min_block: i64,
    pub max_block: i64,
    pub level: u32,
    /// The version of the last mutation applied, or zero.
    pub mutation: i64,
    /// The name spells its level as `u32::MAX`, which ClickHouse reads as [`PartInfo::MAX_LEVEL`].
    legacy_max_level: bool,
}

impl PartInfo {
    /// The level of the covering part that `REPLACE PARTITION` and `DROP PARTITION` leave.
    pub const MAX_LEVEL: u32 = 999_999_999;
    const LEGACY_MAX_LEVEL: u32 = u32::MAX;
    pub const PATCH_PREFIX: &'static str = "patch-";

    pub fn new(partition_id: impl Into<String>, min_block: i64, max_block: i64, level: u32, mutation: i64) -> PartInfo {
        PartInfo { partition_id: partition_id.into(), min_block, max_block, level, mutation, legacy_max_level: false }
    }

    /// Parses a part's directory name. `None` for every other name a table's directory holds, such as
    /// `detached`, `format_version.txt`, `mutation_7.txt` and `tmp_insert_all_1_1_0`.
    pub fn parse(name: &str) -> Option<PartInfo> {
        let (partition_id, rest) = name.split_once('_')?;
        if partition_id.is_empty() {
            return None;
        }
        let mut reader = IntReader { rest };
        let min_block: i64 = reader.signed()?;
        reader.underscore()?;
        let max_block: i64 = reader.signed()?;
        reader.underscore()?;
        let mut level: u32 = reader.unsigned()?;
        if min_block > max_block {
            return None;
        }
        let mut mutation = 0i64;
        if !reader.rest.is_empty() {
            reader.underscore()?;
            mutation = i64::from(reader.unsigned()?);
            if !reader.rest.is_empty() {
                return None;
            }
        }
        let legacy_max_level = level == PartInfo::LEGACY_MAX_LEVEL;
        if legacy_max_level {
            level = PartInfo::MAX_LEVEL;
        }
        Some(PartInfo { partition_id: partition_id.to_string(), min_block, max_block, level, mutation, legacy_max_level })
    }

    /// The part's directory name.
    pub fn name(&self) -> String {
        self.to_string()
    }

    pub fn is_patch(&self) -> bool {
        self.partition_id.starts_with(PartInfo::PATCH_PREFIX)
    }

    /// Mutations with a version above this still apply to the part.
    pub fn data_version(&self) -> i64 {
        if self.mutation != 0 { self.mutation } else { self.min_block }
    }

    /// Whether this part holds everything `other` holds: it came from merging `other` with other parts, or from
    /// mutating it. A part contains itself.
    pub fn contains(&self, other: &PartInfo) -> bool {
        // A containing part may have an equal level only when the block ranges are equal: `all_0_5_2` leaves
        // `all_0_4_2` out, and `all_0_5_3` and `all_0_4_2_9` both hold it.
        let strictly_contains_block_range =
            (self.min_block == other.min_block && self.max_block == other.max_block) || self.level > other.level || self.level == PartInfo::MAX_LEVEL;
        self.partition_id == other.partition_id
            && self.min_block <= other.min_block
            && self.max_block >= other.max_block
            && self.level >= other.level
            && self.mutation >= other.mutation
            && strictly_contains_block_range
    }

    /// Whether the two parts share no block.
    pub fn is_disjoint(&self, other: &PartInfo) -> bool {
        self.partition_id != other.partition_id || self.min_block > other.max_block || self.max_block < other.min_block
    }

    /// Orders a partition's parts so that a part follows every part that can contain it.
    fn sweep_order(&self, other: &PartInfo) -> Ordering {
        self.partition_id
            .cmp(&other.partition_id)
            .then(self.min_block.cmp(&other.min_block))
            .then(other.max_block.cmp(&self.max_block))
            .then(other.level.cmp(&self.level))
            .then(other.mutation.cmp(&self.mutation))
    }
}

impl fmt::Display for PartInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let level = if self.legacy_max_level { PartInfo::LEGACY_MAX_LEVEL } else { self.level };
        write!(f, "{}_{}_{}_{}", self.partition_id, self.min_block, self.max_block, level)?;
        if self.mutation != 0 {
            write!(f, "_{}", self.mutation)?;
        }
        Ok(())
    }
}

/// Reads integers as ClickHouse's `tryReadIntText` does: an optional sign, then at least one digit.
struct IntReader<'a> {
    rest: &'a str,
}

impl IntReader<'_> {
    fn digits(&mut self, signed: bool) -> Option<(bool, &str)> {
        let bytes = self.rest.as_bytes();
        let mut at = 0;
        let mut negative = false;
        match bytes.first() {
            Some(b'+') => at = 1,
            Some(b'-') if signed => {
                negative = true;
                at = 1;
            }
            _ => {}
        }
        let start = at;
        while at < bytes.len() && bytes[at].is_ascii_digit() {
            at += 1;
        }
        if at == start {
            return None;
        }
        let digits = &self.rest[start..at];
        self.rest = &self.rest[at..];
        Some((negative, digits))
    }

    fn signed(&mut self) -> Option<i64> {
        let (negative, digits) = self.digits(true)?;
        let value: i64 = digits.parse().ok()?;
        Some(if negative { -value } else { value })
    }

    fn unsigned(&mut self) -> Option<u32> {
        let (_, digits) = self.digits(false)?;
        digits.parse().ok()
    }

    fn underscore(&mut self) -> Option<()> {
        self.rest = self.rest.strip_prefix('_')?;
        Some(())
    }
}

/// A part that another part contains.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Covered {
    pub part: PartInfo,
    /// The active part that contains it.
    pub by: PartInfo,
}

/// A table's parts sorted into those a reader sees and those a merge or mutation replaced.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PartSet {
    /// The parts no other part contains, by partition and then by block.
    pub active: Vec<PartInfo>,
    pub covered: Vec<Covered>,
    /// Pairs of active parts that share blocks while neither contains the other. A healthy table has none:
    /// ClickHouse refuses to load one that does.
    pub intersections: Vec<(PartInfo, PartInfo)>,
}

impl PartSet {
    /// Sorts parts as ClickHouse's `ActiveDataPartSet` does.
    pub fn of(parts: impl IntoIterator<Item = PartInfo>) -> PartSet {
        let mut parts: Vec<PartInfo> = parts.into_iter().collect();
        parts.sort_by(PartInfo::sweep_order);
        parts.dedup();

        let mut set = PartSet::default();
        // The active parts that reach the block the sweep stands at. A healthy partition keeps one here.
        let mut open: Vec<PartInfo> = Vec::new();
        for part in parts {
            open.retain(|candidate| candidate.partition_id == part.partition_id && candidate.max_block >= part.min_block);
            if let Some(by) = open.iter().find(|candidate| candidate.contains(&part)) {
                set.covered.push(Covered { part, by: by.clone() });
                continue;
            }
            for candidate in &open {
                set.intersections.push((candidate.clone(), part.clone()));
            }
            open.push(part.clone());
            set.active.push(part);
        }
        set
    }

    /// The active parts of one partition, by block.
    pub fn active_in<'a>(&'a self, partition_id: &'a str) -> impl Iterator<Item = &'a PartInfo> + 'a {
        self.active.iter().filter(move |part| part.partition_id == partition_id)
    }

    /// The partitions that hold an active part, in order.
    pub fn partitions(&self) -> Vec<&str> {
        let mut partitions: Vec<&str> = self.active.iter().map(|part| part.partition_id.as_str()).collect();
        partitions.dedup();
        partitions
    }

    /// How many active parts each partition holds, by partition.
    pub fn active_counts(&self) -> Vec<(&str, usize)> {
        let mut counts: Vec<(&str, usize)> = Vec::new();
        for part in &self.active {
            match counts.last_mut() {
                Some((partition, count)) if *partition == part.partition_id => *count += 1,
                _ => counts.push((&part.partition_id, 1)),
            }
        }
        counts
    }
}

#[cfg(test)]
mod tests {
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    use super::*;

    fn part(name: &str) -> PartInfo {
        PartInfo::parse(name).unwrap_or_else(|| panic!("`{name}` is a part name"))
    }

    #[test]
    fn parses_what_clickhouse_parses() {
        assert_eq!(part("all_1_1_0"), PartInfo::new("all", 1, 1, 0, 0));
        assert_eq!(part("202611_3_17_2"), PartInfo::new("202611", 3, 17, 2, 0));
        assert_eq!(part("202611_3_17_2_25"), PartInfo::new("202611", 3, 17, 2, 25));
        assert_eq!(part("-5_10_10_0"), PartInfo::new("-5", 10, 10, 0, 0));
        assert_eq!(part("1b4e28ba2fa1b5a8a5c3f0e9d1c7b2a4_7_9_1"), PartInfo::new("1b4e28ba2fa1b5a8a5c3f0e9d1c7b2a4", 7, 9, 1, 0));
        assert_eq!(part("all_-3_4_1").min_block, -3);
        let patch = part("patch-0123456789abcdef0123456789abcdef-202611_5_5_0");
        assert!(patch.is_patch());
        assert_eq!(patch.partition_id, "patch-0123456789abcdef0123456789abcdef-202611");
        assert!(!part("all_1_1_0").is_patch());
    }

    #[test]
    fn leaves_other_directory_entries_unparsed() {
        for name in [
            "detached",
            "format_version.txt",
            "mutation_7.txt",
            "tmp_insert_all_1_1_0",
            "tmp_merge_all_1_2_1",
            "delete_tmp_all_1_1_0",
            "deduplication_logs",
            "all_1_1",
            "all_1_1_0_",
            "all_1_1_0_2_3",
            "all_2_1_0",
            "_1_1_0",
            "all_a_1_0",
            "all_1_1_0x",
            "all_1_1_-1",
            "all__1_0",
            "",
        ] {
            assert_eq!(PartInfo::parse(name), None, "`{name}`");
        }
    }

    #[test]
    fn names_round_trip() {
        for name in ["all_1_1_0", "202611_3_17_2_25", "-5_10_10_0", "all_0_0_999999999", "all_0_0_4294967295", "all_1_5_3_9"] {
            assert_eq!(part(name).name(), name);
        }
    }

    #[test]
    fn legacy_max_level_reads_as_max_level() {
        let legacy = part("all_0_0_4294967295");
        assert_eq!(legacy.level, PartInfo::MAX_LEVEL);
        assert!(legacy.contains(&part("all_0_0_7")));
    }

    #[test]
    fn data_version_is_the_mutation_or_the_first_block() {
        assert_eq!(part("all_4_9_2").data_version(), 4);
        assert_eq!(part("all_4_9_2_12").data_version(), 12);
    }

    #[test]
    fn containment_follows_clickhouse() {
        let contains = |outer: &str, inner: &str| part(outer).contains(&part(inner));
        assert!(contains("all_0_5_3", "all_0_4_2"));
        assert!(contains("all_0_4_2_9", "all_0_4_2"));
        assert!(!contains("all_0_5_2", "all_0_4_2"), "an equal level needs an equal block range");
        assert!(contains("all_1_1_0", "all_1_1_0"), "a part contains itself");
        assert!(contains("all_1_3_1", "all_2_2_0"));
        assert!(!contains("all_1_3_1", "all_2_2_0_7"), "a merge of unmutated parts leaves a mutated one out");
        assert!(contains("all_1_3_1_7", "all_2_2_0_7"));
        assert!(!contains("all_1_3_1", "all_3_4_0"));
        assert!(!contains("a_1_3_1", "b_2_2_0"));
        assert!(!contains("all_2_2_0", "all_1_3_1"));
        assert!(contains("all_0_9_999999999", "all_3_4_5"), "a drop range holds every level");
    }

    #[test]
    fn disjoint_parts_share_no_block() {
        assert!(part("all_1_2_1").is_disjoint(&part("all_3_3_0")));
        assert!(!part("all_1_3_1").is_disjoint(&part("all_3_4_1")));
        assert!(part("a_1_3_1").is_disjoint(&part("b_1_3_1")));
    }

    fn names(parts: &[PartInfo]) -> Vec<String> {
        parts.iter().map(PartInfo::name).collect()
    }

    #[test]
    fn active_set_drops_merged_and_mutated_parts() {
        let set = PartSet::of(["all_1_1_0", "all_2_2_0", "all_3_3_0", "all_1_2_1", "all_4_4_0", "all_4_4_0_6", "b_5_5_0", "all_7_7_0"].map(part));
        assert_eq!(names(&set.active), ["all_1_2_1", "all_3_3_0", "all_4_4_0_6", "all_7_7_0", "b_5_5_0"]);
        let covered: Vec<(String, String)> = set.covered.iter().map(|c| (c.part.name(), c.by.name())).collect();
        assert_eq!(
            covered,
            [("all_1_1_0", "all_1_2_1"), ("all_2_2_0", "all_1_2_1"), ("all_4_4_0", "all_4_4_0_6")].map(|(a, b)| (a.to_string(), b.to_string()))
        );
        assert!(set.intersections.is_empty());
        assert_eq!(set.partitions(), ["all", "b"]);
        assert_eq!(names(&set.active_in("b").cloned().collect::<Vec<_>>()), ["b_5_5_0"]);
    }

    #[test]
    fn active_set_keeps_the_top_of_a_chain_of_merges() {
        let set = PartSet::of(["all_1_1_0", "all_2_2_0", "all_1_2_1", "all_3_3_0", "all_1_3_2", "all_1_3_2_9"].map(part));
        assert_eq!(names(&set.active), ["all_1_3_2_9"]);
        assert!(set.covered.iter().all(|covered| covered.by.name() == "all_1_3_2_9"));
        assert_eq!(set.covered.len(), 5);
    }

    #[test]
    fn active_set_reports_parts_that_intersect() {
        // A merge of blocks 1 to 3 beside a merge of blocks 3 to 4: neither holds the other.
        let set = PartSet::of(["all_1_3_1", "all_3_4_1", "all_6_6_0"].map(part));
        assert_eq!(names(&set.active), ["all_1_3_1", "all_3_4_1", "all_6_6_0"]);
        assert_eq!(set.intersections, [(part("all_1_3_1"), part("all_3_4_1"))]);

        // A merge that left out its mutated source.
        let set = PartSet::of(["all_1_2_1", "all_1_1_0_5"].map(part));
        assert_eq!(set.intersections.len(), 1);

        // Two parts with one block number in two partitions are separate parts.
        let set = PartSet::of(["a_5_5_0", "b_5_5_0"].map(part));
        assert!(set.intersections.is_empty());
        assert_eq!(set.active.len(), 2);
    }

    /// The slow definition: a part is active when no other part contains it.
    fn naive(parts: &[PartInfo]) -> (Vec<PartInfo>, usize) {
        let mut active: Vec<PartInfo> = parts.iter().filter(|part| !parts.iter().any(|other| other != *part && other.contains(part))).cloned().collect();
        active.sort_by(PartInfo::sweep_order);
        let mut intersections = 0;
        for (index, first) in active.iter().enumerate() {
            intersections += active[index + 1..].iter().filter(|second| !first.is_disjoint(second)).count();
        }
        (active, intersections)
    }

    #[test]
    fn active_set_agrees_with_the_slow_definition_on_random_parts() {
        let mut rng = StdRng::seed_from_u64(0x11bc);
        for round in 0..2_000 {
            let count = rng.random_range(1..14);
            let mut parts: Vec<PartInfo> = (0..count)
                .map(|_| {
                    let min = rng.random_range(0..8);
                    let max = min + rng.random_range(0..4);
                    let partition = if rng.random_bool(0.8) { "a" } else { "b" };
                    let mutation = if rng.random_bool(0.2) { rng.random_range(1..4) } else { 0 };
                    PartInfo::new(partition, min, max, rng.random_range(0..3), mutation)
                })
                .collect();
            parts.sort_by(PartInfo::sweep_order);
            parts.dedup();
            let (active, intersections) = naive(&parts);
            let set = PartSet::of(parts.clone());
            assert_eq!(set.active, active, "round {round}: {:?}", names(&parts));
            assert_eq!(set.intersections.len(), intersections, "round {round}: {:?}", names(&parts));
            assert_eq!(set.active.len() + set.covered.len(), parts.len());
            for covered in &set.covered {
                assert!(covered.by.contains(&covered.part) && set.active.contains(&covered.by));
            }
        }
    }
}
