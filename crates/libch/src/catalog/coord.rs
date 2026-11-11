//! The coordination files BigHouse's shared writers keep beside a disk's tree.
//!
//! Their values are plain text, with no disk value header.

use inodedb::Version;

use crate::Error;

/// Which kind of writer holds the disk.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OwnerMode {
    /// One writer. Its start fenced every other writer.
    Exclusive,
    /// Writers that insert and merge side by side.
    Shared,
}

/// The disk's writer epoch, from its `owner` file. Every commit of a writer requires the version it started
/// under, so a new epoch fences every writer of the old one.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Owner {
    pub mode: OwnerMode,
    /// The id of the writer that began the epoch.
    pub writer: String,
    pub version: Version,
}

impl Owner {
    pub fn parse(value: &[u8], version: Version) -> Result<Owner, Error> {
        let text = std::str::from_utf8(value).map_err(|_| Error::format("owner", "is not text"))?;
        let (mode, writer) = text.trim_end().split_once(' ').unwrap_or((text.trim_end(), ""));
        let mode = match mode {
            "exclusive" => OwnerMode::Exclusive,
            "shared" => OwnerMode::Shared,
            // A writer older than the shared mode wrote a bare id.
            _ if writer.is_empty() => return Ok(Owner { mode: OwnerMode::Exclusive, writer: mode.to_string(), version }),
            other => return Err(Error::format("owner", format!("names the unknown writer mode `{other}`"))),
        };
        Ok(Owner { mode, writer: writer.to_string(), version })
    }
}

/// A table's block counter: its highest committed block number.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Counter {
    pub value: i64,
    /// The version of the counter's file. An insert replaces the counter under this version.
    pub version: Version,
}

impl Counter {
    pub fn parse(value: &[u8], version: Version, path: &str) -> Result<Counter, Error> {
        let value = std::str::from_utf8(value)
            .ok()
            .and_then(|text| text.trim().parse().ok())
            .ok_or_else(|| Error::format(path, "holds a block counter that is not a decimal number"))?;
        Ok(Counter { value, version })
    }
}

/// Where a merge claim stands.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClaimState {
    /// The merge is running. Other writers leave the part alone until the expiry.
    Held {
        /// Milliseconds since the epoch.
        expiry_ms: u64,
    },
    /// The merge committed. The claim stays until the source part's directory is gone.
    Done,
}

/// A merge claim on one source part.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Claim {
    /// The source part's name.
    pub part: String,
    pub state: ClaimState,
    /// The id of the writer that holds, or held, the claim.
    pub writer: String,
    /// The name of the part the merge writes.
    pub result: String,
    pub version: Version,
    pub mtime_ms: u64,
}

impl Claim {
    /// Parses `held {writer} {expiry ms} {result}` or `done {writer} {result}`.
    pub fn parse(part: &str, value: &[u8], version: Version, mtime_ms: u64, path: &str) -> Result<Claim, Error> {
        let malformed = || Error::format(path, format!("holds the malformed merge claim `{}`", String::from_utf8_lossy(value)));
        let text = std::str::from_utf8(value).map_err(|_| malformed())?;
        let fields: Vec<&str> = text.split(' ').collect();
        let (state, writer, result) = match fields.as_slice() {
            ["held", writer, expiry, result] => (ClaimState::Held { expiry_ms: expiry.parse().map_err(|_| malformed())? }, writer, result),
            ["done", writer, result] => (ClaimState::Done, writer, result),
            _ => return Err(malformed()),
        };
        if writer.is_empty() || result.is_empty() {
            return Err(malformed());
        }
        Ok(Claim { part: part.to_string(), state, writer: writer.to_string(), result: result.to_string(), version, mtime_ms })
    }

    /// Whether the claim keeps other writers off the part at `now_ms`: a `held` claim before its expiry, or a
    /// `done` claim.
    pub fn is_live(&self, now_ms: u64) -> bool {
        match self.state {
            ClaimState::Held { expiry_ms } => now_ms < expiry_ms,
            ClaimState::Done => true,
        }
    }

    /// The claim's value, as a control plane writes one.
    pub fn value(&self) -> String {
        match self.state {
            ClaimState::Held { expiry_ms } => format!("held {} {expiry_ms} {}", self.writer, self.result),
            ClaimState::Done => format!("done {} {}", self.writer, self.result),
        }
    }
}

/// A deduplication token: a block that some insert committed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Token {
    /// The block's id, which ClickHouse derives from the partition and the insert's token.
    pub block_id: String,
    /// The name of the part that took the block.
    pub part: String,
    pub version: Version,
    pub mtime_ms: u64,
}

/// A table's coordination files.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Coordination {
    pub counter: Option<Counter>,
    pub claims: Vec<Claim>,
    pub tokens: Vec<Token>,
}

impl Coordination {
    /// The claim on a part, when it has one.
    pub fn claim(&self, part: &str) -> Option<&Claim> {
        self.claims.iter().find(|claim| claim.part == part)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn owner_names_the_mode_and_the_writer() {
        assert_eq!(Owner::parse(b"shared c10757b7", 9).unwrap(), Owner { mode: OwnerMode::Shared, writer: "c10757b7".into(), version: 9 });
        assert_eq!(Owner::parse(b"exclusive 5849403d", 3).unwrap().mode, OwnerMode::Exclusive);
        let bare = Owner::parse(b"Kq3vT9aLmZ0xYb1c", 1).unwrap();
        assert_eq!((bare.mode, bare.writer.as_str()), (OwnerMode::Exclusive, "Kq3vT9aLmZ0xYb1c"));
        assert!(Owner::parse(b"paused 1234", 1).is_err());
    }

    #[test]
    fn counter_is_a_decimal_number() {
        assert_eq!(Counter::parse(b"3", 44, "p").unwrap(), Counter { value: 3, version: 44 });
        let error = Counter::parse(b"three", 1, "coord/t/block_number").unwrap_err().to_string();
        assert!(error.contains("coord/t/block_number"), "{error}");
    }

    #[test]
    fn claims_parse_in_both_states() {
        let held = Claim::parse("all_1_1_0", b"held f56abe92 1794000000123 all_1_2_1", 5, 7, "p").unwrap();
        assert_eq!(held.state, ClaimState::Held { expiry_ms: 1_794_000_000_123 });
        assert_eq!((held.writer.as_str(), held.result.as_str(), held.part.as_str()), ("f56abe92", "all_1_2_1", "all_1_1_0"));
        assert!(held.is_live(1_794_000_000_122));
        assert!(!held.is_live(1_794_000_000_123));
        assert_eq!(held.value(), "held f56abe92 1794000000123 all_1_2_1");

        let done = Claim::parse("all_1_1_0", b"done f56abe92 all_1_2_1", 5, 7, "p").unwrap();
        assert_eq!(done.state, ClaimState::Done);
        assert!(done.is_live(u64::MAX));
        assert_eq!(done.value(), "done f56abe92 all_1_2_1");
    }

    #[test]
    fn malformed_claims_name_their_file() {
        for value in ["", "held", "held w 12", "held w soon r", "done w", "taken w r", "done  r"] {
            let error = Claim::parse("all_1_1_0", value.as_bytes(), 1, 1, "coord/t/claims/all_1_1_0").unwrap_err().to_string();
            assert!(error.contains("coord/t/claims/all_1_1_0"), "{value}: {error}");
        }
    }
}
