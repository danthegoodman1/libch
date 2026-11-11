//! The format of a file's value on an `inodedb` disk, as the BigHouse fork writes it
//! (`InodeDBValue.h` and `DiskObjectStorageMetadata.cpp`).

use crate::Error;

/// What a file's value holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ValueKind {
    /// A [`Record`]: the file's blobs, or its bytes inline.
    Record,
    /// The file's bytes.
    String,
    /// The object key of a blob that holds a record too large for an InodeDB value.
    Spilled,
    /// The record of blobs a transaction dropped from a live file.
    Tombstone,
}

impl ValueKind {
    fn from_byte(byte: u8) -> Option<ValueKind> {
        match byte {
            b'R' => Some(ValueKind::Record),
            b'S' => Some(ValueKind::String),
            b'P' => Some(ValueKind::Spilled),
            b'T' => Some(ValueKind::Tombstone),
            _ => None,
        }
    }

    fn byte(self) -> u8 {
        match self {
            ValueKind::Record => b'R',
            ValueKind::String => b'S',
            ValueKind::Spilled => b'P',
            ValueKind::Tombstone => b'T',
        }
    }
}

/// A file's value: an 11-byte header, then the body.
///
/// ```text
/// byte 0      format version, 1
/// byte 1      kind
/// byte 2      flags: bit 0 is `retained`
/// bytes 3-10  change time, seconds since the epoch, little endian
/// bytes 11-   body
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Value {
    pub kind: ValueKind,
    /// The blobs belong to another owner, so freeing this value deletes none of them.
    pub retained: bool,
    /// Seconds since the epoch.
    pub ctime: u64,
    pub body: Vec<u8>,
}

impl Value {
    pub const FORMAT_VERSION: u8 = 1;
    pub const HEADER_SIZE: usize = 11;

    /// Decodes the value of the file at `path`, which only the error names.
    pub fn decode(bytes: &[u8], path: &str) -> Result<Value, Error> {
        if bytes.len() < Value::HEADER_SIZE || bytes[0] != Value::FORMAT_VERSION {
            return Err(Error::format(path, format!("holds no disk value of format {}", Value::FORMAT_VERSION)));
        }
        let kind = ValueKind::from_byte(bytes[1]).ok_or_else(|| Error::format(path, format!("holds a disk value of unknown kind {}", bytes[1])))?;
        let mut ctime = [0u8; 8];
        ctime.copy_from_slice(&bytes[3..11]);
        Ok(Value { kind, retained: bytes[2] & 1 != 0, ctime: u64::from_le_bytes(ctime), body: bytes[Value::HEADER_SIZE..].to_vec() })
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(Value::HEADER_SIZE + self.body.len());
        bytes.push(Value::FORMAT_VERSION);
        bytes.push(self.kind.byte());
        bytes.push(u8::from(self.retained));
        bytes.extend_from_slice(&self.ctime.to_le_bytes());
        bytes.extend_from_slice(&self.body);
        bytes
    }

    /// The record a `Record` or `Tombstone` value holds.
    pub fn record(&self, path: &str) -> Result<Record, Error> {
        match self.kind {
            ValueKind::Record | ValueKind::Tombstone => Record::parse(&self.body, path),
            kind => Err(Error::format(path, format!("holds a {kind:?} value, which has no record"))),
        }
    }

    /// The file's bytes, when the value itself holds them: a `String` value, or a record with inline data and
    /// no blobs. `None` when the bytes live in S3.
    pub fn inline_bytes(&self, path: &str) -> Result<Option<Vec<u8>>, Error> {
        match self.kind {
            ValueKind::String => Ok(Some(self.body.clone())),
            ValueKind::Record => {
                let record = self.record(path)?;
                Ok(record.blobs.is_empty().then_some(record.inline))
            }
            ValueKind::Spilled | ValueKind::Tombstone => Ok(None),
        }
    }
}

/// One blob of a file.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Blob {
    pub size: u64,
    /// The blob's key as the record stores it: the full object key in a version 5 record, and a path relative
    /// to the disk's key prefix in earlier versions.
    pub key: String,
}

/// A `DiskObjectStorageMetadata` record in its text form.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Record {
    pub version: u32,
    pub blobs: Vec<Blob>,
    pub ref_count: u32,
    pub read_only: bool,
    /// The file's bytes, when the record keeps them and has no blobs.
    pub inline: Vec<u8>,
}

impl Record {
    pub const VERSION_READ_ONLY_FLAG: u32 = 3;
    pub const VERSION_INLINE_DATA: u32 = 4;
    pub const VERSION_FULL_OBJECT_KEY: u32 = 5;

    pub fn parse(body: &[u8], path: &str) -> Result<Record, Error> {
        let mut reader = Reader { bytes: body, at: 0, path };
        let version: u32 = reader.int()?;
        reader.expect(b'\n')?;
        if !(1..=Record::VERSION_FULL_OBJECT_KEY).contains(&version) {
            return Err(Error::format(path, format!("holds a record of unknown version {version}")));
        }
        let count: u32 = reader.int()?;
        reader.expect(b'\t')?;
        let total: u64 = reader.int()?;
        reader.expect(b'\n')?;

        let mut blobs = Vec::with_capacity(count as usize);
        for _ in 0..count {
            let size: u64 = reader.int()?;
            reader.expect(b'\t')?;
            let key = reader.escaped()?;
            reader.expect(b'\n')?;
            let key = String::from_utf8(key).map_err(|_| Error::format(path, "holds a blob key that is not UTF-8"))?;
            blobs.push(Blob { size, key });
        }
        let sum: u64 = blobs.iter().map(|blob| blob.size).sum();
        if sum != total {
            return Err(Error::format(path, format!("records {total} bytes, and its blobs hold {sum}")));
        }

        let ref_count: u32 = reader.int()?;
        reader.expect(b'\n')?;
        let mut read_only = false;
        if version >= Record::VERSION_READ_ONLY_FLAG {
            read_only = match reader.byte()? {
                b'0' => false,
                b'1' => true,
                other => return Err(Error::format(path, format!("holds a read-only flag of {other}"))),
            };
            reader.expect(b'\n')?;
        }
        let mut inline = Vec::new();
        if version >= Record::VERSION_INLINE_DATA {
            inline = reader.escaped()?;
            reader.expect(b'\n')?;
        }
        Ok(Record { version, blobs, ref_count, read_only, inline })
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut out = Vec::new();
        let total: u64 = self.blobs.iter().map(|blob| blob.size).sum();
        out.extend_from_slice(format!("{}\n{}\t{}\n", self.version, self.blobs.len(), total).as_bytes());
        for blob in &self.blobs {
            out.extend_from_slice(format!("{}\t", blob.size).as_bytes());
            escape_into(blob.key.as_bytes(), &mut out);
            out.push(b'\n');
        }
        out.extend_from_slice(format!("{}\n", self.ref_count).as_bytes());
        if self.version >= Record::VERSION_READ_ONLY_FLAG {
            out.extend_from_slice(if self.read_only { b"1\n" } else { b"0\n" });
        }
        if self.version >= Record::VERSION_INLINE_DATA {
            escape_into(&self.inline, &mut out);
            out.push(b'\n');
        }
        out
    }

    /// Each blob's full object key on a disk whose keys start with `key_prefix`.
    pub fn object_keys(&self, key_prefix: &str) -> Vec<String> {
        self.blobs
            .iter()
            .map(|blob| if self.version >= Record::VERSION_FULL_OBJECT_KEY { blob.key.clone() } else { format!("{key_prefix}{}", blob.key) })
            .collect()
    }
}

struct Reader<'a> {
    bytes: &'a [u8],
    at: usize,
    path: &'a str,
}

impl Reader<'_> {
    fn byte(&mut self) -> Result<u8, Error> {
        let byte = *self.bytes.get(self.at).ok_or_else(|| Error::format(self.path, "holds a record that ends early"))?;
        self.at += 1;
        Ok(byte)
    }

    fn expect(&mut self, byte: u8) -> Result<(), Error> {
        let found = self.byte()?;
        if found == byte {
            Ok(())
        } else {
            Err(Error::format(self.path, format!("holds a record with byte {found} where {byte} belongs, at offset {}", self.at - 1)))
        }
    }

    fn int<T: std::str::FromStr>(&mut self) -> Result<T, Error> {
        let start = self.at;
        while self.at < self.bytes.len() && (self.bytes[self.at].is_ascii_digit() || (self.at == start && self.bytes[self.at] == b'-')) {
            self.at += 1;
        }
        std::str::from_utf8(&self.bytes[start..self.at])
            .ok()
            .and_then(|text| text.parse().ok())
            .ok_or_else(|| Error::format(self.path, format!("holds a record with no number at offset {start}")))
    }

    /// Reads a string that ClickHouse's `writeEscapedString` wrote, up to the tab or newline that ends it.
    fn escaped(&mut self) -> Result<Vec<u8>, Error> {
        let mut out = Vec::new();
        while self.at < self.bytes.len() {
            match self.bytes[self.at] {
                b'\t' | b'\n' => break,
                b'\\' => {
                    self.at += 1;
                    let escape = self.byte()?;
                    match escape {
                        b'a' => out.push(0x07),
                        b'b' => out.push(0x08),
                        b'e' => out.push(0x1b),
                        b'f' => out.push(0x0c),
                        b'n' => out.push(b'\n'),
                        b'r' => out.push(b'\r'),
                        b't' => out.push(b'\t'),
                        b'v' => out.push(0x0b),
                        b'0' => out.push(0),
                        b'x' => {
                            let high = hex_digit(self.byte()?);
                            let low = hex_digit(self.byte()?);
                            match (high, low) {
                                (Some(high), Some(low)) => out.push(high << 4 | low),
                                _ => return Err(Error::format(self.path, "holds a record with a malformed \\x escape")),
                            }
                        }
                        other => out.push(other),
                    }
                }
                byte => {
                    out.push(byte);
                    self.at += 1;
                }
            }
        }
        Ok(out)
    }
}

fn hex_digit(byte: u8) -> Option<u8> {
    (byte as char).to_digit(16).map(|digit| digit as u8)
}

/// Escapes as ClickHouse's `writeEscapedString` does.
fn escape_into(bytes: &[u8], out: &mut Vec<u8>) {
    for &byte in bytes {
        match byte {
            0x08 => out.extend_from_slice(b"\\b"),
            0x0c => out.extend_from_slice(b"\\f"),
            b'\n' => out.extend_from_slice(b"\\n"),
            b'\r' => out.extend_from_slice(b"\\r"),
            b'\t' => out.extend_from_slice(b"\\t"),
            0 => out.extend_from_slice(b"\\0"),
            b'\\' => out.extend_from_slice(b"\\\\"),
            b'\'' => out.extend_from_slice(b"\\'"),
            other => out.push(other),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn value_round_trips_through_its_header() {
        let value = Value { kind: ValueKind::String, retained: true, ctime: 1_794_000_123, body: b"ATTACH TABLE".to_vec() };
        let bytes = value.encode();
        assert_eq!(bytes.len(), Value::HEADER_SIZE + 12);
        assert_eq!(&bytes[..3], &[1, b'S', 1]);
        assert_eq!(Value::decode(&bytes, "f").unwrap(), value);
    }

    #[test]
    fn value_rejects_other_formats_and_kinds() {
        assert!(Value::decode(b"short", "f").is_err());
        let mut bytes = Value { kind: ValueKind::Record, retained: false, ctime: 0, body: vec![] }.encode();
        bytes[0] = 2;
        assert!(Value::decode(&bytes, "f").is_err());
        bytes[0] = 1;
        bytes[1] = b'Z';
        let error = Value::decode(&bytes, "tree/x").unwrap_err().to_string();
        assert!(error.contains("tree/x") && error.contains("unknown kind"), "{error}");
    }

    #[test]
    fn record_with_blobs_parses_and_resolves_keys() {
        let body = b"3\n2\t30\n10\tabc/defghij\n20\tklm/nopqrst\n1\n0\n";
        let record = Record::parse(body, "f").unwrap();
        assert_eq!(record.version, 3);
        assert_eq!(record.blobs, vec![Blob { size: 10, key: "abc/defghij".into() }, Blob { size: 20, key: "klm/nopqrst".into() }]);
        assert_eq!((record.ref_count, record.read_only), (1, false));
        assert_eq!(record.object_keys("tenants/t1/"), vec!["tenants/t1/abc/defghij", "tenants/t1/klm/nopqrst"]);
        assert_eq!(record.serialize(), body);
    }

    #[test]
    fn record_with_full_keys_keeps_them() {
        let record = Record::parse(b"5\n1\t7\n7\ttenants/t1/abc/xyz\n0\n0\n\n", "f").unwrap();
        assert_eq!(record.object_keys("ignored/"), vec!["tenants/t1/abc/xyz"]);
    }

    #[test]
    fn record_with_inline_bytes_unescapes_them() {
        let record = Record::parse(b"4\n0\t0\n0\n0\nline one\\nline\\ttwo\\\\\\0\\'q\\'\n", "f").unwrap();
        assert!(record.blobs.is_empty());
        assert_eq!(record.inline, b"line one\nline\ttwo\\\0'q'");
        assert_eq!(Record::parse(&record.serialize(), "f").unwrap(), record);
    }

    #[test]
    fn record_inline_bytes_round_trip_for_every_byte() {
        let inline: Vec<u8> = (0..=255).collect();
        let record = Record { version: 4, blobs: vec![], ref_count: 0, read_only: false, inline };
        assert_eq!(Record::parse(&record.serialize(), "f").unwrap(), record);
    }

    #[test]
    fn record_rejects_a_size_that_disagrees_with_its_blobs() {
        let error = Record::parse(b"3\n1\t11\n10\tabc\n0\n0\n", "tree/p/data.bin").unwrap_err().to_string();
        assert!(error.contains("tree/p/data.bin") && error.contains("11"), "{error}");
    }

    #[test]
    fn record_rejects_truncation() {
        assert!(Record::parse(b"4\n0\t0\n0\n", "f").is_err());
        assert!(Record::parse(b"9\n0\t0\n0\n0\n\n", "f").is_err());
    }

    #[test]
    fn inline_bytes_come_from_strings_and_blobless_records() {
        let string = Value { kind: ValueKind::String, retained: false, ctime: 0, body: b"x".to_vec() };
        assert_eq!(string.inline_bytes("f").unwrap(), Some(b"x".to_vec()));
        let inline = Value { kind: ValueKind::Record, retained: false, ctime: 0, body: b"4\n0\t0\n0\n0\nhi\n".to_vec() };
        assert_eq!(inline.inline_bytes("f").unwrap(), Some(b"hi".to_vec()));
        let blobs = Value { kind: ValueKind::Record, retained: false, ctime: 0, body: b"4\n1\t2\n2\tk\n0\n0\n\n".to_vec() };
        assert_eq!(blobs.inline_bytes("f").unwrap(), None);
    }
}
