//! The protocol between the libch crate and a worker: the messages of `proto/libch.proto`, and the frames
//! that carry them.
//!
//! A frame is a 4-byte little-endian length, then that many bytes. A request and a response are each one
//! frame holding a message. An insert's batch follows its request as frames of raw bytes, and a frame of
//! length zero ends it.

use std::io::{self, Read, Write};

pub use prost::Message;

include!(concat!(env!("OUT_DIR"), "/libch.v1.rs"));

/// The size of a frame's length prefix.
pub const FRAME_HEADER: usize = 4;

/// The largest frame either side accepts. A length above it means the stream holds something other than
/// frames.
pub const MAX_FRAME: usize = 64 << 20;

/// The size of the data frames a batch is cut into.
pub const DATA_FRAME: usize = 1 << 20;

/// What a forking worker writes on its stdout after each child, where a frame's length would stand: a number no
/// frame's length can be. The child's wait status follows it, four bytes, least significant first.
pub const FORK_TRAILER: [u8; FRAME_HEADER] = [0xff; FRAME_HEADER];

/// What a forking worker writes on its stderr after each child: the record separator, which ends that child's
/// log.
pub const FORK_LOG_END: u8 = 0x1e;

/// A message as one frame.
pub fn message_frame(message: &impl Message) -> Vec<u8> {
    let length = message.encoded_len();
    let mut frame = Vec::with_capacity(FRAME_HEADER + length);
    frame.extend_from_slice(&(length as u32).to_le_bytes());
    message.encode(&mut frame).expect("a vector grows to hold the message");
    frame
}

/// Writes bytes as one frame.
pub fn write_frame(out: &mut impl Write, bytes: &[u8]) -> io::Result<()> {
    out.write_all(&(bytes.len() as u32).to_le_bytes())?;
    out.write_all(bytes)
}

/// Reads one frame. `None` when the stream ends before a frame begins.
pub fn read_frame(input: &mut impl Read) -> io::Result<Option<Vec<u8>>> {
    let mut header = [0u8; FRAME_HEADER];
    let mut filled = 0;
    while filled < FRAME_HEADER {
        match input.read(&mut header[filled..])? {
            0 if filled == 0 => return Ok(None),
            0 => return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "the stream ends inside a frame's length")),
            read => filled += read,
        }
    }
    let length = frame_length(header)?;
    let mut bytes = vec![0u8; length];
    input.read_exact(&mut bytes)?;
    Ok(Some(bytes))
}

/// The length a frame's header gives, when it is one a frame may have.
pub fn frame_length(header: [u8; FRAME_HEADER]) -> io::Result<usize> {
    let length = u32::from_le_bytes(header) as usize;
    if length > MAX_FRAME {
        return Err(io::Error::new(io::ErrorKind::InvalidData, format!("a frame of {length} bytes is over the limit of {MAX_FRAME}")));
    }
    Ok(length)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_message_round_trips_through_a_frame() {
        let request = Request {
            tenant: Some(Tenant { inodedb_endpoint: "127.0.0.1:7170".into(), inodedb_key: "1.abc".into(), s3: None }),
            settings_xml: "<merge_tree/>".into(),
            operation: Some(request::Operation::Insert(Insert {
                table: Some(TableName { database: "d".into(), table: "events".into() }),
                format: "JSONEachRow".into(),
                token: "batch-1".into(),
                at_least_once: true,
            })),
        };
        let frame = message_frame(&request);
        assert_eq!(u32::from_le_bytes(frame[..4].try_into().unwrap()) as usize, frame.len() - 4);
        let read = read_frame(&mut frame.as_slice()).unwrap().unwrap();
        assert_eq!(Request::decode(read.as_slice()).unwrap(), request);
    }

    #[test]
    fn frames_follow_one_another_and_the_stream_ends_cleanly() {
        let mut stream = Vec::new();
        write_frame(&mut stream, b"first").unwrap();
        write_frame(&mut stream, b"").unwrap();
        write_frame(&mut stream, &[7u8; 70_000]).unwrap();
        let mut input = stream.as_slice();
        assert_eq!(read_frame(&mut input).unwrap().as_deref(), Some(&b"first"[..]));
        assert_eq!(read_frame(&mut input).unwrap().as_deref(), Some(&b""[..]));
        assert_eq!(read_frame(&mut input).unwrap().map(|frame| frame.len()), Some(70_000));
        assert_eq!(read_frame(&mut input).unwrap(), None);
    }

    #[test]
    fn a_stream_that_ends_inside_a_frame_is_an_error() {
        let mut stream = Vec::new();
        write_frame(&mut stream, b"whole frame").unwrap();
        for cut in [2, 4, 9] {
            let error = read_frame(&mut &stream[..cut]).unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof, "cut at {cut}");
        }
    }

    #[test]
    fn a_length_over_the_limit_is_refused() {
        let header = ((MAX_FRAME + 1) as u32).to_le_bytes();
        assert_eq!(frame_length(header).unwrap_err().kind(), io::ErrorKind::InvalidData);
        assert_eq!(frame_length((MAX_FRAME as u32).to_le_bytes()).unwrap(), MAX_FRAME);
        // Text where a frame belongs reads as an absurd length.
        assert!(read_frame(&mut &b"Code: 60. DB::Exception"[..]).is_err());
    }
}
