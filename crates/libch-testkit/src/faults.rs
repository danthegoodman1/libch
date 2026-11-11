//! Faults between a worker and its InodeDB store: a proxy that stands before the store and, for one chosen
//! call, loses the answer or answers as a standby does.
//!
//! The proxy forwards bytes and reads only HTTP/2's frame headers, which tell it where a call's request ends
//! and where its answer does. It counts the calls whose requests end, which leaves out the one stream a writer
//! keeps open for its lease.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::OwnedReadHalf;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

/// What the proxy does to the call it was armed for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fault {
    /// The store runs the call, and the connection drops before the answer reaches the caller, who cannot tell
    /// whether the call ran.
    LostAnswer,
    /// The caller is told it reached a standby, with the store's own address as the writer's. The store runs
    /// the call as well, as a writer that lost its place just after may have.
    NotWriter,
}

const CLIENT_PREFACE: usize = 24;
const FRAME_HEADER: usize = 9;
const DATA: u8 = 0;
const HEADERS: u8 = 1;
const END_STREAM: u8 = 0x1;
const END_HEADERS: u8 = 0x4;

#[derive(Default)]
struct State {
    /// Calls whose requests ended since the proxy was armed.
    calls: AtomicU64,
    /// Which of them meets the fault, counted from one. Zero for none.
    armed_for: AtomicU64,
    /// `Fault` as a number: 1 for a lost answer, 2 for a standby's.
    fault: AtomicU32,
    /// Faults delivered.
    delivered: AtomicU64,
}

/// A proxy before an InodeDB store. Dropping it stops it.
pub struct FaultyStore {
    /// The proxy's address, to give a tenant as its store.
    pub endpoint: String,
    state: Arc<State>,
    accepting: JoinHandle<()>,
}

impl FaultyStore {
    /// Starts a proxy before the store at `upstream`, as `127.0.0.1:7170`.
    pub async fn before(upstream: &str) -> FaultyStore {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("a port for the proxy");
        let endpoint = listener.local_addr().expect("the proxy's address").to_string();
        let state = Arc::new(State::default());
        let (upstream, shared) = (upstream.to_string(), state.clone());
        let accepting = tokio::spawn(async move {
            while let Ok((client, _)) = listener.accept().await {
                tokio::spawn(connection(client, upstream.clone(), shared.clone()));
            }
        });
        FaultyStore { endpoint, state, accepting }
    }

    /// Arms the proxy: the `call`-th call from now, counted from one, meets `fault`. The count starts again.
    pub fn arm(&self, call: u64, fault: Fault) {
        self.state.calls.store(0, Ordering::SeqCst);
        self.state.fault.store(if fault == Fault::LostAnswer { 1 } else { 2 }, Ordering::SeqCst);
        self.state.armed_for.store(call, Ordering::SeqCst);
    }

    /// Lets every call through, and starts the count again.
    pub fn disarm(&self) {
        self.state.armed_for.store(0, Ordering::SeqCst);
        self.state.calls.store(0, Ordering::SeqCst);
    }

    /// The calls whose requests ended since the proxy was last armed or disarmed.
    pub fn calls(&self) -> u64 {
        self.state.calls.load(Ordering::SeqCst)
    }

    /// The faults delivered since the proxy started.
    pub fn delivered(&self) -> u64 {
        self.state.delivered.load(Ordering::SeqCst)
    }
}

impl Drop for FaultyStore {
    fn drop(&mut self) {
        self.accepting.abort();
    }
}

struct Frame {
    kind: u8,
    flags: u8,
    stream: u32,
    /// The header and the payload, as they go on.
    bytes: Vec<u8>,
}

impl Frame {
    /// Whether this frame is the last its sender puts on its stream.
    fn ends_its_stream(&self) -> bool {
        self.stream != 0 && matches!(self.kind, DATA | HEADERS) && self.flags & END_STREAM != 0
    }
}

async fn read_frame(from: &mut OwnedReadHalf) -> std::io::Result<Frame> {
    let mut bytes = vec![0u8; FRAME_HEADER];
    from.read_exact(&mut bytes).await?;
    let length = u32::from_be_bytes([0, bytes[0], bytes[1], bytes[2]]) as usize;
    let (kind, flags) = (bytes[3], bytes[4]);
    let stream = u32::from_be_bytes([bytes[5], bytes[6], bytes[7], bytes[8]]) & 0x7fff_ffff;
    bytes.resize(FRAME_HEADER + length, 0);
    from.read_exact(&mut bytes[FRAME_HEADER..]).await?;
    Ok(Frame { kind, flags, stream, bytes })
}

/// A header as HPACK writes one it keeps out of its table: both strings whole, each under 127 bytes.
fn literal_header(name: &str, value: &str) -> Vec<u8> {
    let mut field = vec![0x00, name.len() as u8];
    field.extend_from_slice(name.as_bytes());
    field.push(value.len() as u8);
    field.extend_from_slice(value.as_bytes());
    field
}

/// A gRPC answer that is trailers alone: the call failed because this server is a standby, and `writer` is the
/// server to ask.
fn standby_answer(stream: u32, writer: &str) -> Vec<u8> {
    // `:status: 200` is entry 8 of HPACK's static table.
    let mut block = vec![0x88];
    block.extend(literal_header("content-type", "application/grpc"));
    block.extend(literal_header("grpc-status", "14"));
    block.extend(literal_header("grpc-message", "not the writer"));
    block.extend(literal_header("inodedb-error", "not-writer"));
    block.extend(literal_header("inodedb-writer", &format!("http://{writer}")));
    let mut frame = (block.len() as u32).to_be_bytes()[1..].to_vec();
    frame.extend([HEADERS, END_STREAM | END_HEADERS]);
    frame.extend(stream.to_be_bytes());
    frame.extend(block);
    frame
}

/// Forwards one connection until either side closes it or a lost answer cuts it.
async fn connection(client: TcpStream, upstream: String, state: Arc<State>) {
    let Ok(store) = TcpStream::connect(&upstream).await else { return };
    let _ = (client.set_nodelay(true), store.set_nodelay(true));
    let (mut from_client, to_client) = client.into_split();
    let (mut from_store, mut to_store) = store.into_split();
    let to_client = Arc::new(tokio::sync::Mutex::new(to_client));
    // The stream whose answer the caller never gets, and whether the connection drops once that answer ends.
    let withheld = Arc::new(AtomicU32::new(0));
    let cut = Arc::new(AtomicU32::new(0));

    let requests = {
        let (state, withheld, cut, to_client) = (state.clone(), withheld.clone(), cut.clone(), to_client.clone());
        async move {
            let mut preface = [0u8; CLIENT_PREFACE];
            from_client.read_exact(&mut preface).await?;
            to_store.write_all(&preface).await?;
            loop {
                let frame = read_frame(&mut from_client).await?;
                let mut standby = false;
                if frame.ends_its_stream() {
                    let call = state.calls.fetch_add(1, Ordering::SeqCst) + 1;
                    if call == state.armed_for.load(Ordering::SeqCst) {
                        state.armed_for.store(0, Ordering::SeqCst);
                        state.delivered.fetch_add(1, Ordering::SeqCst);
                        withheld.store(frame.stream, Ordering::SeqCst);
                        standby = state.fault.load(Ordering::SeqCst) == 2;
                        cut.store(u32::from(!standby), Ordering::SeqCst);
                    }
                }
                to_store.write_all(&frame.bytes).await?;
                if standby {
                    to_client.lock().await.write_all(&standby_answer(frame.stream, &upstream)).await?;
                }
            }
            #[allow(unreachable_code)]
            Ok::<(), std::io::Error>(())
        }
    };
    let answers = async move {
        loop {
            let frame = read_frame(&mut from_store).await?;
            let held = withheld.load(Ordering::SeqCst);
            if held != 0 && frame.stream == held {
                // The store has run the call once its answer ends, and the caller learns nothing of it.
                if frame.ends_its_stream() && cut.load(Ordering::SeqCst) == 1 {
                    return Ok::<(), std::io::Error>(());
                }
                continue;
            }
            to_client.lock().await.write_all(&frame.bytes).await?;
        }
    };
    // Returning drops both connections.
    tokio::select! {
        _ = requests => {}
        _ = answers => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_standby_answer_is_one_headers_frame_that_ends_its_stream() {
        let frame = standby_answer(7, "127.0.0.1:7170");
        let length = u32::from_be_bytes([0, frame[0], frame[1], frame[2]]) as usize;
        assert_eq!(frame.len(), FRAME_HEADER + length);
        assert_eq!((frame[3], frame[4]), (HEADERS, END_STREAM | END_HEADERS));
        assert_eq!(u32::from_be_bytes([frame[5], frame[6], frame[7], frame[8]]), 7);
        assert_eq!(frame[FRAME_HEADER], 0x88);
        let text = String::from_utf8_lossy(&frame[FRAME_HEADER..]);
        assert!(text.contains("inodedb-error") && text.contains("not-writer") && text.contains("http://127.0.0.1:7170"), "{text}");
    }

    #[test]
    fn a_literal_header_carries_both_lengths() {
        assert_eq!(literal_header("a", "bc"), [0x00, 1, b'a', 2, b'b', b'c']);
    }
}
