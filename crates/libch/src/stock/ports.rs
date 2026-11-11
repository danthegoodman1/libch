//! Ports for ClickHouse servers, leased so that servers started by different processes never share one.

use std::fs::{File, OpenOptions};
use std::io;
use std::net::TcpListener;

use rand::Rng;

/// The range leases come from. It sits below Linux's ephemeral range, so an outgoing connection never takes a
/// leased port, and above the ports BigHouse's own scripts use.
const RANGE: std::ops::Range<u16> = 28_000..32_000;

/// A port held until the lease drops. The lease is a lock on a file named for the port, so it ends with the
/// process that holds it, however that process ends.
#[derive(Debug)]
pub struct PortLease {
    port: u16,
    _lock: File,
}

impl PortLease {
    pub fn port(&self) -> u16 {
        self.port
    }
}

/// Leases a port that no other lease holds and nothing listens on.
pub fn lease() -> io::Result<PortLease> {
    let dir = std::env::temp_dir().join("libch-ports");
    std::fs::create_dir_all(&dir)?;
    let span = RANGE.end - RANGE.start;
    let first = rand::rng().random_range(0..span);
    for offset in 0..span {
        let port = RANGE.start + (first + offset) % span;
        let lock = OpenOptions::new().create(true).truncate(false).write(true).open(dir.join(port.to_string()))?;
        if lock.try_lock().is_err() {
            continue;
        }
        if TcpListener::bind(("127.0.0.1", port)).is_err() {
            continue;
        }
        return Ok(PortLease { port, _lock: lock });
    }
    Err(io::Error::other(format!("every port from {} to {} is leased or in use", RANGE.start, RANGE.end)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn leases_are_distinct_and_free_on_drop() {
        let leases: Vec<PortLease> = (0..32).map(|_| lease().unwrap()).collect();
        let mut ports: Vec<u16> = leases.iter().map(PortLease::port).collect();
        ports.sort_unstable();
        ports.dedup();
        assert_eq!(ports.len(), 32);
        assert!(ports.iter().all(|port| RANGE.contains(port)));

        let port = leases[0].port();
        let path = std::env::temp_dir().join("libch-ports").join(port.to_string());
        let other = OpenOptions::new().write(true).open(&path).unwrap();
        assert!(other.try_lock().is_err(), "the lease holds the lock");
        drop(leases);
        assert!(other.try_lock().is_ok(), "dropping the lease frees the lock");
    }

    #[test]
    fn a_port_in_use_is_skipped() {
        let lease = lease().unwrap();
        let listener = TcpListener::bind(("127.0.0.1", lease.port())).unwrap();
        let port = lease.port();
        drop(lease);
        for _ in 0..64 {
            assert_ne!(super::lease().unwrap().port(), port);
        }
        drop(listener);
    }
}
