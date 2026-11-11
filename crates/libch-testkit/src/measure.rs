//! Measurements of ClickHouse processes.
//!
//! libch starts each stock server with its watchdog off, so a server's pid names the server itself. A server
//! started with the watchdog on forks, and its started pid then names the watchdog.

use std::time::Duration;

/// CPU time, user and system, of every child this process has waited for.
pub fn children_cpu() -> Duration {
    // SAFETY: `getrusage` fills the struct it is given and reads nothing else.
    let usage = unsafe {
        let mut usage: libc::rusage = std::mem::zeroed();
        libc::getrusage(libc::RUSAGE_CHILDREN, &mut usage);
        usage
    };
    timeval(usage.ru_utime) + timeval(usage.ru_stime)
}

fn timeval(time: libc::timeval) -> Duration {
    Duration::from_secs(time.tv_sec as u64) + Duration::from_micros(time.tv_usec as u64)
}

/// CPU time, user and system, a running process has used.
pub fn process_cpu(pid: u32) -> Option<Duration> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    // The command name may hold spaces and parentheses; the fields after its closing parenthesis are fixed.
    let fields: Vec<&str> = stat.rsplit_once(')')?.1.split_whitespace().collect();
    let ticks: u64 = fields.get(11)?.parse::<u64>().ok()? + fields.get(12)?.parse::<u64>().ok()?;
    // SAFETY: `sysconf` reads a system constant.
    let per_second = unsafe { libc::sysconf(libc::_SC_CLK_TCK) } as u64;
    Some(Duration::from_micros(ticks * 1_000_000 / per_second))
}

/// A process's own memory: its anonymous resident pages, in megabytes. Pages mapped from the binary are shared
/// by every process that runs it, and are left out.
pub fn own_memory_mb(pid: u32) -> Option<u64> {
    status_kb(pid, "RssAnon").map(|kb| kb / 1024)
}

/// The most resident memory a process has held, in megabytes.
pub fn peak_memory_mb(pid: u32) -> Option<u64> {
    status_kb(pid, "VmHWM").map(|kb| kb / 1024)
}

fn status_kb(pid: u32, field: &str) -> Option<u64> {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    let line = status.lines().find(|line| line.starts_with(field) && line[field.len()..].starts_with(':'))?;
    line.split_whitespace().nth(1)?.parse().ok()
}

/// How many threads a process runs.
pub fn thread_count(pid: u32) -> Option<usize> {
    Some(std::fs::read_dir(format!("/proc/{pid}/task")).ok()?.count())
}

/// The machine's one-minute load average.
pub fn load_average() -> f64 {
    std::fs::read_to_string("/proc/loadavg").ok().and_then(|text| text.split_whitespace().next()?.parse().ok()).unwrap_or(0.0)
}

/// The median of a set of durations.
pub fn median(mut values: Vec<Duration>) -> Duration {
    assert!(!values.is_empty(), "a median needs a value");
    values.sort();
    values[values.len() / 2]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn this_process_has_cpu_memory_and_threads() {
        let pid = std::process::id();
        let mut spin = 0u64;
        for i in 0..20_000_000u64 {
            spin = spin.wrapping_add(i * i);
        }
        assert!(spin > 0);
        assert!(process_cpu(pid).unwrap() > Duration::ZERO);
        assert!(own_memory_mb(pid).is_some());
        assert!(peak_memory_mb(pid).unwrap() >= own_memory_mb(pid).unwrap());
        assert!(thread_count(pid).unwrap() >= 1);
        assert!(process_cpu(u32::MAX).is_none());
    }

    #[test]
    fn children_cpu_counts_a_waited_child() {
        let before = children_cpu();
        let status = std::process::Command::new("sh").arg("-c").arg("i=0; while [ $i -lt 200000 ]; do i=$((i+1)); done").status().unwrap();
        assert!(status.success());
        assert!(children_cpu() > before);
    }

    #[test]
    fn median_takes_the_middle() {
        let ms = Duration::from_millis;
        assert_eq!(median(vec![ms(9), ms(1), ms(5)]), ms(5));
        assert_eq!(median(vec![ms(4)]), ms(4));
        assert!(load_average() >= 0.0);
    }
}
