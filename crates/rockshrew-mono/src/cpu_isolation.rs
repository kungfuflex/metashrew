//! CPU isolation helpers for the indexer/view split.
//!
//! Linux-only. Two knobs:
//!   * **affinity**: pin a thread to a subset of cores via
//!     `sched_setaffinity`. Used to reserve N cores for the indexer
//!     (block-processor + its tokio workers) so view-handler threads
//!     can never steal them, no matter how many concurrent view calls
//!     are in flight.
//!   * **nice**: lower the indexer's scheduling-class nice value (and
//!     raise the view pool's) via `setpriority`. When the two pools
//!     do contend for the same core (e.g. on the boundary core if
//!     affinity isn't perfect), the kernel CFS scheduler gives the
//!     indexer ~28× more CPU time than view threads at a +15 nice
//!     gap. Requires CAP_SYS_NICE in the container.
//!
//! Both helpers are no-ops if the requested config is "off" (cores
//! empty / nice == 0). `set_thread_nice` logs a warn! and continues
//! on EPERM (missing capability) rather than panicking, so the binary
//! still runs in environments where the cap isn't granted.
//!
//! Detection of the pod's effective CPU set reads
//! /proc/self/status's `Cpus_allowed_list` line, which the kernel
//! populates from the container's cgroup cpuset. Falls back to
//! `num_cpus::get()` if the line is missing or unparseable.

use anyhow::Result;
use log::warn;
use std::fs;

#[cfg(target_os = "linux")]
use libc::{
    cpu_set_t, sched_setaffinity, setpriority, CPU_SET, CPU_ZERO, PRIO_PROCESS,
};

/// Read the kernel-effective set of CPUs the calling process is
/// allowed to run on. Returns a sorted Vec of CPU ids (0-based).
/// Falls back to `0..num_cpus::get()` if /proc/self/status is
/// unavailable or unparseable.
pub fn detect_pod_cpus() -> Vec<usize> {
    if let Ok(s) = fs::read_to_string("/proc/self/status") {
        for line in s.lines() {
            if let Some(rest) = line.strip_prefix("Cpus_allowed_list:") {
                let mut out = parse_cpu_list(rest.trim());
                out.sort();
                if !out.is_empty() {
                    return out;
                }
            }
        }
    }
    (0..num_cpus::get()).collect()
}

/// Parse a Linux-style CPU list ("0-3,7,10-12") into a Vec of CPU ids.
/// Returns empty Vec on any parse failure (caller should fall back).
fn parse_cpu_list(s: &str) -> Vec<usize> {
    let mut out = Vec::new();
    for token in s.split(',') {
        let token = token.trim();
        if token.is_empty() {
            continue;
        }
        if let Some((a, b)) = token.split_once('-') {
            let (a, b) = match (a.parse::<usize>(), b.parse::<usize>()) {
                (Ok(a), Ok(b)) if a <= b => (a, b),
                _ => return Vec::new(),
            };
            for c in a..=b {
                out.push(c);
            }
        } else {
            match token.parse::<usize>() {
                Ok(c) => out.push(c),
                Err(_) => return Vec::new(),
            }
        }
    }
    out
}

/// Given the indexer_cores config and the pod's available cores,
/// return `(indexer_pool, view_pool)`. indexer_pool is the first N
/// cores of the detected set; view_pool is the remainder. Caps
/// indexer_cores at `total - 1` so the view pool is never empty (the
/// HTTP server still needs at least one core to bind on).
///
/// `(vec![], vec![])` shape never happens — if cores is empty the
/// returned indexer/view pools just both equal cores (i.e. unset
/// affinity equals no isolation).
pub fn split_cores(cores: &[usize], indexer_cores: usize) -> (Vec<usize>, Vec<usize>) {
    if indexer_cores == 0 || cores.is_empty() {
        return (cores.to_vec(), cores.to_vec());
    }
    let n = indexer_cores.min(cores.len().saturating_sub(1)).max(1);
    let indexer_pool = cores[..n].to_vec();
    let view_pool = cores[n..].to_vec();
    (indexer_pool, view_pool)
}

/// Set the calling thread's CPU affinity to the given cores. No-op
/// when `cores` is empty.
#[cfg(target_os = "linux")]
pub fn set_thread_affinity(cores: &[usize]) -> Result<()> {
    if cores.is_empty() {
        return Ok(());
    }
    // SAFETY: cpu_set_t is a POD; we zero-init it before use.
    let mut set: cpu_set_t = unsafe { std::mem::zeroed() };
    unsafe {
        CPU_ZERO(&mut set);
        for &c in cores {
            CPU_SET(c, &mut set);
        }
        // pid=0 means "the calling thread" for sched_setaffinity on
        // Linux. (POSIX says process, but Linux extends it to threads
        // since the kernel models them as tasks.)
        let rc = sched_setaffinity(0, std::mem::size_of::<cpu_set_t>(), &set);
        if rc == -1 {
            return Err(std::io::Error::last_os_error().into());
        }
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub fn set_thread_affinity(_cores: &[usize]) -> Result<()> {
    Ok(())
}

/// Set the calling thread's nice value. Logs a warning and returns
/// Ok on EPERM (missing CAP_SYS_NICE) — the binary continues running
/// without priority isolation rather than failing to start. No-op
/// when `nice == 0`.
#[cfg(target_os = "linux")]
pub fn set_thread_nice(nice: i32) -> Result<()> {
    if nice == 0 {
        return Ok(());
    }
    // PRIO_PROCESS with id=0 targets the calling thread on Linux
    // (same task-vs-process extension as sched_setaffinity).
    // setpriority returns -1 on error; errno carries the reason.
    // Clear errno first because setpriority's return value of -1 is
    // a legitimate nice value too (range is -20..19), so we can't
    // use the return value alone to detect failure.
    unsafe {
        *libc::__errno_location() = 0;
        let rc = setpriority(PRIO_PROCESS, 0, nice);
        let err = *libc::__errno_location();
        if rc == -1 && err != 0 {
            if err == libc::EPERM || err == libc::EACCES {
                warn!(
                    "cpu_isolation: setpriority({}) denied (EPERM); \
                     container likely missing CAP_SYS_NICE — \
                     continuing without priority isolation",
                    nice
                );
                return Ok(());
            }
            return Err(std::io::Error::from_raw_os_error(err).into());
        }
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub fn set_thread_nice(_nice: i32) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_cpu_list_handles_common_shapes() {
        assert_eq!(parse_cpu_list("0-3"), vec![0, 1, 2, 3]);
        assert_eq!(parse_cpu_list("0,2,4"), vec![0, 2, 4]);
        assert_eq!(parse_cpu_list("0-1,3,5-7"), vec![0, 1, 3, 5, 6, 7]);
        assert_eq!(parse_cpu_list("7"), vec![7]);
        assert_eq!(parse_cpu_list(""), Vec::<usize>::new());
    }

    #[test]
    fn parse_cpu_list_rejects_garbage() {
        assert!(parse_cpu_list("a-b").is_empty());
        assert!(parse_cpu_list("3-1").is_empty());
        assert!(parse_cpu_list("xxx").is_empty());
    }

    #[test]
    fn split_cores_reserves_indexer_pool_first() {
        let cores: Vec<usize> = (0..8).collect();
        let (idx, view) = split_cores(&cores, 2);
        assert_eq!(idx, vec![0, 1]);
        assert_eq!(view, vec![2, 3, 4, 5, 6, 7]);
    }

    #[test]
    fn split_cores_caps_indexer_at_total_minus_one() {
        let cores: Vec<usize> = (0..4).collect();
        let (idx, view) = split_cores(&cores, 10);
        assert_eq!(idx.len(), 3); // capped at total - 1
        assert_eq!(view.len(), 1);
    }

    #[test]
    fn split_cores_off_means_full_set_for_both() {
        let cores: Vec<usize> = (0..4).collect();
        let (idx, view) = split_cores(&cores, 0);
        assert_eq!(idx, cores);
        assert_eq!(view, cores);
    }
}
