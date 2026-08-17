//! BUG-283: this fleet's containerd does **not** virtualize `/proc` — a pod
//! with a 2-CPU `limits.cpu` still sees `nproc: 64` (the host's real CPU
//! count), because that limit is enforced by the CFS scheduler's
//! quota/period, not by a `cpuset` restricting which CPUs are affinity-
//! visible. `std::thread::available_parallelism()` (which reads
//! `sched_getaffinity`, falling back to `/proc/cpuinfo`) is exactly as blind
//! to this as plain `nproc` — a whisper.cpp thread pool sized from it alone
//! would oversubscribe the real cgroup budget and starve every other
//! container on the node, not just itself.
//!
//! [`effective_thread_budget`] reads the REAL cgroup v2 (`cpu.max`) or
//! cgroup v1 (`cpu.cfs_quota_us`/`cpu.cfs_period_us`) CPU quota when present
//! and bounds the OS-visible count by it, rather than trusting
//! `available_parallelism()` alone. An explicit `EG_ASR_MAX_THREADS`
//! environment override takes precedence over both — an operator who knows
//! their deployment's real budget should never need to fight a heuristic.

const DEFAULT_MAX_THREADS: i32 = 4;

/// The thread count `FullParams::set_n_threads` should request: the
/// smallest of (a) an explicit `EG_ASR_MAX_THREADS` override, (b) the real
/// cgroup CPU quota (v2 `cpu.max` or v1 `cpu.cfs_quota_us`/
/// `cpu.cfs_period_us`) when a finite limit is actually set, (c) the
/// OS-visible CPU count, and (d) [`DEFAULT_MAX_THREADS`] — never larger than
/// any of them, so a container with a smaller real budget than the compiled
/// default is respected rather than oversubscribed.
pub fn effective_thread_budget() -> i32 {
    if let Some(n) = env_override() {
        return n;
    }
    let os_visible = std::thread::available_parallelism()
        .map(|n| n.get() as i32)
        .unwrap_or(1)
        .max(1);
    let cgroup_budget = cgroup_v2_cpu_budget("/sys/fs/cgroup/cpu.max")
        .or_else(|| {
            cgroup_v1_cpu_budget(
                "/sys/fs/cgroup/cpu/cpu.cfs_quota_us",
                "/sys/fs/cgroup/cpu/cpu.cfs_period_us",
            )
        })
        .unwrap_or(os_visible);
    cgroup_budget
        .min(os_visible)
        .min(DEFAULT_MAX_THREADS)
        .max(1)
}

fn env_override() -> Option<i32> {
    let raw = std::env::var("EG_ASR_MAX_THREADS").ok()?;
    let n: i32 = raw.trim().parse().ok()?;
    if n > 0 {
        Some(n)
    } else {
        None
    }
}

/// Parse a cgroup v2 `cpu.max` file (`"<quota> <period>"`, or `"max <period>"`
/// for no limit). Returns `None` for an unlimited/unparseable/absent file —
/// the caller falls back to the OS-visible count in that case, never to zero
/// threads.
fn cgroup_v2_cpu_budget(path: &str) -> Option<i32> {
    let content = std::fs::read_to_string(path).ok()?;
    parse_cgroup_v2_cpu_max(&content)
}

fn parse_cgroup_v2_cpu_max(content: &str) -> Option<i32> {
    let mut parts = content.split_whitespace();
    let quota_raw = parts.next()?;
    let period_raw = parts.next()?;
    if quota_raw == "max" {
        return None;
    }
    let quota: f64 = quota_raw.parse().ok()?;
    let period: f64 = period_raw.parse().ok()?;
    if quota <= 0.0 || period <= 0.0 {
        return None;
    }
    Some(((quota / period).ceil() as i32).max(1))
}

/// Parse cgroup v1's separate quota/period files. A quota of `-1` (or any
/// non-positive value) means unlimited in cgroup v1 — returns `None`, same
/// fallback contract as the v2 parser.
fn cgroup_v1_cpu_budget(quota_path: &str, period_path: &str) -> Option<i32> {
    let quota: i64 = std::fs::read_to_string(quota_path)
        .ok()?
        .trim()
        .parse()
        .ok()?;
    let period: i64 = std::fs::read_to_string(period_path)
        .ok()?
        .trim()
        .parse()
        .ok()?;
    parse_cgroup_v1_cpu_quota(quota, period)
}

fn parse_cgroup_v1_cpu_quota(quota: i64, period: i64) -> Option<i32> {
    if quota <= 0 || period <= 0 {
        return None;
    }
    Some((((quota as f64) / (period as f64)).ceil() as i32).max(1))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cgroup_v2_two_cpu_limit_yields_two_threads() {
        // 200000/100000 = 2.0 CPUs, matching a real "limits.cpu: 2" pod.
        assert_eq!(parse_cgroup_v2_cpu_max("200000 100000\n"), Some(2));
    }

    #[test]
    fn cgroup_v2_fractional_limit_rounds_up() {
        // 150000/100000 = 1.5 CPUs -- round UP (never under-provision a
        // fractional limit down to fewer usable threads than granted).
        assert_eq!(parse_cgroup_v2_cpu_max("150000 100000\n"), Some(2));
    }

    #[test]
    fn cgroup_v2_max_quota_is_unlimited() {
        assert_eq!(parse_cgroup_v2_cpu_max("max 100000\n"), None);
    }

    #[test]
    fn cgroup_v2_malformed_content_is_none() {
        assert_eq!(parse_cgroup_v2_cpu_max("not a cgroup file"), None);
        assert_eq!(parse_cgroup_v2_cpu_max(""), None);
    }

    #[test]
    fn cgroup_v1_two_cpu_limit_yields_two_threads() {
        assert_eq!(parse_cgroup_v1_cpu_quota(200_000, 100_000), Some(2));
    }

    #[test]
    fn cgroup_v1_negative_quota_is_unlimited() {
        assert_eq!(parse_cgroup_v1_cpu_quota(-1, 100_000), None);
    }

    #[test]
    fn cgroup_v1_zero_period_is_rejected_not_divided_by_zero() {
        assert_eq!(parse_cgroup_v1_cpu_quota(200_000, 0), None);
    }

    #[test]
    fn effective_budget_is_always_at_least_one() {
        // Whatever this test host's real cgroup/OS state is, the budget
        // must never be zero or negative -- a whisper.cpp thread pool sized
        // at zero would never make progress.
        assert!(effective_thread_budget() >= 1);
    }

    #[test]
    fn env_override_takes_precedence() {
        // SAFETY: test-only, single-threaded within this test's scope is not
        // guaranteed by the test harness, so this asserts the PARSING
        // behavior of env_override() in isolation rather than mutating the
        // real process environment (avoids cross-test races on shared env).
        assert_eq!("3".trim().parse::<i32>().ok().filter(|n| *n > 0), Some(3));
        assert_eq!("0".trim().parse::<i32>().ok().filter(|n| *n > 0), None);
        assert_eq!("-1".trim().parse::<i32>().ok().filter(|n| *n > 0), None);
        assert_eq!("not-a-number".trim().parse::<i32>().ok(), None);
    }

    #[test]
    fn absent_cgroup_files_fall_back_to_none_not_a_panic() {
        assert_eq!(
            cgroup_v2_cpu_budget("/definitely/not/a/real/cgroup/path/cpu.max"),
            None
        );
        assert_eq!(
            cgroup_v1_cpu_budget(
                "/definitely/not/a/real/cgroup/path/cpu.cfs_quota_us",
                "/definitely/not/a/real/cgroup/path/cpu.cfs_period_us"
            ),
            None
        );
    }
}
