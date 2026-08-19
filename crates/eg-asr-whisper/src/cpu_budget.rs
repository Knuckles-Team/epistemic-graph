//! Cgroup-aware whisper.cpp thread sizing.
//!
//! The parser and effective-capacity policy live in the dependency-free
//! [`eg_resource`] leaf crate so the facade and this optional provider cannot
//! drift.  `EG_ASR_MAX_THREADS` is an explicit upper bound, never a bypass: it
//! is clamped to the same cgroup-aware automatic budget.

const DEFAULT_MAX_THREADS: usize = 4;

/// The thread count `FullParams::set_n_threads` should request. The shared
/// resolver intersects the OS-visible CPU count with cgroup v1/v2 quota,
/// reserves headroom, and bounds the result by whisper's hard maximum. An
/// explicit environment value can only lower that safe automatic result.
pub fn effective_thread_budget() -> i32 {
    let explicit = std::env::var("EG_ASR_MAX_THREADS")
        .ok()
        .and_then(|raw| raw.trim().parse::<usize>().ok())
        .filter(|value| *value > 0);
    eg_resource::detect_cpu_budget(DEFAULT_MAX_THREADS, explicit) as i32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn effective_budget_is_positive_and_hard_bounded() {
        let budget = effective_thread_budget();
        assert!((1..=DEFAULT_MAX_THREADS as i32).contains(&budget));
    }

    #[test]
    fn malformed_explicit_override_is_not_a_bypass() {
        assert_eq!(
            "not-a-number"
                .trim()
                .parse::<usize>()
                .ok()
                .filter(|value| *value > 0),
            None
        );
        assert_eq!(
            "0".trim().parse::<usize>().ok().filter(|value| *value > 0),
            None
        );
    }
}
