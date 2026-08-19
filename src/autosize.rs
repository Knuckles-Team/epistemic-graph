//! Cgroup-aware hardware capacity (CONCEPT:AU-KG.backend.b-auto-size).
//!
//! The implementation lives in the dependency-free `eg-resource` workspace
//! leaf. Keeping this facade preserves the engine's existing public module path
//! while allowing lower crates such as `eg-asr-whisper` to use the exact same
//! v1/v2 CPU and memory parser. Automatic consumers use the effective host /
//! cgroup minimum and reserve 10% CPU plus 20% RAM headroom. Explicit values
//! are upper bounds only and must be clamped to these automatic values.

pub use eg_resource::{
    bound_explicit, default_node_cap, detect_capacity, parse_cgroup_v1_cpu_quota,
    parse_cgroup_v1_memory_limit, parse_cgroup_v2_cpu_max, parse_cgroup_v2_memory_max,
    parse_mem_total, reserve_cpu, reserve_ram, resolve_cgroup_file, resolve_cgroup_files,
    resolve_cpu_budget, resolve_memory_bytes, tier_for, Capacity, CpuLimit, MemoryLimit, Tier,
    BYTES_PER_NODE_EST, CPU_HEADROOM_PERCENT, MALFORMED_RAM_BYTES, MAX_NODE_CAP,
    MEMORY_HEADROOM_PERCENT, UNKNOWN_RAM_BYTES,
};

/// Host RAM only, retained for callers that need the diagnostic `/proc` value.
pub fn total_ram_bytes() -> Option<u64> {
    eg_resource::total_ram_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pi_ram_gets_a_bounded_node_cap() {
        let cap = default_node_cap(1024 * 1024 * 1024);
        assert!(cap > 0);
        assert!(cap < 1_000_000, "a 1 GiB box cap should stay modest: {cap}");
    }

    #[test]
    fn big_box_is_effectively_unbounded_not_oom_capped() {
        let big = default_node_cap(247 * 1024 * 1024 * 1024);
        assert!(big >= 50_000_000, "big box cap too small: {big}");
        assert!(big > default_node_cap(1024 * 1024 * 1024));
    }

    #[test]
    fn unknown_ram_gets_a_conservative_cap() {
        assert_eq!(default_node_cap(0), default_node_cap(1024 * 1024 * 1024));
        assert!(default_node_cap(0) > 0);
    }

    #[test]
    fn capacity_with_undetectable_ram_gets_a_bounded_cap() {
        let c = Capacity {
            cpus: 4,
            total_ram_bytes: 0,
            tier: Tier::Node,
        };
        assert_eq!(c.node_cap(), default_node_cap(0));
        assert!(c.node_cap() > 0);
    }

    #[test]
    fn node_cap_is_monotonic_in_ram() {
        let a = default_node_cap(2 * 1024 * 1024 * 1024);
        let b = default_node_cap(16 * 1024 * 1024 * 1024);
        let c = default_node_cap(128 * 1024 * 1024 * 1024);
        assert!(a <= b && b <= c);
    }

    #[test]
    fn tiers_classify_pi_node_bigbox() {
        assert_eq!(tier_for(1024 * 1024 * 1024), Tier::Pi);
        assert_eq!(tier_for(2 * 1024 * 1024 * 1024), Tier::Pi);
        assert_eq!(tier_for(16 * 1024 * 1024 * 1024), Tier::Node);
        assert_eq!(tier_for(247 * 1024 * 1024 * 1024), Tier::BigBox);
        assert_eq!(tier_for(0), Tier::Node);
    }

    #[test]
    fn effective_cpu_and_memory_respect_fixture_limits() {
        let cpus = resolve_cpu_budget(
            64,
            CpuLimit::Limited {
                quota_us: 200_000,
                period_us: 100_000,
            },
            usize::MAX,
            None,
        );
        let memory = resolve_memory_bytes(
            247 * 1024 * 1024 * 1024,
            MemoryLimit::Limited(256 * 1024 * 1024),
        );
        assert!(cpus <= 2);
        assert!(memory <= 256 * 1024 * 1024);
    }
}
