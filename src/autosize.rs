//! Hardware capacity auto-detection (CONCEPT:AU-KG.backend.b-auto-size).
//!
//! The SAME server binary must be correct + lean on a Raspberry Pi 3 (4 cores /
//! 1 GiB RAM) AND exploit a 64-core / 247 GiB box. Rather than ship per-host knobs
//! an operator has to tune, we DERIVE the concurrency / buffer / memory-cap defaults
//! from `(cpu_count, total_RAM)` at startup — mirroring the precedents already in the
//! tree: `main.rs` sizes the Tokio runtime from `available_parallelism()`,
//! `write_coalescer::CoalescerConfig::auto()` sizes its batch from cpu count, and
//! `cost.rs` reads `/proc/meminfo` for the memory-budget default.
//!
//! Configuration discipline (AGENTS.md): prefer auto-detection over knobs. Every
//! value here stays overridable by its EXISTING env var — we only change the DEFAULT
//! (the unset branch) from a fixed constant to an auto-sized one.

/// Coarse host class derived from total RAM. Used for logging / coarse policy; the
/// concrete sizes below are continuous functions of `(cpus, ram)`, not the tier.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Tier {
    /// Small single-board class (≤ 2 GiB) — a Raspberry Pi 3. Lean + bounded.
    Pi,
    /// Commodity server / workstation class (≤ 32 GiB).
    Node,
    /// Large box (> 32 GiB) — many cores, effectively unbounded graphs.
    BigBox,
}

/// Detected host capacity. `total_ram_bytes == 0` ⇒ RAM was undetectable
/// (non-Linux / restricted `/proc`); the derivations stay conservative in that case.
#[derive(Clone, Copy, Debug)]
pub struct Capacity {
    pub cpus: usize,
    pub total_ram_bytes: u64,
    pub tier: Tier,
}

const GIB: u64 = 1024 * 1024 * 1024;

/// Conservative resident-RAM estimate per graph node (property blob + topology
/// overhead). Embedding vectors are budgeted SEPARATELY (the per-tenant byte budget,
/// CONCEPT:EG-KG.compute.lane-v), and most graphs are embedding-free, so this is deliberately
/// modest. Used only to translate a RAM budget into a node-count cap.
const BYTES_PER_NODE_EST: u64 = 2048;

/// Total system RAM in bytes from `/proc/meminfo` (`MemTotal`). `None` if it cannot
/// be parsed (non-Linux / restricted). Same source `cost.rs` uses for the budget
/// default — kept here independently so capacity detection has no `cost`-feature dep.
pub fn total_ram_bytes() -> Option<u64> {
    let s = std::fs::read_to_string("/proc/meminfo").ok()?;
    for line in s.lines() {
        if let Some(rest) = line.strip_prefix("MemTotal:") {
            let kb = rest.split_whitespace().next()?.parse::<u64>().ok()?;
            return Some(kb * 1024);
        }
    }
    None
}

/// Classify a RAM size into a [`Tier`]. Unknown RAM (`0`) maps to `Node` — the
/// middle, never the Pi/OOM-cap extreme — so a host we cannot measure is never
/// wrongly bounded down.
fn tier_for(total_ram_bytes: u64) -> Tier {
    if total_ram_bytes == 0 {
        Tier::Node
    } else if total_ram_bytes <= 2 * GIB {
        Tier::Pi
    } else if total_ram_bytes <= 32 * GIB {
        Tier::Node
    } else {
        Tier::BigBox
    }
}

/// Detect host capacity from `available_parallelism()` + `/proc/meminfo`.
pub fn detect_capacity() -> Capacity {
    let cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
        .max(1);
    let total_ram_bytes = total_ram_bytes().unwrap_or(0);
    Capacity {
        cpus,
        total_ram_bytes,
        tier: tier_for(total_ram_bytes),
    }
}

/// Translate total RAM into a per-graph node cap (the default for
/// `EPISTEMIC_GRAPH_MAX_NODES_PER_GRAPH`). `0` RAM (undetectable) ⇒ `0` (unbounded —
/// never OOM-cap a host whose RAM we cannot read; a real Pi always reads
/// `/proc/meminfo`). Otherwise half of RAM divided by the per-node estimate, clamped
/// to a sane window so a tiny box still serves a usable graph and a huge box is
/// effectively unbounded.
///
/// Pi-safe because eviction is read-through (CONCEPT:EG-KG.storage.read-through-seam-exercised): a node evicted past
/// the cap still serves from the durable redb tier — the cap bounds RESIDENT RAM with
/// ZERO data loss, which is exactly what stops a 1 GiB Pi from OOM-killing by default.
pub fn default_node_cap(total_ram_bytes: u64) -> usize {
    if total_ram_bytes == 0 {
        return 0;
    }
    let raw = total_ram_bytes / 2 / BYTES_PER_NODE_EST;
    raw.clamp(50_000, 100_000_000) as usize
}

impl Capacity {
    /// Backpressure admission cap (default for `EPISTEMIC_GRAPH_MAX_INFLIGHT`, excess
    /// ⇒ `BUSY`). Scales with cores: a 4-core Pi sheds early at 256, a 64-core box
    /// admits 4096 deep concurrency (well above the old fixed 1024). Floor 256 keeps a
    /// 1-2 core box from starving; ceiling 8192 bounds the semaphore.
    pub fn max_inflight(&self) -> usize {
        (self.cpus * 64).clamp(256, 8192)
    }

    /// Reserved READ-admission lane (CONCEPT:EG-KG.coordination.reserved-read-lane, default for
    /// `EPISTEMIC_GRAPH_READ_RESERVED`). A dedicated pool of in-flight slots that ONLY
    /// read/query requests may use, SEPARATE from `max_inflight` (which writes also
    /// contend for). Under a flood of ingestion writes that saturates `max_inflight`
    /// and the per-graph cap, an interactive read falls back to this lane so an MCP
    /// tool call / query is NEVER shed to `BUSY` behind the write firehose — "at least
    /// N open lanes for reads". Cheap: reads hold a slot only for the brief off-lock
    /// snapshot, so even a small reservation keeps the read path alive. Scales gently
    /// with cores (an eighth of the admission cap), floored so a 1-2 core box still
    /// keeps several read lanes open.
    pub fn read_reserved(&self) -> usize {
        (self.max_inflight() / 8).clamp(8, 1024)
    }

    /// Authoritative writer queue depth. The bounded channel applies backpressure
    /// instead of dropping acknowledged work. It stays small on a Pi and absorbs
    /// larger bursts on high-core hosts.
    pub fn writer_queue(&self) -> usize {
        (self.cpus * 256).clamp(1024, 65_536)
    }

    /// Per-graph node-count cap before LRU eviction back to the durable tier (default
    /// for `EPISTEMIC_GRAPH_MAX_NODES_PER_GRAPH`). See [`default_node_cap`].
    pub fn node_cap(&self) -> usize {
        default_node_cap(self.total_ram_bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pi_ram_gets_a_bounded_node_cap() {
        // A 1 GiB Pi must NOT default to unbounded (the OOM bug). It must get a
        // positive, MODEST cap so the per-graph eviction sweep actually bounds RAM.
        let cap = default_node_cap(GIB);
        assert!(
            cap > 0,
            "Pi must get a bounded (>0) node cap, not unbounded"
        );
        assert!(
            cap < 1_000_000,
            "Pi cap should be modest, got {cap} (a 1 GiB box can't hold ~1M resident nodes)"
        );
    }

    #[test]
    fn big_box_is_effectively_unbounded_not_oom_capped() {
        // A 247 GiB box must NOT be capped down to a small number — it should stay
        // effectively unbounded so it exploits its RAM.
        let big = default_node_cap(247 * GIB);
        assert!(
            big >= 50_000_000,
            "big box must stay effectively unbounded, got {big}"
        );
        // Strictly larger than the Pi's cap — the cap is monotonic in RAM.
        assert!(big > default_node_cap(GIB));
    }

    #[test]
    fn unknown_ram_does_not_oom_cap() {
        // RAM we cannot read ⇒ no cap (0 = unbounded). Better to not cap a box we
        // cannot measure than to wrongly bound a big one; a real Pi always reads
        // /proc/meminfo so it never lands here.
        assert_eq!(default_node_cap(0), 0);
    }

    #[test]
    fn node_cap_is_monotonic_in_ram() {
        let a = default_node_cap(2 * GIB);
        let b = default_node_cap(16 * GIB);
        let c = default_node_cap(128 * GIB);
        assert!(a <= b && b <= c, "node cap must not shrink as RAM grows");
    }

    #[test]
    fn tiers_classify_pi_node_bigbox() {
        assert_eq!(tier_for(GIB), Tier::Pi);
        assert_eq!(tier_for(2 * GIB), Tier::Pi);
        assert_eq!(tier_for(16 * GIB), Tier::Node);
        assert_eq!(tier_for(247 * GIB), Tier::BigBox);
        assert_eq!(tier_for(0), Tier::Node, "unknown RAM ⇒ middle tier");
    }

    #[test]
    fn inflight_scales_with_cpus_pi_vs_bigbox() {
        let pi = Capacity {
            cpus: 4,
            total_ram_bytes: GIB,
            tier: Tier::Pi,
        };
        let big = Capacity {
            cpus: 64,
            total_ram_bytes: 247 * GIB,
            tier: Tier::BigBox,
        };
        // Pi sits at the floor (lean); big box exploits its cores well above the old
        // fixed 1024 default.
        assert_eq!(pi.max_inflight(), 256);
        assert_eq!(big.max_inflight(), 4096);
        assert!(big.max_inflight() > pi.max_inflight());
    }

    #[test]
    fn writer_queue_scales_with_cpus() {
        let pi = Capacity {
            cpus: 4,
            total_ram_bytes: GIB,
            tier: Tier::Pi,
        };
        let big = Capacity {
            cpus: 64,
            total_ram_bytes: 247 * GIB,
            tier: Tier::BigBox,
        };
        assert_eq!(pi.writer_queue(), 1024); // floor — lean on the Pi
        assert_eq!(big.writer_queue(), 16_384); // absorbs bursts on the big box
        assert!(big.writer_queue() > pi.writer_queue());
    }

    #[test]
    fn detect_capacity_is_sane_on_this_host() {
        // Whatever host runs the test suite, the detected values must be self-consistent.
        let c = detect_capacity();
        assert!(c.cpus >= 1);
        assert!(c.max_inflight() >= 256);
        assert!(c.writer_queue() >= 1024);
    }
}
