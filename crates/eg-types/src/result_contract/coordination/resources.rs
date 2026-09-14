//! Resource statistics result bodies of the `coordination` domain.

use serde::{Deserialize, Serialize};

/// Per-graph resource snapshot (CONCEPT:EG-KG.compute.lane-v).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct GraphResourceStats {
    pub graph: String,
    pub tenant: String,
    pub nodes: u64,
    pub edges: u64,
    /// Approximate resident RAM, bytes.
    pub memory_bytes: u64,
    /// `true` if hibernated (in-RAM state dropped, durable in redb).
    pub hibernated: bool,
}

/// Per-tenant rollup (CONCEPT:EG-KG.compute.lane-v).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct TenantResourceStats {
    pub tenant: String,
    pub graphs: u64,
    pub resident_graphs: u64,
    pub hibernated_graphs: u64,
    pub nodes: u64,
    pub edges: u64,
    pub memory_bytes: u64,
    /// The tenant's configured byte budget (before the fair-share cap).
    pub budget_bytes: u64,
    /// `true` when `memory_bytes > budget_bytes` (the enforcer is reclaiming).
    pub over_budget: bool,
}

/// The full resource snapshot returned by `Method::ResourceStatsPage`
/// (CONCEPT:EG-KG.compute.lane-v) and scraped into Prometheus: the signals an
/// autoscaler (OS-5.27) needs in one round-trip.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ResourceSnapshot {
    /// Finite page size applied to the detail arrays (or the explicit request
    /// limit in summary mode).
    pub limit: u64,
    /// Exclusive keyset cursor accepted for this page, if any.
    pub cursor: Option<String>,
    /// Exclusive cursor for the next visible page. `None` means this page was
    /// the final page after ACL/tenant filtering.
    pub next_cursor: Option<String>,
    pub has_more: bool,
    /// `true` when this response intentionally omits detail arrays.
    pub summary: bool,
    /// Total resident RAM across all graphs (sum of `memory_bytes`).
    pub total_memory_bytes: u64,
    /// Current process RSS (falling back to peak RSS), bytes -- the OS-observed
    /// footprint used for calibration and hard-ceiling pressure.
    pub process_rss_bytes: u64,
    /// Configured global ceiling. The cgroup-aware automatic policy is always
    /// positive; an explicit override can lower it but cannot disable it.
    pub global_ceiling_bytes: u64,
    pub total_nodes: u64,
    pub total_edges: u64,
    pub graph_count: u64,
    pub tenant_count: u64,
    pub resident_graphs: u64,
    pub hibernated_graphs: u64,
    /// Effective cgroup-aware CPU lanes after the shared headroom policy.
    pub effective_cpu_cores: u64,
    /// Effective cgroup-aware RAM after the shared headroom policy.
    pub effective_memory_limit_bytes: u64,
    /// True when the bounded tenant rollup reached its cap. In that case
    /// `tenant_count` is a lower bound and the returned tenant array is partial.
    pub tenant_count_truncated: bool,
    /// Requests currently holding an admission permit (in-flight depth).
    pub in_flight: u64,
    /// Admission permits still available (`max_inflight - in_flight`); a small value
    /// means the queue is saturated (shedding BUSY).
    pub inflight_permits_available: u64,
    /// Cumulative LRU nodes evicted by the budget enforcer (rate = delta/interval).
    pub budget_evictions_total: u64,
    /// Cumulative graphs hibernated by the budget enforcer.
    pub budget_hibernations_total: u64,
    /// Number of structural writes waiting in all per-graph coalescer queues.
    pub coalescer_queue_depth: u64,
    /// Approximate bytes held by those queued writes.
    pub coalescer_queue_bytes: u64,
    /// Cumulative structural operations applied by the coalescer, including
    /// operations that used the bounded inline fallback.
    pub coalescer_operations_total: u64,
    pub tenants: Vec<TenantResourceStats>,
    pub graphs: Vec<GraphResourceStats>,
}
