//! Shard catalog, rebalance and distributed materialized-view result bodies of the `cluster` domain.

use serde::{Deserialize, Serialize};

/// One graph moved between durable shards.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ShardReshardReport {
    pub graph: String,
    pub from_shard: u64,
    pub to_shard: u64,
    pub nodes: u64,
    pub edges: u64,
    pub ledger: u64,
    pub semantic: u64,
    pub audit: u64,
    pub delta_nodes: u64,
    pub delta_edges: u64,
    /// The graph already lived on the target shard; nothing moved.
    pub no_op: bool,
}

/// `RebalanceExecute`: every move the plan executed, in order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RebalanceExecution {
    pub executed: Vec<ShardReshardReport>,
}

/// An explicit tenant-catalog placement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct CatalogPlacement {
    pub graph: String,
    pub shard: u32,
    /// `null` for the local node.
    pub node: Option<u32>,
}

/// `CatalogList`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct CatalogListing {
    pub placements: Vec<CatalogPlacement>,
}

/// One planned graph move.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RebalanceMove {
    pub graph: String,
    pub from_shard: u32,
    pub to_shard: u32,
}

/// The load of one shard the plan was computed against.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ShardLoadSummary {
    pub shard: u32,
    pub total: u64,
    pub graphs: u64,
}

/// `RebalancePlan`: the ordered moves plus the per-shard loads they were planned on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RebalancePlanReport {
    pub moves: Vec<RebalanceMove>,
    pub shards: Vec<ShardLoadSummary>,
}

// Preserve the F3d public path while keeping the F3b compute-result body as the
// single owner shared by `DistributedCompute` and `GetMatView`.
#[cfg(feature = "compute-dist")]
pub use crate::compute_result::distributed::{DistResult, LabelRows, ScoreRows};
