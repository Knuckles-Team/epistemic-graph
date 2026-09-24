//! What a topology decision reads (headroom, admissibility) and answers (the
//! plan). The plan PROPOSES capacity; only `AcquireCapacity` grants it.

use serde::{Deserialize, Serialize};

use super::StopRule;
use crate::capacity_lease::{CapacityResourceClass, LeasePriority};
use crate::contract::BoundedVec;

/// One capacity cell's headroom, observed through `CapacityStatus` at the
/// request's priority when the decision was taken. An OBSERVATION premise:
/// it is recorded in the inputs, so replay reads the same number.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct CapacityHeadroom {
    pub cell_id: String,
    pub class: CapacityResourceClass,
    /// What the request's priority may still lease from the cell: capacity
    /// minus live leases, minus the reserved floor when that priority may not
    /// spend it.
    pub available: u64,
    /// The cell epoch the observation was read at.
    pub epoch: u64,
}

/// Who attached the schema source an admissibility fact was entailed under.
/// An admin-attached vocabulary is a DEFINITION premise; a pack-attached one
/// is a CLAIM (SWARM-TOPOLOGY-DECIDE-DESIGN §4).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum SchemaAuthority {
    Admin,
    Pack,
}

/// One entailed admissibility fact: `task_class` admits `topology_class`
/// through the named restriction class of the attached schema. `axioms` are
/// the proof's axioms (bounded), so `OwlExplain(task_class, admit_class)`
/// re-checks it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct TopologyAdmission {
    pub task_class: String,
    pub topology_class: String,
    pub admit_class: String,
    /// The schema source key the restriction class came from.
    pub source_key: String,
    pub authority: SchemaAuthority,
    #[serde(default)]
    pub axioms: BoundedVec<String, 16>,
}

/// One slot's decided width and rounds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SlotPlan {
    pub node_id: String,
    pub width: u8,
    pub rounds: u8,
}

/// The amount one cell must lease for the plan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct CellLease {
    pub cell_id: String,
    pub class: CapacityResourceClass,
    pub amount: u64,
}

/// Every lease the plan needs, acquired all-or-nothing after commit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct LeasePlan {
    pub per_cell: BoundedVec<CellLease, 8>,
    pub priority: LeasePriority,
}

/// What a harness that cannot enforce a child count does with its native
/// sub-agents (operator ruling 2026-09-24, Q1): disabled by default; a
/// per-harness policy opt-in allows them under a token budget instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "fallback", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum SubagentFallback {
    Disabled,
    TokenBudget { max_tokens: u64 },
}

/// The sub-agent allowance of one harness node, derived by the plan. The
/// harness improvises only inside it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SubagentAllowance {
    pub node_id: String,
    pub harness: String,
    pub max_children: u8,
    pub max_depth: u8,
    /// The node's share of the plan's token ceiling, when there is one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u64>,
    pub fallback: SubagentFallback,
}

/// The decided topology, carried in a `Solved` outcome.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct TopologyPlan {
    pub class_iri: String,
    pub slots: BoundedVec<SlotPlan, 6>,
    pub stop: StopRule,
    pub lease: LeasePlan,
    #[serde(default)]
    pub allowances: BoundedVec<SubagentAllowance, 6>,
    /// Declared makespan bound: slots taken as sequential, an upper bound for
    /// any parallel branch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub makespan_ms: Option<u64>,
}
