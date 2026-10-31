//! Swarm topology through the assembly decision (SWARM-TOPOLOGY-DECIDE-DESIGN).
//!
//! A topology decision is an assembly over published graph templates whose
//! slots additionally carry a WIDTH (how many copies of the slot's agent run
//! concurrently) and a number of ROUNDS (how often a peer loop repeats). The
//! answer, a [`TopologyPlan`], fixes the widths, the rounds, the stop rule, the
//! capacity each cell must lease and the sub-agent allowance of every node.
//!
//! The topology VOCABULARY (the classes a template may be, and which task
//! shapes admit which classes) is data: an admin-attached `swarm-topology`
//! schema source reasoned in-process. Nothing here lists topology classes; a
//! template names its class by IRI. [`SlotRole`] and [`StopRule`] are the only
//! closed enums, because the solver and the executor act on them.
//!
//! Every amount is an integer, and a request may only TIGHTEN the policy's
//! caps (`POLICY_LOOSENING` otherwise), exactly like the solver budget.

pub mod capacity_commit;
pub mod plan;
#[cfg(test)]
mod tests;

use serde::{Deserialize, Serialize};

use crate::capacity_lease::{CapacityResourceClass, LeasePriority};
use crate::contract::BoundedVec;

pub use plan::{
    CapacityHeadroom, CellLease, LeasePlan, SchemaAuthority, SlotPlan, SubagentAllowance,
    SubagentFallback, TopologyAdmission, TopologyPlan,
};

/// Largest width one multiplicity slot may take.
pub const MAX_SLOT_WIDTH: u8 = 16;
/// Largest number of rounds one peer loop may run.
pub const MAX_SLOT_ROUNDS: u8 = 8;
/// Largest number of (width, rounds) choices one slot's one-hot may span.
pub const MAX_ONE_HOT_VALUES: usize = 8;
/// Largest nesting depth a topology may declare.
pub const MAX_TOPOLOGY_DEPTH: u8 = 8;

/// A request's (or a policy's) caps on the shape. A request may only tighten
/// the policy's caps.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct TopologyCaps {
    pub max_width: u8,
    pub max_depth: u8,
    pub max_rounds: u8,
    /// Ceiling on the declared tokens the whole plan may spend.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u64>,
}

impl TopologyCaps {
    /// The first field `self` would widen past `policy`, if any.
    pub fn loosened_field(&self, policy: &TopologyCaps) -> Option<&'static str> {
        let widened = [
            ("max_width", self.max_width > policy.max_width),
            ("max_depth", self.max_depth > policy.max_depth),
            ("max_rounds", self.max_rounds > policy.max_rounds),
            (
                "max_tokens",
                tokens_widen(self.max_tokens, policy.max_tokens),
            ),
        ];
        widened
            .into_iter()
            .find(|(_, widens)| *widens)
            .map(|(field, _)| field)
    }

    /// The caps a request actually runs under.
    pub fn tightened_by(&self, request: &TopologyCaps) -> TopologyCaps {
        TopologyCaps {
            max_width: self.max_width.min(request.max_width),
            max_depth: self.max_depth.min(request.max_depth),
            max_rounds: self.max_rounds.min(request.max_rounds),
            max_tokens: match (self.max_tokens, request.max_tokens) {
                (Some(policy), Some(request)) => Some(policy.min(request)),
                (policy, request) => policy.or(request),
            },
        }
    }
}

/// A request that names no token ceiling inherits the policy's; one that
/// names a ceiling above the policy's widens it.
fn tokens_widen(request: Option<u64>, policy: Option<u64>) -> bool {
    match (request, policy) {
        (Some(request), Some(policy)) => request > policy,
        (None, _) | (Some(_), None) => false,
    }
}

/// The capacity cells a plan must fit, read at one priority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct CapacityScope {
    pub cells: BoundedVec<String, 8>,
    pub priority: LeasePriority,
}

/// What a topology request asks for, beside the assembly's own requirements.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct TopologyRequirements {
    /// Task-shape class IRIs (`swarm:IndependentSubtasks`, ...). Admissibility
    /// of a template's class is entailed from these under the attached schema.
    pub task_classes: BoundedVec<String, 8>,
    /// Independent subtasks the fan-out slot has to cover.
    pub subtasks: u32,
    /// Subtasks one agent handles; `demand` needs `width ≥ ⌈subtasks/this⌉`.
    pub per_agent_subtasks: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline_ms: Option<u32>,
    pub caps: TopologyCaps,
    pub capacity: CapacityScope,
}

/// What one slot does in the topology. Closed, because the model rows act on
/// it: a `Verifier` satisfies the `verify` row, a `Child` slot is the fan-out
/// slot `demand` sizes, and a `Peer` slot repeats for rounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum SlotRole {
    Parent,
    Child,
    Peer,
    Aggregator,
    Verifier,
}

/// How much of one resource class one agent of a slot needs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct LeaseDemand {
    pub class: CapacityResourceClass,
    pub amount: u64,
}

/// The topology facts of one template slot (one `Agent` node).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SlotTopology {
    /// The template `Agent` node this slot is.
    pub node_id: String,
    pub role: SlotRole,
    pub min_width: u8,
    pub max_width: u8,
    /// `1` for a slot that runs once.
    pub max_rounds: u8,
    /// Declared p95 of one agent turn of this slot. Unknown is never zero: a
    /// deadline over a slot without one abstains with `UnknownFact`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub p95_ms: Option<u32>,
    /// Declared tokens one agent turn spends.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tokens: Option<u64>,
    /// Per-agent lease, one entry per resource class.
    #[serde(default)]
    pub lease: BoundedVec<LeaseDemand, 6>,
    /// The L4 harness the slot's agent runs on, when it runs on one. Recorded
    /// as a slot fact so the plan can derive its sub-agent allowance.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub harness: Option<String>,
}

/// When a running topology stops. Evaluating it is execution, not decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "rule", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum StopRule {
    MaxRounds { n: u8 },
    Quorum { k: u8, n: u8 },
    VerifierPass { max_rounds: u8 },
    Budget,
    Deadline,
}

impl StopRule {
    /// The round ceiling the rule itself imposes, if any.
    pub fn round_ceiling(&self) -> Option<u8> {
        match self {
            Self::MaxRounds { n } => Some(*n),
            Self::VerifierPass { max_rounds } => Some(*max_rounds),
            Self::Quorum { .. } | Self::Budget | Self::Deadline => None,
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        match self {
            Self::MaxRounds { n } | Self::VerifierPass { max_rounds: n } => {
                check_range("stop rule rounds", *n, 1, MAX_SLOT_ROUNDS)
            }
            Self::Quorum { k, n } => {
                check_range("quorum n", *n, 1, MAX_SLOT_WIDTH)?;
                check_range("quorum k", *k, 1, *n)
            }
            Self::Budget | Self::Deadline => Ok(()),
        }
    }
}

/// The topology facts a published graph template carries. Inside its
/// definition digest, so pinning a template pins its topology too.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct TopologyFacts {
    /// The template's topology class IRI, a class of the attached schema.
    pub class_iri: String,
    /// Nesting depth of the template (1 = no nested graph).
    pub depth: u8,
    pub slots: BoundedVec<SlotTopology, 6>,
    pub stop: StopRule,
}

impl TopologyFacts {
    /// Structural validity on its own: bounds, ranges and distinct slots. The
    /// shape-against-template rules (every slot is an `Agent` node, a fan-out
    /// template has a `Fanout` then a `Join`) are publish-time checks.
    pub fn validate(&self) -> Result<(), String> {
        if self.class_iri.trim().is_empty() || self.class_iri.len() > 512 {
            return Err("topology class_iri must be a non-empty IRI".to_string());
        }
        check_range("topology depth", self.depth, 1, MAX_TOPOLOGY_DEPTH)?;
        if self.slots.is_empty() {
            return Err("a topology names at least one slot".to_string());
        }
        self.stop.validate()?;
        let mut seen = std::collections::BTreeSet::new();
        for slot in &self.slots {
            if !seen.insert(slot.node_id.as_str()) {
                return Err(format!("topology slot '{}' is repeated", slot.node_id));
            }
            validate_slot(slot)?;
        }
        Ok(())
    }

    /// The slot facts for template node `node_id`.
    pub fn slot(&self, node_id: &str) -> Option<&SlotTopology> {
        self.slots.iter().find(|slot| slot.node_id == node_id)
    }
}

/// Hash domain of a template's topology facts inside its definition digest.
pub const TOPOLOGY_FACTS_DIGEST_DOMAIN: &str = "eg/topology-facts/v1";

/// `sha256:<hex>` of `facts` in the decision surface's one digest scheme.
pub fn facts_digest(facts: &TopologyFacts) -> String {
    let digest = String::from(crate::solve::Sha256Digest::of_json(
        TOPOLOGY_FACTS_DIGEST_DOMAIN,
        facts,
    ));
    format!("sha256:{digest}")
}

/// What a topology question read beside the library, recorded in the inputs
/// so replay reads the same facts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct TopologyInputs {
    /// Composed digest of the request graph's schema sources the
    /// admissibility facts were entailed under.
    pub schema_digest: String,
    /// Every entailed admissibility fact, sorted.
    #[serde(default)]
    pub admissions: BoundedVec<TopologyAdmission, 64>,
    /// Task classes entailed to need an independent check (a verifier slot).
    #[serde(default)]
    pub verify_required_by: BoundedVec<String, 8>,
    /// Headroom of every requested cell, in request order.
    #[serde(default)]
    pub headroom: BoundedVec<CapacityHeadroom, 8>,
}

/// A decision policy's topology section: the caps a request may only tighten
/// and the harnesses whose native sub-agents may run under a token budget
/// when they cannot enforce a child count (operator ruling 2026-09-24, Q1).
/// There is no exploration budget: width and rounds are never explored until
/// the synthetic benchmark shows width buys quality (Q2).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct TopologyPolicy {
    pub caps: TopologyCaps,
    #[serde(default)]
    pub token_budget_harnesses: BoundedVec<String, 16>,
}

impl TopologyPolicy {
    /// The engine default: `ElasticTopologyAdmission`'s immutable caps.
    pub fn engine_default() -> Self {
        Self {
            caps: TopologyCaps {
                max_width: MAX_SLOT_WIDTH,
                max_depth: MAX_TOPOLOGY_DEPTH,
                max_rounds: MAX_SLOT_ROUNDS,
                max_tokens: Some(500_000),
            },
            token_budget_harnesses: BoundedVec::default(),
        }
    }
}

fn validate_slot(slot: &SlotTopology) -> Result<(), String> {
    check_range("slot min_width", slot.min_width, 1, MAX_SLOT_WIDTH)?;
    check_range(
        "slot max_width",
        slot.max_width,
        slot.min_width,
        MAX_SLOT_WIDTH,
    )?;
    check_range("slot max_rounds", slot.max_rounds, 1, MAX_SLOT_ROUNDS)?;
    // One one-hot per slot over its (width, rounds) pairs keeps every
    // width- and round-dependent quantity a precomputed linear coefficient.
    let choices = (usize::from(slot.max_width - slot.min_width) + 1) * usize::from(slot.max_rounds);
    if choices > MAX_ONE_HOT_VALUES {
        return Err(format!(
            "slot '{}' spans {choices} (width, rounds) choices; at most {MAX_ONE_HOT_VALUES}",
            slot.node_id
        ));
    }
    let mut classes: Vec<CapacityResourceClass> = slot.lease.iter().map(|d| d.class).collect();
    classes.sort();
    classes.dedup();
    if classes.len() != slot.lease.len() {
        return Err(format!("slot '{}' repeats a lease class", slot.node_id));
    }
    Ok(())
}

fn check_range(field: &str, value: u8, low: u8, high: u8) -> Result<(), String> {
    if value < low || value > high {
        return Err(format!("{field} {value} is outside {low}..={high}"));
    }
    Ok(())
}
