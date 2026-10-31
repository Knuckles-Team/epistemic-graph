//! Typed champion and rollback records for harness and program promotion.

use serde::{Deserialize, Serialize};

use crate::contract::OpaqueId;

/// The active revision for one agent and task class.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ChampionPointer {
    pub agent_ref: OpaqueId,
    pub task_class: String,
    pub revision_ref: OpaqueId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_ref: Option<OpaqueId>,
    pub promoted_by_decision: OpaqueId,
}

/// A decision to move a champion from one revision to another.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RollbackDecision {
    pub pointer: ChampionPointer,
    pub from_ref: OpaqueId,
    pub to_ref: OpaqueId,
    pub reason: String,
    pub evidence_refs: Vec<OpaqueId>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reference(namespace: &str, value: &str) -> OpaqueId {
        OpaqueId::new(format!("{namespace}:{value}")).expect("valid opaque id")
    }

    // spec: MEDULLA-DECISION-ENGINE-R805.1
    #[test]
    fn records_round_trip() {
        let pointer = ChampionPointer {
            agent_ref: reference("agent", "agent-a"),
            task_class: "research".to_owned(),
            revision_ref: reference("revision", "revision-2"),
            parent_ref: Some(reference("revision", "revision-1")),
            promoted_by_decision: reference("decision", "decision-4"),
        };
        let rollback = RollbackDecision {
            pointer: pointer.clone(),
            from_ref: pointer.revision_ref.clone(),
            to_ref: pointer.parent_ref.clone().expect("parent revision"),
            reason: "held_out_regression".to_owned(),
            evidence_refs: vec![reference("evidence", "evaluation-7")],
        };

        let pointer_bytes = rmp_serde::to_vec_named(&pointer).unwrap();
        assert_eq!(
            rmp_serde::from_slice::<ChampionPointer>(&pointer_bytes).unwrap(),
            pointer
        );
        let rollback_bytes = rmp_serde::to_vec_named(&rollback).unwrap();
        assert_eq!(
            rmp_serde::from_slice::<RollbackDecision>(&rollback_bytes).unwrap(),
            rollback
        );
    }
}
