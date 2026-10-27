//! Typed champion and rollback records for harness and program promotion.

use serde::{Deserialize, Serialize};

use crate::contract::OpaqueRef;

/// The active revision for one agent and task class.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ChampionPointer {
    pub agent_ref: OpaqueRef,
    pub task_class: String,
    pub revision_ref: OpaqueRef,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_ref: Option<OpaqueRef>,
    pub promoted_by_decision: OpaqueRef,
}

/// A decision to move a champion from one revision to another.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RollbackDecision {
    pub pointer: ChampionPointer,
    pub from_ref: OpaqueRef,
    pub to_ref: OpaqueRef,
    pub reason: String,
    pub evidence_refs: Vec<OpaqueRef>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn reference(namespace: &str, value: &str) -> OpaqueRef {
        let mut token = value
            .as_bytes()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        while token.len() < 16 {
            token.push('0');
        }
        OpaqueRef::scoped(namespace, &token).expect("valid opaque reference")
    }

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
