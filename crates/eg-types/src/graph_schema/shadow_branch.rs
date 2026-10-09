//! EG-DECISION-ENGINE-R102.1: the typed gate ordering a schema-repair
//! candidate's mapping decision, ABox consistency check, shadow-branch
//! ingest and approval before activation against the live schema.
//!
//! Split out of R102 per the rapid-delivery contract's sizing rule (the
//! full row also names the mapping-decision ladder and the live
//! `GraphSchemaOp::AttachApproved` wiring to a real shadow-branch ingest,
//! further code roots); this slice is the typed model and its refusal
//! test: [`SchemaRepairActivation::checked_for_activation`] refuses
//! activation unless every step -- decided mapping, passed ABox check,
//! shadow-branch ingest, approval ([`super::approval::verify_schema_approval`])
//! -- is recorded, in that order.

use serde::{Deserialize, Serialize};

/// One step of the schema-repair activation gate, in required order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum SchemaRepairStep {
    /// The decision ladder evaluated the repair mapping into a candidate
    /// `GraphSchema` or pack.
    MappingDecided,
    /// The candidate passed an ABox consistency check.
    AboxCheckPassed,
    /// The candidate was ingested on a shadow branch, not the live schema.
    ShadowBranchIngested,
    /// A human (or construction) explicitly approved the shadow-ingested
    /// candidate (`verify_schema_approval`).
    Approved,
}

/// The gate's recorded progress toward activating one schema-repair
/// candidate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SchemaRepairActivation {
    pub candidate_digest: String,
    /// Completed steps, in the order they actually completed.
    pub completed: Vec<SchemaRepairStep>,
}

impl SchemaRepairActivation {
    /// The required order: a candidate may never activate having skipped
    /// or reordered a step.
    pub const REQUIRED_ORDER: [SchemaRepairStep; 4] = [
        SchemaRepairStep::MappingDecided,
        SchemaRepairStep::AboxCheckPassed,
        SchemaRepairStep::ShadowBranchIngested,
        SchemaRepairStep::Approved,
    ];

    /// Refuses activation unless every step in [`Self::REQUIRED_ORDER`] was
    /// completed, in that exact order.
    pub fn checked_for_activation(&self) -> Result<(), String> {
        if self.completed.len() < Self::REQUIRED_ORDER.len() {
            let missing = Self::REQUIRED_ORDER
                .iter()
                .find(|step| !self.completed.contains(step))
                .expect("fewer completed steps than required implies a missing one");
            return Err(format!(
                "schema-repair candidate {} cannot activate: {missing:?} has not completed",
                self.candidate_digest
            ));
        }
        if self.completed.as_slice() != Self::REQUIRED_ORDER.as_slice() {
            return Err(format!(
                "schema-repair candidate {} completed its activation gate steps out of order",
                self.candidate_digest
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candidate(completed: Vec<SchemaRepairStep>) -> SchemaRepairActivation {
        SchemaRepairActivation {
            candidate_digest: "sha256:candidate".to_string(),
            completed,
        }
    }

    #[test]
    fn every_step_completed_in_order_activates() {
        let activation = candidate(SchemaRepairActivation::REQUIRED_ORDER.to_vec());
        assert!(activation.checked_for_activation().is_ok());
    }

    #[test]
    fn activation_without_a_shadow_branch_ingest_is_refused() {
        let activation = candidate(vec![
            SchemaRepairStep::MappingDecided,
            SchemaRepairStep::AboxCheckPassed,
            SchemaRepairStep::Approved,
        ]);
        let err = activation
            .checked_for_activation()
            .expect_err("missing the shadow-branch ingest step");
        assert!(err.contains("ShadowBranchIngested"));
    }

    #[test]
    fn approval_recorded_before_the_shadow_branch_ingest_is_refused() {
        let activation = candidate(vec![
            SchemaRepairStep::MappingDecided,
            SchemaRepairStep::AboxCheckPassed,
            SchemaRepairStep::Approved,
            SchemaRepairStep::ShadowBranchIngested,
        ]);
        let err = activation
            .checked_for_activation()
            .expect_err("out of order");
        assert!(err.contains("out of order"));
    }
}
