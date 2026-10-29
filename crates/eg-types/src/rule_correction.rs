//! Typed rule-output correction with two-principal approval
//! (EG-DURABLE-KERNEL-R088.1).
//!
//! A correction names the rule, the input pattern and the expected output, the
//! original output it corrects, and whether it replaces or revokes that output.
//! An approval names two distinct principals, the approved rule version and the
//! approval time. Served submission and approval are EG-DURABLE-KERNEL-R088.2.

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum CorrectionBinding {
    /// The original output is superseded by this replacement output.
    Replacement { replacement_output_ref: String },
    /// The original output is withdrawn without a replacement.
    Revocation { reason: String },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuleCorrection {
    pub rule_id: String,
    pub input_pattern: String,
    pub expected_output: String,
    pub original_output_ref: String,
    pub binding: CorrectionBinding,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CorrectionApproval {
    pub approved_version: u64,
    pub first_principal: String,
    pub second_principal: String,
    pub approved_at_ms: u64,
}

/// A correction whose approval has passed the two-principal check.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ApprovedCorrection {
    correction: RuleCorrection,
    approval: CorrectionApproval,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum RuleCorrectionError {
    #[error("rule correction field `{0}` is empty")]
    EmptyField(&'static str),
    #[error("a correction approval needs two distinct principals")]
    SamePrincipal,
    #[error("a correction approval must name a rule version above zero")]
    ZeroVersion,
}

fn require(value: &str, field: &'static str) -> Result<(), RuleCorrectionError> {
    if value.trim().is_empty() {
        return Err(RuleCorrectionError::EmptyField(field));
    }
    Ok(())
}

impl RuleCorrection {
    pub fn validate(&self) -> Result<(), RuleCorrectionError> {
        require(&self.rule_id, "rule_id")?;
        require(&self.input_pattern, "input_pattern")?;
        require(&self.expected_output, "expected_output")?;
        require(&self.original_output_ref, "original_output_ref")?;
        match &self.binding {
            CorrectionBinding::Replacement {
                replacement_output_ref,
            } => require(replacement_output_ref, "replacement_output_ref"),
            CorrectionBinding::Revocation { reason } => require(reason, "reason"),
        }
    }
}

impl CorrectionApproval {
    pub fn validate(&self) -> Result<(), RuleCorrectionError> {
        require(&self.first_principal, "first_principal")?;
        require(&self.second_principal, "second_principal")?;
        if self.first_principal.trim() == self.second_principal.trim() {
            return Err(RuleCorrectionError::SamePrincipal);
        }
        if self.approved_version == 0 {
            return Err(RuleCorrectionError::ZeroVersion);
        }
        Ok(())
    }
}

impl ApprovedCorrection {
    pub fn new(
        correction: RuleCorrection,
        approval: CorrectionApproval,
    ) -> Result<Self, RuleCorrectionError> {
        correction.validate()?;
        approval.validate()?;
        Ok(Self {
            correction,
            approval,
        })
    }

    pub fn correction(&self) -> &RuleCorrection {
        &self.correction
    }

    pub fn approval(&self) -> &CorrectionApproval {
        &self.approval
    }

    pub fn revokes(&self) -> bool {
        matches!(
            self.correction.binding,
            CorrectionBinding::Revocation { .. }
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn correction(binding: CorrectionBinding) -> RuleCorrection {
        RuleCorrection {
            rule_id: "rule:discount-tier".into(),
            input_pattern: "order.total > 100".into(),
            expected_output: "tier=gold".into(),
            original_output_ref: "output:sha256:01".into(),
            binding,
        }
    }

    fn approval(first: &str, second: &str) -> CorrectionApproval {
        CorrectionApproval {
            approved_version: 3,
            first_principal: first.into(),
            second_principal: second.into(),
            approved_at_ms: 1_700_000_000_000,
        }
    }

    // spec: EG-DURABLE-KERNEL-R088.1
    #[test]
    fn approval_requires_two_distinct_principals() {
        let fix = correction(CorrectionBinding::Revocation {
            reason: "wrong threshold".into(),
        });
        for (a, b) in [("alice", "alice"), ("alice", " alice ")] {
            assert_eq!(
                ApprovedCorrection::new(fix.clone(), approval(a, b)),
                Err(RuleCorrectionError::SamePrincipal)
            );
        }
        assert_eq!(
            ApprovedCorrection::new(fix.clone(), approval("alice", "")),
            Err(RuleCorrectionError::EmptyField("second_principal"))
        );
        let mut zero = approval("alice", "bob");
        zero.approved_version = 0;
        assert_eq!(
            ApprovedCorrection::new(fix.clone(), zero),
            Err(RuleCorrectionError::ZeroVersion)
        );
        let approved = ApprovedCorrection::new(fix, approval("alice", "bob")).unwrap();
        assert_eq!(approved.approval().approved_at_ms, 1_700_000_000_000);
    }

    // spec: EG-DURABLE-KERNEL-R088.1
    #[test]
    fn replacement_and_revocation_states_bind_and_round_trip() {
        let replace = correction(CorrectionBinding::Replacement {
            replacement_output_ref: "output:sha256:02".into(),
        });
        let revoke = correction(CorrectionBinding::Revocation {
            reason: "obsolete rule".into(),
        });
        for (fix, revokes) in [(replace, false), (revoke, true)] {
            let packed = rmp_serde::to_vec_named(&fix).unwrap();
            assert_eq!(
                rmp_serde::from_slice::<RuleCorrection>(&packed).unwrap(),
                fix
            );
            let approved = ApprovedCorrection::new(fix, approval("alice", "bob")).unwrap();
            assert_eq!(approved.revokes(), revokes);
        }
        let empty = correction(CorrectionBinding::Replacement {
            replacement_output_ref: String::new(),
        });
        assert_eq!(
            empty.validate(),
            Err(RuleCorrectionError::EmptyField("replacement_output_ref"))
        );
        let mut no_rule = correction(CorrectionBinding::Revocation { reason: "x".into() });
        no_rule.rule_id.clear();
        assert_eq!(
            no_rule.validate(),
            Err(RuleCorrectionError::EmptyField("rule_id"))
        );
    }
}
