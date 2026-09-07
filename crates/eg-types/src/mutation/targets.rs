use serde::{Deserialize, Deserializer, Serialize};

use super::budget::{
    deserialize_and_charge, MutationBudgetCharge, MutationBudgetDeserialize, StructuralBudget,
    PRECONDITION_FIXED_BUDGET,
};
use super::MutationDomain;
use crate::authority::AuthorityScope;
use crate::contract::{Digest256, ResourceId, SchemaId, TenantId};
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordTarget {
    pub tenant: TenantId,
    pub scope_digest: Digest256,
    pub domain: MutationDomain,
    pub schema_id: SchemaId,
    pub record_id: ResourceId,
}

impl RecordTarget {
    pub(super) fn validate_for_scope(
        &self,
        tenant: &TenantId,
        scope: &AuthorityScope,
    ) -> Result<(), String> {
        let authorized_scope_digest = scope.digest()?;
        if &self.tenant != tenant
            || scope.tenant.as_ref() != Some(tenant)
            || self.scope_digest != authorized_scope_digest
        {
            return Err("mutation record target differs from authorized tenant/scope".into());
        }
        Ok(())
    }

    pub(super) fn digest(&self) -> Result<Digest256, String> {
        Digest256::framed(
            b"eg/record-target/v1",
            &[
                self.tenant.as_str().as_bytes(),
                self.scope_digest.as_bytes(),
                self.domain.as_str().as_bytes(),
                self.schema_id.as_str().as_bytes(),
                self.record_id.as_str().as_bytes(),
            ],
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum MutationPrecondition {
    RecordAbsent {
        target: RecordTarget,
    },
    RecordDigestEquals {
        target: RecordTarget,
        expected_digest: Digest256,
    },
    ScopeVersionEquals {
        expected_version: u64,
    },
    FenceEquals {
        expected_fence: u64,
    },
    PlacementEpochEquals {
        expected_epoch: u64,
    },
}

impl MutationPrecondition {
    pub(super) fn validate_for_scope(
        &self,
        tenant: &TenantId,
        scope: &AuthorityScope,
    ) -> Result<(), String> {
        match self {
            Self::RecordAbsent { target } | Self::RecordDigestEquals { target, .. } => {
                target.validate_for_scope(tenant, scope)
            }
            Self::ScopeVersionEquals { .. }
            | Self::FenceEquals { .. }
            | Self::PlacementEpochEquals { .. } => Ok(()),
        }
    }

    pub(super) fn digest(&self) -> Result<Digest256, String> {
        match self {
            Self::RecordAbsent { target } => {
                let target = target.digest()?;
                Digest256::framed(
                    b"eg/mutation-precondition/v1",
                    &[b"record_absent", target.as_bytes()],
                )
            }
            Self::RecordDigestEquals {
                target,
                expected_digest,
            } => {
                let target = target.digest()?;
                Digest256::framed(
                    b"eg/mutation-precondition/v1",
                    &[
                        b"record_digest_equals",
                        target.as_bytes(),
                        expected_digest.as_bytes(),
                    ],
                )
            }
            Self::ScopeVersionEquals { expected_version } => Digest256::framed(
                b"eg/mutation-precondition/v1",
                &[b"scope_version_equals", &expected_version.to_be_bytes()],
            ),
            Self::FenceEquals { expected_fence } => Digest256::framed(
                b"eg/mutation-precondition/v1",
                &[b"fence_equals", &expected_fence.to_be_bytes()],
            ),
            Self::PlacementEpochEquals { expected_epoch } => Digest256::framed(
                b"eg/mutation-precondition/v1",
                &[b"placement_epoch_equals", &expected_epoch.to_be_bytes()],
            ),
        }
    }
}

impl MutationBudgetCharge for MutationPrecondition {
    fn mutation_budget_charge(&self) -> Result<usize, String> {
        let mut total = PRECONDITION_FIXED_BUDGET;
        if let Self::RecordAbsent { target } | Self::RecordDigestEquals { target, .. } = self {
            for amount in [
                target.tenant.as_str().len(),
                64,
                target.schema_id.as_str().len(),
                target.record_id.as_str().len(),
            ] {
                total = total
                    .checked_add(amount)
                    .ok_or_else(|| "mutation structural budget overflow".to_string())?;
            }
        }
        Ok(total)
    }
}

impl<'de> MutationBudgetDeserialize<'de> for MutationPrecondition {
    fn deserialize_budgeted<D>(
        deserializer: D,
        budget: &mut StructuralBudget,
    ) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserialize_and_charge(deserializer, budget)
    }
}
