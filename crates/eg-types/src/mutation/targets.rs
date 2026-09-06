use serde::{Deserialize, Deserializer, Serialize};

use super::budget::{
    deserialize_and_charge, MutationBudgetCharge, MutationBudgetDeserialize, StructuralBudget,
    PRECONDITION_FIXED_BUDGET,
};
use super::MutationDomainV1;
use crate::authority::AuthorityScopeV1;
use crate::contract::{Digest256V1, ResourceIdV1, SchemaIdV1, TenantIdV1};
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordTargetV1 {
    pub tenant: TenantIdV1,
    pub scope_digest: Digest256V1,
    pub domain: MutationDomainV1,
    pub schema_id: SchemaIdV1,
    pub record_id: ResourceIdV1,
}

impl RecordTargetV1 {
    pub(super) fn validate_for_scope(
        &self,
        tenant: &TenantIdV1,
        scope: &AuthorityScopeV1,
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

    pub(super) fn digest(&self) -> Result<Digest256V1, String> {
        Digest256V1::framed(
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
pub enum MutationPreconditionV1 {
    RecordAbsent {
        target: RecordTargetV1,
    },
    RecordDigestEquals {
        target: RecordTargetV1,
        expected_digest: Digest256V1,
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

impl MutationPreconditionV1 {
    pub(super) fn validate_for_scope(
        &self,
        tenant: &TenantIdV1,
        scope: &AuthorityScopeV1,
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

    pub(super) fn digest(&self) -> Result<Digest256V1, String> {
        match self {
            Self::RecordAbsent { target } => {
                let target = target.digest()?;
                Digest256V1::framed(
                    b"eg/mutation-precondition/v1",
                    &[b"record_absent", target.as_bytes()],
                )
            }
            Self::RecordDigestEquals {
                target,
                expected_digest,
            } => {
                let target = target.digest()?;
                Digest256V1::framed(
                    b"eg/mutation-precondition/v1",
                    &[
                        b"record_digest_equals",
                        target.as_bytes(),
                        expected_digest.as_bytes(),
                    ],
                )
            }
            Self::ScopeVersionEquals { expected_version } => Digest256V1::framed(
                b"eg/mutation-precondition/v1",
                &[b"scope_version_equals", &expected_version.to_be_bytes()],
            ),
            Self::FenceEquals { expected_fence } => Digest256V1::framed(
                b"eg/mutation-precondition/v1",
                &[b"fence_equals", &expected_fence.to_be_bytes()],
            ),
            Self::PlacementEpochEquals { expected_epoch } => Digest256V1::framed(
                b"eg/mutation-precondition/v1",
                &[b"placement_epoch_equals", &expected_epoch.to_be_bytes()],
            ),
        }
    }
}

impl MutationBudgetCharge for MutationPreconditionV1 {
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

impl<'de> MutationBudgetDeserialize<'de> for MutationPreconditionV1 {
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
