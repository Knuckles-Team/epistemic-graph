use serde::{Deserialize, Serialize};

use super::budget::MutationBudgetCharge;
use super::targets::RecordTargetV1;
use super::EFFECT_FIXED_BUDGET;
use crate::authority::AuthorityScopeV1;
use crate::contract::{Digest256V1, RecordBytesV1, TenantIdV1};
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RecordMutationV1 {
    Put {
        payload: RecordBytesV1,
        payload_digest: Digest256V1,
    },
    Delete {
        expected_record_digest: Digest256V1,
    },
}

impl RecordMutationV1 {
    pub(super) fn validate(&self) -> Result<(), String> {
        if let Self::Put {
            payload,
            payload_digest,
        } = self
        {
            if payload.digest()? != *payload_digest {
                return Err("mutation record payload digest does not match its bytes".into());
            }
        }
        Ok(())
    }

    pub(super) fn digest(&self) -> Result<Digest256V1, String> {
        self.validate()?;
        match self {
            Self::Put { payload_digest, .. } => Digest256V1::framed(
                b"eg/record-mutation/v1",
                &[b"put", payload_digest.as_bytes()],
            ),
            Self::Delete {
                expected_record_digest,
            } => Digest256V1::framed(
                b"eg/record-mutation/v1",
                &[b"delete", expected_record_digest.as_bytes()],
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MutationEffectV1 {
    pub ordinal: u32,
    pub target: RecordTargetV1,
    pub mutation: RecordMutationV1,
}

impl MutationEffectV1 {
    pub(super) fn validate_for_scope(
        &self,
        tenant: &TenantIdV1,
        scope: &AuthorityScopeV1,
    ) -> Result<(), String> {
        self.target.validate_for_scope(tenant, scope)?;
        self.mutation.validate()
    }

    pub(super) fn digest(&self) -> Result<Digest256V1, String> {
        self.mutation.validate()?;
        let ordinal = self.ordinal.to_be_bytes();
        let target = self.target.digest()?;
        let mutation = self.mutation.digest()?;
        Digest256V1::framed(
            b"eg/mutation-effect/v1",
            &[&ordinal, target.as_bytes(), mutation.as_bytes()],
        )
    }
}

impl MutationBudgetCharge for MutationEffectV1 {
    fn mutation_budget_charge(&self) -> Result<usize, String> {
        let payload = match &self.mutation {
            RecordMutationV1::Put { payload, .. } => payload.as_slice().len(),
            RecordMutationV1::Delete { .. } => 0,
        };
        [
            EFFECT_FIXED_BUDGET,
            self.target.tenant.as_str().len(),
            64,
            self.target.schema_id.as_str().len(),
            self.target.record_id.as_str().len(),
            payload,
        ]
        .into_iter()
        .try_fold(0usize, |total, amount| {
            total
                .checked_add(amount)
                .ok_or_else(|| "mutation structural budget overflow".to_string())
        })
    }
}
