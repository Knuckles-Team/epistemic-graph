use serde::{Deserialize, Serialize};

use super::budget::MutationBudgetCharge;
use super::targets::RecordTarget;
use super::EFFECT_FIXED_BUDGET;
use crate::authority::AuthorityScope;
use crate::contract::{Digest256, RecordBytes, TenantId};
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RecordMutation {
    Put {
        payload: RecordBytes,
        payload_digest: Digest256,
    },
    Delete {
        expected_record_digest: Digest256,
    },
}

impl RecordMutation {
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

    pub(super) fn digest(&self) -> Result<Digest256, String> {
        self.validate()?;
        match self {
            Self::Put { payload_digest, .. } => Digest256::framed(
                b"eg/record-mutation/v1",
                &[b"put", payload_digest.as_bytes()],
            ),
            Self::Delete {
                expected_record_digest,
            } => Digest256::framed(
                b"eg/record-mutation/v1",
                &[b"delete", expected_record_digest.as_bytes()],
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MutationEffect {
    pub ordinal: u32,
    pub target: RecordTarget,
    pub mutation: RecordMutation,
}

impl MutationEffect {
    pub(super) fn validate_for_scope(
        &self,
        tenant: &TenantId,
        scope: &AuthorityScope,
    ) -> Result<(), String> {
        self.target.validate_for_scope(tenant, scope)?;
        self.mutation.validate()
    }

    pub(super) fn digest(&self) -> Result<Digest256, String> {
        self.mutation.validate()?;
        let ordinal = self.ordinal.to_be_bytes();
        let target = self.target.digest()?;
        let mutation = self.mutation.digest()?;
        Digest256::framed(
            b"eg/mutation-effect/v1",
            &[&ordinal, target.as_bytes(), mutation.as_bytes()],
        )
    }
}

impl MutationBudgetCharge for MutationEffect {
    fn mutation_budget_charge(&self) -> Result<usize, String> {
        let payload = match &self.mutation {
            RecordMutation::Put { payload, .. } => payload.as_slice().len(),
            RecordMutation::Delete { .. } => 0,
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
