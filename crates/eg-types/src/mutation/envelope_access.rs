use super::effects::MutationEffectV1;
use super::envelope::MutationEnvelopeV1;
use super::outbox_decode::ProvenanceBindingV1;
use super::targets::MutationPreconditionV1;
use super::{MAX_MUTATION_PRECONDITIONS, MAX_PROVENANCE_REFS};
use crate::authority::{
    AuthorityScopeV1, NonceReplayKeyV1, OperationReplayIdentityV1, VerifiedAuthorityV1,
};
use crate::contract::{
    BoundedVecV1, OpaqueIdV1, ResourceIdV1, MAX_MUTATION_EFFECTS, MAX_OUTBOX_INTENTS,
};
use crate::outbox::OutboxIntentV1;

impl MutationEnvelopeV1 {
    pub fn schema_version(&self) -> &ResourceIdV1 {
        &self.schema_version
    }

    pub fn mutation_id(&self) -> &OpaqueIdV1 {
        &self.mutation_id
    }

    pub fn scope(&self) -> &AuthorityScopeV1 {
        &self.scope
    }

    pub fn verified_authority(&self) -> &VerifiedAuthorityV1 {
        &self.verified_authority
    }

    pub fn operation_identity(&self) -> &OperationReplayIdentityV1 {
        &self.operation_identity
    }

    pub fn nonce_replay_key(&self) -> &NonceReplayKeyV1 {
        &self.nonce_replay_key
    }

    pub fn preconditions(
        &self,
    ) -> &BoundedVecV1<MutationPreconditionV1, MAX_MUTATION_PRECONDITIONS> {
        &self.preconditions
    }

    pub fn effects(&self) -> &BoundedVecV1<MutationEffectV1, MAX_MUTATION_EFFECTS> {
        &self.effects
    }

    pub fn outbox(&self) -> &BoundedVecV1<OutboxIntentV1, MAX_OUTBOX_INTENTS> {
        &self.outbox
    }

    pub fn provenance(&self) -> &BoundedVecV1<ProvenanceBindingV1, MAX_PROVENANCE_REFS> {
        &self.provenance
    }
}
