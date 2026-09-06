use super::budget::{MutationBudgetCharge, StructuralBudget};
use super::effects::MutationEffectV1;
use super::outbox_decode::ProvenanceBindingV1;
use super::targets::MutationPreconditionV1;
use super::{MAX_MUTATION_PRECONDITIONS, MAX_PROVENANCE_REFS};
use crate::authority::AuthorityScopeV1;
use crate::contract::{
    BoundedVecV1, Digest256V1, OpaqueIdV1, RequestedMutationResultV1, TenantIdV1,
    MAX_MUTATION_EFFECTS, MAX_OUTBOX_INTENTS,
};
use crate::outbox::OutboxIntentV1;
pub(super) fn validate_payload_shape_and_scope(
    tenant: &TenantIdV1,
    scope: &AuthorityScopeV1,
    preconditions: &BoundedVecV1<MutationPreconditionV1, MAX_MUTATION_PRECONDITIONS>,
    effects: &BoundedVecV1<MutationEffectV1, MAX_MUTATION_EFFECTS>,
    outbox: &BoundedVecV1<OutboxIntentV1, MAX_OUTBOX_INTENTS>,
    provenance: &BoundedVecV1<ProvenanceBindingV1, MAX_PROVENANCE_REFS>,
) -> Result<(), String> {
    if effects.is_empty() {
        return Err("mutation must contain 1..=4096 effects".into());
    }
    for precondition in preconditions {
        precondition.validate_for_scope(tenant, scope)?;
    }
    for effect in effects {
        effect.validate_for_scope(tenant, scope)?;
    }
    for intent in outbox {
        intent.validate_for_scope(tenant, scope)?;
    }
    if effects
        .iter()
        .enumerate()
        .any(|(index, effect)| effect.ordinal as usize != index)
    {
        return Err("mutation effect ordinals must be contiguous from zero".into());
    }
    if provenance
        .windows(2)
        .any(|pair| pair[0].provenance_id >= pair[1].provenance_id)
    {
        return Err("provenance bindings must be strictly sorted and unique".into());
    }
    Ok(())
}

pub(super) fn validate_payload_byte_budget(
    preconditions: &BoundedVecV1<MutationPreconditionV1, MAX_MUTATION_PRECONDITIONS>,
    effects: &BoundedVecV1<MutationEffectV1, MAX_MUTATION_EFFECTS>,
    outbox: &BoundedVecV1<OutboxIntentV1, MAX_OUTBOX_INTENTS>,
    provenance: &BoundedVecV1<ProvenanceBindingV1, MAX_PROVENANCE_REFS>,
) -> Result<(), String> {
    // Conservative upper bound over the named MessagePack representation: the
    // fixed charge covers authority/evidence scalars and map keys; repeated
    // elements charge fixed structure plus exact variable strings/bytes.
    let mut budget = StructuralBudget::new();
    for precondition in preconditions {
        budget.charge(precondition.mutation_budget_charge()?)?;
    }
    for effect in effects {
        budget.charge(effect.mutation_budget_charge()?)?;
    }
    for intent in outbox {
        budget.charge(intent.mutation_budget_charge()?)?;
    }
    for binding in provenance {
        budget.charge(binding.mutation_budget_charge()?)?;
    }
    Ok(())
}

pub(super) fn recompute_canonical_payload_digest(
    mutation_id: &OpaqueIdV1,
    scope: &AuthorityScopeV1,
    preconditions: &BoundedVecV1<MutationPreconditionV1, MAX_MUTATION_PRECONDITIONS>,
    effects: &BoundedVecV1<MutationEffectV1, MAX_MUTATION_EFFECTS>,
    outbox: &BoundedVecV1<OutboxIntentV1, MAX_OUTBOX_INTENTS>,
    provenance: &BoundedVecV1<ProvenanceBindingV1, MAX_PROVENANCE_REFS>,
    requested_result: &RequestedMutationResultV1,
) -> Result<Digest256V1, String> {
    let preconditions = digest_sequence(
        b"eg/mutation-preconditions/v1",
        preconditions.iter().map(MutationPreconditionV1::digest),
    )?;
    let effects = digest_sequence(
        b"eg/mutation-effects/v1",
        effects.iter().map(MutationEffectV1::digest),
    )?;
    let outbox = digest_sequence(
        b"eg/mutation-outbox/v1",
        outbox.iter().map(OutboxIntentV1::digest),
    )?;
    let provenance = digest_sequence(
        b"eg/mutation-provenance/v1",
        provenance.iter().map(ProvenanceBindingV1::digest),
    )?;
    let scope = scope.digest()?;
    Digest256V1::framed(
        b"eg/mutation-canonical-payload/v1",
        &[
            mutation_id.as_str().as_bytes(),
            scope.as_bytes(),
            requested_result.as_str().as_bytes(),
            preconditions.as_bytes(),
            effects.as_bytes(),
            outbox.as_bytes(),
            provenance.as_bytes(),
        ],
    )
}

pub(super) fn recompute_egress_authorization_digest(
    outbox: &BoundedVecV1<OutboxIntentV1, MAX_OUTBOX_INTENTS>,
) -> Result<Digest256V1, String> {
    digest_sequence(
        b"eg/mutation-egress-authorization/v1",
        outbox
            .iter()
            .map(OutboxIntentV1::destination_authorization_digest),
    )
}

pub(super) fn digest_sequence(
    domain: &[u8],
    digests: impl Iterator<Item = Result<Digest256V1, String>>,
) -> Result<Digest256V1, String> {
    let digests: Vec<Digest256V1> = digests.collect::<Result<_, _>>()?;
    let fields: Vec<&[u8]> = digests
        .iter()
        .map(|digest| digest.as_bytes().as_slice())
        .collect();
    Digest256V1::framed(domain, &fields)
}
