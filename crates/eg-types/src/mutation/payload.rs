use super::budget::{MutationBudgetCharge, StructuralBudget};
use super::effects::MutationEffect;
use super::outbox_decode::ProvenanceBinding;
use super::targets::MutationPrecondition;
use super::{MAX_MUTATION_PRECONDITIONS, MAX_PROVENANCE_REFS};
use crate::authority::AuthorityScope;
use crate::contract::{
    BoundedVec, Digest256, OpaqueId, RequestedMutationResult, TenantId,
    MAX_MUTATION_EFFECTS, MAX_OUTBOX_INTENTS,
};
use crate::outbox::OutboxIntent;
pub(super) fn validate_payload_shape_and_scope(
    tenant: &TenantId,
    scope: &AuthorityScope,
    preconditions: &BoundedVec<MutationPrecondition, MAX_MUTATION_PRECONDITIONS>,
    effects: &BoundedVec<MutationEffect, MAX_MUTATION_EFFECTS>,
    outbox: &BoundedVec<OutboxIntent, MAX_OUTBOX_INTENTS>,
    provenance: &BoundedVec<ProvenanceBinding, MAX_PROVENANCE_REFS>,
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
    preconditions: &BoundedVec<MutationPrecondition, MAX_MUTATION_PRECONDITIONS>,
    effects: &BoundedVec<MutationEffect, MAX_MUTATION_EFFECTS>,
    outbox: &BoundedVec<OutboxIntent, MAX_OUTBOX_INTENTS>,
    provenance: &BoundedVec<ProvenanceBinding, MAX_PROVENANCE_REFS>,
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
    mutation_id: &OpaqueId,
    scope: &AuthorityScope,
    preconditions: &BoundedVec<MutationPrecondition, MAX_MUTATION_PRECONDITIONS>,
    effects: &BoundedVec<MutationEffect, MAX_MUTATION_EFFECTS>,
    outbox: &BoundedVec<OutboxIntent, MAX_OUTBOX_INTENTS>,
    provenance: &BoundedVec<ProvenanceBinding, MAX_PROVENANCE_REFS>,
    requested_result: &RequestedMutationResult,
) -> Result<Digest256, String> {
    let preconditions = digest_sequence(
        b"eg/mutation-preconditions/v1",
        preconditions.iter().map(MutationPrecondition::digest),
    )?;
    let effects = digest_sequence(
        b"eg/mutation-effects/v1",
        effects.iter().map(MutationEffect::digest),
    )?;
    let outbox = digest_sequence(
        b"eg/mutation-outbox/v1",
        outbox.iter().map(OutboxIntent::digest),
    )?;
    let provenance = digest_sequence(
        b"eg/mutation-provenance/v1",
        provenance.iter().map(ProvenanceBinding::digest),
    )?;
    let scope = scope.digest()?;
    Digest256::framed(
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
    outbox: &BoundedVec<OutboxIntent, MAX_OUTBOX_INTENTS>,
) -> Result<Digest256, String> {
    digest_sequence(
        b"eg/mutation-egress-authorization/v1",
        outbox
            .iter()
            .map(OutboxIntent::destination_authorization_digest),
    )
}

pub(super) fn digest_sequence(
    domain: &[u8],
    digests: impl Iterator<Item = Result<Digest256, String>>,
) -> Result<Digest256, String> {
    let digests: Vec<Digest256> = digests.collect::<Result<_, _>>()?;
    let fields: Vec<&[u8]> = digests
        .iter()
        .map(|digest| digest.as_bytes().as_slice())
        .collect();
    Digest256::framed(domain, &fields)
}
