use std::fmt;

use serde::de::{Error as _, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Serialize};

use super::budget::{read_map_value_once, BudgetedVecSeed, StructuralBudget};
use super::effects::MutationEffect;
use super::outbox_decode::ProvenanceBinding;
use super::payload::{
    digest_sequence, recompute_canonical_payload_digest, recompute_egress_authorization_digest,
    validate_payload_byte_budget, validate_payload_shape_and_scope,
};
use super::targets::MutationPrecondition;
use super::{MAX_MUTATION_PRECONDITIONS, MAX_PROVENANCE_REFS};
use crate::authority::{
    AuthorityScope, NonceReplayKey, OperationReplayIdentity, VerifiedAuthority,
};
use crate::contract::{
    BoundedVec, Digest256, OpaqueId, RequestedMutationResult, ResourceId,
    MAX_MUTATION_EFFECTS, MAX_OUTBOX_INTENTS,
};
use crate::outbox::OutboxIntent;
/// Compiler-produced mutation payload before authority evidence is attached.
/// It is intentionally not deserializable: producers use the checked
/// constructor and then present its digests to the trusted admission boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MutationPayload {
    pub(super) mutation_id: OpaqueId,
    pub(super) scope: AuthorityScope,
    pub(super) preconditions: BoundedVec<MutationPrecondition, MAX_MUTATION_PRECONDITIONS>,
    pub(super) effects: BoundedVec<MutationEffect, MAX_MUTATION_EFFECTS>,
    pub(super) outbox: BoundedVec<OutboxIntent, MAX_OUTBOX_INTENTS>,
    pub(super) provenance: BoundedVec<ProvenanceBinding, MAX_PROVENANCE_REFS>,
    pub(super) requested_result: RequestedMutationResult,
}

impl MutationPayload {
    pub fn new(
        mutation_id: OpaqueId,
        scope: AuthorityScope,
        preconditions: BoundedVec<MutationPrecondition, MAX_MUTATION_PRECONDITIONS>,
        effects: BoundedVec<MutationEffect, MAX_MUTATION_EFFECTS>,
        outbox: BoundedVec<OutboxIntent, MAX_OUTBOX_INTENTS>,
        provenance: BoundedVec<ProvenanceBinding, MAX_PROVENANCE_REFS>,
        requested_result: RequestedMutationResult,
    ) -> Result<Self, String> {
        let payload = Self {
            mutation_id,
            scope,
            preconditions,
            effects,
            outbox,
            provenance,
            requested_result,
        };
        payload.validate()?;
        Ok(payload)
    }

    pub(super) fn validate(&self) -> Result<(), String> {
        self.scope.validate()?;
        let tenant = self
            .scope
            .tenant
            .as_ref()
            .ok_or_else(|| "mutation payload requires a tenant scope".to_string())?;
        validate_payload_shape_and_scope(
            tenant,
            &self.scope,
            &self.preconditions,
            &self.effects,
            &self.outbox,
            &self.provenance,
        )?;
        validate_payload_byte_budget(
            &self.preconditions,
            &self.effects,
            &self.outbox,
            &self.provenance,
        )
    }

    pub fn canonical_payload_digest(&self) -> Result<Digest256, String> {
        recompute_canonical_payload_digest(
            &self.mutation_id,
            &self.scope,
            &self.preconditions,
            &self.effects,
            &self.outbox,
            &self.provenance,
            &self.requested_result,
        )
    }

    pub fn egress_authorization_digest(&self) -> Result<Digest256, String> {
        recompute_egress_authorization_digest(&self.outbox)
    }

    pub fn effect_digest(&self) -> Result<Digest256, String> {
        digest_sequence(
            b"eg/mutation-effects/v1",
            self.effects.iter().map(MutationEffect::digest),
        )
    }
}

/// Non-deserializable assembly input for the untrusted wire envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MutationRequestEnvelopeParts {
    pub payload: MutationPayload,
    pub verified_authority: VerifiedAuthority,
    pub operation_identity: OperationReplayIdentity,
    pub nonce_replay_key: NonceReplayKey,
}

impl MutationRequestEnvelopeParts {
    pub fn new(
        payload: MutationPayload,
        verified_authority: VerifiedAuthority,
        operation_identity: OperationReplayIdentity,
        nonce_replay_key: NonceReplayKey,
    ) -> Self {
        Self {
            payload,
            verified_authority,
            operation_identity,
            nonce_replay_key,
        }
    }
}

/// Untrusted serialized mutation request and evidence. Structural validation
/// makes this DTO safe to inspect and persist, but never admits execution.
///
/// The future `eg-transaction::MutationKernel` must accept a separate
/// crate-private, non-`Deserialize`, non-`Clone` admitted plan/token minted only
/// after trusted cryptographic, key-registry, current-time, policy, and durable
/// replay verification. That executable type must not live in `eg-types`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MutationRequestEnvelope {
    pub(super) schema_version: ResourceId,
    pub(super) mutation_id: OpaqueId,
    pub(super) verified_authority: VerifiedAuthority,
    pub(super) operation_identity: OperationReplayIdentity,
    pub(super) operation_replay_digest: Digest256,
    pub(super) nonce_replay_key: NonceReplayKey,
    pub(super) nonce_replay_digest: Digest256,
    pub(super) scope: AuthorityScope,
    pub(super) preconditions: BoundedVec<MutationPrecondition, MAX_MUTATION_PRECONDITIONS>,
    pub(super) effects: BoundedVec<MutationEffect, MAX_MUTATION_EFFECTS>,
    pub(super) outbox: BoundedVec<OutboxIntent, MAX_OUTBOX_INTENTS>,
    pub(super) provenance: BoundedVec<ProvenanceBinding, MAX_PROVENANCE_REFS>,
    pub(super) requested_result: RequestedMutationResult,
    pub(super) canonical_payload_digest: Digest256,
    pub(super) envelope_digest: Digest256,
}

#[derive(Deserialize)]
#[serde(field_identifier, rename_all = "snake_case")]
enum MutationRequestEnvelopeField {
    SchemaVersion,
    MutationId,
    VerifiedAuthority,
    OperationIdentity,
    OperationReplayDigest,
    NonceReplayKey,
    NonceReplayDigest,
    Scope,
    Preconditions,
    Effects,
    Outbox,
    Provenance,
    RequestedResult,
    CanonicalPayloadDigest,
    EnvelopeDigest,
}

struct MutationRequestEnvelopeVisitor;

struct MutationRequestEnvelopeFields {
    budget: StructuralBudget,
    schema_version: Option<ResourceId>,
    mutation_id: Option<OpaqueId>,
    verified_authority: Option<VerifiedAuthority>,
    operation_identity: Option<OperationReplayIdentity>,
    operation_replay_digest: Option<Digest256>,
    nonce_replay_key: Option<NonceReplayKey>,
    nonce_replay_digest: Option<Digest256>,
    scope: Option<AuthorityScope>,
    preconditions: Option<BoundedVec<MutationPrecondition, MAX_MUTATION_PRECONDITIONS>>,
    effects: Option<BoundedVec<MutationEffect, MAX_MUTATION_EFFECTS>>,
    outbox: Option<BoundedVec<OutboxIntent, MAX_OUTBOX_INTENTS>>,
    provenance: Option<BoundedVec<ProvenanceBinding, MAX_PROVENANCE_REFS>>,
    requested_result: Option<RequestedMutationResult>,
    canonical_payload_digest: Option<Digest256>,
    envelope_digest: Option<Digest256>,
}

impl MutationRequestEnvelopeFields {
    fn new() -> Self {
        Self {
            budget: StructuralBudget::new(),
            schema_version: None,
            mutation_id: None,
            verified_authority: None,
            operation_identity: None,
            operation_replay_digest: None,
            nonce_replay_key: None,
            nonce_replay_digest: None,
            scope: None,
            preconditions: None,
            effects: None,
            outbox: None,
            provenance: None,
            requested_result: None,
            canonical_payload_digest: None,
            envelope_digest: None,
        }
    }
}

fn read_identity_field<'de, A>(
    fields: &mut MutationRequestEnvelopeFields,
    field: MutationRequestEnvelopeField,
    map: &mut A,
) -> Result<(), A::Error>
where
    A: MapAccess<'de>,
{
    match field {
        MutationRequestEnvelopeField::SchemaVersion => {
            read_map_value_once(&mut fields.schema_version, "schema_version", map)
        }
        MutationRequestEnvelopeField::MutationId => {
            read_map_value_once(&mut fields.mutation_id, "mutation_id", map)
        }
        MutationRequestEnvelopeField::VerifiedAuthority => {
            read_map_value_once(&mut fields.verified_authority, "verified_authority", map)
        }
        MutationRequestEnvelopeField::OperationIdentity => {
            read_map_value_once(&mut fields.operation_identity, "operation_identity", map)
        }
        MutationRequestEnvelopeField::NonceReplayKey => {
            read_map_value_once(&mut fields.nonce_replay_key, "nonce_replay_key", map)
        }
        MutationRequestEnvelopeField::Scope => read_map_value_once(&mut fields.scope, "scope", map),
        _ => unreachable!("identity field dispatcher received a different field group"),
    }
}

fn read_digest_field<'de, A>(
    fields: &mut MutationRequestEnvelopeFields,
    field: MutationRequestEnvelopeField,
    map: &mut A,
) -> Result<(), A::Error>
where
    A: MapAccess<'de>,
{
    match field {
        MutationRequestEnvelopeField::OperationReplayDigest => read_map_value_once(
            &mut fields.operation_replay_digest,
            "operation_replay_digest",
            map,
        ),
        MutationRequestEnvelopeField::NonceReplayDigest => {
            read_map_value_once(&mut fields.nonce_replay_digest, "nonce_replay_digest", map)
        }
        MutationRequestEnvelopeField::RequestedResult => {
            read_map_value_once(&mut fields.requested_result, "requested_result", map)
        }
        MutationRequestEnvelopeField::CanonicalPayloadDigest => read_map_value_once(
            &mut fields.canonical_payload_digest,
            "canonical_payload_digest",
            map,
        ),
        MutationRequestEnvelopeField::EnvelopeDigest => {
            read_map_value_once(&mut fields.envelope_digest, "envelope_digest", map)
        }
        _ => unreachable!("digest field dispatcher received a different field group"),
    }
}

fn read_collection_field<'de, A>(
    fields: &mut MutationRequestEnvelopeFields,
    field: MutationRequestEnvelopeField,
    map: &mut A,
) -> Result<(), A::Error>
where
    A: MapAccess<'de>,
{
    match field {
        MutationRequestEnvelopeField::Preconditions => read_budgeted_collection(
            &mut fields.preconditions,
            "preconditions",
            map,
            &mut fields.budget,
        ),
        MutationRequestEnvelopeField::Effects => {
            read_budgeted_collection(&mut fields.effects, "effects", map, &mut fields.budget)
        }
        MutationRequestEnvelopeField::Outbox => {
            read_budgeted_collection(&mut fields.outbox, "outbox", map, &mut fields.budget)
        }
        MutationRequestEnvelopeField::Provenance => read_budgeted_collection(
            &mut fields.provenance,
            "provenance",
            map,
            &mut fields.budget,
        ),
        _ => unreachable!("collection dispatcher received a different field group"),
    }
}

fn read_budgeted_collection<'de, T, A, const MAXIMUM: usize>(
    slot: &mut Option<BoundedVec<T, MAXIMUM>>,
    field: &'static str,
    map: &mut A,
    budget: &mut StructuralBudget,
) -> Result<(), A::Error>
where
    T: super::budget::MutationBudgetDeserialize<'de>,
    A: MapAccess<'de>,
{
    if slot.is_some() {
        return Err(A::Error::duplicate_field(field));
    }
    *slot = Some(map.next_value_seed(BudgetedVecSeed::new(budget))?);
    Ok(())
}

fn read_envelope_field<'de, A>(
    fields: &mut MutationRequestEnvelopeFields,
    field: MutationRequestEnvelopeField,
    map: &mut A,
) -> Result<(), A::Error>
where
    A: MapAccess<'de>,
{
    match field {
        MutationRequestEnvelopeField::SchemaVersion
        | MutationRequestEnvelopeField::MutationId
        | MutationRequestEnvelopeField::VerifiedAuthority
        | MutationRequestEnvelopeField::OperationIdentity
        | MutationRequestEnvelopeField::NonceReplayKey
        | MutationRequestEnvelopeField::Scope => read_identity_field(fields, field, map),
        MutationRequestEnvelopeField::OperationReplayDigest
        | MutationRequestEnvelopeField::NonceReplayDigest
        | MutationRequestEnvelopeField::RequestedResult
        | MutationRequestEnvelopeField::CanonicalPayloadDigest
        | MutationRequestEnvelopeField::EnvelopeDigest => read_digest_field(fields, field, map),
        MutationRequestEnvelopeField::Preconditions
        | MutationRequestEnvelopeField::Effects
        | MutationRequestEnvelopeField::Outbox
        | MutationRequestEnvelopeField::Provenance => read_collection_field(fields, field, map),
    }
}

fn require_field<T, E>(value: Option<T>, field: &'static str) -> Result<T, E>
where
    E: serde::de::Error,
{
    value.ok_or_else(|| E::missing_field(field))
}

fn finish_envelope<E>(fields: MutationRequestEnvelopeFields) -> Result<MutationRequestEnvelope, E>
where
    E: serde::de::Error,
{
    Ok(MutationRequestEnvelope {
        schema_version: require_field(fields.schema_version, "schema_version")?,
        mutation_id: require_field(fields.mutation_id, "mutation_id")?,
        verified_authority: require_field(fields.verified_authority, "verified_authority")?,
        operation_identity: require_field(fields.operation_identity, "operation_identity")?,
        operation_replay_digest: require_field(
            fields.operation_replay_digest,
            "operation_replay_digest",
        )?,
        nonce_replay_key: require_field(fields.nonce_replay_key, "nonce_replay_key")?,
        nonce_replay_digest: require_field(fields.nonce_replay_digest, "nonce_replay_digest")?,
        scope: require_field(fields.scope, "scope")?,
        preconditions: require_field(fields.preconditions, "preconditions")?,
        effects: require_field(fields.effects, "effects")?,
        outbox: require_field(fields.outbox, "outbox")?,
        provenance: require_field(fields.provenance, "provenance")?,
        requested_result: require_field(fields.requested_result, "requested_result")?,
        canonical_payload_digest: require_field(
            fields.canonical_payload_digest,
            "canonical_payload_digest",
        )?,
        envelope_digest: require_field(fields.envelope_digest, "envelope_digest")?,
    })
}

impl<'de> Visitor<'de> for MutationRequestEnvelopeVisitor {
    type Value = MutationRequestEnvelope;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a named untrusted mutation-envelope.v1 map")
    }

    fn visit_seq<A>(self, _sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        Err(A::Error::custom(
            "MutationRequestEnvelope requires a named map; positional sequences are forbidden",
        ))
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut fields = MutationRequestEnvelopeFields::new();
        while let Some(field) = map.next_key::<MutationRequestEnvelopeField>()? {
            read_envelope_field(&mut fields, field, &mut map)?;
        }
        finish_envelope::<A::Error>(fields)
    }
}

impl<'de> Deserialize<'de> for MutationRequestEnvelope {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "schema_version",
            "mutation_id",
            "verified_authority",
            "operation_identity",
            "operation_replay_digest",
            "nonce_replay_key",
            "nonce_replay_digest",
            "scope",
            "preconditions",
            "effects",
            "outbox",
            "provenance",
            "requested_result",
            "canonical_payload_digest",
            "envelope_digest",
        ];
        let envelope = deserializer.deserialize_struct(
            "MutationRequestEnvelope",
            FIELDS,
            MutationRequestEnvelopeVisitor,
        )?;
        envelope
            .validate_untrusted_request()
            .map_err(serde::de::Error::custom)?;
        Ok(envelope)
    }
}
