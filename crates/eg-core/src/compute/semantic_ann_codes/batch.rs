//! Mutation-batch construction for the serving scope.
//!
//! Building a batch writes nothing: a batch only becomes durable when the
//! serving-scope door admits it (see `door`).

use super::{OperationAttribution, SemanticCodeStore};
use eg_storage::{OwnedStoreHandle, SemanticIndexOwner};
use eg_types::contract::Digest256;
use eg_types::mutation_batch::{
    BatchContent, CompiledOperation, CompiledScope, DurabilityDomain, MutationEnvelope,
    MutationOutboxIntent, MutationSurface, VersionExpectation,
};
use eg_types::semantic_index::SemanticDigest;
use eg_types::{MutationBatch, MutationOperation, MUTATION_BATCH_VERSION};
use sha2::{Digest, Sha256};

/// What one semantic metadata mutation records.
///
/// The four are used together and only together: `batch_id`, `event_type` and
/// `subject` name what the ledger row wrote and what it wrote it for, and
/// `mutation_digest` is the content the recorded operation is addressed by.
/// Where the subject LANDS differs by surface, and neither surface drops it:
/// the maintenance envelope has a structural `subject` field, while the
/// operation envelope names the store and carries the subject in the operation
/// query instead (see `metadata_operation_query`). Every caller that has one
/// has all four. Flattened, it put three bare `&str` adjacent in an
/// argument list of up to twelve, where transposing two of them still compiles
/// and the ledger silently records the wrong subject.
pub(crate) struct MetadataMutation<'a> {
    pub(crate) batch_id: &'a str,
    pub(crate) event_type: &'a str,
    pub(crate) subject: &'a str,
    pub(crate) mutation_digest: SemanticDigest,
}

impl SemanticCodeStore {
    pub(super) fn metadata_batch(
        &self,
        owner: &OwnedStoreHandle<SemanticIndexOwner>,
        version: u64,
        mutation: MetadataMutation<'_>,
        outbox: Vec<MutationOutboxIntent>,
        created_at_ms: u64,
    ) -> Result<MutationBatch, String> {
        let MetadataMutation {
            batch_id,
            event_type,
            subject,
            mutation_digest,
        } = mutation;
        Ok(MutationBatch {
            schema_version: MUTATION_BATCH_VERSION,
            batch_id: batch_id.to_string(),
            envelope: MutationEnvelope::maintenance(
                owner.principal(),
                event_type,
                subject,
                batch_id,
            )?,
            identity: owner.identity().clone(),
            placement_epoch: 0,
            version_expectation: VersionExpectation::Native(version),
            fencing_token: None,
            authoritative_state: None,
            operations: vec![MutationOperation {
                ordinal: 0,
                surface: MutationSurface::Other,
                domain: DurabilityDomain::SemanticIndex,
                method: eg_types::protocol::Method::ApplyMutation {
                    event_type: event_type.to_string(),
                    query: format!("sha256:{}", mutation_digest.to_hex()),
                },
            }],
            outbox,
            created_at_ms,
        })
    }

    pub(super) fn metadata_operation_batch(
        &self,
        owner: &OwnedStoreHandle<SemanticIndexOwner>,
        version: u64,
        mutation: MetadataMutation<'_>,
        mut outbox: Vec<MutationOutboxIntent>,
        created_at_ms: u64,
        attribution: OperationAttribution<'_>,
    ) -> Result<MutationBatch, String> {
        let MetadataMutation {
            batch_id,
            event_type,
            subject,
            mutation_digest,
        } = mutation;
        let OperationAttribution {
            actor,
            idempotency_key,
            nonce,
        } = attribution;
        for intent in &mut outbox {
            intent
                .headers
                .entry("actor".to_string())
                .or_insert_with(|| actor.to_string());
        }
        // The OPERATION surface has no structural subject: `for_scope` names
        // the store, so the caller's finer-grained subject is recorded HERE, in
        // the operation, rather than discarded. See
        // [`metadata_operation_query`].
        let operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Other,
            domain: DurabilityDomain::SemanticIndex,
            method: eg_types::protocol::Method::ApplyMutation {
                event_type: event_type.to_string(),
                query: metadata_operation_query(subject, mutation_digest),
            },
        }];
        let operation = CompiledOperation::for_content(
            owner.identity(),
            BatchContent {
                operations: &operations,
                outbox: &outbox,
                authoritative_state: None,
            },
            Digest256::from_bytes([0; 32]),
        )?;
        let envelope = MutationEnvelope::for_scope(
            CompiledScope {
                identity: owner.identity(),
                actor,
                serving_principal: owner.principal(),
                request_id: created_at_ms,
                idempotency_key,
                nonce,
                now_ms: created_at_ms,
            },
            operation,
        )?;
        Ok(MutationBatch {
            schema_version: MUTATION_BATCH_VERSION,
            batch_id: batch_id.to_string(),
            envelope,
            identity: owner.identity().clone(),
            placement_epoch: 0,
            version_expectation: VersionExpectation::Native(version),
            fencing_token: None,
            authoritative_state: None,
            operations,
            outbox,
            created_at_ms,
        })
    }
}

pub(super) fn semantic_digest(bytes: &[u8]) -> SemanticDigest {
    let mut hasher = Sha256::new();
    hasher.update(b"eg/semantic-index-mutation/v1\0");
    hasher.update((bytes.len() as u64).to_be_bytes());
    hasher.update(bytes);
    SemanticDigest::from_bytes(hasher.finalize().into())
}

/// The operation-surface query that names BOTH the content written and the
/// finer-grained target it was written FOR.
///
/// `MutationEnvelope::for_scope` mints no subject, and an `OperationEnvelope`
/// has no subject field to mint one into: it names the STORE structurally, in
/// `purpose_resource`, which is the compiled scope's own resource. Per
/// `MutationEnvelope::maintenance_for_scope`'s own rule the finer-grained
/// target therefore travels in the OPERATION, and this field is where the
/// operation carries a free-form value -- the same thing
/// `eg_transaction::graft` does with its `/`-delimited `GraftIntent::encode`.
/// The digest stays the LAST `/` segment so one parser
/// ([`mutation_digest_from_batch`]) reads both this shape and the maintenance
/// surface's bare `sha256:{digest}`, including rows committed before the
/// subject was recorded.
fn metadata_operation_query(subject: &str, mutation_digest: SemanticDigest) -> String {
    format!("{subject}/{mutation_digest}")
}
