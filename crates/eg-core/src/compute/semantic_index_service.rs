//! Native semantic-index admission and consumer fencing.
//!
//! This is the EG side of the semantic lifecycle boundary.  It owns typed
//! binding/S1 admission and the proof checks a future S2-S6 consumer must make
//! before doing work.  Durable delivery, retry, fairness, acknowledgement and
//! dead-letter persistence remain the existing mutation-outbox ports; this
//! module deliberately does not create another scheduler or execute a model in
//! a request path.

#![cfg(feature = "ann-redb")]

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;

use eg_storage::ScopeGrantVerifier;
use eg_transaction::{OutboxClaimBudget, OutboxClaimOutcome, OutboxStatus};
use eg_types::contract::Nonce;
use eg_types::mutation_batch::{MutationOutboxLease, MutationOutboxRecord};
use eg_types::semantic_index::{
    SemanticAuthorizationReceipt, SemanticAuthorizationReceiptDraft, SemanticBinding,
    SemanticBindingState, SemanticDigest, SemanticGenerationArtifact, SemanticGenerationCheckpoint,
    SemanticIndexError, SemanticIndexFilter, SemanticSourceDirtyIntent, SemanticSqlSourceManifest,
    SemanticSqlSourceManifestDraft, SemanticStage, SemanticStageArtifact, SemanticStageIntent,
    SemanticStageIntentDraft, SemanticStageScope, SemanticStageTransition,
};
use sha2::{Digest, Sha256};

use super::semantic::SemanticGenerationImage;
use super::semantic_ann_codes::{
    semantic_contract_error, SemanticCodeError, SemanticCodeStore, SemanticMutationReceipt,
    SemanticSourceReconciliationCheckpoint, SemanticSourceReconciliationPhase,
};

/// Construct the canonical tenant-wide SQL source revision returned by E3.
///
/// The event-local native scope is only a wakeup coordinate.  It cannot be
/// used as the source authority because direct, pgwire and SQLite ingress may
/// commit the same tenant table through different native resources.  E3 reads
/// this authority and epoch atomically with the source page and supplies them
/// here; the resulting value is the only revision accepted by source-page
/// coalescing.
pub fn sql_source_revision_for_epoch(
    authority_digest: SemanticDigest,
    source_epoch: u64,
) -> Result<String, SemanticIndexError> {
    if source_epoch == 0 {
        return Err(SemanticIndexError::InvalidField {
            field: "source_epoch".to_string(),
            reason: "tenant SQL source epoch must be nonzero".to_string(),
        });
    }
    Ok(format!(
        "sql-source:{authority_digest}:epoch:{source_epoch}"
    ))
}

/// The raw value resolved from the authoritative SQL source.
///
/// This is deliberately not [`SemanticSqlSourceManifest`].  A manifest is a
/// completed S1 artifact and therefore contains completion receipt/time fields
/// that do not exist while the source read is being admitted.  The read port
/// returns the exact source bytes or an explicit deletion proof; the S1 store
/// derives the content digest and creates the completion manifest later.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SemanticSqlSourceValue {
    Present {
        source_bytes: Vec<u8>,
    },
    Tombstone {
        deletion_proof_digest: SemanticDigest,
    },
}

/// One authorized row observation returned by the SQL source owner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SemanticSqlSourceRecord {
    pub source_identity: eg_types::semantic_index::SemanticSqlSourceIdentity,
    pub source_revision: String,
    pub value: SemanticSqlSourceValue,
    pub source_schema_revision: u64,
    pub source_schema_digest: String,
    pub source_field_set_digest: String,
    pub source_acl_revision: u64,
    pub source_acl_digest: String,
    pub authorization_receipt_digest: SemanticDigest,
}

impl SemanticSqlSourceRecord {
    pub fn source_entity_id(&self) -> String {
        self.source_identity.source_entity_id()
    }

    pub fn is_tombstone(&self) -> bool {
        matches!(self.value, SemanticSqlSourceValue::Tombstone { .. })
    }

    fn source_bytes_len(&self) -> usize {
        match &self.value {
            SemanticSqlSourceValue::Present { source_bytes } => source_bytes.len(),
            SemanticSqlSourceValue::Tombstone { .. } => 0,
        }
    }

    /// Return the canonical digest bound to the admitted source value.
    ///
    /// Present values hash their exact bytes; tombstones carry their
    /// authenticated deletion proof. Server adapters use this method when
    /// binding a leased intent to a typed source record.
    pub fn source_content_digest(&self) -> SemanticDigest {
        match &self.value {
            SemanticSqlSourceValue::Present { source_bytes } => {
                SemanticDigest::from_bytes(Sha256::digest(source_bytes).into())
            }
            SemanticSqlSourceValue::Tombstone {
                deletion_proof_digest,
            } => *deletion_proof_digest,
        }
    }

    fn validate(&self) -> Result<(), SemanticIndexError> {
        self.source_identity.validate()?;
        if self.source_revision.trim().is_empty()
            || self.source_schema_digest.trim().is_empty()
            || self.source_field_set_digest.trim().is_empty()
            || self.source_acl_digest.trim().is_empty()
            || self.source_acl_revision == 0
            || self.authorization_receipt_digest == SemanticDigest::from_bytes([0; 32])
        {
            return Err(SemanticIndexError::SourceManifestMismatch);
        }
        Ok(())
    }

    /// Turn the SQL owner's raw policy decision digest into the complete
    /// semantic authorization receipt used by S1 artifacts.  The adapter
    /// intentionally supplies only the decision digest; the binding supplies
    /// the governed actor/purpose/policy coordinates, and the S1 consumer
    /// supplies the authoritative authorization time when it completes work.
    pub fn authorization_receipt(
        &self,
        binding: &SemanticBinding,
        authorized_at: &str,
    ) -> Result<SemanticAuthorizationReceipt, SemanticIndexError> {
        binding.validate()?;
        validate_source_record_against_binding(binding, self)?;
        SemanticAuthorizationReceipt::create(SemanticAuthorizationReceiptDraft {
            tenant_id: binding.tenant_id.clone(),
            actor_scope: binding.actor_scope.clone(),
            effective_actor_scope: binding.effective_actor_scope.clone(),
            purpose_id: binding.purpose_id.clone(),
            policy_identity: binding.policy_identity.clone(),
            policy_decision_digest: self.authorization_receipt_digest.to_string(),
            binding_id: binding.binding_id.clone(),
            binding_digest: binding.binding_digest,
            generation: binding.generation,
            scope: SemanticStageScope::Entity {
                source_entity_id: self.source_entity_id(),
            },
            source_revision: self.source_revision.clone(),
            authorized_at: authorized_at.to_string(),
        })
    }
}

/// One bounded response from the authoritative SQL read port.
///
/// A partial page must carry an opaque continuation cursor.  A complete page
/// has no cursor.  Tombstones are accepted only on a complete reconciliation
/// page, so a missing row in an intermediate page can never be interpreted as
/// a deletion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SemanticSqlSourceReadPage {
    /// One E3 tenant-wide authority/epoch revision for every row in this
    /// snapshot.  It remains present when the page is empty, which is what
    /// makes an all-rows-deleted complete scan representable.
    pub source_revision: String,
    /// A complete scan must carry the authenticated read proof used to derive
    /// deletion tombstones.  Partial pages deliberately carry no such proof.
    pub complete_snapshot_receipt_digest: Option<SemanticDigest>,
    pub sources: Vec<SemanticSqlSourceRecord>,
    pub next_cursor: Option<Vec<u8>>,
    pub complete: bool,
}

impl SemanticSqlSourceReadPage {
    const MAX_SOURCES: usize = 256;
    const MAX_CURSOR_BYTES: usize = 4096;
    /// Keep this aligned with `eg_query::tables::ROW_SNAPSHOT_MAX_BYTES`.
    const MAX_SOURCE_BYTES: usize = 8 * 1024 * 1024;

    pub fn validate(&self) -> Result<(), SemanticIndexError> {
        if sql_source_revision_parts(&self.source_revision).is_none() {
            return Err(SemanticIndexError::SourceManifestMismatch);
        }
        if self.sources.len() > Self::MAX_SOURCES {
            return Err(SemanticIndexError::InvalidField {
                field: "sql_source_page.sources".to_string(),
                reason: format!("page exceeds {} source rows", Self::MAX_SOURCES),
            });
        }
        self.validate_cursor_state()?;
        self.validate_snapshot_proof()?;
        self.validate_sources()
    }

    fn validate_cursor_state(&self) -> Result<(), SemanticIndexError> {
        match (&self.complete, &self.next_cursor) {
            (true, Some(_)) => Err(SemanticIndexError::InvalidField {
                field: "sql_source_page.next_cursor".to_string(),
                reason: "a complete page cannot carry a continuation cursor".to_string(),
            }),
            (false, None) => Err(SemanticIndexError::InvalidField {
                field: "sql_source_page.next_cursor".to_string(),
                reason: "a partial page requires a continuation cursor".to_string(),
            }),
            (_, Some(cursor)) if cursor.is_empty() || cursor.len() > Self::MAX_CURSOR_BYTES => {
                Err(SemanticIndexError::InvalidField {
                    field: "sql_source_page.next_cursor".to_string(),
                    reason: "continuation cursor is empty or exceeds the bounded size".to_string(),
                })
            }
            _ => Ok(()),
        }
    }

    fn validate_snapshot_proof(&self) -> Result<(), SemanticIndexError> {
        match (self.complete, self.complete_snapshot_receipt_digest) {
            (false, None) => Ok(()),
            (true, Some(digest)) if digest != SemanticDigest::from_bytes([0; 32]) => Ok(()),
            _ => Err(SemanticIndexError::SourceManifestMismatch),
        }
    }

    fn validate_sources(&self) -> Result<(), SemanticIndexError> {
        let mut entities = BTreeSet::new();
        let mut source_bytes = 0usize;
        for source in &self.sources {
            source.validate()?;
            if source.source_revision != self.source_revision
                || (!self.complete && source.is_tombstone())
                || !entities.insert(source.source_entity_id())
            {
                return Err(SemanticIndexError::SourceManifestMismatch);
            }
            source_bytes = source_bytes
                .checked_add(source.source_bytes_len())
                .filter(|bytes| *bytes <= Self::MAX_SOURCE_BYTES)
                .ok_or_else(|| SemanticIndexError::InvalidField {
                    field: "sql_source_page.source_bytes".to_string(),
                    reason: format!("page source bytes exceed {} bytes", Self::MAX_SOURCE_BYTES),
                })?;
        }
        Ok(())
    }
}

/// Receipts and continuation state from one durable source-page admission.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SemanticSqlSourcePageAdmission {
    pub receipts: Vec<SemanticMutationReceipt>,
    pub next_cursor: Option<Vec<u8>>,
    pub complete: bool,
}

/// Result of one bounded source-reconciliation turn. `complete` is true only
/// after the final complete-page proof and every bounded prior-identity
/// tombstone page have been admitted; callers retry the same entrypoint while
/// the native checkpoint retains a continuation.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SemanticSqlSourceReconciliationAdmission {
    pub receipts: Vec<SemanticMutationReceipt>,
    pub source_revision: String,
    pub page_count: usize,
    /// `false` means the native checkpoint retained a continuation.  The
    /// caller retries the same reconciler entrypoint; it never has to own or
    /// reinterpret the SQL cursor.
    pub complete: bool,
    /// `false` means the supplied outbox wakeup was not consumed.  This is
    /// explicit for a newer wakeup that arrives while an older checkpoint is
    /// still owned by another durable outbox record; callers must retain that
    /// lease and retry after the checkpoint owner completes.
    pub wakeup_consumed: bool,
    pub rows_seen: u64,
    pub source_bytes_seen: u64,
}

/// Read-only boundary from EG semantic admission to the authoritative SQL
/// source owner. The `input_digest` in `wakeup` is only a notification hint;
/// implementations must use the authenticated source scope and committed
/// record to read a bounded page of current rows and derive each stable
/// identity, revision, exact bytes or deletion proof. The cursor is opaque to
/// EG and is returned unchanged for the next page request.
pub trait SemanticSqlSourceReadPort {
    fn read_current_sql_source_page(
        &self,
        binding: &SemanticBinding,
        wakeup: &SemanticSourceDirtyIntent,
        record: &MutationOutboxRecord,
        cursor: Option<&[u8]>,
    ) -> Result<SemanticSqlSourceReadPage, SemanticIndexError>;
}

fn validate_source_dirty_record(
    binding: &SemanticBinding,
    resolved_source_scope_digest: SemanticDigest,
    record: &MutationOutboxRecord,
) -> Result<SemanticSourceDirtyIntent, SemanticIndexError> {
    binding.validate()?;
    // This is the SQL catalog scope digest carried by the committed outbox
    // identity. It is deliberately distinct from the semantic binding digest:
    // a selector may have several governed semantic bindings over one source.
    let expected_scope_digest =
        SemanticDigest::from_bytes(*record.identity.binding_digest().as_bytes());
    if resolved_source_scope_digest != expected_scope_digest {
        return Err(SemanticIndexError::SourceManifestMismatch);
    }
    record
        .validate()
        .map_err(|reason| SemanticIndexError::InvalidField {
            field: "outbox_record".to_string(),
            reason,
        })?;
    if record.intent.topic != eg_types::semantic_index::SEMANTIC_SOURCE_DIRTY_TOPIC {
        return Err(SemanticIndexError::InvalidField {
            field: "outbox_topic".to_string(),
            reason: "semantic source dirty consumer received another topic".to_string(),
        });
    }
    if record.intent.key != record.batch_id || !record.intent.headers.is_empty() {
        return Err(SemanticIndexError::InvalidField {
            field: "outbox_identity".to_string(),
            reason: "source dirty wakeup key/headers are not the canonical producer shape"
                .to_string(),
        });
    }
    if record.identity.tenant().as_str() != binding.tenant_id
        || !matches!(
            record.identity.scope(),
            eg_types::mutation_batch::MutationScope::Native {
                domain: eg_types::mutation_batch::DurabilityDomain::SqlCatalog,
                ..
            }
        )
    {
        return Err(SemanticIndexError::SourceManifestMismatch);
    }
    let wakeup = SemanticSourceDirtyIntent::from_canonical_cbor(&record.intent.payload)?;
    wakeup.validate()?;
    if wakeup.source_scope_digest != resolved_source_scope_digest {
        return Err(SemanticIndexError::SourceManifestMismatch);
    }
    Ok(wakeup)
}

/// Expand the coarse SQL source fact into the binding-specific S1 intent.
/// SQL owns neither binding identity nor semantic generation, so this is the
/// only function that may join the source selector to a durable binding before
/// `SemanticCodeStore::enqueue_stage_intent` admits it. The wakeup record is
/// authenticated here, while E3 supplies the tenant-wide source revision from
/// its atomic authoritative read; no request field can select the revision.
fn source_dirty_to_s1(
    binding: &SemanticBinding,
    source_entity_id: &str,
    resolved_source_scope_digest: SemanticDigest,
    record: &MutationOutboxRecord,
    source_revision: &str,
    input_digest: SemanticDigest,
) -> Result<SemanticStageIntent, SemanticIndexError> {
    validate_source_dirty_record(binding, resolved_source_scope_digest, record)?;
    if source_entity_id.trim().is_empty() {
        return Err(SemanticIndexError::SourceManifestMismatch);
    }
    coalesce_authoritative_revision_to_s1(binding, source_entity_id, source_revision, input_digest)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SqlSourceRevision<'a> {
    authority: &'a str,
    epoch: u64,
}

/// Parse the canonical tenant-wide SQL source revision.  The authority is
/// deliberately parsed before the epoch so a numerically newer foreign
/// source cannot supersede the tenant's current source lineage.
fn sql_source_revision_parts(revision: &str) -> Option<SqlSourceRevision<'_>> {
    let rest = revision.strip_prefix("sql-source:")?;
    let (authority, epoch) = rest.rsplit_once(":epoch:")?;
    if authority.strip_prefix("sha256:")?.len() != 64
        || !authority
            .strip_prefix("sha256:")?
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
    {
        return None;
    }
    let epoch = epoch.parse().ok()?;
    if epoch == 0 {
        return None;
    }
    Some(SqlSourceRevision { authority, epoch })
}

fn validate_manifest_against_binding(
    binding: &SemanticBinding,
    source_entity_id: &str,
    snapshot: &SemanticSqlSourceManifest,
) -> Result<(), SemanticIndexError> {
    snapshot.validate()?;
    snapshot.source_identity.validate_against_binding(binding)?;
    if snapshot.binding_id != binding.binding_id
        || snapshot.binding_digest != binding.binding_digest
        || snapshot.generation != binding.generation
        || snapshot.source_entity_id != source_entity_id
        || snapshot.source_schema_digest != binding.source_schema_digest
        || snapshot.source_field_set_digest != binding.source_field_set_digest
        || snapshot.source_acl_revision != binding.policy_identity.components.source_acl_revision
        || snapshot.source_acl_digest != binding.policy_identity.components.source_acl_digest
    {
        return Err(SemanticIndexError::SourceManifestMismatch);
    }
    Ok(())
}

fn validate_source_record_against_binding(
    binding: &SemanticBinding,
    source: &SemanticSqlSourceRecord,
) -> Result<(), SemanticIndexError> {
    source.validate()?;
    source.source_identity.validate_against_binding(binding)?;
    if source.source_schema_digest != binding.source_schema_digest
        || source.source_field_set_digest != binding.source_field_set_digest
        || source.source_acl_revision != binding.policy_identity.components.source_acl_revision
        || source.source_acl_digest != binding.policy_identity.components.source_acl_digest
    {
        return Err(SemanticIndexError::SourceManifestMismatch);
    }
    Ok(())
}

/// Compare a source observation to the durable binding head.  The wakeup is a
/// notification only; E3's atomically read tenant authority/epoch is the
/// source revision, so an event-local native scope never enters this proof.
fn coalesce_authoritative_revision_to_s1(
    binding: &SemanticBinding,
    source_entity_id: &str,
    source_revision: &str,
    source_content_digest: SemanticDigest,
) -> Result<SemanticStageIntent, SemanticIndexError> {
    let source = sql_source_revision_parts(source_revision)
        .ok_or(SemanticIndexError::SourceManifestMismatch)?;
    let binding_revision = sql_source_revision_parts(&binding.source_revision)
        .ok_or(SemanticIndexError::SourceManifestMismatch)?;
    if binding_revision.authority != source.authority {
        return Err(SemanticIndexError::SourceManifestMismatch);
    }
    if source.epoch < binding_revision.epoch {
        return Err(SemanticIndexError::SourceRevisionStale);
    }
    source_dirty_intent(
        binding,
        source_entity_id,
        source_revision,
        source_content_digest,
    )
}

fn source_dirty_intent(
    binding: &SemanticBinding,
    source_entity_id: &str,
    source_revision: &str,
    input_digest: SemanticDigest,
) -> Result<SemanticStageIntent, SemanticIndexError> {
    SemanticStageIntent::create(SemanticStageIntentDraft {
        binding_id: binding.binding_id.clone(),
        binding_digest: binding.binding_digest,
        generation: binding.generation,
        scope: SemanticStageScope::Entity {
            source_entity_id: source_entity_id.to_string(),
        },
        source_revision: source_revision.to_string(),
        stage: SemanticStage::SourceCommit,
        predecessor: eg_types::semantic_index::SemanticStagePredecessor::None,
        input_digest,
    })
}

fn validate_sql_revision_lineage(
    current_revision: &str,
    replacement_revision: &str,
) -> Result<(), SemanticIndexError> {
    let current = sql_source_revision_parts(current_revision)
        .ok_or(SemanticIndexError::SourceManifestMismatch)?;
    let replacement = sql_source_revision_parts(replacement_revision)
        .ok_or(SemanticIndexError::SourceManifestMismatch)?;
    if current.authority != replacement.authority {
        return Err(SemanticIndexError::SourceManifestMismatch);
    }
    if replacement.epoch <= current.epoch {
        return Err(SemanticIndexError::SourceRevisionStale);
    }
    Ok(())
}

/// Reconcile a coarse source wakeup with an authorized raw SQL observation.
/// The observation owns the exact source bytes or deletion proof; only its
/// digest enters the durable S1 intent.
pub fn coalesce_sql_source_record_to_s1(
    binding: &SemanticBinding,
    resolved_source_scope_digest: SemanticDigest,
    record: &MutationOutboxRecord,
    source: &SemanticSqlSourceRecord,
) -> Result<SemanticStageIntent, SemanticIndexError> {
    binding.validate()?;
    validate_source_record_against_binding(binding, source)?;
    let source_entity_id = source.source_entity_id();
    source_dirty_to_s1(
        binding,
        &source_entity_id,
        resolved_source_scope_digest,
        record,
        &source.source_revision,
        source.source_content_digest(),
    )
}

/// Build the completed S1 artifact from the same raw source claim that
/// produced its intent.  Keeping this construction beside coalescing prevents
/// the SQL ACL decision digest from being mistaken for the full authorization
/// receipt digest when a consumer persists the manifest.
pub fn sql_source_stage_artifact(
    binding: &SemanticBinding,
    transition: &SemanticStageTransition,
    source: &SemanticSqlSourceRecord,
    authorized_at: &str,
) -> Result<SemanticStageArtifact, SemanticIndexError> {
    transition.validate()?;
    if transition.intent.stage != SemanticStage::SourceCommit {
        return Err(SemanticIndexError::StageArtifactMismatch);
    }
    let source_entity_id = source.source_entity_id();
    let source_content_digest = source.source_content_digest();
    if transition.intent.scope
        != (SemanticStageScope::Entity {
            source_entity_id: source_entity_id.clone(),
        })
        || transition.intent.source_revision != source.source_revision
        || transition.intent.input_digest != source_content_digest
        || transition.receipt.output_digest != source_content_digest
    {
        return Err(SemanticIndexError::StageArtifactMismatch);
    }
    let authorization = source.authorization_receipt(binding, authorized_at)?;
    let manifest = SemanticSqlSourceManifest::create(SemanticSqlSourceManifestDraft {
        binding_id: binding.binding_id.clone(),
        binding_digest: binding.binding_digest,
        generation: binding.generation,
        source_identity: source.source_identity.clone(),
        source_revision: source.source_revision.clone(),
        source_content_digest,
        source_schema_revision: source.source_schema_revision,
        source_schema_digest: source.source_schema_digest.clone(),
        source_field_set_digest: source.source_field_set_digest.clone(),
        source_acl_revision: source.source_acl_revision,
        source_acl_digest: source.source_acl_digest.clone(),
        authorization_receipt_digest: authorization.authorization_receipt_digest,
        completed_receipt_digest: transition.receipt.receipt_digest(),
        completed_at: transition.receipt.completed_at.clone(),
    })?;
    let artifact = SemanticStageArtifact::SqlSourceManifest {
        manifest: Box::new(manifest),
        authorization: Box::new(authorization),
    };
    artifact.validate_against(transition)?;
    Ok(artifact)
}

/// Build the S1 intent for a row that was absent from an authenticated,
/// complete E3 snapshot.  A tombstone carries only the durable entity key:
/// reconstructing a deleted row's primary-key bytes in core would invent
/// source data.  The complete-page receipt and tenant revision make the
/// deletion proof specific to this authoritative scan.
fn coalesce_sql_source_tombstone_to_s1(
    binding: &SemanticBinding,
    resolved_source_scope_digest: SemanticDigest,
    record: &MutationOutboxRecord,
    source_entity_id: &str,
    source_revision: &str,
    deletion_proof_digest: SemanticDigest,
) -> Result<SemanticStageIntent, SemanticIndexError> {
    binding.validate()?;
    validate_source_dirty_record(binding, resolved_source_scope_digest, record)?;
    if !valid_source_entity_id(source_entity_id)
        || deletion_proof_digest == SemanticDigest::from_bytes([0; 32])
    {
        return Err(SemanticIndexError::SourceManifestMismatch);
    }
    coalesce_authoritative_revision_to_s1(
        binding,
        source_entity_id,
        source_revision,
        deletion_proof_digest,
    )
}

/// Compute the canonical proof that one source entity was absent from a
/// complete authoritative snapshot.  Server adapters use this helper when
/// constructing a tombstone transition so the deletion digest cannot drift
/// from the core reconciliation path.
pub fn sql_source_deletion_proof(
    source_entity_id: &str,
    source_revision: &str,
    complete_snapshot_receipt_digest: SemanticDigest,
) -> SemanticDigest {
    let mut hasher = Sha256::new();
    hasher.update(b"eg/semantic-sql-source-deletion/v1\0");
    hasher.update(source_entity_id.as_bytes());
    hasher.update((source_entity_id.len() as u64).to_be_bytes());
    hasher.update(source_revision.as_bytes());
    hasher.update((source_revision.len() as u64).to_be_bytes());
    hasher.update(complete_snapshot_receipt_digest.as_bytes());
    SemanticDigest::from_bytes(hasher.finalize().into())
}

fn read_reconciliation_page(
    read_port: &dyn SemanticSqlSourceReadPort,
    binding: &SemanticBinding,
    wakeup: &SemanticSourceDirtyIntent,
    record: &MutationOutboxRecord,
    cursor: Option<&[u8]>,
) -> Result<SemanticSqlSourceReadPage, SemanticCodeError> {
    read_port
        .read_current_sql_source_page(binding, wakeup, record, cursor)
        .map_err(|error| {
            SemanticCodeError::Refused(format!("authoritative SQL source read rejected: {error:?}"))
        })
}

fn source_reconciliation_wakeup_digest(
    record: &MutationOutboxRecord,
    wakeup: &SemanticSourceDirtyIntent,
) -> SemanticDigest {
    let mut hasher = Sha256::new();
    hasher.update(b"eg/semantic-source-reconciliation-wakeup/v1\0");
    hasher.update((record.batch_id.len() as u64).to_be_bytes());
    hasher.update(record.batch_id.as_bytes());
    hasher.update(record.ordinal.to_be_bytes());
    match record.committed_version {
        eg_types::mutation_batch::CommittedVersion::Graph { source, target } => {
            hasher.update([b'g']);
            hasher.update(source.to_be_bytes());
            hasher.update(target.to_be_bytes());
        }
        eg_types::mutation_batch::CommittedVersion::Native { source, target } => {
            hasher.update([b'n']);
            hasher.update(source.to_be_bytes());
            hasher.update(target.to_be_bytes());
        }
        eg_types::mutation_batch::CommittedVersion::None => hasher.update([b'0']),
    }
    hasher.update(wakeup.source_scope_digest.as_bytes());
    hasher.update(wakeup.input_digest.as_bytes());
    SemanticDigest::from_bytes(hasher.finalize().into())
}

fn checkpoint_matches_wakeup(
    checkpoint: &SemanticSourceReconciliationCheckpoint,
    wakeup_digest: SemanticDigest,
) -> bool {
    checkpoint.source_wakeup_digest == wakeup_digest
}

fn validate_page_revision_against_binding(
    binding: &SemanticBinding,
    source_revision: &str,
) -> Result<(), SemanticCodeError> {
    let page = sql_source_revision_parts(source_revision).ok_or_else(|| {
        SemanticCodeError::Refused(
            "authoritative SQL source page has a non-canonical source revision".to_string(),
        )
    })?;
    let binding_revision =
        sql_source_revision_parts(&binding.source_revision).ok_or_else(|| {
            SemanticCodeError::Corrupt(
                "durable semantic binding has a non-canonical SQL source revision".to_string(),
            )
        })?;
    if page.authority != binding_revision.authority {
        return Err(SemanticCodeError::Refused(
            "authoritative SQL source page belongs to another source authority".to_string(),
        ));
    }
    if page.epoch < binding_revision.epoch {
        return Err(SemanticCodeError::Refused(
            "authoritative SQL source page is stale relative to the binding head".to_string(),
        ));
    }
    Ok(())
}

fn validate_reconciliation_page(
    service: &SemanticIndexService,
    binding: &SemanticBinding,
    page: &SemanticSqlSourceReadPage,
    cursor: Option<&[u8]>,
    seen_cursors: &mut BTreeSet<Vec<u8>>,
    previous_revision: &mut Option<String>,
    seen_entities: &mut BTreeSet<String>,
) -> Result<u64, SemanticCodeError> {
    if let Some(cursor_bytes) = cursor {
        if !seen_cursors.insert(cursor_bytes.to_vec()) {
            return Err(SemanticCodeError::Refused(
                "authoritative SQL source cursor repeated during reconciliation".to_string(),
            ));
        }
    }
    page.validate().map_err(|error| {
        SemanticCodeError::Refused(format!("authoritative SQL source page rejected: {error:?}"))
    })?;
    // This check deliberately precedes the source loop. An empty page still
    // carries an authority/epoch and must not bypass the durable binding head.
    validate_page_revision_against_binding(binding, &page.source_revision)?;
    match previous_revision {
        Some(previous) if previous != &page.source_revision => {
            return Err(SemanticCodeError::Refused(
                "authoritative SQL source revision changed between pages".to_string(),
            ));
        }
        None => *previous_revision = Some(page.source_revision.clone()),
        _ => {}
    }
    let page_source_bytes = page
        .sources
        .iter()
        .try_fold(0u64, |total, source| {
            total.checked_add(source.source_bytes_len() as u64)
        })
        .ok_or_else(|| {
            SemanticCodeError::Refused(
                "authoritative SQL source page byte count overflowed".to_string(),
            )
        })?;
    for source in &page.sources {
        validate_source_record_against_binding(binding, source).map_err(|error| {
            SemanticCodeError::Refused(format!("authoritative SQL source row rejected: {error:?}"))
        })?;
        let source_entity_id = source.source_entity_id();
        if !seen_entities.insert(source_entity_id.clone()) {
            return Err(SemanticCodeError::Refused(
                "authoritative SQL source entity is repeated during reconciliation".to_string(),
            ));
        }
        if source.is_tombstone()
            && !service
                .store
                .source_entity_exists(binding.generation, &source_entity_id)?
        {
            return Err(SemanticCodeError::Refused(
                "authoritative SQL source tombstone names no durable prior entity".to_string(),
            ));
        }
    }
    Ok(page_source_bytes)
}

fn valid_source_entity_id(entity: &str) -> bool {
    entity
        .strip_prefix("semantic-sql-source:")
        .and_then(|digest| SemanticDigest::parse(digest).ok())
        .is_some()
}

fn admission_result(
    receipts: Vec<SemanticMutationReceipt>,
    checkpoint: &SemanticSourceReconciliationCheckpoint,
    complete: bool,
) -> Result<SemanticSqlSourceReconciliationAdmission, SemanticCodeError> {
    let page_count = usize::try_from(checkpoint.pages_seen).map_err(|_| {
        SemanticCodeError::Refused("source reconciliation page count exceeds usize".to_string())
    })?;
    Ok(SemanticSqlSourceReconciliationAdmission {
        receipts,
        source_revision: checkpoint.source_revision.clone(),
        page_count,
        complete,
        wakeup_consumed: complete,
        rows_seen: checkpoint.rows_seen,
        source_bytes_seen: checkpoint.source_bytes_seen,
    })
}

fn validate_checkpoint_against_binding(
    binding: &SemanticBinding,
    checkpoint: &SemanticSourceReconciliationCheckpoint,
) -> Result<(), SemanticCodeError> {
    if checkpoint.source_wakeup_digest == SemanticDigest::from_bytes([0; 32]) {
        return Err(SemanticCodeError::Corrupt(
            "source reconciliation checkpoint has an empty wakeup identity".to_string(),
        ));
    }
    validate_page_revision_against_binding(binding, &checkpoint.source_revision)?;
    validate_checkpoint_phase(checkpoint)?;
    validate_checkpoint_cursors(checkpoint)
}

fn validate_checkpoint_phase(
    checkpoint: &SemanticSourceReconciliationCheckpoint,
) -> Result<(), SemanticCodeError> {
    match &checkpoint.phase {
        SemanticSourceReconciliationPhase::Scanning => {
            if checkpoint.source_cursor.is_none()
                || checkpoint.complete_snapshot_receipt_digest.is_some()
                || checkpoint.prior_cursor.is_some()
            {
                return Err(SemanticCodeError::Corrupt(
                    "scanning source checkpoint has an invalid continuation state".to_string(),
                ));
            }
        }
        SemanticSourceReconciliationPhase::FinalizingTombstones => {
            if checkpoint.source_cursor.is_some()
                || checkpoint.complete_snapshot_receipt_digest.is_none()
            {
                return Err(SemanticCodeError::Corrupt(
                    "finalizing source checkpoint has an invalid continuation state".to_string(),
                ));
            }
        }
    }
    Ok(())
}

fn validate_checkpoint_cursors(
    checkpoint: &SemanticSourceReconciliationCheckpoint,
) -> Result<(), SemanticCodeError> {
    if let Some(cursor) = checkpoint.source_cursor.as_ref() {
        if cursor.is_empty() || cursor.len() > SemanticSqlSourceReadPage::MAX_CURSOR_BYTES {
            return Err(SemanticCodeError::Corrupt(
                "source checkpoint cursor exceeds the bounded size".to_string(),
            ));
        }
    }
    if let Some(entity) = checkpoint.prior_cursor.as_deref() {
        if !valid_source_entity_id(entity) {
            return Err(SemanticCodeError::Corrupt(
                "source checkpoint prior cursor is not a source entity id".to_string(),
            ));
        }
    }
    Ok(())
}

fn checkpoint_for_scan(
    source_revision: &str,
    source_cursor: Option<Vec<u8>>,
    previous: Option<&SemanticSourceReconciliationCheckpoint>,
) -> Result<SemanticSourceReconciliationCheckpoint, SemanticCodeError> {
    let previous = previous.ok_or_else(|| {
        SemanticCodeError::Corrupt(
            "source scan budget was exhausted before a checkpoint could be established".to_string(),
        )
    })?;
    Ok(SemanticSourceReconciliationCheckpoint {
        source_wakeup_digest: previous.source_wakeup_digest,
        source_revision: source_revision.to_string(),
        phase: SemanticSourceReconciliationPhase::Scanning,
        source_cursor,
        prior_cursor: None,
        rows_seen: previous.rows_seen,
        source_bytes_seen: previous.source_bytes_seen,
        pages_seen: previous.pages_seen,
        complete_snapshot_receipt_digest: None,
    })
}

#[derive(Debug, Default)]
struct ReconciliationTurnBudget {
    pages: usize,
    rows: u64,
    source_bytes: u64,
}

impl ReconciliationTurnBudget {
    fn would_exceed(&self, rows: u64, source_bytes: u64) -> bool {
        self.pages > 0
            && (self.rows.saturating_add(rows)
                > SemanticIndexService::MAX_SOURCE_RECONCILIATION_ROWS_PER_TURN
                || self.source_bytes.saturating_add(source_bytes)
                    > SemanticIndexService::MAX_SOURCE_RECONCILIATION_BYTES_PER_TURN)
    }

    fn record(&mut self, rows: u64, source_bytes: u64) {
        self.pages = self.pages.saturating_add(1);
        self.rows = self.rows.saturating_add(rows);
        self.source_bytes = self.source_bytes.saturating_add(source_bytes);
    }

    fn exhausted(&self) -> bool {
        self.pages >= SemanticIndexService::MAX_SOURCE_RECONCILIATION_PAGES_PER_TURN
            || self.rows >= SemanticIndexService::MAX_SOURCE_RECONCILIATION_ROWS_PER_TURN
            || self.source_bytes >= SemanticIndexService::MAX_SOURCE_RECONCILIATION_BYTES_PER_TURN
    }
}

enum PartialReconciliationProgress {
    Yield(SemanticSqlSourceReconciliationAdmission),
    Continue {
        checkpoint: SemanticSourceReconciliationCheckpoint,
        cursor: Vec<u8>,
        receipts: Vec<SemanticMutationReceipt>,
    },
}

enum ReconciliationCheckpointState {
    Owned(Option<SemanticSourceReconciliationCheckpoint>),
    Pending(SemanticSqlSourceReconciliationAdmission),
}

fn validate_prior_entity_page(
    entities: Vec<String>,
    next_cursor: Option<&str>,
    previous_cursor: Option<&str>,
    limit: usize,
) -> Result<Vec<String>, SemanticCodeError> {
    if entities.len() > limit {
        return Err(SemanticCodeError::Corrupt(
            "durable source-progress page exceeded the tombstone work budget".to_string(),
        ));
    }
    if entities.is_empty() && next_cursor.is_some() {
        return Err(SemanticCodeError::Corrupt(
            "durable source-progress page advanced without an entity".to_string(),
        ));
    }
    let mut unique = BTreeSet::new();
    for entity in &entities {
        if !valid_source_entity_id(entity) || !unique.insert(entity.clone()) {
            return Err(SemanticCodeError::Corrupt(
                "durable source-progress page contains an invalid or duplicate entity".to_string(),
            ));
        }
    }
    if let Some(next_cursor) = next_cursor {
        if previous_cursor == Some(next_cursor) {
            return Err(SemanticCodeError::Corrupt(
                "durable source-progress cursor did not advance".to_string(),
            ));
        }
    }
    Ok(entities)
}

/// Reconcile a completed source manifest supplied by the refresh lifecycle.
/// Read ports use [`coalesce_sql_source_record_to_s1`] instead; a completed
/// manifest is valid here because refresh already owns its predecessor proof.
pub fn coalesce_sql_snapshot_to_s1(
    binding: &SemanticBinding,
    source_entity_id: &str,
    resolved_source_scope_digest: SemanticDigest,
    record: &MutationOutboxRecord,
    snapshot: &SemanticSqlSourceManifest,
) -> Result<SemanticStageIntent, SemanticIndexError> {
    binding.validate()?;
    validate_manifest_against_binding(binding, source_entity_id, snapshot)?;
    validate_source_dirty_record(binding, resolved_source_scope_digest, record)?;
    coalesce_authoritative_revision_to_s1(
        binding,
        source_entity_id,
        &snapshot.source_revision,
        snapshot.source_content_digest,
    )
}

/// Expand a source wakeup for a replacement generation. A newer snapshot
/// cannot be admitted under the old generation because its lexical/ANN
/// identities are bound to the old source revision.  This helper is the
/// pre-refresh proof; after the refresh commits, the service reads the new
/// durable head and uses [`coalesce_sql_snapshot_to_s1`].
pub fn coalesce_sql_snapshot_to_replacement_s1(
    current: &SemanticBinding,
    replacement: &SemanticBinding,
    source_entity_id: &str,
    resolved_source_scope_digest: SemanticDigest,
    record: &MutationOutboxRecord,
    snapshot: &SemanticSqlSourceManifest,
) -> Result<SemanticStageIntent, SemanticIndexError> {
    current.validate()?;
    replacement.validate()?;
    if replacement.binding_id != current.binding_id
        || replacement.tenant_id != current.tenant_id
        || replacement.generation != current.generation.saturating_add(1)
        || replacement.durable_state != SemanticBindingState::Pending
    {
        return Err(SemanticIndexError::GenerationMismatch {
            expected: current.generation.saturating_add(1),
            actual: replacement.generation,
        });
    }
    validate_source_dirty_record(current, resolved_source_scope_digest, record)?;
    snapshot.validate()?;
    snapshot.validate_against_binding(replacement)?;
    if snapshot.source_entity_id != source_entity_id {
        return Err(SemanticIndexError::SourceManifestMismatch);
    }
    coalesce_authoritative_revision_to_s1(
        replacement,
        source_entity_id,
        &snapshot.source_revision,
        snapshot.source_content_digest,
    )
}

/// EG's callable semantic service.  Heavy stage execution and outbox
/// scheduling are intentionally outside this object; callers receive a
/// durable receipt immediately after native admission.
#[derive(Debug)]
pub struct SemanticIndexService {
    store: SemanticCodeStore,
}

impl SemanticIndexService {
    /// These are per invocation budgets. A complete source may be larger; the
    /// native checkpoint turns exhaustion into a resumable prefix instead of
    /// rejecting after committing a prefix and restarting from page zero.
    const MAX_SOURCE_RECONCILIATION_PAGES_PER_TURN: usize = 4096;
    const MAX_SOURCE_RECONCILIATION_ROWS_PER_TURN: u64 = 100_000;
    const MAX_SOURCE_RECONCILIATION_BYTES_PER_TURN: u64 = 64 * 1024 * 1024;
    /// Tombstone derivation is a separate bounded turn after the complete
    /// page proof. The owner page size is the same bound as current rows.
    const MAX_SOURCE_RECONCILIATION_TOMBSTONES_PER_TURN: usize = 256;

    pub fn open(
        dir: &Path,
        verifier: Arc<dyn ScopeGrantVerifier>,
        principal: &str,
        proof: &[u8],
        tenant: &str,
        binding: &str,
    ) -> Result<Self, SemanticCodeError> {
        let store = SemanticCodeStore::open(dir, verifier, principal, proof, tenant, binding)?;
        Ok(Self { store })
    }

    /// Persist a validated binding and publish its durable creation event.
    ///
    /// `pub`, not `pub(crate)`: the un-operation-bound sibling of
    /// [`Self::admit_binding_operation`]. It carries no actor, idempotency key
    /// or attempt nonce, so it is deliberately NOT one of the wire contract's
    /// operations -- `Method::SemanticIndex`'s `AdmitBinding` routes through
    /// `admit_binding_operation` and mints the nonce engine-side. What needs it
    /// is an in-process governed producer that supplies replay identity
    /// separately, and the facade's own adapter tests, which live in a
    /// different crate and so cannot reach a `pub(crate)` item at all.
    pub fn admit_binding(
        &self,
        binding: &SemanticBinding,
        now_ms: u64,
    ) -> Result<SemanticMutationReceipt, SemanticCodeError> {
        self.store.store_binding(binding, now_ms)
    }

    pub fn admit_binding_operation(
        &self,
        binding: &SemanticBinding,
        now_ms: u64,
        actor: &str,
        idempotency_key: &str,
        nonce: Nonce,
    ) -> Result<SemanticMutationReceipt, SemanticCodeError> {
        self.store
            .store_binding_operation(binding, now_ms, actor, idempotency_key, nonce)
    }

    /// Replace a live source generation with an operation-bound pending
    /// generation.  The old active pointer remains the serving proof until
    /// [`Self::activate_s6`] publishes the replacement.
    pub fn refresh_binding_operation(
        &self,
        expected_generation: u64,
        replacement: &SemanticBinding,
        source_manifest: &SemanticSqlSourceManifest,
        now_ms: u64,
        actor: &str,
        idempotency_key: &str,
        nonce: Nonce,
    ) -> Result<SemanticMutationReceipt, SemanticCodeError> {
        if let Some(current) = self.store.read_binding()? {
            if current.generation == expected_generation {
                validate_sql_revision_lineage(
                    &current.source_revision,
                    &replacement.source_revision,
                )
                .map_err(|error| {
                    SemanticCodeError::Refused(format!(
                        "semantic refresh source lineage rejected: {error:?}"
                    ))
                })?;
            }
        }
        source_manifest
            .validate_against_binding(replacement)
            .map_err(|error| {
                SemanticCodeError::Refused(format!(
                    "semantic refresh source manifest rejected: {error:?}"
                ))
            })?;
        let replacement_intent = coalesce_authoritative_revision_to_s1(
            replacement,
            &source_manifest.source_entity_id,
            &source_manifest.source_revision,
            source_manifest.source_content_digest,
        )
        .map_err(|error| {
            SemanticCodeError::Refused(format!(
                "semantic refresh source intent rejected: {error:?}"
            ))
        })?;
        self.store.refresh_binding_operation_with_s1(
            expected_generation,
            replacement,
            source_manifest,
            &replacement_intent,
            now_ms,
            actor,
            idempotency_key,
            nonce,
        )
    }

    pub fn transition_binding_operation(
        &self,
        expected_generation: u64,
        next: SemanticBindingState,
        now_ms: u64,
        actor: &str,
        idempotency_key: &str,
        nonce: Nonce,
    ) -> Result<SemanticMutationReceipt, SemanticCodeError> {
        self.store.transition_binding_operation(
            expected_generation,
            next,
            now_ms,
            actor,
            idempotency_key,
            nonce,
        )
    }

    pub fn drop_binding_operation(
        &self,
        expected_generation: u64,
        now_ms: u64,
        actor: &str,
        idempotency_key: &str,
        nonce: Nonce,
    ) -> Result<SemanticMutationReceipt, SemanticCodeError> {
        self.store.drop_binding_operation(
            expected_generation,
            now_ms,
            actor,
            idempotency_key,
            nonce,
        )
    }

    /// Expand and admit one raw SQL source observation. The committed version
    /// is supplied by the SQL receipt, while the source revision/content comes
    /// from the authoritative read port, never from request-body text.
    pub(crate) fn admit_sql_source_dirty(
        &self,
        binding: &SemanticBinding,
        resolved_source_scope_digest: SemanticDigest,
        record: &MutationOutboxRecord,
        source: &SemanticSqlSourceRecord,
        now_ms: u64,
    ) -> Result<SemanticMutationReceipt, SemanticCodeError> {
        let intent =
            coalesce_sql_source_record_to_s1(binding, resolved_source_scope_digest, record, source)
                .map_err(|error| {
                    SemanticCodeError::Refused(format!(
                        "semantic contract rejected record: {error:?}"
                    ))
                })?;
        self.store.enqueue_stage_intent(&intent, now_ms)
    }

    fn admit_sql_source_page(
        &self,
        binding: &SemanticBinding,
        resolved_source_scope_digest: SemanticDigest,
        record: &MutationOutboxRecord,
        page: &SemanticSqlSourceReadPage,
        now_ms: u64,
    ) -> Result<SemanticSqlSourcePageAdmission, SemanticCodeError> {
        validate_page_revision_against_binding(binding, &page.source_revision)?;
        let mut receipts = Vec::with_capacity(page.sources.len());
        for source in &page.sources {
            let source_entity_id = source.source_entity_id();
            if source.is_tombstone()
                && !self
                    .store
                    .source_entity_exists(binding.generation, &source_entity_id)?
            {
                return Err(SemanticCodeError::Refused(
                    "authoritative SQL source tombstone names no durable prior entity".to_string(),
                ));
            }
            receipts.push(self.admit_sql_source_dirty(
                binding,
                resolved_source_scope_digest,
                record,
                source,
                now_ms,
            )?);
        }
        Ok(SemanticSqlSourcePageAdmission {
            receipts,
            next_cursor: page.next_cursor.clone(),
            complete: page.complete,
        })
    }

    fn resume_tombstone_reconciliation(
        &self,
        binding: &SemanticBinding,
        scope_digest: SemanticDigest,
        record: &MutationOutboxRecord,
        checkpoint: Option<&SemanticSourceReconciliationCheckpoint>,
        now_ms: u64,
    ) -> Result<Option<SemanticSqlSourceReconciliationAdmission>, SemanticCodeError> {
        let Some(existing) = checkpoint else {
            return Ok(None);
        };
        validate_checkpoint_against_binding(binding, existing)?;
        if !matches!(
            &existing.phase,
            SemanticSourceReconciliationPhase::FinalizingTombstones
        ) {
            return Ok(None);
        }
        self.finalize_source_reconciliation(
            binding,
            scope_digest,
            record,
            existing.clone(),
            Vec::new(),
            now_ms,
        )
        .map(Some)
    }

    fn load_checkpoint_for_wakeup(
        &self,
        binding: &SemanticBinding,
        wakeup_digest: SemanticDigest,
    ) -> Result<ReconciliationCheckpointState, SemanticCodeError> {
        let checkpoint = self
            .store
            .read_source_reconciliation_checkpoint(binding.generation)?;
        let Some(existing) = checkpoint else {
            return Ok(ReconciliationCheckpointState::Owned(None));
        };
        validate_checkpoint_against_binding(binding, &existing)?;
        if checkpoint_matches_wakeup(&existing, wakeup_digest) {
            return Ok(ReconciliationCheckpointState::Owned(Some(existing)));
        }
        // Keep the older checkpoint owned by its durable outbox record.  A
        // mismatching wakeup is explicitly pending, so alternating retries
        // cannot clear/restart one another's bounded continuation.
        Ok(ReconciliationCheckpointState::Pending(admission_result(
            Vec::new(),
            &existing,
            false,
        )?))
    }

    fn persist_scan_budget_checkpoint(
        &self,
        binding: &SemanticBinding,
        previous_revision: Option<&str>,
        cursor: Option<Vec<u8>>,
        checkpoint: Option<&SemanticSourceReconciliationCheckpoint>,
        receipts: Vec<SemanticMutationReceipt>,
    ) -> Result<SemanticSqlSourceReconciliationAdmission, SemanticCodeError> {
        let state = checkpoint_for_scan(
            previous_revision.ok_or_else(|| {
                SemanticCodeError::Corrupt(
                    "source page had no canonical revision after validation".to_string(),
                )
            })?,
            cursor,
            checkpoint,
        )?;
        // All pages before this cursor were admitted before it advanced. If
        // the process dies between those native commits, retry replays the
        // deterministic S1 rows instead of skipping them.
        self.store.write_source_reconciliation_checkpoint(
            binding.generation,
            checkpoint,
            &state,
        )?;
        admission_result(receipts, &state, false)
    }

    fn persist_partial_reconciliation_page(
        &self,
        binding: &SemanticBinding,
        checkpoint: Option<&SemanticSourceReconciliationCheckpoint>,
        source_wakeup_digest: SemanticDigest,
        source_revision: &str,
        page: &SemanticSqlSourceReadPage,
        cursor: Option<&[u8]>,
        rows_seen: u64,
        source_bytes_seen: u64,
        pages_seen: u64,
        budget: &ReconciliationTurnBudget,
        receipts: Vec<SemanticMutationReceipt>,
    ) -> Result<PartialReconciliationProgress, SemanticCodeError> {
        let next_cursor = page.next_cursor.clone().ok_or_else(|| {
            SemanticCodeError::Corrupt(
                "validated partial source page lost its continuation cursor".to_string(),
            )
        })?;
        if cursor == Some(next_cursor.as_slice()) {
            return Err(SemanticCodeError::Refused(
                "authoritative SQL source cursor did not advance".to_string(),
            ));
        }
        let state = SemanticSourceReconciliationCheckpoint {
            source_wakeup_digest,
            source_revision: source_revision.to_string(),
            phase: SemanticSourceReconciliationPhase::Scanning,
            source_cursor: Some(next_cursor.clone()),
            prior_cursor: None,
            rows_seen,
            source_bytes_seen,
            pages_seen,
            complete_snapshot_receipt_digest: None,
        };
        self.store.write_source_reconciliation_checkpoint(
            binding.generation,
            checkpoint,
            &state,
        )?;
        if budget.exhausted() {
            return admission_result(receipts, &state, false)
                .map(PartialReconciliationProgress::Yield);
        }
        Ok(PartialReconciliationProgress::Continue {
            checkpoint: state,
            cursor: next_cursor,
            receipts,
        })
    }

    fn admit_missing_tombstones(
        &self,
        binding: &SemanticBinding,
        scope_digest: SemanticDigest,
        record: &MutationOutboxRecord,
        state: &SemanticSourceReconciliationCheckpoint,
        source_entities: &[String],
        complete_receipt: SemanticDigest,
        mut receipts: Vec<SemanticMutationReceipt>,
        now_ms: u64,
    ) -> Result<Vec<SemanticMutationReceipt>, SemanticCodeError> {
        for source_entity_id in source_entities {
            if self.store.source_entity_seen_at_revision(
                binding.generation,
                source_entity_id,
                &state.source_revision,
            )? {
                continue;
            }
            let proof = sql_source_deletion_proof(
                source_entity_id,
                &state.source_revision,
                complete_receipt,
            );
            let intent = coalesce_sql_source_tombstone_to_s1(
                binding,
                scope_digest,
                record,
                source_entity_id,
                &state.source_revision,
                proof,
            )
            .map_err(|error| {
                SemanticCodeError::Refused(format!(
                    "authoritative SQL deletion proof rejected: {error:?}"
                ))
            })?;
            receipts.push(
                self.store
                    .enqueue_reconciliation_tombstone(&intent, state, now_ms)?,
            );
        }
        Ok(receipts)
    }

    /// Resolve one bounded SQL wakeup page against the binding already
    /// admitted in this owner. Each row is admitted through the same durable
    /// S1 path; retrying a partially committed page replays already admitted
    /// row intents before continuing with the opaque cursor.
    pub fn admit_sql_source_dirty_page(
        &self,
        record: &MutationOutboxRecord,
        read_port: &dyn SemanticSqlSourceReadPort,
        cursor: Option<&[u8]>,
        now_ms: u64,
    ) -> Result<SemanticSqlSourcePageAdmission, SemanticCodeError> {
        let binding = self.store.read_binding()?.ok_or_else(|| {
            SemanticCodeError::Refused("source wakeup has no durable semantic binding".to_string())
        })?;
        let scope_digest = SemanticDigest::from_bytes(*record.identity.binding_digest().as_bytes());
        let wakeup =
            validate_source_dirty_record(&binding, scope_digest, record).map_err(|error| {
                SemanticCodeError::Refused(format!("source wakeup rejected: {error:?}"))
            })?;
        let page = read_port
            .read_current_sql_source_page(&binding, &wakeup, record, cursor)
            .map_err(|error| {
                SemanticCodeError::Refused(format!(
                    "authoritative SQL source read rejected: {error:?}"
                ))
            })?;
        page.validate().map_err(|error| {
            SemanticCodeError::Refused(format!("authoritative SQL source page rejected: {error:?}"))
        })?;
        validate_page_revision_against_binding(&binding, &page.source_revision)?;
        self.admit_sql_source_page(&binding, scope_digest, record, &page, now_ms)
    }

    /// Read and admit one bounded prefix of an authoritative SQL snapshot.
    /// The owner checkpoint is advanced only after the page's S1 intents are
    /// durable, so a crash can replay a prefix but can never skip it. A
    /// complete page enters a second bounded phase that pages durable prior
    /// identities and emits absence tombstones only after the complete proof.
    /// The source-progress rows written by each S1 admission are the durable
    /// seen set; the checkpoint stores only cursors and counters, avoiding an
    /// unbounded serialized identity vector.
    pub fn admit_sql_source_dirty_reconcile(
        &self,
        record: &MutationOutboxRecord,
        read_port: &dyn SemanticSqlSourceReadPort,
        now_ms: u64,
    ) -> Result<SemanticSqlSourceReconciliationAdmission, SemanticCodeError> {
        let binding = self.store.read_binding()?.ok_or_else(|| {
            SemanticCodeError::Refused("source wakeup has no durable semantic binding".to_string())
        })?;
        let scope_digest = SemanticDigest::from_bytes(*record.identity.binding_digest().as_bytes());
        let wakeup =
            validate_source_dirty_record(&binding, scope_digest, record).map_err(|error| {
                SemanticCodeError::Refused(format!("source wakeup rejected: {error:?}"))
            })?;
        let wakeup_digest = source_reconciliation_wakeup_digest(record, &wakeup);
        let mut checkpoint = match self.load_checkpoint_for_wakeup(&binding, wakeup_digest)? {
            ReconciliationCheckpointState::Owned(checkpoint) => checkpoint,
            ReconciliationCheckpointState::Pending(admission) => return Ok(admission),
        };
        if let Some(admission) = self.resume_tombstone_reconciliation(
            &binding,
            scope_digest,
            record,
            checkpoint.as_ref(),
            now_ms,
        )? {
            return Ok(admission);
        }
        let mut cursor = checkpoint
            .as_ref()
            .and_then(|state| state.source_cursor.clone());
        let mut previous_revision = checkpoint
            .as_ref()
            .map(|state| state.source_revision.clone());
        let mut seen_entities = BTreeSet::new();
        let mut seen_cursors = BTreeSet::new();
        let mut receipts = Vec::new();
        let mut budget = ReconciliationTurnBudget::default();

        loop {
            let page =
                read_reconciliation_page(read_port, &binding, &wakeup, record, cursor.as_deref())?;
            let page_source_bytes = validate_reconciliation_page(
                self,
                &binding,
                &page,
                cursor.as_deref(),
                &mut seen_cursors,
                &mut previous_revision,
                &mut seen_entities,
            )?;
            let page_rows = page.sources.len() as u64;
            if budget.would_exceed(page_rows, page_source_bytes) {
                return self.persist_scan_budget_checkpoint(
                    &binding,
                    previous_revision.as_deref(),
                    cursor.clone(),
                    checkpoint.as_ref(),
                    receipts,
                );
            }

            let page_admission =
                self.admit_sql_source_page(&binding, scope_digest, record, &page, now_ms)?;
            receipts.extend(page_admission.receipts);
            budget.record(page_rows, page_source_bytes);

            let revision = previous_revision.as_deref().ok_or_else(|| {
                SemanticCodeError::Corrupt(
                    "source page had no canonical revision after admission".to_string(),
                )
            })?;
            let rows_seen = checkpoint
                .as_ref()
                .map_or(0, |state| state.rows_seen)
                .checked_add(page_rows)
                .ok_or_else(|| {
                    SemanticCodeError::Refused(
                        "source reconciliation row counter overflowed".to_string(),
                    )
                })?;
            let source_bytes_seen = checkpoint
                .as_ref()
                .map_or(0, |state| state.source_bytes_seen)
                .checked_add(page_source_bytes)
                .ok_or_else(|| {
                    SemanticCodeError::Refused(
                        "source reconciliation byte counter overflowed".to_string(),
                    )
                })?;
            let pages_seen = checkpoint
                .as_ref()
                .map_or(0, |state| state.pages_seen)
                .checked_add(1)
                .ok_or_else(|| {
                    SemanticCodeError::Refused(
                        "source reconciliation page counter overflowed".to_string(),
                    )
                })?;

            if !page.complete {
                match self.persist_partial_reconciliation_page(
                    &binding,
                    checkpoint.as_ref(),
                    wakeup_digest,
                    revision,
                    &page,
                    cursor.as_deref(),
                    rows_seen,
                    source_bytes_seen,
                    pages_seen,
                    &budget,
                    receipts,
                )? {
                    PartialReconciliationProgress::Yield(admission) => return Ok(admission),
                    PartialReconciliationProgress::Continue {
                        checkpoint: next_checkpoint,
                        cursor: next_cursor,
                        receipts: next_receipts,
                    } => {
                        checkpoint = Some(next_checkpoint);
                        cursor = Some(next_cursor);
                        receipts = next_receipts;
                    }
                }
                continue;
            }

            let complete_receipt = page.complete_snapshot_receipt_digest.ok_or_else(|| {
                SemanticCodeError::Corrupt(
                    "validated complete source page lost its read proof".to_string(),
                )
            })?;
            let state = SemanticSourceReconciliationCheckpoint {
                source_wakeup_digest: wakeup_digest,
                source_revision: revision.to_string(),
                phase: SemanticSourceReconciliationPhase::FinalizingTombstones,
                source_cursor: None,
                prior_cursor: None,
                rows_seen,
                source_bytes_seen,
                pages_seen,
                complete_snapshot_receipt_digest: Some(complete_receipt),
            };
            self.store.write_source_reconciliation_checkpoint(
                binding.generation,
                checkpoint.as_ref(),
                &state,
            )?;
            return self.finalize_source_reconciliation(
                &binding,
                scope_digest,
                record,
                state,
                receipts,
                now_ms,
            );
        }
    }

    fn finalize_source_reconciliation(
        &self,
        binding: &SemanticBinding,
        scope_digest: SemanticDigest,
        record: &MutationOutboxRecord,
        state: SemanticSourceReconciliationCheckpoint,
        mut receipts: Vec<SemanticMutationReceipt>,
        now_ms: u64,
    ) -> Result<SemanticSqlSourceReconciliationAdmission, SemanticCodeError> {
        validate_checkpoint_against_binding(binding, &state)?;
        let complete_receipt = state.complete_snapshot_receipt_digest.ok_or_else(|| {
            SemanticCodeError::Corrupt(
                "tombstone reconciliation checkpoint has no complete read proof".to_string(),
            )
        })?;
        let (prior_entities, next_prior_cursor) = self.store.list_source_entities_page(
            binding.generation,
            state.prior_cursor.as_deref(),
            Self::MAX_SOURCE_RECONCILIATION_TOMBSTONES_PER_TURN,
        )?;
        let prior_entities = validate_prior_entity_page(
            prior_entities,
            next_prior_cursor.as_deref(),
            state.prior_cursor.as_deref(),
            Self::MAX_SOURCE_RECONCILIATION_TOMBSTONES_PER_TURN,
        )?;
        receipts = self.admit_missing_tombstones(
            binding,
            scope_digest,
            record,
            &state,
            &prior_entities,
            complete_receipt,
            receipts,
            now_ms,
        )?;

        if let Some(prior_cursor) = next_prior_cursor {
            let next_state = SemanticSourceReconciliationCheckpoint {
                prior_cursor: Some(prior_cursor),
                ..state.clone()
            };
            self.store.write_source_reconciliation_checkpoint(
                binding.generation,
                Some(&state),
                &next_state,
            )?;
            return admission_result(receipts, &next_state, false);
        }

        // Clearing follows every final tombstone intent. A crash before this
        // point rechecks the bounded prior page and replays already-durable
        // intents; a crash after it cannot leave a false complete marker.
        self.store
            .clear_source_reconciliation_checkpoint(binding.generation, &state)?;
        admission_result(receipts, &state, true)
    }

    /// Resolve one coarse SQL wakeup when the source owner guarantees one
    /// complete row. Multi-row providers must use
    /// [`Self::admit_sql_source_dirty_page`].
    pub fn admit_sql_source_dirty_record(
        &self,
        record: &MutationOutboxRecord,
        read_port: &dyn SemanticSqlSourceReadPort,
        now_ms: u64,
    ) -> Result<SemanticMutationReceipt, SemanticCodeError> {
        let binding = self.store.read_binding()?.ok_or_else(|| {
            SemanticCodeError::Refused("source wakeup has no durable semantic binding".to_string())
        })?;
        let scope_digest = SemanticDigest::from_bytes(*record.identity.binding_digest().as_bytes());
        let wakeup =
            validate_source_dirty_record(&binding, scope_digest, record).map_err(|error| {
                SemanticCodeError::Refused(format!("source wakeup rejected: {error:?}"))
            })?;
        let page = read_port
            .read_current_sql_source_page(&binding, &wakeup, record, None)
            .map_err(|error| {
                SemanticCodeError::Refused(format!(
                    "authoritative SQL source read rejected: {error:?}"
                ))
            })?;
        page.validate().map_err(|error| {
            SemanticCodeError::Refused(format!("authoritative SQL source page rejected: {error:?}"))
        })?;
        validate_page_revision_against_binding(&binding, &page.source_revision)?;
        if !page.complete || page.sources.len() != 1 {
            return Err(SemanticCodeError::Refused(
                "single-row SQL admission requires one complete source page".to_string(),
            ));
        }
        let admission =
            self.admit_sql_source_page(&binding, scope_digest, record, &page, now_ms)?;
        admission.receipts.into_iter().next().ok_or_else(|| {
            SemanticCodeError::Refused("complete single-row SQL page was empty".to_string())
        })
    }

    /// Admit S1 after a source refresh has advanced the durable binding head.
    /// The current and replacement rows are both checked before the intent is
    /// built, so a caller cannot route a snapshot into an unrelated binding or
    /// generation.
    pub fn admit_sql_source_dirty_replacement(
        &self,
        replacement: &SemanticBinding,
        source_manifest: &SemanticSqlSourceManifest,
        record: &MutationOutboxRecord,
        now_ms: u64,
    ) -> Result<SemanticMutationReceipt, SemanticCodeError> {
        let current = self.store.read_binding()?.ok_or_else(|| {
            SemanticCodeError::Refused(
                "replacement source wakeup has no current semantic binding".to_string(),
            )
        })?;
        if current.binding_id != replacement.binding_id
            || current.tenant_id != replacement.tenant_id
            || current.generation != replacement.generation
            || current.binding_digest != replacement.binding_digest
            || current.durable_state != SemanticBindingState::Pending
        {
            return Err(SemanticCodeError::Refused(
                "replacement source wakeup does not name the durable binding head".to_string(),
            ));
        }
        let scope_digest = SemanticDigest::from_bytes(*record.identity.binding_digest().as_bytes());
        validate_source_dirty_record(&current, scope_digest, record).map_err(|error| {
            SemanticCodeError::Refused(format!("source wakeup rejected: {error:?}"))
        })?;
        let source_entity_id = source_manifest.source_entity_id.as_str();
        let intent = coalesce_sql_snapshot_to_s1(
            &current,
            source_entity_id,
            scope_digest,
            record,
            source_manifest,
        )
        .map_err(|error| {
            SemanticCodeError::Refused(format!("semantic replacement rejected: {error:?}"))
        })?;
        self.store.enqueue_stage_intent(&intent, now_ms)
    }

    pub fn binding(&self) -> Result<Option<SemanticBinding>, SemanticCodeError> {
        self.store.read_binding()
    }

    /// Read the one durable prior SQL source manifest needed to prove a
    /// complete-page deletion. The store fences this lookup to the current
    /// binding generation and canonical entity key; this method never
    /// reconstructs deleted bytes or invents a deletion proof.
    pub fn sql_source_manifest(
        &self,
        generation: u64,
        source_entity_id: &str,
    ) -> Result<Option<SemanticSqlSourceManifest>, SemanticCodeError> {
        self.store.sql_source_manifest(generation, source_entity_id)
    }

    /// Bounded catalog read over the one authenticated binding owner. The
    /// cursor is the last binding id, so a caller cannot turn this into an
    /// unbounded file scan or select a different tenant by cursor text.
    pub fn list_bindings(
        &self,
        filter: &SemanticIndexFilter,
        cursor: Option<&str>,
    ) -> Result<(Vec<SemanticBinding>, Option<String>), SemanticCodeError> {
        filter.validate().map_err(|error| {
            SemanticCodeError::Refused(format!("semantic filter rejected: {error:?}"))
        })?;
        let Some(binding) = self.store.read_binding()? else {
            return Ok((Vec::new(), None));
        };
        if !self.store.binding_matches_filter(filter)? {
            return Ok((Vec::new(), None));
        }
        if let Some(cursor) = cursor {
            if cursor != binding.binding_id {
                return Err(SemanticCodeError::Refused(
                    "semantic binding cursor is outside this owner".to_string(),
                ));
            }
            return Ok((Vec::new(), None));
        }
        Ok((vec![binding], None))
    }

    pub fn live_generation(&self) -> Result<Option<u64>, SemanticCodeError> {
        self.store.live_generation()
    }

    /// Check the existing outbox lease and canonical S1-S6 identity before an
    /// executor starts.  The executor must still persist the stage transition
    /// and acknowledge through the existing durable consumer port.
    pub fn validate_stage_lease(
        &self,
        lease: &eg_types::mutation_batch::MutationOutboxLease,
        consumer: &str,
        now_ms: u64,
    ) -> Result<SemanticStageIntent, SemanticCodeError> {
        self.store.validate_stage_lease(lease, consumer, now_ms)
    }

    pub fn subscribe_stage_consumer(&self, consumer: &str) -> Result<(), SemanticCodeError> {
        self.store.subscribe_stage_consumer(consumer)
    }

    pub fn claim_stage_leases(
        &self,
        consumer: &str,
        budget: &mut OutboxClaimBudget,
    ) -> Result<OutboxClaimOutcome, SemanticCodeError> {
        self.store.claim_stage_leases(consumer, budget)
    }

    pub fn stage_status(
        &self,
        consumer: &str,
        now_ms: u64,
    ) -> Result<OutboxStatus, SemanticCodeError> {
        self.store.stage_status(consumer, now_ms)
    }

    /// Persist one terminal transition, publish the successor intent when its
    /// predecessor proof is complete, then acknowledge the exact lease through
    /// the shared outbox cursor.
    pub fn complete_stage(
        &self,
        lease: &MutationOutboxLease,
        transition: &SemanticStageTransition,
        artifact: &SemanticStageArtifact,
        successor: Option<&SemanticStageIntent>,
        now_ms: u64,
    ) -> Result<SemanticMutationReceipt, SemanticCodeError> {
        self.store
            .complete_stage(lease, transition, artifact, successor, now_ms)
    }

    /// Complete an S1 source transition from its authenticated raw SQL claim.
    /// The claim's ACL decision is converted into a full authorization receipt
    /// before the manifest and receipt are handed to the durable store. The
    /// caller must provide the decision's authoritative authorization time;
    /// this method never fabricates one from the admission clock.
    pub fn complete_sql_source_stage(
        &self,
        lease: &MutationOutboxLease,
        transition: &SemanticStageTransition,
        source: &SemanticSqlSourceRecord,
        authorized_at: &str,
        successor: Option<&SemanticStageIntent>,
        now_ms: u64,
    ) -> Result<SemanticMutationReceipt, SemanticCodeError> {
        let binding = self.store.read_binding()?.ok_or_else(|| {
            SemanticCodeError::Refused(
                "SQL source completion has no durable semantic binding".to_string(),
            )
        })?;
        let artifact = sql_source_stage_artifact(&binding, transition, source, authorized_at)
            .map_err(|error| {
                SemanticCodeError::Refused(format!(
                    "semantic SQL source completion rejected: {error:?}"
                ))
            })?;
        self.store
            .complete_stage(lease, transition, &artifact, successor, now_ms)
    }

    /// Replay a previously committed SQL S1 completion before the caller
    /// re-reads the authoritative source or ACL.  The store owns the durable
    /// mutation and lease lookup: it only acknowledges a current, fenced
    /// lease when the persisted `RecordStageTransition` and SQL manifest
    /// artifact match `expected_intent`, then returns the durable transition
    /// and ledger receipt.  The cursor and completion time are read from the
    /// retained transition, so a restarted caller does not reconstruct them.
    /// `None` means the S1 mutation is not committed yet and the caller may
    /// proceed through [`Self::complete_sql_source_stage`].
    pub fn replay_completed_sql_source_stage(
        &self,
        lease: &MutationOutboxLease,
        expected_intent: &SemanticStageIntent,
        now_ms: u64,
    ) -> Result<Option<(SemanticStageTransition, SemanticMutationReceipt)>, SemanticCodeError> {
        expected_intent
            .validate()
            .map_err(semantic_contract_error)?;
        if expected_intent.stage != SemanticStage::SourceCommit
            || expected_intent.scope.source_entity_id().is_none()
        {
            return Err(SemanticCodeError::Refused(
                "semantic SQL source replay requires one completed entity-scoped S1".to_string(),
            ));
        }
        self.store
            .replay_completed_sql_source_stage(lease, expected_intent, now_ms)
    }

    /// Complete S3 or S5 and persist its canonical lexical/ANN manifest in
    /// the same owner mutation as the transition, checkpoint, successor and
    /// lease acknowledgement.
    pub fn complete_generation_stage(
        &self,
        lease: &MutationOutboxLease,
        transition: &SemanticStageTransition,
        artifact: &SemanticGenerationArtifact,
        successor: Option<&SemanticStageIntent>,
        now_ms: u64,
    ) -> Result<SemanticMutationReceipt, SemanticCodeError> {
        self.store
            .complete_generation_stage(lease, transition, artifact, successor, now_ms)
    }

    pub fn release_stage_lease(
        &self,
        lease: &MutationOutboxLease,
    ) -> Result<(), SemanticCodeError> {
        self.store.release_stage_lease(lease)
    }

    /// The sole semantic publication entry point. S6 must be a completed
    /// generation-scoped transition whose activation target/pointer and both
    /// lexical/ANN checkpoints validate before the durable code tier is made
    /// live. The lower-level code writer remains an implementation primitive;
    /// request and consumer routes call this method instead.
    pub fn activate_s6(
        &self,
        lease: &MutationOutboxLease,
        transition: &SemanticStageTransition,
        checkpoint: &SemanticGenerationCheckpoint,
        artifact: &SemanticGenerationArtifact,
        image: &SemanticGenerationImage,
        now_ms: u64,
    ) -> Result<SemanticMutationReceipt, SemanticCodeError> {
        if transition.intent.stage != SemanticStage::ReconcileAndActivate
            || !matches!(transition.intent.scope, SemanticStageScope::Generation)
        {
            return Err(SemanticCodeError::Refused(
                "ANN publication requires a generation-scoped S6 transition".to_string(),
            ));
        }
        let SemanticGenerationArtifact::Activation { target, pointer } = artifact else {
            return Err(SemanticCodeError::Refused(
                "S6 publication requires the canonical activation artifact".to_string(),
            ));
        };
        target.validate().map_err(|error| {
            SemanticCodeError::Refused(format!("activation target rejected: {error:?}"))
        })?;
        pointer.validate().map_err(|error| {
            SemanticCodeError::Refused(format!("active pointer rejected: {error:?}"))
        })?;
        let receipt = self
            .store
            .finalize_generation(lease, transition, checkpoint, artifact, image, now_ms)?;
        Ok(receipt)
    }
}

/// Shared `SemanticBinding` test-fixture builder for this module's own unit
/// tests and the reconciliation regression suite in the sibling
/// `reconciliation_regression` module below. RF-019's `SemanticBindingDraft`
/// carries roughly twenty governed fields -- the RBAC/row/source-ACL policy
/// digests, the SQL source selector, the embedding model identity, the
/// lexical and ANN index specs -- and both suites had independently settled
/// on the same values for every field except the ones that identify what is
/// actually under test: which tenant and binding, which SQL column the
/// source selector names, that source's declared schema/field-set digests,
/// and the binding's mint timestamp. Those become parameters; everything
/// else is the one governed shape both suites already agreed on.
#[cfg(test)]
#[allow(clippy::too_many_arguments)]
pub(super) fn semantic_binding_test_fixture(
    tenant_id: &str,
    binding_id: &str,
    table_id: &str,
    source_revision: &str,
    source_schema_digest: &str,
    source_field_set_digest: &str,
    created_at: &str,
) -> SemanticBinding {
    use eg_types::semantic_index::{
        SemanticAnnIndexMethod, SemanticAnnIndexSpec, SemanticBindingDraft,
        SemanticLexicalIndexSpec, SemanticModelIdentity, SemanticPolicyComponents,
        SemanticSourceSelector, SemanticVectorMetric, SqlColumnRef, SEMANTIC_SQL_CATALOG_ID,
    };

    SemanticBinding::create(SemanticBindingDraft {
        binding_id: binding_id.into(),
        tenant_id: tenant_id.into(),
        actor_scope: "semantic:index-maintainer".into(),
        effective_actor_scope: "semantic:agent:index-maintainer".into(),
        purpose_id: "retrieval".into(),
        policy: SemanticPolicyComponents {
            rbac_policy_revision: 1,
            rbac_policy_digest: "sha256:rbac".into(),
            row_policy_revision: 1,
            row_policy_digest: "sha256:row-policy".into(),
            source_acl_revision: 7,
            source_acl_digest: "sha256:sql-acl-state".into(),
        },
        source_selector: SemanticSourceSelector::SqlColumnRef(SqlColumnRef {
            catalog_id: SEMANTIC_SQL_CATALOG_ID.into(),
            schema_id: "public".into(),
            table_id: table_id.into(),
            column_id: "body".into(),
        }),
        source_schema_digest: source_schema_digest.into(),
        source_revision: source_revision.into(),
        source_field_set_digest: source_field_set_digest.into(),
        dimension: 3,
        metric: SemanticVectorMetric::Cosine,
        model: SemanticModelIdentity {
            model_id: "embedding-model".into(),
            model_revision: "revision-1".into(),
            preprocess_digest: "sha256:preprocess".into(),
            model_digest: "sha256:model".into(),
        },
        generation: 1,
        maintenance_policy_id: "semantic-maintenance".into(),
        lexical_index: SemanticLexicalIndexSpec {
            analyzer_id: "standard".into(),
            analyzer_revision: "1".into(),
            analyzer_config_digest: "sha256:analyzer".into(),
        },
        ann_index: SemanticAnnIndexSpec {
            method: SemanticAnnIndexMethod::IvfPq,
            parameters_digest: "sha256:ann-parameters".into(),
        },
        created_at: created_at.into(),
    })
    .unwrap()
}

#[cfg(test)]
mod reconciliation_regression;

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    use sha2::{Digest, Sha256};

    use super::{
        coalesce_sql_source_record_to_s1, source_reconciliation_wakeup_digest,
        sql_source_deletion_proof, sql_source_revision_for_epoch, sql_source_stage_artifact,
        validate_page_revision_against_binding, SemanticIndexService,
        SemanticSourceReconciliationCheckpoint, SemanticSourceReconciliationPhase,
        SemanticSqlSourceReadPage, SemanticSqlSourceReadPort, SemanticSqlSourceRecord,
        SemanticSqlSourceValue,
    };
    use crate::test_scope_grant::{TEST_PRINCIPAL, TEST_PROOF};
    use eg_storage::OwnerLayout;
    use eg_transaction::OutboxClaimBudget;
    use eg_types::contract::Nonce;
    use eg_types::mutation_batch::{
        CommittedVersion, DurabilityDomain, MutationOutboxIntent, MutationOutboxRecord,
        MutationScopeIdentity, MUTATION_BATCH_VERSION,
    };
    use eg_types::semantic_index::{
        SemanticBinding, SemanticBindingState, SemanticDigest, SemanticIndexError,
        SemanticSourceDirtyIntent, SemanticSqlSourceIdentity, SemanticStageArtifact,
        SemanticStageIntent, SemanticStageOutcome, SemanticStageReceipt, SemanticStageTransition,
        SqlColumnRef, SEMANTIC_SOURCE_DIRTY_TOPIC, SEMANTIC_SQL_CATALOG_ID,
    };

    /// Tenant-parameterized scope fence for this module's durable fixtures.
    ///
    /// `crate::test_scope_grant::TestScopeVerifier` is fenced to the literal
    /// tenant `"native"`, which is the scope eg-core's OTHER store fixtures
    /// open. These fixtures open a real tenant name, so the shared verifier
    /// refused their first durable write with "test scope authority rejected"
    /// -- the failure three of this module's own tests hit the moment the
    /// module was first compiled, years of never being built later. Same
    /// fence, parameterized by the tenant actually under test.
    struct SemanticTenantScopeVerifier {
        tenant: &'static str,
    }

    impl eg_storage::ScopeGrantVerifier for SemanticTenantScopeVerifier {
        fn verify(
            &self,
            _physical: &eg_storage::PhysicalStoreIdentity,
            layout: OwnerLayout,
            identity: &eg_types::MutationScopeIdentity,
            principal: &str,
            proof: &[u8],
        ) -> Result<(), String> {
            if layout != OwnerLayout::SemanticIndex
                || identity.tenant().as_str() != self.tenant
                || principal != TEST_PRINCIPAL
                || proof != TEST_PROOF
            {
                return Err("semantic tenant scope authority rejected".to_string());
            }
            Ok(())
        }
    }

    fn digest(byte: u8) -> SemanticDigest {
        SemanticDigest::from_bytes([byte; 32])
    }

    fn content_digest(bytes: &[u8]) -> SemanticDigest {
        SemanticDigest::from_bytes(Sha256::digest(bytes).into())
    }

    fn binding(source_revision: &str) -> SemanticBinding {
        binding_for("tenant-a", "binding:articles-body", source_revision)
    }

    fn binding_for(tenant_id: &str, binding_id: &str, source_revision: &str) -> SemanticBinding {
        super::semantic_binding_test_fixture(
            tenant_id,
            binding_id,
            "articles",
            source_revision,
            "sha256:schema",
            "sha256:field-set",
            "2026-09-08T00:00:00Z",
        )
    }

    fn source_record(
        binding: &SemanticBinding,
        record_identity_byte: u8,
        source_revision: &str,
        value: SemanticSqlSourceValue,
    ) -> SemanticSqlSourceRecord {
        let selector = SqlColumnRef {
            catalog_id: SEMANTIC_SQL_CATALOG_ID.into(),
            schema_id: "public".into(),
            table_id: "articles".into(),
            column_id: "body".into(),
        };
        SemanticSqlSourceRecord {
            source_identity: SemanticSqlSourceIdentity::create(
                &selector,
                binding.tenant_id.clone(),
                digest(record_identity_byte),
            ),
            source_revision: source_revision.into(),
            value,
            source_schema_revision: 9,
            source_schema_digest: binding.source_schema_digest.clone(),
            source_field_set_digest: binding.source_field_set_digest.clone(),
            source_acl_revision: binding.policy_identity.components.source_acl_revision,
            source_acl_digest: binding.policy_identity.components.source_acl_digest.clone(),
            authorization_receipt_digest: digest(40),
        }
    }

    fn dirty_record(method_input_byte: u8, source: u64, target: u64) -> MutationOutboxRecord {
        dirty_record_for("tenant-a", method_input_byte, source, target)
    }

    fn dirty_record_for(
        tenant: &str,
        method_input_byte: u8,
        source: u64,
        target: u64,
    ) -> MutationOutboxRecord {
        let identity = MutationScopeIdentity::fixed_native(
            tenant,
            DurabilityDomain::SqlCatalog,
            "catalog",
            "authority-a",
        )
        .unwrap();
        let wakeup = SemanticSourceDirtyIntent::new(
            SemanticDigest::from_bytes(*identity.binding_digest().as_bytes()),
            digest(method_input_byte),
        )
        .to_canonical_cbor()
        .unwrap();
        MutationOutboxRecord {
            schema_version: MUTATION_BATCH_VERSION,
            batch_id: "batch-1".into(),
            ordinal: 0,
            identity,
            committed_version: CommittedVersion::Native { source, target },
            commit_sequence: None,
            intent: MutationOutboxIntent {
                topic: SEMANTIC_SOURCE_DIRTY_TOPIC.into(),
                key: "batch-1".into(),
                payload: wakeup,
                headers: BTreeMap::new(),
            },
            created_at_ms: 1,
        }
    }

    fn record_revision(record: &MutationOutboxRecord) -> String {
        let _ = record;
        sql_source_revision_for_epoch(digest(170), 2).unwrap()
    }

    #[test]
    fn tenant_source_revision_binds_authority_and_epoch() {
        let revision = sql_source_revision_for_epoch(digest(170), 42).unwrap();
        assert_eq!(
            revision,
            "sql-source:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:epoch:42"
        );
        assert!(sql_source_revision_for_epoch(digest(170), 0).is_err());
        assert!(sql_source_revision_for_epoch(digest(171), 43).is_ok());
    }

    #[test]
    fn source_bytes_change_without_changing_the_resolved_entity() {
        let authority = digest(170);
        let first_revision = sql_source_revision_for_epoch(authority, 1).unwrap();
        let first_binding = binding(&first_revision);
        let first = source_record(
            &first_binding,
            11,
            &first_revision,
            SemanticSqlSourceValue::Present {
                source_bytes: b"article body at source revision one".to_vec(),
            },
        );
        let changed = source_record(
            &first_binding,
            11,
            &sql_source_revision_for_epoch(authority, 2).unwrap(),
            SemanticSqlSourceValue::Present {
                source_bytes: b"article body at source revision two".to_vec(),
            },
        );
        assert_eq!(first.source_entity_id(), changed.source_entity_id());
        assert_ne!(
            first.source_content_digest(),
            changed.source_content_digest()
        );
    }

    #[test]
    fn repeated_dirty_notifications_use_authoritative_content_once() {
        let record = dirty_record(31, 1, 2);
        let revision = record_revision(&record);
        let binding = binding(&revision);
        let source = source_record(
            &binding,
            11,
            &revision,
            SemanticSqlSourceValue::Present {
                source_bytes: b"authoritative article body".to_vec(),
            },
        );
        let scope_digest = SemanticDigest::from_bytes(*record.identity.binding_digest().as_bytes());
        let first =
            coalesce_sql_source_record_to_s1(&binding, scope_digest, &record, &source).unwrap();

        let repeated = dirty_record(33, 1, 2);
        let second =
            coalesce_sql_source_record_to_s1(&binding, scope_digest, &repeated, &source).unwrap();
        assert_eq!(first, second);
        assert_eq!(
            first.input_digest,
            content_digest(b"authoritative article body")
        );
        assert_ne!(first.input_digest, digest(31));
        assert_ne!(first.input_digest, digest(33));
    }

    #[test]
    fn sql_acl_decision_is_wrapped_before_s1_manifest_artifact_creation() {
        let revision = sql_source_revision_for_epoch(digest(170), 2).unwrap();
        let binding = binding(&revision);
        let source = source_record(
            &binding,
            11,
            &revision,
            SemanticSqlSourceValue::Present {
                source_bytes: b"authorized article body".to_vec(),
            },
        );
        let raw_decision_digest = source.authorization_receipt_digest;
        let authorization = source
            .authorization_receipt(&binding, "2026-09-08T00:00:05Z")
            .unwrap();
        assert_eq!(
            authorization.policy_decision_digest,
            raw_decision_digest.to_string()
        );
        assert_ne!(
            authorization.authorization_receipt_digest, raw_decision_digest,
            "the ACL decision digest is not itself the full receipt digest"
        );

        let record = dirty_record(91, 1, 2);
        let scope_digest = SemanticDigest::from_bytes(*record.identity.binding_digest().as_bytes());
        let intent =
            coalesce_sql_source_record_to_s1(&binding, scope_digest, &record, &source).unwrap();
        let transition = SemanticStageTransition {
            intent: intent.clone(),
            receipt: SemanticStageReceipt {
                intent_digest: intent.intent_digest,
                output_digest: source.source_content_digest(),
                cursor: "source:complete".to_string(),
                completed_at: "2026-09-08T00:00:06Z".to_string(),
                outcome: SemanticStageOutcome::Completed,
            },
            generation_checkpoint: None,
        };
        let artifact =
            sql_source_stage_artifact(&binding, &transition, &source, "2026-09-08T00:00:05Z")
                .unwrap();
        let SemanticStageArtifact::SqlSourceManifest {
            manifest,
            authorization,
        } = artifact
        else {
            panic!("S1 source completion must create a SQL manifest artifact");
        };
        assert_eq!(
            authorization.policy_decision_digest,
            raw_decision_digest.to_string()
        );
        assert_eq!(
            manifest.authorization_receipt_digest,
            authorization.authorization_receipt_digest
        );
        assert_ne!(
            manifest.authorization_receipt_digest, raw_decision_digest,
            "the persisted manifest must point at the canonical full receipt"
        );
    }

    #[test]
    fn s1_artifact_rejects_source_bytes_different_from_admitted_intent() {
        let revision = sql_source_revision_for_epoch(digest(170), 2).unwrap();
        let binding = binding(&revision);
        let source_a = source_record(
            &binding,
            11,
            &revision,
            SemanticSqlSourceValue::Present {
                source_bytes: b"source bytes admitted by S1".to_vec(),
            },
        );
        let source_b = source_record(
            &binding,
            11,
            &revision,
            SemanticSqlSourceValue::Present {
                source_bytes: b"different bytes read during completion".to_vec(),
            },
        );
        let record = dirty_record(92, 1, 2);
        let scope_digest = SemanticDigest::from_bytes(*record.identity.binding_digest().as_bytes());
        let intent =
            coalesce_sql_source_record_to_s1(&binding, scope_digest, &record, &source_a).unwrap();
        let transition = SemanticStageTransition {
            intent: intent.clone(),
            receipt: SemanticStageReceipt {
                intent_digest: intent.intent_digest,
                output_digest: source_b.source_content_digest(),
                cursor: "source:complete".to_string(),
                completed_at: "2026-09-08T00:00:06Z".to_string(),
                outcome: SemanticStageOutcome::Completed,
            },
            generation_checkpoint: None,
        };
        assert!(matches!(
            sql_source_stage_artifact(&binding, &transition, &source_b, "2026-09-08T00:00:05Z"),
            Err(SemanticIndexError::StageArtifactMismatch)
        ));
    }

    #[test]
    fn sql_source_s1_rejects_idempotent_noop_for_fresh_manifest() {
        let revision = sql_source_revision_for_epoch(digest(170), 2).unwrap();
        let binding = binding(&revision);
        let source = source_record(
            &binding,
            11,
            &revision,
            SemanticSqlSourceValue::Present {
                source_bytes: b"source bytes for completed S1".to_vec(),
            },
        );
        let record = dirty_record(93, 1, 2);
        let scope_digest = SemanticDigest::from_bytes(*record.identity.binding_digest().as_bytes());
        let intent =
            coalesce_sql_source_record_to_s1(&binding, scope_digest, &record, &source).unwrap();
        let transition = SemanticStageTransition {
            intent: intent.clone(),
            receipt: SemanticStageReceipt {
                intent_digest: intent.intent_digest,
                output_digest: source.source_content_digest(),
                cursor: "source:idempotent-noop".to_string(),
                completed_at: "2026-09-08T00:00:07Z".to_string(),
                outcome: SemanticStageOutcome::IdempotentNoop,
            },
            generation_checkpoint: None,
        };

        // A SQL source S1 must produce a manifest with a completed receipt;
        // the generic no-op/None-artifact path cannot stand in for that proof.
        assert!(matches!(
            sql_source_stage_artifact(&binding, &transition, &source, "2026-09-08T00:00:05Z"),
            Err(SemanticIndexError::StageArtifactMismatch)
        ));
    }

    struct SingleSourcePage {
        source: SemanticSqlSourceRecord,
    }

    impl SemanticSqlSourceReadPort for SingleSourcePage {
        fn read_current_sql_source_page(
            &self,
            _binding: &SemanticBinding,
            _wakeup: &SemanticSourceDirtyIntent,
            _record: &MutationOutboxRecord,
            cursor: Option<&[u8]>,
        ) -> Result<SemanticSqlSourceReadPage, SemanticIndexError> {
            if cursor.is_some() {
                return Err(SemanticIndexError::SourceManifestMismatch);
            }
            Ok(SemanticSqlSourceReadPage {
                source_revision: self.source.source_revision.clone(),
                complete_snapshot_receipt_digest: Some(digest(77)),
                sources: vec![self.source.clone()],
                next_cursor: None,
                complete: true,
            })
        }
    }

    struct RecordingEmptyPage {
        revision: String,
        calls: Arc<Mutex<Vec<Option<Vec<u8>>>>>,
    }

    impl SemanticSqlSourceReadPort for RecordingEmptyPage {
        fn read_current_sql_source_page(
            &self,
            _binding: &SemanticBinding,
            _wakeup: &SemanticSourceDirtyIntent,
            _record: &MutationOutboxRecord,
            cursor: Option<&[u8]>,
        ) -> Result<SemanticSqlSourceReadPage, SemanticIndexError> {
            self.calls
                .lock()
                .unwrap()
                .push(cursor.map(|bytes| bytes.to_vec()));
            Ok(SemanticSqlSourceReadPage {
                source_revision: self.revision.clone(),
                complete_snapshot_receipt_digest: Some(digest(78)),
                sources: Vec::new(),
                next_cursor: None,
                complete: true,
            })
        }
    }

    #[test]
    fn durable_service_replays_repeated_dirty_after_authoritative_read() {
        let dir = std::env::temp_dir().join(format!(
            "eg-semantic-source-service-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let record = dirty_record_for("native", 61, 1, 2);
        let revision = record_revision(&record);
        let binding = binding_for("native", "binding:articles-body", &revision);
        let source = source_record(
            &binding,
            11,
            &revision,
            SemanticSqlSourceValue::Present {
                source_bytes: b"durable authoritative body".to_vec(),
            },
        );
        let service = SemanticIndexService::open(
            &dir,
            Arc::new(SemanticTenantScopeVerifier {
                tenant: "native",
            }),
            TEST_PRINCIPAL,
            TEST_PROOF,
            "native",
            "binding:articles-body",
        )
        .unwrap();
        service.admit_binding(&binding, 1).unwrap();
        let read_port = SingleSourcePage { source };

        let first = service
            .admit_sql_source_dirty_record(&record, &read_port, 2)
            .unwrap();
        let repeated = dirty_record_for("native", 63, 1, 2);
        let second = service
            .admit_sql_source_dirty_record(&repeated, &read_port, 3)
            .unwrap();
        assert!(!first.replayed);
        assert!(second.replayed);
        assert_eq!(first.mutation_digest, second.mutation_digest);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn newer_dirty_wakeup_waits_for_an_older_finalizing_checkpoint() {
        let dir = std::env::temp_dir().join(format!(
            "eg-semantic-source-reconcile-r2-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let record_r1 = dirty_record(81, 41, 42);
        let record_r2 = dirty_record(81, 42, 43);
        let revision_r1 = sql_source_revision_for_epoch(digest(170), 41).unwrap();
        let revision_r2 = sql_source_revision_for_epoch(digest(170), 42).unwrap();
        let binding = binding(&revision_r1);
        let service = SemanticIndexService::open(
            &dir,
            Arc::new(SemanticTenantScopeVerifier {
                tenant: "tenant-a",
            }),
            TEST_PRINCIPAL,
            TEST_PROOF,
            "tenant-a",
            "binding:articles-body",
        )
        .unwrap();
        service.admit_binding(&binding, 1).unwrap();
        let wakeup_r1 =
            SemanticSourceDirtyIntent::from_canonical_cbor(&record_r1.intent.payload).unwrap();
        let checkpoint = SemanticSourceReconciliationCheckpoint {
            source_wakeup_digest: source_reconciliation_wakeup_digest(&record_r1, &wakeup_r1),
            source_revision: revision_r1.clone(),
            phase: SemanticSourceReconciliationPhase::FinalizingTombstones,
            source_cursor: None,
            prior_cursor: None,
            rows_seen: 1,
            source_bytes_seen: 2,
            pages_seen: 1,
            complete_snapshot_receipt_digest: Some(digest(77)),
        };
        service
            .store
            .write_source_reconciliation_checkpoint(binding.generation, None, &checkpoint)
            .unwrap();

        let calls_r2 = Arc::new(Mutex::new(Vec::new()));
        let read_port_r2 = RecordingEmptyPage {
            revision: revision_r2,
            calls: Arc::clone(&calls_r2),
        };
        let pending = service
            .admit_sql_source_dirty_reconcile(&record_r2, &read_port_r2, 2)
            .unwrap();
        assert!(!pending.complete);
        assert!(!pending.wakeup_consumed);
        assert_eq!(pending.source_revision, revision_r1);
        assert_eq!(pending.rows_seen, 1);
        assert!(calls_r2.lock().unwrap().is_empty());
        assert!(service
            .store
            .read_source_reconciliation_checkpoint(binding.generation)
            .unwrap()
            .is_some());

        let calls_r1 = Arc::new(Mutex::new(Vec::new()));
        let read_port_r1 = RecordingEmptyPage {
            revision: sql_source_revision_for_epoch(digest(170), 41).unwrap(),
            calls: Arc::clone(&calls_r1),
        };
        let admission_r1 = service
            .admit_sql_source_dirty_reconcile(&record_r1, &read_port_r1, 3)
            .unwrap();
        assert!(admission_r1.complete);
        assert!(admission_r1.wakeup_consumed);
        assert!(calls_r1.lock().unwrap().is_empty());
        assert!(service
            .store
            .read_source_reconciliation_checkpoint(binding.generation)
            .unwrap()
            .is_none());

        let admission_r2 = service
            .admit_sql_source_dirty_reconcile(&record_r2, &read_port_r2, 4)
            .unwrap();
        assert!(admission_r2.complete);
        assert!(admission_r2.wakeup_consumed);
        assert_eq!(calls_r2.lock().unwrap().as_slice(), &[None]);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn same_dirty_wakeup_resumes_finalization_and_stale_clear_is_rejected() {
        let dir = std::env::temp_dir().join(format!(
            "eg-semantic-source-reconcile-replay-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let record = dirty_record(83, 51, 52);
        let revision = sql_source_revision_for_epoch(digest(170), 51).unwrap();
        let binding = binding(&revision);
        let service = SemanticIndexService::open(
            &dir,
            Arc::new(SemanticTenantScopeVerifier {
                tenant: "tenant-a",
            }),
            TEST_PRINCIPAL,
            TEST_PROOF,
            "tenant-a",
            "binding:articles-body",
        )
        .unwrap();
        service.admit_binding(&binding, 1).unwrap();
        let wakeup =
            SemanticSourceDirtyIntent::from_canonical_cbor(&record.intent.payload).unwrap();
        let checkpoint = SemanticSourceReconciliationCheckpoint {
            source_wakeup_digest: source_reconciliation_wakeup_digest(&record, &wakeup),
            source_revision: revision.clone(),
            phase: SemanticSourceReconciliationPhase::FinalizingTombstones,
            source_cursor: None,
            prior_cursor: None,
            rows_seen: 1,
            source_bytes_seen: 2,
            pages_seen: 1,
            complete_snapshot_receipt_digest: Some(digest(79)),
        };
        service
            .store
            .write_source_reconciliation_checkpoint(binding.generation, None, &checkpoint)
            .unwrap();
        let calls = Arc::new(Mutex::new(Vec::new()));
        let read_port = RecordingEmptyPage {
            revision,
            calls: Arc::clone(&calls),
        };
        let admission = service
            .admit_sql_source_dirty_reconcile(&record, &read_port, 2)
            .unwrap();
        assert!(admission.complete);
        assert!(calls.lock().unwrap().is_empty());
        assert!(service
            .store
            .clear_source_reconciliation_checkpoint(binding.generation, &checkpoint)
            .is_err());
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn complete_snapshot_replays_s1_after_reopen_and_emits_cross_epoch_tombstone() {
        let dir = std::env::temp_dir().join(format!(
            "eg-semantic-source-tombstone-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let revision_r1 = sql_source_revision_for_epoch(digest(170), 41).unwrap();
        let revision_r2 = sql_source_revision_for_epoch(digest(170), 42).unwrap();
        let binding = binding(&revision_r1);
        let record_r1 = dirty_record(84, 41, 42);
        let record_r2 = dirty_record(84, 42, 43);
        let source_r1 = source_record(
            &binding,
            11,
            &revision_r1,
            SemanticSqlSourceValue::Present {
                source_bytes: b"row present at epoch one".to_vec(),
            },
        );

        let service = SemanticIndexService::open(
            &dir,
            Arc::new(SemanticTenantScopeVerifier {
                tenant: "tenant-a",
            }),
            TEST_PRINCIPAL,
            TEST_PROOF,
            "tenant-a",
            "binding:articles-body",
        )
        .unwrap();
        service.admit_binding(&binding, 1).unwrap();

        // Seed the completed R1 progress through the public source admission
        // and consumer lifecycle. The test must exercise the same durable
        // stage/receipt rows as production so the R2 absence proof cannot
        // depend on private store tables or test-only mutation injection.
        let read_port_r1 = SingleSourcePage {
            source: source_r1.clone(),
        };
        service
            .admit_sql_source_dirty_record(&record_r1, &read_port_r1, 2)
            .unwrap();
        service
            .transition_binding_operation(
                binding.generation,
                SemanticBindingState::Building,
                3,
                "semantic:index-maintainer",
                "open-stage-consumer",
                Nonce::from_bytes([91; 32]),
            )
            .unwrap();
        service
            .subscribe_stage_consumer("semantic-s1-worker")
            .unwrap();
        let mut budget = OutboxClaimBudget::new(1, 5_000, 4).unwrap();
        let claims = service
            .claim_stage_leases("semantic-s1-worker", &mut budget)
            .unwrap();
        assert_eq!(claims.claims.len(), 1);
        let lease = claims.claims[0].clone();
        let intent = service
            .validate_stage_lease(&lease, "semantic-s1-worker", 5)
            .unwrap();
        let transition = SemanticStageTransition {
            intent: intent.clone(),
            receipt: SemanticStageReceipt {
                intent_digest: intent.intent_digest,
                output_digest: source_r1.source_content_digest(),
                cursor: "source:old-complete".to_string(),
                completed_at: "2026-09-08T00:00:41Z".to_string(),
                outcome: SemanticStageOutcome::Completed,
            },
            generation_checkpoint: None,
        };
        let first_receipt = service
            .complete_sql_source_stage(
                &lease,
                &transition,
                &source_r1,
                "2026-09-08T00:00:40Z",
                None,
                6,
            )
            .unwrap();

        // The durable stage mutation retains the cursor and authorization
        // completion time. Reopen the service and replay with only the
        // durable lease envelope plus an intent decoded from its durable
        // payload; the caller does not retain or reconstruct the transition.
        let durable_lease = lease.clone();
        let durable_intent =
            SemanticStageIntent::from_canonical_cbor(&durable_lease.record.intent.payload).unwrap();
        drop(transition);
        drop(intent);
        drop(service);
        let service = SemanticIndexService::open(
            &dir,
            Arc::new(SemanticTenantScopeVerifier {
                tenant: "tenant-a",
            }),
            TEST_PRINCIPAL,
            TEST_PROOF,
            "tenant-a",
            "binding:articles-body",
        )
        .unwrap();
        let (replayed_transition, replayed_receipt) = service
            .replay_completed_sql_source_stage(&durable_lease, &durable_intent, 7)
            .unwrap()
            .expect("the reopened service must recover the retained S1 result");
        assert_eq!(replayed_transition.intent, durable_intent);
        assert_eq!(replayed_transition.receipt.cursor, "source:old-complete");
        assert_eq!(
            replayed_transition.receipt.completed_at,
            "2026-09-08T00:00:41Z"
        );
        assert!(replayed_receipt.replayed);
        assert_eq!(
            replayed_receipt.mutation_digest,
            first_receipt.mutation_digest
        );

        let prior_manifest = service
            .sql_source_manifest(binding.generation, &source_r1.source_entity_id())
            .unwrap()
            .unwrap();
        assert_eq!(
            prior_manifest.source_entity_id,
            source_r1.source_entity_id()
        );
        assert_eq!(prior_manifest.source_identity, source_r1.source_identity);
        assert_eq!(prior_manifest.source_revision, revision_r1);
        assert_eq!(
            prior_manifest.source_content_digest,
            source_r1.source_content_digest()
        );

        let read_port = RecordingEmptyPage {
            revision: revision_r2.clone(),
            calls: Arc::new(Mutex::new(Vec::new())),
        };
        let admission = service
            .admit_sql_source_dirty_reconcile(&record_r2, &read_port, 43)
            .unwrap();
        assert!(admission.complete);
        assert!(admission.wakeup_consumed);
        assert_eq!(admission.source_revision, revision_r2);
        assert_eq!(admission.receipts.len(), 1);
        assert!(service
            .store
            .source_entity_seen_at_revision(
                binding.generation,
                &source_r1.source_entity_id(),
                &revision_r2,
            )
            .unwrap());
        assert!(service
            .store
            .read_source_reconciliation_checkpoint(binding.generation)
            .unwrap()
            .is_none());
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn newer_authoritative_version_and_tombstone_keep_the_same_entity() {
        let record = dirty_record(41, 1, 2);
        let revision = record_revision(&record);
        let binding = binding(&revision);
        let newer_revision = sql_source_revision_for_epoch(digest(170), 3).unwrap();
        let tombstone = source_record(
            &binding,
            11,
            &newer_revision,
            SemanticSqlSourceValue::Tombstone {
                deletion_proof_digest: digest(99),
            },
        );
        let scope_digest = SemanticDigest::from_bytes(*record.identity.binding_digest().as_bytes());
        assert!(tombstone.is_tombstone());
        let intent =
            coalesce_sql_source_record_to_s1(&binding, scope_digest, &record, &tombstone).unwrap();
        let entity_id = tombstone.source_entity_id();
        assert_eq!(intent.scope.source_entity_id(), Some(entity_id.as_str()));
        assert_eq!(intent.source_revision, tombstone.source_revision);
        assert_eq!(intent.input_digest, digest(99));
    }

    #[test]
    fn newer_foreign_authority_is_rejected_before_counter_ordering() {
        let record = dirty_record(45, 1, 2);
        let revision = record_revision(&record);
        let binding = binding(&revision);
        let foreign_revision = sql_source_revision_for_epoch(digest(171), 9).unwrap();
        let source = source_record(
            &binding,
            11,
            &foreign_revision,
            SemanticSqlSourceValue::Present {
                source_bytes: b"foreign authority".to_vec(),
            },
        );
        let scope_digest = SemanticDigest::from_bytes(*record.identity.binding_digest().as_bytes());
        assert!(
            coalesce_sql_source_record_to_s1(&binding, scope_digest, &record, &source).is_err()
        );
    }

    #[test]
    fn caller_can_route_distinct_source_entities_from_one_authoritative_page() {
        let record = dirty_record(51, 1, 2);
        let revision = record_revision(&record);
        let binding = binding(&revision);
        let expected = source_record(
            &binding,
            11,
            &revision,
            SemanticSqlSourceValue::Present {
                source_bytes: b"expected article body".to_vec(),
            },
        );
        let wrong_identity = source_record(
            &binding_for("tenant-b", "binding:articles-body", &revision),
            12,
            &revision,
            SemanticSqlSourceValue::Present {
                source_bytes: b"other article body".to_vec(),
            },
        );
        let scope_digest = SemanticDigest::from_bytes(*record.identity.binding_digest().as_bytes());
        assert!(
            coalesce_sql_source_record_to_s1(&binding, scope_digest, &record, &wrong_identity)
                .is_err()
        );
        assert!(
            coalesce_sql_source_record_to_s1(&binding, scope_digest, &record, &expected).is_ok()
        );
    }

    #[test]
    fn partial_pages_cannot_turn_missing_rows_into_tombstones() {
        let binding = binding(&sql_source_revision_for_epoch(digest(170), 1).unwrap());
        let tombstone = source_record(
            &binding,
            11,
            &binding.source_revision,
            SemanticSqlSourceValue::Tombstone {
                deletion_proof_digest: digest(88),
            },
        );
        let partial = super::SemanticSqlSourceReadPage {
            source_revision: binding.source_revision.clone(),
            complete_snapshot_receipt_digest: None,
            sources: vec![tombstone],
            next_cursor: Some(b"next".to_vec()),
            complete: false,
        };
        assert!(partial.validate().is_err());
    }

    #[test]
    fn complete_empty_page_requires_a_snapshot_receipt_and_deletion_proof_is_scoped() {
        let revision = sql_source_revision_for_epoch(digest(170), 7).unwrap();
        let missing_receipt = super::SemanticSqlSourceReadPage {
            source_revision: revision.clone(),
            complete_snapshot_receipt_digest: None,
            sources: Vec::new(),
            next_cursor: None,
            complete: true,
        };
        assert!(missing_receipt.validate().is_err());
        let complete = super::SemanticSqlSourceReadPage {
            source_revision: revision.clone(),
            complete_snapshot_receipt_digest: Some(digest(77)),
            sources: Vec::new(),
            next_cursor: None,
            complete: true,
        };
        assert!(complete.validate().is_ok());
        assert_ne!(
            sql_source_deletion_proof(
                "semantic-sql-source:sha256:1111111111111111111111111111111111111111111111111111111111111111",
                &revision,
                digest(77),
            ),
            sql_source_deletion_proof(
                "semantic-sql-source:sha256:2222222222222222222222222222222222222222222222222222222222222222",
                &revision,
                digest(77),
            )
        );
    }

    #[test]
    fn empty_source_pages_still_fence_authority_and_epoch() {
        let binding_revision = sql_source_revision_for_epoch(digest(170), 7).unwrap();
        let binding = binding(&binding_revision);
        let empty = SemanticSqlSourceReadPage {
            source_revision: sql_source_revision_for_epoch(digest(171), 99).unwrap(),
            complete_snapshot_receipt_digest: Some(digest(77)),
            sources: Vec::new(),
            next_cursor: None,
            complete: true,
        };
        assert!(validate_page_revision_against_binding(&binding, &empty.source_revision).is_err());

        let stale = SemanticSqlSourceReadPage {
            source_revision: sql_source_revision_for_epoch(digest(170), 6).unwrap(),
            ..empty.clone()
        };
        assert!(validate_page_revision_against_binding(&binding, &stale.source_revision).is_err());
        let current = SemanticSqlSourceReadPage {
            source_revision: binding_revision,
            ..empty
        };
        assert!(validate_page_revision_against_binding(&binding, &current.source_revision).is_ok());
    }

    #[test]
    fn source_page_byte_cap_rejects_an_oversized_present_value() {
        let revision = sql_source_revision_for_epoch(digest(170), 8).unwrap();
        let binding = binding(&revision);
        let source = source_record(
            &binding,
            11,
            &revision,
            SemanticSqlSourceValue::Present {
                source_bytes: vec![b'x'; super::SemanticSqlSourceReadPage::MAX_SOURCE_BYTES + 1],
            },
        );
        let page = super::SemanticSqlSourceReadPage {
            source_revision: revision,
            complete_snapshot_receipt_digest: Some(digest(77)),
            sources: vec![source],
            next_cursor: None,
            complete: true,
        };
        assert!(page.validate().is_err());
    }
}
