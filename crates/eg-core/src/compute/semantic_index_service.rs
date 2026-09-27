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
use super::semantic_ann_codes::reconciliation::{
    SemanticSourceReconciliationCheckpoint, SemanticSourceReconciliationPhase,
};
use super::semantic_ann_codes::{
    semantic_contract_error, ExistingTenantOwnerOpen, OperationAttribution, SemanticCodeError,
    SemanticCodeStore, SemanticMutationReceipt,
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

/// The page just read from the authoritative SQL source, together with the
/// cursor it was requested with.
///
/// A page is only interpretable against the cursor that produced it: the
/// non-advance check compares the page's own `next_cursor` against this
/// `cursor`, so a page paired with the wrong request cursor would pass that
/// check and loop forever. They are never useful apart.
struct ScannedPage<'a> {
    page: &'a SemanticSqlSourceReadPage,
    cursor: Option<&'a [u8]>,
}

/// The part of a source-reconciliation checkpoint that carries forward from one
/// bounded turn to the next.
///
/// These five are exactly the fields the next
/// `SemanticSourceReconciliationCheckpoint` inherits unchanged: which wakeup
/// digest and source revision the scan is reconciling, and how much of it has
/// been consumed. The remaining checkpoint fields -- `phase`, the two cursors,
/// the complete-read proof -- are derived per turn, which is why they are
/// deliberately NOT here: a struct holding them too would invite a caller to
/// supply a phase the turn has not reached.
struct SourceScanProgress<'a> {
    source_wakeup_digest: SemanticDigest,
    source_revision: &'a str,
    rows_seen: u64,
    source_bytes_seen: u64,
    pages_seen: u64,
}

/// The authenticated source wakeup one reconciliation turn runs under.
///
/// `binding` is the durable semantic binding, `scope_digest` is the binding
/// digest the wakeup's mutation identity carried, and `record` is the committed
/// outbox record that authorizes the turn. `validate_source_dirty_record`
/// checks the three against each other ONCE, and every admission below it then
/// needs all three; passing them separately let a checked triple be re-split
/// and recombined with a different record.
struct AdmittedSourceWakeup<'a> {
    binding: &'a SemanticBinding,
    scope_digest: SemanticDigest,
    record: &'a MutationOutboxRecord,
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

    /// Resolve a scope already present in a migrated tenant owner. This is a
    /// lower-layer composition prerequisite, not a request route: the server
    /// keeps v3 disabled until a trusted transition receipt can authorize it.
    /// In particular this constructor cannot create a missing binding scope.
    pub fn open_tenant(
        path: &Path,
        input: ExistingTenantOwnerOpen<'_>,
    ) -> Result<Self, SemanticCodeError> {
        let store = SemanticCodeStore::open_tenant(path, input)?;
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
        attribution: OperationAttribution<'_>,
    ) -> Result<SemanticMutationReceipt, SemanticCodeError> {
        let OperationAttribution {
            actor,
            idempotency_key,
            nonce,
        } = attribution;
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
            OperationAttribution {
                actor,
                idempotency_key,
                nonce,
            },
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

mod source_contract;
use source_contract::*;
pub use source_contract::{
    coalesce_sql_source_record_to_s1, sql_source_deletion_proof, sql_source_stage_artifact,
};

mod operator;
mod source_scan;
mod source_turn;
mod source_types;
mod stage_lifecycle;
#[cfg(test)]
mod test_fixture;
#[cfg(test)]
mod tests;
