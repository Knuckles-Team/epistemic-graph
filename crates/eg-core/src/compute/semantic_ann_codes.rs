//! Durable ANN code tier for the semantic index (feature `ann-redb`).
//!
//! RF-RULING-007. One index generation becomes durable as **one admitted
//! `Native(SemanticIndex)` mutation**: the generation's buffers go into the
//! `eg_ann` owner table of [`eg_storage::OwnerLayout::SemanticIndex`], keyed
//! `(tenant, binding, generation, part)`, and the binding's live-generation
//! pointer is flipped in the SAME transaction, so a generation never becomes
//! live without its codes and never carries codes it cannot serve from.
//! Retiring a generation is `purge_scope_with`, so its ledger authority and its
//! payload retire together or not at all.
//!
//! **One scope, bound once.** The serving scope is bound at `open` and is this
//! module's ONLY scope: every read is a [`eg_storage::ScopedRead`] on it and
//! every write is admitted against it through `commit_metadata_fenced`. So
//! probing an unactivated generation creates no authority, and a read after
//! retirement cannot resurrect one, because no read or write path can mint a
//! scope at all.
//!
//! **Deliberate non-goal: a per-generation write scope.** This module once also
//! bound a SEPARATE authenticated `MutationScopeIdentity` per generation
//! (`{binding}:generation:{n}`), cached per generation, so a generation being
//! BUILT held a different ledger scope from the one being SERVED, and retiring
//! it was `purge_scope_with` against that scope plus an
//! `OwnerPayloadRetirement` sweep of its rows. That is not how a generation is
//! isolated or retired here, and reintroducing it would put two authorities on
//! one fact:
//!
//!   * **Isolation** of one generation's payload is a ROW-KEY property, and
//!     [`rows::BoundCodeRows`] holds it: an `eg_ann` write is bound to exactly
//!     one `(tenant, binding, generation)` and refuses every other key. The
//!     generation number is the only part of that key that comes from request
//!     data, so bounding it is the whole of the property. A second ledger scope
//!     per generation cost two extra committed transactions and bought nothing
//!     the row bound does not already give.
//!   * **Retirement** is the admitted binding lifecycle: `semantic_tombstones`
//!     keyed `(tenant, binding, generation)`, the `SemanticBindingState`
//!     machine, and the active-pointer removal in
//!     `transition_binding_operation` / `drop_binding_operation` — all in ONE
//!     caller-attributed mutation on the serving scope. `activate` and `retire`
//!     are therefore closed (`Refused`) rather than plumbed.
//!
//! `generation_identity` and its per-generation scope binding were removed for
//! that reason and not for want of a caller.
//!
//! Two properties this replaces, both defects rather than plumbing:
//!   * `eg_ann::redb_store` opened its own `redb::Database` — a leaf crate as a
//!     second physical authority, which RF-RULING-004 forbids.
//!   * it wrote the fixed keys `meta`/`codes`/`refine`, so building generation
//!     `N+1` overwrote the generation `N` that was still serving. The
//!     generation key component is what makes the two coexist.
//!
//! This module holds no physical authority of its own: the [`StorageKernel`]
//! it owns is the sole opener of the file, and every write is admitted, ordered
//! and committed by [`MutationKernel`].

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use eg_storage::{
    OwnedStoreHandle, PhysicalStoreIdentity, RecordedOperation, ScopeGrantVerifier, ScopedRead,
    SemanticIndexOwner, StorageKernel, ANN_CODES, SEMANTIC_ANN, SEMANTIC_AUTH_RECEIPTS,
    SEMANTIC_BINDINGS, SEMANTIC_CHECKPOINTS, SEMANTIC_CHECKPOINT_HEADS, SEMANTIC_DEAD_LETTERS,
    SEMANTIC_GRAPH_PROJECTIONS, SEMANTIC_HEADS, SEMANTIC_LEXICAL, SEMANTIC_POINTERS,
    SEMANTIC_SOURCE_PROGRESS, SEMANTIC_SQL_SOURCES, SEMANTIC_STAGES, SEMANTIC_STATES,
    SEMANTIC_TOMBSTONES, SEMANTIC_VECTORS,
};
use eg_transaction::{
    AdmittedMutation, AdmittedOwnerWrite, Begin, MutationKernel, OutboxClaimBudget,
    OutboxClaimOutcome,
};
use eg_types::contract::{Digest256, Nonce};
use eg_types::mutation_batch::{
    BatchContent, CompiledOperation, CompiledScope, DurabilityDomain, MutationEnvelope,
    MutationOutboxIntent, MutationOutboxLease, MutationSurface, VersionExpectation,
};
use eg_types::semantic_index::{
    SemanticActivePointer, SemanticAnnIndexManifest, SemanticBinding, SemanticBindingState,
    SemanticBindingStateTransition, SemanticDigest, SemanticExpectedEntity,
    SemanticGenerationAggregate, SemanticGenerationArtifact, SemanticGenerationCheckpoint,
    SemanticGenerationCheckpointDraft, SemanticGenerationCheckpointUpdate,
    SemanticGenerationDependency, SemanticGenerationMember, SemanticIndexError,
    SemanticIndexFilter, SemanticIndexMutation, SemanticLexicalIndexManifest,
    SemanticSourceProgress, SemanticSqlSourceManifest, SemanticStage, SemanticStageArtifact,
    SemanticStageIntent, SemanticStageOutcome, SemanticStagePredecessor, SemanticStageTransition,
    SemanticTombstone, SemanticTombstoneDraft,
};
use eg_types::{MutationBatch, MutationOperation, MUTATION_BATCH_VERSION};
use redb::ReadableTable;
use sha2::{Digest, Sha256};

use crate::compute::semantic::SemanticGenerationImage;

#[path = "semantic_ann_codes/rows.rs"]
mod rows;

use rows::{read_part, BoundCodeRows, DIGEST_PART, PARTS};

/// Operator-facing identity of the one physical semantic-index owner file.
const SEMANTIC_PHYSICAL_STORE_PREFIX: &str = "eg-core:semantic-index:v2";

/// Errors from the durable ANN code tier.
#[derive(Debug)]
pub enum SemanticCodeError {
    /// Creating the persist dir failed.
    Io(std::io::Error),
    /// A kernel admission, capability, table or commit operation failed.
    Kernel(String),
    /// A stored generation is absent, truncated, or does not describe itself.
    Corrupt(String),
    /// A write was refused by this owner's own row-key or identity ACL.
    Refused(String),
}

impl std::fmt::Display for SemanticCodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(f, "semantic code store io error: {error}"),
            Self::Kernel(error) => write!(f, "semantic code store kernel error: {error}"),
            Self::Corrupt(error) => write!(f, "semantic code store content error: {error}"),
            Self::Refused(error) => write!(f, "semantic code store refused: {error}"),
        }
    }
}

impl std::error::Error for SemanticCodeError {}

impl From<std::io::Error> for SemanticCodeError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

pub(crate) fn kernel_error(error: impl std::fmt::Display) -> SemanticCodeError {
    SemanticCodeError::Kernel(error.to_string())
}

/// The only outbox event that represents a stage claimable by the semantic
/// consumer. Its payload is canonical `semantic-stage-intent/v1`; the key is
/// the canonical digest of the complete intent identity.
pub const SEMANTIC_STAGE_INTENT_TOPIC: &str = "engine.semantic-index.stage-intent.v1";
pub const SEMANTIC_STAGE_RECEIPT_TOPIC: &str = "engine.semantic-index.stage-receipt.v1";
pub const SEMANTIC_BINDING_STATE_TOPIC: &str = "engine.semantic-index.binding-state.v1";
pub const SEMANTIC_BINDING_DROPPED_TOPIC: &str = "engine.semantic-index.binding-dropped.v1";
/// SQL commits emit a source fact first. EG resolves the matching binding and
/// expands it into [`SEMANTIC_STAGE_INTENT_TOPIC`].
pub const SEMANTIC_SOURCE_DIRTY_TOPIC: &str = eg_types::semantic_index::SEMANTIC_SOURCE_DIRTY_TOPIC;
pub const SEMANTIC_BINDING_CREATED_TOPIC: &str = "engine.semantic-index.binding-created.v1";

/// One durable continuation for a source reconciliation generation.  The
/// continuation lives in the existing source-progress owner table under this
/// reserved entity key; it is deliberately not a second registry/table
/// authority.  Source entity ids are `semantic-sql-source:<digest>`, so this
/// key cannot collide with a source row accepted by the read port.
const SEMANTIC_RECONCILIATION_CHECKPOINT_ENTITY: &str =
    "__semantic_source_reconciliation_checkpoint_v1__";
const SEMANTIC_RECONCILIATION_CHECKPOINT_MAGIC: &[u8] = b"semantic-source-reconciliation/v1\0";
const SEMANTIC_RECONCILIATION_MAX_CURSOR_BYTES: usize = 4096;
const SEMANTIC_RECONCILIATION_MAX_REVISION_BYTES: usize = 256;
const SEMANTIC_RECONCILIATION_MAX_ENTITY_BYTES: usize = 512;

/// The durable phase of a source reconciliation cursor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SemanticSourceReconciliationPhase {
    /// The authoritative SQL source is still being paged.  `source_cursor`
    /// is required and no tombstone proof may be used yet.
    Scanning,
    /// The complete source snapshot has been admitted and the prior durable
    /// source identity set is being paged for deletion tombstones.
    FinalizingTombstones,
}

/// A bounded, CAS-protected native continuation for SQL source reconciliation.
/// The source revision and complete snapshot receipt are persisted alongside
/// the cursors, so a crash resumes from the exact authority/epoch proof rather
/// than trusting a caller's in-memory page position.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SemanticSourceReconciliationCheckpoint {
    pub(crate) source_wakeup_digest: SemanticDigest,
    pub(crate) source_revision: String,
    pub(crate) phase: SemanticSourceReconciliationPhase,
    pub(crate) source_cursor: Option<Vec<u8>>,
    pub(crate) prior_cursor: Option<String>,
    pub(crate) rows_seen: u64,
    pub(crate) source_bytes_seen: u64,
    pub(crate) pages_seen: u64,
    pub(crate) complete_snapshot_receipt_digest: Option<SemanticDigest>,
}

/// Serializable: this is what `Method::SemanticIndex` returns to a
/// connector for every committed semantic mutation.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SemanticMutationReceipt {
    pub batch_id: String,
    pub mutation_digest: SemanticDigest,
    pub source_version: u64,
    pub target_version: u64,
    pub replayed: bool,
}

pub(super) fn semantic_contract_error(error: SemanticIndexError) -> SemanticCodeError {
    SemanticCodeError::Refused(format!("semantic contract rejected record: {error:?}"))
}

fn semantic_digest(bytes: &[u8]) -> SemanticDigest {
    let mut hasher = Sha256::new();
    hasher.update(b"eg/semantic-index-mutation/v1\0");
    hasher.update((bytes.len() as u64).to_be_bytes());
    hasher.update(bytes);
    SemanticDigest::from_bytes(hasher.finalize().into())
}

fn mutation_digest_from_batch(batch: &MutationBatch) -> Result<SemanticDigest, String> {
    batch
        .operations
        .first()
        .and_then(|operation| match &operation.method {
            eg_types::protocol::Method::ApplyMutation { query, .. } => {
                query.strip_prefix("sha256:")
            }
            _ => None,
        })
        .and_then(|hex_digest| hex::decode(hex_digest).ok())
        .and_then(|bytes| <[u8; 32]>::try_from(bytes).ok())
        .map(SemanticDigest::from_bytes)
        .ok_or_else(|| "semantic replay has no durable mutation digest".to_string())
}

/// The read-only serving scope of one binding. Bound once at `open` so that
/// every later read is a pure snapshot; it owns no generation and is never
/// purged with one.
fn serving_identity(
    tenant: &str,
    binding: &str,
) -> Result<eg_types::MutationScopeIdentity, SemanticCodeError> {
    scope_identity(
        tenant,
        &format!("{binding}:serving"),
        "semantic-ann-serving:v1",
    )
}

fn scope_identity(
    tenant: &str,
    resource: &str,
    incarnation: &str,
) -> Result<eg_types::MutationScopeIdentity, SemanticCodeError> {
    eg_types::MutationScopeIdentity::fixed_native(
        tenant,
        eg_types::mutation_batch::DurabilityDomain::SemanticIndex,
        resource,
        incarnation,
    )
    .map_err(SemanticCodeError::Kernel)
}

/// What one semantic metadata mutation records.
///
/// The four are used together and only together: `batch_id`, `event_type` and
/// `subject` form the ledger envelope, and `mutation_digest` is the content the
/// recorded operation is addressed by (`sha256:{digest}`). Every caller that
/// has one has all four. Flattened, it put three bare `&str` adjacent in an
/// argument list of up to twelve, where transposing two of them still compiles
/// and the ledger silently records the wrong subject.
pub(crate) struct MetadataMutation<'a> {
    pub(crate) batch_id: &'a str,
    pub(crate) event_type: &'a str,
    pub(crate) subject: &'a str,
    pub(crate) mutation_digest: SemanticDigest,
}

/// Who asked for a caller-attributed (operation-surface) mutation, and under
/// what replay identity.
///
/// RF-RULING-005 requires every `Operation` mutation to carry all three: the
/// verified actor, the idempotency key that makes a retry REPLAY rather than
/// reapply, and the nonce the kernel's replay seal consumes. That is why the
/// triple recurs across eleven eg-core signatures. Two of the three are bare
/// `&str`, so naming the group is also what stops an actor being passed where
/// an idempotency key belongs.
#[derive(Clone, Copy)]
pub struct OperationAttribution<'a> {
    pub actor: &'a str,
    pub idempotency_key: &'a str,
    pub nonce: Nonce,
}

/// Durable, kernel-backed ANN code tier for one `(tenant, binding)` semantic
/// index, holding any number of generations of which exactly one is live.
pub struct SemanticCodeStore {
    kernel: StorageKernel,
    mutations: MutationKernel,
    tenant: String,
    binding: String,
    /// The read-only serving scope, bound once at `open` — and the only scope
    /// this store ever holds. See the module's per-generation-scope non-goal.
    serving: OwnedStoreHandle<SemanticIndexOwner>,
}

impl std::fmt::Debug for SemanticCodeStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SemanticCodeStore")
            .field("tenant", &self.tenant)
            .field("binding", &self.binding)
            .finish_non_exhaustive()
    }
}

impl SemanticCodeStore {
    /// Open (or create) this binding's owner file through the storage kernel
    /// under [`eg_storage::OwnerLayout::SemanticIndex`].
    ///
    /// The file name is derived from `(tenant, binding)`, because one physical
    /// file serves one binding: two bindings opened against the same directory
    /// would otherwise collide on redb's file lock.
    ///
    /// `verifier` is the composition root's proof authority: only it may decide
    /// that `principal` is entitled to serve this binding's scope. It is used
    /// here and not retained: the serving scope is the only scope this store
    /// binds, so there is no later authentication to hold it for.
    pub fn open(
        dir: &Path,
        verifier: Arc<dyn ScopeGrantVerifier>,
        principal: &str,
        proof: &[u8],
        tenant: &str,
        binding: &str,
    ) -> Result<Self, SemanticCodeError> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join(store_file_name(tenant, binding));
        // Each binding owns one physical authority.  A constant identity let a
        // copied file under another `(tenant,binding)` pathname pass the
        // storage manifest check before the serving rows were even read.
        // Include the framed, digest-derived file identity rather than joining
        // tenant and binding with a delimiter. Otherwise `(tenant:a,binding)`
        // and `(tenant,a:binding)` would install the same manifest identity,
        // allowing a copied owner file to pass the physical transplant fence.
        let physical_name = format!(
            "{SEMANTIC_PHYSICAL_STORE_PREFIX}:{}",
            store_file_name(tenant, binding)
        );
        let physical = PhysicalStoreIdentity::new(&physical_name).map_err(kernel_error)?;
        let kernel = if path.exists() {
            StorageKernel::open_owner::<SemanticIndexOwner>(&path, physical, None)
        } else {
            StorageKernel::create_owner::<SemanticIndexOwner>(&path, physical, None)
        }
        .map_err(kernel_error)?;
        let (kernel, authority) = kernel
            .into_read_and_mutation_authority()
            .map_err(kernel_error)?;
        let mutations = MutationKernel::new(authority);
        let serving = bind_scope(
            &kernel,
            &mutations,
            verifier.as_ref(),
            principal,
            proof,
            serving_identity(tenant, binding)?,
        )?;
        Ok(Self {
            kernel,
            mutations,
            tenant: tenant.to_string(),
            binding: binding.to_string(),
            serving,
        })
    }

    /// Read the binding authority selected by the serving head.  The head and
    /// binding rows are read from one kernel snapshot, so a caller cannot
    /// observe a generation from one binding paired with bytes from another.
    pub fn read_binding(&self) -> Result<Option<SemanticBinding>, SemanticCodeError> {
        let read = self.serving_read()?;
        self.read_binding_in(&read)
    }

    /// Apply the closed list filter against the authenticated owner snapshot.
    /// The service currently owns one binding file, so this is a bounded
    /// catalog lookup over at most the filter's validated entity set. A filter
    /// must never be silently ignored: source visibility and revision are
    /// proven from durable source-progress rows before the binding is returned.
    pub(crate) fn binding_matches_filter(
        &self,
        filter: &SemanticIndexFilter,
    ) -> Result<bool, SemanticCodeError> {
        filter.validate().map_err(semantic_contract_error)?;
        let Some(binding) = self.read_binding()? else {
            return Ok(false);
        };
        if filter.source_entity_ids.is_empty() {
            return Ok(filter
                .required_source_revision
                .as_deref()
                .is_none_or(|revision| revision == binding.source_revision.as_str()));
        }
        let read = self.serving_read()?;
        let table = read
            .open_owner_table(SEMANTIC_SOURCE_PROGRESS)
            .map_err(kernel_error)?;
        for source_entity_id in &filter.source_entity_ids {
            let Some(raw) = table
                .get((
                    self.tenant.as_str(),
                    self.binding.as_str(),
                    binding.generation,
                    source_entity_id.as_str(),
                ))
                .map_err(kernel_error)?
                .map(|value| value.value().to_vec())
            else {
                return Ok(false);
            };
            let progress = SemanticSourceProgress::from_canonical_cbor(&raw)
                .map_err(semantic_contract_error)?;
            if progress.binding_id != binding.binding_id
                || progress.binding_digest != binding.binding_digest
                || progress.generation != binding.generation
                || filter
                    .required_source_revision
                    .as_deref()
                    .is_some_and(|revision| progress.source_revision.as_str() != revision)
            {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Read the one native reconciliation continuation for the current
    /// binding generation.  The generation/head check is part of the same
    /// serving snapshot as the checkpoint row, so a worker that resumes after
    /// a refresh cannot accidentally continue an obsolete source stream.
    pub(crate) fn read_source_reconciliation_checkpoint(
        &self,
        generation: u64,
    ) -> Result<Option<SemanticSourceReconciliationCheckpoint>, SemanticCodeError> {
        if generation == 0 {
            return Err(SemanticCodeError::Refused(
                "source reconciliation generation must be nonzero".to_string(),
            ));
        }
        let read = self.serving_read()?;
        let current = self.read_binding_in(&read)?.ok_or_else(|| {
            SemanticCodeError::Refused(
                "source reconciliation requires a durable binding head".to_string(),
            )
        })?;
        if current.generation != generation {
            return Err(SemanticCodeError::Refused(
                "source reconciliation checkpoint is for a stale binding generation".to_string(),
            ));
        }
        let raw = read
            .open_owner_table(SEMANTIC_SOURCE_PROGRESS)
            .map_err(kernel_error)?
            .get((
                self.tenant.as_str(),
                self.binding.as_str(),
                generation,
                SEMANTIC_RECONCILIATION_CHECKPOINT_ENTITY,
            ))
            .map_err(kernel_error)?
            .map(|value| value.value().to_vec());
        raw.map(|bytes| decode_reconciliation_checkpoint(&bytes))
            .transpose()
    }

    /// Write a reconciliation continuation using an admitted owner mutation.
    /// The `expected` row is compared inside that write, which makes a stale
    /// worker fail before changing source progress or publishing another
    /// continuation.  Repeating an already committed `next` is idempotent.
    pub(crate) fn write_source_reconciliation_checkpoint(
        &self,
        generation: u64,
        expected: Option<&SemanticSourceReconciliationCheckpoint>,
        next: &SemanticSourceReconciliationCheckpoint,
    ) -> Result<(), SemanticCodeError> {
        if generation == 0 {
            return Err(SemanticCodeError::Refused(
                "source reconciliation generation must be nonzero".to_string(),
            ));
        }
        let next_bytes = encode_reconciliation_checkpoint(next)?;
        let expected_bytes = expected.map(encode_reconciliation_checkpoint).transpose()?;
        let digest = semantic_digest(&next_bytes);
        let batch_id = format!(
            "semantic-index:reconciliation-checkpoint:{}",
            semantic_digest(
                &[
                    b"write\0".as_slice(),
                    self.tenant.as_bytes(),
                    b"\0",
                    self.binding.as_bytes(),
                    b"\0",
                    &generation.to_be_bytes(),
                    b"\0",
                    next_bytes.as_slice(),
                ]
                .concat()
            )
        );
        let owner = &self.serving;
        self.commit_metadata(
            |version| {
                self.metadata_batch(
                    owner,
                    version,
                    MetadataMutation {
                        batch_id: &batch_id,
                        event_type: "semantic_source_reconciliation_checkpoint",
                        subject: &format!("{}:{generation}", self.binding),
                        mutation_digest: digest,
                    },
                    Vec::new(),
                    0,
                )
            },
            digest,
            0,
            |write, rows| {
                let current_binding = self.read_binding_in_write(write)?.ok_or_else(|| {
                    SemanticCodeError::Refused(
                        "source reconciliation requires a durable binding head".to_string(),
                    )
                })?;
                if current_binding.generation != generation {
                    return Err(SemanticCodeError::Refused(
                        "source reconciliation checkpoint targets a stale generation".to_string(),
                    ));
                }
                let current_raw = write
                    .open_read_table(SEMANTIC_SOURCE_PROGRESS)
                    .map_err(kernel_error)?
                    .get((
                        self.tenant.as_str(),
                        self.binding.as_str(),
                        generation,
                        SEMANTIC_RECONCILIATION_CHECKPOINT_ENTITY,
                    ))
                    .map_err(kernel_error)?
                    .map(|value| value.value().to_vec());
                let current = current_raw
                    .as_deref()
                    .map(decode_reconciliation_checkpoint)
                    .transpose()?;
                let is_next = current_raw.as_deref() == Some(next_bytes.as_slice());
                let matches_expected = match (expected_bytes.as_deref(), current_raw.as_deref()) {
                    (None, None) => true,
                    (Some(expected), Some(actual)) => actual == expected,
                    _ => false,
                };
                if !matches_expected && !is_next {
                    return Err(SemanticCodeError::Refused(
                        "source reconciliation checkpoint CAS predecessor is stale".to_string(),
                    ));
                }
                if current.is_some() && is_next {
                    return Ok(());
                }
                let mut table = rows
                    .open_table(SEMANTIC_SOURCE_PROGRESS)
                    .map_err(kernel_error)?;
                table
                    .insert(
                        (
                            self.tenant.as_str(),
                            self.binding.as_str(),
                            generation,
                            SEMANTIC_RECONCILIATION_CHECKPOINT_ENTITY,
                        ),
                        next_bytes.as_slice(),
                    )
                    .map_err(kernel_error)?;
                Ok(())
            },
        )
        .map(|_| ())
    }

    /// Remove a completed reconciliation continuation with an in-write CAS.
    /// A replay of the same clear batch is safe because the first commit has
    /// already consumed the exact expected row.
    pub(crate) fn clear_source_reconciliation_checkpoint(
        &self,
        generation: u64,
        expected: &SemanticSourceReconciliationCheckpoint,
    ) -> Result<(), SemanticCodeError> {
        if generation == 0 {
            return Err(SemanticCodeError::Refused(
                "source reconciliation generation must be nonzero".to_string(),
            ));
        }
        let expected_bytes = encode_reconciliation_checkpoint(expected)?;
        let digest = semantic_digest(&expected_bytes);
        let batch_id = format!(
            "semantic-index:reconciliation-clear:{}",
            semantic_digest(
                &[
                    b"clear\0".as_slice(),
                    self.tenant.as_bytes(),
                    b"\0",
                    self.binding.as_bytes(),
                    b"\0",
                    &generation.to_be_bytes(),
                    b"\0",
                    expected_bytes.as_slice(),
                ]
                .concat()
            )
        );
        let owner = &self.serving;
        self.commit_metadata(
            |version| {
                self.metadata_batch(
                    owner,
                    version,
                    MetadataMutation {
                        batch_id: &batch_id,
                        event_type: "semantic_source_reconciliation_checkpoint_clear",
                        subject: &format!("{}:{generation}", self.binding),
                        mutation_digest: digest,
                    },
                    Vec::new(),
                    0,
                )
            },
            digest,
            0,
            |write, rows| {
                let current_binding = self.read_binding_in_write(write)?.ok_or_else(|| {
                    SemanticCodeError::Refused(
                        "source reconciliation requires a durable binding head".to_string(),
                    )
                })?;
                if current_binding.generation != generation {
                    return Err(SemanticCodeError::Refused(
                        "source reconciliation clear targets a stale generation".to_string(),
                    ));
                }
                let current = write
                    .open_read_table(SEMANTIC_SOURCE_PROGRESS)
                    .map_err(kernel_error)?
                    .get((
                        self.tenant.as_str(),
                        self.binding.as_str(),
                        generation,
                        SEMANTIC_RECONCILIATION_CHECKPOINT_ENTITY,
                    ))
                    .map_err(kernel_error)?
                    .map(|value| value.value().to_vec());
                if current.as_deref() != Some(expected_bytes.as_slice()) {
                    return Err(SemanticCodeError::Refused(
                        "source reconciliation checkpoint clear predecessor is stale".to_string(),
                    ));
                }
                rows.open_table(SEMANTIC_SOURCE_PROGRESS)
                    .map_err(kernel_error)?
                    .remove((
                        self.tenant.as_str(),
                        self.binding.as_str(),
                        generation,
                        SEMANTIC_RECONCILIATION_CHECKPOINT_ENTITY,
                    ))
                    .map_err(kernel_error)?;
                Ok(())
            },
        )
        .map(|_| ())
    }

    /// Return a bounded lexicographic page of the generation's durable source
    /// identities.  The returned cursor is the last returned identity, so a
    /// retry after a crash is deterministic and cannot skip the row following
    /// the page boundary.
    pub(crate) fn list_source_entities_page(
        &self,
        generation: u64,
        after: Option<&str>,
        limit: usize,
    ) -> Result<(Vec<String>, Option<String>), SemanticCodeError> {
        if generation == 0 || limit == 0 || limit > 256 {
            return Err(SemanticCodeError::Refused(
                "source entity page has an invalid generation or bounded limit".to_string(),
            ));
        }
        if let Some(after) = after {
            if !valid_source_entity_id_for_reconciliation(after) {
                return Err(SemanticCodeError::Refused(
                    "source entity page cursor is not a canonical source identity".to_string(),
                ));
            }
        }
        let read = self.serving_read()?;
        let binding = self.read_binding_in(&read)?.ok_or_else(|| {
            SemanticCodeError::Refused(
                "source entity paging requires a durable binding head".to_string(),
            )
        })?;
        if binding.generation != generation {
            return Err(SemanticCodeError::Refused(
                "source entity page targets a stale generation".to_string(),
            ));
        }
        let table = read
            .open_owner_table(SEMANTIC_SOURCE_PROGRESS)
            .map_err(kernel_error)?;
        let mut entities = Vec::with_capacity(limit);
        let mut next_cursor = None;
        let range_start = after.unwrap_or("");
        let rows = table
            .range(
                (
                    self.tenant.as_str(),
                    self.binding.as_str(),
                    generation,
                    range_start,
                )..,
            )
            .map_err(kernel_error)?;
        for row in rows {
            let (key, value) = row.map_err(kernel_error)?;
            let key = key.value();
            if key.0 != self.tenant || key.1 != self.binding || key.2 != generation {
                break;
            }
            if key.3 == SEMANTIC_RECONCILIATION_CHECKPOINT_ENTITY
                || after.is_some_and(|cursor| key.3 <= cursor)
            {
                continue;
            }
            if !valid_source_entity_id_for_reconciliation(key.3)
                || key.3.len() > SEMANTIC_RECONCILIATION_MAX_ENTITY_BYTES
            {
                return Err(SemanticCodeError::Corrupt(
                    "source-progress page contains a non-canonical source identity".to_string(),
                ));
            }
            let progress = SemanticSourceProgress::from_canonical_cbor(value.value())
                .map_err(semantic_contract_error)?;
            progress.validate().map_err(semantic_contract_error)?;
            if progress.binding_id != self.binding
                || progress.binding_digest != binding.binding_digest
                || progress.generation != generation
                || progress.source_entity_id != key.3
            {
                return Err(SemanticCodeError::Corrupt(
                    "source-progress page row is outside its durable binding generation"
                        .to_string(),
                ));
            }
            if entities.len() < limit {
                entities.push(key.3.to_string());
            } else {
                next_cursor = entities.last().cloned();
                break;
            }
        }
        Ok((entities, next_cursor))
    }

    /// Test whether a source identity has a durable progress row in this
    /// generation.  This is an existence proof for tombstones; it does not
    /// expose arbitrary owner rows to the caller.
    pub(crate) fn source_entity_exists(
        &self,
        generation: u64,
        source_entity_id: &str,
    ) -> Result<bool, SemanticCodeError> {
        if generation == 0 || !valid_source_entity_id_for_reconciliation(source_entity_id) {
            return Err(SemanticCodeError::Refused(
                "source entity existence query has an invalid identity".to_string(),
            ));
        }
        let read = self.serving_read()?;
        let binding = self.read_binding_in(&read)?.ok_or_else(|| {
            SemanticCodeError::Refused(
                "source entity existence requires a durable binding head".to_string(),
            )
        })?;
        if binding.generation != generation {
            return Ok(false);
        }
        let raw = read
            .open_owner_table(SEMANTIC_SOURCE_PROGRESS)
            .map_err(kernel_error)?
            .get((
                self.tenant.as_str(),
                self.binding.as_str(),
                generation,
                source_entity_id,
            ))
            .map_err(kernel_error)?
            .map(|value| value.value().to_vec());
        let Some(raw) = raw else {
            return Ok(false);
        };
        let progress =
            SemanticSourceProgress::from_canonical_cbor(&raw).map_err(semantic_contract_error)?;
        progress.validate().map_err(semantic_contract_error)?;
        if progress.binding_id != self.binding
            || progress.binding_digest != binding.binding_digest
            || progress.generation != generation
            || progress.source_entity_id != source_entity_id
        {
            return Err(SemanticCodeError::Corrupt(
                "source-progress existence row is outside its durable binding generation"
                    .to_string(),
            ));
        }
        Ok(true)
    }

    /// Read the retained canonical SQL manifest for one historical source
    /// entity.  Reconciliation uses this after a source row has disappeared:
    /// the complete snapshot supplies the deletion proof, while this row is
    /// the only durable source identity that may be carried into the
    /// tombstone transition.  The lookup is bounded to one owner-table key;
    /// it never reconstructs a deleted row or scans unrelated history.
    pub(crate) fn sql_source_manifest(
        &self,
        generation: u64,
        source_entity_id: &str,
    ) -> Result<Option<SemanticSqlSourceManifest>, SemanticCodeError> {
        if generation == 0 || !valid_source_entity_id_for_reconciliation(source_entity_id) {
            return Err(SemanticCodeError::Refused(
                "SQL source manifest lookup has an invalid generation or source identity"
                    .to_string(),
            ));
        }
        let read = self.serving_read()?;
        let raw = read
            .open_owner_table(SEMANTIC_SQL_SOURCES)
            .map_err(kernel_error)?
            .get((
                self.tenant.as_str(),
                self.binding.as_str(),
                generation,
                source_entity_id,
            ))
            .map_err(kernel_error)?
            .map(|value| value.value().to_vec());
        let Some(raw) = raw else {
            return Ok(None);
        };
        let manifest = SemanticSqlSourceManifest::from_canonical_cbor(&raw)
            .map_err(semantic_contract_error)?;
        manifest.validate().map_err(semantic_contract_error)?;
        validate_sql_source_revision(&manifest.source_revision)?;
        if manifest.binding_id != self.binding
            || manifest.generation != generation
            || manifest.source_entity_id != source_entity_id
        {
            return Err(SemanticCodeError::Corrupt(
                "SQL source manifest row does not match its canonical key".to_string(),
            ));
        }
        let binding = self
            .read_binding_generation_in(&read, generation)?
            .ok_or_else(|| {
                SemanticCodeError::Corrupt(
                    "SQL source manifest names a missing binding generation".to_string(),
                )
            })?;
        manifest
            .source_identity
            .validate_against_binding(&binding)
            .map_err(semantic_contract_error)?;
        if manifest.binding_id != binding.binding_id
            || manifest.binding_digest != binding.binding_digest
            || manifest.generation != binding.generation
            || manifest.source_schema_digest != binding.source_schema_digest
            || manifest.source_field_set_digest != binding.source_field_set_digest
            || manifest.source_acl_revision
                != binding.policy_identity.components.source_acl_revision
            || manifest.source_acl_digest != binding.policy_identity.components.source_acl_digest
        {
            return Err(SemanticCodeError::Corrupt(
                "SQL source manifest is outside its durable binding authority".to_string(),
            ));
        }
        let manifest_revision =
            sql_source_revision_parts(&manifest.source_revision).ok_or_else(|| {
                SemanticCodeError::Corrupt(
                    "SQL source manifest has no canonical source authority".to_string(),
                )
            })?;
        let binding_revision =
            sql_source_revision_parts(&binding.source_revision).ok_or_else(|| {
                SemanticCodeError::Corrupt(
                    "durable binding has no canonical SQL source authority".to_string(),
                )
            })?;
        if manifest_revision.authority != binding_revision.authority {
            return Err(SemanticCodeError::Corrupt(
                "SQL source manifest authority differs from its durable binding authority"
                    .to_string(),
            ));
        }
        Ok(Some(manifest))
    }

    /// Return whether the current, non-superseded progress row was observed at
    /// the supplied source revision.  A row at an older revision remains
    /// useful for history but cannot satisfy a newer reconciliation tombstone.
    pub(crate) fn source_entity_seen_at_revision(
        &self,
        generation: u64,
        source_entity_id: &str,
        source_revision: &str,
    ) -> Result<bool, SemanticCodeError> {
        validate_sql_source_revision(source_revision)?;
        if generation == 0 || !valid_source_entity_id_for_reconciliation(source_entity_id) {
            return Err(SemanticCodeError::Refused(
                "source revision query has an invalid identity".to_string(),
            ));
        }
        let read = self.serving_read()?;
        let binding = self.read_binding_in(&read)?.ok_or_else(|| {
            SemanticCodeError::Refused(
                "source revision query requires a durable binding head".to_string(),
            )
        })?;
        if binding.generation != generation {
            return Ok(false);
        }
        let raw = read
            .open_owner_table(SEMANTIC_SOURCE_PROGRESS)
            .map_err(kernel_error)?
            .get((
                self.tenant.as_str(),
                self.binding.as_str(),
                generation,
                source_entity_id,
            ))
            .map_err(kernel_error)?
            .map(|value| value.value().to_vec());
        let Some(raw) = raw else {
            return Ok(false);
        };
        let progress =
            SemanticSourceProgress::from_canonical_cbor(&raw).map_err(semantic_contract_error)?;
        progress.validate().map_err(semantic_contract_error)?;
        if progress.binding_id != self.binding
            || progress.binding_digest != binding.binding_digest
            || progress.generation != generation
            || progress.source_entity_id != source_entity_id
        {
            return Err(SemanticCodeError::Corrupt(
                "source revision row is outside its durable binding generation".to_string(),
            ));
        }
        Ok(
            progress.source_revision == source_revision
                && progress.superseded_by_revision.is_none(),
        )
    }

    /// Admit the immutable binding definition and publish the first typed
    /// semantic outbox event.  This is the native admission half of S1: it
    /// persists the binding before any consumer can claim work, and it never
    /// performs projection, embedding, ANN, or activation work inline.
    pub(crate) fn store_binding(
        &self,
        binding: &SemanticBinding,
        now_ms: u64,
    ) -> Result<SemanticMutationReceipt, SemanticCodeError> {
        binding.validate().map_err(semantic_contract_error)?;
        if binding.tenant_id != self.tenant || binding.binding_id != self.binding {
            return Err(SemanticCodeError::Refused(
                "semantic binding is outside this store's authenticated owner".to_string(),
            ));
        }
        if binding.durable_state != eg_types::semantic_index::SemanticBindingState::Pending {
            return Err(SemanticCodeError::Refused(
                "semantic binding admission requires pending durable state".to_string(),
            ));
        }
        let payload = binding
            .to_canonical_cbor()
            .map_err(semantic_contract_error)?;
        let mutation = SemanticIndexMutation::StoreBinding {
            binding: Box::new(binding.clone()),
        };
        mutation.validate().map_err(semantic_contract_error)?;
        let mutation_bytes = mutation
            .to_canonical_cbor()
            .map_err(semantic_contract_error)?;
        let mutation_digest = semantic_digest(&mutation_bytes);
        let batch_id = format!("semantic-index:binding:{}", mutation_digest);
        let mut headers = BTreeMap::new();
        headers.insert(
            "schema".to_string(),
            eg_types::semantic_index::SEMANTIC_BINDING_SCHEMA.to_string(),
        );
        headers.insert("binding_id".to_string(), binding.binding_id.clone());
        headers.insert(
            "binding_digest".to_string(),
            binding.binding_digest.to_string(),
        );
        headers.insert("generation".to_string(), binding.generation.to_string());
        headers.insert(
            "source_revision".to_string(),
            binding.source_revision.clone(),
        );
        let outbox = MutationOutboxIntent {
            topic: SEMANTIC_BINDING_CREATED_TOPIC.to_string(),
            key: format!("{}:{}", binding.binding_id, binding.generation),
            payload: payload.clone(),
            headers,
        };
        let owner = &self.serving;
        let payload_digest = mutation_digest;
        let binding_bytes = payload;
        self.commit_metadata(
            |version| {
                self.metadata_batch(
                    owner,
                    version,
                    MetadataMutation {
                        batch_id: &batch_id,
                        event_type: "semantic_index_binding_stored",
                        subject: &format!("binding:{}", binding.binding_digest),
                        mutation_digest: payload_digest,
                    },
                    vec![outbox],
                    now_ms,
                )
            },
            mutation_digest,
            now_ms,
            |write, rows| {
                if let Some(existing) = self.read_binding_in_write(write)? {
                    if existing != *binding {
                        return Err(SemanticCodeError::Refused(
                            "semantic binding identity already names different bytes".to_string(),
                        ));
                    }
                }
                let heads = write
                    .open_read_table(SEMANTIC_HEADS)
                    .map_err(kernel_error)?;
                if let Some(head) = heads
                    .get((self.tenant.as_str(), self.binding.as_str()))
                    .map_err(kernel_error)?
                    .map(|value| value.value())
                {
                    if head != binding.generation {
                        return Err(SemanticCodeError::Refused(
                            "semantic binding head already names another generation".to_string(),
                        ));
                    }
                }
                drop(heads);
                rows.open_table(SEMANTIC_BINDINGS)
                    .map_err(kernel_error)?
                    .insert(
                        (
                            self.tenant.as_str(),
                            self.binding.as_str(),
                            binding.generation,
                        ),
                        binding_bytes.as_slice(),
                    )
                    .map_err(kernel_error)?;
                rows.open_table(SEMANTIC_HEADS)
                    .map_err(kernel_error)?
                    .insert(
                        (self.tenant.as_str(), self.binding.as_str()),
                        binding.generation,
                    )
                    .map_err(kernel_error)?;
                Ok(())
            },
        )
    }

    /// Caller-attributed binding admission. Unlike engine maintenance (used
    /// only by internal lifecycle producers), this path carries the verified
    /// actor, stable idempotency key and attempt nonce into the kernel's
    /// operation replay ledger.
    pub(crate) fn store_binding_operation(
        &self,
        binding: &SemanticBinding,
        now_ms: u64,
        actor: &str,
        idempotency_key: &str,
        nonce: Nonce,
    ) -> Result<SemanticMutationReceipt, SemanticCodeError> {
        binding.validate().map_err(semantic_contract_error)?;
        if binding.tenant_id != self.tenant || binding.binding_id != self.binding {
            return Err(SemanticCodeError::Refused(
                "semantic binding is outside this store's authenticated owner".to_string(),
            ));
        }
        if actor.trim().is_empty() || idempotency_key.trim().is_empty() {
            return Err(SemanticCodeError::Refused(
                "semantic operation requires verified actor and idempotency key".to_string(),
            ));
        }
        if let Some(receipt) =
            self.replay_operation_if_recorded(actor, idempotency_key, nonce, now_ms, |batch| {
                let existing = batch.outbox.first().ok_or_else(|| {
                    SemanticCodeError::Corrupt(
                        "semantic binding replay batch has no binding event".to_string(),
                    )
                })?;
                let existing_binding = SemanticBinding::from_canonical_cbor(&existing.payload)
                    .map_err(semantic_contract_error)?;
                if existing_binding == *binding {
                    Ok(())
                } else {
                    Err(SemanticCodeError::Refused(
                        "semantic binding idempotency key names different content".to_string(),
                    ))
                }
            })?
        {
            return Ok(receipt);
        }
        if binding.durable_state != eg_types::semantic_index::SemanticBindingState::Pending {
            return Err(SemanticCodeError::Refused(
                "semantic binding admission requires pending durable state".to_string(),
            ));
        }
        let payload = binding
            .to_canonical_cbor()
            .map_err(semantic_contract_error)?;
        let mutation = SemanticIndexMutation::StoreBinding {
            binding: Box::new(binding.clone()),
        };
        mutation.validate().map_err(semantic_contract_error)?;
        let mutation_bytes = mutation
            .to_canonical_cbor()
            .map_err(semantic_contract_error)?;
        let mutation_digest = semantic_digest(&mutation_bytes);
        let mut headers = BTreeMap::new();
        headers.insert(
            "schema".to_string(),
            eg_types::semantic_index::SEMANTIC_BINDING_SCHEMA.to_string(),
        );
        headers.insert("binding_id".to_string(), binding.binding_id.clone());
        headers.insert(
            "binding_digest".to_string(),
            binding.binding_digest.to_string(),
        );
        headers.insert("generation".to_string(), binding.generation.to_string());
        headers.insert(
            "source_revision".to_string(),
            binding.source_revision.clone(),
        );
        headers.insert("actor".to_string(), actor.to_string());
        let outbox = vec![MutationOutboxIntent {
            topic: SEMANTIC_BINDING_CREATED_TOPIC.to_string(),
            key: format!("{}:{}", binding.binding_id, binding.generation),
            payload,
            headers,
        }];
        let batch_id = format!("semantic-index:operation:{idempotency_key}");
        let binding_bytes = binding
            .to_canonical_cbor()
            .map_err(semantic_contract_error)?;
        let owner = &self.serving;
        self.commit_metadata(
            |version| {
                self.metadata_operation_batch(
                    owner,
                    version,
                    MetadataMutation {
                        batch_id: &batch_id,
                        event_type: "semantic_binding_stored",
                        subject: &format!("binding:{}", binding.binding_digest),
                        mutation_digest,
                    },
                    outbox.clone(),
                    now_ms,
                    OperationAttribution {
                        actor,
                        idempotency_key,
                        nonce,
                    },
                )
            },
            mutation_digest,
            now_ms,
            |write, rows| {
                if let Some(existing) = self.read_binding_in_write(write)? {
                    if existing != *binding {
                        return Err(SemanticCodeError::Refused(
                            "semantic binding identity already names different bytes".to_string(),
                        ));
                    }
                    return Err(SemanticCodeError::Refused(
                        "semantic binding is already admitted; retry its original idempotency key"
                            .to_string(),
                    ));
                }
                rows.open_table(SEMANTIC_BINDINGS)
                    .map_err(kernel_error)?
                    .insert(
                        (
                            self.tenant.as_str(),
                            self.binding.as_str(),
                            binding.generation,
                        ),
                        binding_bytes.as_slice(),
                    )
                    .map_err(kernel_error)?;
                rows.open_table(SEMANTIC_HEADS)
                    .map_err(kernel_error)?
                    .insert(
                        (self.tenant.as_str(), self.binding.as_str()),
                        binding.generation,
                    )
                    .map_err(kernel_error)?;
                Ok(())
            },
        )
    }

    /// Refresh a source generation and admit its replacement S1 intent in the
    /// same caller-attributed mutation. The query-plan service uses this seam
    /// when it has already read the authoritative source snapshot. There is no
    /// head-moving refresh operation without this intent: a later standalone
    /// wakeup would otherwise leave a crash window between the moved
    /// generation head and its first claimable work.
    pub(crate) fn refresh_binding_operation_with_s1(
        &self,
        expected_generation: u64,
        replacement: &SemanticBinding,
        source_manifest: &SemanticSqlSourceManifest,
        replacement_intent: &SemanticStageIntent,
        now_ms: u64,
        attribution: OperationAttribution<'_>,
    ) -> Result<SemanticMutationReceipt, SemanticCodeError> {
        let OperationAttribution {
            actor,
            idempotency_key,
            nonce,
        } = attribution;
        self.refresh_binding_operation_inner(
            expected_generation,
            replacement,
            source_manifest,
            replacement_intent,
            now_ms,
            OperationAttribution {
                actor,
                idempotency_key,
                nonce,
            },
        )
    }

    fn refresh_binding_operation_inner(
        &self,
        expected_generation: u64,
        replacement: &SemanticBinding,
        source_manifest: &SemanticSqlSourceManifest,
        replacement_intent: &SemanticStageIntent,
        now_ms: u64,
        attribution: OperationAttribution<'_>,
    ) -> Result<SemanticMutationReceipt, SemanticCodeError> {
        let OperationAttribution {
            actor,
            idempotency_key,
            nonce,
        } = attribution;
        if actor.trim().is_empty() || idempotency_key.trim().is_empty() {
            return Err(SemanticCodeError::Refused(
                "semantic refresh requires verified actor and idempotency key".to_string(),
            ));
        }
        let source_manifest_digest = source_manifest.manifest_digest.to_string();
        // These are pure content checks, so they may run before replay lookup.
        // They prevent a caller from preserving an old manifest_digest field
        // while changing the manifest body and accidentally taking the replay
        // path without proving the new body is canonical.
        replacement.validate().map_err(semantic_contract_error)?;
        source_manifest
            .validate_against_binding(replacement)
            .map_err(semantic_contract_error)?;
        replacement_intent
            .validate()
            .map_err(semantic_contract_error)?;
        if replacement_intent.binding_id != replacement.binding_id
            || replacement_intent.binding_digest != replacement.binding_digest
            || replacement_intent.generation != replacement.generation
            || replacement_intent.source_revision != replacement.source_revision
            || replacement_intent.input_digest != source_manifest.source_content_digest
            || replacement_intent.scope.source_entity_id()
                != Some(source_manifest.source_entity_id.as_str())
            || replacement_intent.stage != SemanticStage::SourceCommit
            || !matches!(
                &replacement_intent.predecessor,
                SemanticStagePredecessor::None
            )
        {
            return Err(SemanticCodeError::Refused(
                "refresh S1 intent is not bound to the replacement source manifest".to_string(),
            ));
        }
        if let Some(receipt) =
            self.replay_operation_if_recorded(actor, idempotency_key, nonce, now_ms, |batch| {
                let event = batch.outbox.first().ok_or_else(|| {
                    SemanticCodeError::Corrupt(
                        "semantic refresh replay batch has no binding event".to_string(),
                    )
                })?;
                let existing = SemanticBinding::from_canonical_cbor(&event.payload)
                    .map_err(semantic_contract_error)?;
                if existing != *replacement
                    || event.headers.get("source_entity_id").map(String::as_str)
                        != Some(source_manifest.source_entity_id.as_str())
                    || event
                        .headers
                        .get("source_manifest_digest")
                        .map(String::as_str)
                        != Some(source_manifest_digest.as_str())
                {
                    return Err(SemanticCodeError::Refused(
                        "semantic refresh idempotency key names different content".to_string(),
                    ));
                }
                let event = batch
                    .outbox
                    .iter()
                    .find(|event| event.topic == SEMANTIC_STAGE_INTENT_TOPIC)
                    .ok_or_else(|| {
                        SemanticCodeError::Corrupt(
                            "semantic refresh replay batch has no replacement S1 event".to_string(),
                        )
                    })?;
                let existing_intent = SemanticStageIntent::from_canonical_cbor(&event.payload)
                    .map_err(semantic_contract_error)?;
                if existing_intent != *replacement_intent {
                    return Err(SemanticCodeError::Refused(
                        "semantic refresh idempotency key names a different replacement S1"
                            .to_string(),
                    ));
                }
                Ok(())
            })?
        {
            return Ok(receipt);
        }
        if replacement.tenant_id != self.tenant
            || replacement.binding_id != self.binding
            || replacement.durable_state != SemanticBindingState::Pending
        {
            return Err(SemanticCodeError::Refused(
                "semantic refresh replacement is outside this pending owner".to_string(),
            ));
        }
        let current = self.read_binding()?.ok_or_else(|| {
            SemanticCodeError::Refused("semantic refresh has no durable binding".to_string())
        })?;
        if current.generation != expected_generation
            || current.durable_state != SemanticBindingState::Live
        {
            return Err(SemanticCodeError::Refused(
                "semantic refresh requires the expected live generation".to_string(),
            ));
        }
        let expected_generation = current.generation.checked_add(1).ok_or_else(|| {
            SemanticCodeError::Refused("semantic refresh generation exhausted".to_string())
        })?;
        if replacement.generation != expected_generation
            || replacement.actor_scope != current.actor_scope
            || replacement.effective_actor_scope != current.effective_actor_scope
            || replacement.purpose_id != current.purpose_id
            || replacement.policy_identity != current.policy_identity
            || replacement.policy_digest != current.policy_digest
            || replacement.source_selector != current.source_selector
            || replacement.vector_target_id != current.vector_target_id
            || replacement.dimension != current.dimension
            || replacement.metric != current.metric
            || replacement.model_id != current.model_id
            || replacement.model_revision != current.model_revision
            || replacement.preprocess_digest != current.preprocess_digest
            || replacement.model_digest != current.model_digest
            || replacement.maintenance_policy_id != current.maintenance_policy_id
            || replacement.queue_profile_id != current.queue_profile_id
            || replacement.lexical_index_identity.spec() != current.lexical_index_identity.spec()
            || replacement.ann_index_identity.spec() != current.ann_index_identity.spec()
        {
            return Err(SemanticCodeError::Refused(
                "semantic refresh changed an immutable binding authority".to_string(),
            ));
        }
        validate_sql_source_revision(&replacement.source_revision)?;
        if current.source_revision.starts_with("sql-source:") {
            let current_revision =
                sql_source_revision_parts(&current.source_revision).ok_or_else(|| {
                    SemanticCodeError::Refused(
                        "semantic refresh live head has an invalid SQL source revision".to_string(),
                    )
                })?;
            let replacement_revision = sql_source_revision_parts(&replacement.source_revision)
                .ok_or_else(|| {
                    SemanticCodeError::Refused(
                        "semantic refresh replacement has an invalid SQL source revision"
                            .to_string(),
                    )
                })?;
            if current_revision.authority != replacement_revision.authority
                || replacement_revision.epoch <= current_revision.epoch
            {
                return Err(SemanticCodeError::Refused(
                    "semantic refresh source revision is not a newer epoch in the live SQL authority"
                        .to_string(),
                ));
            }
        } else if compare_source_revision(&replacement.source_revision, &current.source_revision)
            != std::cmp::Ordering::Greater
        {
            return Err(SemanticCodeError::Refused(
                "semantic refresh source revision is not newer than the live head".to_string(),
            ));
        }
        let read = self.serving_read()?;
        let progress = read
            .open_owner_table(SEMANTIC_SOURCE_PROGRESS)
            .map_err(kernel_error)?
            .get((
                self.tenant.as_str(),
                self.binding.as_str(),
                current.generation,
                source_manifest.source_entity_id.as_str(),
            ))
            .map_err(kernel_error)?
            .map(|value| value.value().to_vec())
            .ok_or_else(|| {
                SemanticCodeError::Refused(
                    "semantic refresh lacks the old generation source progress proof".to_string(),
                )
            })?;
        let progress = SemanticSourceProgress::from_canonical_cbor(&progress)
            .map_err(semantic_contract_error)?;
        let retained_receipt_digest = progress.completed_receipt_digest.ok_or_else(|| {
            SemanticCodeError::Refused(
                "semantic refresh requires a completed predecessor receipt".to_string(),
            )
        })?;
        let completed_stage = progress.completed_stage.ok_or_else(|| {
            SemanticCodeError::Refused(
                "semantic refresh predecessor has no completed stage identity".to_string(),
            )
        })?;
        let retained_receipt_key = stage_receipt_index_key(retained_receipt_digest);
        let retained_receipt = read
            .open_owner_table(SEMANTIC_STAGES)
            .map_err(kernel_error)?
            .get((
                self.tenant.as_str(),
                self.binding.as_str(),
                retained_receipt_key.as_str(),
            ))
            .map_err(kernel_error)?
            .map(|value| value.value().to_vec())
            .ok_or_else(|| {
                SemanticCodeError::Refused(
                    "semantic refresh predecessor receipt has no indexed stage row".to_string(),
                )
            })?;
        let retained_receipt = SemanticIndexMutation::from_canonical_cbor(&retained_receipt)
            .map_err(semantic_contract_error)?;
        let retained_receipt_verified = match retained_receipt {
            SemanticIndexMutation::RecordStageTransition { transition, .. } => {
                transition.intent.binding_id == current.binding_id
                    && transition.intent.binding_digest == current.binding_digest
                    && transition.intent.generation == current.generation
                    && transition.intent.source_revision == progress.source_revision
                    && transition.intent.stage == completed_stage
                    && transition.intent.scope.source_entity_id()
                        == Some(source_manifest.source_entity_id.as_str())
                    && matches!(
                        transition.receipt.outcome,
                        SemanticStageOutcome::Completed | SemanticStageOutcome::IdempotentNoop
                    )
                    && transition.receipt.receipt_digest() == retained_receipt_digest
            }
            _ => false,
        };
        if !retained_receipt_verified {
            return Err(SemanticCodeError::Refused(
                "semantic refresh predecessor receipt is not the exact durable stage receipt"
                    .to_string(),
            ));
        }
        if progress.binding_id != current.binding_id
            || progress.binding_digest != current.binding_digest
            || progress.generation != current.generation
            || progress.source_revision != current.source_revision
            || compare_source_revision(&replacement.source_revision, &progress.source_revision)
                != std::cmp::Ordering::Greater
        {
            return Err(SemanticCodeError::Refused(
                "semantic refresh predecessor proof is stale".to_string(),
            ));
        }
        let mutation = SemanticIndexMutation::SupersedeSourceRevision {
            binding_id: replacement.binding_id.clone(),
            binding_digest: replacement.binding_digest,
            generation: replacement.generation,
            source_entity_id: source_manifest.source_entity_id.clone(),
            superseded_revision: progress.source_revision.clone(),
            replacement_revision: replacement.source_revision.clone(),
            retained_receipt_digest,
        };
        mutation.validate().map_err(semantic_contract_error)?;
        let mutation_bytes = mutation
            .to_canonical_cbor()
            .map_err(semantic_contract_error)?;
        let mutation_digest = semantic_digest(&mutation_bytes);
        let replacement_bytes = replacement
            .to_canonical_cbor()
            .map_err(semantic_contract_error)?;
        let mut headers = BTreeMap::from([
            (
                "schema".to_string(),
                eg_types::semantic_index::SEMANTIC_BINDING_SCHEMA.to_string(),
            ),
            ("actor".to_string(), actor.to_string()),
            ("binding_id".to_string(), replacement.binding_id.clone()),
            (
                "source_entity_id".to_string(),
                source_manifest.source_entity_id.clone(),
            ),
            (
                "binding_digest".to_string(),
                replacement.binding_digest.to_string(),
            ),
            ("generation".to_string(), replacement.generation.to_string()),
            (
                "superseded_revision".to_string(),
                progress.source_revision.clone(),
            ),
            (
                "replacement_revision".to_string(),
                replacement.source_revision.clone(),
            ),
        ]);
        headers.insert(
            "retained_receipt_digest".to_string(),
            retained_receipt_digest.to_string(),
        );
        headers.insert("source_manifest_digest".to_string(), source_manifest_digest);
        let mut outbox = vec![MutationOutboxIntent {
            // A refresh creates a new durable binding generation; reuse the
            // existing binding-created topic so composition has one governed
            // binding-definition event rather than a parallel compatibility
            // route. The operation event type and headers carry supersession
            // proof for consumers that need to distinguish the cause.
            topic: SEMANTIC_BINDING_CREATED_TOPIC.to_string(),
            key: format!("{}:{}", replacement.binding_id, replacement.generation),
            payload: replacement_bytes.clone(),
            headers,
        }];
        outbox.push(stage_intent_outbox(replacement_intent)?);
        let batch_id = format!("semantic-index:operation:{idempotency_key}");
        let tenant = self.tenant.clone();
        let binding_id = self.binding.clone();
        let current_for_write = current.clone();
        let progress_for_write = progress.clone();
        let source_entity_id_for_write = source_manifest.source_entity_id.clone();
        let replacement_for_write = replacement.clone();
        let replacement_intent_for_write = replacement_intent.clone();
        let refresh_at = now_ms;
        self.commit_metadata(
            |version| {
                self.metadata_operation_batch(
                    &self.serving,
                    version,
                    MetadataMutation {
                        batch_id: &batch_id,
                        event_type: "semantic_binding_refreshed",
                        subject: &format!("binding:{}", replacement.binding_digest),
                        mutation_digest,
                    },
                    outbox,
                    now_ms,
                    OperationAttribution {
                        actor,
                        idempotency_key,
                        nonce,
                    },
                )
            },
            mutation_digest,
            now_ms,
            |write, rows| {
                let current = self.read_binding_in_write(write)?.ok_or_else(|| {
                    SemanticCodeError::Refused(
                        "semantic refresh head disappeared during admission".to_string(),
                    )
                })?;
                if current != current_for_write {
                    return Err(SemanticCodeError::Refused(
                        "semantic refresh head changed during admission".to_string(),
                    ));
                }
                let progress_raw = write
                    .open_read_table(SEMANTIC_SOURCE_PROGRESS)
                    .map_err(kernel_error)?
                    .get((
                        tenant.as_str(),
                        binding_id.as_str(),
                        current_for_write.generation,
                        source_entity_id_for_write.as_str(),
                    ))
                    .map_err(kernel_error)?
                    .map(|value| value.value().to_vec())
                    .ok_or_else(|| {
                        SemanticCodeError::Refused(
                            "semantic refresh source progress disappeared during admission"
                                .to_string(),
                        )
                    })?;
                let durable_progress = SemanticSourceProgress::from_canonical_cbor(&progress_raw)
                    .map_err(semantic_contract_error)?;
                if durable_progress != progress_for_write {
                    return Err(SemanticCodeError::Refused(
                        "semantic refresh source progress changed during admission".to_string(),
                    ));
                }
                if durable_progress.superseded_by_revision.is_some() {
                    return Err(SemanticCodeError::Refused(
                        "semantic refresh source progress was already superseded".to_string(),
                    ));
                }
                let binding_key = (
                    tenant.as_str(),
                    binding_id.as_str(),
                    replacement_for_write.generation,
                );
                let mut bindings = rows.open_table(SEMANTIC_BINDINGS).map_err(kernel_error)?;
                put_bytes_once(&mut bindings, binding_key, &replacement_bytes)?;
                drop(bindings);
                let progress = SemanticSourceProgress {
                    binding_id: replacement_intent_for_write.binding_id.clone(),
                    binding_digest: replacement_intent_for_write.binding_digest,
                    generation: replacement_intent_for_write.generation,
                    source_entity_id: source_entity_id_for_write.clone(),
                    source_revision: replacement_intent_for_write.source_revision.clone(),
                    completed_stage: None,
                    completed_receipt_digest: None,
                    superseded_by_revision: None,
                    updated_at: format!("unix-ms:{refresh_at}"),
                };
                progress.validate().map_err(semantic_contract_error)?;
                let progress_bytes = progress
                    .to_canonical_cbor()
                    .map_err(semantic_contract_error)?;
                rows.open_table(SEMANTIC_SOURCE_PROGRESS)
                    .map_err(kernel_error)?
                    .insert(
                        (
                            tenant.as_str(),
                            binding_id.as_str(),
                            replacement_intent_for_write.generation,
                            source_entity_id_for_write.as_str(),
                        ),
                        progress_bytes.as_slice(),
                    )
                    .map_err(kernel_error)?;
                let mut superseded_progress = durable_progress;
                superseded_progress.superseded_by_revision =
                    Some(replacement_for_write.source_revision.clone());
                superseded_progress.updated_at = format!("unix-ms:{refresh_at}");
                superseded_progress
                    .validate()
                    .map_err(semantic_contract_error)?;
                let superseded_progress_bytes = superseded_progress
                    .to_canonical_cbor()
                    .map_err(semantic_contract_error)?;
                let mut progress_rows = rows
                    .open_table(SEMANTIC_SOURCE_PROGRESS)
                    .map_err(kernel_error)?;
                replace_bytes(
                    &mut progress_rows,
                    (
                        tenant.as_str(),
                        binding_id.as_str(),
                        current_for_write.generation,
                        source_entity_id_for_write.as_str(),
                    ),
                    &superseded_progress_bytes,
                )?;
                drop(progress_rows);
                let mut heads = rows.open_table(SEMANTIC_HEADS).map_err(kernel_error)?;
                heads
                    .insert(
                        (tenant.as_str(), binding_id.as_str()),
                        replacement_for_write.generation,
                    )
                    .map_err(kernel_error)?;
                Ok(())
            },
        )
    }

    /// Apply one caller-attributed binding state transition. The current
    /// binding row is re-read through the admitted write before the transition
    /// and pointer change, so an expected generation/state cannot be bypassed
    /// by a stale request body.
    pub(crate) fn transition_binding_operation(
        &self,
        expected_generation: u64,
        next: SemanticBindingState,
        now_ms: u64,
        actor: &str,
        idempotency_key: &str,
        nonce: Nonce,
    ) -> Result<SemanticMutationReceipt, SemanticCodeError> {
        if actor.trim().is_empty() || idempotency_key.trim().is_empty() {
            return Err(SemanticCodeError::Refused(
                "semantic state transition requires verified actor and idempotency key".to_string(),
            ));
        }
        if !matches!(
            next,
            SemanticBindingState::Building | SemanticBindingState::Disabled
        ) {
            return Err(SemanticCodeError::Refused(
                "caller state operation may only start a pending build or disable a live binding"
                    .to_string(),
            ));
        }
        if let Some(receipt) =
            self.replay_operation_if_recorded(actor, idempotency_key, nonce, now_ms, |batch| {
                let event = batch.outbox.first().ok_or_else(|| {
                    SemanticCodeError::Corrupt(
                        "semantic state replay batch has no transition event".to_string(),
                    )
                })?;
                let transition =
                    SemanticBindingStateTransition::from_canonical_cbor(&event.payload)
                        .map_err(semantic_contract_error)?;
                if transition.binding_id == self.binding
                    && transition.generation == expected_generation
                    && transition.next == next
                {
                    Ok(())
                } else {
                    Err(SemanticCodeError::Refused(
                        "semantic state idempotency key names different content".to_string(),
                    ))
                }
            })?
        {
            return Ok(receipt);
        }
        let binding = self.read_binding()?.ok_or_else(|| {
            SemanticCodeError::Refused(
                "semantic state transition has no durable binding".to_string(),
            )
        })?;
        if binding.generation != expected_generation {
            return Err(SemanticCodeError::Refused(
                "semantic state transition generation is stale".to_string(),
            ));
        }
        if !matches!(
            (binding.durable_state, next),
            (
                SemanticBindingState::Pending,
                SemanticBindingState::Building,
            ) | (SemanticBindingState::Live, SemanticBindingState::Disabled)
        ) {
            return Err(SemanticCodeError::Refused(
                "semantic state transition is not valid for the durable binding state".to_string(),
            ));
        }
        let transition = SemanticBindingStateTransition::create(
            &binding,
            next,
            "caller_requested_semantic_binding_state",
        )
        .map_err(semantic_contract_error)?;
        let mutation = SemanticIndexMutation::SetBindingState {
            transition: transition.clone(),
        };
        mutation.validate().map_err(semantic_contract_error)?;
        let mutation_bytes = mutation
            .to_canonical_cbor()
            .map_err(semantic_contract_error)?;
        let mutation_digest = semantic_digest(&mutation_bytes);
        let payload = transition
            .to_canonical_cbor()
            .map_err(semantic_contract_error)?;
        let mut headers = BTreeMap::new();
        headers.insert(
            "schema".to_string(),
            eg_types::semantic_index::SEMANTIC_BINDING_STATE_TRANSITION_SCHEMA.to_string(),
        );
        headers.insert("binding_id".to_string(), binding.binding_id.clone());
        headers.insert("generation".to_string(), binding.generation.to_string());
        headers.insert("actor".to_string(), actor.to_string());
        let outbox = vec![MutationOutboxIntent {
            topic: SEMANTIC_BINDING_STATE_TOPIC.to_string(),
            key: format!(
                "{}:{}:{}",
                binding.binding_id,
                binding.generation,
                next.as_str()
            ),
            payload,
            headers,
        }];
        let batch_id = format!("semantic-index:operation:{idempotency_key}");
        let binding_id = self.binding.clone();
        let tenant = self.tenant.clone();
        self.commit_metadata(
            |version| {
                self.metadata_operation_batch(
                    &self.serving,
                    version,
                    MetadataMutation {
                        batch_id: &batch_id,
                        event_type: "semantic_binding_state_transition",
                        subject: &format!("binding:{}", binding.binding_digest),
                        mutation_digest,
                    },
                    outbox,
                    now_ms,
                    OperationAttribution {
                        actor,
                        idempotency_key,
                        nonce,
                    },
                )
            },
            mutation_digest,
            now_ms,
            |write, rows| {
                let current = self.read_binding_in_write(write)?.ok_or_else(|| {
                    SemanticCodeError::Refused(
                        "semantic binding disappeared during state transition".to_string(),
                    )
                })?;
                if current.binding_id != binding_id
                    || current.generation != expected_generation
                    || current.binding_digest != binding.binding_digest
                    || current.durable_state != transition.expected
                {
                    return Err(SemanticCodeError::Refused(
                        "semantic binding state or generation changed during transition"
                            .to_string(),
                    ));
                }
                let updated = current
                    .apply_state_transition(&transition)
                    .map_err(semantic_contract_error)?;
                let binding_bytes = updated
                    .to_canonical_cbor()
                    .map_err(semantic_contract_error)?;
                let mut bindings = rows.open_table(SEMANTIC_BINDINGS).map_err(kernel_error)?;
                replace_bytes(
                    &mut bindings,
                    (tenant.as_str(), binding_id.as_str(), expected_generation),
                    &binding_bytes,
                )?;
                drop(bindings);
                let state_bytes = transition
                    .to_canonical_cbor()
                    .map_err(semantic_contract_error)?;
                let mut states = rows.open_table(SEMANTIC_STATES).map_err(kernel_error)?;
                replace_bytes(
                    &mut states,
                    (tenant.as_str(), binding_id.as_str()),
                    &state_bytes,
                )?;
                drop(states);
                if next == SemanticBindingState::Disabled {
                    rows.open_table(SEMANTIC_POINTERS)
                        .map_err(kernel_error)?
                        .remove((tenant.as_str(), binding_id.as_str()))
                        .map_err(kernel_error)?;
                }
                Ok(())
            },
        )
    }

    /// Drop is a two-proof lifecycle operation: the binding must first be in
    /// Disabled/Failed state, then the canonical tombstone and Dropping state
    /// are written in the same caller-attributed mutation. The old binding row
    /// remains as the durable tombstone's identity anchor.
    pub(crate) fn drop_binding_operation(
        &self,
        expected_generation: u64,
        now_ms: u64,
        actor: &str,
        idempotency_key: &str,
        nonce: Nonce,
    ) -> Result<SemanticMutationReceipt, SemanticCodeError> {
        if actor.trim().is_empty() || idempotency_key.trim().is_empty() {
            return Err(SemanticCodeError::Refused(
                "semantic drop requires verified actor and idempotency key".to_string(),
            ));
        }
        if let Some(receipt) =
            self.replay_operation_if_recorded(actor, idempotency_key, nonce, now_ms, |batch| {
                let event = batch.outbox.first().ok_or_else(|| {
                    SemanticCodeError::Corrupt(
                        "semantic drop replay batch has no tombstone event".to_string(),
                    )
                })?;
                let tombstone = SemanticTombstone::from_canonical_cbor(&event.payload)
                    .map_err(semantic_contract_error)?;
                if tombstone.tenant_id == self.tenant
                    && tombstone.binding_id == self.binding
                    && tombstone.generation == expected_generation
                {
                    Ok(())
                } else {
                    Err(SemanticCodeError::Refused(
                        "semantic drop idempotency key names different content".to_string(),
                    ))
                }
            })?
        {
            return Ok(receipt);
        }
        let binding = self.read_binding()?.ok_or_else(|| {
            SemanticCodeError::Refused("semantic drop has no durable binding".to_string())
        })?;
        if binding.generation != expected_generation
            || !matches!(
                binding.durable_state,
                SemanticBindingState::Disabled | SemanticBindingState::Failed
            )
        {
            return Err(SemanticCodeError::Refused(
                "semantic drop requires the expected disabled or failed generation".to_string(),
            ));
        }
        let tombstone = SemanticTombstone::create(SemanticTombstoneDraft {
            tenant_id: binding.tenant_id.clone(),
            binding_id: binding.binding_id.clone(),
            binding_digest: binding.binding_digest,
            generation: binding.generation,
            deleted_at: format!("unix-ms:{now_ms}"),
        })
        .map_err(semantic_contract_error)?;
        let state_transition = SemanticBindingStateTransition::create(
            &binding,
            SemanticBindingState::Dropping,
            "caller_requested_semantic_binding_drop",
        )
        .map_err(semantic_contract_error)?;
        let mutation = SemanticIndexMutation::DeleteBinding {
            tombstone: Box::new(tombstone.clone()),
        };
        mutation.validate().map_err(semantic_contract_error)?;
        let mutation_bytes = mutation
            .to_canonical_cbor()
            .map_err(semantic_contract_error)?;
        let mutation_digest = semantic_digest(&mutation_bytes);
        let payload = tombstone
            .to_canonical_cbor()
            .map_err(semantic_contract_error)?;
        let outbox = vec![MutationOutboxIntent {
            topic: SEMANTIC_BINDING_DROPPED_TOPIC.to_string(),
            key: format!("{}:{}", binding.binding_id, binding.generation),
            payload,
            headers: BTreeMap::from([
                (
                    "schema".to_string(),
                    eg_types::semantic_index::SEMANTIC_TOMBSTONE_SCHEMA.to_string(),
                ),
                ("actor".to_string(), actor.to_string()),
            ]),
        }];
        let batch_id = format!("semantic-index:operation:{idempotency_key}");
        let binding_id = self.binding.clone();
        let tenant = self.tenant.clone();
        self.commit_metadata(
            |version| {
                self.metadata_operation_batch(
                    &self.serving,
                    version,
                    MetadataMutation {
                        batch_id: &batch_id,
                        event_type: "semantic_binding_dropped",
                        subject: &format!("binding:{}", binding.binding_digest),
                        mutation_digest,
                    },
                    outbox,
                    now_ms,
                    OperationAttribution {
                        actor,
                        idempotency_key,
                        nonce,
                    },
                )
            },
            mutation_digest,
            now_ms,
            |write, rows| {
                let current = self.read_binding_in_write(write)?.ok_or_else(|| {
                    SemanticCodeError::Refused(
                        "semantic binding disappeared during drop".to_string(),
                    )
                })?;
                if current.binding_id != binding_id
                    || current.generation != expected_generation
                    || current.binding_digest != binding.binding_digest
                    || current.durable_state != binding.durable_state
                {
                    return Err(SemanticCodeError::Refused(
                        "semantic binding changed during drop".to_string(),
                    ));
                }
                let updated = current
                    .apply_state_transition(&state_transition)
                    .map_err(semantic_contract_error)?;
                let binding_bytes = updated
                    .to_canonical_cbor()
                    .map_err(semantic_contract_error)?;
                let mut bindings = rows.open_table(SEMANTIC_BINDINGS).map_err(kernel_error)?;
                replace_bytes(
                    &mut bindings,
                    (tenant.as_str(), binding_id.as_str(), expected_generation),
                    &binding_bytes,
                )?;
                drop(bindings);
                let state_bytes = state_transition
                    .to_canonical_cbor()
                    .map_err(semantic_contract_error)?;
                let mut states = rows.open_table(SEMANTIC_STATES).map_err(kernel_error)?;
                replace_bytes(
                    &mut states,
                    (tenant.as_str(), binding_id.as_str()),
                    &state_bytes,
                )?;
                drop(states);
                let tombstone_bytes = tombstone
                    .to_canonical_cbor()
                    .map_err(semantic_contract_error)?;
                let mut tombstones = rows.open_table(SEMANTIC_TOMBSTONES).map_err(kernel_error)?;
                put_bytes_once(
                    &mut tombstones,
                    (tenant.as_str(), binding_id.as_str(), expected_generation),
                    &tombstone_bytes,
                )?;
                drop(tombstones);
                rows.open_table(SEMANTIC_POINTERS)
                    .map_err(kernel_error)?
                    .remove((tenant.as_str(), binding_id.as_str()))
                    .map_err(kernel_error)?;
                Ok(())
            },
        )
    }

    /// Admit an S1 source intent and persist its source cursor atomically with
    /// the claimable outbox row.  Only S1 is accepted here; S2-S6 are produced
    /// by the existing durable consumer/executor ports after their predecessor
    /// receipts are present.  In particular, this method never activates an
    /// ANN generation.
    pub fn enqueue_stage_intent(
        &self,
        intent: &SemanticStageIntent,
        now_ms: u64,
    ) -> Result<SemanticMutationReceipt, SemanticCodeError> {
        intent.validate().map_err(semantic_contract_error)?;
        if intent.binding_id != self.binding || intent.generation == 0 {
            return Err(SemanticCodeError::Refused(
                "semantic stage intent is outside this binding".to_string(),
            ));
        }
        if intent.stage != SemanticStage::SourceCommit
            || !matches!(&intent.predecessor, SemanticStagePredecessor::None)
        {
            return Err(SemanticCodeError::Refused(
                "native semantic admission accepts only an S1 intent with no predecessor"
                    .to_string(),
            ));
        }
        let source_entity_id = intent.scope.source_entity_id().ok_or_else(|| {
            SemanticCodeError::Refused("S1 semantic intent must name one source entity".to_string())
        })?;
        let payload = intent
            .to_canonical_cbor()
            .map_err(semantic_contract_error)?;
        // The canonical intent digest covers every identity field, including
        // delimiters and predecessor data.  A concatenated transport key could
        // alias two otherwise distinct intents when a caller supplied a
        // delimiter-containing component.
        let key = stage_intent_key(intent);
        let mut headers = BTreeMap::new();
        headers.insert(
            "schema".to_string(),
            eg_types::semantic_index::SEMANTIC_STAGE_INTENT_SCHEMA.to_string(),
        );
        headers.insert("binding_id".to_string(), intent.binding_id.clone());
        headers.insert(
            "binding_digest".to_string(),
            intent.binding_digest.to_string(),
        );
        headers.insert("generation".to_string(), intent.generation.to_string());
        headers.insert("source_entity_id".to_string(), source_entity_id.to_string());
        headers.insert(
            "source_revision".to_string(),
            intent.source_revision.clone(),
        );
        headers.insert("stage".to_string(), intent.stage.as_str().to_string());
        headers.insert(
            "intent_digest".to_string(),
            intent.intent_digest.to_string(),
        );
        let outbox = MutationOutboxIntent {
            topic: SEMANTIC_STAGE_INTENT_TOPIC.to_string(),
            key: key.clone(),
            payload,
            headers,
        };
        let batch_id = format!("semantic-index:intent:{}", intent.intent_digest);
        let owner = &self.serving;
        let intent_digest = intent.intent_digest;
        self.commit_metadata(
            |version| {
                self.metadata_batch(
                         owner,
                         version,
                         MetadataMutation {
                             batch_id: &batch_id,
                             event_type: "semantic_stage_intent_enqueued",
                             subject: &format!("intent:{}", intent.intent_digest),
                             mutation_digest: intent_digest,
                         },
                         vec![outbox],
                         now_ms,
                     )
            },
            intent_digest,
            now_ms,
            |write, rows| {
                let binding = self.read_binding_in_write(write)?.ok_or_else(|| {
                    SemanticCodeError::Refused(
                        "semantic stage intent names no durable binding".to_string(),
                    )
                })?;
                if binding.binding_digest != intent.binding_digest
                    || binding.generation != intent.generation
                {
                    return Err(SemanticCodeError::Refused(
                        "semantic stage intent binding digest or generation is stale".to_string(),
                    ));
                }
                let old = write
                    .open_read_table(SEMANTIC_SOURCE_PROGRESS)
                    .map_err(kernel_error)?
                    .get((
                        self.tenant.as_str(),
                        self.binding.as_str(),
                        intent.generation,
                        source_entity_id,
                    ))
                    .map_err(kernel_error)?
                    .map(|value| value.value().to_vec());
                if let Some(old) = old {
                    let old = SemanticSourceProgress::from_canonical_cbor(&old)
                        .map_err(semantic_contract_error)?;
                    if old.source_revision == intent.source_revision {
                        return Err(SemanticCodeError::Refused(
                            "semantic source revision is already admitted; retry the same intent"
                                .to_string(),
                        ));
                    }
                    if compare_source_revision(&intent.source_revision, &old.source_revision)
                        != std::cmp::Ordering::Greater
                    {
                        return Err(SemanticCodeError::Refused(
                            "semantic source revision is stale or not strictly newer".to_string(),
                        ));
                    }
                    if old.completed_stage.is_none() {
                        return Err(SemanticCodeError::Refused(
                            "semantic source revision has no completed predecessor; retain the row and refresh the binding generation"
                                .to_string(),
                        ));
                    }
                    return Err(SemanticCodeError::Refused(
                        "semantic source revision supersession requires a replacement binding generation"
                            .to_string(),
                    ));
                }
                let progress = SemanticSourceProgress {
                    binding_id: intent.binding_id.clone(),
                    binding_digest: intent.binding_digest,
                    generation: intent.generation,
                    source_entity_id: source_entity_id.to_string(),
                    source_revision: intent.source_revision.clone(),
                    completed_stage: None,
                    completed_receipt_digest: None,
                    superseded_by_revision: None,
                    updated_at: format!("unix-ms:{now_ms}"),
                };
                progress.validate().map_err(semantic_contract_error)?;
                let bytes = progress
                    .to_canonical_cbor()
                    .map_err(semantic_contract_error)?;
                rows.open_table(SEMANTIC_SOURCE_PROGRESS)
                    .map_err(kernel_error)?
                    .insert(
                        (
                            self.tenant.as_str(),
                            self.binding.as_str(),
                            intent.generation,
                            source_entity_id,
                        ),
                        bytes.as_slice(),
                    )
                    .map_err(kernel_error)?;
                Ok(())
            },
        )
    }

    /// Admit the S1 tombstone derived from a completed authoritative source
    /// reconciliation.  This is deliberately a separate admission from
    /// [`Self::enqueue_stage_intent`]: a normal source wakeup must continue to
    /// reject a same-revision row and must require a replacement generation
    /// for a newer revision.  A complete snapshot is a stronger deletion
    /// proof, and may replace an already completed row in the same binding
    /// generation even when the snapshot advances the source revision.  A
    /// newer source revision with incomplete prior progress remains on the
    /// replacement-generation path.
    pub(crate) fn enqueue_reconciliation_tombstone(
        &self,
        intent: &SemanticStageIntent,
        checkpoint: &SemanticSourceReconciliationCheckpoint,
        now_ms: u64,
    ) -> Result<SemanticMutationReceipt, SemanticCodeError> {
        intent.validate().map_err(semantic_contract_error)?;
        if intent.binding_id != self.binding
            || intent.generation == 0
            || intent.stage != SemanticStage::SourceCommit
            || !matches!(&intent.predecessor, SemanticStagePredecessor::None)
        {
            return Err(SemanticCodeError::Refused(
                "semantic reconciliation tombstone must be an S1 intent for this binding"
                    .to_string(),
            ));
        }
        let source_entity_id = intent.scope.source_entity_id().ok_or_else(|| {
            SemanticCodeError::Refused(
                "semantic reconciliation tombstone must name one source entity".to_string(),
            )
        })?;
        if !valid_source_entity_id_for_reconciliation(source_entity_id) {
            return Err(SemanticCodeError::Refused(
                "semantic reconciliation tombstone has a non-canonical source entity".to_string(),
            ));
        }
        validate_reconciliation_checkpoint(checkpoint)?;
        if !matches!(
            checkpoint.phase,
            SemanticSourceReconciliationPhase::FinalizingTombstones
        ) || checkpoint.source_revision != intent.source_revision
        {
            return Err(SemanticCodeError::Refused(
                "semantic reconciliation tombstone lacks an exact finalizing source proof"
                    .to_string(),
            ));
        }
        let complete_receipt = checkpoint.complete_snapshot_receipt_digest.ok_or_else(|| {
            SemanticCodeError::Refused(
                "semantic reconciliation tombstone has no complete snapshot proof".to_string(),
            )
        })?;
        let expected_input = reconciliation_tombstone_input_digest(
            source_entity_id,
            &checkpoint.source_revision,
            complete_receipt,
        );
        if intent.input_digest != expected_input {
            return Err(SemanticCodeError::Refused(
                "semantic reconciliation tombstone input is not the exact deletion proof"
                    .to_string(),
            ));
        }
        let checkpoint_bytes = encode_reconciliation_checkpoint(checkpoint)?;
        let payload = intent
            .to_canonical_cbor()
            .map_err(semantic_contract_error)?;
        let key = stage_intent_key(intent);
        let mut outbox = stage_intent_outbox(intent)?;
        outbox.headers.insert(
            "reconciliation_proof".to_string(),
            "semantic-source-tombstone/v1".to_string(),
        );
        outbox.headers.insert(
            "source_wakeup_digest".to_string(),
            checkpoint.source_wakeup_digest.to_string(),
        );
        outbox.headers.insert(
            "complete_snapshot_receipt_digest".to_string(),
            complete_receipt.to_string(),
        );
        // Keep the payload/key generated by the canonical stage helper. The
        // local binding below only exists so the immutable payload remains
        // obvious in this admission's proof construction.
        debug_assert_eq!(outbox.key, key);
        debug_assert_eq!(outbox.payload, payload);
        let batch_id = format!(
            "semantic-index:reconciliation-tombstone:{}:{}",
            intent.intent_digest, checkpoint.source_wakeup_digest
        );
        let owner = &self.serving;
        let tenant = self.tenant.clone();
        let binding_id = self.binding.clone();
        let intent_digest = intent.intent_digest;
        let source_entity_id = source_entity_id.to_string();
        let intent_for_write = intent.clone();
        let checkpoint_for_write = checkpoint.clone();
        self.commit_metadata(
            |version| {
                self.metadata_batch(
                         owner,
                         version,
                         MetadataMutation {
                             batch_id: &batch_id,
                             event_type: "semantic_source_tombstone_enqueued",
                             subject: &format!("source:{source_entity_id}"),
                             mutation_digest: intent_digest,
                         },
                         vec![outbox],
                         now_ms,
                     )
            },
            intent_digest,
            now_ms,
            |write, rows| {
                let current_binding = self.read_binding_in_write(write)?.ok_or_else(|| {
                    SemanticCodeError::Refused(
                        "semantic reconciliation tombstone has no durable binding".to_string(),
                    )
                })?;
                if current_binding.binding_id != binding_id
                    || current_binding.binding_digest != intent_for_write.binding_digest
                    || current_binding.generation != intent_for_write.generation
                {
                    return Err(SemanticCodeError::Refused(
                        "semantic reconciliation tombstone binding proof is stale".to_string(),
                    ));
                }
                let binding_revision =
                    sql_source_revision_parts(&current_binding.source_revision).ok_or_else(|| {
                        SemanticCodeError::Corrupt(
                            "durable semantic binding has a non-canonical SQL source revision"
                                .to_string(),
                        )
                    })?;
                let intent_revision =
                    sql_source_revision_parts(&intent_for_write.source_revision).ok_or_else(
                        || {
                            SemanticCodeError::Refused(
                                "semantic reconciliation tombstone has a non-canonical SQL source revision"
                                    .to_string(),
                            )
                        },
                    )?;
                if binding_revision.authority != intent_revision.authority {
                    return Err(SemanticCodeError::Refused(
                        "semantic reconciliation tombstone belongs to another source authority"
                            .to_string(),
                    ));
                }
                let durable_checkpoint = write
                    .open_read_table(SEMANTIC_SOURCE_PROGRESS)
                    .map_err(kernel_error)?
                    .get((
                        tenant.as_str(),
                        binding_id.as_str(),
                        intent_for_write.generation,
                        SEMANTIC_RECONCILIATION_CHECKPOINT_ENTITY,
                    ))
                    .map_err(kernel_error)?
                    .map(|value| value.value().to_vec())
                    .ok_or_else(|| {
                        SemanticCodeError::Refused(
                            "semantic reconciliation tombstone has no durable checkpoint"
                                .to_string(),
                        )
                    })?;
                if durable_checkpoint != checkpoint_bytes {
                    return Err(SemanticCodeError::Refused(
                        "semantic reconciliation tombstone checkpoint proof is stale"
                            .to_string(),
                    ));
                }
                let old_raw = write
                    .open_read_table(SEMANTIC_SOURCE_PROGRESS)
                    .map_err(kernel_error)?
                    .get((
                        tenant.as_str(),
                        binding_id.as_str(),
                        intent_for_write.generation,
                        source_entity_id.as_str(),
                    ))
                    .map_err(kernel_error)?
                    .map(|value| value.value().to_vec())
                    .ok_or_else(|| {
                        SemanticCodeError::Refused(
                            "semantic reconciliation tombstone names no durable prior entity"
                                .to_string(),
                        )
                    })?;
                let old = SemanticSourceProgress::from_canonical_cbor(&old_raw)
                    .map_err(semantic_contract_error)?;
                old.validate().map_err(semantic_contract_error)?;
                validate_sql_source_revision(&old.source_revision)?;
                let old_revision = sql_source_revision_parts(&old.source_revision).ok_or_else(|| {
                    SemanticCodeError::Corrupt(
                        "semantic source progress has a non-canonical SQL source revision"
                            .to_string(),
                    )
                })?;
                if old_revision.authority != intent_revision.authority {
                    return Err(SemanticCodeError::Corrupt(
                        "semantic source progress belongs to another source authority".to_string(),
                    ));
                }
                if old.binding_id != binding_id
                    || old.binding_digest != intent_for_write.binding_digest
                    || old.generation != intent_for_write.generation
                    || old.source_entity_id != source_entity_id
                    || old.superseded_by_revision.is_some()
                {
                    return Err(SemanticCodeError::Refused(
                        "semantic reconciliation tombstone prior progress is outside the current generation"
                            .to_string(),
                    ));
                }
                if old.source_revision != intent_for_write.source_revision
                    && compare_source_revision(
                        &intent_for_write.source_revision,
                        &old.source_revision,
                    ) != std::cmp::Ordering::Greater
                {
                    return Err(SemanticCodeError::Refused(
                        "semantic reconciliation tombstone source revision is stale".to_string(),
                    ));
                }
                let retained_receipt_digest = old.completed_receipt_digest.ok_or_else(|| {
                    SemanticCodeError::Refused(
                        "semantic reconciliation tombstone requires completed prior progress"
                            .to_string(),
                    )
                })?;
                let completed_stage = old.completed_stage.ok_or_else(|| {
                    SemanticCodeError::Refused(
                        "semantic reconciliation tombstone requires a completed prior stage"
                            .to_string(),
                    )
                })?;
                let retained_key = stage_receipt_index_key(retained_receipt_digest);
                let retained_raw = write
                    .open_read_table(SEMANTIC_STAGES)
                    .map_err(kernel_error)?
                    .get((tenant.as_str(), binding_id.as_str(), retained_key.as_str()))
                    .map_err(kernel_error)?
                    .map(|value| value.value().to_vec())
                    .ok_or_else(|| {
                        SemanticCodeError::Refused(
                            "semantic reconciliation tombstone prior receipt is not indexed"
                                .to_string(),
                        )
                    })?;
                let retained = SemanticIndexMutation::from_canonical_cbor(&retained_raw)
                    .map_err(semantic_contract_error)?;
                let retained_verified = match retained {
                    SemanticIndexMutation::RecordStageTransition { transition, .. } => {
                        transition.intent.binding_id == binding_id
                            && transition.intent.binding_digest
                                == intent_for_write.binding_digest
                            && transition.intent.generation == intent_for_write.generation
                            && transition.intent.scope.source_entity_id()
                                == Some(source_entity_id.as_str())
                            && transition.intent.source_revision == old.source_revision
                            && transition.intent.stage == completed_stage
                            && matches!(
                                transition.receipt.outcome,
                                SemanticStageOutcome::Completed
                                    | SemanticStageOutcome::IdempotentNoop
                            )
                            && transition.receipt.receipt_digest() == retained_receipt_digest
                    }
                    _ => false,
                };
                if !retained_verified {
                    return Err(SemanticCodeError::Refused(
                        "semantic reconciliation tombstone prior receipt is not exact"
                            .to_string(),
                    ));
                }
                let progress = SemanticSourceProgress {
                    binding_id: intent_for_write.binding_id.clone(),
                    binding_digest: intent_for_write.binding_digest,
                    generation: intent_for_write.generation,
                    source_entity_id: source_entity_id.clone(),
                    source_revision: intent_for_write.source_revision.clone(),
                    completed_stage: None,
                    completed_receipt_digest: None,
                    superseded_by_revision: None,
                    updated_at: format!("unix-ms:{now_ms}"),
                };
                progress.validate().map_err(semantic_contract_error)?;
                let progress_bytes = progress
                    .to_canonical_cbor()
                    .map_err(semantic_contract_error)?;
                let mut progress_rows = rows
                    .open_table(SEMANTIC_SOURCE_PROGRESS)
                    .map_err(kernel_error)?;
                replace_bytes(
                    &mut progress_rows,
                    (
                        tenant.as_str(),
                        binding_id.as_str(),
                        intent_for_write.generation,
                        source_entity_id.as_str(),
                    ),
                    &progress_bytes,
                )?;
                drop(progress_rows);
                // The checkpoint is part of the proof read above. Keep a
                // defensive equality check here so a future caller cannot
                // accidentally discard the proof while extending the write.
                if checkpoint_for_write.complete_snapshot_receipt_digest
                    != Some(complete_receipt)
                {
                    return Err(SemanticCodeError::Corrupt(
                        "semantic reconciliation tombstone proof changed during admission"
                            .to_string(),
                    ));
                }
                Ok(())
            },
        )
    }

    /// Validate a leased stage row before a consumer may run work. The
    /// durable lease is the executor's fencing proof and the current binding
    /// must already be durably `Building`; the canonical payload, key, headers,
    /// owner identity, attempt and expiry are all checked before any stage
    /// transition can be constructed. Actual S2-S6 execution and ack/receipt
    /// persistence remain on the existing outbox consumer port.
    pub fn validate_stage_lease(
        &self,
        lease: &MutationOutboxLease,
        consumer: &str,
        now_ms: u64,
    ) -> Result<SemanticStageIntent, SemanticCodeError> {
        let intent = self.parse_stage_lease(lease, consumer, now_ms)?;
        let read = self.serving_read()?;
        self.validate_stage_predecessor_in(&read, &intent, true)?;
        Ok(intent)
    }

    /// Parse and fence the transport-visible lease fields. This helper is
    /// deliberately separate from predecessor validation so a retry can find
    /// an already-recorded transition and acknowledge a newly reclaimed lease
    /// without treating a completed stage as fresh work.
    fn parse_stage_lease(
        &self,
        lease: &MutationOutboxLease,
        consumer: &str,
        now_ms: u64,
    ) -> Result<SemanticStageIntent, SemanticCodeError> {
        lease.record.validate().map_err(|error| {
            SemanticCodeError::Refused(format!("invalid semantic lease record: {error}"))
        })?;
        if consumer.trim().is_empty() || lease.consumer != consumer {
            return Err(SemanticCodeError::Refused(
                "semantic stage lease consumer does not match".to_string(),
            ));
        }
        if lease.lease_epoch == 0 || lease.attempt == 0 || lease.lease_until_ms <= now_ms {
            return Err(SemanticCodeError::Refused(
                "semantic stage lease is absent, unissued, or expired".to_string(),
            ));
        }
        if lease.record.identity != *self.serving.identity()
            || lease.record.intent.topic != SEMANTIC_STAGE_INTENT_TOPIC
        {
            return Err(SemanticCodeError::Refused(
                "semantic stage lease is outside this serving owner".to_string(),
            ));
        }
        let intent = SemanticStageIntent::from_canonical_cbor(&lease.record.intent.payload)
            .map_err(semantic_contract_error)?;
        if intent.binding_id != self.binding {
            return Err(SemanticCodeError::Refused(
                "semantic stage lease names another binding".to_string(),
            ));
        }
        let scope_key = match &intent.scope {
            eg_types::semantic_index::SemanticStageScope::Entity { source_entity_id } => {
                source_entity_id.clone()
            }
            eg_types::semantic_index::SemanticStageScope::Generation => "generation".to_string(),
        };
        let expected_key = stage_intent_key(&intent);
        if lease.record.intent.key != expected_key {
            return Err(SemanticCodeError::Refused(
                "semantic stage lease key does not match its canonical intent".to_string(),
            ));
        }
        let headers = &lease.record.intent.headers;
        for (name, expected) in [
            (
                "schema",
                eg_types::semantic_index::SEMANTIC_STAGE_INTENT_SCHEMA.to_string(),
            ),
            ("binding_id", intent.binding_id.clone()),
            ("binding_digest", intent.binding_digest.to_string()),
            ("generation", intent.generation.to_string()),
            ("source_entity_id", scope_key),
            ("source_revision", intent.source_revision.clone()),
            ("stage", intent.stage.as_str().to_string()),
            ("intent_digest", intent.intent_digest.to_string()),
        ] {
            if headers.get(name).map(String::as_str) != Some(expected.as_str()) {
                return Err(SemanticCodeError::Refused(format!(
                    "semantic stage lease header {name} does not match its canonical intent"
                )));
            }
        }
        let read = self.serving_read()?;
        let binding = self.read_binding_in(&read)?.ok_or_else(|| {
            SemanticCodeError::Refused(
                "semantic stage lease has no durable binding authority".to_string(),
            )
        })?;
        if binding.binding_digest != intent.binding_digest
            || binding.generation != intent.generation
        {
            return Err(SemanticCodeError::Refused(
                "semantic stage lease binding generation proof is stale".to_string(),
            ));
        }
        if binding.durable_state != SemanticBindingState::Building {
            return Err(SemanticCodeError::Refused(
                "semantic stage lease requires a durable Building binding state".to_string(),
            ));
        }
        Ok(intent)
    }

    fn validate_stage_predecessor_in(
        &self,
        read: &ScopedRead<'_, SemanticIndexOwner>,
        intent: &SemanticStageIntent,
        reject_completed: bool,
    ) -> Result<(), SemanticCodeError> {
        match &intent.scope {
            eg_types::semantic_index::SemanticStageScope::Entity { source_entity_id } => {
                let raw = read
                    .open_owner_table(SEMANTIC_SOURCE_PROGRESS)
                    .map_err(kernel_error)?
                    .get((
                        self.tenant.as_str(),
                        self.binding.as_str(),
                        intent.generation,
                        source_entity_id.as_str(),
                    ))
                    .map_err(kernel_error)?
                    .map(|value| value.value().to_vec())
                    .ok_or_else(|| {
                        SemanticCodeError::Refused(
                            "semantic stage lease has no durable source-progress row".to_string(),
                        )
                    })?;
                let progress = SemanticSourceProgress::from_canonical_cbor(&raw)
                    .map_err(semantic_contract_error)?;
                if progress.binding_digest != intent.binding_digest
                    || progress.generation != intent.generation
                    || progress.source_entity_id != *source_entity_id
                    || progress.source_revision != intent.source_revision
                {
                    return Err(SemanticCodeError::Refused(
                        "semantic stage lease source-progress proof does not match intent"
                            .to_string(),
                    ));
                }
                if progress.superseded_by_revision.is_some() {
                    return Err(SemanticCodeError::Refused(
                        "semantic stage lease names a superseded source revision".to_string(),
                    ));
                }
                if reject_completed
                    && progress
                        .completed_stage
                        .is_some_and(|completed| completed >= intent.stage)
                {
                    return Err(SemanticCodeError::Refused(
                        "semantic stage lease is already completed".to_string(),
                    ));
                }
                self.validate_predecessor_proof_in(read, intent, &Some(&progress))?;
            }
            eg_types::semantic_index::SemanticStageScope::Generation => {
                if intent.stage != SemanticStage::ReconcileAndActivate {
                    return Err(SemanticCodeError::Refused(
                        "only S6 may use a generation-scoped lease".to_string(),
                    ));
                }
                self.validate_predecessor_proof_in(read, intent, &None)?;
            }
        }
        Ok(())
    }

    fn validate_predecessor_proof_in(
        &self,
        read: &ScopedRead<'_, SemanticIndexOwner>,
        intent: &SemanticStageIntent,
        progress: &Option<&SemanticSourceProgress>,
    ) -> Result<(), SemanticCodeError> {
        let predecessor = &intent.predecessor;
        match predecessor {
            SemanticStagePredecessor::None => {
                if progress.is_some_and(|progress| progress.completed_stage.is_some()) {
                    return Err(SemanticCodeError::Refused(
                        "S1 source progress already has a completed predecessor".to_string(),
                    ));
                }
            }
            SemanticStagePredecessor::EntityReceipt {
                stage,
                receipt_digest,
            } => {
                let Some(progress) = progress else {
                    return Err(SemanticCodeError::Refused(
                        "entity receipt requires entity source progress".to_string(),
                    ));
                };
                if progress.completed_stage != Some(*stage)
                    || progress.completed_receipt_digest != Some(*receipt_digest)
                {
                    return Err(SemanticCodeError::Refused(
                        "entity predecessor receipt is not the durable receipt".to_string(),
                    ));
                }
            }
            SemanticStagePredecessor::GenerationCheckpoint {
                stage,
                checkpoint_digest,
            } => {
                if !progress.is_some_and(|progress| progress.completed_stage == Some(*stage))
                    || !self.has_checkpoint_in(read, intent, *checkpoint_digest, *stage)?
                {
                    return Err(SemanticCodeError::Refused(
                        "generation predecessor checkpoint is absent or stale".to_string(),
                    ));
                }
            }
            SemanticStagePredecessor::GenerationCoverage { checkpoint } => {
                checkpoint
                    .require_complete()
                    .map_err(semantic_contract_error)?;
                if !progress
                    .is_some_and(|progress| progress.completed_stage == Some(SemanticStage::Vector))
                    || !self.has_checkpoint_in(
                        read,
                        intent,
                        checkpoint.checkpoint_digest,
                        SemanticStage::Vector,
                    )?
                {
                    return Err(SemanticCodeError::Refused(
                        "generation coverage checkpoint is absent or stale".to_string(),
                    ));
                }
            }
            SemanticStagePredecessor::Activation {
                lexical_checkpoint_digest,
                ann_checkpoint_digest,
            } => {
                if !self.has_checkpoint_in(
                    read,
                    intent,
                    *lexical_checkpoint_digest,
                    SemanticStage::LexicalIndex,
                )? || !self.has_checkpoint_in(
                    read,
                    intent,
                    *ann_checkpoint_digest,
                    SemanticStage::AnnIndex,
                )? {
                    return Err(SemanticCodeError::Refused(
                        "S6 activation checkpoints are absent, mixed, or stale".to_string(),
                    ));
                }
            }
        }
        Ok(())
    }

    fn has_checkpoint_in(
        &self,
        read: &ScopedRead<'_, SemanticIndexOwner>,
        intent: &SemanticStageIntent,
        digest: SemanticDigest,
        stage: SemanticStage,
    ) -> Result<bool, SemanticCodeError> {
        let checkpoints = read
            .open_owner_table(SEMANTIC_CHECKPOINTS)
            .map_err(kernel_error)?;
        let heads = read
            .open_owner_table(SEMANTIC_CHECKPOINT_HEADS)
            .map_err(kernel_error)?;
        let current = current_checkpoint_from_tables(
            &checkpoints,
            &heads,
            GenerationCoordinates {
                tenant: self.tenant.as_str(),
                binding: self.binding.as_str(),
                generation: intent.generation,
                binding_digest: intent.binding_digest,
                source_revision: &intent.source_revision,
            },
            stage,
        )?;
        Ok(current.is_some_and(|checkpoint| {
            checkpoint.checkpoint_digest == digest && checkpoint.require_complete().is_ok()
        }))
    }

    fn validate_six_checkpoint_in(
        &self,
        read: &ScopedRead<'_, SemanticIndexOwner>,
        checkpoint: &SemanticGenerationCheckpoint,
    ) -> Result<(), SemanticCodeError> {
        let checkpoints = read
            .open_owner_table(SEMANTIC_CHECKPOINTS)
            .map_err(kernel_error)?;
        let heads = read
            .open_owner_table(SEMANTIC_CHECKPOINT_HEADS)
            .map_err(kernel_error)?;
        validate_six_checkpoint_from_tables(
            &checkpoints,
            &heads,
            &read
                .open_owner_table(SEMANTIC_SOURCE_PROGRESS)
                .map_err(kernel_error)?,
            &read
                .open_owner_table(SEMANTIC_STAGES)
                .map_err(kernel_error)?,
            self.tenant.as_str(),
            self.binding.as_str(),
            checkpoint,
        )
    }

    fn validate_generation_manifests_in(
        &self,
        read: &ScopedRead<'_, SemanticIndexOwner>,
        binding: &SemanticBinding,
        intent: &SemanticStageIntent,
    ) -> Result<(), SemanticCodeError> {
        let lexical_raw = read
            .open_owner_table(SEMANTIC_LEXICAL)
            .map_err(kernel_error)?
            .get((
                self.tenant.as_str(),
                self.binding.as_str(),
                intent.generation,
            ))
            .map_err(kernel_error)?
            .map(|value| value.value().to_vec())
            .ok_or_else(|| {
                SemanticCodeError::Refused(
                    "S6 activation has no durable lexical index manifest".to_string(),
                )
            })?;
        let lexical = SemanticLexicalIndexManifest::from_canonical_cbor(&lexical_raw)
            .map_err(semantic_contract_error)?;
        lexical.validate().map_err(semantic_contract_error)?;
        if lexical.binding_id != binding.binding_id
            || lexical.binding_digest != binding.binding_digest
            || lexical.generation != binding.generation
            || lexical.source_revision != binding.source_revision
            || lexical.identity != binding.lexical_index_identity
        {
            return Err(SemanticCodeError::Refused(
                "S6 lexical manifest is not bound to the durable binding".to_string(),
            ));
        }

        let ann_raw = read
            .open_owner_table(SEMANTIC_ANN)
            .map_err(kernel_error)?
            .get((
                self.tenant.as_str(),
                self.binding.as_str(),
                intent.generation,
            ))
            .map_err(kernel_error)?
            .map(|value| value.value().to_vec())
            .ok_or_else(|| {
                SemanticCodeError::Refused(
                    "S6 activation has no durable ANN index manifest".to_string(),
                )
            })?;
        let ann = SemanticAnnIndexManifest::from_canonical_cbor(&ann_raw)
            .map_err(semantic_contract_error)?;
        ann.validate().map_err(semantic_contract_error)?;
        if ann.binding_id != binding.binding_id
            || ann.binding_digest != binding.binding_digest
            || ann.generation != binding.generation
            || ann.source_revision != binding.source_revision
            || ann.identity != binding.ann_index_identity
        {
            return Err(SemanticCodeError::Refused(
                "S6 ANN manifest is not bound to the durable binding".to_string(),
            ));
        }
        Ok(())
    }

    /// Install the one durable subscription used by semantic executors.  The
    /// outbox remains the shared queue; this method only declares the topic on
    /// this owner scope and is idempotent for the same consumer/topic pair.
    pub(crate) fn subscribe_stage_consumer(&self, consumer: &str) -> Result<(), SemanticCodeError> {
        self.mutations
            .outbox_subscribe(&self.serving, consumer, SEMANTIC_STAGE_INTENT_TOPIC)
            .map_err(kernel_error)
    }

    /// Bounded, non-blocking claim port for the existing durable outbox.  A
    /// scheduler owns the budget and tenant order; this adapter never loops or
    /// creates an in-memory queue.
    pub(crate) fn claim_stage_leases(
        &self,
        consumer: &str,
        budget: &mut OutboxClaimBudget,
    ) -> Result<OutboxClaimOutcome, SemanticCodeError> {
        if consumer.trim().is_empty() || budget.limit() > 256 {
            return Err(SemanticCodeError::Refused(
                "semantic stage claim is empty or exceeds the bounded consumer budget".to_string(),
            ));
        }
        let read = self.serving_read()?;
        let binding = self.read_binding_in(&read)?.ok_or_else(|| {
            SemanticCodeError::Refused(
                "semantic stage claim has no durable binding authority".to_string(),
            )
        })?;
        if binding.durable_state != SemanticBindingState::Building {
            return Err(SemanticCodeError::Refused(
                "semantic stage claim requires a durable Building binding state".to_string(),
            ));
        }
        self.mutations
            .outbox_claim(&self.serving, consumer, budget)
            .map_err(kernel_error)
    }

    pub(crate) fn stage_status(
        &self,
        consumer: &str,
        now_ms: u64,
    ) -> Result<eg_transaction::OutboxStatus, SemanticCodeError> {
        let read = self.serving_read()?;
        eg_transaction::outbox_status(&read, consumer, now_ms).map_err(kernel_error)
    }

    /// Acknowledge only a semantic stage topic lease.  The transaction kernel
    /// rechecks the durable epoch/expiry/record and advances the watermark in
    /// the same delivery transaction; fabricated or reclaimed leases therefore
    /// cannot be accepted by this port.
    pub(crate) fn ack_stage_lease(
        &self,
        lease: &MutationOutboxLease,
        now_ms: u64,
    ) -> Result<(), SemanticCodeError> {
        let _ = self.parse_stage_lease(lease, &lease.consumer, now_ms)?;
        self.mutations
            .outbox_ack(&self.serving, lease, now_ms)
            .map(|_| ())
            .map_err(kernel_error)
    }

    pub(crate) fn release_stage_lease(
        &self,
        lease: &MutationOutboxLease,
    ) -> Result<(), SemanticCodeError> {
        if lease.record.identity != *self.serving.identity()
            || lease.record.intent.topic != SEMANTIC_STAGE_INTENT_TOPIC
        {
            return Err(SemanticCodeError::Refused(
                "semantic stage release is outside this serving owner".to_string(),
            ));
        }
        self.mutations
            .outbox_release(&self.serving, lease)
            .map_err(kernel_error)
    }

    /// Record one terminal S1-S6 transition and enqueue its successor in the
    /// same semantic owner mutation.  The delivery acknowledgement follows the
    /// committed transition and is replay-safe: if a process dies between the
    /// two durable transactions, the next lease finds the byte-identical stage
    /// mutation and only retries the acknowledgement.
    pub(crate) fn complete_stage(
        &self,
        lease: &MutationOutboxLease,
        transition: &SemanticStageTransition,
        artifact: &SemanticStageArtifact,
        successor: Option<&SemanticStageIntent>,
        now_ms: u64,
    ) -> Result<SemanticMutationReceipt, SemanticCodeError> {
        self.complete_stage_inner(lease, transition, artifact, successor, None, now_ms)
    }

    /// Complete S3 or S5 with its canonical generation manifest. The
    /// checkpoint and manifest are persisted beside the stage transition and
    /// successor intent, so a replay cannot observe a completed stage without
    /// the artifact that its checkpoint names.
    pub(crate) fn complete_generation_stage(
        &self,
        lease: &MutationOutboxLease,
        transition: &SemanticStageTransition,
        artifact: &SemanticGenerationArtifact,
        successor: Option<&SemanticStageIntent>,
        now_ms: u64,
    ) -> Result<SemanticMutationReceipt, SemanticCodeError> {
        if !matches!(
            transition.intent.stage,
            SemanticStage::LexicalIndex | SemanticStage::AnnIndex
        ) {
            return Err(SemanticCodeError::Refused(
                "generation manifests are only completed by S3 or S5".to_string(),
            ));
        }
        let checkpoint = transition.generation_checkpoint.as_ref().ok_or_else(|| {
            SemanticCodeError::Refused(
                "generation manifest completion requires its exact checkpoint".to_string(),
            )
        })?;
        artifact
            .validate_against(&checkpoint.successor)
            .map_err(semantic_contract_error)?;
        let no_entity_artifact = SemanticStageArtifact::None;
        self.complete_stage_inner(
            lease,
            transition,
            &no_entity_artifact,
            successor,
            Some(artifact),
            now_ms,
        )
    }

    fn complete_stage_inner(
        &self,
        lease: &MutationOutboxLease,
        transition: &SemanticStageTransition,
        artifact: &SemanticStageArtifact,
        successor: Option<&SemanticStageIntent>,
        generation_artifact: Option<&SemanticGenerationArtifact>,
        now_ms: u64,
    ) -> Result<SemanticMutationReceipt, SemanticCodeError> {
        transition.validate().map_err(semantic_contract_error)?;
        if transition.intent.stage == SemanticStage::SourceCommit
            && transition.receipt.outcome == SemanticStageOutcome::Completed
            && transition.receipt.output_digest != transition.intent.input_digest
        {
            return Err(SemanticCodeError::Refused(
                "completed SQL source S1 output must equal its admitted input".to_string(),
            ));
        }
        if matches!(
            transition.receipt.outcome,
            SemanticStageOutcome::DeferredBackpressured
                | SemanticStageOutcome::ParkedAwaitingPredecessor
        ) {
            if successor.is_some() {
                return Err(SemanticCodeError::Refused(
                    "deferred semantic stage cannot publish a successor".to_string(),
                ));
            }
            // Backpressure and an unavailable predecessor are delivery
            // decisions, not terminal semantic transitions. Release the exact
            // lease so the existing durable outbox can claim it again; no
            // stage, checkpoint, receipt, or successor rows are written.
            self.release_stage_lease(lease)?;
            return Err(SemanticCodeError::Refused(
                "deferred semantic stage remains pending and unacknowledged".to_string(),
            ));
        }
        let mutation = SemanticIndexMutation::RecordStageTransition {
            transition: Box::new(transition.clone()),
            artifact: artifact.clone(),
        };
        mutation.validate().map_err(semantic_contract_error)?;
        let intent = self.parse_stage_lease(lease, &lease.consumer, now_ms)?;
        if intent != transition.intent {
            return Err(SemanticCodeError::Refused(
                "stage transition intent does not equal the leased canonical intent".to_string(),
            ));
        }
        let stored = self.read_stage_mutation(intent.intent_digest)?;
        if let Some(stored) = stored {
            if stored != mutation {
                return Err(SemanticCodeError::Refused(
                    "stage intent already has a different durable transition".to_string(),
                ));
            }
            // This path is specifically for a reclaimed lease after the stage
            // rows committed.  It still validates the supplied lease envelope;
            // outbox_ack performs the authoritative epoch/record fence.
            self.ack_stage_lease(lease, now_ms)?;
            return self
                .stage_receipt(transition, true)
                .map_err(|error| SemanticCodeError::Corrupt(error));
        }
        let read = self.serving_read()?;
        self.validate_stage_predecessor_in(&read, &intent, true)?;
        let successor = validate_successor_intent(transition, successor)?;
        let mutation_bytes = mutation
            .to_canonical_cbor()
            .map_err(semantic_contract_error)?;
        let mutation_digest = semantic_digest(&mutation_bytes);
        let transition_payload = transition
            .to_canonical_cbor()
            .map_err(semantic_contract_error)?;
        let mut outbox = vec![MutationOutboxIntent {
            topic: SEMANTIC_STAGE_RECEIPT_TOPIC.to_string(),
            key: transition.intent.intent_digest.to_string(),
            payload: transition_payload,
            headers: stage_receipt_headers(transition),
        }];
        if let Some(successor) = &successor {
            outbox.push(stage_intent_outbox(successor)?);
        }
        let batch_id = format!("semantic-index:stage:{}", transition.intent.intent_digest);
        let source_entity_id = transition
            .intent
            .scope
            .source_entity_id()
            .map(str::to_string);
        let binding = self.binding.clone();
        let tenant = self.tenant.clone();
        let transition_for_write = transition.clone();
        let mutation_for_write = mutation.clone();
        let now = now_ms;
        let receipt = self.commit_metadata_fenced(
            |version| {
                self.metadata_batch(
                    &self.serving,
                    version,
                    MetadataMutation {
                        batch_id: &batch_id,
                        event_type: "semantic_stage_transition_recorded",
                        subject: &format!("stage:{}", transition.intent.intent_digest),
                        mutation_digest,
                    },
                    outbox,
                    now,
                )
            },
            mutation_digest,
            now_ms,
            Some(lease),
            |write, rows| {
                // Re-read the predecessor rows through the admitted write. A
                // serving snapshot checked above is useful for early refusal,
                // but this is the decision that is serialized with the write.
                validate_stage_predecessor_write(
                    write,
                    rows,
                    &tenant,
                    &binding,
                    &transition_for_write.intent,
                    true,
                )?;
                let stage_key = transition_for_write.intent.intent_digest.to_string();
                if let Some(existing) = write
                    .open_read_table(SEMANTIC_STAGES)
                    .map_err(kernel_error)?
                    .get((tenant.as_str(), binding.as_str(), stage_key.as_str()))
                    .map_err(kernel_error)?
                    .map(|value| value.value().to_vec())
                {
                    let existing = SemanticIndexMutation::from_canonical_cbor(&existing)
                        .map_err(semantic_contract_error)?;
                    if existing != mutation_for_write {
                        return Err(SemanticCodeError::Refused(
                            "stage intent already has a different durable transition".to_string(),
                        ));
                    }
                    let receipt_key =
                        stage_receipt_index_key(transition_for_write.receipt.receipt_digest());
                    let mut stages = rows.open_table(SEMANTIC_STAGES).map_err(kernel_error)?;
                    put_bytes_once(
                        &mut stages,
                        (tenant.as_str(), binding.as_str(), receipt_key.as_str()),
                        &mutation_bytes,
                    )?;
                    return Ok(());
                }
                let mut stages = rows.open_table(SEMANTIC_STAGES).map_err(kernel_error)?;
                stages
                    .insert(
                        (tenant.as_str(), binding.as_str(), stage_key.as_str()),
                        mutation_bytes.as_slice(),
                    )
                    .map_err(kernel_error)?;
                let receipt_key =
                    stage_receipt_index_key(transition_for_write.receipt.receipt_digest());
                put_bytes_once(
                    &mut stages,
                    (tenant.as_str(), binding.as_str(), receipt_key.as_str()),
                    &mutation_bytes,
                )?;
                drop(stages);
                persist_transition_checkpoints(
                    &rows,
                    &tenant,
                    &binding,
                    &transition_for_write,
                    successor.as_ref(),
                )?;
                persist_stage_artifact_with_lease(
                    &rows,
                    &tenant,
                    &binding,
                    &transition_for_write,
                    Some(lease),
                    artifact,
                )?;
                if let Some(generation_artifact) = generation_artifact {
                    persist_generation_artifact(
                        &rows,
                        &tenant,
                        &binding,
                        &transition_for_write,
                        generation_artifact,
                    )?;
                }
                if let Some(source_entity_id) = &source_entity_id {
                    update_source_progress(
                        &rows,
                        &tenant,
                        &binding,
                        &transition_for_write,
                        source_entity_id,
                        now,
                    )?;
                }
                Ok(())
            },
        )?;
        Ok(receipt)
    }

    /// Finalize a generation-scoped S6 transition. The final checkpoint,
    /// canonical active pointer, binding state, ANN image and exact replay
    /// mutation are admitted through one serving-owner write. This is kept
    /// separate from `complete_stage` because S6 is a
    /// `SemanticIndexMutation::FinalizeGeneration`, not an entity artifact.
    pub(crate) fn finalize_generation(
        &self,
        lease: &MutationOutboxLease,
        transition: &SemanticStageTransition,
        checkpoint: &SemanticGenerationCheckpoint,
        artifact: &SemanticGenerationArtifact,
        image: &SemanticGenerationImage,
        now_ms: u64,
    ) -> Result<SemanticMutationReceipt, SemanticCodeError> {
        transition.validate().map_err(semantic_contract_error)?;
        if transition.intent.stage != SemanticStage::ReconcileAndActivate
            || !matches!(
                transition.intent.scope,
                eg_types::semantic_index::SemanticStageScope::Generation
            )
            || transition.receipt.outcome != SemanticStageOutcome::Completed
        {
            return Err(SemanticCodeError::Refused(
                "generation finalization requires a completed generation-scoped S6 transition"
                    .to_string(),
            ));
        }
        checkpoint
            .require_complete()
            .map_err(semantic_contract_error)?;
        if checkpoint.binding_id != transition.intent.binding_id
            || checkpoint.binding_digest != transition.intent.binding_digest
            || checkpoint.generation != transition.intent.generation
            || checkpoint.source_revision != transition.intent.source_revision
            || checkpoint.stage != SemanticStage::ReconcileAndActivate
        {
            return Err(SemanticCodeError::Refused(
                "S6 checkpoint does not match its leased transition".to_string(),
            ));
        }
        let mutation = SemanticIndexMutation::FinalizeGeneration {
            checkpoint: Box::new(checkpoint.clone()),
            artifact: artifact.clone(),
        };
        mutation.validate().map_err(semantic_contract_error)?;
        let intent = self.parse_stage_lease(lease, &lease.consumer, now_ms)?;
        if intent != transition.intent {
            return Err(SemanticCodeError::Refused(
                "S6 transition intent does not equal the leased canonical intent".to_string(),
            ));
        }
        let binding = self.read_binding()?.ok_or_else(|| {
            SemanticCodeError::Refused(
                "S6 publication requires a durable semantic binding".to_string(),
            )
        })?;
        if binding.binding_digest != transition.intent.binding_digest
            || binding.generation != transition.intent.generation
        {
            return Err(SemanticCodeError::Refused(
                "S6 binding identity is stale".to_string(),
            ));
        }
        let (dimensions, model_digest) = image.identity()?;
        if binding.dimension as usize != dimensions
            || Some(binding.model_digest.as_str()) != model_digest.as_deref()
        {
            return Err(SemanticCodeError::Refused(
                "S6 ANN image does not match the durable binding model".to_string(),
            ));
        }
        let SemanticGenerationArtifact::Activation { target, pointer } = artifact else {
            return Err(SemanticCodeError::Refused(
                "S6 publication requires the canonical activation artifact".to_string(),
            ));
        };
        target
            .validate_against_binding(&binding)
            .map_err(semantic_contract_error)?;
        pointer.validate().map_err(semantic_contract_error)?;
        if pointer.binding_id != binding.binding_id
            || pointer.binding_digest != binding.binding_digest
            || pointer.generation != binding.generation
            || pointer.source_revision != binding.source_revision
            || pointer.activation_receipt_digest != checkpoint.checkpoint_digest
            || pointer.activated_at != checkpoint.completed_at
        {
            return Err(SemanticCodeError::Refused(
                "S6 active pointer is not bound to the current checkpoint".to_string(),
            ));
        }
        // Validate the durable predecessor, generation manifests, and S6
        // checkpoint before taking the replay shortcut. A stage row without
        // these canonical proofs is incomplete and cannot be acknowledged
        // merely because its mutation bytes happen to decode.
        let read = self.serving_read()?;
        self.validate_stage_predecessor_in(&read, &intent, true)?;
        self.validate_generation_manifests_in(&read, &binding, &intent)?;
        self.validate_six_checkpoint_in(&read, checkpoint)?;
        if let Some(stored) = self.read_stage_mutation(intent.intent_digest)? {
            if stored != mutation {
                return Err(SemanticCodeError::Refused(
                    "S6 intent already has a different durable finalization".to_string(),
                ));
            }
            if self.live_generation_in(&read)? != Some(intent.generation) {
                return Err(SemanticCodeError::Refused(
                    "S6 replay has no matching durable live pointer".to_string(),
                ));
            }
            self.ack_stage_lease(lease, now_ms)?;
            let batch_id = format!("semantic-index:finalize:{}", intent.intent_digest);
            return self
                .stage_receipt_for_batch(&batch_id, true)
                .map_err(SemanticCodeError::Corrupt);
        }
        let mutation_bytes = mutation
            .to_canonical_cbor()
            .map_err(semantic_contract_error)?;
        let mutation_digest = semantic_digest(&mutation_bytes);
        let batch_id = format!("semantic-index:finalize:{}", intent.intent_digest);
        let transition_payload = transition
            .to_canonical_cbor()
            .map_err(semantic_contract_error)?;
        let outbox = vec![MutationOutboxIntent {
            topic: SEMANTIC_STAGE_RECEIPT_TOPIC.to_string(),
            key: intent.intent_digest.to_string(),
            payload: transition_payload,
            headers: stage_receipt_headers(transition),
        }];
        let tenant = self.tenant.clone();
        let binding_id = self.binding.clone();
        let transition_for_write = transition.clone();
        let mutation_for_write = mutation.clone();
        let checkpoint_for_write = checkpoint.clone();
        let pointer_for_write = pointer.clone();
        let image_for_write = image.clone();
        let receipt = self.commit_metadata_fenced(
            |version| {
                self.metadata_batch(
                         &self.serving,
                         version,
                         MetadataMutation {
                             batch_id: &batch_id,
                             event_type: "semantic_generation_finalized",
                             subject: &format!("generation:{}", intent.generation),
                             mutation_digest,
                         },
                         outbox,
                         now_ms,
                     )
            },
            mutation_digest,
            now_ms,
            Some(lease),
            |write, rows| {
                validate_stage_predecessor_write(
                    write,
                    rows,
                    &tenant,
                    &binding_id,
                    &transition_for_write.intent,
                    true,
                )?;
                validate_six_checkpoint_write(rows, &tenant, &binding_id, &checkpoint_for_write)?;
                let stage_key = transition_for_write.intent.intent_digest.to_string();
                if let Some(existing) = write
                    .open_read_table(SEMANTIC_STAGES)
                    .map_err(kernel_error)?
                    .get((tenant.as_str(), binding_id.as_str(), stage_key.as_str()))
                    .map_err(kernel_error)?
                    .map(|value| value.value().to_vec())
                {
                    let existing = SemanticIndexMutation::from_canonical_cbor(&existing)
                        .map_err(semantic_contract_error)?;
                    if existing != mutation_for_write {
                        return Err(SemanticCodeError::Refused(
                            "S6 intent already has a different durable finalization".to_string(),
                        ));
                    }
                    let receipt_key = stage_receipt_index_key(
                        transition_for_write.receipt.receipt_digest(),
                    );
                    let mut stages = rows.open_table(SEMANTIC_STAGES).map_err(kernel_error)?;
                    put_bytes_once(
                        &mut stages,
                        (tenant.as_str(), binding_id.as_str(), receipt_key.as_str()),
                        &mutation_bytes,
                    )?;
                    return Ok(());
                }
                let mut stages = rows.open_table(SEMANTIC_STAGES).map_err(kernel_error)?;
                stages
                    .insert(
                        (tenant.as_str(), binding_id.as_str(), stage_key.as_str()),
                        mutation_bytes.as_slice(),
                    )
                    .map_err(kernel_error)?;
                let receipt_key =
                    stage_receipt_index_key(transition_for_write.receipt.receipt_digest());
                put_bytes_once(
                    &mut stages,
                    (tenant.as_str(), binding_id.as_str(), receipt_key.as_str()),
                    &mutation_bytes,
                )?;
                drop(stages);
                let checkpoint_key = checkpoint_for_write.checkpoint_digest.to_string();
                let checkpoint_bytes = checkpoint_for_write
                    .to_canonical_cbor()
                    .map_err(semantic_contract_error)?;
                let mut checkpoints = rows
                    .open_table(SEMANTIC_CHECKPOINTS)
                    .map_err(kernel_error)?;
                put_bytes_once(
                    &mut checkpoints,
                    (
                        tenant.as_str(),
                        binding_id.as_str(),
                        checkpoint_for_write.generation,
                        checkpoint_key.as_str(),
                    ),
                    &checkpoint_bytes,
                )?;
                drop(checkpoints);
                advance_checkpoint_head(
                    rows,
                    &tenant,
                    &binding_id,
                    &checkpoint_for_write,
                    None,
                )?;

                let current = self.read_binding_in_write(write)?.ok_or_else(|| {
                    SemanticCodeError::Refused(
                        "S6 binding disappeared during admitted finalization".to_string(),
                    )
                })?;
                if current.binding_digest != transition_for_write.intent.binding_digest
                    || current.generation != transition_for_write.intent.generation
                {
                    return Err(SemanticCodeError::Refused(
                        "S6 binding changed during admitted finalization".to_string(),
                    ));
                }
                if current.durable_state == SemanticBindingState::Dropping
                    || current.durable_state == SemanticBindingState::Disabled
                    || current.durable_state == SemanticBindingState::Failed
                {
                    return Err(SemanticCodeError::Refused(
                        "S6 cannot publish a disabled, dropping, or failed binding".to_string(),
                    ));
                }
                let prior_live_generation = write
                    .open_read_table(SEMANTIC_POINTERS)
                    .map_err(kernel_error)?
                    .get((tenant.as_str(), binding_id.as_str()))
                    .map_err(kernel_error)?
                    .map(|value| {
                        SemanticActivePointer::from_canonical_cbor(value.value())
                            .map_err(semantic_contract_error)
                    })
                    .transpose()?
                    .map(|pointer| pointer.generation);
                if let Some(prior_live_generation) = prior_live_generation {
                    if prior_live_generation > current.generation {
                        return Err(SemanticCodeError::Corrupt(
                            "S6 active pointer names a future generation".to_string(),
                        ));
                    }
                    if prior_live_generation < current.generation {
                        demote_prior_live_binding_in_write(
                            rows,
                            &tenant,
                            &binding_id,
                            prior_live_generation,
                        )?;
                    }
                }
                let (state_transition, updated) = match current.durable_state {
                    SemanticBindingState::Pending => {
                        return Err(SemanticCodeError::Refused(
                            "S6 requires a durable Pending-to-Building transition before publication"
                                .to_string(),
                        ));
                    }
                    SemanticBindingState::Building => {
                        let live = SemanticBindingStateTransition::create(
                            &current,
                            SemanticBindingState::Live,
                            "semantic_generation_activated",
                        )
                        .map_err(semantic_contract_error)?;
                        let updated = current
                            .apply_state_transition(&live)
                            .map_err(semantic_contract_error)?;
                        (live, updated)
                    }
                    SemanticBindingState::Live => {
                        return Err(SemanticCodeError::Refused(
                            "S6 binding is already live without an exact replay record".to_string(),
                        ));
                    }
                    SemanticBindingState::Disabled
                    | SemanticBindingState::Dropping
                    | SemanticBindingState::Failed => unreachable!(),
                };
                let state_bytes = state_transition
                    .to_canonical_cbor()
                    .map_err(semantic_contract_error)?;
                let mut states = rows.open_table(SEMANTIC_STATES).map_err(kernel_error)?;
                replace_bytes(
                    &mut states,
                    (tenant.as_str(), binding_id.as_str()),
                    &state_bytes,
                )?;
                drop(states);
                let binding_bytes = updated
                    .to_canonical_cbor()
                    .map_err(semantic_contract_error)?;
                let mut bindings = rows.open_table(SEMANTIC_BINDINGS).map_err(kernel_error)?;
                replace_bytes(
                    &mut bindings,
                    (tenant.as_str(), binding_id.as_str(), updated.generation),
                    &binding_bytes,
                )?;
                drop(bindings);

                let pointer_bytes = pointer_for_write
                    .to_canonical_cbor()
                    .map_err(semantic_contract_error)?;
                let mut pointers = rows.open_table(SEMANTIC_POINTERS).map_err(kernel_error)?;
                replace_bytes(
                    &mut pointers,
                    (tenant.as_str(), binding_id.as_str()),
                    &pointer_bytes,
                )?;
                drop(pointers);

                let digest = image_digest(&image_for_write);
                let (image_dimensions, image_model) = image_for_write.identity()?;
                if image_dimensions != dimensions || image_model != model_digest {
                    return Err(SemanticCodeError::Refused(
                        "S6 image identity changed during finalization".to_string(),
                    ));
                }
                let mut codes = BoundCodeRows::new(
                    rows.open_table(ANN_CODES).map_err(kernel_error)?,
                    &tenant,
                    &binding_id,
                    transition_for_write.intent.generation,
                );
                for (part, bytes) in [
                    ("meta", image_for_write.index.codes.meta.as_slice()),
                    ("codes", image_for_write.index.codes.codes.as_slice()),
                    ("refine", image_for_write.index.codes.refine.as_slice()),
                    ("ids", image_for_write.index.ids.as_slice()),
                    ("manifest", image_for_write.manifest.as_slice()),
                ] {
                    codes.put_part(part, bytes)?;
                }
                codes.insert(
                    (
                        &tenant,
                        &binding_id,
                        transition_for_write.intent.generation,
                        DIGEST_PART,
                    ),
                    digest.as_bytes(),
                )?;
                drop(codes);

                Ok(())
            },
        )?;
        Ok(receipt)
    }

    fn read_stage_mutation(
        &self,
        intent_digest: SemanticDigest,
    ) -> Result<Option<SemanticIndexMutation>, SemanticCodeError> {
        let key = intent_digest.to_string();
        let read = self.serving_read()?;
        let raw = read
            .open_owner_table(SEMANTIC_STAGES)
            .map_err(kernel_error)?
            .get((self.tenant.as_str(), self.binding.as_str(), key.as_str()))
            .map_err(kernel_error)?
            .map(|value| value.value().to_vec());
        raw.map(|bytes| {
            SemanticIndexMutation::from_canonical_cbor(&bytes).map_err(semantic_contract_error)
        })
        .transpose()
    }

    /// Reconcile a reclaimed SQL S1 lease against the already committed
    /// source transition before the caller performs another source or ACL
    /// read.  The stage row and its receipt-index row are the canonical
    /// retained evidence; both must contain the same bytes, and the mutation
    /// must carry a complete SQL manifest plus authorization receipt for the
    /// leased S1 intent.  The caller supplies only that durable intent:
    /// cursor, completion time and receipt digest are server-owned and are
    /// read from the retained transition.  A missing stage row means the
    /// first attempt did not commit and leaves the caller free to perform
    /// fresh source work.
    ///
    /// Lease parsing is deliberately done before the read and ACK repeats the
    /// authoritative outbox fence afterward.  Thus a fabricated, expired,
    /// reclaimed, wrong-consumer, or wrong-owner lease cannot acknowledge a
    /// retained result, while a changed client intent conflicts before any
    /// delivery cursor is advanced.
    pub(crate) fn replay_completed_sql_source_stage(
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
        let intent = self.parse_stage_lease(lease, &lease.consumer, now_ms)?;
        if intent != *expected_intent {
            return Err(SemanticCodeError::Refused(
                "semantic SQL source replay intent does not equal the leased intent".to_string(),
            ));
        }

        let stage_key = intent.intent_digest.to_string();
        let read = self.serving_read()?;
        let stages = read
            .open_owner_table(SEMANTIC_STAGES)
            .map_err(kernel_error)?;
        let Some(raw) = stages
            .get((
                self.tenant.as_str(),
                self.binding.as_str(),
                stage_key.as_str(),
            ))
            .map_err(kernel_error)?
            .map(|value| value.value().to_vec())
        else {
            return Ok(None);
        };
        let mutation =
            SemanticIndexMutation::from_canonical_cbor(&raw).map_err(semantic_contract_error)?;
        mutation.validate().map_err(semantic_contract_error)?;
        let SemanticIndexMutation::RecordStageTransition {
            transition: stored_transition,
            artifact,
        } = &mutation
        else {
            return Err(SemanticCodeError::Refused(
                "semantic SQL source replay retained a non-stage mutation".to_string(),
            ));
        };
        if stored_transition.intent != *expected_intent {
            return Err(SemanticCodeError::Refused(
                "semantic SQL source replay retained a different intent".to_string(),
            ));
        }
        if stored_transition.receipt.outcome != SemanticStageOutcome::Completed {
            return Err(SemanticCodeError::Refused(
                "semantic SQL source replay retained a non-completed S1 result".to_string(),
            ));
        }
        if stored_transition.receipt.output_digest != expected_intent.input_digest {
            return Err(SemanticCodeError::Refused(
                "semantic SQL source replay output does not equal the leased source input"
                    .to_string(),
            ));
        }
        if !matches!(&artifact, SemanticStageArtifact::SqlSourceManifest { .. }) {
            return Err(SemanticCodeError::Refused(
                "semantic SQL source replay retained no SQL source manifest".to_string(),
            ));
        }
        artifact
            .validate_against(stored_transition)
            .map_err(semantic_contract_error)?;
        let receipt_key = stage_receipt_index_key(stored_transition.receipt.receipt_digest());
        let indexed = stages
            .get((
                self.tenant.as_str(),
                self.binding.as_str(),
                receipt_key.as_str(),
            ))
            .map_err(kernel_error)?
            .map(|value| value.value().to_vec())
            .ok_or_else(|| {
                SemanticCodeError::Refused(
                    "semantic SQL source replay has no retained receipt index".to_string(),
                )
            })?;
        if indexed != raw {
            return Err(SemanticCodeError::Refused(
                "semantic SQL source replay receipt index differs from the retained stage"
                    .to_string(),
            ));
        }
        drop(stages);
        drop(read);

        let batch_id = format!("semantic-index:stage:{}", intent.intent_digest);
        let receipt = self
            .stage_receipt_for_batch(&batch_id, true)
            .map_err(SemanticCodeError::Corrupt)?;
        let retained_digest = semantic_digest(&raw);
        if receipt.mutation_digest != retained_digest {
            return Err(SemanticCodeError::Corrupt(
                "semantic SQL source replay ledger digest differs from the retained stage"
                    .to_string(),
            ));
        }
        self.ack_stage_lease(lease, now_ms)?;
        // `stored_transition` binds a `&Box<_>` from the retained mutation, so
        // clone THROUGH the box: this returns the transition itself, which is
        // what the caller's signature promises.
        Ok(Some(((**stored_transition).clone(), receipt)))
    }

    /// Test-only readback for restart proofs.  The production replay route
    /// above returns the retained transition and durable receipt; tests that
    /// verify the retained authorization time use this narrow artifact accessor instead of
    /// reaching into the semantic owner's tables or kernel fields.
    #[cfg(test)]
    pub(crate) fn recorded_sql_source_artifact(
        &self,
        intent_digest: SemanticDigest,
    ) -> Result<Option<SemanticStageArtifact>, SemanticCodeError> {
        let Some(mutation) = self.read_stage_mutation(intent_digest)? else {
            return Ok(None);
        };
        let SemanticIndexMutation::RecordStageTransition {
            transition,
            artifact,
        } = mutation
        else {
            return Err(SemanticCodeError::Refused(
                "recorded SQL source artifact names a non-stage mutation".to_string(),
            ));
        };
        if transition.intent.stage != SemanticStage::SourceCommit {
            return Err(SemanticCodeError::Refused(
                "recorded SQL source artifact names a non-S1 stage".to_string(),
            ));
        }
        if !matches!(&artifact, SemanticStageArtifact::SqlSourceManifest { .. }) {
            return Err(SemanticCodeError::Refused(
                "recorded SQL source artifact is not a SQL manifest".to_string(),
            ));
        }
        artifact
            .validate_against(&transition)
            .map_err(semantic_contract_error)?;
        Ok(Some(artifact))
    }

    fn stage_receipt(
        &self,
        transition: &SemanticStageTransition,
        replayed: bool,
    ) -> Result<SemanticMutationReceipt, String> {
        let batch_id = format!("semantic-index:stage:{}", transition.intent.intent_digest);
        self.stage_receipt_for_batch(&batch_id, replayed)
    }

    fn stage_receipt_for_batch(
        &self,
        batch_id: &str,
        replayed: bool,
    ) -> Result<SemanticMutationReceipt, String> {
        let read = self.serving_read().map_err(|error| error.to_string())?;
        let record = eg_transaction::read_ledger(&read, batch_id)?
            .ok_or_else(|| "stage replay has no durable ledger receipt".to_string())?;
        let mutation_digest = mutation_digest_from_batch(&record.batch)
            .map_err(|error| format!("stage replay has no mutation digest receipt: {error}"))?;
        let (source_version, target_version) = match record.committed_version {
            eg_types::mutation_batch::CommittedVersion::Native { source, target } => {
                (source, target)
            }
            other => return Err(format!("stage replay has non-native version {other:?}")),
        };
        Ok(SemanticMutationReceipt {
            batch_id: batch_id.to_string(),
            mutation_digest,
            source_version,
            target_version,
            replayed,
        })
    }

    /// Persist one generation's image and make it live, as ONE admitted
    /// maintenance mutation.
    ///
    /// Deliberately closed. Publication is the admitted S6 activation
    /// transition (`finalize_generation`), which writes the generation's code
    /// rows, its `semantic_bindings` authority and its active pointer in ONE
    /// mutation on the serving scope. A direct publication door would be a
    /// second writer of "which generation is live".
    pub fn activate(
        &self,
        _generation: u64,
        _image: &SemanticGenerationImage,
    ) -> Result<(), SemanticCodeError> {
        Err(SemanticCodeError::Refused(
            "direct ANN publication is disabled; use the admitted S6 activation transition"
                .to_string(),
        ))
    }

    /// The live generation's image, or `None` when this binding has none.
    ///
    /// This is the serving read, and it serves ONLY the live generation: a
    /// generation that has been superseded or retired is not reachable here.
    pub fn read_live(&self) -> Result<Option<(u64, SemanticGenerationImage)>, SemanticCodeError> {
        let read = self.serving_read()?;
        let Some(generation) = self.live_generation_in(&read)? else {
            return Ok(None);
        };
        Ok(self
            .read_generation_in(&read, generation)?
            .map(|image| (generation, image)))
    }

    /// One generation's image whether or not it is live -- the MAINTENANCE
    /// read, for building, verifying and retiring a generation. Writes nothing:
    /// an unactivated or retired generation is simply `None`.
    pub fn read_generation(
        &self,
        generation: u64,
    ) -> Result<Option<SemanticGenerationImage>, SemanticCodeError> {
        let read = self.serving_read()?;
        self.read_generation_in(&read, generation)
    }

    /// The live generation number, if any.
    pub fn live_generation(&self) -> Result<Option<u64>, SemanticCodeError> {
        let read = self.serving_read()?;
        self.live_generation_in(&read)
    }

    /// Legacy direct retirement is deliberately closed. Retirement must be
    /// admitted by the binding lifecycle path so a pending delete/tombstone
    /// proof and the active pointer change share one mutation.
    pub fn retire(&self, _generation: u64) -> Result<(), SemanticCodeError> {
        Err(SemanticCodeError::Refused(
            "direct ANN retirement is disabled; use the admitted binding lifecycle transition"
                .to_string(),
        ))
    }

    fn read_binding_in(
        &self,
        read: &ScopedRead<'_, SemanticIndexOwner>,
    ) -> Result<Option<SemanticBinding>, SemanticCodeError> {
        let generation = read
            .open_owner_table(SEMANTIC_HEADS)
            .map_err(kernel_error)?
            .get((self.tenant.as_str(), self.binding.as_str()))
            .map_err(kernel_error)?
            .map(|value| value.value());
        let Some(generation) = generation else {
            return Ok(None);
        };
        self.read_binding_generation_in(read, generation)
    }

    /// Read one historical binding row from the serving snapshot.  The head
    /// may have advanced during a refresh while the active pointer still
    /// intentionally names the previous live generation.
    fn read_binding_generation_in(
        &self,
        read: &ScopedRead<'_, SemanticIndexOwner>,
        generation: u64,
    ) -> Result<Option<SemanticBinding>, SemanticCodeError> {
        let raw = read
            .open_owner_table(SEMANTIC_BINDINGS)
            .map_err(kernel_error)?
            .get((self.tenant.as_str(), self.binding.as_str(), generation))
            .map_err(kernel_error)?
            .map(|value| value.value().to_vec())
            .ok_or_else(|| {
                SemanticCodeError::Corrupt(
                    "semantic binding head names a missing binding row".to_string(),
                )
            })?;
        let binding =
            SemanticBinding::from_canonical_cbor(&raw).map_err(semantic_contract_error)?;
        if binding.tenant_id != self.tenant
            || binding.binding_id != self.binding
            || binding.generation != generation
        {
            return Err(SemanticCodeError::Corrupt(
                "semantic binding row does not match its serving head".to_string(),
            ));
        }
        Ok(Some(binding))
    }

    fn read_binding_in_write(
        &self,
        write: &AdmittedMutation<'_, SemanticIndexOwner>,
    ) -> Result<Option<SemanticBinding>, SemanticCodeError> {
        let generation = write
            .open_read_table(SEMANTIC_HEADS)
            .map_err(kernel_error)?
            .get((self.tenant.as_str(), self.binding.as_str()))
            .map_err(kernel_error)?
            .map(|value| value.value());
        let Some(generation) = generation else {
            return Ok(None);
        };
        let raw = write
            .open_read_table(SEMANTIC_BINDINGS)
            .map_err(kernel_error)?
            .get((self.tenant.as_str(), self.binding.as_str(), generation))
            .map_err(kernel_error)?
            .map(|value| value.value().to_vec())
            .ok_or_else(|| {
                SemanticCodeError::Corrupt(
                    "semantic binding head names a missing binding row".to_string(),
                )
            })?;
        let binding =
            SemanticBinding::from_canonical_cbor(&raw).map_err(semantic_contract_error)?;
        if binding.tenant_id != self.tenant
            || binding.binding_id != self.binding
            || binding.generation != generation
        {
            return Err(SemanticCodeError::Corrupt(
                "semantic binding row does not match its serving head".to_string(),
            ));
        }
        Ok(Some(binding))
    }

    /// Resolve a caller operation's durable replay row before any lifecycle
    /// state/OCC checks. A retry can legitimately observe the state produced by
    /// its first attempt (Pending -> Building, Live -> Disabled, or a moved
    /// refresh head), so reading that state first would incorrectly reject a
    /// valid fresh-nonce replay. The original committed batch supplies the
    /// stable content and the new envelope supplies only the attempt facts;
    /// kernel admission then enforces actor/key/content conflict and consumes
    /// the fresh nonce without reapplying owner rows.
    fn replay_operation_if_recorded<F>(
        &self,
        actor: &str,
        idempotency_key: &str,
        nonce: Nonce,
        now_ms: u64,
        validate_content: F,
    ) -> Result<Option<SemanticMutationReceipt>, SemanticCodeError>
    where
        F: FnOnce(&MutationBatch) -> Result<(), SemanticCodeError>,
    {
        let read = self.serving_read()?;
        let Some(operation) =
            eg_transaction::read_replay_operation(&read, idempotency_key).map_err(kernel_error)?
        else {
            return Ok(None);
        };
        let batch_id = match &operation.recorded {
            RecordedOperation::Batch(batch_id) => batch_id.clone(),
            RecordedOperation::Receipt(_) => {
                return Err(SemanticCodeError::Corrupt(
                    "semantic lifecycle replay row has no durable batch".to_string(),
                ));
            }
        };
        let record = eg_transaction::read_ledger(&read, &batch_id)
            .map_err(kernel_error)?
            .ok_or_else(|| {
                SemanticCodeError::Corrupt(
                    "semantic lifecycle replay row names a missing batch".to_string(),
                )
            })?;
        validate_content(&record.batch)?;
        let old_operation = record.batch.envelope.operation().ok_or_else(|| {
            SemanticCodeError::Corrupt(
                "semantic lifecycle replay batch is not caller-attributed".to_string(),
            )
        })?;
        let compiled = CompiledOperation {
            method: old_operation.method.clone(),
            method_schema_id: old_operation.method_schema_id.clone(),
            method_schema_digest: old_operation.method_schema_digest,
            canonical_payload_digest: old_operation.canonical_payload_digest,
        };
        let mut replay_batch = record.batch.clone();
        replay_batch.envelope = MutationEnvelope::for_scope(
            CompiledScope {
                identity: &replay_batch.identity,
                actor,
                serving_principal: self.serving.principal(),
                request_id: now_ms,
                idempotency_key,
                nonce,
                now_ms,
            },
            compiled,
        )
        .map_err(SemanticCodeError::Corrupt)?;
        replay_batch.created_at_ms = now_ms;
        drop(read);

        let owner = &self.serving;
        let (write, batch, begun) = self
            .mutations
            .admit_current(owner, move |version| {
                replay_batch.version_expectation = VersionExpectation::Native(version);
                Ok(replay_batch)
            })
            .map_err(kernel_error)?;
        match begun {
            Begin::Replay(record) => {
                self.mutations.commit(write, &batch).map_err(kernel_error)?;
                self.stage_receipt_for_batch(&record.batch.batch_id, true)
                    .map(Some)
                    .map_err(SemanticCodeError::Corrupt)
            }
            Begin::Apply { .. } => {
                let _ = write.abort();
                Err(SemanticCodeError::Corrupt(
                    "semantic lifecycle replay row was not admitted as a replay".to_string(),
                ))
            }
        }
    }

    fn commit_metadata<B, F>(
        &self,
        build: B,
        mutation_digest: SemanticDigest,
        now_ms: u64,
        apply: F,
    ) -> Result<SemanticMutationReceipt, SemanticCodeError>
    where
        B: FnOnce(u64) -> Result<MutationBatch, String>,
        F: FnOnce(
            &AdmittedMutation<'_, SemanticIndexOwner>,
            &AdmittedOwnerWrite<'_, SemanticIndexOwner>,
        ) -> Result<(), SemanticCodeError>,
    {
        self.commit_metadata_fenced(build, mutation_digest, now_ms, None, apply)
    }

    /// Commit semantic owner rows and, for an outbox consumer transition, the
    /// delivery acknowledgement in one admitted redb mutation.  The lease is
    /// validated before the owner-write gate opens and acknowledged only after
    /// the owner rows plus the exact ledger receipt/successor have been
    /// written.  A stale lease therefore aborts every effect in this method.
    fn commit_metadata_fenced<B, F>(
        &self,
        build: B,
        mutation_digest: SemanticDigest,
        now_ms: u64,
        lease: Option<&MutationOutboxLease>,
        apply: F,
    ) -> Result<SemanticMutationReceipt, SemanticCodeError>
    where
        B: FnOnce(u64) -> Result<MutationBatch, String>,
        F: FnOnce(
            &AdmittedMutation<'_, SemanticIndexOwner>,
            &AdmittedOwnerWrite<'_, SemanticIndexOwner>,
        ) -> Result<(), SemanticCodeError>,
    {
        let owner = &self.serving;
        let (write, batch, begun) = self
            .mutations
            .admit_current(owner, build)
            .map_err(kernel_error)?;
        match begun {
            Begin::Replay(record) => {
                let committed = record.committed_version;
                let durable_mutation_digest = mutation_digest_from_batch(&record.batch)
                    .map_err(SemanticCodeError::Corrupt)?;
                if let Some(lease) = lease {
                    if let Err(error) = self
                        .mutations
                        .outbox_ack_in(&write, owner, lease, now_ms)
                        .map_err(kernel_error)
                    {
                        let _ = write.abort();
                        return Err(error);
                    }
                }
                // A replay still has a fresh attempt nonce to consume.  The
                // kernel's replay seal records that nonce only when this
                // admitted write commits; aborting here made a successful
                // replay indistinguishable from a probe and allowed reuse.
                self.mutations.commit(write, &batch).map_err(kernel_error)?;
                let (source_version, target_version) = match committed {
                    eg_types::mutation_batch::CommittedVersion::Native { source, target } => {
                        (source, target)
                    }
                    other => {
                        return Err(SemanticCodeError::Corrupt(format!(
                            "semantic metadata replay has non-native committed version {other:?}"
                        )));
                    }
                };
                Ok(SemanticMutationReceipt {
                    batch_id: batch.batch_id,
                    mutation_digest: durable_mutation_digest,
                    source_version,
                    target_version,
                    replayed: true,
                })
            }
            Begin::Apply { source_version } => {
                if let Some(lease) = lease {
                    if let Err(error) = self
                        .mutations
                        .outbox_validate_in(&write, owner, lease, now_ms)
                        .map_err(kernel_error)
                    {
                        let _ = write.abort();
                        return Err(error);
                    }
                }
                let owner_write = write.owner_rows(owner, &batch).map_err(kernel_error)?;
                let applied = apply(&write, &owner_write);
                owner_write.finish_owner().map_err(kernel_error)?;
                if let Err(error) = applied {
                    write.abort().map_err(kernel_error)?;
                    return Err(error);
                }
                let record = self
                    .mutations
                    .finish(&write, &batch, None, now_ms, source_version)
                    .map_err(kernel_error)?;
                if let Some(lease) = lease {
                    if let Err(error) = self
                        .mutations
                        .outbox_ack_in(&write, owner, lease, now_ms)
                        .map_err(kernel_error)
                    {
                        let _ = write.abort();
                        return Err(error);
                    }
                }
                self.mutations.commit(write, &batch).map_err(kernel_error)?;
                let (source_version, target_version) = match record.committed_version {
                    eg_types::mutation_batch::CommittedVersion::Native { source, target } => {
                        (source, target)
                    }
                    other => {
                        return Err(SemanticCodeError::Corrupt(format!(
                            "semantic metadata commit has non-native committed version {other:?}"
                        )));
                    }
                };
                Ok(SemanticMutationReceipt {
                    batch_id: batch.batch_id,
                    mutation_digest,
                    source_version,
                    target_version,
                    replayed: false,
                })
            }
        }
    }

    fn metadata_batch(
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

    fn metadata_operation_batch(
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
            // The OPERATION surface derives the mutation's resource from the
            // compiled scope (`MutationEnvelope::for_scope` takes no subject),
            // so the caller's `subject` is discarded here -- unlike the
            // maintenance surface, where `MutationEnvelope::maintenance`
            // records it. That asymmetry predates this grouping; it was an
            // unused parameter before and is named here rather than left as a
            // bare `unused_variables` warning.
            subject: _,
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
        let operations = vec![MutationOperation {
            ordinal: 0,
            surface: MutationSurface::Other,
            domain: DurabilityDomain::SemanticIndex,
            method: eg_types::protocol::Method::ApplyMutation {
                event_type: event_type.to_string(),
                query: format!("sha256:{}", mutation_digest.to_hex()),
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

    /// One kernel-issued scoped read over the binding's serving scope. Owner
    /// tables are layout-bounded, so this one snapshot serves every generation.
    fn serving_read(&self) -> Result<ScopedRead<'_, SemanticIndexOwner>, SemanticCodeError> {
        self.kernel.read_scope(&self.serving).map_err(kernel_error)
    }

    fn live_generation_in(
        &self,
        read: &ScopedRead<'_, SemanticIndexOwner>,
    ) -> Result<Option<u64>, SemanticCodeError> {
        let pointers = read
            .open_owner_table(SEMANTIC_POINTERS)
            .map_err(kernel_error)?;
        let raw = pointers
            .get((self.tenant.as_str(), self.binding.as_str()))
            .map_err(kernel_error)?
            .map(|value| value.value().to_vec());
        let Some(raw) = raw else {
            return Ok(None);
        };
        let pointer =
            SemanticActivePointer::from_canonical_cbor(&raw).map_err(semantic_contract_error)?;
        pointer.validate().map_err(semantic_contract_error)?;
        let head_generation = read
            .open_owner_table(SEMANTIC_HEADS)
            .map_err(kernel_error)?
            .get((self.tenant.as_str(), self.binding.as_str()))
            .map_err(kernel_error)?
            .map(|value| value.value())
            .ok_or_else(|| {
                SemanticCodeError::Corrupt(
                    "semantic active pointer has no durable binding head".to_string(),
                )
            })?;
        if pointer.generation > head_generation {
            return Err(SemanticCodeError::Corrupt(
                "semantic active pointer names a future binding generation".to_string(),
            ));
        }
        let binding = self
            .read_binding_generation_in(read, pointer.generation)?
            .ok_or_else(|| {
                SemanticCodeError::Corrupt(
                    "semantic active pointer has no durable binding authority".to_string(),
                )
            })?;
        if pointer.tenant_id != binding.tenant_id
            || pointer.binding_id != binding.binding_id
            || pointer.binding_digest != binding.binding_digest
            || pointer.generation != binding.generation
            || pointer.source_revision != binding.source_revision
            || pointer.vector_target_id != binding.vector_target_id
            || pointer.lexical_index_identity != binding.lexical_index_identity
            || pointer.ann_index_identity != binding.ann_index_identity
            || pointer.composite_policy_digest != binding.policy_digest
            || binding.durable_state != SemanticBindingState::Live
        {
            return Err(SemanticCodeError::Refused(
                "semantic active pointer is not bound to the serving binding".to_string(),
            ));
        }
        Ok(Some(pointer.generation))
    }

    fn read_generation_in(
        &self,
        read: &ScopedRead<'_, SemanticIndexOwner>,
        generation: u64,
    ) -> Result<Option<SemanticGenerationImage>, SemanticCodeError> {
        let codes = read.open_owner_table(ANN_CODES).map_err(kernel_error)?;
        let mut parts = BTreeMap::new();
        for part in PARTS {
            let Some(bytes) = read_part(&codes, &self.tenant, &self.binding, generation, part)?
            else {
                return Ok(None);
            };
            parts.insert(part, bytes);
        }
        let mut take = |name: &str| parts.remove(name).unwrap_or_default();
        Ok(Some(SemanticGenerationImage {
            index: crate::compute::semantic_ann::AnnIndexImage {
                codes: eg_ann::durable_codes::AnnCodeArtifact {
                    meta: take("meta"),
                    codes: take("codes"),
                    refine: take("refine"),
                },
                ids: take("ids"),
            },
            manifest: take("manifest"),
        }))
    }
}

fn stage_scope_key(intent: &SemanticStageIntent) -> String {
    match &intent.scope {
        eg_types::semantic_index::SemanticStageScope::Entity { source_entity_id } => {
            source_entity_id.clone()
        }
        eg_types::semantic_index::SemanticStageScope::Generation => "generation".to_string(),
    }
}

fn stage_intent_key(intent: &SemanticStageIntent) -> String {
    format!("semantic-stage-intent:{}", intent.intent_digest)
}

/// Secondary lookup key for a completed receipt. It lives in the canonical
/// `SEMANTIC_STAGES` table and stores the exact same mutation bytes as the
/// intent row, so refresh proof is a bounded direct read without introducing a
/// second stage authority.
fn stage_receipt_index_key(receipt_digest: SemanticDigest) -> String {
    format!("semantic-stage-receipt:{}", receipt_digest)
}

fn is_stage_receipt_index_key(key: &str) -> bool {
    key.starts_with("semantic-stage-receipt:")
}

fn stage_intent_outbox(
    intent: &SemanticStageIntent,
) -> Result<MutationOutboxIntent, SemanticCodeError> {
    let payload = intent
        .to_canonical_cbor()
        .map_err(semantic_contract_error)?;
    let mut headers = BTreeMap::new();
    headers.insert(
        "schema".to_string(),
        eg_types::semantic_index::SEMANTIC_STAGE_INTENT_SCHEMA.to_string(),
    );
    headers.insert("binding_id".to_string(), intent.binding_id.clone());
    headers.insert(
        "binding_digest".to_string(),
        intent.binding_digest.to_string(),
    );
    headers.insert("generation".to_string(), intent.generation.to_string());
    headers.insert("source_entity_id".to_string(), stage_scope_key(intent));
    headers.insert(
        "source_revision".to_string(),
        intent.source_revision.clone(),
    );
    headers.insert("stage".to_string(), intent.stage.as_str().to_string());
    headers.insert(
        "intent_digest".to_string(),
        intent.intent_digest.to_string(),
    );
    Ok(MutationOutboxIntent {
        topic: SEMANTIC_STAGE_INTENT_TOPIC.to_string(),
        key: stage_intent_key(intent),
        payload,
        headers,
    })
}

fn stage_receipt_headers(transition: &SemanticStageTransition) -> BTreeMap<String, String> {
    BTreeMap::from([
        (
            "schema".to_string(),
            eg_types::semantic_index::SEMANTIC_STAGE_TRANSITION_SCHEMA.to_string(),
        ),
        (
            "binding_id".to_string(),
            transition.intent.binding_id.clone(),
        ),
        (
            "binding_digest".to_string(),
            transition.intent.binding_digest.to_string(),
        ),
        (
            "generation".to_string(),
            transition.intent.generation.to_string(),
        ),
        (
            "stage".to_string(),
            transition.intent.stage.as_str().to_string(),
        ),
        (
            "intent_digest".to_string(),
            transition.intent.intent_digest.to_string(),
        ),
        (
            "receipt_digest".to_string(),
            transition.receipt.receipt_digest().to_string(),
        ),
    ])
}

fn validate_successor_intent(
    transition: &SemanticStageTransition,
    successor: Option<&SemanticStageIntent>,
) -> Result<Option<SemanticStageIntent>, SemanticCodeError> {
    if !matches!(
        transition.receipt.outcome,
        SemanticStageOutcome::Completed | SemanticStageOutcome::IdempotentNoop
    ) {
        if successor.is_some() {
            return Err(SemanticCodeError::Refused(
                "non-terminal semantic stage outcome cannot publish a successor".to_string(),
            ));
        }
        return Ok(None);
    }
    let derived = match transition.intent.stage {
        SemanticStage::SourceCommit => Some(SemanticStageIntent::create(
            eg_types::semantic_index::SemanticStageIntentDraft {
                binding_id: transition.intent.binding_id.clone(),
                binding_digest: transition.intent.binding_digest,
                generation: transition.intent.generation,
                scope: transition.intent.scope.clone(),
                source_revision: transition.intent.source_revision.clone(),
                stage: SemanticStage::GraphProjection,
                predecessor: SemanticStagePredecessor::EntityReceipt {
                    stage: SemanticStage::SourceCommit,
                    receipt_digest: transition.receipt.receipt_digest(),
                },
                input_digest: transition.receipt.output_digest,
            },
        )),
        SemanticStage::GraphProjection => Some(SemanticStageIntent::create(
            eg_types::semantic_index::SemanticStageIntentDraft {
                binding_id: transition.intent.binding_id.clone(),
                binding_digest: transition.intent.binding_digest,
                generation: transition.intent.generation,
                scope: transition.intent.scope.clone(),
                source_revision: transition.intent.source_revision.clone(),
                stage: SemanticStage::LexicalIndex,
                predecessor: SemanticStagePredecessor::EntityReceipt {
                    stage: SemanticStage::GraphProjection,
                    receipt_digest: transition.receipt.receipt_digest(),
                },
                input_digest: transition.receipt.output_digest,
            },
        )),
        SemanticStage::LexicalIndex => {
            let checkpoint = transition.generation_checkpoint.as_ref().ok_or_else(|| {
                SemanticCodeError::Refused(
                    "completed S3 transition has no lexical checkpoint".to_string(),
                )
            })?;
            Some(SemanticStageIntent::create(
                eg_types::semantic_index::SemanticStageIntentDraft {
                    binding_id: transition.intent.binding_id.clone(),
                    binding_digest: transition.intent.binding_digest,
                    generation: transition.intent.generation,
                    scope: transition.intent.scope.clone(),
                    source_revision: transition.intent.source_revision.clone(),
                    stage: SemanticStage::Vector,
                    predecessor: SemanticStagePredecessor::GenerationCheckpoint {
                        stage: SemanticStage::LexicalIndex,
                        checkpoint_digest: checkpoint.successor.checkpoint_digest,
                    },
                    input_digest: transition.receipt.output_digest,
                },
            ))
        }
        SemanticStage::Vector => None,
        SemanticStage::AnnIndex => None,
        SemanticStage::ReconcileAndActivate => None,
    }
    .transpose()
    .map_err(|error| SemanticCodeError::Refused(format!("successor intent rejected: {error:?}")))?;
    let Some(successor) = successor else {
        if matches!(
            transition.intent.stage,
            SemanticStage::Vector | SemanticStage::AnnIndex
        ) {
            return Err(SemanticCodeError::Refused(
                "completed S4/S5 transition requires an explicit successor proof".to_string(),
            ));
        }
        return Ok(derived);
    };
    successor.validate().map_err(semantic_contract_error)?;
    if successor.binding_id != transition.intent.binding_id
        || successor.binding_digest != transition.intent.binding_digest
        || successor.generation != transition.intent.generation
        || successor.source_revision != transition.intent.source_revision
        || successor.input_digest != transition.receipt.output_digest
    {
        return Err(SemanticCodeError::Refused(
            "successor intent coordinates or predecessor input are stale".to_string(),
        ));
    }
    let expected = derived.as_ref();
    match transition.intent.stage {
        SemanticStage::SourceCommit
        | SemanticStage::GraphProjection
        | SemanticStage::LexicalIndex => {
            if expected != Some(successor) {
                return Err(SemanticCodeError::Refused(
                    "successor intent does not carry the exact stage receipt proof".to_string(),
                ));
            }
        }
        SemanticStage::Vector => {
            let complete = match &successor.predecessor {
                SemanticStagePredecessor::GenerationCoverage { checkpoint } => {
                    checkpoint.require_complete().is_ok()
                        && checkpoint.binding_id == successor.binding_id
                        && checkpoint.binding_digest == successor.binding_digest
                        && checkpoint.generation == successor.generation
                        && checkpoint.source_revision == successor.source_revision
                }
                _ => false,
            };
            if successor.stage != SemanticStage::AnnIndex || !complete {
                return Err(SemanticCodeError::Refused(
                    "S4 successor must be a complete S5 generation-coverage intent".to_string(),
                ));
            }
        }
        SemanticStage::AnnIndex => {
            if successor.stage != SemanticStage::ReconcileAndActivate
                || !matches!(
                    successor.scope,
                    eg_types::semantic_index::SemanticStageScope::Generation
                )
                || !matches!(
                    successor.predecessor,
                    SemanticStagePredecessor::Activation { .. }
                )
            {
                return Err(SemanticCodeError::Refused(
                    "S5 successor must be the generation-scoped S6 activation intent".to_string(),
                ));
            }
        }
        SemanticStage::ReconcileAndActivate => {
            return Err(SemanticCodeError::Refused(
                "S6 is terminal and cannot publish another stage".to_string(),
            ));
        }
    }
    Ok(Some(successor.clone()))
}

/// Where one generation's rows live, and the binding identity those rows must
/// agree with.
///
/// Fifteen call sites plucked exactly these five off a
/// `SemanticGenerationCheckpoint`, a `SemanticBinding` or a
/// `SemanticStageTransition` and passed them one by one.
/// `tenant`/`binding`/`generation` are the row-key prefix of every
/// per-generation semantic table; `source_revision` selects the checkpoint head
/// inside that prefix; and `binding_digest` is the identity the rows found
/// there must carry, so a row written under a DIFFERENT binding of the same
/// name is refused rather than read. The digest travelling with the key is the
/// point of the grouping: a lookup given only the key could not tell those two
/// apart, and every caller that has the key has the digest.
struct GenerationCoordinates<'a> {
    tenant: &'a str,
    binding: &'a str,
    generation: u64,
    binding_digest: SemanticDigest,
    source_revision: &'a str,
}

fn current_checkpoint_from_tables<TC, TH>(
    checkpoint_table: &TC,
    head_table: &TH,
    at: GenerationCoordinates<'_>,
    stage: SemanticStage,
) -> Result<Option<SemanticGenerationCheckpoint>, SemanticCodeError>
where
    TC: redb::ReadableTable<(&'static str, &'static str, u64, &'static str), &'static [u8]>,
    TH: redb::ReadableTable<
        (&'static str, &'static str, u64, &'static str, &'static str),
        &'static [u8],
    >,
{
    let GenerationCoordinates {
        tenant,
        binding,
        generation,
        binding_digest,
        source_revision,
    } = at;
    let Some(head_bytes) = head_table
        .get((tenant, binding, generation, source_revision, stage.as_str()))
        .map_err(kernel_error)?
        .map(|value| value.value().to_vec())
    else {
        return Ok(None);
    };
    let head = SemanticGenerationCheckpoint::from_canonical_cbor(&head_bytes)
        .map_err(semantic_contract_error)?;
    head.validate().map_err(semantic_contract_error)?;
    if head.binding_id != binding
        || head.binding_digest != binding_digest
        || head.generation != generation
        || head.source_revision != source_revision
        || head.stage != stage
    {
        return Err(SemanticCodeError::Corrupt(
            "semantic checkpoint head pointer coordinates do not match its key".to_string(),
        ));
    }
    let checkpoint_key = head.checkpoint_digest.to_string();
    let checkpoint_bytes = checkpoint_table
        .get((tenant, binding, generation, checkpoint_key.as_str()))
        .map_err(kernel_error)?
        .map(|value| value.value().to_vec())
        .ok_or_else(|| {
            SemanticCodeError::Corrupt(
                "semantic checkpoint head points to a missing checkpoint row".to_string(),
            )
        })?;
    if checkpoint_bytes != head_bytes {
        return Err(SemanticCodeError::Corrupt(
            "semantic checkpoint head bytes differ from its checkpoint row".to_string(),
        ));
    }
    let checkpoint = SemanticGenerationCheckpoint::from_canonical_cbor(&checkpoint_bytes)
        .map_err(semantic_contract_error)?;
    if checkpoint != head || checkpoint.checkpoint_digest.to_string() != checkpoint_key {
        return Err(SemanticCodeError::Corrupt(
            "semantic checkpoint head key or row bytes do not match the checkpoint digest"
                .to_string(),
        ));
    }
    Ok(Some(checkpoint))
}

fn authoritative_generation_state<TP, TS>(
    source_progress_table: &TP,
    stage_table: &TS,
    at: GenerationCoordinates<'_>,
    member_stage: SemanticStage,
    aggregate_stage: SemanticStage,
    current_entity: Option<&str>,
) -> Result<
    (
        SemanticGenerationAggregate,
        BTreeMap<String, SemanticGenerationMember>,
    ),
    SemanticCodeError,
>
where
    TP: redb::ReadableTable<(&'static str, &'static str, u64, &'static str), &'static [u8]>,
    TS: redb::ReadableTable<(&'static str, &'static str, &'static str), &'static [u8]>,
{
    let GenerationCoordinates {
        tenant,
        binding,
        generation,
        binding_digest,
        source_revision,
    } = at;
    let mut progress_by_entity = BTreeMap::new();
    let progress_rows = source_progress_table
        .range((tenant, binding, generation, "")..)
        .map_err(kernel_error)?;
    for row in progress_rows {
        let (key, value) = row.map_err(kernel_error)?;
        let key = key.value();
        if key.0 != tenant || key.1 != binding || key.2 != generation {
            break;
        }
        if key.3 == SEMANTIC_RECONCILIATION_CHECKPOINT_ENTITY {
            continue;
        }
        let progress = SemanticSourceProgress::from_canonical_cbor(value.value())
            .map_err(semantic_contract_error)?;
        if progress.binding_id != binding
            || progress.binding_digest != binding_digest
            || progress.generation != generation
            || progress.source_entity_id != key.3
            || progress.source_revision != source_revision
        {
            return Err(SemanticCodeError::Refused(
                "semantic checkpoint source-progress row is outside the current generation"
                    .to_string(),
            ));
        }
        if progress.superseded_by_revision.is_some() {
            return Err(SemanticCodeError::Refused(
                "semantic checkpoint source-progress row is superseded".to_string(),
            ));
        }
        if progress_by_entity
            .insert(progress.source_entity_id.clone(), progress)
            .is_some()
        {
            return Err(SemanticCodeError::Corrupt(
                "semantic checkpoint source-progress rows duplicate an entity".to_string(),
            ));
        }
    }
    if progress_by_entity.is_empty() {
        return Err(SemanticCodeError::Refused(
            "semantic checkpoint has no authoritative source-progress entities".to_string(),
        ));
    }

    let mut members_by_entity = BTreeMap::new();
    let stage_rows = stage_table
        .range((tenant, binding, "")..)
        .map_err(kernel_error)?;
    for row in stage_rows {
        let (key, value) = row.map_err(kernel_error)?;
        let key = key.value();
        if key.0 != tenant || key.1 != binding {
            break;
        }
        // Receipt index rows carry the same canonical transition bytes as the
        // intent row. They are direct proof lookups, not additional completed
        // members of the generation aggregate.
        if is_stage_receipt_index_key(key.2) {
            continue;
        }
        let mutation = SemanticIndexMutation::from_canonical_cbor(value.value())
            .map_err(semantic_contract_error)?;
        let SemanticIndexMutation::RecordStageTransition { transition, .. } = mutation else {
            continue;
        };
        if transition.intent.binding_id != binding
            || transition.intent.binding_digest != binding_digest
            || transition.intent.generation != generation
            || transition.intent.source_revision != source_revision
            || transition.intent.stage != member_stage
        {
            continue;
        }
        let Some(source_entity_id) = transition.intent.scope.source_entity_id() else {
            continue;
        };
        if !matches!(
            transition.receipt.outcome,
            SemanticStageOutcome::Completed | SemanticStageOutcome::IdempotentNoop
        ) {
            continue;
        }
        let member = SemanticGenerationMember {
            source_entity_id: source_entity_id.to_string(),
            source_revision: source_revision.to_string(),
            receipt_digest: transition.receipt.receipt_digest(),
            artifact_digest: transition.receipt.output_digest,
        };
        if members_by_entity
            .insert(member.source_entity_id.clone(), member)
            .is_some()
        {
            return Err(SemanticCodeError::Corrupt(
                "semantic checkpoint stage rows contain duplicate completed entities".to_string(),
            ));
        }
    }

    let mut expected = Vec::with_capacity(progress_by_entity.len());
    let mut completed = Vec::new();
    for (source_entity_id, progress) in &progress_by_entity {
        expected.push(SemanticExpectedEntity {
            source_entity_id: source_entity_id.clone(),
            source_revision: source_revision.to_string(),
        });
        let is_current_entity = current_entity == Some(source_entity_id.as_str());
        let completed_at_stage = progress
            .completed_stage
            .is_some_and(|completed| completed >= member_stage);
        if completed_at_stage || is_current_entity {
            let member = members_by_entity.get(source_entity_id).ok_or_else(|| {
                SemanticCodeError::Refused(
                    "semantic checkpoint has completed source progress without a stage receipt"
                        .to_string(),
                )
            })?;
            if progress.completed_stage == Some(member_stage)
                && progress.completed_receipt_digest != Some(member.receipt_digest)
            {
                return Err(SemanticCodeError::Refused(
                    "semantic checkpoint source-progress receipt differs from its stage receipt"
                        .to_string(),
                ));
            }
            completed.push(member.clone());
        } else if members_by_entity.contains_key(source_entity_id) {
            return Err(SemanticCodeError::Refused(
                "semantic checkpoint has an uncommitted stage receipt for an incomplete entity"
                    .to_string(),
            ));
        }
    }
    for source_entity_id in members_by_entity.keys() {
        if !progress_by_entity.contains_key(source_entity_id) {
            return Err(SemanticCodeError::Refused(
                "semantic checkpoint stage receipt names an omitted entity".to_string(),
            ));
        }
    }
    let aggregate = SemanticGenerationAggregate::create(
        binding,
        binding_digest,
        generation,
        source_revision,
        aggregate_stage,
        expected,
        completed,
    )
    .map_err(semantic_contract_error)?;
    Ok((aggregate, members_by_entity))
}

fn validate_six_checkpoint_from_tables<TC, TH, TP, TS>(
    checkpoint_table: &TC,
    checkpoint_head_table: &TH,
    source_progress_table: &TP,
    stage_table: &TS,
    tenant: &str,
    binding: &str,
    checkpoint: &SemanticGenerationCheckpoint,
) -> Result<(), SemanticCodeError>
where
    TC: redb::ReadableTable<(&'static str, &'static str, u64, &'static str), &'static [u8]>,
    TH: redb::ReadableTable<
        (&'static str, &'static str, u64, &'static str, &'static str),
        &'static [u8],
    >,
    TP: redb::ReadableTable<(&'static str, &'static str, u64, &'static str), &'static [u8]>,
    TS: redb::ReadableTable<(&'static str, &'static str, &'static str), &'static [u8]>,
{
    checkpoint
        .require_complete()
        .map_err(semantic_contract_error)?;
    if checkpoint.binding_id != binding || checkpoint.stage != SemanticStage::ReconcileAndActivate {
        return Err(SemanticCodeError::Refused(
            "S6 checkpoint is outside the current semantic binding".to_string(),
        ));
    }
    let lexical = current_checkpoint_from_tables(
        checkpoint_table,
        checkpoint_head_table,
        GenerationCoordinates {
            tenant,
            binding,
            generation: checkpoint.generation,
            binding_digest: checkpoint.binding_digest,
            source_revision: &checkpoint.source_revision,
        },
        SemanticStage::LexicalIndex,
    )?
    .ok_or_else(|| {
        SemanticCodeError::Refused(
            "S6 checkpoint has no current lexical generation checkpoint".to_string(),
        )
    })?;
    lexical
        .require_complete()
        .map_err(semantic_contract_error)?;
    let ann = current_checkpoint_from_tables(
        checkpoint_table,
        checkpoint_head_table,
        GenerationCoordinates {
            tenant,
            binding,
            generation: checkpoint.generation,
            binding_digest: checkpoint.binding_digest,
            source_revision: &checkpoint.source_revision,
        },
        SemanticStage::AnnIndex,
    )?
    .ok_or_else(|| {
        SemanticCodeError::Refused(
            "S6 checkpoint has no current ANN generation checkpoint".to_string(),
        )
    })?;
    ann.require_complete().map_err(semantic_contract_error)?;
    let SemanticGenerationDependency::Activation {
        lexical_checkpoint_digest,
        ann_checkpoint_digest,
    } = &checkpoint.dependency
    else {
        return Err(SemanticCodeError::Refused(
            "S6 checkpoint has no exact lexical/ANN dependency proof".to_string(),
        ));
    };
    if *lexical_checkpoint_digest != lexical.checkpoint_digest
        || *ann_checkpoint_digest != ann.checkpoint_digest
    {
        return Err(SemanticCodeError::Refused(
            "S6 checkpoint dependency is not the current lexical/ANN head".to_string(),
        ));
    }
    let (aggregate, _) = authoritative_generation_state(
        source_progress_table,
        stage_table,
        GenerationCoordinates {
            tenant,
            binding,
            generation: checkpoint.generation,
            binding_digest: checkpoint.binding_digest,
            source_revision: &checkpoint.source_revision,
        },
        SemanticStage::AnnIndex,
        SemanticStage::ReconcileAndActivate,
        None,
    )?;
    if checkpoint.aggregate != aggregate {
        return Err(SemanticCodeError::Refused(
            "S6 checkpoint aggregate is not the authoritative current generation state".to_string(),
        ));
    }
    Ok(())
}

fn validate_six_checkpoint_write(
    rows: &AdmittedOwnerWrite<'_, SemanticIndexOwner>,
    tenant: &str,
    binding: &str,
    checkpoint: &SemanticGenerationCheckpoint,
) -> Result<(), SemanticCodeError> {
    let checkpoint_table = rows
        .open_table(SEMANTIC_CHECKPOINTS)
        .map_err(kernel_error)?;
    let source_progress_table = rows
        .open_table(SEMANTIC_SOURCE_PROGRESS)
        .map_err(kernel_error)?;
    let stage_table = rows.open_table(SEMANTIC_STAGES).map_err(kernel_error)?;
    let checkpoint_head_table = rows
        .open_table(SEMANTIC_CHECKPOINT_HEADS)
        .map_err(kernel_error)?;
    validate_six_checkpoint_from_tables(
        &checkpoint_table,
        &checkpoint_head_table,
        &source_progress_table,
        &stage_table,
        tenant,
        binding,
        checkpoint,
    )
}

fn validate_stage_predecessor_write(
    write: &AdmittedMutation<'_, SemanticIndexOwner>,
    rows: &AdmittedOwnerWrite<'_, SemanticIndexOwner>,
    tenant: &str,
    binding: &str,
    intent: &SemanticStageIntent,
    reject_completed: bool,
) -> Result<(), SemanticCodeError> {
    let progress = if let Some(source_entity_id) = intent.scope.source_entity_id() {
        let raw = write
            .open_read_table(SEMANTIC_SOURCE_PROGRESS)
            .map_err(kernel_error)?
            .get((tenant, binding, intent.generation, source_entity_id))
            .map_err(kernel_error)?
            .map(|value| value.value().to_vec())
            .ok_or_else(|| {
                SemanticCodeError::Refused(
                    "semantic stage transition has no source-progress row".to_string(),
                )
            })?;
        let progress =
            SemanticSourceProgress::from_canonical_cbor(&raw).map_err(semantic_contract_error)?;
        if progress.binding_digest != intent.binding_digest
            || progress.generation != intent.generation
            || progress.source_entity_id != source_entity_id
            || progress.source_revision != intent.source_revision
        {
            return Err(SemanticCodeError::Refused(
                "semantic transition source-progress proof does not match intent".to_string(),
            ));
        }
        if progress.superseded_by_revision.is_some() {
            return Err(SemanticCodeError::Refused(
                "semantic transition names a superseded source revision".to_string(),
            ));
        }
        if reject_completed
            && progress
                .completed_stage
                .is_some_and(|completed| completed >= intent.stage)
        {
            return Err(SemanticCodeError::Refused(
                "semantic transition stage is already completed".to_string(),
            ));
        }
        Some(progress)
    } else {
        if intent.stage != SemanticStage::ReconcileAndActivate {
            return Err(SemanticCodeError::Refused(
                "only S6 may use a generation scope".to_string(),
            ));
        }
        None
    };
    let predecessor = &intent.predecessor;
    let checkpoint_table = rows
        .open_table(SEMANTIC_CHECKPOINTS)
        .map_err(kernel_error)?;
    let checkpoint_head_table = rows
        .open_table(SEMANTIC_CHECKPOINT_HEADS)
        .map_err(kernel_error)?;
    let checkpoint = |digest: SemanticDigest,
                      stage: SemanticStage|
     -> Result<bool, SemanticCodeError> {
        Ok(current_checkpoint_from_tables(
            &checkpoint_table,
            &checkpoint_head_table,
            GenerationCoordinates {
                tenant,
                binding,
                generation: intent.generation,
                binding_digest: intent.binding_digest,
                source_revision: &intent.source_revision,
            },
            stage,
        )?
        .is_some_and(|value| value.checkpoint_digest == digest && value.require_complete().is_ok()))
    };
    match predecessor {
        SemanticStagePredecessor::None => {
            if progress.is_some_and(|value| value.completed_stage.is_some()) {
                return Err(SemanticCodeError::Refused(
                    "S1 transition has a completed predecessor".to_string(),
                ));
            }
        }
        SemanticStagePredecessor::EntityReceipt {
            stage,
            receipt_digest,
        } => {
            let Some(progress) = progress.as_ref() else {
                return Err(SemanticCodeError::Refused(
                    "entity receipt requires source progress".to_string(),
                ));
            };
            if progress.completed_stage != Some(*stage)
                || progress.completed_receipt_digest != Some(*receipt_digest)
            {
                return Err(SemanticCodeError::Refused(
                    "entity receipt is not the durable predecessor receipt".to_string(),
                ));
            }
        }
        SemanticStagePredecessor::GenerationCheckpoint {
            stage,
            checkpoint_digest,
        } => {
            if progress.is_none_or(|value| value.completed_stage != Some(*stage))
                || !checkpoint(*checkpoint_digest, *stage)?
            {
                return Err(SemanticCodeError::Refused(
                    "generation checkpoint is absent or mismatched".to_string(),
                ));
            }
        }
        SemanticStagePredecessor::GenerationCoverage { checkpoint: value } => {
            value.require_complete().map_err(semantic_contract_error)?;
            if progress.is_none_or(|row| row.completed_stage != Some(SemanticStage::Vector))
                || !checkpoint(value.checkpoint_digest, SemanticStage::Vector)?
            {
                return Err(SemanticCodeError::Refused(
                    "generation coverage is absent or mismatched".to_string(),
                ));
            }
        }
        SemanticStagePredecessor::Activation {
            lexical_checkpoint_digest,
            ann_checkpoint_digest,
        } => {
            if !checkpoint(*lexical_checkpoint_digest, SemanticStage::LexicalIndex)?
                || !checkpoint(*ann_checkpoint_digest, SemanticStage::AnnIndex)?
            {
                return Err(SemanticCodeError::Refused(
                    "S6 activation proof is absent or mixed-generation".to_string(),
                ));
            }
        }
    }
    Ok(())
}

fn persist_transition_checkpoints(
    rows: &AdmittedOwnerWrite<'_, SemanticIndexOwner>,
    tenant: &str,
    binding: &str,
    transition: &SemanticStageTransition,
    successor: Option<&SemanticStageIntent>,
) -> Result<(), SemanticCodeError> {
    if transition.intent.stage == SemanticStage::Vector {
        persist_vector_checkpoint(rows, tenant, binding, transition, successor)?;
    }
    {
        let table = rows
            .open_table(SEMANTIC_CHECKPOINTS)
            .map_err(kernel_error)?;
        let head_table = rows
            .open_table(SEMANTIC_CHECKPOINT_HEADS)
            .map_err(kernel_error)?;
        let require_current = |checkpoint: &SemanticGenerationCheckpoint,
                               stage: SemanticStage|
         -> Result<(), SemanticCodeError> {
            let current = current_checkpoint_from_tables(
                &table,
                &head_table,
                GenerationCoordinates {
                    tenant,
                    binding,
                    generation: checkpoint.generation,
                    binding_digest: checkpoint.binding_digest,
                    source_revision: &checkpoint.source_revision,
                },
                stage,
            )?;
            if current.as_ref() != Some(checkpoint) || checkpoint.require_complete().is_err() {
                return Err(SemanticCodeError::Refused(
                    "semantic checkpoint predecessor is not the current durable head".to_string(),
                ));
            }
            Ok(())
        };
        if let SemanticStagePredecessor::GenerationCoverage { checkpoint } =
            &transition.intent.predecessor
        {
            require_current(checkpoint, SemanticStage::Vector)?;
        }
    }
    if let Some(update) = &transition.generation_checkpoint {
        persist_generation_checkpoint_update(rows, tenant, binding, transition, update)?;
    }
    Ok(())
}

fn persist_vector_checkpoint(
    rows: &AdmittedOwnerWrite<'_, SemanticIndexOwner>,
    tenant: &str,
    binding: &str,
    transition: &SemanticStageTransition,
    successor: Option<&SemanticStageIntent>,
) -> Result<(), SemanticCodeError> {
    let successor = successor.ok_or_else(|| {
        SemanticCodeError::Refused(
            "completed S4 transition has no generation coverage successor".to_string(),
        )
    })?;
    let SemanticStagePredecessor::GenerationCoverage {
        checkpoint: coverage,
    } = &successor.predecessor
    else {
        return Err(SemanticCodeError::Refused(
            "S4 successor has no generation coverage checkpoint".to_string(),
        ));
    };
    let lexical_checkpoint_digest = match &transition.intent.predecessor {
        SemanticStagePredecessor::GenerationCheckpoint {
            stage: SemanticStage::LexicalIndex,
            checkpoint_digest,
        } => *checkpoint_digest,
        _ => {
            return Err(SemanticCodeError::Refused(
                "S4 transition has no lexical checkpoint predecessor".to_string(),
            ));
        }
    };
    let checkpoint_table = rows
        .open_table(SEMANTIC_CHECKPOINTS)
        .map_err(kernel_error)?;
    let checkpoint_head_table = rows
        .open_table(SEMANTIC_CHECKPOINT_HEADS)
        .map_err(kernel_error)?;
    let lexical = current_checkpoint_from_tables(
        &checkpoint_table,
        &checkpoint_head_table,
        GenerationCoordinates {
            tenant,
            binding,
            generation: transition.intent.generation,
            binding_digest: transition.intent.binding_digest,
            source_revision: &transition.intent.source_revision,
        },
        SemanticStage::LexicalIndex,
    )?
    .ok_or_else(|| {
        SemanticCodeError::Refused(
            "S4 transition has no current lexical generation checkpoint".to_string(),
        )
    })?;
    if lexical.checkpoint_digest != lexical_checkpoint_digest || lexical.require_complete().is_err()
    {
        return Err(SemanticCodeError::Refused(
            "S4 transition lexical checkpoint is stale or incomplete".to_string(),
        ));
    }
    let previous_vector = current_checkpoint_from_tables(
        &checkpoint_table,
        &checkpoint_head_table,
        GenerationCoordinates {
            tenant,
            binding,
            generation: transition.intent.generation,
            binding_digest: transition.intent.binding_digest,
            source_revision: &transition.intent.source_revision,
        },
        SemanticStage::Vector,
    )?;
    let previous_completed_count = previous_vector
        .as_ref()
        .map_or(0, |checkpoint| checkpoint.aggregate.completed_entity_count);
    drop(checkpoint_head_table);
    drop(checkpoint_table);

    let source_progress_table = rows
        .open_table(SEMANTIC_SOURCE_PROGRESS)
        .map_err(kernel_error)?;
    let stage_table = rows.open_table(SEMANTIC_STAGES).map_err(kernel_error)?;
    let current_entity = transition.intent.scope.source_entity_id().ok_or_else(|| {
        SemanticCodeError::Refused("S4 transition must be entity-scoped".to_string())
    })?;
    let (aggregate, members) = authoritative_generation_state(
        &source_progress_table,
        &stage_table,
        GenerationCoordinates {
            tenant,
            binding,
            generation: transition.intent.generation,
            binding_digest: transition.intent.binding_digest,
            source_revision: &transition.intent.source_revision,
        },
        SemanticStage::Vector,
        SemanticStage::Vector,
        Some(current_entity),
    )?;
    if aggregate.completed_entity_count != previous_completed_count.saturating_add(1) {
        return Err(SemanticCodeError::Refused(
            "S4 vector checkpoint is stale relative to authoritative entity progress".to_string(),
        ));
    }
    let member = members.get(current_entity).ok_or_else(|| {
        SemanticCodeError::Refused(
            "S4 vector checkpoint has no authoritative current entity member".to_string(),
        )
    })?;
    let expected = SemanticGenerationCheckpoint::create(SemanticGenerationCheckpointDraft {
        binding_id: transition.intent.binding_id.clone(),
        binding_digest: transition.intent.binding_digest,
        generation: transition.intent.generation,
        source_revision: transition.intent.source_revision.clone(),
        stage: SemanticStage::Vector,
        aggregate: aggregate.clone(),
        dependency: SemanticGenerationDependency::Checkpoint {
            stage: SemanticStage::LexicalIndex,
            checkpoint_digest: lexical_checkpoint_digest,
        },
        artifact_digest: aggregate.aggregate_artifact_digest,
        completed_at: transition.receipt.completed_at.clone(),
    })
    .map_err(semantic_contract_error)?;
    if coverage.as_ref() != &expected {
        return Err(SemanticCodeError::Refused(
            "S4 successor coverage is not the authoritative vector checkpoint".to_string(),
        ));
    }
    if member.receipt_digest != transition.receipt.receipt_digest()
        || member.artifact_digest != transition.receipt.output_digest
    {
        return Err(SemanticCodeError::Refused(
            "S4 vector checkpoint member differs from the durable transition".to_string(),
        ));
    }
    let bytes = expected
        .to_canonical_cbor()
        .map_err(semantic_contract_error)?;
    let key = expected.checkpoint_digest.to_string();
    let mut table = rows
        .open_table(SEMANTIC_CHECKPOINTS)
        .map_err(kernel_error)?;
    put_bytes_once(
        &mut table,
        (tenant, binding, expected.generation, key.as_str()),
        &bytes,
    )?;
    drop(table);
    advance_checkpoint_head(rows, tenant, binding, &expected, previous_vector.as_ref())
}

fn persist_generation_checkpoint_update(
    rows: &AdmittedOwnerWrite<'_, SemanticIndexOwner>,
    tenant: &str,
    binding: &str,
    transition: &SemanticStageTransition,
    update: &SemanticGenerationCheckpointUpdate,
) -> Result<(), SemanticCodeError> {
    update.validate().map_err(semantic_contract_error)?;
    let stage = transition.intent.stage;
    if !matches!(stage, SemanticStage::LexicalIndex | SemanticStage::AnnIndex) {
        return Err(SemanticCodeError::Refused(
            "only S3 and S5 may advance a generation checkpoint".to_string(),
        ));
    }
    let checkpoint_table = rows
        .open_table(SEMANTIC_CHECKPOINTS)
        .map_err(kernel_error)?;
    let checkpoint_head_table = rows
        .open_table(SEMANTIC_CHECKPOINT_HEADS)
        .map_err(kernel_error)?;
    let current = current_checkpoint_from_tables(
        &checkpoint_table,
        &checkpoint_head_table,
        GenerationCoordinates {
            tenant,
            binding,
            generation: transition.intent.generation,
            binding_digest: transition.intent.binding_digest,
            source_revision: &transition.intent.source_revision,
        },
        stage,
    )?;
    match current.as_ref() {
        None if update.expected_previous_completed_count != 0
            || update.expected_previous_checkpoint_digest.is_some() =>
        {
            return Err(SemanticCodeError::Refused(
                "semantic checkpoint CAS predecessor is absent".to_string(),
            ));
        }
        None => {}
        Some(previous)
            if previous.aggregate.completed_entity_count
                != update.expected_previous_completed_count
                || Some(previous.checkpoint_digest)
                    != update.expected_previous_checkpoint_digest =>
        {
            return Err(SemanticCodeError::Refused(
                "semantic checkpoint CAS predecessor is stale".to_string(),
            ));
        }
        Some(_) => {}
    }
    drop(checkpoint_head_table);
    drop(checkpoint_table);

    let source_progress_table = rows
        .open_table(SEMANTIC_SOURCE_PROGRESS)
        .map_err(kernel_error)?;
    let stage_table = rows.open_table(SEMANTIC_STAGES).map_err(kernel_error)?;
    let current_entity = transition.intent.scope.source_entity_id();
    let (aggregate, members) = authoritative_generation_state(
        &source_progress_table,
        &stage_table,
        GenerationCoordinates {
            tenant,
            binding,
            generation: transition.intent.generation,
            binding_digest: transition.intent.binding_digest,
            source_revision: &transition.intent.source_revision,
        },
        stage,
        stage,
        current_entity,
    )?;
    let member = current_entity
        .and_then(|source_entity_id| members.get(source_entity_id))
        .ok_or_else(|| {
            SemanticCodeError::Refused(
                "semantic checkpoint update has no authoritative current entity member".to_string(),
            )
        })?;
    if member != &update.member
        || aggregate.completed_entity_count
            != update.expected_previous_completed_count.saturating_add(1)
    {
        return Err(SemanticCodeError::Refused(
            "semantic checkpoint successor does not match authoritative entity progress"
                .to_string(),
        ));
    }
    let expected_successor =
        SemanticGenerationCheckpoint::create(SemanticGenerationCheckpointDraft {
            binding_id: update.successor.binding_id.clone(),
            binding_digest: update.successor.binding_digest,
            generation: update.successor.generation,
            source_revision: update.successor.source_revision.clone(),
            stage: update.successor.stage,
            aggregate,
            dependency: update.successor.dependency.clone(),
            artifact_digest: update.successor.artifact_digest,
            completed_at: update.successor.completed_at.clone(),
        })
        .map_err(semantic_contract_error)?;
    if expected_successor != update.successor {
        return Err(SemanticCodeError::Refused(
            "semantic checkpoint successor digest or aggregate is not authoritative".to_string(),
        ));
    }

    let mut table = rows
        .open_table(SEMANTIC_CHECKPOINTS)
        .map_err(kernel_error)?;
    let key = update.successor.checkpoint_digest.to_string();
    let bytes = update
        .successor
        .to_canonical_cbor()
        .map_err(semantic_contract_error)?;
    put_bytes_once(
        &mut table,
        (tenant, binding, update.successor.generation, key.as_str()),
        &bytes,
    )?;
    drop(table);
    advance_checkpoint_head(rows, tenant, binding, &update.successor, current.as_ref())
}

fn persist_stage_artifact_with_lease(
    rows: &AdmittedOwnerWrite<'_, SemanticIndexOwner>,
    tenant: &str,
    binding: &str,
    transition: &SemanticStageTransition,
    lease: Option<&MutationOutboxLease>,
    artifact: &SemanticStageArtifact,
) -> Result<(), SemanticCodeError> {
    match artifact {
        SemanticStageArtifact::None => Ok(()),
        SemanticStageArtifact::SqlSourceManifest {
            manifest,
            authorization,
        } => {
            let source_entity_id = transition.intent.scope.source_entity_id().ok_or_else(|| {
                SemanticCodeError::Refused(
                    "SQL source artifact requires an entity-scoped transition".to_string(),
                )
            })?;
            let mut manifests = rows
                .open_table(SEMANTIC_SQL_SOURCES)
                .map_err(kernel_error)?;
            let manifest_bytes = manifest
                .to_canonical_cbor()
                .map_err(semantic_contract_error)?;
            let manifest_key = (
                tenant,
                binding,
                transition.intent.generation,
                source_entity_id,
            );
            let existing_manifest = manifests
                .get(manifest_key)
                .map_err(kernel_error)?
                .map(|value| value.value().to_vec());
            match existing_manifest {
                None => {
                    manifests
                        .insert(manifest_key, manifest_bytes.as_slice())
                        .map_err(kernel_error)?;
                }
                Some(existing) if existing == manifest_bytes => {}
                Some(existing) => {
                    replace_reconciled_sql_manifest(
                        &mut manifests,
                        manifest_key,
                        &existing,
                        manifest,
                        &manifest_bytes,
                        transition,
                        lease,
                    )?;
                }
            }
            let mut receipts = rows
                .open_table(SEMANTIC_AUTH_RECEIPTS)
                .map_err(kernel_error)?;
            let receipt_bytes = authorization
                .to_canonical_cbor()
                .map_err(semantic_contract_error)?;
            let receipt_key = (
                tenant,
                binding,
                transition.intent.generation,
                source_entity_id,
            );
            let existing_receipt = receipts
                .get(receipt_key)
                .map_err(kernel_error)?
                .map(|value| value.value().to_vec());
            match existing_receipt {
                None => {
                    receipts
                        .insert(receipt_key, receipt_bytes.as_slice())
                        .map_err(kernel_error)?;
                    Ok(())
                }
                Some(existing) if existing == receipt_bytes => Ok(()),
                Some(_) if is_reconciled_sql_tombstone(transition, lease)? => {
                    replace_bytes(&mut receipts, receipt_key, &receipt_bytes)
                }
                Some(_) => Err(SemanticCodeError::Refused(
                    "semantic authorization receipt key already names different bytes".to_string(),
                )),
            }
        }
        SemanticStageArtifact::GraphProjectionManifest { manifest } => {
            let source_entity_id = transition.intent.scope.source_entity_id().ok_or_else(|| {
                SemanticCodeError::Refused(
                    "graph projection artifact requires an entity-scoped transition".to_string(),
                )
            })?;
            let mut table = rows
                .open_table(SEMANTIC_GRAPH_PROJECTIONS)
                .map_err(kernel_error)?;
            let bytes = manifest
                .to_canonical_cbor()
                .map_err(semantic_contract_error)?;
            put_bytes_once(
                &mut table,
                (
                    tenant,
                    binding,
                    transition.intent.generation,
                    source_entity_id,
                ),
                &bytes,
            )
        }
        SemanticStageArtifact::Vector { vector } => {
            let source_entity_id = transition.intent.scope.source_entity_id().ok_or_else(|| {
                SemanticCodeError::Refused(
                    "vector artifact requires an entity-scoped transition".to_string(),
                )
            })?;
            let mut table = rows.open_table(SEMANTIC_VECTORS).map_err(kernel_error)?;
            let bytes = vector
                .to_canonical_cbor()
                .map_err(semantic_contract_error)?;
            put_bytes_once(
                &mut table,
                (
                    tenant,
                    binding,
                    transition.intent.generation,
                    source_entity_id,
                ),
                &bytes,
            )
        }
        SemanticStageArtifact::DeadLetter { dead_letter } => {
            let mut table = rows
                .open_table(SEMANTIC_DEAD_LETTERS)
                .map_err(kernel_error)?;
            let bytes = dead_letter
                .to_canonical_cbor()
                .map_err(semantic_contract_error)?;
            let intent_digest = dead_letter.intent.intent_digest.to_string();
            put_bytes_once(
                &mut table,
                (tenant, binding, intent_digest.as_str(), dead_letter.attempt),
                &bytes,
            )
        }
    }
}

fn is_reconciled_sql_tombstone(
    transition: &SemanticStageTransition,
    lease: Option<&MutationOutboxLease>,
) -> Result<bool, SemanticCodeError> {
    let Some(lease) = lease else {
        return Ok(false);
    };
    let headers = &lease.record.intent.headers;
    if headers.get("reconciliation_proof").map(String::as_str)
        != Some("semantic-source-tombstone/v1")
    {
        return Ok(false);
    }
    if transition.intent.stage != SemanticStage::SourceCommit
        || transition.receipt.outcome != SemanticStageOutcome::Completed
        || !matches!(
            &transition.intent.predecessor,
            SemanticStagePredecessor::None
        )
    {
        return Err(SemanticCodeError::Refused(
            "semantic tombstone artifact has an invalid S1 transition".to_string(),
        ));
    }
    let source_entity_id = transition.intent.scope.source_entity_id().ok_or_else(|| {
        SemanticCodeError::Refused("semantic tombstone artifact is not entity scoped".to_string())
    })?;
    let complete_receipt =
        SemanticDigest::parse(headers.get("complete_snapshot_receipt_digest").ok_or_else(
            || {
                SemanticCodeError::Refused(
                    "semantic tombstone artifact has no complete snapshot receipt".to_string(),
                )
            },
        )?)
        .map_err(|_| {
            SemanticCodeError::Refused(
                "semantic tombstone complete snapshot receipt is not canonical".to_string(),
            )
        })?;
    let expected = reconciliation_tombstone_input_digest(
        source_entity_id,
        &transition.intent.source_revision,
        complete_receipt,
    );
    if transition.intent.input_digest != expected {
        return Err(SemanticCodeError::Refused(
            "semantic tombstone artifact input is not its complete snapshot proof".to_string(),
        ));
    }
    Ok(true)
}

fn replace_reconciled_sql_manifest(
    table: &mut redb::Table<'_, (&str, &str, u64, &str), &[u8]>,
    key: (&str, &str, u64, &str),
    existing_bytes: &[u8],
    manifest: &SemanticSqlSourceManifest,
    manifest_bytes: &[u8],
    transition: &SemanticStageTransition,
    lease: Option<&MutationOutboxLease>,
) -> Result<(), SemanticCodeError> {
    if !is_reconciled_sql_tombstone(transition, lease)? {
        return Err(SemanticCodeError::Refused(
            "semantic artifact key already names different bytes".to_string(),
        ));
    }
    let existing = SemanticSqlSourceManifest::from_canonical_cbor(existing_bytes)
        .map_err(semantic_contract_error)?;
    existing.validate().map_err(semantic_contract_error)?;
    validate_sql_source_revision(&existing.source_revision)?;
    if existing.binding_id != manifest.binding_id
        || existing.binding_digest != manifest.binding_digest
        || existing.generation != manifest.generation
        || existing.source_entity_id != manifest.source_entity_id
        || existing.source_identity != manifest.source_identity
    {
        return Err(SemanticCodeError::Refused(
            "semantic tombstone source manifest does not retain the prior identity".to_string(),
        ));
    }
    let existing_revision =
        sql_source_revision_parts(&existing.source_revision).ok_or_else(|| {
            SemanticCodeError::Corrupt(
                "retained SQL source manifest has a non-canonical source revision".to_string(),
            )
        })?;
    let manifest_revision =
        sql_source_revision_parts(&manifest.source_revision).ok_or_else(|| {
            SemanticCodeError::Corrupt(
                "replacement SQL source manifest has a non-canonical source revision".to_string(),
            )
        })?;
    if existing_revision.authority != manifest_revision.authority {
        return Err(SemanticCodeError::Refused(
            "semantic tombstone source authority differs from the retained manifest".to_string(),
        ));
    }
    if compare_source_revision(&manifest.source_revision, &existing.source_revision)
        != std::cmp::Ordering::Greater
    {
        return Err(SemanticCodeError::Refused(
            "semantic tombstone source revision does not advance the retained manifest".to_string(),
        ));
    }
    replace_bytes(table, key, manifest_bytes)
}

fn persist_generation_artifact(
    rows: &AdmittedOwnerWrite<'_, SemanticIndexOwner>,
    tenant: &str,
    binding: &str,
    transition: &SemanticStageTransition,
    artifact: &SemanticGenerationArtifact,
) -> Result<(), SemanticCodeError> {
    let checkpoint = transition.generation_checkpoint.as_ref().ok_or_else(|| {
        SemanticCodeError::Refused("generation artifact has no transition checkpoint".to_string())
    })?;
    artifact
        .validate_against(&checkpoint.successor)
        .map_err(semantic_contract_error)?;
    match artifact {
        SemanticGenerationArtifact::LexicalIndexManifest { manifest } => {
            let bytes = manifest
                .to_canonical_cbor()
                .map_err(semantic_contract_error)?;
            let mut table = rows.open_table(SEMANTIC_LEXICAL).map_err(kernel_error)?;
            put_bytes_once(
                &mut table,
                (tenant, binding, transition.intent.generation),
                &bytes,
            )
        }
        SemanticGenerationArtifact::AnnIndexManifest { manifest } => {
            let bytes = manifest
                .to_canonical_cbor()
                .map_err(semantic_contract_error)?;
            let mut table = rows.open_table(SEMANTIC_ANN).map_err(kernel_error)?;
            put_bytes_once(
                &mut table,
                (tenant, binding, transition.intent.generation),
                &bytes,
            )
        }
        SemanticGenerationArtifact::Activation { .. } => Err(SemanticCodeError::Refused(
            "activation artifacts belong to the admitted S6 finalization path".to_string(),
        )),
    }
}

fn advance_checkpoint_head(
    rows: &AdmittedOwnerWrite<'_, SemanticIndexOwner>,
    tenant: &str,
    binding: &str,
    successor: &SemanticGenerationCheckpoint,
    previous: Option<&SemanticGenerationCheckpoint>,
) -> Result<(), SemanticCodeError> {
    successor.validate().map_err(semantic_contract_error)?;
    let successor_bytes = successor
        .to_canonical_cbor()
        .map_err(semantic_contract_error)?;
    let checkpoint_key = successor.checkpoint_digest.to_string();
    let checkpoint = rows
        .open_table(SEMANTIC_CHECKPOINTS)
        .map_err(kernel_error)?
        .get((
            tenant,
            binding,
            successor.generation,
            checkpoint_key.as_str(),
        ))
        .map_err(kernel_error)?
        .map(|value| value.value().to_vec())
        .ok_or_else(|| {
            SemanticCodeError::Corrupt(
                "semantic checkpoint head advance has no checkpoint row".to_string(),
            )
        })?;
    if checkpoint != successor_bytes {
        return Err(SemanticCodeError::Corrupt(
            "semantic checkpoint head advance does not match checkpoint row bytes".to_string(),
        ));
    }
    let mut heads = rows
        .open_table(SEMANTIC_CHECKPOINT_HEADS)
        .map_err(kernel_error)?;
    let head_key = (
        tenant,
        binding,
        successor.generation,
        successor.source_revision.as_str(),
        successor.stage.as_str(),
    );
    let existing = heads
        .get(head_key)
        .map_err(kernel_error)?
        .map(|value| value.value().to_vec());
    match (previous, existing.as_deref()) {
        (Some(previous), Some(existing)) => {
            let previous_bytes = previous
                .to_canonical_cbor()
                .map_err(semantic_contract_error)?;
            if existing != previous_bytes.as_slice() {
                return Err(SemanticCodeError::Refused(
                    "semantic checkpoint head CAS predecessor is stale".to_string(),
                ));
            }
        }
        (Some(_), None) => {
            return Err(SemanticCodeError::Refused(
                "semantic checkpoint head CAS predecessor is absent".to_string(),
            ));
        }
        (None, Some(existing)) if existing != successor_bytes.as_slice() => {
            return Err(SemanticCodeError::Refused(
                "semantic checkpoint head already names another successor".to_string(),
            ));
        }
        (None, Some(_)) => return Ok(()),
        (None, None) => {}
    }
    replace_bytes(&mut heads, head_key, &successor_bytes)
}

fn put_bytes_once<'k, K>(
    table: &mut redb::Table<'_, K, &[u8]>,
    key: K::SelfType<'k>,
    bytes: &[u8],
) -> Result<(), SemanticCodeError>
where
    K: redb::Key + 'static,
{
    if let Some(existing) = table
        .get(&key)
        .map_err(kernel_error)?
        .map(|value| value.value().to_vec())
    {
        if existing != bytes {
            return Err(SemanticCodeError::Refused(
                "semantic artifact key already names different bytes".to_string(),
            ));
        }
        return Ok(());
    }
    table.insert(&key, bytes).map_err(kernel_error)?;
    Ok(())
}

fn replace_bytes<'k, K>(
    table: &mut redb::Table<'_, K, &[u8]>,
    key: K::SelfType<'k>,
    bytes: &[u8],
) -> Result<(), SemanticCodeError>
where
    K: redb::Key + 'static,
{
    table.insert(&key, bytes).map_err(kernel_error)?;
    Ok(())
}

fn valid_source_entity_id_for_reconciliation(entity: &str) -> bool {
    entity.len() <= SEMANTIC_RECONCILIATION_MAX_ENTITY_BYTES
        && entity
            .strip_prefix("semantic-sql-source:")
            .and_then(|digest| SemanticDigest::parse(digest).ok())
            .is_some()
}

/// Derive the only input digest accepted for a source-reconciliation
/// tombstone.  This is intentionally the same domain-separated proof used by
/// the SQL source reconciler: an entity identity, the exact authoritative
/// source revision, and the receipt for the complete snapshot.  A caller
/// cannot turn an arbitrary payload into a deletion merely by presenting a
/// valid S1 intent.
fn reconciliation_tombstone_input_digest(
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

fn validate_reconciliation_checkpoint(
    checkpoint: &SemanticSourceReconciliationCheckpoint,
) -> Result<(), SemanticCodeError> {
    if checkpoint.source_wakeup_digest == SemanticDigest::from_bytes([0; 32]) {
        return Err(SemanticCodeError::Refused(
            "source reconciliation wakeup identity is zero".to_string(),
        ));
    }
    validate_sql_source_revision(&checkpoint.source_revision)?;
    if checkpoint.source_revision.len() > SEMANTIC_RECONCILIATION_MAX_REVISION_BYTES {
        return Err(SemanticCodeError::Refused(
            "source reconciliation revision exceeds the bounded size".to_string(),
        ));
    }
    if let Some(cursor) = checkpoint.source_cursor.as_ref() {
        if cursor.is_empty() || cursor.len() > SEMANTIC_RECONCILIATION_MAX_CURSOR_BYTES {
            return Err(SemanticCodeError::Refused(
                "source reconciliation cursor exceeds the bounded size".to_string(),
            ));
        }
    }
    if let Some(cursor) = checkpoint.prior_cursor.as_deref() {
        if !valid_source_entity_id_for_reconciliation(cursor) {
            return Err(SemanticCodeError::Refused(
                "source reconciliation prior cursor is not a source identity".to_string(),
            ));
        }
    }
    match &checkpoint.phase {
        SemanticSourceReconciliationPhase::Scanning => {
            if checkpoint.source_cursor.is_none()
                || checkpoint.prior_cursor.is_some()
                || checkpoint.complete_snapshot_receipt_digest.is_some()
            {
                return Err(SemanticCodeError::Refused(
                    "scanning reconciliation checkpoint has invalid phase fields".to_string(),
                ));
            }
        }
        SemanticSourceReconciliationPhase::FinalizingTombstones => {
            if checkpoint.source_cursor.is_some()
                || checkpoint.complete_snapshot_receipt_digest.is_none()
            {
                return Err(SemanticCodeError::Refused(
                    "tombstone reconciliation checkpoint has invalid phase fields".to_string(),
                ));
            }
        }
    }
    if checkpoint
        .complete_snapshot_receipt_digest
        .is_some_and(|digest| digest == SemanticDigest::from_bytes([0; 32]))
    {
        return Err(SemanticCodeError::Refused(
            "source reconciliation complete snapshot proof is zero".to_string(),
        ));
    }
    Ok(())
}

fn encode_reconciliation_checkpoint(
    checkpoint: &SemanticSourceReconciliationCheckpoint,
) -> Result<Vec<u8>, SemanticCodeError> {
    validate_reconciliation_checkpoint(checkpoint)?;
    let mut encoded = Vec::with_capacity(256);
    encoded.extend_from_slice(SEMANTIC_RECONCILIATION_CHECKPOINT_MAGIC);
    encoded.extend_from_slice(checkpoint.source_wakeup_digest.as_bytes());
    append_checkpoint_bytes(&mut encoded, checkpoint.source_revision.as_bytes())?;
    encoded.push(match &checkpoint.phase {
        SemanticSourceReconciliationPhase::Scanning => 0,
        SemanticSourceReconciliationPhase::FinalizingTombstones => 1,
    });
    append_optional_checkpoint_bytes(&mut encoded, checkpoint.source_cursor.as_deref())?;
    append_optional_checkpoint_bytes(
        &mut encoded,
        checkpoint.prior_cursor.as_deref().map(str::as_bytes),
    )?;
    encoded.extend_from_slice(&checkpoint.rows_seen.to_be_bytes());
    encoded.extend_from_slice(&checkpoint.source_bytes_seen.to_be_bytes());
    encoded.extend_from_slice(&checkpoint.pages_seen.to_be_bytes());
    match checkpoint.complete_snapshot_receipt_digest {
        Some(digest) => {
            encoded.push(1);
            encoded.extend_from_slice(digest.as_bytes());
        }
        None => encoded.push(0),
    }
    if encoded.len() > 16 * 1024 {
        return Err(SemanticCodeError::Refused(
            "source reconciliation checkpoint exceeds the bounded size".to_string(),
        ));
    }
    Ok(encoded)
}

fn append_checkpoint_bytes(encoded: &mut Vec<u8>, bytes: &[u8]) -> Result<(), SemanticCodeError> {
    let length = u32::try_from(bytes.len()).map_err(|_| {
        SemanticCodeError::Refused("source reconciliation field is too large".to_string())
    })?;
    encoded.extend_from_slice(&length.to_be_bytes());
    encoded.extend_from_slice(bytes);
    Ok(())
}

fn append_optional_checkpoint_bytes(
    encoded: &mut Vec<u8>,
    bytes: Option<&[u8]>,
) -> Result<(), SemanticCodeError> {
    match bytes {
        Some(bytes) => {
            encoded.push(1);
            append_checkpoint_bytes(encoded, bytes)?;
        }
        None => encoded.push(0),
    }
    Ok(())
}

fn decode_reconciliation_checkpoint(
    encoded: &[u8],
) -> Result<SemanticSourceReconciliationCheckpoint, SemanticCodeError> {
    if encoded.len() > 16 * 1024 || !encoded.starts_with(SEMANTIC_RECONCILIATION_CHECKPOINT_MAGIC) {
        return Err(SemanticCodeError::Corrupt(
            "source reconciliation checkpoint has an unknown encoding".to_string(),
        ));
    }
    let mut offset = SEMANTIC_RECONCILIATION_CHECKPOINT_MAGIC.len();
    let wakeup_bytes = take_checkpoint_fixed(encoded, &mut offset, 32)?;
    let source_wakeup_digest =
        SemanticDigest::from_bytes(wakeup_bytes.try_into().map_err(|_| {
            SemanticCodeError::Corrupt("source reconciliation wakeup is not 32 bytes".to_string())
        })?);
    let source_revision = String::from_utf8(
        take_checkpoint_bytes(
            encoded,
            &mut offset,
            SEMANTIC_RECONCILIATION_MAX_REVISION_BYTES,
        )?
        .to_vec(),
    )
    .map_err(|_| {
        SemanticCodeError::Corrupt("source reconciliation revision is not UTF-8".to_string())
    })?;
    let phase = match take_checkpoint_byte(encoded, &mut offset)? {
        0 => SemanticSourceReconciliationPhase::Scanning,
        1 => SemanticSourceReconciliationPhase::FinalizingTombstones,
        _ => {
            return Err(SemanticCodeError::Corrupt(
                "source reconciliation checkpoint has an unknown phase".to_string(),
            ));
        }
    };
    let source_cursor = take_optional_checkpoint_bytes(
        encoded,
        &mut offset,
        SEMANTIC_RECONCILIATION_MAX_CURSOR_BYTES,
    )?;
    let prior_cursor = take_optional_checkpoint_bytes(
        encoded,
        &mut offset,
        SEMANTIC_RECONCILIATION_MAX_ENTITY_BYTES,
    )?
    .map(|bytes| String::from_utf8(bytes.to_vec()))
    .transpose()
    .map_err(|_| {
        SemanticCodeError::Corrupt("source reconciliation cursor is not UTF-8".to_string())
    })?;
    let rows_seen = take_checkpoint_u64(encoded, &mut offset)?;
    let source_bytes_seen = take_checkpoint_u64(encoded, &mut offset)?;
    let pages_seen = take_checkpoint_u64(encoded, &mut offset)?;
    let complete_snapshot_receipt_digest = match take_checkpoint_byte(encoded, &mut offset)? {
        0 => None,
        1 => {
            let bytes = take_checkpoint_fixed(encoded, &mut offset, 32)?;
            Some(SemanticDigest::from_bytes(bytes.try_into().map_err(
                |_| {
                    SemanticCodeError::Corrupt(
                        "source reconciliation proof is not 32 bytes".to_string(),
                    )
                },
            )?))
        }
        _ => {
            return Err(SemanticCodeError::Corrupt(
                "source reconciliation checkpoint has an unknown proof marker".to_string(),
            ));
        }
    };
    if offset != encoded.len() {
        return Err(SemanticCodeError::Corrupt(
            "source reconciliation checkpoint has trailing bytes".to_string(),
        ));
    }
    let checkpoint = SemanticSourceReconciliationCheckpoint {
        source_wakeup_digest,
        source_revision,
        phase,
        source_cursor,
        prior_cursor,
        rows_seen,
        source_bytes_seen,
        pages_seen,
        complete_snapshot_receipt_digest,
    };
    validate_reconciliation_checkpoint(&checkpoint)
        .map_err(|error| SemanticCodeError::Corrupt(error.to_string()))?;
    Ok(checkpoint)
}

fn take_checkpoint_byte(encoded: &[u8], offset: &mut usize) -> Result<u8, SemanticCodeError> {
    let byte = *encoded.get(*offset).ok_or_else(|| {
        SemanticCodeError::Corrupt("source reconciliation checkpoint is truncated".to_string())
    })?;
    *offset += 1;
    Ok(byte)
}

fn take_checkpoint_fixed<'a>(
    encoded: &'a [u8],
    offset: &mut usize,
    length: usize,
) -> Result<&'a [u8], SemanticCodeError> {
    let end = (*offset).checked_add(length).ok_or_else(|| {
        SemanticCodeError::Corrupt("source reconciliation checkpoint overflows".to_string())
    })?;
    let bytes = encoded.get(*offset..end).ok_or_else(|| {
        SemanticCodeError::Corrupt("source reconciliation checkpoint is truncated".to_string())
    })?;
    *offset = end;
    Ok(bytes)
}

fn take_checkpoint_u64(encoded: &[u8], offset: &mut usize) -> Result<u64, SemanticCodeError> {
    let bytes = take_checkpoint_fixed(encoded, offset, 8)?;
    Ok(u64::from_be_bytes(bytes.try_into().map_err(|_| {
        SemanticCodeError::Corrupt("source reconciliation counter is not 64 bits".to_string())
    })?))
}

fn take_checkpoint_bytes<'a>(
    encoded: &'a [u8],
    offset: &mut usize,
    max_length: usize,
) -> Result<&'a [u8], SemanticCodeError> {
    let length = u32::from_be_bytes(
        take_checkpoint_fixed(encoded, offset, 4)?
            .try_into()
            .map_err(|_| {
                SemanticCodeError::Corrupt(
                    "source reconciliation field length is not 32 bits".to_string(),
                )
            })?,
    ) as usize;
    if length > max_length {
        return Err(SemanticCodeError::Corrupt(
            "source reconciliation field exceeds the bounded size".to_string(),
        ));
    }
    take_checkpoint_fixed(encoded, offset, length)
}

fn take_optional_checkpoint_bytes(
    encoded: &[u8],
    offset: &mut usize,
    max_length: usize,
) -> Result<Option<Vec<u8>>, SemanticCodeError> {
    match take_checkpoint_byte(encoded, offset)? {
        0 => Ok(None),
        1 => Ok(Some(
            take_checkpoint_bytes(encoded, offset, max_length)?.to_vec(),
        )),
        _ => Err(SemanticCodeError::Corrupt(
            "source reconciliation optional field has an unknown marker".to_string(),
        )),
    }
}

/// Move the generation named by an existing active pointer out of `Live`
/// while the successor generation is being published. This helper is called
/// inside the same admitted S6 write as the successor binding and pointer;
/// callers cannot observe a pointer that names two live generations.
fn demote_prior_live_binding_in_write(
    rows: &AdmittedOwnerWrite<'_, SemanticIndexOwner>,
    tenant: &str,
    binding: &str,
    generation: u64,
) -> Result<(), SemanticCodeError> {
    let prior_raw = rows
        .open_table(SEMANTIC_BINDINGS)
        .map_err(kernel_error)?
        .get((tenant, binding, generation))
        .map_err(kernel_error)?
        .map(|value| value.value().to_vec())
        .ok_or_else(|| {
            SemanticCodeError::Corrupt(
                "S6 active pointer names a missing prior binding".to_string(),
            )
        })?;
    let mut prior =
        SemanticBinding::from_canonical_cbor(&prior_raw).map_err(semantic_contract_error)?;
    if prior.durable_state != SemanticBindingState::Live {
        return Err(SemanticCodeError::Refused(
            "S6 prior active generation is not durably live".to_string(),
        ));
    }
    prior.durable_state = SemanticBindingState::Disabled;
    prior.validate().map_err(semantic_contract_error)?;
    let prior_bytes = prior.to_canonical_cbor().map_err(semantic_contract_error)?;
    let mut bindings = rows.open_table(SEMANTIC_BINDINGS).map_err(kernel_error)?;
    replace_bytes(&mut bindings, (tenant, binding, generation), &prior_bytes)
}

fn update_source_progress(
    rows: &AdmittedOwnerWrite<'_, SemanticIndexOwner>,
    tenant: &str,
    binding: &str,
    transition: &SemanticStageTransition,
    source_entity_id: &str,
    now_ms: u64,
) -> Result<(), SemanticCodeError> {
    let mut table = rows
        .open_table(SEMANTIC_SOURCE_PROGRESS)
        .map_err(kernel_error)?;
    let raw = table
        .get((
            tenant,
            binding,
            transition.intent.generation,
            source_entity_id,
        ))
        .map_err(kernel_error)?
        .map(|value| value.value().to_vec())
        .ok_or_else(|| {
            SemanticCodeError::Refused("stage transition source progress disappeared".to_string())
        })?;
    let mut progress =
        SemanticSourceProgress::from_canonical_cbor(&raw).map_err(semantic_contract_error)?;
    if matches!(
        transition.receipt.outcome,
        SemanticStageOutcome::Completed | SemanticStageOutcome::IdempotentNoop
    ) {
        progress.completed_stage = Some(transition.intent.stage);
        progress.completed_receipt_digest = Some(transition.receipt.receipt_digest());
    }
    progress.updated_at = format!("unix-ms:{now_ms}");
    progress.validate().map_err(semantic_contract_error)?;
    let bytes = progress
        .to_canonical_cbor()
        .map_err(semantic_contract_error)?;
    table
        .insert(
            (
                tenant,
                binding,
                transition.intent.generation,
                source_entity_id,
            ),
            bytes.as_slice(),
        )
        .map_err(kernel_error)?;
    Ok(())
}

/// Authenticate one scope and bind it, bootstrapping the ledger. TWO committed
/// write transactions -- which is exactly why no read path calls this.
fn bind_scope(
    kernel: &StorageKernel,
    mutations: &MutationKernel,
    verifier: &dyn ScopeGrantVerifier,
    principal: &str,
    proof: &[u8],
    identity: eg_types::MutationScopeIdentity,
) -> Result<OwnedStoreHandle<SemanticIndexOwner>, SemanticCodeError> {
    let grant = kernel
        .authenticate_scope::<SemanticIndexOwner>(verifier, identity, principal.to_string(), proof)
        .map_err(kernel_error)?;
    let owner = kernel.bind_serving_scope(grant, 0).map_err(kernel_error)?;
    mutations.bootstrap_ledger(&owner).map_err(kernel_error)?;
    Ok(owner)
}

/// One physical file per `(tenant, binding)`, named by a digest so a binding
/// name never becomes a path component.
fn store_file_name(tenant: &str, binding: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"eg/semantic-index-file/v1\0");
    hasher.update(tenant.as_bytes());
    hasher.update([0]);
    hasher.update(binding.as_bytes());
    format!("semantic_index-{}.redb", hex::encode(hasher.finalize()))
}

/// `sha256` over the image in a fixed part order, so the batch identity and the
/// "already durable" decision are both the content.
fn image_digest(image: &SemanticGenerationImage) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"eg/semantic-ann-generation/v1\0");
    for bytes in [
        image.index.codes.meta.as_slice(),
        image.index.codes.codes.as_slice(),
        image.index.codes.refine.as_slice(),
        image.index.ids.as_slice(),
        image.manifest.as_slice(),
    ] {
        hasher.update((bytes.len() as u64).to_le_bytes());
        hasher.update(bytes);
    }
    hex::encode(hasher.finalize())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SqlSourceRevision<'a> {
    authority: &'a str,
    epoch: u64,
}

/// Parse the tenant-wide SQL source revision emitted by the authoritative SQL
/// snapshot port.  The authority digest is part of the lineage, so equal
/// numeric epochs from two SQL owners cannot be compared as one stream.
fn sql_source_revision_parts(revision: &str) -> Option<SqlSourceRevision<'_>> {
    let rest = revision.strip_prefix("sql-source:")?;
    let (authority, epoch) = rest.rsplit_once(":epoch:")?;
    let digest = authority.strip_prefix("sha256:")?;
    if digest.len() != 64
        || digest
            .bytes()
            .any(|byte| !matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
    {
        return None;
    }
    let epoch = epoch.parse().ok()?;
    if epoch == 0 {
        return None;
    }
    Some(SqlSourceRevision { authority, epoch })
}

fn compare_source_revision(left: &str, right: &str) -> std::cmp::Ordering {
    match (
        sql_source_revision_parts(left),
        sql_source_revision_parts(right),
    ) {
        (Some(left_parts), Some(right_parts)) if left_parts.authority == right_parts.authority => {
            left_parts
                .epoch
                .cmp(&right_parts.epoch)
                .then_with(|| left.cmp(right))
        }
        _ => left.cmp(right),
    }
}

/// SQL refresh revisions are the tenant-wide source authority and epoch
/// returned by the atomic SQL snapshot read.  A bare or event-local counter
/// is insufficient because two SQL resources can commit the same table.
fn validate_sql_source_revision(revision: &str) -> Result<(), SemanticCodeError> {
    if sql_source_revision_parts(revision).is_none() {
        return Err(SemanticCodeError::Refused(
            "semantic SQL refresh revision must bind a canonical source authority and nonzero epoch"
                .to_string(),
        ));
    }
    Ok(())
}

#[cfg(test)]
#[path = "semantic_ann_codes/tests.rs"]
mod tests;
