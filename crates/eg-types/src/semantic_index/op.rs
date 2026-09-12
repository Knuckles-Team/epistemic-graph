//! The `Method::SemanticIndex` operation set: the wire contract that makes the
//! S1-S6 tiered semantic ingestion pipeline drivable by an external connector.
//!
//! Before this module the whole pipeline was reachable only from inside the
//! engine: [`crate::semantic_index::SemanticIndexCommand`] carried the
//! CONTENT-level binding/search DTO, but the stage QUEUE -- admission,
//! subscription, lease claiming, stage completion -- had no representable
//! request at all. A connector could describe what it wanted indexed and never
//! move it through a single stage.
//!
//! Three properties are deliberate and load-bearing:
//!
//! * **Nothing that carries authority travels on the wire.** The actor, the
//!   attempt nonce, the admission clock, and the authoritative SQL source row
//!   are all supplied by the engine from the verified carrier. What a caller
//!   sends is a claim to bind against that authority, never the authority
//!   itself. That is why no variant here carries an `actor_scope` to act as,
//!   a [`crate::contract::Nonce`], a `now_ms`, or source text.
//! * **Every variant names its tenant**, and the handler compares it with the
//!   verified request tenant before anything is resolved. Resolution by id
//!   alone is an execution grant.
//! * **Every unbounded thing has a named bound** and is refused by that name:
//!   claim limits, lease durations, page sizes, cursor lengths.

use serde::{Deserialize, Serialize};

use super::identity::{validate_generation, validate_text};
use super::{
    SemanticBinding, SemanticBindingDraft, SemanticBindingState, SemanticGenerationArtifact,
    SemanticIndexError, SemanticIndexFilter, SemanticQueueClass, SemanticSqlSourceManifestDraft,
    SemanticStageArtifact, SemanticStageIntent, SemanticStageTransition,
};
use crate::mutation_batch::{MutationOutboxLease, MutationOutboxRecord};

/// The largest number of stage leases one claim may take.
///
/// This is not a fresh opinion: `SemanticCodeStore::claim_stage_leases` already
/// refuses a budget above 256, so a wire request that asked for more would be
/// rejected deep inside the store with a message about a "bounded consumer
/// budget" rather than about the field the caller actually set. Naming the same
/// bound here lets the refusal name the field.
pub const MAX_SEMANTIC_STAGE_CLAIM_LIMIT: u32 = 256;

/// The longest lease a claim may request, in milliseconds.
///
/// A lease is a promise that no other worker touches the row until it lapses.
/// An unbounded one lets a single crashed connector park a generation forever,
/// because the only recovery from a too-long lease is waiting for it.
pub const MAX_SEMANTIC_STAGE_LEASE_MS: u64 = 15 * 60 * 1000;

/// The largest binding page one `ListBindings` may return.
pub const MAX_SEMANTIC_BINDING_PAGE_LIMIT: u32 = 256;

/// The longest opaque cursor accepted on any paginated semantic read.
///
/// Semantic cursors are engine-minted and MAC-bound to the tenant they were
/// issued for (`server::sql_catalog_acl`'s `SemanticCursorMac`), so this bound
/// is about refusing an obviously-forged value cheaply, before the MAC check
/// has to hash it.
pub const MAX_SEMANTIC_CURSOR_CHARS: usize = 8192;

/// One page of the bounded binding catalog.
///
/// An OBJECT, never a bare array, for the reason
/// [`crate::agent_component::AgentComponentSearchPage`] is one: a read that
/// starts life as a JSON array can only grow a cursor by breaking every reader
/// that already parses it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SemanticBindingPage {
    pub entries: Vec<SemanticBinding>,
    /// Where to resume. OPAQUE: engine-produced and only ever handed back
    /// unmodified, and it cannot address a binding outside the owner that
    /// minted it.
    pub next_cursor: Option<String>,
}

/// One claimed stage lease, with the intent it leases and the queue class that
/// intent's stage belongs to.
///
/// The class is returned rather than assumed because it is what a tiered worker
/// routes on: an S1 `Fast` row and an S5 `SlowHeavy` row are the same shape and
/// wildly different work.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SemanticStageLeaseEntry {
    pub lease: MutationOutboxLease,
    pub intent: SemanticStageIntent,
    pub queue_class: SemanticQueueClass,
}

/// The bounded result of one claim turn.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SemanticStageLeasePage {
    /// The class this turn asked for. Echoed so a worker can prove the tier it
    /// got is the tier it wanted.
    pub queue_class: SemanticQueueClass,
    pub entries: Vec<SemanticStageLeaseEntry>,
    /// Whether the selection scan stopped at its page bound, so an immediate
    /// second claim may find more.
    pub more_available: bool,
    /// How many claimed rows belonged to another queue class and were released
    /// back unexecuted. A persistently non-zero value means this consumer is
    /// competing with the wrong tier, which is an operational fact worth
    /// surfacing rather than silently absorbing.
    pub released_other_class: u32,
}

/// Operations over one durable semantic binding and its S1-S6 stage queue.
///
/// Shaped like [`crate::agent_component::AgentComponentOp`] and its three
/// sibling layers: one `Method` carrying a closed op enum, with the read/write
/// classification living on the op itself ([`Self::is_mutation`]) so the
/// capability policy and `server::access::requires_write` cannot drift apart
/// about which operations write.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum SemanticIndexOp {
    // ---------------------------------------------------------------- binding
    /// Mint and persist a binding, publishing its durable creation event.
    ///
    /// The DRAFT is what travels, not a built `SemanticBinding`: the engine
    /// calls `SemanticBinding::create`, so the binding digest is computed over
    /// what the engine validated rather than accepted from the caller.
    AdmitBinding {
        tenant_id: String,
        binding_id: String,
        /// Boxed: a binding draft carries roughly twenty governed fields and
        /// would otherwise set the size of every `Method`.
        draft: Box<SemanticBindingDraft>,
        /// The caller's stable retry identity. The per-attempt nonce is minted
        /// by the engine and is deliberately not representable here.
        idempotency_key: String,
    },
    /// Replace a live generation with an operation-bound pending one. The old
    /// active pointer remains the serving proof until S6 publishes.
    RefreshBinding {
        tenant_id: String,
        binding_id: String,
        expected_generation: u64,
        draft: Box<SemanticBindingDraft>,
        source_manifest: Box<SemanticSqlSourceManifestDraft>,
        idempotency_key: String,
    },
    /// Move the durable binding state machine (e.g. `Pending` -> `Building`),
    /// fenced on the exact generation the caller believes is live.
    TransitionBinding {
        tenant_id: String,
        binding_id: String,
        expected_generation: u64,
        next_state: SemanticBindingState,
        idempotency_key: String,
    },
    /// Retire the binding at an exact generation.
    DropBinding {
        tenant_id: String,
        binding_id: String,
        expected_generation: u64,
        idempotency_key: String,
    },

    // ----------------------------------------------------------- S1 admission
    /// One coarse wakeup whose source owner guarantees exactly one complete
    /// row. Multi-row providers must use [`Self::AdmitSourcePage`].
    AdmitSourceRecord {
        tenant_id: String,
        binding_id: String,
        /// The committed SQL mutation this wakeup names. Boxed for size.
        record: Box<MutationOutboxRecord>,
    },
    /// One bounded page of a multi-row wakeup, resumed by the previous page's
    /// opaque cursor.
    AdmitSourcePage {
        tenant_id: String,
        binding_id: String,
        record: Box<MutationOutboxRecord>,
        cursor: Option<String>,
    },
    /// A complete-snapshot reconciliation turn: page the authoritative source,
    /// then emit absence tombstones once the complete-page proof holds. Bounded
    /// per turn by the service's own page/row/byte budgets, so a large source
    /// resumes rather than being refused.
    AdmitSourceReconcile {
        tenant_id: String,
        binding_id: String,
        record: Box<MutationOutboxRecord>,
    },
    /// Admit S1 after a refresh has advanced the durable binding head.
    AdmitSourceReplacement {
        tenant_id: String,
        binding_id: String,
        draft: Box<SemanticBindingDraft>,
        source_manifest: Box<SemanticSqlSourceManifestDraft>,
        record: Box<MutationOutboxRecord>,
    },

    // ------------------------------------------------------- consumer / worker
    /// Durably subscribe a named consumer to this binding's stage topic. A
    /// consumer with no durable subscription claims nothing.
    SubscribeStageConsumer {
        tenant_id: String,
        binding_id: String,
        consumer: String,
    },
    /// Take a bounded set of stage leases for one queue class.
    ClaimStageLeases {
        tenant_id: String,
        binding_id: String,
        consumer: String,
        /// The tier this worker serves. Rows whose stage belongs to another
        /// class are released back unexecuted rather than handed to a worker
        /// that cannot do them -- tier selection is the whole point of a
        /// tiered queue, so it has to be expressible on the wire.
        queue_class: SemanticQueueClass,
        /// At most [`MAX_SEMANTIC_STAGE_CLAIM_LIMIT`].
        limit: u32,
        /// At most [`MAX_SEMANTIC_STAGE_LEASE_MS`].
        lease_ms: u64,
    },
    /// Re-check a held lease and return the canonical intent it leases, without
    /// committing anything. A worker calls this before starting expensive work.
    ValidateStageLease {
        tenant_id: String,
        binding_id: String,
        consumer: String,
        lease: Box<MutationOutboxLease>,
    },
    /// The bounded queue's per-consumer liveness, capacity and in-flight count.
    StageStatus {
        tenant_id: String,
        binding_id: String,
        consumer: String,
    },
    /// Persist one terminal entity-scoped transition with its artifact, publish
    /// the successor intent when the predecessor proof is complete, and
    /// acknowledge the exact lease -- one durable mutation.
    CompleteStage {
        tenant_id: String,
        binding_id: String,
        lease: Box<MutationOutboxLease>,
        transition: Box<SemanticStageTransition>,
        artifact: Box<SemanticStageArtifact>,
        successor: Option<Box<SemanticStageIntent>>,
    },
    /// Complete S3 or S5 and persist its canonical lexical/ANN manifest in the
    /// same owner mutation as the transition, checkpoint, successor and lease
    /// acknowledgement.
    CompleteGenerationStage {
        tenant_id: String,
        binding_id: String,
        lease: Box<MutationOutboxLease>,
        transition: Box<SemanticStageTransition>,
        artifact: Box<SemanticGenerationArtifact>,
        successor: Option<Box<SemanticStageIntent>>,
    },
    /// Complete an S1 source transition. No source row travels: the engine
    /// re-reads the authoritative, ACL-checked SQL page itself and converts its
    /// decision into the authorization receipt. A caller that could supply the
    /// row could index text it is not allowed to read.
    CompleteSqlSourceStage {
        tenant_id: String,
        binding_id: String,
        lease: Box<MutationOutboxLease>,
        transition: Box<SemanticStageTransition>,
        successor: Option<Box<SemanticStageIntent>>,
        /// The opaque page cursor the intent was claimed from, so the exact
        /// bounded page is re-read.
        page_cursor: Option<String>,
    },
    /// Resolve an S1 completion that may already have committed before a crash.
    /// `None` means it has not, and the caller may proceed through
    /// [`Self::CompleteSqlSourceStage`].
    ReplayCompletedSqlSourceStage {
        tenant_id: String,
        binding_id: String,
        lease: Box<MutationOutboxLease>,
        expected_intent: Box<SemanticStageIntent>,
    },
    /// Hand a lease back unexecuted. Without this a connector that cannot do
    /// the work it claimed wedges the row until the lease lapses.
    ReleaseStageLease {
        tenant_id: String,
        binding_id: String,
        lease: Box<MutationOutboxLease>,
    },

    // ------------------------------------------------------------------ reads
    /// The durable binding head of this owner.
    Binding {
        tenant_id: String,
        binding_id: String,
    },
    /// The one retained prior SQL source manifest a complete-page deletion
    /// proof needs. It never reconstructs deleted bytes.
    SqlSourceManifest {
        tenant_id: String,
        binding_id: String,
        generation: u64,
        source_entity_id: String,
    },
    /// The bounded binding catalog. Paginated; see [`SemanticBindingPage`].
    ListBindings {
        tenant_id: String,
        binding_id: String,
        filter: SemanticIndexFilter,
        cursor: Option<String>,
    },
    /// The generation currently serving reads, if any.
    LiveGeneration {
        tenant_id: String,
        binding_id: String,
    },
}

impl SemanticIndexOp {
    /// Whether this operation commits durable state.
    ///
    /// The ONE classifier. `server::access::requires_write` delegates to it and
    /// `eg_capabilities::semantic_index_policy` derives from it, so the access
    /// decision and the capability ledger cannot disagree about an operation.
    ///
    /// Three calls that read as reads are writes here, and each for a durable
    /// reason:
    /// * `SubscribeStageConsumer` persists a subscription row.
    /// * `ClaimStageLeases` writes lease epochs and expiries.
    /// * `ReplayCompletedSqlSourceStage` acknowledges the lease of an already
    ///   committed transition, advancing the durable outbox cursor.
    pub fn is_mutation(&self) -> bool {
        match self {
            Self::AdmitBinding { .. }
            | Self::RefreshBinding { .. }
            | Self::TransitionBinding { .. }
            | Self::DropBinding { .. }
            | Self::AdmitSourceRecord { .. }
            | Self::AdmitSourcePage { .. }
            | Self::AdmitSourceReconcile { .. }
            | Self::AdmitSourceReplacement { .. }
            | Self::SubscribeStageConsumer { .. }
            | Self::ClaimStageLeases { .. }
            | Self::CompleteStage { .. }
            | Self::CompleteGenerationStage { .. }
            | Self::CompleteSqlSourceStage { .. }
            | Self::ReplayCompletedSqlSourceStage { .. }
            | Self::ReleaseStageLease { .. } => true,
            Self::ValidateStageLease { .. }
            | Self::StageStatus { .. }
            | Self::Binding { .. }
            | Self::SqlSourceManifest { .. }
            | Self::ListBindings { .. }
            | Self::LiveGeneration { .. } => false,
        }
    }

    /// The authorization action this operation needs.
    ///
    /// Six actions, not one, because these are genuinely different privileges
    /// and collapsing them grants the union. Curating WHAT is indexed
    /// (`binding-write`) is not the same as feeding rows into an
    /// already-approved binding (`source-admit`), which is not the same as
    /// taking work off the queue (`stage-claim`), which is not the same as
    /// declaring a stage's result durable (`stage-complete`). Naming follows
    /// the `agent:component-write` / `agent:component-read` precedent.
    pub fn authz_action(&self) -> &'static str {
        match self {
            Self::AdmitBinding { .. }
            | Self::RefreshBinding { .. }
            | Self::TransitionBinding { .. }
            | Self::DropBinding { .. } => "semantic:binding-write",
            Self::Binding { .. } | Self::ListBindings { .. } | Self::LiveGeneration { .. } => {
                "semantic:binding-read"
            }
            Self::AdmitSourceRecord { .. }
            | Self::AdmitSourcePage { .. }
            | Self::AdmitSourceReconcile { .. }
            | Self::AdmitSourceReplacement { .. } => "semantic:source-admit",
            Self::SubscribeStageConsumer { .. }
            | Self::ClaimStageLeases { .. }
            | Self::ReleaseStageLease { .. } => "semantic:stage-claim",
            Self::CompleteStage { .. }
            | Self::CompleteGenerationStage { .. }
            | Self::CompleteSqlSourceStage { .. }
            | Self::ReplayCompletedSqlSourceStage { .. } => "semantic:stage-complete",
            Self::ValidateStageLease { .. }
            | Self::StageStatus { .. }
            | Self::SqlSourceManifest { .. } => "semantic:stage-read",
        }
    }

    /// The tenant this operation names. Compared against the verified request
    /// tenant in the handler, once, so a new variant cannot forget it.
    pub fn tenant_id(&self) -> &str {
        match self {
            Self::AdmitBinding { tenant_id, .. }
            | Self::RefreshBinding { tenant_id, .. }
            | Self::TransitionBinding { tenant_id, .. }
            | Self::DropBinding { tenant_id, .. }
            | Self::AdmitSourceRecord { tenant_id, .. }
            | Self::AdmitSourcePage { tenant_id, .. }
            | Self::AdmitSourceReconcile { tenant_id, .. }
            | Self::AdmitSourceReplacement { tenant_id, .. }
            | Self::SubscribeStageConsumer { tenant_id, .. }
            | Self::ClaimStageLeases { tenant_id, .. }
            | Self::ValidateStageLease { tenant_id, .. }
            | Self::StageStatus { tenant_id, .. }
            | Self::CompleteStage { tenant_id, .. }
            | Self::CompleteGenerationStage { tenant_id, .. }
            | Self::CompleteSqlSourceStage { tenant_id, .. }
            | Self::ReplayCompletedSqlSourceStage { tenant_id, .. }
            | Self::ReleaseStageLease { tenant_id, .. }
            | Self::Binding { tenant_id, .. }
            | Self::SqlSourceManifest { tenant_id, .. }
            | Self::ListBindings { tenant_id, .. }
            | Self::LiveGeneration { tenant_id, .. } => tenant_id,
        }
    }

    /// The binding owner this operation addresses.
    ///
    /// Every semantic owner is `(tenant, binding)`-scoped, so EVERY operation
    /// names one -- including the lease-bearing ones, which would otherwise have
    /// to be resolved from a leased intent the handler cannot read until it has
    /// already opened an owner. Naming it is not trusting it: the durable store
    /// refuses a lease or transition whose canonical intent belongs to a
    /// different binding than the owner it was presented to.
    pub fn binding_id(&self) -> &str {
        match self {
            Self::AdmitBinding { binding_id, .. }
            | Self::RefreshBinding { binding_id, .. }
            | Self::TransitionBinding { binding_id, .. }
            | Self::DropBinding { binding_id, .. }
            | Self::AdmitSourceRecord { binding_id, .. }
            | Self::AdmitSourcePage { binding_id, .. }
            | Self::AdmitSourceReconcile { binding_id, .. }
            | Self::AdmitSourceReplacement { binding_id, .. }
            | Self::SubscribeStageConsumer { binding_id, .. }
            | Self::ClaimStageLeases { binding_id, .. }
            | Self::ValidateStageLease { binding_id, .. }
            | Self::StageStatus { binding_id, .. }
            | Self::CompleteStage { binding_id, .. }
            | Self::CompleteGenerationStage { binding_id, .. }
            | Self::CompleteSqlSourceStage { binding_id, .. }
            | Self::ReplayCompletedSqlSourceStage { binding_id, .. }
            | Self::ReleaseStageLease { binding_id, .. }
            | Self::Binding { binding_id, .. }
            | Self::SqlSourceManifest { binding_id, .. }
            | Self::ListBindings { binding_id, .. }
            | Self::LiveGeneration { binding_id, .. } => binding_id,
        }
    }

    /// Shape validation only. A successful result authorizes nothing: the
    /// handler still binds every claim here to the verified carrier authority.
    pub fn validate(&self) -> Result<(), SemanticIndexError> {
        validate_text("tenant_id", self.tenant_id())?;
        validate_text("binding_id", self.binding_id())?;
        match self {
            Self::AdmitBinding {
                tenant_id,
                binding_id,
                draft,
                idempotency_key,
            } => {
                validate_idempotency_key(idempotency_key)?;
                validate_draft_identity(tenant_id, binding_id, draft)
            }
            Self::RefreshBinding {
                tenant_id,
                binding_id,
                expected_generation,
                draft,
                source_manifest,
                idempotency_key,
            } => {
                validate_generation(*expected_generation)?;
                validate_idempotency_key(idempotency_key)?;
                validate_draft_identity(tenant_id, binding_id, draft)?;
                validate_text("source_manifest.binding_id", &source_manifest.binding_id)?;
                validate_generation(source_manifest.generation)
            }
            Self::TransitionBinding {
                expected_generation,
                idempotency_key,
                ..
            }
            | Self::DropBinding {
                expected_generation,
                idempotency_key,
                ..
            } => {
                validate_generation(*expected_generation)?;
                validate_idempotency_key(idempotency_key)
            }
            Self::AdmitSourceRecord { record, .. } | Self::AdmitSourceReconcile { record, .. } => {
                validate_outbox_record(record)
            }
            Self::AdmitSourcePage { record, cursor, .. } => {
                validate_outbox_record(record)?;
                validate_cursor("cursor", cursor)
            }
            Self::AdmitSourceReplacement {
                tenant_id,
                binding_id,
                draft,
                source_manifest,
                record,
            } => {
                validate_draft_identity(tenant_id, binding_id, draft)?;
                validate_text("source_manifest.binding_id", &source_manifest.binding_id)?;
                validate_generation(source_manifest.generation)?;
                validate_outbox_record(record)
            }
            Self::SubscribeStageConsumer { consumer, .. } | Self::StageStatus { consumer, .. } => {
                validate_text("consumer", consumer)
            }
            Self::ClaimStageLeases {
                consumer,
                limit,
                lease_ms,
                ..
            } => {
                validate_text("consumer", consumer)?;
                if *limit == 0 || *limit > MAX_SEMANTIC_STAGE_CLAIM_LIMIT {
                    return Err(SemanticIndexError::InvalidField {
                        field: "limit".to_string(),
                        reason: format!("must be within 1..={MAX_SEMANTIC_STAGE_CLAIM_LIMIT}"),
                    });
                }
                if *lease_ms == 0 || *lease_ms > MAX_SEMANTIC_STAGE_LEASE_MS {
                    return Err(SemanticIndexError::InvalidField {
                        field: "lease_ms".to_string(),
                        reason: format!("must be within 1..={MAX_SEMANTIC_STAGE_LEASE_MS}"),
                    });
                }
                Ok(())
            }
            Self::ValidateStageLease {
                consumer, lease, ..
            } => {
                validate_text("consumer", consumer)?;
                validate_lease(lease)
            }
            Self::CompleteStage {
                lease,
                transition,
                successor,
                ..
            } => {
                validate_lease(lease)?;
                transition.validate()?;
                // The artifact is checked with `validate_against(binding)`, not
                // in isolation: an artifact is only well-formed relative to the
                // binding it belongs to. That check is the STORE's, which holds
                // the durable binding; doing it here would need a binding the
                // wire cannot be trusted to supply.
                validate_successor(successor)
            }
            Self::CompleteGenerationStage {
                lease,
                transition,
                successor,
                ..
            } => {
                validate_lease(lease)?;
                transition.validate()?;
                validate_successor(successor)
            }
            Self::CompleteSqlSourceStage {
                lease,
                transition,
                successor,
                page_cursor,
                ..
            } => {
                validate_lease(lease)?;
                transition.validate()?;
                validate_successor(successor)?;
                validate_cursor("page_cursor", page_cursor)
            }
            Self::ReplayCompletedSqlSourceStage {
                lease,
                expected_intent,
                ..
            } => {
                validate_lease(lease)?;
                expected_intent.validate()
            }
            Self::ReleaseStageLease { lease, .. } => validate_lease(lease),
            Self::Binding { .. } | Self::LiveGeneration { .. } => Ok(()),
            Self::SqlSourceManifest {
                generation,
                source_entity_id,
                ..
            } => {
                validate_generation(*generation)?;
                validate_text("source_entity_id", source_entity_id)
            }
            Self::ListBindings { filter, cursor, .. } => {
                filter.validate()?;
                if filter.max_results > MAX_SEMANTIC_BINDING_PAGE_LIMIT {
                    return Err(SemanticIndexError::InvalidField {
                        field: "filter.max_results".to_string(),
                        reason: format!("must be at most {MAX_SEMANTIC_BINDING_PAGE_LIMIT}"),
                    });
                }
                validate_cursor("cursor", cursor)
            }
        }
    }
}

fn validate_idempotency_key(value: &str) -> Result<(), SemanticIndexError> {
    validate_text("idempotency_key", value)
}

/// A draft must name the very binding the operation addresses.
///
/// Without this an operation could open one owner and admit a draft belonging
/// to another -- the resolution-by-id-alone hole, one level down.
///
/// The draft's TENANT is deliberately not compared: the handler overwrites it
/// (along with both actor scopes) from the verified carrier before admission,
/// so comparing it here would only reject a caller for guessing the engine's
/// own opaque tenant scope wrong -- a value a connector has no way to know and
/// is never trusted to supply.
fn validate_draft_identity(
    _tenant_id: &str,
    binding_id: &str,
    draft: &SemanticBindingDraft,
) -> Result<(), SemanticIndexError> {
    validate_text("draft.binding_id", &draft.binding_id)?;
    if draft.binding_id != binding_id {
        return Err(SemanticIndexError::AuthorizationContextMismatch);
    }
    Ok(())
}

fn validate_cursor(field: &str, cursor: &Option<String>) -> Result<(), SemanticIndexError> {
    let Some(cursor) = cursor else {
        return Ok(());
    };
    if cursor.is_empty() || cursor.len() > MAX_SEMANTIC_CURSOR_CHARS {
        return Err(SemanticIndexError::InvalidField {
            field: field.to_string(),
            reason: format!("must be within 1..={MAX_SEMANTIC_CURSOR_CHARS} bytes"),
        });
    }
    if cursor.bytes().any(|byte| !byte.is_ascii_hexdigit()) {
        return Err(SemanticIndexError::InvalidField {
            field: field.to_string(),
            reason: "an opaque semantic cursor is lowercase hexadecimal".to_string(),
        });
    }
    Ok(())
}

fn validate_outbox_record(record: &MutationOutboxRecord) -> Result<(), SemanticIndexError> {
    record
        .validate()
        .map_err(|reason| SemanticIndexError::InvalidField {
            field: "record".to_string(),
            reason,
        })
}

fn validate_lease(lease: &MutationOutboxLease) -> Result<(), SemanticIndexError> {
    validate_text("lease.consumer", &lease.consumer)?;
    if lease.lease_epoch == 0 || lease.attempt == 0 {
        return Err(SemanticIndexError::InvalidField {
            field: "lease".to_string(),
            reason: "an unissued lease names no durable claim".to_string(),
        });
    }
    validate_outbox_record(&lease.record)
}

fn validate_successor(
    successor: &Option<Box<SemanticStageIntent>>,
) -> Result<(), SemanticIndexError> {
    match successor {
        Some(intent) => intent.validate(),
        None => Ok(()),
    }
}
