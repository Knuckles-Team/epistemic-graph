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
//! This module holds no physical authority of its own: the [`eg_storage::StorageKernel`]
//! it owns is the sole opener of the file, and every write is admitted, ordered
//! and committed by [`eg_transaction::MutationKernel`].
//!
//! **Layout.** The one kernel-facing type is `door::ServingDoor`: it owns the
//! storage kernel, the mutation kernel and the serving scope in private fields,
//! so the chokepoint above is enforced by visibility rather than by
//! convention. The store's lifecycle modules (`binding`, `refresh`, `stage`,
//! `completion`, `activation`, `reconciliation`, `tombstone`) reach durable
//! state only through that door, and the in-write helpers (`persist`,
//! `predecessor`, `checkpoint`) only ever receive a write the door opened.

use door::ServingDoor;
use eg_storage::ScopeGrantVerifier;
use eg_types::contract::Nonce;
use eg_types::semantic_index::{SemanticDigest, SemanticIndexError};
use std::path::Path;
use std::sync::Arc;

mod activation;
mod batch;
mod binding;
mod checkpoint;
mod completion;
mod door;
mod persist;
mod predecessor;
pub(crate) mod reconciliation;
mod reconciliation_codec;
mod refresh;
mod rows;
mod stage;
mod tombstone;

#[cfg(test)]
mod tests;
#[cfg(test)]
mod write_door_tests;

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
    /// The store's only access to a kernel: the serving scope bound once at
    /// `open` and the one door every durable write passes through. See the
    /// module's per-generation-scope non-goal.
    door: ServingDoor,
    tenant: String,
    binding: String,
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
        let door = ServingDoor::open(dir, verifier.as_ref(), principal, proof, tenant, binding)?;
        Ok(Self {
            door,
            tenant: tenant.to_string(),
            binding: binding.to_string(),
        })
    }
}
