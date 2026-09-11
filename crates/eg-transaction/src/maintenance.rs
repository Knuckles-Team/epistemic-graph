//! The one owner-maintenance batch constructor (RF-RULING-005).
//!
//! Four consumers used to hand-roll this batch: the root binary's sidecar
//! stores, its KV store, its blob store, and `eg-tsdb`. Every copy built its
//! `batch_id` as `{event}:v{version}` from a version read OUTSIDE the write
//! transaction, and put nothing else caller-specific in the batch. Two
//! concurrent callers that observed the same version therefore produced
//! BYTE-IDENTICAL batches, and [`crate::commit::begin`] checks idempotency
//! BEFORE the version expectation: the second caller replayed the first's
//! record instead of applying its own write.
//!
//! This constructor exists once and is reachable only through
//! [`crate::MutationKernel::admit_current`], which resolves the version
//! INSIDE the exclusive write transaction. redb serializes writers, so exactly
//! one batch commits per version and the next writer necessarily builds its id
//! from the version the first one produced. Two attempts are byte-identical
//! only when they genuinely are the same write, retried after a crash that lost
//! the commit -- which is precisely when a replay is the right answer.

use eg_storage::{OwnedStoreHandle, OwnerDomain};
use eg_types::mutation_batch::DurabilityDomain;
use eg_types::protocol::Method;
use eg_types::mutation_batch::MutationEnvelope;
use eg_types::{
    MutationBatch, MutationOperation, MutationScope, MutationSurface, VersionExpectation,
    MUTATION_BATCH_VERSION,
};

/// One owner-maintenance write, named by what it does (`kind`) and the row set
/// it acts on (`subject`).
///
/// Both land in the durable batch id and in the operation's `query`, so a
/// store's ledger says which maintenance write produced each of its versions
/// and against what -- the audit property RF-RULING-005 asks the class to carry.
pub struct MaintenanceBatch<'a> {
    domain: DurabilityDomain,
    kind: &'a str,
    subject: &'a str,
}

impl<'a> MaintenanceBatch<'a> {
    /// Name one maintenance write. `domain` is the owner's mutation domain,
    /// `kind` the operation (`kv_put`, `blob_sweep_v1`, ...), `subject` the row
    /// set it touches (a content digest, the scope's ledger key).
    ///
    /// Both go into the batch id verbatim, and `MutationBatch::validate` admits
    /// no control character and no surrounding whitespace there, so a subject
    /// derived from arbitrary caller bytes must be digested by its caller
    /// rather than spelled out. [`Self::for_scope_version`] fails closed on one
    /// that is not -- it never mangles a subject to make it fit.
    pub fn new(domain: DurabilityDomain, kind: &'a str, subject: &'a str) -> Self {
        Self {
            domain,
            kind,
            subject,
        }
    }

    /// The batch for this write at `version`, the scope's authoritative version
    /// as resolved inside the write transaction that will commit it.
    ///
    /// Pass this as the `build` closure of
    /// [`crate::MutationKernel::admit_current`]; it is the only caller that
    /// can supply an in-lock version, and it refuses any other expectation.
    pub fn for_scope_version<D: OwnerDomain>(
        &self,
        owner: &OwnedStoreHandle<D>,
        version: u64,
    ) -> Result<MutationBatch, String> {
        let batch_id = format!("{}/{}:v{version}", self.kind, self.subject);
        // `admit_current` resolves the version from the bound identity, so the
        // constructor must carry the same version namespace. A graph-shaped
        // owner (including the ledger-only cluster-admin coordinator) advances
        // its graph version; native owner files advance their native version.
        let version_expectation = match owner.identity().scope() {
            MutationScope::Graph { .. } => VersionExpectation::Graph(version),
            MutationScope::Native { .. } => VersionExpectation::Native(version),
        };
        let batch = MutationBatch {
            schema_version: MUTATION_BATCH_VERSION,
            batch_id: batch_id.clone(),
            // A maintenance write has no caller, so it has no authority to
            // derive an operation identity from and no attempt nonce to consume
            // (RF-RULING-005). Its `kind` and `subject` are mandatory here, so
            // the ledger row NAMES what it wrote rather than merely recording
            // that something was written -- the C1 P1-2 complaint.
            envelope: MutationEnvelope::maintenance(
                owner.principal(),
                self.kind,
                self.subject,
                &batch_id,
            )?,
            identity: owner.identity().clone(),
            placement_epoch: 0,
            version_expectation,
            fencing_token: None,
            authoritative_state: None,
            operations: vec![MutationOperation {
                ordinal: 0,
                surface: MutationSurface::Other,
                domain: self.domain,
                method: Method::ApplyMutation {
                    event_type: self.kind.to_string(),
                    query: batch_id,
                },
            }],
            outbox: Vec::new(),
            created_at_ms: 0,
        };
        batch.validate()?;
        Ok(batch)
    }
}
