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
//! [`crate::MutationKernelV1::admit_current`], which resolves the version
//! INSIDE the exclusive write transaction. redb serializes writers, so exactly
//! one batch commits per version and the next writer necessarily builds its id
//! from the version the first one produced. Two attempts are byte-identical
//! only when they genuinely are the same write, retried after a crash that lost
//! the commit -- which is precisely when a replay is the right answer.

use eg_storage::{OwnedStoreHandle, OwnerDomain};
use eg_types::mutation_batch::MutationDomain;
use eg_types::protocol::Method;
use eg_types::{
    MutationBatch, MutationOperation, MutationRequestContext, MutationSurface, VersionExpectation,
    MUTATION_BATCH_VERSION,
};

/// One owner-maintenance write, named by what it does (`kind`) and the row set
/// it acts on (`subject`).
///
/// Both land in the durable batch id and in the operation's `query`, so a
/// store's ledger says which maintenance write produced each of its versions
/// and against what -- the audit property RF-RULING-005 asks the class to carry.
pub struct MaintenanceBatch<'a> {
    domain: MutationDomain,
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
    pub fn new(domain: MutationDomain, kind: &'a str, subject: &'a str) -> Self {
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
    /// [`crate::MutationKernelV1::admit_current`]; it is the only caller that
    /// can supply an in-lock version, and it refuses any other expectation.
    pub fn for_scope_version<D: OwnerDomain>(
        &self,
        owner: &OwnedStoreHandle<D>,
        version: u64,
    ) -> Result<MutationBatch, String> {
        let batch_id = format!("{}/{}:v{version}", self.kind, self.subject);
        let batch = MutationBatch {
            schema_version: MUTATION_BATCH_VERSION,
            batch_id: batch_id.clone(),
            context: MutationRequestContext {
                request_id: 0,
                principal: owner.principal().to_string(),
                purpose: None,
                policy_fingerprint: None,
                trace_id: None,
                // A maintenance mutation claims no capability: a plain
                // `Native`-versioned write, not the reserved-system
                // `Unversioned` path. Empty is the true fact, not a placeholder.
                verified_capabilities: std::collections::BTreeSet::new(),
            },
            identity: owner.identity().clone(),
            placement_epoch: 0,
            idempotency_key: batch_id.clone(),
            version_expectation: VersionExpectation::Native(version),
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
