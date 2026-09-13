//! The serving-scope write door — the one place this store touches a kernel.
//!
//! `ServingDoor` owns the storage kernel, the mutation kernel and the single
//! serving scope bound at `open`, in private fields. Every other module reaches
//! durable state only through its methods: reads through `serving_read`, owner
//! rows through `commit_metadata_fenced`, caller replays through
//! `replay_operation_if_recorded`, and outbox delivery through the `outbox_*`
//! wrappers. No method mints or binds a second scope, which is the parent
//! module's per-generation-scope non-goal enforced by visibility.

use super::{kernel_error, SemanticCodeError, SemanticMutationReceipt};
use eg_storage::{
    OwnedStoreHandle, PhysicalStoreIdentity, RecordedOperation, ScopeGrantVerifier, ScopedRead,
    SemanticIndexOwner, StorageKernel,
};
use eg_transaction::{
    AdmittedMutation, AdmittedOwnerWrite, Begin, MutationKernel, OutboxClaimBudget,
    OutboxClaimOutcome,
};
use eg_types::contract::Nonce;
use eg_types::mutation_batch::{
    CompiledOperation, CompiledScope, MutationEnvelope, MutationOutboxLease,
    MutationProjectionCursor, VersionExpectation,
};
use eg_types::semantic_index::SemanticDigest;
use eg_types::MutationBatch;
use sha2::{Digest, Sha256};
use std::path::Path;

/// The serving scope and the two kernels that can act on it, held privately.
///
/// Nothing outside this module can name a field, so nothing outside it can
/// open a write, bind a scope or commit a batch except through the methods
/// below.
pub(super) struct ServingDoor {
    kernel: StorageKernel,
    mutations: MutationKernel,
    /// The read-only serving scope, bound once at `open` — and the only scope
    /// this door ever holds. See the parent module's per-generation-scope
    /// non-goal.
    serving: OwnedStoreHandle<SemanticIndexOwner>,
}

/// Operator-facing identity of the one physical semantic-index owner file.
const SEMANTIC_PHYSICAL_STORE_PREFIX: &str = "eg-core:semantic-index:v2";

impl ServingDoor {
    /// Open (or create) the binding's owner file and bind its serving scope.
    /// See `SemanticCodeStore::open`, which is the only caller.
    pub(super) fn open(
        dir: &Path,
        verifier: &dyn ScopeGrantVerifier,
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
            verifier,
            principal,
            proof,
            serving_identity(tenant, binding)?,
        )?;
        Ok(Self {
            kernel,
            mutations,
            serving,
        })
    }

    /// The bound serving scope, for building a batch against it. Holding the
    /// handle grants no write: only this door's kernels admit one.
    pub(super) fn owner(&self) -> &OwnedStoreHandle<SemanticIndexOwner> {
        &self.serving
    }

    pub(super) fn outbox_subscribe(&self, consumer: &str, topic: &str) -> Result<(), String> {
        self.mutations
            .outbox_subscribe(&self.serving, consumer, topic)
    }

    pub(super) fn outbox_claim(
        &self,
        consumer: &str,
        budget: &mut OutboxClaimBudget,
    ) -> Result<OutboxClaimOutcome, String> {
        self.mutations.outbox_claim(&self.serving, consumer, budget)
    }

    pub(super) fn outbox_ack(
        &self,
        lease: &MutationOutboxLease,
        now_ms: u64,
    ) -> Result<MutationProjectionCursor, String> {
        self.mutations.outbox_ack(&self.serving, lease, now_ms)
    }

    pub(super) fn outbox_release(&self, lease: &MutationOutboxLease) -> Result<(), String> {
        self.mutations.outbox_release(&self.serving, lease)
    }

    /// Test seeding only: the storage kernel, for fixtures that read or write
    /// rows no production path does. Compiled out of every non-test build.
    #[cfg(test)]
    pub(super) fn storage_kernel(&self) -> &StorageKernel {
        &self.kernel
    }

    /// Test seeding only: the mutation kernel. Compiled out of every
    /// non-test build.
    #[cfg(test)]
    pub(super) fn mutation_kernel(&self) -> &MutationKernel {
        &self.mutations
    }

    pub(super) fn stage_receipt_for_batch(
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

    /// Resolve a caller operation's durable replay row before any lifecycle
    /// state/OCC checks. A retry can legitimately observe the state produced by
    /// its first attempt (Pending -> Building, Live -> Disabled, or a moved
    /// refresh head), so reading that state first would incorrectly reject a
    /// valid fresh-nonce replay. The original committed batch supplies the
    /// stable content and the new envelope supplies only the attempt facts;
    /// kernel admission then enforces actor/key/content conflict and consumes
    /// the fresh nonce without reapplying owner rows.
    pub(super) fn replay_operation_if_recorded<F>(
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

    pub(super) fn commit_metadata<B, F>(
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
    pub(super) fn commit_metadata_fenced<B, F>(
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

    /// One kernel-issued scoped read over the binding's serving scope. Owner
    /// tables are layout-bounded, so this one snapshot serves every generation.
    pub(super) fn serving_read(
        &self,
    ) -> Result<ScopedRead<'_, SemanticIndexOwner>, SemanticCodeError> {
        self.kernel.read_scope(&self.serving).map_err(kernel_error)
    }
}

pub(super) fn mutation_digest_from_batch(batch: &MutationBatch) -> Result<SemanticDigest, String> {
    batch
        .operations
        .first()
        .and_then(|operation| match &operation.method {
            // The digest is the trailing `/` segment on both surfaces: alone on
            // the maintenance surface, behind the recorded subject on the
            // operation surface (see [`metadata_operation_query`]).
            eg_types::protocol::Method::ApplyMutation { query, .. } => query
                .rsplit('/')
                .next()
                .and_then(|segment| segment.strip_prefix("sha256:")),
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
pub(super) fn serving_identity(
    tenant: &str,
    binding: &str,
) -> Result<eg_types::MutationScopeIdentity, SemanticCodeError> {
    scope_identity(
        tenant,
        &format!("{binding}:serving"),
        "semantic-ann-serving:v1",
    )
}

pub(super) fn scope_identity(
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
pub(super) fn store_file_name(tenant: &str, binding: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"eg/semantic-index-file/v1\0");
    hasher.update(tenant.as_bytes());
    hasher.update([0]);
    hasher.update(binding.as_bytes());
    format!("semantic_index-{}.redb", hex::encode(hasher.finalize()))
}
