//! The one admitted-write lifecycle every ConnectorPack catalog write shares.
//!
//! Import, projection completion, binding and retirement all commit the same
//! way: replay admission under a deterministic operation identity, one staged
//! Agent Library batch, owner rows, a typed domain receipt and the ledger
//! finish. They differ only in their identity, their effects and the rows they
//! write, so those three are the parameters and the lifecycle is written once.

use serde::de::DeserializeOwned;
use serde::Serialize;

use eg_types::agent_library::AgentLibraryMutationContext;
use eg_types::mutation::{MutationReceipt, MutationResult};
use eg_types::mutation_batch::{MutationOperation, MutationOutboxIntent};

use crate::server::persistence::agent_library::{
    admitted_context, agent_library_operation_identity, native_lifecycle_batch,
    owner_batch_receipt, resolve_nonce_first, validate_context, AgentLibraryStore,
};
use crate::server::persistence::agent_revision::{
    apply_owner_rows, finish_committed_ledger, finish_replayed, policy_admitted_context,
    recorded_receipt, stage_batch, within_write,
};

/// The owner-row capability one pack write applies its rows through.
pub(crate) type PackOwnerWrite<'w> =
    eg_transaction::AdmittedOwnerWrite<'w, eg_storage::AgentLibraryOwner>;

/// Who one pack write is, for replay: the same identity replays the stored
/// result, a different body under the same key is `IDEMPOTENCY_CONFLICT`.
pub(crate) struct PackWriteIdentity<'a> {
    /// The purpose the admitted context is stamped with.
    pub(crate) purpose: &'static str,
    /// The operation kind folded into the replay identity.
    pub(crate) kind: &'static str,
    /// The receipt slug.
    pub(crate) slug: &'static str,
    /// The subject (connector) the operation names.
    pub(crate) subject: &'a str,
    /// The revision the operation is admitted against.
    pub(crate) revision: u64,
    /// The operation's content discriminator, when it has one.
    pub(crate) discriminator: Option<&'a str>,
    /// The typed mutation-result schema of the recorded result.
    pub(crate) result_schema: &'static str,
    /// The noun refusals name.
    pub(crate) noun: &'static str,
}

/// The kernel-visible effects of one pack write.
pub(crate) struct PackWriteEffects {
    pub(crate) operations: Vec<MutationOperation>,
    pub(crate) outbox: Vec<MutationOutboxIntent>,
}

/// What the staged batch commits as, for rows and receipts that name it.
pub(crate) struct PackWriteStaged {
    pub(crate) batch_id: String,
    pub(crate) committed_version: u64,
}

impl AgentLibraryStore {
    /// Commit one pack write, or replay the result its identity recorded.
    ///
    /// `apply` writes the owner rows and returns the typed result the ledger
    /// records; every fallible step after the write opens aborts it.
    pub(crate) fn commit_pack_write<R>(
        &self,
        context: &AgentLibraryMutationContext,
        identity: PackWriteIdentity<'_>,
        effects: PackWriteEffects,
        apply: impl FnOnce(&PackOwnerWrite<'_>, &PackWriteStaged) -> Result<R, String>,
    ) -> Result<R, String>
    where
        R: Serialize + DeserializeOwned,
    {
        validate_context(self, context)?;
        let owner = self.scope_handle(&context.tenant_id)?;
        let txn = self.mutations.open_write(&owner)?;
        // The replay identity is minted from the SAME policy-admitted context
        // the batch envelope carries: an N-operation pack batch has its own
        // effective policy digest, and an identity minted from the
        // single-operation digest would never bind that batch's authority.
        let replay_context = policy_admitted_context(
            &admitted_context(context, identity.purpose)?,
            &effects.operations,
        )?;
        let nonce = resolve_nonce_first(&self.mutations, &txn, &replay_context)?;
        let operation = bind_batch_method(
            agent_library_operation_identity(
                &owner,
                &replay_context,
                identity.kind,
                identity.subject,
                identity.revision,
                identity.discriminator,
            )?,
            &effects.operations,
        )?;
        let replay = self.mutations.resolve_replay(&txn, &operation, &nonce)?;
        if let Some(receipt) = recorded_receipt(replay, identity.noun)? {
            let result = decode_recorded::<R>(&receipt, identity.result_schema, identity.noun)?;
            return finish_replayed(&self.mutations, txn, &operation, &nonce, result, receipt);
        }
        let admission = PackAdmission {
            owner: &owner,
            context: &replay_context,
            operation: &operation,
            nonce: &nonce,
        };
        let (txn, committed) = within_write(txn, |txn| {
            self.stage_pack_write(txn, &admission, &identity, effects, apply)
        })?;
        self.mutations.commit(txn, &committed.0)?;
        Ok(committed.1)
    }

    /// Stage the batch, apply its owner rows and finish its ledger half
    /// inside the open write; the caller commits or the write aborts.
    fn stage_pack_write<R: Serialize>(
        &self,
        txn: &eg_transaction::AdmittedMutation<'_, eg_storage::AgentLibraryOwner>,
        admission: &PackAdmission<'_>,
        identity: &PackWriteIdentity<'_>,
        effects: PackWriteEffects,
        apply: impl FnOnce(&PackOwnerWrite<'_>, &PackWriteStaged) -> Result<R, String>,
    ) -> Result<(eg_types::mutation_batch::MutationBatch, R), String> {
        let batch_context = policy_admitted_context(admission.context, &effects.operations)?;
        let (batch_id, staged) = stage_batch(
            self,
            txn,
            admission.owner,
            &batch_context,
            (admission.operation, admission.nonce),
            identity.noun,
            |version, batch_id| {
                native_lifecycle_batch(
                    admission.owner,
                    &batch_context,
                    batch_id,
                    version,
                    effects.operations,
                    effects.outbox,
                )
            },
        )?;
        let meta = PackWriteStaged {
            batch_id,
            committed_version: staged.committed_version,
        };
        let mut applied = None;
        apply_owner_rows(txn, admission.owner, &staged.batch, |write| {
            applied = Some(apply(write, &meta)?);
            Ok(())
        })?;
        let result =
            applied.ok_or_else(|| format!("{} owner rows produced no result", identity.noun))?;
        let mutation_result = crate::server::persistence::agent_row::domain_result(
            &result,
            identity.result_schema,
            identity.noun,
        )?;
        let result_bytes = eg_storage::encode_bounded(
            &mutation_result,
            &format!("{} domain result", identity.noun),
        )?;
        let authority_receipt = owner_batch_receipt(
            admission.operation,
            admission.nonce,
            &staged.batch,
            identity.slug,
            mutation_result,
            staged.committed_version,
            admission.context.created_at_ms,
        )?;
        finish_committed_ledger(
            &self.mutations,
            txn,
            (&staged, &result_bytes),
            admission.context.created_at_ms,
            (admission.operation, admission.nonce, &authority_receipt),
            identity.noun,
        )?;
        Ok((staged.batch, result))
    }
}

/// The replay admission one pack write was granted before it staged.
struct PackAdmission<'a> {
    owner: &'a eg_storage::OwnedStoreHandle<eg_storage::AgentLibraryOwner>,
    context: &'a AgentLibraryMutationContext,
    operation: &'a eg_types::authority::OperationReplayIdentity,
    nonce: &'a eg_types::authority::NonceReplayKey,
}

/// Decode the typed result a replayed receipt recorded, refusing a receipt of
/// any other schema: a replay must answer what the first attempt answered.
fn decode_recorded<R: DeserializeOwned>(
    receipt: &MutationReceipt,
    schema: &str,
    noun: &str,
) -> Result<R, String> {
    receipt.validate()?;
    let MutationResult::DomainResult {
        schema_id, payload, ..
    } = &receipt.result
    else {
        return Err(format!(
            "CORRUPT_MUTATION_LEDGER: {noun} replay has no result"
        ));
    };
    if schema_id.as_str() != schema {
        return Err(format!(
            "CORRUPT_MUTATION_LEDGER: {noun} result schema differs"
        ));
    }
    crate::server::persistence::agent_row::decode(payload.as_slice(), noun)
}

/// Name the method the batch envelope will carry. A single-operation batch is
/// that operation's method; an N-operation pack batch is the compiled-methods
/// batch, and an identity that still named `ApplyMutation` would never bind
/// the envelope's authority (the kernel refuses the begin). The method schema
/// digest stays the envelope's `ApplyMutation` digest.
fn bind_batch_method(
    mut operation: eg_types::authority::OperationReplayIdentity,
    operations: &[MutationOperation],
) -> Result<eg_types::authority::OperationReplayIdentity, String> {
    let method = eg_types::mutation_batch::batch_method_id(operations, false)?;
    operation.method_schema_id = eg_types::mutation_batch::method_schema_id(&method)?;
    operation.method = method;
    operation.validate()?;
    Ok(operation)
}
