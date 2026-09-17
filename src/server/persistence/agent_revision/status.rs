//! Durable status reads: resolve a prior attempt's committed outcome from the
//! mutation ledger without re-committing it.

use std::sync::Arc;

use eg_storage::OwnedStoreHandle;
use eg_types::mutation_batch::MutationBatchStatus;

use super::super::agent_library::{batch_id, validate_context};
use super::*;

/// A committed ledger record and the tenant scope it was read under.
type StatusRecord = (Arc<OwnedStoreHandle<Owner>>, eg_types::MutationBatchRecord);

/// The ledger record one caller operation committed, with the tenant scope it
/// was read under, or `None` when that operation never committed.
pub(in crate::server::persistence) fn ledger_status_record(
    store: &AgentLibraryStore,
    context: &AgentLibraryMutationContext,
    record_id: &str,
) -> Result<Option<StatusRecord>, String> {
    validate_context(store, context)?;
    eg_types::agent_library::validate_key(&context.tenant_id, record_id)?;
    let owner = store.scope_handle(&context.tenant_id)?;
    let batch_id = batch_id(&context.idempotency_key)?;
    let record = {
        let read = store.kernel.read_scope(&owner)?;
        eg_transaction::read_ledger(&read, &batch_id)?
    };
    Ok(record.map(|record| (owner, record)))
}

/// The typed result bytes a committed status record must carry.
pub(in crate::server::persistence) fn status_result_bytes<L: RevisionLayer>(
    record: &eg_types::MutationBatchRecord,
) -> Result<&[u8], String> {
    record.result_msgpack.as_deref().ok_or_else(|| {
        format!(
            "CORRUPT_MUTATION_LEDGER: {} status has no typed result",
            L::NOUN
        )
    })
}

/// A graph or template status: the committed result, bound to the operation
/// the caller names.
pub(in crate::server::persistence) fn committed_status<L: RevisionLayer>(
    store: &AgentLibraryStore,
    context: &AgentLibraryMutationContext,
    record_id: &str,
    kind: L::Kind,
) -> Result<Option<L::WriteResult>, String> {
    let Some((owner, record)) = ledger_status_record(store, context, record_id)? else {
        return Ok(None);
    };
    let committed = decode_status_result::<L>(status_result_bytes::<L>(&record)?)?;
    validate_status_record::<L>(
        &record,
        owner.identity(),
        context,
        (record_id, kind),
        &committed,
    )?;
    Ok(Some(L::write_result(committed, true)))
}

pub(in crate::server::persistence) fn decode_status_result<L: RevisionLayer>(
    bytes: &[u8],
) -> Result<L::Committed, String> {
    let mutation_result: MutationResult =
        agent_row::decode(bytes, &format!("{} status result", L::NOUN))?;
    mutation_result.validate()?;
    let MutationResult::DomainResult {
        schema_id, payload, ..
    } = mutation_result
    else {
        return Err(format!(
            "CORRUPT_MUTATION_LEDGER: {} status result is not a DomainResult",
            L::NOUN
        ));
    };
    if schema_id.as_str() != L::RESULT_SCHEMA_ID {
        return Err(format!(
            "CORRUPT_MUTATION_LEDGER: {} status result has an unexpected schema",
            L::NOUN
        ));
    }
    decode_committed::<L>(payload.as_slice())
}

/// Bind a decoded status result to its durable record and the caller's
/// operation. `operation` is the record id and mutation kind the caller asked
/// about.
pub(in crate::server::persistence) fn validate_status_record<L: RevisionLayer>(
    record: &eg_types::MutationBatchRecord,
    owner_identity: &eg_types::MutationScopeIdentity,
    context: &AgentLibraryMutationContext,
    operation: (&str, L::Kind),
    committed: &L::Committed,
) -> Result<(), String> {
    validate_status_durable_state::<L>(record)?;
    validate_status_identity::<L>(record, owner_identity, context, committed)?;
    validate_status_result::<L>(record, operation, committed)
}

fn validate_status_durable_state<L: RevisionLayer>(
    record: &eg_types::MutationBatchRecord,
) -> Result<(), String> {
    if record.status != MutationBatchStatus::Committed {
        return Err(format!(
            "CORRUPT_MUTATION_LEDGER: {} status is not committed",
            L::NOUN
        ));
    }
    record.validate()
}

fn validate_status_identity<L: RevisionLayer>(
    record: &eg_types::MutationBatchRecord,
    owner_identity: &eg_types::MutationScopeIdentity,
    context: &AgentLibraryMutationContext,
    committed: &L::Committed,
) -> Result<(), String> {
    let expected_batch_id = batch_id(&context.idempotency_key)?;
    let (committed_batch_id, _) = L::committed_binding(committed);
    if record.identity != *owner_identity
        || record.batch.batch_id != expected_batch_id
        || committed_batch_id != expected_batch_id
        || record.batch.batch_id != committed_batch_id
    {
        return Err(format!(
            "CORRUPT_MUTATION_LEDGER: {} status batch identity is invalid",
            L::NOUN
        ));
    }
    if record.committing_tenant()? != context.tenant_id
        || L::revision_definition(L::committed_entry(committed)).tenant_id != context.tenant_id
    {
        return Err(format!(
            "CORRUPT_MUTATION_LEDGER: {} status tenant is invalid",
            L::NOUN
        ));
    }
    Ok(())
}

fn validate_status_result<L: RevisionLayer>(
    record: &eg_types::MutationBatchRecord,
    operation: (&str, L::Kind),
    committed: &L::Committed,
) -> Result<(), String> {
    let (record_id, kind) = operation;
    let (_, committed_version) = L::committed_binding(committed);
    if record.committed_version.target() != Some(committed_version) {
        return Err(format!(
            "CORRUPT_MUTATION_LEDGER: {} status version is invalid",
            L::NOUN
        ));
    }
    let definition = L::revision_definition(L::committed_entry(committed));
    if definition.record_id != record_id {
        return Err(format!(
            "CORRUPT_MUTATION_LEDGER: {} status resolved a different {}",
            L::NOUN,
            L::RECORD
        ));
    }
    if definition.lifecycle != L::verb(kind).lifecycle() {
        return Err(format!(
            "CORRUPT_MUTATION_LEDGER: {} status kind does not match lifecycle",
            L::NOUN
        ));
    }
    Ok(())
}
