//! Native SQL source publication through the already-issued SQL mutation gate.
//!
//! This is not a permission boundary. The served handler authorizes the carrier,
//! table INSERT grant and RLS stamp before constructing the operation. Here
//! actual content digests bind the provider checkpoint, never grant access.
//! Provider cursors are business state, distinct from semantic dirty progress.

use super::{
    finish_and_commit_sql_mutation, sql_batch_commit, sql_scope_key,
    verify_batch_is_sql_catalog_only, MutationBatch, MutationBatchCommit, SqlMutation,
    SqlMutationCrashpoint, SqlWrite, TableStore,
};
use eg_transaction::Begin;
use eg_types::contract::Digest256;
use eg_types::protocol::Method;
use eg_types::storage_wire::{SqlSourceBatchDigests, SqlSourceBatchRequest, SqlSourceBatchResult};

mod checkpoint;
mod rows;
#[cfg(test)]
mod tests;

impl TableStore {
    /// Commit one compiled source submission through this authenticated SQL
    /// store's existing mutation authority. The served handler verifies the
    /// carrier, table INSERT grant and RLS stamp before compiling the batch.
    /// Descriptor digests establish integrity and checkpoint continuity only.
    pub fn commit_source_batch(
        &self,
        batch: &MutationBatch,
        committed_at_ms: u64,
    ) -> Result<MutationBatchCommit, String> {
        commit(self, batch, committed_at_ms, None)
    }
}

pub(super) fn commit(
    store: &TableStore,
    batch: &MutationBatch,
    committed_at_ms: u64,
    crashpoint: Option<SqlMutationCrashpoint>,
) -> Result<MutationBatchCommit, String> {
    let invocation = Invocation::check(store, batch)?;
    let (mut mutation, begun) = store.authority.begin_operation(batch)?;
    match begun {
        Begin::Replay(record) => replay(mutation, *record, &invocation),
        Begin::Apply { source_version } => {
            mutation.set_source_version(source_version);
            publish(
                store,
                mutation,
                batch,
                &invocation,
                committed_at_ms,
                crashpoint,
            )
        }
    }
}

struct Invocation<'a> {
    tenant: &'a str,
    request: &'a SqlSourceBatchRequest,
    digests: SqlSourceBatchDigests,
    authority_digest: Digest256,
}

impl<'a> Invocation<'a> {
    fn check(store: &'a TableStore, batch: &'a MutationBatch) -> Result<Self, String> {
        batch.validate_write_budget()?;
        verify_batch_is_sql_catalog_only(batch)?;
        if batch.envelope.operation().is_none() {
            return Err("SQL source publication requires an admitted caller operation".into());
        }
        if !matches!(
            batch.version_expectation,
            eg_types::mutation_batch::VersionExpectation::Native(_)
        ) {
            return Err("SQL source publication requires native OCC fencing".into());
        }
        let (tenant, _) = sql_scope_key(batch)?;
        if tenant != store.index_scope() {
            return Err("SQL source batch tenant does not match authenticated store".into());
        }
        let [operation] = batch.operations.as_slice() else {
            return Err("SQL source publication requires exactly one operation".into());
        };
        if operation.surface != eg_types::mutation_batch::MutationSurface::Query {
            return Err("SQL source publication requires the query mutation surface".into());
        }
        let Method::SqlSourceBatch { batch: request } = &operation.method else {
            return Err("SQL source publication requires its typed operation".into());
        };
        // The closed request was checked at construction/deserialization. The
        // batch validator binds these actual bytes into its operation envelope.
        let digests = request.canonical_digests()?;
        Ok(Self {
            tenant,
            request,
            digests,
            authority_digest: Digest256::from_bytes(store.authority.source_authority_digest()),
        })
    }
}

fn replay(
    mutation: SqlMutation<'_>,
    record: eg_types::mutation_batch::MutationBatchRecord,
    invocation: &Invocation<'_>,
) -> Result<MutationBatchCommit, String> {
    let commit = match sql_batch_commit(record, true)
        .and_then(|commit| validate_terminal_result(commit, invocation))
    {
        Ok(commit) => commit,
        Err(error) => {
            mutation.abort()?;
            return Err(error);
        }
    };
    // Original rows, cursor, epoch, outbox and typed result stay untouched. The
    // existing kernel commits only its validated fresh-attempt nonce decision.
    mutation.commit_finished()?;
    Ok(commit)
}

fn validate_terminal_result(
    commit: MutationBatchCommit,
    invocation: &Invocation<'_>,
) -> Result<MutationBatchCommit, String> {
    let bytes = commit
        .record
        .result_msgpack
        .as_deref()
        .ok_or_else(|| "SQL source receipt has no terminal result".to_string())?;
    let result: SqlSourceBatchResult = eg_storage::decode_ledger_record(bytes)?;
    let submitted = invocation.request.as_batch();
    if result.canonical_digests != invocation.digests
        || result.accepted_position != submitted.position
        || result.affected_count != submitted.rows.len() as u64
    {
        return Err("SQL source receipt is not bound to submitted content".into());
    }
    // Authority and epoch belong to the original committed result. Comparing
    // them with a later sampled epoch would invalidate legitimate old replay.
    Ok(commit)
}

fn publish(
    store: &TableStore,
    mutation: SqlMutation<'_>,
    batch: &MutationBatch,
    invocation: &Invocation<'_>,
    committed_at_ms: u64,
    crashpoint: Option<SqlMutationCrashpoint>,
) -> Result<MutationBatchCommit, String> {
    let terminal = match mutation.owner_rows_with_epoch(
        |write| apply_rows(store, write, batch, invocation, crashpoint),
        |write, affected, epoch| checkpoint::finalize(write, batch, invocation, affected, epoch),
    ) {
        Ok(bytes) => bytes,
        Err(error) => {
            mutation.abort()?;
            return Err(error);
        }
    };
    finish_and_commit_sql_mutation(mutation, batch, terminal, committed_at_ms, crashpoint)
}

fn apply_rows(
    store: &TableStore,
    write: &SqlWrite<'_>,
    batch: &MutationBatch,
    invocation: &Invocation<'_>,
    crashpoint: Option<SqlMutationCrashpoint>,
) -> Result<usize, String> {
    if crashpoint == Some(SqlMutationCrashpoint::BeforeRows) {
        return Err("injected crash before SQL mutation rows".into());
    }
    eg_types::mutation_batch::apply_certification_fault(
        batch,
        eg_types::mutation_batch::MutationCommitPhase::BeforeRows,
    )?;
    // Schema, provider CAS and old mapping binding are checked through exactly
    // the same admitted writer that applies rows. DDL cannot interleave here.
    checkpoint::validate_previous(write, invocation)?;
    let affected = rows::apply(write, store.index_scope(), invocation.request)?;
    if crashpoint == Some(SqlMutationCrashpoint::AfterRowsBeforeMetadata) {
        return Err("injected crash after SQL mutation rows".into());
    }
    eg_types::mutation_batch::apply_certification_fault(
        batch,
        eg_types::mutation_batch::MutationCommitPhase::AfterRowsBeforeMetadata,
    )?;
    Ok(affected)
}
