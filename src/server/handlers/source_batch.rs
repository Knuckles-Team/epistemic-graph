//! Native, tenant-scoped SQL source append through the existing owner gate.
//! The typed result reports inserted rows and the provider checkpoint; this is
//! not CRUD synchronization, upsert, deletion, or rehydration.
//!
//! Descriptor and mapping bytes are inert provenance content, not provider
//! credentials or permission to execute a mapping. The verified carrier and
//! existing SQL INSERT grants authorize publication into an existing table.
//! RLS submissions must supply the exact verified agent stamp explicitly; this
//! path never converts typed cells through JSON or silently rewrites the request.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use eg_query::TableStore;
use eg_types::contract::{Digest256, Nonce};
use eg_types::storage_wire::{SqlSourceBatchRequest, SqlSourceBatchResult};
use tokio::sync::RwLock;

use crate::mutation_batch::MutationBatch;
use crate::protocol::{Response, ResultPayload};
use crate::server::access::CarrierAuthority;
use crate::server::auth::VerifiedRequestContext;
use crate::server::ServerState;

mod authorization;
#[cfg(test)]
mod tests;

pub(crate) async fn handle(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified: &VerifiedRequestContext,
    request: SqlSourceBatchRequest,
) -> Response {
    let authority = match CarrierAuthority::from_verified(verified) {
        Ok(authority) => authority,
        Err(error) => return Response::err(req_id, error),
    };
    if !authority.can_write() {
        return Response::err(
            req_id,
            "ACCESS_DENIED: SQL source publication requires kg:write",
        );
    }
    let persist_dir = match persist_directory(state).await {
        Ok(directory) => directory,
        Err(error) => return Response::err(req_id, error),
    };
    let attempt_nonce = verified.attempt_nonce();
    let now_ms = crate::server::dispatch::authoritative_now_ms();
    let result = run_publication_job(move || {
        publish(
            req_id,
            &authority,
            attempt_nonce,
            request,
            &persist_dir,
            now_ms,
        )
    })
    .await;
    match result {
        Ok(result) => Response::ok(
            req_id,
            ResultPayload::of::<eg_types::result_contract::storage::SqlSourceBatch>(result),
        ),
        Err(error) => Response::err(req_id, error),
    }
}

async fn persist_directory(state: &Arc<RwLock<ServerState>>) -> Result<PathBuf, String> {
    state
        .read()
        .await
        .persist_dir
        .as_ref()
        .map(PathBuf::from)
        .ok_or_else(|| {
            "SQL source publication requires the configured persistence directory".to_string()
        })
}

/// The blocking job owns the admitted lifecycle. Cancelling its waiter does not
/// cancel a running write or acknowledge an unfinished commit. Panic details are
/// intentionally absent from the response because they can contain source data.
async fn run_publication_job<T, F>(job: F) -> Result<T, String>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, String> + Send + 'static,
{
    tokio::task::spawn_blocking(job)
        .await
        .map_err(|_| "SQL source publication task failed".to_string())?
}

fn publish(
    req_id: u64,
    authority: &CarrierAuthority,
    attempt_nonce: Option<Nonce>,
    request: SqlSourceBatchRequest,
    persist_dir: &Path,
    now_ms: u64,
) -> Result<SqlSourceBatchResult, String> {
    if !authority.can_write() {
        return Err("ACCESS_DENIED: SQL source publication requires kg:write".into());
    }
    // LocalOnly is refused before opening or mutating a SQL owner in active Raft.
    crate::server::sql_catalog_acl::with_source_authority_write(persist_dir, authority, |source| {
        authorization::authorize_source_insert(source, authority, &request)?;
        let store =
            crate::server::sql_tables::tenant_table_store(authority.tenant_scope(), persist_dir)?;
        let batch = compile_batch(&store, req_id, authority, attempt_nonce, request, now_ms)?;
        let committed = store.commit_source_batch(&batch, now_ms)?;
        let bytes = committed
            .record
            .result_msgpack
            .as_deref()
            .ok_or_else(|| "committed SQL source publication has no result".to_string())?;
        eg_storage::decode_ledger_record(bytes)
    })
}

fn compile_batch(
    store: &TableStore,
    req_id: u64,
    authority: &CarrierAuthority,
    attempt_nonce: Option<Nonce>,
    request: SqlSourceBatchRequest,
    now_ms: u64,
) -> Result<MutationBatch, String> {
    let scope = publication_scope(authority, &request)?;
    crate::server::sql_tables::SqlOwnerMutation {
        authority,
        kind: "sql-source",
        scope: &scope,
        request_id: req_id,
        attempt_nonce,
        created_at_ms: now_ms,
    }
    .compile(store, |context| {
        crate::server::mutation_batch::compile_sql_source_batch(context, request)
    })
}

fn publication_scope(
    authority: &CarrierAuthority,
    request: &SqlSourceBatchRequest,
) -> Result<String, String> {
    let batch = request.as_batch();
    let identity = Digest256::framed(
        b"eg/sql-source-publication-scope",
        &[
            batch.source.as_str().as_bytes(),
            batch.partition.as_str().as_bytes(),
            batch.table.as_str().as_bytes(),
        ],
    )?;
    Ok(authority.namespace("sql-source", &identity.to_hex()))
}
