use std::sync::Arc;

use crate::protocol::{Method, Response, ResultPayload};
use crate::server::persistence::PersistenceBackend;

use super::{idempotency_store, CommitPrep, MutationCtx, MutationPlan};

/// Where one attempt's durable batch lands.
///
/// [`commit_mutation_body`] resolves all three once, before it knows which
/// commit shape the method takes: the backend that owns the write, the
/// sanitized graph file name the batch is keyed under, and the opaque batch id
/// that makes a retry idempotent. Every phase of the durable path — the replay
/// probe, the staged commit, the prepublish fast path — has to address the
/// SAME three, and a phase that addressed a different file name or batch id
/// than the probe that cleared it would silently commit a second batch, so
/// they travel as one address rather than three parallel arguments.
pub(super) struct DurableBatchTarget<'a> {
    pub(super) persistence: &'a Arc<dyn PersistenceBackend>,
    /// `persist::sanitize`d graph name — the durable key, not `ctx.graph_name`.
    pub(super) fname: &'a str,
    /// Opaque per-attempt-identity batch id from
    /// `mutation_batch::opaque_idempotency_key_for_context`.
    pub(super) batch_id: &'a str,
}

pub(super) struct DurableBatchAttempt<'a> {
    pub(super) target: DurableBatchTarget<'a>,
    pub(super) created_at_ms: u64,
}

/// The replay-repair check at the top of [`commit_mutation_body`]'s durable path:
/// a retry after `fsync` but before RAM publication repairs the serving projection
/// from authority and returns the exact stored result — no handler is re-executed
/// and no duplicate outbox row is produced. `Some(response)` is the caller's
/// immediate return (a replay was found and repaired, or the status lookup itself
/// failed); `None` means no prior committed batch exists, so the caller proceeds
/// to actually apply the mutation.
pub(super) async fn commit_staged_replay_probe(
    ctx: &MutationCtx<'_>,
    plan: &MutationPlan,
    methods: Vec<Method>,
    default_surface: crate::mutation_batch::MutationSurface,
    target: DurableBatchTarget<'_>,
    prep: &CommitPrep,
) -> Option<Response> {
    let DurableBatchTarget {
        persistence,
        fname,
        batch_id,
    } = target;
    match persistence.read_mutation_batch(fname, batch_id).await {
        Ok(Some(record)) => {
            commit_staged_replay_found(
                ctx,
                plan,
                methods,
                default_surface,
                DurableBatchTarget {
                    persistence,
                    fname,
                    batch_id,
                },
                record,
                prep,
            )
            .await
        }
        Ok(None) => None,
        Err(error) => Some(Response::err(
            ctx.req_id,
            format!("MutationBatch status lookup failed: {error}"),
        )),
    }
}

async fn commit_staged_replay_found(
    ctx: &MutationCtx<'_>,
    plan: &MutationPlan,
    methods: Vec<Method>,
    default_surface: crate::mutation_batch::MutationSurface,
    target: DurableBatchTarget<'_>,
    record: eg_types::mutation_batch::MutationBatchRecord,
    prep: &CommitPrep,
) -> Option<Response> {
    let DurableBatchTarget {
        persistence,
        fname,
        batch_id,
    } = target;
    let (descriptor, source_version) =
        match staged_replay_descriptor(ctx, persistence, fname, &record).await {
            Ok(values) => values,
            Err(response) => return Some(response),
        };
    let created_at_ms = crate::server::dispatch::authoritative_now_ms();
    let batch = match compile_replay_batch(
        ctx,
        batch_id,
        source_version,
        created_at_ms,
        default_surface,
        Some(descriptor),
        methods,
    ) {
        Ok(batch) => batch,
        Err(response) => return Some(response),
    };
    match persistence
        .commit_mutation_batch_state(fname, &batch, Vec::new(), None, created_at_ms, plan.audited)
        .await
    {
        Ok(committed) if committed.replayed => Some(
            commit_mutation_body_replay_response(ctx, persistence, fname, committed.record, prep)
                .await,
        ),
        Ok(_) => Some(Response::err(
            ctx.req_id,
            "MutationBatch replay probe unexpectedly committed a fresh operation",
        )),
        Err(error) => Some(Response::err(
            ctx.req_id,
            format!("MutationBatch replay probe failed: {error}"),
        )),
    }
}

async fn staged_replay_descriptor(
    ctx: &MutationCtx<'_>,
    persistence: &Arc<dyn PersistenceBackend>,
    fname: &str,
    record: &eg_types::mutation_batch::MutationBatchRecord,
) -> Result<(crate::mutation_batch::MutationStateDescriptor, u64), Response> {
    let mut descriptor = record.batch.authoritative_state.clone().ok_or_else(|| {
        Response::err(
            ctx.req_id,
            "committed staged MutationBatch has no authoritative state",
        )
    })?;
    let source_version = read_replay_source_version(
        ctx,
        persistence,
        fname,
        "committed staged MutationBatch has no authoritative graph version",
    )
    .await?;
    descriptor.source_graph_version = source_version;
    descriptor.target_graph_version = source_version
        .checked_add(1)
        .ok_or_else(|| Response::err(ctx.req_id, "authoritative graph version overflow"))?;
    Ok((descriptor, source_version))
}

async fn read_replay_source_version(
    ctx: &MutationCtx<'_>,
    persistence: &Arc<dyn PersistenceBackend>,
    fname: &str,
    missing_message: &'static str,
) -> Result<u64, Response> {
    persistence
        .read_mutation_graph_version(fname)
        .await
        .map_err(|error| {
            Response::err(
                ctx.req_id,
                format!("authoritative version read failed: {error}"),
            )
        })?
        .ok_or_else(|| Response::err(ctx.req_id, missing_message))
}

fn compile_replay_batch(
    ctx: &MutationCtx<'_>,
    batch_id: &str,
    source_version: u64,
    created_at_ms: u64,
    default_surface: crate::mutation_batch::MutationSurface,
    authoritative_state: Option<crate::mutation_batch::MutationStateDescriptor>,
    methods: Vec<Method>,
) -> Result<crate::mutation_batch::MutationBatch, Response> {
    crate::server::mutation_batch::compile_methods(
        crate::server::mutation_batch::CompileBatch {
            batch_id,
            request_id: ctx.req_id,
            attempt_nonce: ctx.attempt_nonce,
            principal: ctx.caller,
            tenant: ctx.tenant_scope,
            graph: ctx.graph_name,
            placement_epoch: 0,
            idempotency_key: ctx.idempotency_key,
            expected_graph_version: Some(source_version),
            fencing_token: None,
            created_at_ms,
            default_surface,
            authoritative_state,
        },
        methods,
    )
    .map_err(|error| {
        Response::err(
            ctx.req_id,
            format!("MutationBatch replay probe compile failed: {error}"),
        )
    })
}

pub(super) async fn commit_mutation_body_replay_response(
    ctx: &MutationCtx<'_>,
    persistence: &Arc<dyn PersistenceBackend>,
    fname: &str,
    record: eg_types::mutation_batch::MutationBatchRecord,
    prep: &CommitPrep,
) -> Response {
    match persistence.read_authoritative_graph_snapshot(fname).await {
        Ok(Some((snapshot, version))) => {
            if let Err(error) = ctx.core.install_committed_snapshot(snapshot, version) {
                return Response::err(
                    ctx.req_id,
                    format!("committed projection reconciliation failed: {error}"),
                );
            }
        }
        Ok(None) => {
            return Response::err(
                ctx.req_id,
                "committed projection reconciliation found no authoritative graph",
            );
        }
        Err(error) => {
            return Response::err(
                ctx.req_id,
                format!("committed projection reconciliation failed: {error}"),
            );
        }
    }
    let response = record
        .result_msgpack
        .as_deref()
        .ok_or_else(|| "committed MutationBatch has no durable result".to_string())
        .and_then(|bytes| {
            eg_types::msgpack::decode_bounded(
                bytes,
                eg_types::msgpack::MsgpackLimits::new(64 * 1024 * 1024, 1_000_000, 64),
            )
            .map_err(|_| "committed MutationBatch result is corrupt".to_string())
        })
        .map(|payload: ResultPayload| Response::ok(ctx.req_id, payload))
        .unwrap_or_else(|error| Response::err(ctx.req_id, error));
    if let Some(key) = &prep.dedup_key {
        idempotency_store().insert(key.clone(), response.clone());
    }
    response
}

pub(super) async fn commit_row_replay_probe(
    ctx: &MutationCtx<'_>,
    method: &Method,
    persistence: &Arc<dyn PersistenceBackend>,
    fname: &str,
    batch_id: &str,
    prep: &CommitPrep,
) -> Option<Response> {
    match persistence.read_mutation_batch(fname, batch_id).await {
        Ok(None) => None,
        Err(error) => Some(Response::err(
            ctx.req_id,
            format!("MutationBatch status lookup failed: {error}"),
        )),
        Ok(Some(_)) => {
            commit_row_replay_found(
                ctx,
                method,
                DurableBatchTarget {
                    persistence,
                    fname,
                    batch_id,
                },
                prep,
            )
            .await
        }
    }
}

async fn commit_row_replay_found(
    ctx: &MutationCtx<'_>,
    method: &Method,
    target: DurableBatchTarget<'_>,
    prep: &CommitPrep,
) -> Option<Response> {
    let DurableBatchTarget {
        persistence,
        fname,
        batch_id,
    } = target;
    let source_version = match read_replay_source_version(
        ctx,
        persistence,
        fname,
        "committed MutationBatch has no authoritative graph version",
    )
    .await
    {
        Ok(version) => version,
        Err(response) => return Some(response),
    };
    let created_at_ms = crate::server::dispatch::authoritative_now_ms();
    let batch = match compile_replay_batch(
        ctx,
        batch_id,
        source_version,
        created_at_ms,
        crate::mutation_batch::MutationSurface::Graph,
        None,
        vec![method.clone()],
    ) {
        Ok(batch) => batch,
        Err(response) => return Some(response),
    };
    match persistence
        .commit_mutation_batch(fname, &batch, None, created_at_ms)
        .await
    {
        Ok(committed) if committed.replayed => Some(
            commit_mutation_body_replay_response(ctx, persistence, fname, committed.record, prep)
                .await,
        ),
        Ok(_) => Some(Response::err(
            ctx.req_id,
            "MutationBatch replay probe unexpectedly committed a fresh operation",
        )),
        Err(error) => Some(Response::err(
            ctx.req_id,
            format!("MutationBatch replay probe failed: {error}"),
        )),
    }
}

/// Compile `methods` into a batch and encode `payload` as its durable result — the
/// compile+encode pair every commit path performs before its own
/// `commit_mutation_batch`/`commit_mutation_batch_state` call. Shared by
/// [`commit_mutation_body_prepublish_fast_path`],
/// [`super::commit_encode::commit_mutation_body_commit_staged`], and
/// [`super::conditional::commit_conditional_commit_staged`].
pub(super) fn compile_batch_and_encode_result(
    ctx: &MutationCtx<'_>,
    compile: crate::server::mutation_batch::CompileBatch<'_>,
    methods: Vec<Method>,
    payload: &ResultPayload,
) -> Result<(crate::mutation_batch::MutationBatch, Vec<u8>), Response> {
    let batch = match crate::server::mutation_batch::compile_methods(compile, methods) {
        Ok(batch) => batch,
        Err(error) => {
            return Err(Response::err(
                ctx.req_id,
                format!("MutationBatch compile failed: {error}"),
            ));
        }
    };
    let result = match rmp_serde::to_vec_named(payload) {
        Ok(result) => result,
        Err(error) => {
            return Err(Response::err(
                ctx.req_id,
                format!("MutationBatch result encode failed: {error}"),
            ));
        }
    };
    Ok((batch, result))
}
