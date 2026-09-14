use std::sync::Arc;

use crate::graph::GraphCore;
use crate::protocol::{Method, Response, ResultPayload};
use crate::server::persistence::PersistenceBackend;

use super::{
    commit_finalize, commit_mutation_body_commit_staged, commit_mutation_body_replay_response,
    commit_row_replay_probe, commit_staged_replay_probe, compile_batch_and_encode_result,
    diff_and_serialize_staged_mutation, durable_receipt_method, preserves_node_derived_indexes,
    CommitFinalizeOptions, CommitPrep, DurableBatchAttempt, DurableBatchTarget, MutationCtx,
    MutationPlan, StagedMutation,
};

/// The row-local fast path of [`commit_mutation_body`]: [`prepublish_success`]
/// already knows the deterministic result, so the compact `Method` itself commits
/// directly (no staged diff) before `apply` runs against the live projection.
pub(super) async fn commit_mutation_body_prepublish_fast_path<F>(
    ctx: &MutationCtx<'_>,
    plan: &MutationPlan,
    method: &Method,
    attempt: DurableBatchAttempt<'_>,
    predicted: ResultPayload,
    prep: CommitPrep,
    apply: F,
) -> Response
where
    F: FnOnce(&GraphCore) -> Result<ResultPayload, String>,
{
    let DurableBatchAttempt {
        target:
            DurableBatchTarget {
                persistence,
                fname,
                batch_id,
            },
        created_at_ms,
    } = attempt;
    if let Some(response) =
        commit_row_replay_probe(ctx, method, persistence, fname, batch_id, &prep).await
    {
        return response;
    }
    let committed = match commit_prepublish_durable_batch(
        ctx,
        method,
        persistence,
        fname,
        batch_id,
        created_at_ms,
        &predicted,
    )
    .await
    {
        Ok(committed) => committed,
        Err(response) => return response,
    };
    if committed.replayed {
        return commit_mutation_body_replay_response(
            ctx,
            persistence,
            fname,
            committed.record,
            &prep,
        )
        .await;
    }
    // The coalescing decision (batch this RAM publish with concurrent
    // siblings vs apply it here inline) is made by the CALLER now —
    // `commit_coalescable_mutation` for the five coalescable structural
    // writes, before this function is ever invoked — never here. By the
    // time `commit_mutation_body` runs, `apply` is simply run directly:
    // for the ordinary single-call path that's the only thing that ever
    // happened; for a queued coalescable op the worker already decided to
    // run this op's own `apply` (never re-enqueuing), so this is still
    // the only mutation of `core` for this op either way.
    let response = match apply(ctx.core) {
        Ok(payload) => Response::ok(ctx.req_id, payload),
        Err(error) => Response::err(ctx.req_id, error),
    };
    let indexes_maintained = preserves_node_derived_indexes(method);
    commit_finalize(
        ctx,
        plan,
        method,
        response,
        prep,
        CommitFinalizeOptions {
            durability_already_committed: true,
            preserve_node_indexes: indexes_maintained,
        },
    )
    .await
}

async fn commit_prepublish_durable_batch(
    ctx: &MutationCtx<'_>,
    method: &Method,
    persistence: &Arc<dyn PersistenceBackend>,
    fname: &str,
    batch_id: &str,
    created_at_ms: u64,
    predicted: &ResultPayload,
) -> Result<crate::mutation_batch::MutationBatchCommit, Response> {
    let source_version =
        crate::server::mutation_batch::authoritative_graph_version(persistence, fname, ctx.core)
            .await
            .map_err(|error| {
                Response::err(
                    ctx.req_id,
                    format!("authoritative version read failed: {error}"),
                )
            })?;
    let (batch, result) = compile_batch_and_encode_result(
        ctx,
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
            default_surface: crate::mutation_batch::MutationSurface::Graph,
            authoritative_state: None,
        },
        vec![method.clone()],
        predicted,
    )?;
    persistence
        .commit_mutation_batch(fname, &batch, Some(&result), created_at_ms)
        .await
        .map_err(|error| {
            Response::err(
                ctx.req_id,
                format!("MutationBatch durable commit failed: {error}"),
            )
        })
}

/// The runtime-result path of [`commit_mutation_body`]: stage `apply` against an
/// isolated authoritative image (never the live projection), diff it into a row
/// delta, and durably commit that delta + terminal result + version/fence + outbox
/// in one redb commit point before publishing to the live projection.
pub(super) async fn commit_mutation_body_staged_path<F>(
    ctx: &MutationCtx<'_>,
    plan: &MutationPlan,
    method: &Method,
    attempt: DurableBatchAttempt<'_>,
    prep: CommitPrep,
    apply: F,
) -> Response
where
    F: FnOnce(&GraphCore) -> Result<ResultPayload, String>,
{
    let DurableBatchAttempt {
        target:
            DurableBatchTarget {
                persistence,
                fname,
                batch_id,
            },
        created_at_ms,
    } = attempt;
    if let Some(response) = commit_staged_replay_probe(
        ctx,
        plan,
        vec![durable_receipt_method(method)],
        crate::mutation_batch::MutationSurface::Graph,
        DurableBatchTarget {
            persistence,
            fname,
            batch_id,
        },
        &prep,
    )
    .await
    {
        return response;
    }
    let staged = match commit_mutation_body_stage_and_diff(ctx, persistence, fname, apply).await {
        Ok(staged) => staged,
        Err(response) => return response,
    };
    commit_mutation_body_commit_staged(
        ctx,
        plan,
        method,
        DurableBatchAttempt {
            target: DurableBatchTarget {
                persistence,
                fname,
                batch_id,
            },
            created_at_ms,
        },
        prep,
        staged,
    )
    .await
}

/// The staged apply + diff half of [`commit_mutation_body_staged_path`]: resolve
/// the authoritative base image, apply the mutation against an isolated staged
/// core, enforce the native-write integrity policy, and diff staged-vs-base into a
/// row delta + its serialized/size-checked state blob.
pub(super) async fn commit_mutation_body_stage_and_diff<F>(
    ctx: &MutationCtx<'_>,
    persistence: &Arc<dyn PersistenceBackend>,
    fname: &str,
    apply: F,
) -> Result<StagedMutation, Response>
where
    F: FnOnce(&GraphCore) -> Result<ResultPayload, String>,
{
    let (base_snapshot, source_version) =
        resolve_authoritative_base_snapshot(ctx, persistence, fname).await?;
    let base_snapshot_for_delta = base_snapshot.clone();
    let staged = match GraphCore::from_snapshot(base_snapshot, source_version) {
        Ok(staged) => staged,
        Err(error) => {
            return Err(Response::err(
                ctx.req_id,
                format!("graph staging failed: {error}"),
            ));
        }
    };
    let payload = match apply(&staged) {
        Ok(payload) => payload,
        Err(error) => return Err(Response::err(ctx.req_id, error)),
    };
    // X5-enforce, native writes (W4.13, CONCEPT:EG-KG.ontology.rdf-update-guard): the same
    // registered per-graph integrity policy the RDF write path enforces, extended
    // (opt-in, `EPISTEMIC_GRAPH_ICV_NATIVE_WRITES`) to this gateway's staged/
    // diffable native-write path (`CompareAndSetNodeFields`, `ApplyMutation`, and
    // any other write NOT on the `prepublish_success` row-local fast path — see
    // `commit_conditional_mutation_async_inner` for the sibling `CypherQuery`
    // path). A rejection here discards the staged image before it is ever
    // diffed, snapshotted, or durably committed.
    #[cfg(feature = "shacl")]
    if let Err(rejection) =
        crate::server::icv_guard::check_native_write(ctx.graph_name, ctx.core, &staged)
    {
        return Err(Response::err(
            ctx.req_id,
            format!("mutation rejected by integrity policy: {rejection}"),
        ));
    }
    let staged_snapshot = staged.snapshot();
    let (row_delta, state_msgpack) =
        diff_and_serialize_staged_mutation(ctx, &base_snapshot_for_delta, &staged_snapshot)?;
    Ok(StagedMutation {
        payload,
        row_delta,
        state_msgpack,
        source_version,
    })
}

/// Resolve the base authoritative image to stage a mutation against: the
/// persistence backend's own staged snapshot when it has one, else the live
/// projection's snapshot at its authoritative version. Shared by
/// [`commit_mutation_body_stage_and_diff`] and [`commit_conditional_stage_and_diff`].
pub(super) async fn resolve_authoritative_base_snapshot(
    ctx: &MutationCtx<'_>,
    persistence: &Arc<dyn PersistenceBackend>,
    fname: &str,
) -> Result<(crate::graph::GraphSnapshot, u64), Response> {
    match persistence.read_authoritative_graph_snapshot(fname).await {
        Ok(Some(snapshot)) => Ok(snapshot),
        Ok(None) => match crate::server::mutation_batch::authoritative_graph_version(
            persistence,
            fname,
            ctx.core,
        )
        .await
        {
            Ok(version) => Ok((ctx.core.snapshot(), version)),
            Err(error) => Err(Response::err(
                ctx.req_id,
                format!("authoritative version read failed: {error}"),
            )),
        },
        Err(error) => Err(Response::err(
            ctx.req_id,
            format!("authoritative graph staging read failed: {error}"),
        )),
    }
}
