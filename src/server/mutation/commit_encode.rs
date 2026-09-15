use std::sync::Arc;

use crate::graph::GraphCore;
use crate::protocol::{Method, Response, ResultPayload};
use crate::server::persistence::PersistenceBackend;

use super::{
    commit_finalize, commit_mutation_body_replay_response, compile_batch_and_encode_result,
    durable_receipt_method, CommitFinalizeOptions, CommitPrep, DurableBatchAttempt,
    DurableBatchTarget, MutationCtx, MutationPlan,
};

/// Diff `staged_snapshot` against `base_snapshot_for_delta` into a row delta,
/// serialize it, and enforce the configured size limit. Shared by
/// [`super::commit_paths::commit_mutation_body_stage_and_diff`] and
/// [`super::conditional::commit_conditional_stage_and_diff`].
pub(super) fn diff_and_serialize_staged_mutation(
    ctx: &MutationCtx<'_>,
    base_snapshot_for_delta: &crate::graph::GraphSnapshot,
    staged_snapshot: &crate::graph::GraphSnapshot,
) -> Result<(crate::graph_delta::GraphRowDelta, Vec<u8>), Response> {
    let row_delta = match crate::graph_delta::GraphRowDelta::between(
        base_snapshot_for_delta,
        staged_snapshot,
    ) {
        Ok(delta) => delta,
        Err(error) => {
            return Err(Response::err(
                ctx.req_id,
                format!("staged graph delta failed: {error}"),
            ));
        }
    };
    let state_msgpack = match row_delta.to_msgpack() {
        Ok(bytes) => bytes,
        Err(error) => {
            return Err(Response::err(
                ctx.req_id,
                format!("staged graph delta serialization failed: {error}"),
            ));
        }
    };
    let max_bytes = mutation_snapshot_max_bytes();
    if max_bytes > 0 && state_msgpack.len() > max_bytes {
        return Err(Response::err(
            ctx.req_id,
            format!(
                "staged mutation delta is {} bytes, above the configured {} byte limit",
                state_msgpack.len(),
                max_bytes
            ),
        ));
    }
    Ok((row_delta, state_msgpack))
}

/// The prepared-but-not-yet-committed output of
/// [`super::commit_paths::commit_mutation_body_stage_and_diff`], handed to
/// [`commit_mutation_body_commit_staged`].
pub(super) struct StagedMutation {
    pub(super) payload: ResultPayload,
    pub(super) row_delta: crate::graph_delta::GraphRowDelta,
    pub(super) state_msgpack: Vec<u8>,
    pub(super) source_version: u64,
}

/// The durable-commit + publish half of [`commit_mutation_body_staged_path`]:
/// compile the durable receipt method with the staged delta's
/// [`crate::mutation_batch::MutationStateDescriptor`], commit it, then either
/// reconcile from a replay or publish the row delta to the live projection.
pub(super) async fn commit_mutation_body_commit_staged(
    ctx: &MutationCtx<'_>,
    plan: &MutationPlan,
    method: &Method,
    attempt: DurableBatchAttempt<'_>,
    prep: CommitPrep,
    staged: StagedMutation,
) -> Response {
    let DurableBatchAttempt {
        target:
            DurableBatchTarget {
                persistence,
                fname,
                batch_id,
            },
        created_at_ms,
    } = attempt;
    let StagedMutation {
        payload,
        row_delta,
        state_msgpack,
        source_version,
    } = staged;
    let descriptor = match staged_mutation_descriptor(ctx, source_version, &state_msgpack) {
        Ok(descriptor) => descriptor,
        Err(response) => return response,
    };
    let (batch, result) = match compile_batch_and_encode_result(
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
            authoritative_state: Some(descriptor),
        },
        vec![durable_receipt_method(method)],
        &payload,
    ) {
        Ok(pair) => pair,
        Err(response) => return response,
    };
    let committed = match persistence
        .commit_mutation_batch_state(
            fname,
            &batch,
            state_msgpack,
            Some(&result),
            created_at_ms,
            // The ORIGINAL method's policy-audited answer, captured before
            // `compile_methods` erased its identity into an opaque state receipt
            // (see `redb_store::commit_mutation_batch_inner`'s doc comment).
            // `TouchNodes` is the standing durable-but-unaudited example.
            plan.audited,
        )
        .await
    {
        Ok(committed) => committed,
        Err(error) => {
            return Response::err(
                ctx.req_id,
                format!("staged MutationBatch durable commit failed: {error}"),
            );
        }
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
    } else if let Err(error) =
        publish_committed_row_delta(persistence, fname, ctx.core, &row_delta, source_version).await
    {
        return Response::err(
            ctx.req_id,
            format!("durable mutation projection publish failed: {error}"),
        );
    }
    commit_finalize(
        ctx,
        plan,
        method,
        Response::ok(ctx.req_id, payload),
        prep,
        CommitFinalizeOptions {
            durability_already_committed: true,
            preserve_node_indexes: row_delta.preserves_node_derived_indexes(),
        },
    )
    .await
}

/// Build the [`crate::mutation_batch::MutationStateDescriptor`] for a staged
/// row-delta commit: the target version (source + 1, checked) and the delta's
/// content digest. Shared by [`commit_mutation_body_commit_staged`] and
/// [`super::conditional::commit_conditional_commit_staged`].
pub(super) fn staged_mutation_descriptor(
    ctx: &MutationCtx<'_>,
    source_version: u64,
    state_msgpack: &[u8],
) -> Result<crate::mutation_batch::MutationStateDescriptor, Response> {
    use sha2::{Digest, Sha256};
    let target_graph_version = match source_version.checked_add(1) {
        Some(version) => version,
        None => {
            return Err(Response::err(
                ctx.req_id,
                "authoritative graph version overflow",
            ));
        }
    };
    Ok(crate::mutation_batch::MutationStateDescriptor {
        algorithm: crate::graph_delta::ROW_DELTA_ALGORITHM.to_string(),
        digest: hex::encode(Sha256::digest(state_msgpack)),
        source_graph_version: source_version,
        target_graph_version,
    })
}

pub(super) fn preserves_node_derived_indexes(method: &Method) -> bool {
    matches!(
        method,
        Method::AddEdge { .. }
            | Method::RemoveEdge { .. }
            | Method::AddEmbedding { .. }
            | Method::InvalidateEdge { .. }
    )
}

pub(crate) async fn publish_committed_row_delta(
    persistence: &Arc<dyn PersistenceBackend>,
    graph_fname: &str,
    core: &Arc<GraphCore>,
    delta: &crate::graph_delta::GraphRowDelta,
    source_version: u64,
) -> Result<(), String> {
    if core.version() == source_version && delta.apply_to(core).is_ok() {
        return Ok(());
    }
    // A bypass writer or an unexpected projection error cannot undo the already
    // committed authority. Repair from redb only on that exceptional path; the
    // normal path installs `D` affected rows and performs no full-image read.
    let (snapshot, version) = persistence
        .read_authoritative_graph_snapshot(graph_fname)
        .await?
        .ok_or_else(|| "committed graph projection is missing".to_string())?;
    let expected_target = source_version
        .checked_add(1)
        .ok_or_else(|| "committed graph version overflow".to_string())?;
    if version != expected_target {
        return Err("committed graph projection has an unexpected version".to_string());
    }
    // The caller still owns the normal finalize/mark-dirty step. Install at the
    // source version so that step advances exactly once and emits one change.
    core.prepare_snapshot_publish(snapshot, source_version)
}

pub(super) fn mutation_snapshot_max_bytes() -> usize {
    static MAX: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *MAX.get_or_init(|| {
        let automatic = crate::autosize::detect_capacity().mutation_snapshot_bytes();
        if let Ok(value) = std::env::var("EPISTEMIC_GRAPH_MUTATION_SNAPSHOT_MAX_BYTES") {
            if let Ok(parsed) = value.trim().parse::<usize>() {
                return crate::autosize::bound_explicit(parsed, automatic);
            }
        }
        automatic
    })
}

/// Return the exact success payload for operations whose row effect can be
/// validated without publishing it.  This is the safe live rollout set for
/// durable-before-RAM; no optimistic guess is made for complex/runtime-derived
/// mutations.
pub(super) fn prepublish_success(core: &GraphCore, method: &Method) -> Option<ResultPayload> {
    match method {
        Method::AddNode { .. }
        | Method::RemoveNode { .. }
        | Method::RemoveEdge { .. }
        | Method::ClearGraph
        // `ClearLedger` mutates `GraphCore::ledger` (an in-memory `Mutex<Vec<String>>`
        // that is NOT part of the node/edge row data `GraphRowDelta` diffs) -- omitting
        // it here previously routed it into the "runtime-result" branch below, which
        // runs `apply` against an ISOLATED staged clone (`GraphCore::from_snapshot`)
        // built for diffing row mutations, then computes+publishes only the ROW delta
        // between base and staged snapshots. Since clearing the ledger produces no row
        // delta at all (it isn't a node/edge field), the staged clone's ledger got
        // cleared and then silently discarded with the rest of that throwaway clone --
        // the LIVE core's ledger was never touched, so `ClearLedger` durably recorded
        // and ACKED success while doing nothing observable. Same fix as `ClearGraph`
        // (already listed here for the identical reason): route it through the
        // row-local fast path, so `apply` runs directly against the live `ctx.core`.
        | Method::ClearLedger
        | Method::AddEmbedding { .. } => Some(ResultPayload::String("ok".to_string())),
        Method::AddEdge {
            source_id,
            target_id,
            ..
        } if core.has_node(source_id) && core.has_node(target_id) => {
            Some(ResultPayload::String("ok".to_string()))
        }
        Method::BatchUpdate { operations_msgpack } => {
            // Falling through to `None` here is not a no-op: it routes this write
            // into the runtime-result branch, which reads the ENTIRE authoritative
            // graph snapshot back out of redb, clones it, and rebuilds a whole
            // `GraphCore` from it before applying. On a 56k-node graph that is
            // seconds per write instead of milliseconds. Swallowing the reason with
            // `.ok()?` made that cliff invisible — the write still succeeded, just
            // via the expensive path, so nothing surfaced and no metric moved.
            let result = match crate::algorithms::batch_update_preview(core, operations_msgpack) {
                Ok(result) => result,
                Err(error) => {
                    tracing::warn!(
                        target: "epistemic_graph::mutation",
                        %error,
                        "BatchUpdate preview failed; falling back to the \
                         full-snapshot runtime-result commit path"
                    );
                    return None;
                }
            };
            match eg_types::result_contract::transactions::BatchUpdateReport::decode(&result)
                .and_then(
                    ResultPayload::of::<eg_types::result_contract::transactions::BatchUpdate>,
                ) {
                Ok(payload) => Some(payload),
                Err(error) => {
                    tracing::warn!(
                        target: "epistemic_graph::mutation",
                        %error,
                        "BatchUpdate preview summary failed to decode; falling back \
                         to the full-snapshot runtime-result commit path"
                    );
                    None
                }
            }
        }
        _ => None,
    }
}
