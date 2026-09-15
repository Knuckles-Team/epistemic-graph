use std::sync::Arc;

use eg_capabilities::DurabilityDomain;

use crate::graph::GraphCore;
use crate::isolation::AccessLevel;
use crate::protocol::{Method, Response, ResultPayload};
use crate::server::access::check_graph_access;
use crate::server::persistence::PersistenceBackend;

use super::{
    advance_authoritative_manifest, commit_finalize, commit_mutation,
    commit_mutation_body_replay_response, commit_prepare, commit_staged_replay_probe,
    compile_batch_and_encode_result, consensus_apply_is_authorized,
    diff_and_serialize_staged_mutation, method_variant_name, publish_committed_row_delta,
    resolve_authoritative_base_snapshot, staged_mutation_descriptor, CommitFinalizeOptions,
    CommitPrep, DurableBatchAttempt, DurableBatchTarget, MutationCtx, MutationPlan, StagedMutation,
};

/// The gateway entry point for a RUNTIME-CONDITIONAL method (CONCEPT:EG-P0-2, L11
/// rollout continued) -- one whose `eg_capabilities::policy()` `mutates: true` is
/// a conservative UPPER BOUND (see the `RUNTIME_CONDITIONAL` divergence table in
/// `eg-capabilities/tests/consistency.rs`) because the REAL answer depends on a
/// per-invocation field (a `writeback: bool`, or a parsed query) that a static
/// per-variant table cannot see. `mutates_now` is that resolved, per-call truth,
/// decided by the CALLER (the gateway match arm) from the request's own field --
/// never re-derived here, and never a second classifier: this function still
/// drives everything through the SAME `plan` (sourced from `policy()`) on the
/// `mutates_now == true` branch.
///
/// - `mutates_now == true`: identical to [`commit_mutation`] -- full authz
///   (Write) + durability + audit + CDC + idempotency-replay, driven by `plan`.
/// - `mutates_now == false`: THIS INVOCATION is a plain read (the upper bound
///   didn't materialize), so it is treated as one: a Read-only ACL check (never
///   Write), `apply` runs, and NOTHING durable/audited/CDC-emitted/cached happens
///   -- exactly what a method never in [`GATEWAY_ROUTED`] does for an ordinary
///   read. This is what keeps a `writeback: false` call from being incorrectly
///   gated behind Write access or persisted as a phantom mutation (the bug this
///   whole function exists to prevent -- see the module docs' RUNTIME_CONDITIONAL
///   discussion).
pub async fn commit_conditional_mutation<F>(
    ctx: &MutationCtx<'_>,
    plan: &MutationPlan,
    method: &Method,
    mutates_now: bool,
    apply: F,
) -> Response
where
    F: FnOnce(&GraphCore) -> Result<ResultPayload, String>,
{
    if mutates_now {
        return commit_mutation(ctx, plan, method, apply).await;
    }
    // Read-only path: same ACL surface every other read-only graph method goes
    // through (`access::check_graph_access`), just Read instead of Write.
    if !consensus_apply_is_authorized() {
        if let Err(denied) = check_graph_access(
            ctx.isolation,
            ctx.caller,
            ctx.graph_name,
            ctx.graph_type,
            ctx.owner,
            AccessLevel::Read,
        ) {
            return Response::err(ctx.req_id, denied);
        }
    }
    match apply(ctx.core) {
        Ok(payload) => Response::ok(ctx.req_id, payload),
        Err(e) => Response::err(ctx.req_id, e),
    }
}

/// The ASYNC-apply twin of [`commit_conditional_mutation`] (CONCEPT:EG-P0-2, L11):
/// for a routed method whose EXECUTION is itself `async` and needs more than a bare
/// `&GraphCore` — the query surface (`Sql`/`CypherQuery`/`GraphQl`, run on the
/// blocking pool via `handlers::query::try_handle`, needing `state`/`caller`/`rls`)
/// and the native RDF surface (`AddTriples`/`RemoveTriples`/`DropNamedGraph`, run
/// via `handlers::rdf::try_handle`; lossless multi-valued literals live inside the
/// staged graph image). The `apply` closure captures whatever
/// state it needs and returns the payload; the gateway wraps it with the IDENTICAL
/// authz / durability / audit / CDC / idempotency contract as the sync path (via the
/// shared [`commit_prepare`] / [`commit_finalize`]).
///
/// `mutates_now` is the resolved, per-call truth (never `policy()`'s conservative
/// upper bound): the RDF ops pass `true` (they always mutate), while the query ops
/// pass the SAME runtime parse `access::requires_write` already uses
/// (`sql_is_write` / `cypher_is_write` / `graphql_is_mutation`) — so a `SELECT` /
/// read-only Cypher / GraphQL `query` is a Read-authz passthrough with NO
/// durability/audit/CDC, while a SQL write / Cypher `CREATE|SET|DELETE` / GraphQL
/// `mutation` goes through the full Write-authz commit.
pub async fn commit_conditional_mutation_async<F, Fut>(
    ctx: &MutationCtx<'_>,
    plan: &MutationPlan,
    method: &Method,
    mutates_now: bool,
    apply: F,
) -> Response
where
    F: FnOnce(Arc<GraphCore>) -> Fut,
    Fut: std::future::Future<Output = Result<ResultPayload, String>>,
{
    let response =
        commit_conditional_mutation_async_inner(ctx, plan, method, mutates_now, apply).await;
    if mutates_now {
        advance_authoritative_manifest(ctx, plan, &response);
    }
    response
}

pub(super) async fn commit_conditional_mutation_async_inner<F, Fut>(
    ctx: &MutationCtx<'_>,
    plan: &MutationPlan,
    method: &Method,
    mutates_now: bool,
    apply: F,
) -> Response
where
    F: FnOnce(Arc<GraphCore>) -> Fut,
    Fut: std::future::Future<Output = Result<ResultPayload, String>>,
{
    if !mutates_now {
        return commit_conditional_read_only(ctx, apply).await;
    }

    // Mutating path: stage against a complete authoritative image. Query/RDF
    // handlers never receive the live serving core, so an execution or durable
    // commit failure cannot leak a partial mutation into RAM.
    let _mutation_guard = crate::server::mutation_batch::lock_graph(ctx.graph_name).await;
    let prep = match commit_prepare(ctx, plan, method) {
        Ok(p) => p,
        Err(short_circuit) => return short_circuit,
    };
    if matches!(plan.durability_domain, DurabilityDomain::None) {
        let response = match apply(Arc::clone(ctx.core)).await {
            Ok(payload) => Response::ok(ctx.req_id, payload),
            Err(error) => Response::err(ctx.req_id, error),
        };
        return commit_finalize(
            ctx,
            plan,
            method,
            response,
            prep,
            CommitFinalizeOptions {
                durability_already_committed: true,
                preserve_node_indexes: false,
            },
        )
        .await;
    }
    let Some(persistence) = ctx.persistence else {
        return Response::err(
            ctx.req_id,
            "authoritative MutationBatch commit requires a persistence backend",
        );
    };

    let fname = crate::persist::sanitize(ctx.graph_name);
    let batch_id = crate::server::mutation_batch::opaque_idempotency_key_for_context(
        "rpc-async",
        ctx.tenant_scope,
        ctx.graph_name,
        ctx.caller,
        ctx.idempotency_key,
    );
    if let Some(response) =
        commit_conditional_replay_check(ctx, plan, method, persistence, &fname, &batch_id, &prep)
            .await
    {
        return response;
    }

    let created_at_ms = crate::server::dispatch::authoritative_now_ms();
    match commit_conditional_stage_and_diff(ctx, persistence, &fname, apply).await {
        Ok(staged) => {
            commit_conditional_commit_staged(
                ctx,
                plan,
                method,
                DurableBatchAttempt {
                    target: DurableBatchTarget {
                        persistence,
                        fname: &fname,
                        batch_id: &batch_id,
                    },
                    created_at_ms,
                },
                prep,
                staged,
            )
            .await
        }
        Err(response) => response,
    }
}

/// The `!mutates_now` arm of [`commit_conditional_mutation_async_inner`]: Read-ACL
/// only, apply, and NOTHING durable/audited/CDC-emitted/cached — exactly what any
/// non-mutating graph read does.
pub(super) async fn commit_conditional_read_only<F, Fut>(
    ctx: &MutationCtx<'_>,
    apply: F,
) -> Response
where
    F: FnOnce(Arc<GraphCore>) -> Fut,
    Fut: std::future::Future<Output = Result<ResultPayload, String>>,
{
    if !consensus_apply_is_authorized() {
        if let Err(denied) = check_graph_access(
            ctx.isolation,
            ctx.caller,
            ctx.graph_name,
            ctx.graph_type,
            ctx.owner,
            AccessLevel::Read,
        ) {
            return Response::err(ctx.req_id, denied);
        }
    }
    match apply(Arc::clone(ctx.core)).await {
        Ok(payload) => Response::ok(ctx.req_id, payload),
        Err(e) => Response::err(ctx.req_id, e),
    }
}

/// The replay-repair check of [`commit_conditional_mutation_async_inner`]'s durable
/// path — the async-apply twin of [`commit_mutation_body_replay_check`]. `Some(
/// response)` is the caller's immediate return; `None` means no prior committed
/// batch exists.
pub(super) async fn commit_conditional_replay_check(
    ctx: &MutationCtx<'_>,
    plan: &MutationPlan,
    method: &Method,
    persistence: &Arc<dyn PersistenceBackend>,
    fname: &str,
    batch_id: &str,
    prep: &CommitPrep,
) -> Option<Response> {
    let default_surface = if is_rdf_gateway_method(method) {
        crate::mutation_batch::MutationSurface::Rdf
    } else {
        crate::mutation_batch::MutationSurface::Query
    };
    commit_staged_replay_probe(
        ctx,
        plan,
        vec![method.clone()],
        default_surface,
        DurableBatchTarget {
            persistence,
            fname,
            batch_id,
        },
        prep,
    )
    .await
}

/// The staged apply + diff half of [`commit_conditional_mutation_async_inner`]'s
/// durable path — the async-apply twin of
/// [`super::commit_paths::commit_mutation_body_stage_and_diff`].
/// Query handlers never receive the live serving core, so an execution or durable
/// commit failure cannot leak a partial mutation into RAM.
pub(super) async fn commit_conditional_stage_and_diff<F, Fut>(
    ctx: &MutationCtx<'_>,
    persistence: &Arc<dyn PersistenceBackend>,
    fname: &str,
    apply: F,
) -> Result<StagedMutation, Response>
where
    F: FnOnce(Arc<GraphCore>) -> Fut,
    Fut: std::future::Future<Output = Result<ResultPayload, String>>,
{
    let (base_snapshot, source_version) =
        resolve_authoritative_base_snapshot(ctx, persistence, fname).await?;
    let base_snapshot_for_delta = base_snapshot.clone();
    let staged = match GraphCore::from_snapshot(base_snapshot, source_version) {
        Ok(staged) => Arc::new(staged),
        Err(error) => {
            return Err(Response::err(
                ctx.req_id,
                format!("graph staging failed: {error}"),
            ));
        }
    };
    let payload = match apply(Arc::clone(&staged)).await {
        Ok(payload) => payload,
        Err(error) => return Err(Response::err(ctx.req_id, error)),
    };
    // X5-enforce, native writes (W4.13, CONCEPT:EG-KG.ontology.rdf-update-guard): this
    // is the path `CypherQuery` writes actually take (see the doc comment above) —
    // the RDF surface routed here (AddTriples/RemoveTriples/DropNamedGraph) already
    // enforces via `check_before_write` inside its own handler before the staged
    // mutation lands, so this call is a fast no-op for those (no diff to find);
    // for a native Cypher write it is the ONLY enforcement point.
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

/// The durable-commit + publish half of
/// [`commit_conditional_mutation_async_inner`]'s durable path — the async-apply
/// twin of [`commit_mutation_body_commit_staged`]. The default mutation surface
/// (RDF vs Query) is derived from `method` since this gateway serves both.
pub(super) async fn commit_conditional_commit_staged(
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
    let default_surface = if is_rdf_gateway_method(method) {
        crate::mutation_batch::MutationSurface::Rdf
    } else {
        crate::mutation_batch::MutationSurface::Query
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
            default_surface,
            authoritative_state: Some(descriptor),
        },
        vec![method.clone()],
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
    }
    if let Err(error) =
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

/// Is `m` one of the query-surface gateway methods routed via
/// [`commit_conditional_mutation_async`] at the query dispatch site (they need
/// `state`/`rls` the graph-ops `try_handle_gateway` entry point does not carry)?
/// Part of [`GATEWAY_ROUTED`], but handed back by `try_handle_gateway` so it reaches
/// its dedicated wrap in `dispatch.rs`.
pub fn is_query_gateway_method(m: &Method) -> bool {
    matches!(method_variant_name(m), "Sql" | "CypherQuery" | "GraphQl")
}

/// SQL table/catalog writes and GraphQL cross-modal operations own native domain
/// coordinators. Their commit points now include universal MutationBatch status,
/// fences, idempotency and outbox, but they must not also be wrapped in a second
/// graph-snapshot batch. Ordinary graph SQL/GraphQL mutations still return `false`
/// and use the universal staged gateway.
pub fn is_query_native_coordinator(m: &Method) -> bool {
    #[cfg(feature = "query")]
    if let Method::Sql { query, .. } = m {
        use eg_query::StatementKind;
        return match eg_query::classify(query) {
            Ok(
                StatementKind::InsertNodes(_)
                | StatementKind::InsertNodesSelect(_)
                | StatementKind::UpdateNodes(_)
                | StatementKind::UpdateNodesJoin(_)
                | StatementKind::DeleteNodes(_)
                | StatementKind::DeleteNodesJoin(_),
            ) => false,
            // User-table/catalog statements atomically commit their rows and
            // universal batch metadata inside TableStore. Running them against a
            // graph staging closure would introduce a second, unrelated authority.
            Ok(_) => true,
            Err(_) => false,
        };
    }
    #[cfg(feature = "graphql")]
    if let Method::GraphQl { query, .. } = m {
        return !matches!(
            eg_graphql::classify_crossmodal(query),
            eg_graphql::CrossModalRoute::NotCrossModal
        );
    }
    let _ = m;
    false
}

/// Is `m` one of the native-RDF gateway methods routed via
/// [`commit_conditional_mutation_async`] at the rdf dispatch site (their durable
/// write carries the lossless RDF dataset in the staged graph image)? Part of
/// [`GATEWAY_ROUTED`]; handed back by `try_handle_gateway` for the same reason as
/// [`is_query_gateway_method`].
pub fn is_rdf_gateway_method(m: &Method) -> bool {
    matches!(
        method_variant_name(m),
        "AddTriples" | "RemoveTriples" | "DropNamedGraph"
    )
}
