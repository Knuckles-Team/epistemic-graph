use std::sync::Arc;

use eg_capabilities::DurabilityDomain;

use crate::graph::GraphCore;
use crate::isolation::AccessLevel;
use crate::protocol::{Method, Response, ResultPayload};
use crate::server::access::check_graph_access;
use crate::server::persistence::PersistenceBackend;

use super::{
    advance_authoritative_manifest, commit_mutation_body_prepublish_fast_path,
    commit_mutation_body_staged_path, idempotency_key, idempotency_store, prepublish_success,
    DurableBatchAttempt, DurableBatchTarget, MutationCtx, MutationPlan,
};

/// Pre-apply state carried from [`commit_prepare`] to [`commit_finalize`], so the
/// SAME steps-1-3 / steps-5-8 logic backs BOTH the sync-apply [`commit_mutation`]
/// (graph-core/broker/memory ops) and the async-apply
/// [`commit_conditional_mutation_async`] (the query + RDF surfaces, whose execution
/// is `async` and needs `state`/`rls`) — one implementation, never two drifting
/// copies of the durability/audit/CDC/idempotency contract.
pub(super) struct CommitPrep {
    /// `Some` only for a policy-idempotent method — the replay-dedup cache key.
    pub(super) dedup_key: Option<String>,
    /// CDC pre-image captured before apply (Skip when policy `emits_cdc == false`).
    #[cfg(feature = "streaming")]
    pub(super) cdc_pre: crate::server::cdc::CdcPre,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct CommitFinalizeOptions {
    pub(super) durability_already_committed: bool,
    pub(super) preserve_node_indexes: bool,
}

/// Steps 1-3 of the commit gateway (CONCEPT:EG-P0-2): Write-authz, idempotency-replay
/// short-circuit, and the pre-apply CDC image. Returns `Err(Response)` to short-
/// circuit the whole call (an ACCESS_DENIED, or a cached idempotent replay — no re-
/// apply), else `Ok(CommitPrep)` for the caller to run apply + [`commit_finalize`].
pub(super) fn commit_prepare(
    ctx: &MutationCtx<'_>,
    plan: &MutationPlan,
    method: &Method,
) -> Result<CommitPrep, Response> {
    // 1. Authz -- the SAME isolation ACL check every other graph-scoped method
    // goes through, driven by the plan's `mutates` flag. (A read-only invocation of
    // a runtime-conditional method never reaches here — see
    // `commit_conditional_mutation_async`'s `mutates_now == false` branch.)
    if !consensus_apply_is_authorized() {
        if let Err(denied) = check_graph_access(
            ctx.isolation,
            ctx.caller,
            ctx.graph_name,
            ctx.graph_type,
            ctx.owner,
            AccessLevel::Write,
        ) {
            return Err(Response::err(ctx.req_id, denied));
        }
    }

    // 2. Idempotency-replay dedup -- ONLY for methods policy marks idempotent. A
    // byte-identical replay short-circuits BEFORE touching storage: no re-apply, no
    // second durable record, no second audit-chain entry, no duplicate CDC event.
    //
    // BUG (found via a hang repro: two `ClearGraph` calls on the same connection,
    // the second never returning): the cached `Response` was replayed VERBATIM,
    // including its `id` field frozen at whatever request first populated this
    // dedup key. `idempotency_key` is content-addressed on `(graph_name, method)`
    // only -- for a zero-argument method like `ClearGraph` that key is the SAME
    // for every call to this graph for the rest of the process's life, so every
    // later caller got back a response correlated to a DIFFERENT (the very
    // first) request id. The transport demuxes purely by `Response.id`
    // (`epistemic_graph/client.py`'s `self._pending[req_id]`), so a mismatched id
    // is silently unmatchable -- the caller's future never resolves and the RPC
    // hangs until its own client-side timeout, even though the engine already
    // "answered". Rewriting `id` to the CURRENT request before returning is
    // required for ANY cached-response replay, independent of key granularity --
    // a response must always correlate to the request it is answering.
    let dedup_key = (plan.idempotent && matches!(plan.durability_domain, DurabilityDomain::None))
        .then(|| idempotency_key(ctx.graph_name, method, ctx.req_id));
    if let Some(key) = &dedup_key {
        if let Some(mut cached) = idempotency_store().get(key) {
            cached.id = ctx.req_id;
            return Err(cached);
        }
    }

    // 3. CDC pre-image, captured BEFORE the mutation applies -- gated on the
    // POLICY's `emits_cdc`.
    #[cfg(feature = "streaming")]
    let cdc_pre = if plan.emits_cdc {
        crate::server::cdc::capture_before(ctx.core, method)
    } else {
        crate::server::cdc::CdcPre::Skip
    };

    Ok(CommitPrep {
        dedup_key,
        #[cfg(feature = "streaming")]
        cdc_pre,
    })
}

/// Steps 5-8 of the commit gateway: mark-dirty, durable commit (which for a redb
/// backend also appends the tamper-evident audit-chain entry — see module docs),
/// CDC emit (+ the `epistemic-tms` truth-maintenance hook, step 7.5, riding the same
/// ordering), and idempotency-replay cache insert. `response` is the applied
/// outcome; on an ERROR response NOTHING durable/audited/CDC-emitted/cached happens.
pub(super) async fn commit_finalize(
    ctx: &MutationCtx<'_>,
    plan: &MutationPlan,
    method: &Method,
    response: Response,
    prep: CommitPrep,
    options: CommitFinalizeOptions,
) -> Response {
    if response.error.is_some() {
        return response;
    }

    // 5. Mark the graph dirty (checkpoint scheduling). Idempotent flag set.
    commit_finalize_mark_dirty(ctx, plan, options.preserve_node_indexes);

    // 6. Durable commit -- reuses the EXISTING `PersistenceBackend` plumbing.
    if !options.durability_already_committed
        && !matches!(plan.durability_domain, DurabilityDomain::None)
    {
        if let Some(err_response) = commit_finalize_durable(ctx, method).await {
            return err_response;
        }
    }

    // 6.5. Refresh the per-graph node/edge size gauges. See
    // `commit_finalize_refresh_size_gauges`'s doc for why this runs here.
    #[cfg(feature = "metrics")]
    commit_finalize_refresh_size_gauges(ctx, plan);

    // 7. CDC emit -- only AFTER the authoritative durable commit succeeds.
    #[cfg(feature = "streaming")]
    commit_finalize_emit_cdc(ctx, plan, method, prep.cdc_pre);

    // 8. Cache the response for idempotent-replay dedup.
    if let Some(key) = prep.dedup_key {
        idempotency_store().insert(key, response.clone());
    }

    response
}

/// Step 5 of [`commit_finalize`].
pub(super) fn commit_finalize_mark_dirty(
    ctx: &MutationCtx<'_>,
    plan: &MutationPlan,
    preserve_node_indexes: bool,
) {
    if !plan.mutates {
        return;
    }
    if preserve_node_indexes {
        ctx.core.mark_dirty_preserving_indexes();
    } else {
        ctx.core.mark_dirty();
    }
}

/// Step 6 of [`commit_finalize`]: the durable commit. `Some(response)` is the
/// caller's early-return error response (no persistence backend configured, or the
/// backend's write itself failed); `None` means it succeeded.
pub(super) async fn commit_finalize_durable(
    ctx: &MutationCtx<'_>,
    method: &Method,
) -> Option<Response> {
    let Some(persistence) = ctx.persistence else {
        return Some(Response::err(
            ctx.req_id,
            "durable mutation requires a persistence backend",
        ));
    };
    let fname = crate::persist::sanitize(ctx.graph_name);
    if let Err(e) = persistence.record_durable(&fname, method).await {
        return Some(Response::err(
            ctx.req_id,
            format!("durable commit failed (write not acknowledged): {e}"),
        ));
    }
    None
}

/// Step 6.5 of [`commit_finalize`]: refresh the per-graph node/edge size gauges.
/// `commit_finalize` is the universal mutation gateway essentially all current
/// writes route through (GATEWAY_ROUTED) -- the refresh previously lived ONLY in
/// the legacy non-gateway dispatch tail (`dispatch.rs`'s post-`'dispatch` block)
/// and Raft snapshot-install (`raft/store.rs`), so a graph mutated exclusively via
/// gateway-routed writes (the common case -- AddNode/AddEmbedding/etc.) never
/// refreshed its gauge at all and silently froze at its last snapshot-install
/// value. Confirmed live: the `/metrics` gauge sat at 56,882 nodes while a live
/// `NodeCount` RPC, a Cypher `count(n)`, and full label enumeration all
/// independently agreed on 25,122 -- unchanged across two scrapes ten minutes
/// apart despite `graph_ops_total` advancing by ~30k in that window.
///
/// Placed AFTER the durable-commit step (not alongside `mark_dirty` in step 5) so
/// it only fires once a mutation is confirmed durable, and gated on `plan.mutates`
/// exactly like `mark_dirty` above and `dispatch.rs`'s equivalent
/// `AccessLevel::Write` gate, so a read-only gateway call never pays for it. Both
/// counts are O(1) (`GraphCore::node_count`/`edge_count` read the already-resident
/// `StableGraph`'s own cardinality, no full scan) -- the same cost `dispatch.rs`
/// already accepted on every mutation ("both petgraph counts are O(1), so this
/// adds no meaningful write-path cost"), so refreshing unconditionally on every
/// commit (rather than on a bounded cadence) is the right choice here too: a gauge
/// that costs an O(1) read on the write path is strictly better than one that
/// silently freezes, and there is no hot-loop concern (this fires once per
/// already-durably-committed mutation, not once per poll).
#[cfg(feature = "metrics")]
pub(super) fn commit_finalize_refresh_size_gauges(ctx: &MutationCtx<'_>, plan: &MutationPlan) {
    if !plan.mutates {
        return;
    }
    crate::metrics::set_graph_size(
        ctx.graph_name,
        ctx.core.node_count() as i64,
        ctx.core.edge_count() as i64,
    );
}

/// Step 7 of [`commit_finalize`]: CDC emit, only after the authoritative durable
/// commit succeeds.
#[cfg(feature = "streaming")]
pub(super) fn commit_finalize_emit_cdc(
    ctx: &MutationCtx<'_>,
    plan: &MutationPlan,
    method: &Method,
    cdc_pre: crate::server::cdc::CdcPre,
) {
    if !plan.emits_cdc {
        return;
    }
    let Some(hub) = ctx.cdc else { return };
    crate::server::cdc::emit_for_method(hub, ctx.core, ctx.graph_name, method, cdc_pre);
}

/// The single commit gateway (CONCEPT:EG-P0-2): authz, apply, durable commit,
/// audit (delegated — see module docs), CDC, and idempotency-replay dedup, all in
/// ONE call, driven entirely by `plan` (sourced from `eg_capabilities::policy`).
///
/// `apply` performs the actual `eg-core` mutation and returns the success payload;
/// it is only invoked after authz passes and (for an idempotent replay hit) is
/// skipped entirely. On a failed apply, nothing durable/audited/CDC-emitted/cached
/// happens.
pub async fn commit_mutation<F>(
    ctx: &MutationCtx<'_>,
    plan: &MutationPlan,
    method: &Method,
    apply: F,
) -> Response
where
    F: FnOnce(&GraphCore) -> Result<ResultPayload, String>,
{
    let response = commit_mutation_inner(ctx, plan, method, apply).await;
    advance_authoritative_manifest(ctx, plan, &response);
    response
}

pub(super) async fn commit_mutation_inner<F>(
    ctx: &MutationCtx<'_>,
    plan: &MutationPlan,
    method: &Method,
    apply: F,
) -> Response
where
    F: FnOnce(&GraphCore) -> Result<ResultPayload, String>,
{
    // Every mutation shares one logical graph lane from validation through RAM
    // publication. Durable domains fail closed without an authoritative batch
    // backend. This is the ordinary single-call path: one op, one lock
    // acquisition, exactly like every non-coalescable routed method. The five
    // coalescable structural writes (AddNode/RemoveNode/AddEdge/RemoveEdge/CAS) go
    // through [`commit_coalescable_mutation`] instead, which acquires this SAME
    // lock ONCE PER BATCH in the routed-write-coalescer worker and calls
    // [`commit_mutation_body`] once per queued op inside that one hold — see its
    // doc comment for the invariant this preserves and why it must.
    let _mutation_guard = crate::server::mutation_batch::lock_graph(ctx.graph_name).await;
    commit_mutation_body(ctx, plan, method, apply).await
}

/// The prepare → durable-commit → RAM-publish sequence for ONE routed mutation
/// (CONCEPT:EG-P0-2 / L18 rewrite): authz, idempotency-replay short-circuit, CDC
/// pre-image, durable commit, apply, mark-dirty/RAM publish, CDC emit, and
/// idempotency-replay cache insert.
///
/// ASSUMES the caller already holds this graph's `lock_graph` lane for the
/// ENTIRE call — this function never acquires or releases it. That is what
/// makes it safely reusable by TWO callers with different lock-hold
/// granularity, with no second, diverging copy of this durability/audit/CDC
/// kernel:
///   * [`commit_mutation_inner`] — the ordinary path — acquires the lock, calls
///     this ONCE, releases it. One lock acquisition per op, as before.
///   * the routed-write-coalescer worker (`server::routed_write_coalescer::
///     run_worker`) — acquires the lock ONCE per flushed batch, then calls this
///     N times back-to-back (once per queued op, in FIFO order) before
///     releasing. One lock acquisition per BATCH, not per durable write: each
///     op still gets its OWN `commit_mutation_batch` call (own principal/
///     tenant/idempotency-key/audit/CDC — batching durable commits across
///     different callers would misattribute provenance and is deliberately
///     NOT done), but no third party (Transaction Commit, ApplyChangeEnvelope,
///     another coalescable write, WorkItem commit, …) can acquire `lock_graph`
///     between one op's durable commit and its RAM publish, because the SAME
///     lock stays held across the whole batch. That is the fix for the race
///     documented on `commit_coalescable_mutation`: a durably-committed-but-
///     not-yet-RAM-published write can no longer be observed by anyone who
///     must hold `lock_graph` to look.
pub(super) async fn commit_mutation_body<F>(
    ctx: &MutationCtx<'_>,
    plan: &MutationPlan,
    method: &Method,
    apply: F,
) -> Response
where
    F: FnOnce(&GraphCore) -> Result<ResultPayload, String>,
{
    let prep = match commit_prepare(ctx, plan, method) {
        Ok(p) => p,
        Err(short_circuit) => return short_circuit,
    };

    if matches!(plan.durability_domain, DurabilityDomain::None) {
        let response = match apply(ctx.core) {
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
        "rpc",
        ctx.tenant_scope,
        ctx.graph_name,
        ctx.caller,
        ctx.idempotency_key,
    );

    let created_at_ms = crate::server::dispatch::authoritative_now_ms();

    // Row-local operations have a deterministic success result and can commit
    // their compact Method directly before applying the serving projection.
    if let Some(predicted) = prepublish_success(ctx.core, method) {
        return commit_mutation_body_prepublish_fast_path(
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
            predicted,
            prep,
            apply,
        )
        .await;
    }

    // Runtime-result operations execute against an isolated authoritative image,
    // never the live projection. Its authenticated affected rows, terminal result,
    // batch, version/fence, and outbox share one redb commit point.
    commit_mutation_body_staged_path(
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
        apply,
    )
    .await
}
