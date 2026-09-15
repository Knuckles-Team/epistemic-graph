use std::sync::Arc;

use crate::graph::GraphCore;
use crate::isolation::IsolationLayer;
use crate::protocol::{Method, Response, ResultPayload};

use super::{
    advance_authoritative_manifest, commit_mutation, commit_mutation_body, MutationCtx,
    MutationPlan,
};

/// Is `method` one of the coalescable structural writes
/// (`AddNode`/`RemoveNode`/`AddEdge`/`RemoveEdge`/`CompareAndSetNodeFields`)?
/// These are the ONLY methods
/// routed through [`commit_coalescable_mutation`] — every other routed method
/// (`CreateSummaryNode`/`Consolidate`/`Reinforce`, the rest of the graph-core
/// family, broker/query/RDF, …) is a multi-field or async-execution op the
/// coalescer does not model and keeps going through the ordinary
/// [`commit_mutation`] single-call path.
enum CoalescableWrite<'a> {
    AddNode {
        node_id: &'a str,
        properties_msgpack: &'a [u8],
    },
    RemoveNode {
        node_id: &'a str,
    },
    AddEdge {
        source_id: &'a str,
        target_id: &'a str,
        properties_msgpack: &'a [u8],
    },
    RemoveEdge {
        source_id: &'a str,
        target_id: &'a str,
    },
    CompareAndSetNodeFields {
        node_id: &'a str,
        conditions_msgpack: &'a [u8],
        updates_msgpack: &'a [u8],
    },
}

impl<'a> TryFrom<&'a Method> for CoalescableWrite<'a> {
    type Error = ();

    fn try_from(method: &'a Method) -> Result<Self, Self::Error> {
        match method {
            Method::AddNode {
                node_id,
                properties_msgpack,
            } => Ok(Self::AddNode {
                node_id,
                properties_msgpack,
            }),
            Method::RemoveNode { node_id } => Ok(Self::RemoveNode { node_id }),
            Method::AddEdge {
                source_id,
                target_id,
                properties_msgpack,
            } => Ok(Self::AddEdge {
                source_id,
                target_id,
                properties_msgpack,
            }),
            Method::RemoveEdge {
                source_id,
                target_id,
            } => Ok(Self::RemoveEdge {
                source_id,
                target_id,
            }),
            Method::CompareAndSetNodeFields {
                node_id,
                conditions_msgpack,
                updates_msgpack,
            } => Ok(Self::CompareAndSetNodeFields {
                node_id,
                conditions_msgpack,
                updates_msgpack,
            }),
            _ => Err(()),
        }
    }
}

pub(crate) fn is_coalescable_structural_write(method: &Method) -> bool {
    CoalescableWrite::try_from(method).is_ok()
}

/// Apply ONE coalescable structural write (`AddNode`/`RemoveNode`/`AddEdge`/
/// `RemoveEdge`/`CompareAndSetNodeFields`) to `core`, incrementally maintaining
/// its heavy secondary
/// indexes (vector/text/temporal) under the SAME `core.txn()` topology-lock
/// hold the write itself takes — the `apply` closure every
/// `commit_gateway_coalescable` call site in `handlers::graph_ops` uses.
///
/// This mirrors `write_coalescer::apply_batch`'s PER-OP effect (same
/// `ChangeSet` capture + `maintain_indexes_at` call), which matters for
/// correctness, not just performance: the served query path's persistent
/// (incrementally-maintained) index depends on this running for every
/// coalescable write. Calling `core.add_node()`/etc. directly and relying on
/// `commit_finalize`'s later `mark_dirty()` to signal staleness only
/// INVALIDATES the index (forcing a snapshot-derived rebuild on next read)
/// instead of keeping it current — a real regression `served_query_
/// completeness::served_ranktext_pushes_down_into_persistent_index_not_
/// snapshot_fallback` and `served_spatial_completeness::served_spatial_scan_
/// pushes_down_into_persistent_index_not_snapshot_fallback` caught.
///
/// This is NOT extracted from `apply_batch` and reused by it: `apply_batch`
/// deliberately shares ONE `core.txn()`/`ChangeSet` across its WHOLE batch
/// (that IS its own batching win, exercised by
/// `write_coalescer::tests::concurrent_writes_coalesce_into_fewer_lock_
/// acquisitions`), whereas the routed-write-coalescer worker already gets its
/// batching win at the `lock_graph` layer (see `commit_coalescable_mutation`)
/// and calls this once per op — sharing a single function across both would
/// force one or the other to give up its own txn-scoping. The five match arms
/// are intentionally the same shape as `apply_batch`'s (so the two stay easy
/// to compare/keep in sync by inspection), not shared code.
pub(crate) fn apply_coalescable_write(
    core: &GraphCore,
    method: &Method,
) -> Result<ResultPayload, String> {
    let mut txn = core.txn();
    let capture_content = core.wants_change_content();
    let mut change = crate::index::ChangeSet::new();
    let result = apply_coalescable_write_op(core, &mut txn, &mut change, capture_content, method);
    if !change.is_empty() {
        core.maintain_indexes_at(
            &change,
            core.version().saturating_add(change.len() as u64),
            txn.node_count(),
            txn.edge_count(),
        );
    }
    drop(txn);
    result
}

/// Dispatch one of the five coalescable methods to its RAM-apply arm, each of which
/// mutates `txn` and captures the corresponding [`crate::index::ChangeSet`] entry
/// (this function's five match arms are intentionally the same shape as
/// `write_coalescer::apply_batch`'s — see [`apply_coalescable_write`]'s doc).
fn apply_coalescable_write_op(
    core: &GraphCore,
    txn: &mut crate::graph::GraphTxn<'_>,
    change: &mut crate::index::ChangeSet,
    capture_content: bool,
    method: &Method,
) -> Result<ResultPayload, String> {
    let Ok(write) = CoalescableWrite::try_from(method) else {
        unreachable!("apply_coalescable_write called with a non-coalescable method: {method:?}")
    };
    match write {
        CoalescableWrite::AddNode {
            node_id,
            properties_msgpack,
        } => apply_coalescable_add_node(txn, change, capture_content, node_id, properties_msgpack),
        CoalescableWrite::RemoveNode { node_id } => {
            apply_coalescable_remove_node(core, txn, change, node_id)
        }
        CoalescableWrite::AddEdge {
            source_id,
            target_id,
            properties_msgpack,
        } => apply_coalescable_add_edge(txn, change, source_id, target_id, properties_msgpack),
        CoalescableWrite::RemoveEdge {
            source_id,
            target_id,
        } => apply_coalescable_remove_edge(txn, change, source_id, target_id),
        CoalescableWrite::CompareAndSetNodeFields {
            node_id,
            conditions_msgpack,
            updates_msgpack,
        } => apply_coalescable_cas_fields(
            txn,
            change,
            capture_content,
            node_id,
            conditions_msgpack,
            updates_msgpack,
        ),
    }
}

fn apply_coalescable_add_node(
    txn: &mut crate::graph::GraphTxn<'_>,
    change: &mut crate::index::ChangeSet,
    capture_content: bool,
    node_id: &str,
    properties_msgpack: &[u8],
) -> Result<ResultPayload, String> {
    if capture_content {
        change
            .added_nodes
            .push(crate::index::NodeChange::with_properties(
                node_id.to_string(),
                properties_msgpack.to_vec(),
            ));
    } else {
        change.record_add_node(node_id.to_string());
    }
    txn.add_node(node_id.to_string(), properties_msgpack.to_vec());
    Ok(ResultPayload::scalar::<
        eg_types::result_contract::graph::AddNode,
    >("ok".to_string()))
}

fn apply_coalescable_remove_node(
    core: &GraphCore,
    txn: &mut crate::graph::GraphTxn<'_>,
    change: &mut crate::index::ChangeSet,
    node_id: &str,
) -> Result<ResultPayload, String> {
    match txn.get_node_properties(node_id) {
        Some(props) => change.record_remove_node_with_properties(node_id.to_string(), props),
        None => change.record_remove_node(node_id.to_string()),
    }
    txn.remove_node(node_id.to_string());
    core.semantic_store.write().remove_embedding(node_id);
    Ok(ResultPayload::scalar::<
        eg_types::result_contract::graph::RemoveNode,
    >("ok".to_string()))
}

fn apply_coalescable_add_edge(
    txn: &mut crate::graph::GraphTxn<'_>,
    change: &mut crate::index::ChangeSet,
    source_id: &str,
    target_id: &str,
    properties_msgpack: &[u8],
) -> Result<ResultPayload, String> {
    match txn.add_edge(
        source_id.to_string(),
        target_id.to_string(),
        properties_msgpack.to_vec(),
    ) {
        Ok(()) => {
            change.record_add_edge(source_id.to_string(), target_id.to_string());
            Ok(ResultPayload::String("ok".to_string()))
        }
        Err(e) => Err(e),
    }
}

fn apply_coalescable_remove_edge(
    txn: &mut crate::graph::GraphTxn<'_>,
    change: &mut crate::index::ChangeSet,
    source_id: &str,
    target_id: &str,
) -> Result<ResultPayload, String> {
    change.record_remove_edge(source_id.to_string(), target_id.to_string());
    txn.remove_edge(source_id.to_string(), target_id.to_string());
    Ok(ResultPayload::scalar::<
        eg_types::result_contract::graph::RemoveEdge,
    >("ok".to_string()))
}

fn apply_coalescable_cas_fields(
    txn: &mut crate::graph::GraphTxn<'_>,
    change: &mut crate::index::ChangeSet,
    capture_content: bool,
    node_id: &str,
    conditions_msgpack: &[u8],
    updates_msgpack: &[u8],
) -> Result<ResultPayload, String> {
    let conditions = match eg_types::msgpack::decode_property_object(conditions_msgpack) {
        Ok(value) => value,
        Err(_) => return Ok(ResultPayload::Bool(false)),
    };
    let updates = match eg_types::msgpack::decode_property_object(updates_msgpack) {
        Ok(value) => value,
        Err(_) => return Ok(ResultPayload::Bool(false)),
    };
    let ok = txn.compare_and_set_fields(node_id, &conditions, &updates);
    if ok {
        if capture_content {
            let blob = rmp_serde::to_vec_named(&serde_json::Value::Object(updates.clone()))
                .unwrap_or_default();
            change
                .updated_nodes
                .push(crate::index::NodeChange::with_properties_and_fields(
                    node_id.to_string(),
                    blob,
                    updates.keys().cloned().collect(),
                ));
        } else {
            change
                .updated_nodes
                .push(crate::index::NodeChange::with_fields(
                    node_id.to_string(),
                    updates.keys().cloned().collect(),
                ));
        }
    }
    Ok(ResultPayload::Bool(ok))
}

/// The commit gateway entry point for a coalescable structural write
/// (CONCEPT:EG-KG.sharding.per-graph-write-coalescer, L18 rewrite). Identical contract to
/// [`commit_mutation`] (same authz/durability/audit/CDC/idempotency guarantees,
/// same `Response`), but for the five hot-path methods
/// (`AddNode`/`RemoveNode`/`AddEdge`/`RemoveEdge`/`CompareAndSetNodeFields`) it batches the WHOLE
/// prepare→durable→publish sequence — not just the RAM apply — with concurrent
/// siblings on the SAME graph, via the per-graph
/// `server::routed_write_coalescer` worker.
///
/// ## Why the whole sequence, not just the RAM apply
///
/// An earlier version of this fix released `lock_graph` around just the RAM
/// publish (leaving the durable `commit_mutation_batch` call under the
/// caller's OWN, per-op lock hold, as it always was). That is UNSAFE: once a
/// caller's durable commit finishes and it drops the lock — before its RAM
/// publish has actually landed in `core` — ANY other `lock_graph` holder that
/// reads live graph state (`handlers::txn::commit_transaction`'s
/// `txn.validate(&core)`, `dispatch::ApplyChangeEnvelope`'s `core.version()`
/// check, or a second coalescable write's own CDC pre-image capture) can
/// observe a `core` that is durably stale — behind the redb-authoritative
/// version by exactly the pending RAM publish. Worse: Transaction Commit
/// performs validate → durable-commit → RAM-publish atomically under ONE
/// `lock_graph` hold, so if it interleaves in that gap its RAM writes land in
/// `core` BEFORE the earlier caller's still-pending write — producing a RAM
/// apply order (T then A) that diverges from the durable commit order (A then
/// T), a lasting divergence between served and authoritative state that only
/// self-heals on reload. See `mutation_batch::lock_graph`'s own doc: "OCC
/// validation cannot race a gateway write while its durable-before-RAM batch
/// is in flight."
///
/// The fix here closes that gap by construction: this function hands the
/// ENTIRE sequence (as a boxed `'static` job — see
/// `server::routed_write_coalescer`) to the per-graph worker, which acquires
/// `lock_graph` ONCE and runs every queued job's full sequence
/// (`commit_mutation_body`) inside that ONE hold before releasing. There is no
/// window in which a durable commit is visible to redb but not yet to `core`
/// while `lock_graph` is free for anyone else to take.
///
/// Durable commits are still issued one per op (never merged across callers —
/// see `commit_mutation_body`'s doc), so the batching win is lock
/// ACQUISITIONS, not durable WRITES: N concurrent callers to the same graph
/// pay `⌈N / max_batch⌉` `lock_graph` acquisitions instead of N.
///
/// On a full/closed coalescer queue this returns an explicit `BUSY` response
/// without running the job. Rejected work has no durable or RAM effect, and an
/// overflow path therefore cannot overtake an accepted ticket.
pub async fn commit_coalescable_mutation<F>(
    ctx: &MutationCtx<'_>,
    plan: &MutationPlan,
    method: &Method,
    apply: F,
) -> Response
where
    F: FnOnce(&GraphCore) -> Result<ResultPayload, String> + Send + 'static,
{
    debug_assert!(
        is_coalescable_structural_write(method),
        "commit_coalescable_mutation called with a non-coalescable method"
    );
    let Some(coalescer) = ctx.write_coalescer else {
        // No coalescer configured: identical to the ordinary path.
        return commit_mutation(ctx, plan, method, apply).await;
    };
    let response = commit_via_coalescer(ctx, plan, method, apply, coalescer).await;
    advance_authoritative_manifest(ctx, plan, &response);
    response
}

/// Type-erased, boxed "apply" closure for one coalescable mutation — the same
/// shape [`commit_via_coalescer`] needs after erasing the per-call generic
/// `F: FnOnce(&GraphCore) -> Result<ResultPayload, String> + Send + 'static`
/// so it can be captured into a `'static` boxed future and enqueued on the
/// coalescer worker (clippy `type_complexity`: the raw
/// `Box<dyn FnOnce(&GraphCore) -> Result<ResultPayload, String> + Send>`
/// spelled out inline was flagged as very complex).
type BoxedApplyFn = Box<dyn FnOnce(&GraphCore) -> Result<ResultPayload, String> + Send>;

struct OwnedMutationCtx {
    req_id: u64,
    caller: Option<String>,
    attempt_nonce: Option<eg_types::contract::Nonce>,
    idempotency_key: String,
    tenant_scope: String,
    graph_name: String,
    graph_type: crate::protocol::GraphType,
    owner: Option<String>,
    isolation: IsolationLayer,
    core: Arc<GraphCore>,
    persistence: Option<Arc<dyn crate::server::persistence::PersistenceBackend>>,
    #[cfg(feature = "streaming")]
    cdc: Option<Arc<crate::server::cdc::CdcHub>>,
    materialization_manifest:
        Option<Arc<std::sync::RwLock<crate::registry::MaterializationManifest>>>,
}

impl OwnedMutationCtx {
    fn from_context(ctx: &MutationCtx<'_>) -> Self {
        Self {
            req_id: ctx.req_id,
            caller: ctx.caller.map(str::to_owned),
            attempt_nonce: ctx.attempt_nonce,
            idempotency_key: ctx.idempotency_key.to_owned(),
            tenant_scope: ctx.tenant_scope.to_owned(),
            graph_name: ctx.graph_name.to_owned(),
            graph_type: ctx.graph_type,
            owner: ctx.owner.map(str::to_owned),
            isolation: ctx.isolation.clone(),
            core: ctx.core.clone(),
            persistence: ctx.persistence.cloned(),
            #[cfg(feature = "streaming")]
            cdc: ctx.cdc.cloned(),
            materialization_manifest: ctx.materialization_manifest.cloned(),
        }
    }

    fn borrow(&self) -> MutationCtx<'_> {
        MutationCtx {
            req_id: self.req_id,
            caller: self.caller.as_deref(),
            attempt_nonce: self.attempt_nonce,
            idempotency_key: &self.idempotency_key,
            tenant_scope: &self.tenant_scope,
            graph_name: &self.graph_name,
            graph_type: self.graph_type,
            owner: self.owner.as_deref(),
            isolation: &self.isolation,
            core: &self.core,
            persistence: self.persistence.as_ref(),
            #[cfg(feature = "streaming")]
            cdc: self.cdc.as_ref(),
            materialization_manifest: self.materialization_manifest.as_ref(),
            write_coalescer: None,
        }
    }
}

/// Package one coalescable op's full `commit_mutation_body` sequence as a
/// boxed `'static` job (detaching it from `ctx`'s borrow — the worker task
/// that eventually runs it outlives this call), enqueue it on this graph's
/// routed-write-coalescer worker, and await its `Response`. A full or closed
/// queue returns `BUSY`; the job is never run outside the ordered drain.
async fn commit_via_coalescer<F>(
    ctx: &MutationCtx<'_>,
    plan: &MutationPlan,
    method: &Method,
    apply: F,
    coalescer: &Arc<crate::server::routed_write_coalescer::RoutedWriteCoalescerRegistry>,
) -> Response
where
    F: FnOnce(&GraphCore) -> Result<ResultPayload, String> + Send + 'static,
{
    // Detach everything `commit_mutation_body` needs from `ctx`'s borrow. The
    // `IsolationLayer` clone mirrors the SAME cost `dispatch_graph_op_inner`
    // already pays once per gateway-routed request (see its
    // `gateway_authz_ctx` comment: "an IsolationLayer clone is not free, so
    // this is skipped entirely for the other ~330 methods") — this is a
    // second clone specifically for the coalescable subset, so the job can
    // run on the worker task after this request's own stack frame is gone.
    let req_id = ctx.req_id;
    let owned = OwnedMutationCtx::from_context(ctx);
    let plan = plan.clone();
    let method = method.clone();
    let apply: BoxedApplyFn = Box::new(apply);

    let run: std::pin::Pin<Box<dyn std::future::Future<Output = Response> + Send>> =
        Box::pin(async move {
            // Never re-enqueue from inside a job that is the dequeued unit of
            // work; this context is only used for the direct commit below.
            let owned_ctx = owned.borrow();
            commit_mutation_body(&owned_ctx, &plan, &method, apply).await
        });

    let (reply, reply_rx) = tokio::sync::oneshot::channel();
    let writer = coalescer.writer_for(ctx.graph_name);
    let job = crate::server::routed_write_coalescer::RoutedCommitJob::new(run, reply)
        .with_request_id(req_id);
    match writer.try_enqueue(job) {
        Ok(()) => reply_rx.await.unwrap_or_else(|_| {
            Response::err(req_id, "routed write worker unavailable".to_string())
        }),
        Err(job) => {
            // The bounded queue is the ordering authority. Drop the unaccepted
            // job and shed the request explicitly; running it here would let it
            // acquire `lock_graph` before an already accepted queued write.
            drop(job);
            Response::err(
                req_id,
                "BUSY: routed write coalescer queue is full; retry with backoff",
            )
        }
    }
}
