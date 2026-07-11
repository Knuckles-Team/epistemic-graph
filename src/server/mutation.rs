//! `MutationPlan` + the single commit gateway (CONCEPT:EG-P0-2).
//!
//! ## Why this exists
//!
//! `eg-capabilities` (CONCEPT:EG-P0-1) is a machine-checked, exhaustive `MethodPolicy`
//! table over every `Method` variant — but on its own it is a one-way AUDIT: it
//! cross-checks the existing hand-rolled classifiers (`access.rs::requires_write`,
//! `wal.rs::is_durable_mutation`, `audit.rs::audit_line`, `cdc.rs::emit_for_method`)
//! against its declared policy, without either side CONSUMING the other. Handlers
//! still mutate `eg-core` directly (`src/server/handlers/graph_ops.rs`'s
//! `g.add_node(...)` etc.), and the dispatch shell (`src/server/dispatch.rs`)
//! separately re-derives whether to WAL-record / CDC-emit a given method from its
//! own classifiers.
//!
//! This module is the other direction: [`MutationPlan`] is POPULATED FROM
//! `eg_capabilities::policy` (never re-hardcoded), and [`commit_mutation`] is the
//! ONE place a routed mutation's authz + durable write + CDC emission happen
//! together, driven by that plan. The invariant this buys: for a method in
//! [`GATEWAY_ROUTED`], mutation + durability + audit + CDC happen together, in one
//! call, declared by policy — never scattered back across the dispatch shell.
//!
//! ## Scope (read before assuming more is covered)
//!
//! Only [`GATEWAY_ROUTED`] methods are wired through this gateway —
//! `AddNode`/`RemoveNode`/`AddEdge`/`RemoveEdge` (the primary node/edge CRUD) and
//! `CreateSummaryNode`/`Consolidate`/`Reinforce` (the agent-memory writes). This is
//! a DELIBERATE, documented increment (EG-P0-2), not a claim of full coverage — see
//! `tests::gateway_routed_set_matches_mutating_policy_surface` for the machine-
//! visible backlog of mutating methods NOT yet migrated. `src/server/dispatch.rs`
//! wires `handlers::graph_ops::try_handle_gateway` in AHEAD of both the dispatch-
//! shell write-coalescer branch (`try_coalesce_write`) and the legacy WAL/CDC tail
//! for exactly this set, so there is never a double-apply: a routed method never
//! reaches `dispatch::try_coalesce_write`, the old direct-`eg-core`-call arms in
//! `graph_ops::try_handle`, or the dispatch shell's own `record_method`/`cdc_pre`/
//! `cdc_method` computation (all three are gated off [`is_gateway_routed`] there).
//! The coalescer BATCHING itself is not lost, though — the gateway re-enters the
//! SAME per-graph writer from INSIDE `commit_mutation` (L18, see
//! [`try_coalesce_apply`]), so a routed structural write still batches.
//!
//! ## What this gateway does NOT reimplement
//!
//! Audit-chain emission (the tamper-evident hash chain, `audit.rs` +
//! `redb_store::append_audit_entry`) is stateful PER GRAPH (each entry chains off
//! the previous one's hash) and already lives inside the durable-commit path
//! (`PersistenceBackend::record`/`record_durable` → the redb backend). Reimplementing
//! that chaining here would risk a second, diverging chain. Instead,
//! `commit_mutation` DELEGATES to the existing `record`/`record_durable` call for
//! durability (which — for a redb backend — appends the audit entry atomically with
//! the data row when `audit::audit_line(method)` resolves), and `plan.audited`
//! (sourced from `eg_capabilities::policy`) is the assertable, tested FACT that this
//! delegation actually happens for the methods policy says should be audited (see
//! `tests::routed_mutation_produces_one_wal_record_one_audit_entry_and_one_cdc_event`,
//! which uses a REAL `RedbBackend` and reads the audit chain back).
//!
//! The per-graph write-coalescer's batching optimization is NO LONGER bypassed for
//! the routed set (L18/EG-P0-6): [`commit_mutation`] step 4 routes a coalescable
//! routed mutation (`AddNode`/`RemoveNode`/`AddEdge`/`RemoveEdge`) through the SAME
//! `WriteCoalescerRegistry::writer_for` path the legacy `dispatch::try_coalesce_write`
//! uses (see [`try_coalesce_apply`]), so the hot-path structural writes batch again
//! (`stats().ops()` counts them) while WAL/audit/CDC ordering is preserved. The
//! non-coalescable routed memory ops (`CreateSummaryNode`/`Consolidate`/`Reinforce`)
//! keep the inline `apply`. The Raft consensus barrier / cross-region replica log are
//! still untouched (both already return from `dispatch_graph_op` before this gateway
//! is reached, or are simply not yet wired into it) — see the module docs above and
//! the workstream report for the full backlog.

use std::sync::Arc;

use eg_capabilities::DurabilityDomain;

use crate::graph::GraphCore;
use crate::isolation::{AccessLevel, IsolationLayer};
use crate::protocol::{GraphType, Method, Response, ResultPayload};
use crate::server::access::check_graph_access;
use crate::server::persistence::PersistenceBackend;

/// The pre-authz identity/placement context a gateway call needs, captured by the
/// caller (`dispatch_graph_op`) from the graph registry BEFORE its read lock is
/// dropped — `(isolation, graph_type, owner)`, mirroring exactly what
/// `access::check_graph_access` already requires for every other graph-scoped
/// method. An owned tuple (not borrowed from the registry) because it must outlive
/// the registry lock guard.
pub type GatewayAuthzCtx = (IsolationLayer, GraphType, Option<String>);

/// The declared capability profile of one mutation, POPULATED FROM
/// `eg_capabilities::policy` -- never re-hardcoded here (CONCEPT:EG-P0-2's central
/// requirement: this crate is a consumer of that table, not a second source of
/// truth). See `eg-capabilities`' own docs for the meaning of each field.
#[derive(Debug, Clone)]
pub struct MutationPlan {
    /// The `Method` variant's name (for logging/tests only — never used to
    /// re-derive policy; see [`method_variant_name`]).
    pub method_name: &'static str,
    pub mutates: bool,
    pub durability_domain: DurabilityDomain,
    pub authz_action: &'static str,
    pub idempotent: bool,
    pub audited: bool,
    pub emits_cdc: bool,
    pub txn_participation: eg_capabilities::TxnParticipation,
}

impl MutationPlan {
    /// Build the plan for `method` straight from `eg_capabilities::policy` — the
    /// ONE source of truth. Never inspects `method`'s payload beyond what
    /// `policy()` itself does (i.e. this never hardcodes a per-variant judgment
    /// call that duplicates/diverges from the ledger).
    pub fn for_method(method: &Method) -> Self {
        let p = eg_capabilities::policy(method);
        MutationPlan {
            method_name: method_variant_name(method),
            mutates: p.mutates,
            durability_domain: p.durability_domain,
            authz_action: p.authz_action,
            idempotent: p.idempotent,
            audited: p.audited,
            emits_cdc: p.emits_cdc,
            txn_participation: p.txn_participation,
        }
    }
}

/// The methods currently routed through [`commit_mutation`] (CONCEPT:EG-P0-2). Kept
/// as a public, testable allowlist so the migration surface is machine-visible: see
/// `tests::gateway_routed_set_matches_mutating_policy_surface` for the computed
/// complement (every OTHER mutating method, per `eg_capabilities::ALL_METHODS`) —
/// that complement IS the EG-P0-2 rollout backlog.
pub const GATEWAY_ROUTED: &[&str] = &[
    "AddNode",
    "RemoveNode",
    "AddEdge",
    "RemoveEdge",
    "CreateSummaryNode",
    "Consolidate",
    "Reinforce",
];

/// Extract a `Method` variant's name as a `&'static str`, covering exactly
/// [`GATEWAY_ROUTED`] (the only names this module's logic branches on) plus a
/// catch-all `"other"` for every non-routed variant. NOT a general-purpose
/// reflection helper — deliberately narrow to this workstream's routed set.
pub fn method_variant_name(m: &Method) -> &'static str {
    match m {
        Method::AddNode { .. } => "AddNode",
        Method::RemoveNode { .. } => "RemoveNode",
        Method::AddEdge { .. } => "AddEdge",
        Method::RemoveEdge { .. } => "RemoveEdge",
        Method::CreateSummaryNode { .. } => "CreateSummaryNode",
        Method::Consolidate { .. } => "Consolidate",
        Method::Reinforce { .. } => "Reinforce",
        _ => "other",
    }
}

/// Is `method` one of [`GATEWAY_ROUTED`]? `dispatch_graph_op` uses this to (a)
/// route it to `handlers::graph_ops::try_handle_gateway` AHEAD of the write-
/// coalescer, and (b) suppress its own legacy `record_method`/`cdc_pre`/
/// `cdc_method` computation for the SAME method, so durability/audit/CDC happen
/// exactly once, from `commit_mutation`, never from both places.
pub fn is_gateway_routed(m: &Method) -> bool {
    GATEWAY_ROUTED.contains(&method_variant_name(m))
}

/// Everything [`commit_mutation`] needs beyond the plan + the apply closure.
/// Borrowed, not owned — built fresh per request from already-resolved
/// `dispatch_graph_op` locals (the registry lock is never re-acquired here).
pub struct MutationCtx<'a> {
    pub req_id: u64,
    pub caller: Option<&'a str>,
    pub graph_name: &'a str,
    pub graph_type: GraphType,
    pub owner: Option<&'a str>,
    pub isolation: &'a IsolationLayer,
    pub core: &'a Arc<GraphCore>,
    pub persistence: Option<&'a Arc<dyn PersistenceBackend>>,
    pub redb_authoritative: bool,
    #[cfg(feature = "streaming")]
    pub cdc: Option<&'a Arc<crate::server::cdc::CdcHub>>,
    /// Per-graph write-coalescer registry (CONCEPT:EG-KG.sharding.per-graph-write-coalescer, L18).
    /// When present + enabled, a coalescable routed mutation
    /// (`AddNode`/`RemoveNode`/`AddEdge`/`RemoveEdge`) has its in-memory apply
    /// BATCHED onto this graph's single-writer queue instead of taking the topology
    /// lock itself, exactly like the legacy `dispatch::try_coalesce_write` path — so
    /// routing a hot-path write through the gateway no longer loses write-batching.
    /// `None` (or a disabled/non-coalescable method) ⇒ the `apply` closure runs
    /// inline, unchanged. See [`commit_mutation`] step 4.
    pub write_coalescer: Option<&'a Arc<crate::write_coalescer::WriteCoalescerRegistry>>,
}

/// Bounded process-global idempotency-replay cache (CONCEPT:EG-P0-2). Keyed by a
/// deterministic `(method, graph, identity-args)` string (see [`idempotency_key`]).
///
/// Bounded, not TTL/LRU, for this increment: past [`MAX_IDEMPOTENCY_ENTRIES`] a new
/// key is simply not cached (fails OPEN — the mutation still applies correctly, it
/// just stops being replay-dedup-eligible until the process restarts or an entry
/// frees up) rather than evicting an arbitrary entry and risking a wrong cache hit
/// for a DIFFERENT request that happens to reuse a key. A real bounded LRU/TTL
/// policy is a documented follow-up, not required to prove the gateway pattern.
pub struct IdempotencyStore {
    seen: dashmap::DashMap<String, Response>,
}

/// Cap on live idempotency-cache entries (see [`IdempotencyStore`] docs).
const MAX_IDEMPOTENCY_ENTRIES: usize = 10_000;

impl IdempotencyStore {
    pub fn new() -> Self {
        IdempotencyStore {
            seen: dashmap::DashMap::new(),
        }
    }

    pub fn get(&self, key: &str) -> Option<Response> {
        self.seen.get(key).map(|r| r.clone())
    }

    pub fn insert(&self, key: String, response: Response) {
        if self.seen.len() < MAX_IDEMPOTENCY_ENTRIES {
            self.seen.insert(key, response);
        }
    }

    #[cfg(test)]
    pub fn clear(&self) {
        self.seen.clear();
    }
}

impl Default for IdempotencyStore {
    fn default() -> Self {
        Self::new()
    }
}

/// The process-global idempotency cache. A `OnceLock` singleton (matching the
/// `max_response_nodes()` precedent in `state.rs`) rather than a new `ServerState`
/// field, so wiring this gateway in touches none of the many `ServerState`
/// construction sites across the codebase.
fn idempotency_store() -> &'static IdempotencyStore {
    static STORE: std::sync::OnceLock<IdempotencyStore> = std::sync::OnceLock::new();
    STORE.get_or_init(IdempotencyStore::new)
}

/// Derive a deterministic replay-dedup key for an idempotent method, scoped to
/// `graph_name` (so the SAME node id in two different graphs never collides). Only
/// meaningful when `MutationPlan::idempotent` is true; the fallback `Debug`-based
/// arm covers a future idempotent addition to [`GATEWAY_ROUTED`] generically
/// (correct but not as precise as a bespoke key).
fn idempotency_key(graph_name: &str, method: &Method) -> String {
    match method {
        Method::RemoveNode { node_id } => format!("RemoveNode|{graph_name}|{node_id}"),
        Method::RemoveEdge {
            source_id,
            target_id,
        } => format!("RemoveEdge|{graph_name}|{source_id}|{target_id}"),
        other => format!("{other:?}|{graph_name}"),
    }
}

/// Try to apply a coalescable routed mutation THROUGH this graph's write-coalescer
/// (CONCEPT:EG-KG.sharding.per-graph-write-coalescer, L18), returning `Some(Response)` when it was coalesced
/// (the outcome mapped to the SAME `Response` the gateway's inline `apply` closure
/// would produce) or `None` when it must fall back to the inline closure.
///
/// `None` is returned when ANY of: no coalescer is configured on `ctx`; the
/// coalescer is disabled (`EPISTEMIC_GRAPH_WRITE_COALESCE=0`); or `method` is not
/// one of the four coalescable structural writes (`AddNode`/`RemoveNode`/`AddEdge`/
/// `RemoveEdge`) — the other routed methods (`CreateSummaryNode`/`Consolidate`/
/// `Reinforce`) are multi-field memory ops the coalescer does not model, so they
/// keep the inline closure. Mirrors `dispatch::try_coalesce_write` exactly (enqueue
/// with a per-op linger `Instant`, fall back to `apply_one_inline` on a full/closed
/// queue, await the writer's outcome), so a routed write batches identically to how
/// the same method batched on the legacy path before EG-P0-2 routed it.
async fn try_coalesce_apply(ctx: &MutationCtx<'_>, method: &Method) -> Option<Response> {
    use crate::write_coalescer::{WriteOp, WriteOutcome};
    use tokio::sync::oneshot;

    let coalescer = ctx.write_coalescer?;
    if !coalescer.enabled() {
        return None;
    }

    let (reply, reply_rx) = oneshot::channel::<WriteOutcome>();
    // Only the four coalescable structural writes map to a WriteOp; every other
    // routed method (memory ops) returns None to keep the inline apply.
    let op = match method {
        Method::AddNode {
            node_id,
            properties_msgpack,
        } => WriteOp::AddNode {
            node_id: node_id.clone(),
            properties_msgpack: properties_msgpack.clone(),
            reply,
        },
        Method::RemoveNode { node_id } => WriteOp::RemoveNode {
            node_id: node_id.clone(),
            reply,
        },
        Method::AddEdge {
            source_id,
            target_id,
            properties_msgpack,
        } => WriteOp::AddEdge {
            source_id: source_id.clone(),
            target_id: target_id.clone(),
            properties_msgpack: properties_msgpack.clone(),
            reply,
        },
        Method::RemoveEdge {
            source_id,
            target_id,
        } => WriteOp::RemoveEdge {
            source_id: source_id.clone(),
            target_id: target_id.clone(),
            reply,
        },
        _ => return None,
    };

    let writer = coalescer.writer_for(ctx.graph_name, ctx.core)?;
    // Enqueue; on a full/closed queue apply this single op inline under its own txn
    // (same engine effect, just not batched) so a saturated writer never drops or
    // stalls the write — identical to `dispatch::try_coalesce_write`.
    if let Err(op) = writer.try_enqueue(op) {
        writer.apply_one_inline(ctx.core, ctx.graph_name, op);
    }

    // Await the outcome and rebuild the exact Response the inline closure returns:
    // every coalescable routed op's closure yields `String("ok")` on success (an
    // AddEdge failure surfaces its error string).
    let outcome = reply_rx.await.unwrap_or(WriteOutcome::WriterGone);
    Some(match outcome {
        WriteOutcome::Ok => Response::ok(ctx.req_id, ResultPayload::String("ok".to_string())),
        WriteOutcome::Cas(b) => Response::ok(ctx.req_id, ResultPayload::Bool(b)),
        WriteOutcome::Err(e) => Response::err(ctx.req_id, e),
        WriteOutcome::WriterGone => {
            Response::err(ctx.req_id, "write worker unavailable".to_string())
        }
    })
}

/// The single commit gateway (CONCEPT:EG-P0-2): authz, apply, durable commit,
/// audit (delegated — see module docs), CDC, and idempotency-replay dedup, all in
/// ONE call, driven entirely by `plan` (sourced from `eg_capabilities::policy`).
///
/// `apply` performs the actual `eg-core` mutation and returns the success payload;
/// it is only invoked after authz passes and (for an idempotent replay hit) is
/// skipped entirely. On a failed apply, nothing durable/audited/CDC-emitted/cached
/// happens — exactly like the legacy per-handler + dispatch-shell path.
pub async fn commit_mutation<F>(
    ctx: &MutationCtx<'_>,
    plan: &MutationPlan,
    method: &Method,
    apply: F,
) -> Response
where
    F: FnOnce(&GraphCore) -> Result<ResultPayload, String>,
{
    // 1. Authz -- the SAME isolation ACL check every other graph-scoped method
    // goes through (`access::check_graph_access`), driven by the plan's `mutates`
    // flag rather than a second write/read classifier.
    let access = if plan.mutates {
        AccessLevel::Write
    } else {
        AccessLevel::Read
    };
    if let Err(denied) = check_graph_access(
        ctx.isolation,
        ctx.caller,
        ctx.graph_name,
        ctx.graph_type,
        ctx.owner,
        access,
    ) {
        return Response::err(ctx.req_id, denied);
    }

    // 2. Idempotency-replay dedup -- ONLY for methods policy marks idempotent. A
    // byte-identical replay (e.g. a client retry after a lost ack) short-circuits
    // BEFORE touching storage: no re-apply, no second WAL record, no second
    // audit-chain entry, no duplicate CDC event.
    let dedup_key = plan
        .idempotent
        .then(|| idempotency_key(ctx.graph_name, method));
    if let Some(key) = &dedup_key {
        if let Some(cached) = idempotency_store().get(key) {
            return cached;
        }
    }

    // 3. CDC pre-image, captured BEFORE the mutation applies -- gated on the
    // POLICY's `emits_cdc`, not the legacy shell's implicit `is_durable_mutation`
    // check.
    #[cfg(feature = "streaming")]
    let cdc_pre = if plan.emits_cdc {
        crate::server::cdc::capture_before(ctx.core, method)
    } else {
        crate::server::cdc::CdcPre::Skip
    };

    // 4. Apply the actual eg-core mutation.
    //
    // L18: a coalescable routed mutation (`AddNode`/`RemoveNode`/`AddEdge`/
    // `RemoveEdge`) is BATCHED onto this graph's single-writer coalescer queue when
    // one is configured + enabled — the SAME `writer_for` path the legacy
    // `dispatch::try_coalesce_write` uses — so routing the hottest writes through
    // the gateway no longer bypasses write-batching (the coalescer's `stats().ops()`
    // counts them again, and N concurrent writers collapse to ⌈N/batch⌉ topology-
    // lock acquisitions). It returns the SAME `Response` the `apply` closure would
    // (every coalescable routed op's closure yields `String("ok")` / an AddEdge
    // error). Anything else — a non-coalescable routed op (`CreateSummaryNode`/
    // `Consolidate`/`Reinforce`), a disabled/absent coalescer — falls back to the
    // inline `apply` closure, unchanged. Steps 5-8 (dirty/durable/audit/CDC) run
    // per-op AFTER this returns, exactly as before, so ordering is intact.
    let response = match try_coalesce_apply(ctx, method).await {
        Some(resp) => resp,
        None => match apply(ctx.core) {
            Ok(payload) => Response::ok(ctx.req_id, payload),
            Err(e) => Response::err(ctx.req_id, e),
        },
    };
    if response.error.is_some() {
        return response;
    }

    // 5. Mark the graph dirty (checkpoint scheduling). Safe to call unconditionally
    // on a successful mutating op even when the eg-core primitive already marks
    // itself dirty internally (`mark_dirty` is an idempotent flag set).
    if plan.mutates {
        ctx.core.mark_dirty();
    }

    // 6. Durable commit -- reuses the EXISTING `PersistenceBackend` plumbing (WAL /
    // redb write-through). For a redb backend this is ALSO where the tamper-
    // evident audit-chain entry is appended atomically with the data row (see the
    // module docs for why that is delegated rather than reimplemented here).
    if !matches!(plan.durability_domain, DurabilityDomain::None) {
        if let Some(p) = ctx.persistence {
            let fname = crate::persist::sanitize(ctx.graph_name);
            if ctx.redb_authoritative {
                if let Err(e) = p.record_durable(&fname, method).await {
                    return Response::err(
                        ctx.req_id,
                        format!("durable commit failed (write not acknowledged): {e}"),
                    );
                }
            } else {
                p.record(&fname, method);
            }
        }
    }

    // 7. CDC emit -- AFTER the durable commit succeeded (or was skipped when no
    // backend is configured), mirroring the legacy shell's ordering.
    #[cfg(feature = "streaming")]
    if plan.emits_cdc {
        if let Some(hub) = ctx.cdc {
            crate::server::cdc::emit_for_method(hub, ctx.core, ctx.graph_name, method, cdc_pre);
        }
    }

    // 8. Cache the response for idempotent-replay dedup.
    if let Some(key) = dedup_key {
        idempotency_store().insert(key, response.clone());
    }

    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::GraphType;
    #[cfg(feature = "redb")]
    use crate::server::persistence::redb_backend::RedbBackend;
    #[cfg(feature = "redb")]
    use crate::wal_service::FsyncPolicy;

    fn isolation_no_rules() -> IsolationLayer {
        IsolationLayer::new()
    }

    fn temp_dir(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "eg-mutation-gateway-test-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    /// (a) A GATEWAY_ROUTED, audited+CDC-emitting mutation (`AddNode`) produces a
    /// WAL/redb record + an audit-chain entry + a CDC event -- all from the ONE
    /// `commit_mutation` call, against a REAL `RedbBackend` (not a mock), so this
    /// is exercising the actual durable-commit + audit-chain-append path, not just
    /// asserting the gateway's own bookkeeping.
    #[cfg(feature = "redb")]
    #[tokio::test(flavor = "multi_thread")]
    async fn routed_mutation_produces_one_wal_record_one_audit_entry_and_one_cdc_event() {
        let dir = temp_dir("wal-audit-cdc");
        let dir_s = dir.to_string_lossy().to_string();
        let backend = RedbBackend::open(dir_s, FsyncPolicy::Each, 64).expect("open redb backend");
        let persistence: Arc<dyn PersistenceBackend> = Arc::new(backend);

        let core = Arc::new(GraphCore::new());
        let isolation = isolation_no_rules();
        let cdc_hub = Arc::new(crate::server::cdc::CdcHub::new());
        let graph_name = "g-eg-p0-2-a";

        let method = Method::AddNode {
            node_id: "n1".to_string(),
            properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({"v": 1}))
                .unwrap(),
        };
        let plan = MutationPlan::for_method(&method);
        assert!(plan.mutates, "AddNode must be classified as a mutation");
        assert!(plan.audited, "AddNode is policy-audited");
        assert!(plan.emits_cdc, "AddNode policy-emits CDC");
        assert_eq!(plan.durability_domain, DurabilityDomain::GraphRedb);

        let ctx = MutationCtx {
            req_id: 1,
            caller: Some("system-agent"),
            graph_name,
            graph_type: GraphType::Commons,
            owner: None,
            isolation: &isolation,
            core: &core,
            persistence: Some(&persistence),
            // Authoritative (commit-before-ack) so `commit_mutation` AWAITS the
            // durable group-commit before returning -- the write (and its audit-
            // chain entry) is provably on disk when we read it back below, with no
            // race against the off-reactor writer that the fire-and-forget
            // `record()` path would introduce.
            redb_authoritative: true,
            cdc: Some(&cdc_hub),
            write_coalescer: None,
        };

        let (node_id, props) = match &method {
            Method::AddNode {
                node_id,
                properties_msgpack,
            } => (node_id.clone(), properties_msgpack.clone()),
            _ => unreachable!(),
        };
        let resp = commit_mutation(&ctx, &plan, &method, move |core| {
            core.add_node(node_id, props);
            Ok(ResultPayload::String("ok".to_string()))
        })
        .await;
        assert!(resp.error.is_none(), "commit_mutation failed: {:?}", resp.error);

        // WAL/redb: the write actually landed durably.
        let fname = crate::persist::sanitize(graph_name);
        let read_back = persistence
            .as_redb()
            .expect("configured backend is redb")
            .read_node(&fname, "n1")
            .await
            .expect("read back the durably-committed node");
        assert!(read_back.is_some(), "AddNode did not durably commit via the gateway");

        // Audit: the tamper-evident chain grew by exactly one entry for this graph.
        let report = persistence
            .as_redb()
            .expect("configured backend is redb")
            .audit_verify_blocking(&fname)
            .expect("audit_verify_blocking");
        assert!(report.ok, "audit chain broke: {report:?}");
        assert_eq!(
            report.entries, 1,
            "exactly one audited mutation (AddNode) went through the gateway"
        );

        // CDC: one AddNode event was emitted into the hub's feed for this graph.
        let events = cdc_hub.read(graph_name, 0, 100).expect("cdc read");
        assert_eq!(events.len(), 1, "expected exactly one CDC event, got {events:?}");
    }

    /// (a2) A GATEWAY_ROUTED durable + audited BUT NON-CDC mutation (`Reinforce`)
    /// commits durably, appends exactly ONE audit-chain entry, and leaves the CDC
    /// feed UNTOUCHED. As of L3/EG-P0-6, `audit_line` is exhaustive over the full
    /// durable-mutation surface, so `Reinforce` is now policy-`audited: true` (its
    /// agent-memory write chains into the tamper-evident log); it stays
    /// `emits_cdc: false`. This test now proves CDC -- NOT audit -- is the
    /// policy-gated leg of the gateway: audit fires, CDC does not.
    #[cfg(feature = "redb")]
    #[tokio::test(flavor = "multi_thread")]
    async fn audited_mutation_writes_audit_but_cdc_stays_policy_gated() {
        let dir = temp_dir("reinforce-audit-no-cdc");
        let dir_s = dir.to_string_lossy().to_string();
        let backend = RedbBackend::open(dir_s, FsyncPolicy::Each, 64).expect("open redb backend");
        let persistence: Arc<dyn PersistenceBackend> = Arc::new(backend);

        let core = Arc::new(GraphCore::new());
        core.add_node("n1".to_string(), Vec::new());
        let isolation = isolation_no_rules();
        let cdc_hub = Arc::new(crate::server::cdc::CdcHub::new());
        let graph_name = "g-eg-p0-2-a2";

        let method = Method::Reinforce {
            node_id: "n1".to_string(),
            now_ms: 1_000,
            weight: 1.0,
        };
        let plan = MutationPlan::for_method(&method);
        assert!(plan.mutates);
        assert!(plan.audited, "Reinforce is now policy-audited (L3/EG-P0-6)");
        assert!(!plan.emits_cdc, "Reinforce policy-does-NOT-emit-CDC");
        assert_eq!(plan.durability_domain, DurabilityDomain::GraphRedb);

        let ctx = MutationCtx {
            req_id: 2,
            caller: Some("system-agent"),
            graph_name,
            graph_type: GraphType::Commons,
            owner: None,
            isolation: &isolation,
            core: &core,
            persistence: Some(&persistence),
            // Authoritative so `commit_mutation` AWAITS the durable group-commit
            // (which appends the audit-chain entry) before returning, so the
            // read-back below never races the off-reactor writer.
            redb_authoritative: true,
            cdc: Some(&cdc_hub),
            write_coalescer: None,
        };
        let resp = commit_mutation(&ctx, &plan, &method, |core| {
            core.reinforce("n1", 1_000, 1.0);
            Ok(ResultPayload::Bool(true))
        })
        .await;
        assert!(resp.error.is_none());

        let fname = crate::persist::sanitize(graph_name);
        let report = persistence
            .as_redb()
            .unwrap()
            .audit_verify_blocking(&fname)
            .expect("audit_verify_blocking");
        assert!(report.ok, "audit chain broke: {report:?}");
        assert_eq!(
            report.entries, 1,
            "Reinforce is now audited -- exactly one audit-chain entry"
        );
        assert_eq!(
            cdc_hub.read(graph_name, 0, 100).expect("cdc read").len(),
            0,
            "Reinforce must NOT emit a CDC event (CDC stays the policy-gated leg)"
        );
    }

    /// (b) An unauthorized actor is rejected AT THE GATEWAY, before `apply` ever
    /// runs (proven by asserting the graph is left untouched).
    #[tokio::test(flavor = "multi_thread")]
    async fn unauthorized_actor_is_rejected_at_the_gateway() {
        let core = Arc::new(GraphCore::new());
        let mut isolation = IsolationLayer::new();
        // Registering ANY identity flips `has_rules()` on, switching graph-
        // targeted dispatch into enforcing mode.
        isolation.register_agent(crate::acl::AgentIdentity {
            agent_id: "owner".to_string(),
            role: crate::acl::AgentRole::Agent,
            teams: Vec::new(),
            roles: Vec::new(),
        });
        isolation.register_agent(crate::acl::AgentIdentity {
            agent_id: "intruder".to_string(),
            role: crate::acl::AgentRole::Agent,
            teams: Vec::new(),
            roles: Vec::new(),
        });

        let method = Method::AddNode {
            node_id: "n1".to_string(),
            properties_msgpack: Vec::new(),
        };
        let plan = MutationPlan::for_method(&method);

        let ctx = MutationCtx {
            req_id: 3,
            caller: Some("intruder"),
            graph_name: "agent:owner",
            graph_type: GraphType::Agent,
            owner: Some("owner"),
            isolation: &isolation,
            core: &core,
            persistence: None,
            redb_authoritative: false,
            cdc: None,
            write_coalescer: None,
        };

        let mut apply_ran = false;
        let resp = commit_mutation(&ctx, &plan, &method, |core| {
            apply_ran = true;
            core.add_node("n1".to_string(), Vec::new());
            Ok(ResultPayload::String("ok".to_string()))
        })
        .await;

        assert!(resp.error.is_some(), "expected ACCESS_DENIED, got {:?}", resp);
        assert!(resp.error.unwrap().contains("ACCESS_DENIED"));
        assert!(!apply_ran, "apply must never run for a denied caller");
        assert!(!core.has_node("n1"), "the graph must be untouched");
    }

    /// (c) An idempotent method's (`RemoveNode`) replay with the SAME key is
    /// deduped: the second call returns the cached response WITHOUT re-invoking
    /// `apply` (a re-`apply` on an already-removed node would still succeed, so
    /// only an explicit apply-count check proves dedup, not just a repeated OK).
    #[tokio::test(flavor = "multi_thread")]
    async fn idempotent_replay_is_deduped_without_reapplying() {
        let core = Arc::new(GraphCore::new());
        core.add_node("n1".to_string(), Vec::new());
        let isolation = isolation_no_rules();
        let graph_name = "g-eg-p0-2-idem-unique-9f31";

        let method = Method::RemoveNode {
            node_id: "n1".to_string(),
        };
        let plan = MutationPlan::for_method(&method);
        assert!(plan.idempotent, "RemoveNode is policy-idempotent");

        let ctx = MutationCtx {
            req_id: 4,
            caller: None,
            graph_name,
            graph_type: GraphType::Commons,
            owner: None,
            isolation: &isolation,
            core: &core,
            persistence: None,
            redb_authoritative: false,
            cdc: None,
            write_coalescer: None,
        };

        let apply_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let count1 = apply_count.clone();
        let resp1 = commit_mutation(&ctx, &plan, &method, |core| {
            count1.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            core.remove_node("n1".to_string());
            Ok(ResultPayload::String("ok".to_string()))
        })
        .await;
        assert!(resp1.error.is_none());
        assert_eq!(apply_count.load(std::sync::atomic::Ordering::SeqCst), 1);

        // Replay: identical method + graph -- must hit the cache, NOT re-apply.
        let count2 = apply_count.clone();
        let resp2 = commit_mutation(&ctx, &plan, &method, |core| {
            count2.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            core.remove_node("n1".to_string());
            Ok(ResultPayload::String("ok".to_string()))
        })
        .await;
        assert!(resp2.error.is_none());
        assert_eq!(
            apply_count.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "replay must be deduped, not re-applied"
        );
        assert_eq!(
            format!("{:?}", resp1.result),
            format!("{:?}", resp2.result),
            "replay returns the cached response"
        );
    }

    /// (d) Bypass guard, part 1: `MutationPlan` never diverges from
    /// `eg_capabilities::policy` for any [`GATEWAY_ROUTED`] method -- if someone
    /// edits `MutationPlan::for_method` to hardcode a field instead of reading it
    /// off `policy()`, this catches the divergence.
    #[test]
    fn routed_methods_plan_is_never_hardcoded_relative_to_policy() {
        let samples: Vec<Method> = vec![
            Method::AddNode {
                node_id: "x".into(),
                properties_msgpack: Vec::new(),
            },
            Method::RemoveNode {
                node_id: "x".into(),
            },
            Method::AddEdge {
                source_id: "a".into(),
                target_id: "b".into(),
                properties_msgpack: Vec::new(),
            },
            Method::RemoveEdge {
                source_id: "a".into(),
                target_id: "b".into(),
            },
            Method::CreateSummaryNode {
                level: 1,
                child_ids: vec!["a".into()],
                props_msgpack: Vec::new(),
            },
            Method::Consolidate {
                episodic_ids: vec!["a".into()],
                semantic_props_msgpack: Vec::new(),
            },
            Method::Reinforce {
                node_id: "x".into(),
                now_ms: 0,
                weight: 0.0,
            },
        ];
        assert_eq!(samples.len(), GATEWAY_ROUTED.len());
        for m in &samples {
            assert!(is_gateway_routed(m), "{m:?} must be in GATEWAY_ROUTED");
            let plan = MutationPlan::for_method(m);
            let p = eg_capabilities::policy(m);
            assert_eq!(plan.mutates, p.mutates, "{}", plan.method_name);
            assert_eq!(
                plan.durability_domain, p.durability_domain,
                "{}",
                plan.method_name
            );
            assert_eq!(plan.authz_action, p.authz_action, "{}", plan.method_name);
            assert_eq!(plan.idempotent, p.idempotent, "{}", plan.method_name);
            assert_eq!(plan.audited, p.audited, "{}", plan.method_name);
            assert_eq!(plan.emits_cdc, p.emits_cdc, "{}", plan.method_name);
            assert_eq!(
                plan.txn_participation, p.txn_participation,
                "{}",
                plan.method_name
            );
        }
    }

    /// (d) Bypass guard, part 2: the migration surface is machine-visible. Every
    /// name in [`GATEWAY_ROUTED`] really exists in `eg_capabilities::ALL_METHODS`
    /// and really is `mutates == true` (catches a rename/typo silently un-routing
    /// a method); the COMPLEMENT (every other mutating method) is the EG-P0-2
    /// rollout backlog, asserted non-empty and printed so it stays visible in test
    /// output rather than silently claimed as "done".
    #[test]
    fn gateway_routed_set_matches_mutating_policy_surface() {
        use std::collections::BTreeSet;

        let all_mutating: BTreeSet<&'static str> = eg_capabilities::ALL_METHODS
            .iter()
            .filter(|(_, p, _)| p.mutates)
            .map(|(name, _, _)| *name)
            .collect();

        for routed in GATEWAY_ROUTED {
            assert!(
                all_mutating.contains(routed),
                "GATEWAY_ROUTED name '{routed}' is not a mutating method in \
                 eg_capabilities::ALL_METHODS (renamed/typo'd?)"
            );
        }

        let routed_set: BTreeSet<&'static str> = GATEWAY_ROUTED.iter().copied().collect();
        let not_yet_migrated: Vec<&'static str> = all_mutating
            .difference(&routed_set)
            .copied()
            .collect();

        // This backlog is EXPECTED to be large (EG-P0-2 is a deliberate, narrow
        // increment — see the module docs) — the assertion is that it exists and
        // is visible, not that it's empty.
        assert!(
            !not_yet_migrated.is_empty(),
            "expected a non-empty EG-P0-2 rollout backlog"
        );
        assert_eq!(
            all_mutating.len(),
            routed_set.len() + not_yet_migrated.len()
        );
        println!(
            "EG-P0-2 gateway migration surface: {} routed / {} mutating total; \
             {} NOT yet migrated (backlog): {:?}",
            routed_set.len(),
            all_mutating.len(),
            not_yet_migrated.len(),
            not_yet_migrated
        );
    }
}
