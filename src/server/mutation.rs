//! `MutationPlan` + the single commit gateway (CONCEPT:EG-P0-2).
//!
//! ## Why this exists
//!
//! `eg-capabilities` (CONCEPT:EG-P0-1) is a machine-checked, exhaustive `MethodPolicy`
//! table over every `Method` variant — but on its own it is a one-way AUDIT: it
//! cross-checks the existing hand-rolled classifiers (`access.rs::requires_write`,
//! `mutation_apply::is_durable_mutation`, `audit.rs::audit_line`, `cdc.rs::emit_for_method`)
//! against its declared policy, without either side CONSUMING the other. Handlers
//! still mutate `eg-core` directly (`src/server/handlers/graph_ops.rs`'s
//! `g.add_node(...)` etc.), and the dispatch shell (`src/server/dispatch.rs`)
//! separately re-derives whether to durably commit / CDC-emit a given method from its
//! own classifiers.
//!
//! This module is the other direction: [`MutationPlan`] is POPULATED FROM
//! `eg_capabilities::policy` (never re-hardcoded), and [`commit_mutation`] is the
//! ONE place a routed mutation's authz + durable write + CDC emission happen
//! together, driven by that plan. The invariant this buys: for a method in
//! [`plan::GATEWAY_ROUTED`], mutation + durability + audit + CDC happen together, in one
//! call, declared by policy — never scattered back across the dispatch shell.
//!
//! ## Scope (read before assuming more is covered)
//!
//! [`plan::GATEWAY_ROUTED`] methods are wired through this gateway. EG-P0-2 started with
//! 7 (`AddNode`/`RemoveNode`/`AddEdge`/`RemoveEdge` + `CreateSummaryNode`/
//! `Consolidate`/`Reinforce`); the L11 rollout expanded it across the full routed
//! graph surface, adding:
//!   * the rest of the plain graph-core family (CAS/claim/edge-temporal/scene/
//!     trajectory/embedding/ledger/lifecycle ops — all handled in `graph_ops.rs`
//!     already, same shape as the original 7);
//!   * the message-broker/stream family (`Outbox` durability domain, behind
//!     `feature = "broker"` — proves the gateway is domain-agnostic: `commit_mutation`
//!     doesn't branch on `DurabilityDomain`, only on whether it's `None`);
//!   * `IcvConfigure` (validated graph control state carried in the staged
//!     `GraphCore`, so policy and data share the graph-scoped durable commit);
//!   * BOTH runtime-conditional families whose REAL `mutates` answer is a
//!     per-invocation `writeback: bool` field, not `policy()`'s conservative
//!     upper bound: `GraphLearnFit`/`GraphLearnPredict` and every writeback-capable
//!     `Mine*` — routed via [`commit_conditional_mutation`], NOT `commit_mutation`,
//!     so a `writeback: false` call is correctly treated as a plain read (Read
//!     ACL, no durability/audit/CDC) instead of being incorrectly gated behind
//!     Write access or persisted as a phantom mutation.
//!
//! Coverage is machine-partitioned by
//! `tests::gateway_routed_set_matches_mutating_policy_surface`: graph-scoped
//! mutations use this gateway; WorkItem, OCC/2PC, lifecycle, and non-graph stores
//! identify their native atomic coordinator. `OPEN_NOT_JUSTIFIED` is asserted
//! empty, so a new mutating method cannot silently bypass both categories.
//!
//! `src/server/dispatch.rs` wires `handlers::graph_ops::try_handle_gateway` ahead
//! of the generic handler chain. A routed method therefore reaches exactly one
//! mutation kernel. The five coalescable structural writes route through
//! [`commit_coalescable_mutation`] (L18, see its doc) instead of [`commit_mutation`],
//! so routed structural writes retain batching without a second durability path.
//!
//! ## What this gateway does NOT reimplement
//!
//! Audit-chain emission (the tamper-evident hash chain, `audit.rs` +
//! `redb_store::append_audit_entry`) is stateful PER GRAPH (each entry chains off
//! the previous one's hash) and already lives inside the durable-commit path
//! ([`crate::server::persistence::PersistenceBackend::record_durable`] → the redb backend). Reimplementing
//! that chaining here would risk a second, diverging chain. Instead,
//! Compact row commits and staged-state MutationBatch commits both append the audit
//! entry inside their authoritative redb transaction. `plan.audited`
//! (sourced from `eg_capabilities::policy`) is the assertable, tested FACT that this
//! delegation actually happens for the methods policy says should be audited (see
//! `tests::routed_mutation_produces_one_durable_record_one_audit_entry_and_one_cdc_event`,
//! which uses a REAL `RedbBackend` and reads the audit chain back).
//!
//! The per-graph write-coalescer's batching is genuinely live for the routed set
//! (L18/EG-P0-6, rewritten): the five coalescable structural writes
//! (`AddNode`/`RemoveNode`/`AddEdge`/`RemoveEdge`/`CompareAndSetNodeFields`) are routed through
//! [`commit_coalescable_mutation`] instead of [`commit_mutation`] (see
//! `handlers::graph_ops::try_handle_gateway`'s dedicated arms). That function
//! hands the WHOLE prepare→durable-commit→RAM-publish sequence — not just the RAM
//! apply — to the per-graph `server::routed_write_coalescer` worker, which runs a
//! flushed batch's sequences back-to-back inside ONE `mutation_batch::lock_graph`
//! acquisition instead of one per op (`stats().batches() < stats().ops()` under
//! concurrent load). [`commit_mutation_body`] is the single, shared implementation
//! of that sequence — called once per op by both the ordinary single-call path
//! ([`commit_entry::commit_mutation_inner`]) and the worker, so there is exactly one copy of the
//! durability/audit/CDC kernel regardless of which lock-hold granularity wraps it.
//! The non-coalescable routed memory ops (`CreateSummaryNode`/`Consolidate`/
//! `Reinforce`) and every other routed method keep going through
//! [`commit_mutation`]/[`commit_entry::commit_mutation_inner`] unchanged, one lock acquisition per
//! op. With Raft active, dispatch reaches the consensus barrier before this local
//! gateway; each committed ordinary Raft method is then staged and committed through
//! the same state-backed MutationBatch authority on every replica.
//!
//! See [`commit_coalescable_mutation`]'s doc for exactly why batching the RAM
//! publish ALONE (an earlier version of this fix) is unsafe, and why the durable
//! commit must move into the SAME lock-held sequence rather than staying outside it.

#[cfg(test)]
use std::sync::Arc;

#[cfg(all(test, feature = "redb"))]
use eg_capabilities::DurabilityDomain;

#[cfg(test)]
use crate::graph::GraphCore;
#[cfg(test)]
use crate::isolation::IsolationLayer;
#[cfg(test)]
use crate::protocol::{Method, ResultPayload};
#[cfg(all(test, feature = "redb"))]
use crate::server::persistence::PersistenceBackend;

mod coalescer;
mod commit_encode;
mod commit_entry;
mod commit_paths;
mod commit_replay;
mod conditional;
mod context;
mod plan;

use plan::consensus_apply_is_authorized;
#[cfg(test)]
pub use plan::GATEWAY_ROUTED;
#[cfg(feature = "raft")]
pub use plan::LOCAL_ONLY_CLUSTER_REFUSAL;
#[cfg(any(feature = "raft", test))]
pub use plan::{cluster_mutation_route, ClusterMutationRoute};
pub use plan::{is_gateway_routed, method_variant_name, GatewayAuthzCtx, MutationPlan};
#[cfg(all(test, feature = "raft"))]
use plan::{CONSENSUS_FANOUT_METHODS, SELF_ROUTED_ADMIN_METHODS};

pub use context::MutationCtx;
use context::{advance_authoritative_manifest, idempotency_key, idempotency_store};
pub(crate) use context::{durable_receipt_method, LifecycleAttempt};

pub use coalescer::commit_coalescable_mutation;
pub(crate) use coalescer::{apply_coalescable_write, is_coalescable_structural_write};

pub use commit_entry::commit_mutation;
use commit_entry::{
    commit_finalize, commit_mutation_body, commit_prepare, CommitFinalizeOptions, CommitPrep,
};

pub(super) use commit_encode::publish_committed_row_delta;
use commit_encode::{
    commit_mutation_body_commit_staged, diff_and_serialize_staged_mutation, prepublish_success,
    preserves_node_derived_indexes, staged_mutation_descriptor, StagedMutation,
};
use commit_paths::{
    commit_mutation_body_prepublish_fast_path, commit_mutation_body_staged_path,
    resolve_authoritative_base_snapshot,
};
use commit_replay::{
    commit_mutation_body_replay_response, commit_row_replay_probe, commit_staged_replay_probe,
    compile_batch_and_encode_result, DurableBatchAttempt, DurableBatchTarget,
};

// Each conditional-commit entry point is re-exported exactly where its callers
// are compiled: the Mine*/GraphLearn*/ML gateways (whose tests are gated on
// `mining`), and the query (SQL/Cypher/GraphQL, incl. Bolt) and RDF gateways.
#[cfg(any(
    feature = "mining",
    feature = "graphlearn",
    feature = "ml-pipeline",
    feature = "modality-serving"
))]
pub use conditional::commit_conditional_mutation;
#[cfg(any(
    feature = "query",
    feature = "cypher",
    feature = "graphql",
    feature = "rdf"
))]
pub use conditional::commit_conditional_mutation_async;
#[cfg(any(feature = "query", feature = "cypher", feature = "graphql"))]
pub use conditional::is_query_native_coordinator;
pub use conditional::{is_query_gateway_method, is_rdf_gateway_method};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::GraphType;
    #[cfg(feature = "redb")]
    use crate::server::persistence::redb_backend::RedbBackend;

    /// W1c: `check_graph_access_with_policy` (`src/server/access.rs`, the L-RLS-1
    /// hardening) now mandates a provisioned identity for EVERY graph-scoped
    /// access -- an empty, rule-less `IsolationLayer` (no registered agents) is
    /// unconditionally denied, not just under-ruled. Every test below needs a
    /// caller that actually clears that gate, so they provision "system-agent"
    /// with `AgentRole::System` (the one role `IsolationLayer::check_access`
    /// treats as unconditionally allowed, same mechanism
    /// `unauthorized_actor_is_rejected_at_the_gateway` already relies on for its
    /// registered identities) instead of an insufficient no-rules layer.
    fn isolation_with_system_agent() -> IsolationLayer {
        let mut isolation = IsolationLayer::new();
        isolation.register_agent(crate::acl::AgentIdentity {
            agent_id: "system-agent".to_string(),
            role: crate::acl::AgentRole::System,
            teams: Vec::new(),
            roles: Vec::new(),
        });
        isolation
    }

    fn temp_dir(tag: &str) -> std::path::PathBuf {
        crate::test_support::temp_dir("eg-mutation-gateway-test", tag)
    }

    #[test]
    fn validated_public_batch_has_a_deterministic_prepublish_result() {
        let core = GraphCore::new();
        let operations = rmp_serde::to_vec_named(&serde_json::json!([
            {"op": "add_node", "id": "a", "properties": {"text": "alpha"}},
            {"op": "add_embedding", "id": "a", "embedding": [1.0, 0.0]}
        ]))
        .unwrap();
        let method = Method::BatchUpdate {
            operations_msgpack: operations,
        };
        let Some(ResultPayload::Json(result)) = prepublish_success(&core, &method) else {
            panic!("validated BatchUpdate must use compact authoritative rows");
        };
        assert_eq!(result["added_nodes"], 1);
        assert_eq!(result["added_embeddings"], 1);

        let malformed = Method::BatchUpdate {
            operations_msgpack: vec![0xc1],
        };
        assert!(prepublish_success(&core, &malformed).is_none());
    }

    /// (a) A GATEWAY_ROUTED, audited+CDC-emitting mutation (`AddNode`) produces a
    /// durable record + an audit-chain entry + a CDC event -- all from the one
    /// `commit_mutation` call, against a REAL `RedbBackend` (not a mock), so this
    /// is exercising the actual durable-commit + audit-chain-append path, not just
    /// asserting the gateway's own bookkeeping.
    #[cfg(all(feature = "redb", feature = "security", feature = "streaming"))]
    #[tokio::test(flavor = "multi_thread")]
    async fn routed_mutation_produces_one_durable_record_one_audit_entry_and_one_cdc_event() {
        // Reads the ambient encryption env at its durable open, so the env must hold
        // still for this whole body. READ guard: it excludes only a key MUTATOR, never
        // another opener. See `crate::crypto::acquire_test_env_read_lock`'s doc.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        let dir = temp_dir("durable-audit-cdc");
        let dir_s = dir.to_string_lossy().to_string();
        let backend = RedbBackend::open(dir_s, 64).expect("open redb backend");
        let persistence: Arc<dyn PersistenceBackend> = Arc::new(backend);

        let core = Arc::new(GraphCore::new());
        let isolation = isolation_with_system_agent();
        let cdc_hub = Arc::new(crate::server::cdc::CdcHub::new());
        let graph_name = "g-eg-p0-2-a";

        let method = Method::AddNode {
            node_id: "n1".to_string(),
            properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({"v": 1})).unwrap(),
        };
        let plan = MutationPlan::for_method(&method);
        assert!(plan.mutates, "AddNode must be classified as a mutation");
        assert!(plan.audited, "AddNode is policy-audited");
        assert!(plan.emits_cdc, "AddNode policy-emits CDC");
        assert_eq!(plan.durability_domain, DurabilityDomain::GraphRedb);

        let ctx = MutationCtx {
            req_id: 1,
            caller: Some("system-agent"),
            attempt_nonce: None,
            idempotency_key: "test-idempotency",
            tenant_scope: "opaque-test-tenant",
            graph_name,
            graph_type: GraphType::Commons,
            owner: None,
            isolation: &isolation,
            core: &core,
            persistence: Some(&persistence),
            cdc: Some(&cdc_hub),
            materialization_manifest: None,
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
        assert!(
            resp.error.is_none(),
            "commit_mutation failed: {:?}",
            resp.error
        );

        // The write actually landed durably.
        let fname = crate::persist::sanitize(graph_name);
        let read_back = persistence
            .as_redb()
            .expect("configured backend is redb")
            .read_node(&fname, "n1")
            .await
            .expect("read back the durably-committed node");
        assert!(
            read_back.is_some(),
            "AddNode did not durably commit via the gateway"
        );

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
        let events = cdc_hub.read(graph_name, 0, 100).events;
        assert_eq!(
            events.len(),
            1,
            "expected exactly one CDC event, got {events:?}"
        );
    }

    /// Regression: `commit_finalize` must refresh the per-graph `epistemic_graph_
    /// graph_nodes`/`epistemic_graph_graph_edges` gauges. Before the fix, that
    /// refresh existed only in the legacy non-gateway dispatch tail and Raft
    /// snapshot-install -- NOT in `commit_finalize`, the universal gateway
    /// essentially all current writes (including `AddNode`, GATEWAY_ROUTED) route
    /// through -- so a graph mutated exclusively via gateway-routed writes never
    /// refreshed its gauge and silently froze at its last snapshot-install value
    /// (confirmed live: `/metrics` reported 56,882 nodes while a live `NodeCount`
    /// RPC, a Cypher `count(n)`, and full label enumeration all independently
    /// agreed on 25,122). This drives two REAL `AddNode` calls through
    /// `commit_mutation` (never touching `set_graph_size` directly) and asserts
    /// the rendered gauge tracks the actual count after each one.
    #[cfg(all(feature = "redb", feature = "security", feature = "streaming"))]
    #[tokio::test(flavor = "multi_thread")]
    async fn commit_finalize_refreshes_the_graph_size_gauges() {
        // Reads the ambient encryption env at its durable open, so the env must hold
        // still for this whole body. READ guard: it excludes only a key MUTATOR, never
        // another opener. See `crate::crypto::acquire_test_env_read_lock`'s doc.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        let dir = temp_dir("gauge-refresh");
        let dir_s = dir.to_string_lossy().to_string();
        let backend = RedbBackend::open(dir_s, 64).expect("open redb backend");
        let persistence: Arc<dyn PersistenceBackend> = Arc::new(backend);

        let core = Arc::new(GraphCore::new());
        let isolation = isolation_with_system_agent();
        let graph_name = "g-eg-p0-2-gauge-refresh";

        fn gauge_value(metric: &str, graph_name: &str) -> Option<i64> {
            let rendered = crate::metrics::render();
            let needle = format!("{metric}{{graph=\"{graph_name}\"}} ");
            rendered.lines().find_map(|line| {
                line.strip_prefix(&needle)
                    .and_then(|rest| rest.trim().parse::<i64>().ok())
            })
        }

        async fn add_node(
            core: &Arc<GraphCore>,
            isolation: &IsolationLayer,
            persistence: &Arc<dyn PersistenceBackend>,
            graph_name: &'static str,
            node_id: &str,
        ) {
            let method = Method::AddNode {
                node_id: node_id.to_string(),
                properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({"v": 1})).unwrap(),
            };
            let plan = MutationPlan::for_method(&method);
            let ctx = MutationCtx {
                req_id: 1,
                caller: Some("system-agent"),
                attempt_nonce: None,
                // Each AddNode is a distinct caller operation. Reusing a stable
                // key for n1 and n2 correctly produces a kernel conflict.
                idempotency_key: node_id,
                tenant_scope: "opaque-test-tenant",
                graph_name,
                graph_type: GraphType::Commons,
                owner: None,
                isolation,
                core,
                persistence: Some(persistence),
                cdc: None,
                materialization_manifest: None,
                write_coalescer: None,
            };
            let node_id_owned = node_id.to_string();
            let props = match &method {
                Method::AddNode {
                    properties_msgpack, ..
                } => properties_msgpack.clone(),
                _ => unreachable!(),
            };
            let resp = commit_mutation(&ctx, &plan, &method, move |core| {
                core.add_node(node_id_owned, props);
                Ok(ResultPayload::String("ok".to_string()))
            })
            .await;
            assert!(resp.error.is_none(), "AddNode failed: {:?}", resp.error);
        }

        // Before any write, either unset (no series yet) or zero -- never stale.
        add_node(&core, &isolation, &persistence, graph_name, "n1").await;
        assert_eq!(
            gauge_value("epistemic_graph_graph_nodes", graph_name),
            Some(1),
            "gauge must reflect the ACTUAL count (1) immediately after the first \
             gateway-routed AddNode, not a frozen/absent value"
        );

        add_node(&core, &isolation, &persistence, graph_name, "n2").await;
        assert_eq!(
            gauge_value("epistemic_graph_graph_nodes", graph_name),
            Some(2),
            "a SECOND gateway-routed AddNode must advance the gauge again -- this \
             is exactly what stayed frozen before the fix (measured live: the \
             gauge was unchanged across two scrapes ten minutes apart despite \
             graph_ops_total advancing by ~30k)"
        );
        assert_eq!(core.node_count(), 2, "sanity: the graph really has 2 nodes");
    }

    /// (a2) A GATEWAY_ROUTED durable + audited BUT NON-CDC mutation (`Reinforce`)
    /// commits durably, appends exactly ONE audit-chain entry, and leaves the CDC
    /// feed UNTOUCHED. As of L3/EG-P0-6, `audit_line` is exhaustive over the full
    /// durable-mutation surface, so `Reinforce` is now policy-`audited: true` (its
    /// agent-memory write chains into the tamper-evident log); it stays
    /// `emits_cdc: false`. This test now proves CDC -- NOT audit -- is the
    /// policy-gated leg of the gateway: audit fires, CDC does not.
    #[cfg(all(feature = "redb", feature = "security", feature = "streaming"))]
    #[tokio::test(flavor = "multi_thread")]
    async fn audited_mutation_writes_audit_but_cdc_stays_policy_gated() {
        // Reads the ambient encryption env at its durable open, so the env must hold
        // still for this whole body. READ guard: it excludes only a key MUTATOR, never
        // another opener. See `crate::crypto::acquire_test_env_read_lock`'s doc.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        let dir = temp_dir("reinforce-audit-no-cdc");
        let dir_s = dir.to_string_lossy().to_string();
        let backend = RedbBackend::open(dir_s, 64).expect("open redb backend");
        let persistence: Arc<dyn PersistenceBackend> = Arc::new(backend);

        let core = Arc::new(GraphCore::new());
        core.add_node("n1".to_string(), Vec::new());
        let isolation = isolation_with_system_agent();
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
            attempt_nonce: None,
            idempotency_key: "test-idempotency",
            tenant_scope: "opaque-test-tenant",
            graph_name,
            graph_type: GraphType::Commons,
            owner: None,
            isolation: &isolation,
            core: &core,
            persistence: Some(&persistence),
            cdc: Some(&cdc_hub),
            materialization_manifest: None,
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
            cdc_hub.read(graph_name, 0, 100).events.len(),
            0,
            "Reinforce must NOT emit a CDC event (CDC stays the policy-gated leg)"
        );
    }

    /// (a2b) W1c: drive one `method` through the REAL `commit_mutation` gateway
    /// against a fresh `RedbBackend`, asserting the policy is `audited: true` +
    /// `emits_cdc: true` and that exactly one audit-chain entry and one CDC marker
    /// event actually land. Shared by every W1c admin/ledger-method case below (each
    /// gets its own temp dir/backend/graph, so `report.entries`/the CDC feed start
    /// clean per call).
    #[cfg(all(feature = "redb", feature = "security", feature = "streaming"))]
    async fn assert_w1c_method_audits_and_emits_one_cdc_marker<F>(
        tag: &str,
        method: Method,
        apply: F,
    ) where
        F: FnOnce(&GraphCore) -> Result<ResultPayload, String>,
    {
        let dir = temp_dir(&format!("w1c-{tag}"));
        let dir_s = dir.to_string_lossy().to_string();
        let backend = RedbBackend::open(dir_s, 64).expect("open redb backend");
        let persistence: Arc<dyn PersistenceBackend> = Arc::new(backend);
        let core = Arc::new(GraphCore::new());
        let isolation = isolation_with_system_agent();
        let cdc_hub = Arc::new(crate::server::cdc::CdcHub::new());
        let graph_name = format!("g-w1c-{tag}");

        let plan = MutationPlan::for_method(&method);
        assert!(plan.mutates, "{tag}: must be classified as a mutation");
        assert!(plan.audited, "{tag}: W1c must now be policy-audited");
        assert!(plan.emits_cdc, "{tag}: W1c must now policy-emit CDC");
        assert_eq!(plan.durability_domain, DurabilityDomain::GraphRedb);

        let ctx = MutationCtx {
            req_id: 1,
            caller: Some("system-agent"),
            attempt_nonce: None,
            idempotency_key: "test-idempotency",
            tenant_scope: "opaque-test-tenant",
            graph_name: &graph_name,
            graph_type: GraphType::Commons,
            owner: None,
            isolation: &isolation,
            core: &core,
            persistence: Some(&persistence),
            cdc: Some(&cdc_hub),
            materialization_manifest: None,
            write_coalescer: None,
        };
        let resp = commit_mutation(&ctx, &plan, &method, apply).await;
        assert!(
            resp.error.is_none(),
            "{tag}: commit_mutation failed: {:?}",
            resp.error
        );

        let fname = crate::persist::sanitize(&graph_name);
        let report = persistence
            .as_redb()
            .expect("configured backend is redb")
            .audit_verify_blocking(&fname)
            .expect("audit_verify_blocking");
        assert!(report.ok, "{tag}: audit chain broke: {report:?}");
        assert_eq!(
            report.entries, 1,
            "{tag}: exactly one audit-chain entry must land"
        );

        let events = cdc_hub.read(&graph_name, 0, 100).events;
        assert_eq!(
            events.len(),
            1,
            "{tag}: exactly one CDC marker event, got {events:?}"
        );
    }

    /// (a2c) W1c: the 9 durable admin/ledger methods (`FromMsgpack`/`ClearLedger`/
    /// `ApplyLedger`/`CompactNodesByType`/`RunDatalogReasoning`/`Reconcile`/
    /// `ApplyMutation`/`ApplyMultisigMutation`/`IcvConfigure`) were GATEWAY_ROUTED
    /// and GraphRedb-durable but `audited: false, emits_cdc: false` -- invisible to
    /// both the tamper-evident audit chain and the CDC feed despite committing
    /// durably. Each now audits + emits CDC, consistent with the rest of the
    /// durable mutation surface (`audit::audit_line` / `cdc::emit_for_method`).
    /// 7 of the 9 don't map onto a single node/edge row, so each emits ONE
    /// reserved-marker `UpdateNode` CDC event (the same shape `ApplyChangeEnvelope`/
    /// `ServedModality` already use); those 7 are exercised here.
    #[cfg(all(feature = "redb", feature = "security", feature = "streaming"))]
    #[tokio::test(flavor = "multi_thread")]
    async fn w1c_marker_admin_ledger_methods_now_audit_and_emit_cdc() {
        // Reads the ambient encryption env at its durable open, so the env must hold
        // still for this whole body. READ guard: it excludes only a key MUTATOR, never
        // another opener. See `crate::crypto::acquire_test_env_read_lock`'s doc.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        assert_w1c_method_audits_and_emits_one_cdc_marker(
            "clear-ledger",
            Method::ClearLedger,
            |core| {
                core.clear_ledger();
                Ok(ResultPayload::String("ok".to_string()))
            },
        )
        .await;

        assert_w1c_method_audits_and_emits_one_cdc_marker(
            "apply-ledger",
            Method::ApplyLedger {
                transactions: Vec::new(),
            },
            |core| {
                core.apply_ledger(Vec::new())
                    .map(|()| ResultPayload::String("ok".to_string()))
            },
        )
        .await;

        assert_w1c_method_audits_and_emits_one_cdc_marker(
            "compact-nodes-by-type",
            Method::CompactNodesByType {
                node_type: "x".into(),
                threshold: 1,
            },
            |core| {
                let removed = core.compact_nodes_by_type("x", 1);
                Ok(ResultPayload::Json(
                    serde_json::json!({ "removed_nodes": removed }),
                ))
            },
        )
        .await;

        assert_w1c_method_audits_and_emits_one_cdc_marker(
            "apply-mutation",
            Method::ApplyMutation {
                event_type: "w1c_test_event".into(),
                query: "SELECT 1".into(),
            },
            |_core| Ok(ResultPayload::String("ok".to_string())),
        )
        .await;

        assert_w1c_method_audits_and_emits_one_cdc_marker(
            "apply-multisig-mutation",
            Method::ApplyMultisigMutation {
                signatures: vec!["sig1".into(), "sig2".into()],
                threshold: 2,
                mutation_type: "policy_update".into(),
                query: "UPDATE policy SET x = 1".into(),
            },
            |_core| Ok(ResultPayload::Bool(true)),
        )
        .await;

        assert_w1c_method_audits_and_emits_one_cdc_marker(
            "icv-configure",
            Method::IcvConfigure {
                graph: None,
                mode: "enforce".into(),
                shapes: "@prefix sh: <http://www.w3.org/ns/shacl#> .".into(),
            },
            |_core| Ok(ResultPayload::Bool(true)),
        )
        .await;

        assert_w1c_method_audits_and_emits_one_cdc_marker(
            "run-datalog-reasoning",
            Method::RunDatalogReasoning {
                subclass_relations: Vec::new(),
                subproperty_relations: Vec::new(),
                symmetric_properties: Vec::new(),
                transitive_properties: Vec::new(),
                inverse_properties: Vec::new(),
                domain_rules: Vec::new(),
                range_rules: Vec::new(),
                property_chains: Vec::new(),
            },
            |_core| {
                Ok(ResultPayload::Json(serde_json::json!({
                    "inferred_count": 0,
                    "inferred_triples": Vec::<()>::new(),
                })))
            },
        )
        .await;
    }

    /// (a2d) W1c: `FromMsgpack`/`Reconcile` both replace the graph's ENTIRE
    /// node/edge content with an imported/merged authoritative image
    /// (`GraphCore::from_msgpack`) -- the same "whole graph replaced" shape as
    /// `ClearGraph`, so their CDC signal is a feed RESET (`CdcHub::reset_graph`),
    /// not a marker event. Proven by seeding one real CDC event first (a durable
    /// `AddNode`) and observing the feed empty out afterward, while the audit
    /// chain still grows by one entry for the `FromMsgpack`/`Reconcile` call
    /// itself.
    #[cfg(all(feature = "redb", feature = "security", feature = "streaming"))]
    #[tokio::test(flavor = "multi_thread")]
    async fn w1c_from_msgpack_and_reconcile_reset_the_cdc_feed_and_audit() {
        // Reads the ambient encryption env at its durable open, so the env must hold
        // still for this whole body. READ guard: it excludes only a key MUTATOR, never
        // another opener. See `crate::crypto::acquire_test_env_read_lock`'s doc.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        for (tag, build_method) in [
            (
                "from-msgpack",
                (|msgpack: Vec<u8>| Method::FromMsgpack { msgpack }) as fn(Vec<u8>) -> Method,
            ),
            (
                "reconcile",
                (|msgpack: Vec<u8>| Method::Reconcile {
                    graph_name: "other-graph".to_string(),
                    msgpack,
                }) as fn(Vec<u8>) -> Method,
            ),
        ] {
            let dir = temp_dir(&format!("w1c-{tag}"));
            let dir_s = dir.to_string_lossy().to_string();
            let backend = RedbBackend::open(dir_s, 64).expect("open redb backend");
            let persistence: Arc<dyn PersistenceBackend> = Arc::new(backend);
            let core = Arc::new(GraphCore::new());
            let isolation = isolation_with_system_agent();
            let cdc_hub = Arc::new(crate::server::cdc::CdcHub::new());
            let graph_name = format!("g-w1c-{tag}");

            let ctx = MutationCtx {
                req_id: 1,
                caller: Some("system-agent"),
                attempt_nonce: None,
                idempotency_key: "w1c-replace",
                tenant_scope: "opaque-test-tenant",
                graph_name: &graph_name,
                graph_type: GraphType::Commons,
                owner: None,
                isolation: &isolation,
                core: &core,
                persistence: Some(&persistence),
                cdc: Some(&cdc_hub),
                materialization_manifest: None,
                write_coalescer: None,
            };

            // Seed one durable AddNode -- one audit entry, one CDC event.
            let seed = Method::AddNode {
                node_id: "seed".to_string(),
                properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({"v": 1})).unwrap(),
            };
            let seed_plan = MutationPlan::for_method(&seed);
            let seed_ctx = MutationCtx {
                idempotency_key: "w1c-seed",
                ..ctx
            };
            let seed_resp = commit_mutation(&seed_ctx, &seed_plan, &seed, |core| {
                core.add_node("seed".to_string(), Vec::new());
                Ok(ResultPayload::String("ok".to_string()))
            })
            .await;
            assert!(seed_resp.error.is_none(), "{tag}: seed AddNode failed");
            assert_eq!(
                cdc_hub.read(&graph_name, 0, 100).events.len(),
                1,
                "{tag}: seed AddNode must emit exactly one CDC event"
            );

            // FromMsgpack/Reconcile: replace with a (valid, empty) authoritative
            // snapshot of the SAME core -- policy-audited + policy-CDC-emitting.
            let msgpack = core.to_msgpack().expect("to_msgpack");
            let method = build_method(msgpack.clone());
            let plan = MutationPlan::for_method(&method);
            assert!(plan.audited, "{tag}: W1c must now be policy-audited");
            assert!(plan.emits_cdc, "{tag}: W1c must now policy-emit CDC");

            let resp = commit_mutation(&ctx, &plan, &method, move |core| {
                core.from_msgpack(&msgpack)
                    .map(|()| ResultPayload::String("ok".to_string()))
            })
            .await;
            assert!(
                resp.error.is_none(),
                "{tag}: commit_mutation failed: {:?}",
                resp.error
            );

            let fname = crate::persist::sanitize(&graph_name);
            let report = persistence
                .as_redb()
                .expect("configured backend is redb")
                .audit_verify_blocking(&fname)
                .expect("audit_verify_blocking");
            assert!(report.ok, "{tag}: audit chain broke: {report:?}");
            assert_eq!(
                report.entries, 2,
                "{tag}: seed AddNode + {tag} == exactly two audit-chain entries"
            );

            assert_eq!(
                cdc_hub.read(&graph_name, 0, 100).events.len(),
                0,
                "{tag}: the CDC feed must be RESET (empty) after the whole-graph replace"
            );
        }
    }

    /// (a5) L11 rollout batch 2, family representative: a message-broker method
    /// (`DeclareExchange`, `DurabilityDomain::Outbox`) routed through the SAME
    /// `commit_mutation` gateway produces the policy-declared durable/audit effect
    /// (audited, no CDC) against a REAL `RedbBackend` — proving the Outbox
    /// durability domain commits through the identical `record_durable` path as
    /// the GraphRedb-domain methods above (the backend does not branch on
    /// `DurabilityDomain`, only on whether it's `None`).
    #[cfg(all(
        feature = "redb",
        feature = "broker",
        feature = "security",
        feature = "streaming"
    ))]
    #[tokio::test(flavor = "multi_thread")]
    async fn broker_family_routed_mutation_is_audited_with_no_cdc() {
        // Reads the ambient encryption env at its durable open, so the env must hold
        // still for this whole body. READ guard: it excludes only a key MUTATOR, never
        // another opener. See `crate::crypto::acquire_test_env_read_lock`'s doc.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        let dir = temp_dir("broker-declare-exchange");
        let dir_s = dir.to_string_lossy().to_string();
        let backend = RedbBackend::open(dir_s, 64).expect("open redb backend");
        let persistence: Arc<dyn PersistenceBackend> = Arc::new(backend);

        let core = Arc::new(GraphCore::new());
        let isolation = isolation_with_system_agent();
        let cdc_hub = Arc::new(crate::server::cdc::CdcHub::new());
        let graph_name = "g-eg-p0-2-l11-broker-a";

        let method = Method::DeclareExchange {
            exchange: "orders".to_string(),
            kind: "direct".to_string(),
        };
        let plan = MutationPlan::for_method(&method);
        assert!(plan.mutates);
        assert_eq!(plan.durability_domain, DurabilityDomain::Outbox);
        assert!(plan.audited, "DeclareExchange is policy-audited");
        assert!(!plan.emits_cdc, "DeclareExchange policy-does-NOT-emit-CDC");

        let ctx = MutationCtx {
            req_id: 10,
            caller: Some("system-agent"),
            attempt_nonce: None,
            idempotency_key: "test-idempotency",
            tenant_scope: "opaque-test-tenant",
            graph_name,
            graph_type: GraphType::Commons,
            owner: None,
            isolation: &isolation,
            core: &core,
            persistence: Some(&persistence),
            cdc: Some(&cdc_hub),
            materialization_manifest: None,
            write_coalescer: None,
        };

        let resp = commit_mutation(&ctx, &plan, &method, move |core| {
            let Some(k) = crate::broker::ExchangeKind::parse("direct") else {
                return Err("bad kind".to_string());
            };
            crate::broker::declare_exchange(core, "orders", k)
                .map(|()| ResultPayload::String("ok".to_string()))
        })
        .await;
        assert!(
            resp.error.is_none(),
            "commit_mutation failed: {:?}",
            resp.error
        );

        let fname = crate::persist::sanitize(graph_name);
        let report = persistence
            .as_redb()
            .expect("configured backend is redb")
            .audit_verify_blocking(&fname)
            .expect("audit_verify_blocking");
        assert!(report.ok, "audit chain broke: {report:?}");
        assert_eq!(
            report.entries, 1,
            "exactly one audited mutation (DeclareExchange) went through the gateway"
        );
        assert_eq!(
            cdc_hub.read(graph_name, 0, 100).events.len(),
            0,
            "DeclareExchange must NOT emit a CDC event (Outbox methods never do)"
        );
    }

    /// (a6) State-backed maintenance operations use the same authoritative
    /// MutationBatch boundary as ordinary graph writes. `TouchNodes` remains
    /// intentionally unaudited and CDC-silent, but its resulting graph image and
    /// terminal result must survive restart.
    #[cfg(all(feature = "redb", feature = "security"))]
    #[tokio::test(flavor = "multi_thread")]
    async fn touch_nodes_commits_authoritative_state_without_audit_or_cdc() {
        // Reads the ambient encryption env at its durable open, so the env must hold
        // still for this whole body. READ guard: it excludes only a key MUTATOR, never
        // another opener. See `crate::crypto::acquire_test_env_read_lock`'s doc.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        let dir = temp_dir("touch-nodes-state-backed");
        let dir_s = dir.to_string_lossy().to_string();
        let backend = RedbBackend::open(dir_s, 64).expect("open redb backend");
        let persistence: Arc<dyn PersistenceBackend> = Arc::new(backend);

        let core = Arc::new(GraphCore::new());
        core.add_node(
            "n1".to_string(),
            rmp_serde::to_vec_named(&serde_json::json!({})).expect("encode seed node"),
        );
        let isolation = isolation_with_system_agent();
        let graph_name = "g-eg-p0-2-l11-none-durability-a";

        let method = Method::TouchNodes {
            node_ids: vec!["n1".to_string()],
        };
        let plan = MutationPlan::for_method(&method);
        assert!(plan.mutates);
        assert_eq!(plan.durability_domain, DurabilityDomain::GraphRedb);
        assert!(!plan.audited);
        assert!(!plan.emits_cdc);

        let ctx = MutationCtx {
            req_id: 11,
            caller: Some("system-agent"),
            attempt_nonce: None,
            idempotency_key: "test-idempotency",
            tenant_scope: "opaque-test-tenant",
            graph_name,
            graph_type: GraphType::Commons,
            owner: None,
            isolation: &isolation,
            core: &core,
            persistence: Some(&persistence),
            #[cfg(feature = "streaming")]
            cdc: None,
            materialization_manifest: None,
            write_coalescer: None,
        };

        let resp = commit_mutation(&ctx, &plan, &method, move |core| {
            let now = 1_000u64;
            let touched = core.touch_nodes(&["n1".to_string()], now);
            Ok(ResultPayload::Count(touched as u64))
        })
        .await;
        assert!(
            resp.error.is_none(),
            "commit_mutation failed: {:?}",
            resp.error
        );

        let fname = crate::persist::sanitize(graph_name);
        let batch_id = crate::server::mutation_batch::opaque_idempotency_key_for_context(
            "rpc",
            "opaque-test-tenant",
            graph_name,
            Some("system-agent"),
            "test-idempotency",
        );
        let record = persistence
            .read_mutation_batch(&fname, &batch_id)
            .await
            .expect("read mutation batch")
            .expect("TouchNodes batch is durable");
        assert_eq!(
            record.batch.identity.tenant().as_str(),
            eg_storage::GRAPH_SHARD_TENANT
        );
        let (snapshot, version) = persistence
            .read_authoritative_graph_snapshot(&fname)
            .await
            .expect("read authoritative graph")
            .expect("TouchNodes graph image is durable");
        assert!(version > 0);
        let node = snapshot
            .nodes
            .iter()
            .find(|(id, _)| id == "n1")
            .expect("durable node");
        let properties: serde_json::Value =
            eg_types::msgpack::decode_property_value(node.1.as_slice())
                .expect("decode durable node");
        assert_eq!(properties["last_access"], serde_json::json!(1_000));
        assert_eq!(properties["confidence"], serde_json::json!(1.0));

        let report = persistence
            .as_redb()
            .expect("configured backend is redb")
            .audit_verify_blocking(&fname)
            .expect("audit_verify_blocking");
        assert!(report.ok, "audit chain broke: {report:?}");
        assert_eq!(
            report.entries, 0,
            "TouchNodes is state-backed but intentionally unaudited"
        );
    }

    /// (a7) L11 rollout batch 3, RUNTIME-CONDITIONAL family representative:
    /// `MineAssociate` proves `commit_conditional_mutation` resolves the
    /// durability/audit/authz gateway from the request's OWN `writeback` field,
    /// not from `policy()`'s conservative upper bound:
    ///   * `writeback: true`  -> Write access required, one audited durable entry.
    ///   * `writeback: false` -> Read access SUFFICES (an agent with only Read
    ///     succeeds), and NOTHING durable/audited is produced -- proving a
    ///     read-only call is never incorrectly gated behind Write or persisted as
    ///     a phantom mutation (the bug this function exists to prevent).
    #[cfg(all(feature = "redb", feature = "mining", feature = "security"))]
    #[tokio::test(flavor = "multi_thread")]
    async fn mining_family_writeback_gates_durability_and_authz() {
        // Reads the ambient encryption env at its durable open, so the env must hold
        // still for this whole body. READ guard: it excludes only a key MUTATOR, never
        // another opener. See `crate::crypto::acquire_test_env_read_lock`'s doc.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        let dir = temp_dir("mine-associate-conditional");
        let dir_s = dir.to_string_lossy().to_string();
        let backend = RedbBackend::open(dir_s, 64).expect("open redb backend");
        let persistence: Arc<dyn PersistenceBackend> = Arc::new(backend);

        let core = Arc::new(GraphCore::new());
        let graph_name = "g-eg-p0-2-l11-mining-a";

        // This test's job is the DURABILITY/AUDIT half of the runtime-conditional
        // gateway (below). The Read-vs-Write ACL distinction is already covered
        // structurally by `commit_conditional_mutation`'s own code path (the
        // `mutates_now == false` branch calls `check_graph_access(.., AccessLevel::
        // Read)`; `true` goes through `commit_mutation`, which uses `AccessLevel::
        // Write`) and by `unauthorized_actor_is_rejected_at_the_gateway` proving the
        // Write gate itself denies an unauthorized caller.
        let isolation = isolation_with_system_agent();

        let transactions = vec![
            vec!["a".to_string(), "b".to_string()],
            vec!["a".to_string(), "b".to_string()],
            vec!["a".to_string(), "b".to_string()],
        ];

        // ── writeback: false -- a plain read: no durability, no audit ──
        let method_ro = Method::MineAssociate {
            transactions: transactions.clone(),
            source: None,
            min_support: 0.5,
            min_confidence: 0.5,
            algorithm: crate::protocol::MineAlgorithm::default(),
            writeback: false,
            #[cfg(feature = "epistemic")]
            as_claim: false,
        };
        let plan_ro = MutationPlan::for_method(&method_ro);
        let ctx_ro = MutationCtx {
            req_id: 20,
            caller: Some("system-agent"),
            attempt_nonce: None,
            idempotency_key: "test-idempotency",
            tenant_scope: "opaque-test-tenant",
            graph_name,
            graph_type: GraphType::Commons,
            owner: None,
            isolation: &isolation,
            core: &core,
            persistence: Some(&persistence),
            #[cfg(feature = "streaming")]
            cdc: None,
            materialization_manifest: None,
            write_coalescer: None,
        };
        let resp = commit_conditional_mutation(&ctx_ro, &plan_ro, &method_ro, false, |core| {
            let resp = crate::server::handlers::mining::handle_associate(
                20,
                core,
                crate::server::handlers::mining::AssociationRequest {
                    transactions: transactions.clone(),
                    source: None,
                    min_support: 0.5,
                    min_confidence: 0.5,
                    algorithm: crate::protocol::MineAlgorithm::default(),
                    writeback: crate::server::handlers::mining::WritebackOptions {
                        enabled: false,
                        #[cfg(feature = "epistemic")]
                        as_claim: false,
                    },
                },
            );
            match resp.error {
                Some(e) => Err(e),
                None => Ok(resp.result.unwrap()),
            }
        })
        .await;
        assert!(
            resp.error.is_none(),
            "read-only call failed: {:?}",
            resp.error
        );

        let fname = crate::persist::sanitize(graph_name);
        let report = persistence
            .as_redb()
            .expect("configured backend is redb")
            .audit_verify_blocking(&fname)
            .expect("audit_verify_blocking");
        assert!(report.ok);
        assert_eq!(
            report.entries, 0,
            "writeback:false must produce NO durable/audit record"
        );

        // ── writeback: true -- a real mutation: durable + audited ──
        let method_rw = Method::MineAssociate {
            transactions: transactions.clone(),
            source: None,
            min_support: 0.5,
            min_confidence: 0.5,
            algorithm: crate::protocol::MineAlgorithm::default(),
            writeback: true,
            #[cfg(feature = "epistemic")]
            as_claim: false,
        };
        let plan_rw = MutationPlan::for_method(&method_rw);
        assert!(plan_rw.audited, "MineAssociate is policy-audited");
        assert!(!plan_rw.emits_cdc, "MineAssociate policy-does-NOT-emit-CDC");
        let ctx_rw = MutationCtx {
            req_id: 21,
            caller: Some("system-agent"),
            attempt_nonce: None,
            idempotency_key: "test-idempotency",
            tenant_scope: "opaque-test-tenant",
            graph_name,
            graph_type: GraphType::Commons,
            owner: None,
            isolation: &isolation,
            core: &core,
            persistence: Some(&persistence),
            #[cfg(feature = "streaming")]
            cdc: None,
            materialization_manifest: None,
            write_coalescer: None,
        };
        let resp = commit_conditional_mutation(&ctx_rw, &plan_rw, &method_rw, true, |core| {
            let resp = crate::server::handlers::mining::handle_associate(
                21,
                core,
                crate::server::handlers::mining::AssociationRequest {
                    transactions: transactions.clone(),
                    source: None,
                    min_support: 0.5,
                    min_confidence: 0.5,
                    algorithm: crate::protocol::MineAlgorithm::default(),
                    writeback: crate::server::handlers::mining::WritebackOptions {
                        enabled: true,
                        #[cfg(feature = "epistemic")]
                        as_claim: false,
                    },
                },
            );
            match resp.error {
                Some(e) => Err(e),
                None => Ok(resp.result.unwrap()),
            }
        })
        .await;
        assert!(
            resp.error.is_none(),
            "writeback call failed: {:?}",
            resp.error
        );

        let report = persistence
            .as_redb()
            .expect("configured backend is redb")
            .audit_verify_blocking(&fname)
            .expect("audit_verify_blocking");
        assert!(report.ok);
        assert_eq!(
            report.entries, 1,
            "writeback:true must produce exactly one audited durable record"
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
            attempt_nonce: None,
            idempotency_key: "test-idempotency",
            tenant_scope: "opaque-test-tenant",
            graph_name: "agent:owner",
            graph_type: GraphType::Agent,
            owner: Some("owner"),
            isolation: &isolation,
            core: &core,
            persistence: None,
            #[cfg(feature = "streaming")]
            cdc: None,
            materialization_manifest: None,
            write_coalescer: None,
        };

        let mut apply_ran = false;
        let resp = commit_mutation(&ctx, &plan, &method, |core| {
            apply_ran = true;
            core.add_node("n1".to_string(), Vec::new());
            Ok(ResultPayload::String("ok".to_string()))
        })
        .await;

        assert!(
            resp.error.is_some(),
            "expected ACCESS_DENIED, got {:?}",
            resp
        );
        assert!(resp.error.unwrap().contains("ACCESS_DENIED"));
        assert!(!apply_ran, "apply must never run for a denied caller");
        assert!(!core.has_node("n1"), "the graph must be untouched");
    }

    /// Durable facade replay is decided by the kernel: a fresh nonce and request
    /// id replay the stable caller key without invoking the effect again, while
    /// the original nonce and a changed payload under that key are refused.
    #[cfg(feature = "redb")]
    #[tokio::test(flavor = "multi_thread")]
    async fn durable_facade_replay_uses_stable_key_and_kernel_nonce_authority() {
        // Reads the ambient encryption env at its durable open, so the env must hold
        // still for this whole body. READ guard: it excludes only a key MUTATOR, never
        // another opener. See `crate::crypto::acquire_test_env_read_lock`'s doc.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        let dir = temp_dir("stable-kernel-replay");
        let dir_s = dir.to_string_lossy().to_string();
        let persistence: Arc<dyn PersistenceBackend> =
            Arc::new(RedbBackend::open(dir_s.clone(), 64).expect("open redb backend"));
        let core = Arc::new(GraphCore::new());
        let isolation = isolation_with_system_agent();
        let graph_name = "g-eg-stable-kernel-replay";
        let method = Method::RemoveNode {
            node_id: "n1".to_string(),
        };
        let plan = MutationPlan::for_method(&method);
        let apply_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let first = MutationCtx {
            req_id: 4,
            caller: Some("system-agent"),
            attempt_nonce: Some(eg_types::contract::Nonce::from_bytes([1; 32])),
            idempotency_key: "caller-stable-remove",
            tenant_scope: "opaque-test-tenant",
            graph_name,
            graph_type: GraphType::Commons,
            owner: None,
            isolation: &isolation,
            core: &core,
            persistence: Some(&persistence),
            #[cfg(feature = "streaming")]
            cdc: None,
            materialization_manifest: None,
            write_coalescer: None,
        };
        let count = Arc::clone(&apply_count);
        let response = commit_mutation(&first, &plan, &method, move |core| {
            count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            core.remove_node("n1".to_string());
            Ok(ResultPayload::String("ok".to_string()))
        })
        .await;
        assert!(response.error.is_none(), "{:?}", response.error);
        let _ = first;
        drop(persistence);
        drop(core);

        let persistence: Arc<dyn PersistenceBackend> =
            Arc::new(RedbBackend::open(dir_s, 64).expect("reopen redb backend"));
        let core = Arc::new(GraphCore::new());
        let retry = MutationCtx {
            req_id: 5,
            caller: Some("system-agent"),
            attempt_nonce: Some(eg_types::contract::Nonce::from_bytes([2; 32])),
            idempotency_key: "caller-stable-remove",
            tenant_scope: "opaque-test-tenant",
            graph_name,
            graph_type: GraphType::Commons,
            owner: None,
            isolation: &isolation,
            core: &core,
            persistence: Some(&persistence),
            #[cfg(feature = "streaming")]
            cdc: None,
            materialization_manifest: None,
            write_coalescer: None,
        };
        let count = Arc::clone(&apply_count);
        let response = commit_mutation(&retry, &plan, &method, move |_| {
            count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(ResultPayload::String("duplicate".to_string()))
        })
        .await;
        assert!(response.error.is_none(), "{:?}", response.error);
        assert_eq!(format!("{:?}", response.result), "Some(String(\"ok\"))");
        assert_eq!(apply_count.load(std::sync::atomic::Ordering::SeqCst), 1);

        let consumed = MutationCtx {
            req_id: 6,
            attempt_nonce: Some(eg_types::contract::Nonce::from_bytes([1; 32])),
            ..retry
        };
        let response = commit_mutation(&consumed, &plan, &method, |_| {
            panic!("consumed nonce must be rejected before apply")
        })
        .await;
        assert!(response
            .error
            .as_deref()
            .is_some_and(|e| e.contains("REPLAY_NONCE_CONSUMED")));

        let changed = Method::RemoveNode {
            node_id: "different-node".to_string(),
        };
        let changed_plan = MutationPlan::for_method(&changed);
        let conflict = MutationCtx {
            req_id: 7,
            attempt_nonce: Some(eg_types::contract::Nonce::from_bytes([3; 32])),
            ..consumed
        };
        let response = commit_mutation(&conflict, &changed_plan, &changed, |_| {
            panic!("idempotency conflict must be rejected before apply")
        })
        .await;
        assert!(response
            .error
            .as_deref()
            .is_some_and(|e| e.contains("IDEMPOTENCY_CONFLICT")));
        assert_eq!(apply_count.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    /// The async query facade uses the same durable replay authority without
    /// rerunning a staged query to discover whether its key committed before a
    /// lost acknowledgement.
    #[cfg(all(feature = "redb", feature = "cypher"))]
    #[tokio::test(flavor = "multi_thread")]
    async fn staged_query_replay_probes_kernel_before_running_the_handler() {
        // Reads the ambient encryption env at its durable open, so the env must hold
        // still for this whole body. READ guard: it excludes only a key MUTATOR, never
        // another opener. See `crate::crypto::acquire_test_env_read_lock`'s doc.
        let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
        let dir = temp_dir("staged-query-kernel-replay");
        let dir_s = dir.to_string_lossy().to_string();
        let persistence: Arc<dyn PersistenceBackend> =
            Arc::new(RedbBackend::open(dir_s.clone(), 64).expect("open redb backend"));
        let core = Arc::new(GraphCore::new());
        let isolation = isolation_with_system_agent();
        let graph_name = "g-eg-staged-query-replay";
        let method = Method::CypherQuery {
            query: "CREATE (n:ReplayProbe {id: 'n1'})".to_string(),
            mode: crate::protocol::CypherMode::Write,
        };
        let plan = MutationPlan::for_method(&method);
        let apply_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let first = MutationCtx {
            req_id: 40,
            caller: Some("system-agent"),
            attempt_nonce: Some(eg_types::contract::Nonce::from_bytes([11; 32])),
            idempotency_key: "caller-stable-cypher",
            tenant_scope: "opaque-test-tenant",
            graph_name,
            graph_type: GraphType::Commons,
            owner: None,
            isolation: &isolation,
            core: &core,
            persistence: Some(&persistence),
            #[cfg(feature = "streaming")]
            cdc: None,
            materialization_manifest: None,
            write_coalescer: None,
        };
        let count = Arc::clone(&apply_count);
        let response = commit_conditional_mutation_async(
            &first,
            &plan,
            &method,
            true,
            move |staged| async move {
                count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                staged.add_node("n1".to_string(), Vec::new());
                Ok(ResultPayload::String("created".to_string()))
            },
        )
        .await;
        assert!(response.error.is_none(), "{:?}", response.error);
        let _ = first;
        drop(persistence);
        drop(core);

        let persistence: Arc<dyn PersistenceBackend> =
            Arc::new(RedbBackend::open(dir_s, 64).expect("reopen redb backend"));
        let core = Arc::new(GraphCore::new());
        let retry = MutationCtx {
            req_id: 41,
            caller: Some("system-agent"),
            attempt_nonce: Some(eg_types::contract::Nonce::from_bytes([12; 32])),
            idempotency_key: "caller-stable-cypher",
            tenant_scope: "opaque-test-tenant",
            graph_name,
            graph_type: GraphType::Commons,
            owner: None,
            isolation: &isolation,
            core: &core,
            persistence: Some(&persistence),
            #[cfg(feature = "streaming")]
            cdc: None,
            materialization_manifest: None,
            write_coalescer: None,
        };
        let response = commit_conditional_mutation_async(&retry, &plan, &method, true, |_| async {
            panic!("stable replay must not rerun staged query")
        })
        .await;
        assert!(response.error.is_none(), "{:?}", response.error);
        assert_eq!(
            format!("{:?}", response.result),
            "Some(String(\"created\"))"
        );
        assert_eq!(apply_count.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert!(core.has_node("n1"));

        let consumed = MutationCtx {
            req_id: 42,
            attempt_nonce: Some(eg_types::contract::Nonce::from_bytes([11; 32])),
            ..retry
        };
        let response =
            commit_conditional_mutation_async(&consumed, &plan, &method, true, |_| async {
                panic!("consumed nonce must not rerun staged query")
            })
            .await;
        assert!(response
            .error
            .as_deref()
            .is_some_and(|error| error.contains("REPLAY_NONCE_CONSUMED")));

        let changed = Method::CypherQuery {
            query: "CREATE (n:ReplayProbe {id: 'n2'})".to_string(),
            mode: crate::protocol::CypherMode::Write,
        };
        let changed_plan = MutationPlan::for_method(&changed);
        let conflict = MutationCtx {
            req_id: 43,
            attempt_nonce: Some(eg_types::contract::Nonce::from_bytes([13; 32])),
            ..consumed
        };
        let response = commit_conditional_mutation_async(
            &conflict,
            &changed_plan,
            &changed,
            true,
            |_| async { panic!("conflict must not rerun staged query") },
        )
        .await;
        assert!(response
            .error
            .as_deref()
            .is_some_and(|error| error.contains("IDEMPOTENCY_CONFLICT")));
        assert!(!core.has_node("n2"));
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
            Method::CreateNodeIfAbsent {
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
            // ── L11 rollout batch 2: graph-core family ──
            Method::CompareAndSetNodeFields {
                node_id: "x".into(),
                conditions_msgpack: Vec::new(),
                updates_msgpack: Vec::new(),
            },
            Method::ClaimNext {
                label: "x".into(),
                updates_msgpack: Vec::new(),
            },
            Method::DecayNode {
                node_id: "x".into(),
                now_ms: 0,
                half_life_ms: 1,
            },
            Method::DecayMemories {
                now_ms: 0,
                half_life_ms: 1,
                ids: vec!["x".into()],
            },
            Method::EvictBelow {
                ids: vec!["x".into()],
                threshold: 0.0,
                delete: false,
            },
            Method::Maintain {
                ids: vec!["x".into()],
                now_ms: 0,
                half_life_ms: 1,
                evict_threshold: 0.0,
                delete: false,
            },
            Method::AddSceneObject {
                pose_msgpack: Vec::new(),
                parent: None,
            },
            Method::SetPose {
                node_id: "x".into(),
                pose_msgpack: Vec::new(),
            },
            Method::Reparent {
                node_id: "x".into(),
                new_parent: None,
            },
            Method::StartTrajectory {
                props_msgpack: Vec::new(),
            },
            Method::AppendStep {
                traj_id: "x".into(),
                action_msgpack: Vec::new(),
                reward: 0.0,
                state_ref: None,
                next_state_ref: None,
                t: 0,
            },
            Method::AddEmbedding {
                node_id: "x".into(),
                embedding: vec![0.0],
            },
            Method::InvalidateEdge {
                source_id: "a".into(),
                target_id: "b".into(),
                relationship: "r".into(),
                invalid_at: 0,
                tx_now: 0,
            },
            Method::SupersedeEdge {
                source_id: "a".into(),
                target_id: "b".into(),
                properties_msgpack: Vec::new(),
                prior_source: "a".into(),
                prior_target: "b".into(),
                prior_relationship: "r".into(),
                valid_at: 0,
                tx_now: 0,
            },
            Method::ClearGraph,
            Method::EvictLRU { max_nodes: 1 },
            Method::DecaySweep {
                half_life_secs: 1.0,
                floor: 0.0,
                prune: false,
            },
            Method::TouchNodes {
                node_ids: vec!["x".into()],
            },
            Method::FromMsgpack {
                msgpack: Vec::new(),
            },
            Method::Reconcile {
                graph_name: "g".into(),
                msgpack: Vec::new(),
            },
            Method::ApplyMutation {
                event_type: "e".into(),
                query: "q".into(),
            },
            #[cfg(feature = "shacl")]
            Method::IcvConfigure {
                graph: None,
                mode: "enforce".into(),
                shapes: "@prefix sh: <http://www.w3.org/ns/shacl#> .".into(),
            },
            #[cfg(feature = "reasoning")]
            Method::RunDatalogReasoning {
                subclass_relations: Vec::new(),
                subproperty_relations: Vec::new(),
                symmetric_properties: Vec::new(),
                transitive_properties: Vec::new(),
                inverse_properties: Vec::new(),
                domain_rules: Vec::new(),
                range_rules: Vec::new(),
                property_chains: Vec::new(),
            },
            Method::PruneByLifecycle {
                max_age_secs: 1,
                min_score: 0.0,
            },
            Method::BatchUpdate {
                operations_msgpack: Vec::new(),
            },
            Method::ClearLedger,
            Method::ApplyLedger {
                transactions: Vec::new(),
            },
            Method::CompactNodesByType {
                node_type: "x".into(),
                threshold: 1,
            },
            // ── L11 rollout batch 2: message-broker / stream family ──
            #[cfg(feature = "broker")]
            Method::DeclareExchange {
                exchange: "x".into(),
                kind: "direct".into(),
            },
            #[cfg(feature = "broker")]
            Method::DeleteExchange {
                exchange: "x".into(),
            },
            #[cfg(feature = "broker")]
            Method::BindQueue {
                exchange: "x".into(),
                queue: "q".into(),
                routing_key: "k".into(),
            },
            #[cfg(feature = "broker")]
            Method::UnbindQueue {
                exchange: "x".into(),
                queue: "q".into(),
                routing_key: "k".into(),
            },
            #[cfg(feature = "broker")]
            Method::Publish {
                exchange: "x".into(),
                routing_key: "k".into(),
                payload: Vec::new(),
            },
            #[cfg(feature = "broker")]
            Method::DeclareQueue {
                queue: "q".into(),
                dl_exchange: None,
                dl_routing_key: None,
                max_delivery_count: None,
                message_ttl_ms: None,
                queue_expiry_ms: None,
                max_priority: None,
            },
            #[cfg(feature = "broker")]
            Method::PublishEx {
                exchange: "x".into(),
                routing_key: "k".into(),
                payload: Vec::new(),
                priority: 0,
                delay_ms: None,
                ttl_ms: None,
                now_ms: None,
            },
            #[cfg(feature = "broker")]
            Method::BrokerConsume {
                queue: "q".into(),
                group: "g".into(),
                consumer: "c".into(),
                now_ms: 0,
                lease_ms: 0,
                prefetch: 0,
            },
            #[cfg(feature = "broker")]
            Method::BrokerAck {
                queue: "q".into(),
                node_id: "x".into(),
            },
            #[cfg(feature = "broker")]
            Method::BrokerReject {
                queue: "q".into(),
                node_id: "x".into(),
                requeue: false,
                now_ms: 0,
            },
            #[cfg(feature = "broker")]
            Method::SweepExpired { now_ms: 0 },
            #[cfg(feature = "broker")]
            Method::StreamDeclare {
                stream: "s".into(),
                max_messages: None,
                max_age_ms: None,
            },
            #[cfg(feature = "broker")]
            Method::StreamPublish {
                stream: "s".into(),
                payload: Vec::new(),
                now_ms: 0,
            },
            #[cfg(feature = "broker")]
            Method::StreamTrim {
                stream: "s".into(),
                now_ms: 0,
            },
            #[cfg(feature = "broker")]
            Method::StreamCommitOffset {
                stream: "s".into(),
                group: "g".into(),
                offset: 0,
            },
            #[cfg(feature = "broker")]
            Method::PublishConfirmed {
                exchange: "x".into(),
                routing_key: "k".into(),
                payload: Vec::new(),
                priority: 0,
                delay_ms: None,
                ttl_ms: None,
                now_ms: None,
            },
            #[cfg(feature = "broker")]
            Method::PublishIdempotent {
                exchange: "x".into(),
                routing_key: "k".into(),
                payload: Vec::new(),
                producer_id: None,
                seq: 0,
                priority: 0,
                delay_ms: None,
                ttl_ms: None,
                now_ms: None,
            },
            #[cfg(feature = "broker")]
            Method::BrokerAckTag {
                delivery_tag: 0,
                consumer: "c".into(),
            },
            #[cfg(feature = "broker")]
            Method::BrokerNackTag {
                delivery_tag: 0,
                consumer: "c".into(),
                requeue: false,
                now_ms: 0,
            },
            #[cfg(feature = "broker")]
            Method::BrokerRenewTag {
                delivery_tag: 0,
                consumer: "c".into(),
                now_ms: 0,
                lease_ms: 1,
            },
            // ── L11 rollout batch 3: runtime-conditional graph-learning family ──
            #[cfg(feature = "graphlearn")]
            Method::GraphLearnFit {
                source: crate::protocol::GraphSource {
                    node_label: "x".into(),
                    direction: "any".into(),
                    relation: None,
                    limit: 0,
                },
                params: crate::protocol::GraphLearnParams {
                    basis: "chebyshev".into(),
                    degree: 3,
                    hidden: 0,
                    epochs: 1,
                    lr: 0.1,
                    neg_ratio: 1.0,
                    seed: 0,
                    alpha: 0.5,
                },
                writeback: false,
            },
            #[cfg(feature = "graphlearn")]
            Method::GraphLearnPredict {
                model: serde_json::Value::Null,
                source: crate::protocol::GraphSource {
                    node_label: "x".into(),
                    direction: "any".into(),
                    relation: None,
                    limit: 0,
                },
                candidate_pairs: Vec::new(),
                top_k: 10,
                writeback: false,
            },
            // ── L11 rollout batch 3: runtime-conditional data-mining family.
            // `MineAssociate` is a SPOT CHECK, not full coverage of all 17: every
            // Mine* variant's `policy()` arm (`crates/eg-capabilities/src/lib.rs`)
            // is one of 4 groups that share the IDENTICAL `MethodPolicy` value, and
            // `MutationPlan::for_method`'s call site has no per-variant branching at
            // all (it is `eg_capabilities::policy(method)` verbatim for every Method
            // in existence) -- so one sample exercises the same code path the other
            // 16 would. Full name-level coverage (every Mine* name really is a
            // mutating, policy-known method) is still asserted for ALL 17 by
            // `gateway_routed_set_matches_mutating_policy_surface` below, which
            // needs no `Method` construction (it reads
            // `eg_capabilities::method_policy_entries()` by name). The runtime-conditional (`writeback`) gateway PATH itself is
            // proven end-to-end (WAL + audit, no CDC, Read-vs-Write ACL both ways)
            // by `mining_family_writeback_gates_durability_and_authz` below.
            #[cfg(feature = "mining")]
            Method::MineAssociate {
                transactions: vec![vec!["a".into(), "b".into()]],
                source: None,
                min_support: 0.1,
                min_confidence: 0.5,
                algorithm: crate::protocol::MineAlgorithm::default(),
                writeback: false,
                #[cfg(feature = "epistemic")]
                as_claim: false,
            },
        ];
        // NOTE: this crate's default/test feature set always includes `broker` +
        // `reasoning` + `graphlearn` + `mining` (`default = ["graph", "algorithms",
        // "metrics", "full"]`), so every sample above is unconditionally constructed
        // in the build this test actually runs under; the `#[cfg]`s only keep the
        // file correct (it still compiles) for a slimmer, non-default feature
        // selection. `samples` is intentionally a SUBSET of `GATEWAY_ROUTED` now
        // (see the Mine* comment above) -- every sample must still be routed +
        // policy-consistent, but not every routed name needs a hand-built sample.
        assert!(samples.len() <= GATEWAY_ROUTED.len());
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

    /// L11 rollout: every mutating method NOT in [`GATEWAY_ROUTED`] falls into
    /// EXACTLY one of two documented buckets -- there is no silent third category.
    ///
    /// **NON_GATEWAY_COORDINATED** -- does not fit `commit_mutation`'s
    /// `(ctx, plan, method, apply)` shape for a real, load-bearing architectural
    /// reason (a different commit protocol entirely, a non-graph-scoped store
    /// with no `GraphCore`/`graph_name` in scope, a cross-shard/cluster-wide op,
    /// or a registry-lifecycle op that runs before/across a single graph's core
    /// exists). Each entry cites the native MutationBatch, saga, translation, or
    /// explicitly non-authoritative staging mechanism that owns the operation.
    ///
    /// **OPEN_NOT_JUSTIFIED** -- reserved for a graph-scoped, structurally routable
    /// mutation that has no coordinator yet. It is intentionally empty and asserted
    /// empty below; adding such a method is therefore a visible test-gated regression.
    const NON_GATEWAY_COORDINATED: &[(&str, &str)] = &[
        // ── OCC/2PC multi-op transaction protocol (src/server/handlers/txn.rs) ──
        // `Commit` applies a DYNAMIC list of already-staged Methods through one or
        // more child MutationBatches, while a multi-graph commit routes through 2PC
        // (`CrossShardCoordinator`), and a cross-modal commit lands
        // graph+vector+blob+series in ONE redb `WriteTransaction`
        // (`commit_cross_modal_txn`) -- none of that is a single `(ctx, plan,
        // method, apply)` call the gateway models. The Txn* STAGE ops don't even
        // mutate durable state at stage time (only `Commit` does); they self-route
        // in `dispatch.rs` BEFORE `dispatch_graph_op` entirely (resolved from
        // `open_txns`, not `req.graph`).
        ("BeginTxn", "encrypted Raft-native OCC staging authority"),
        ("TxnAddNode", "replicated OCC staging; named Commit receipt and graph MutationBatch own publication authority"),
        ("TxnRemoveNode", "ephemeral OCC staging; named Commit receipt and graph MutationBatch own authority"),
        ("TxnAddEdge", "ephemeral OCC staging; named Commit receipt and graph MutationBatch own authority"),
        ("TxnRemoveEdge", "ephemeral OCC staging; named Commit receipt and graph MutationBatch own authority"),
        ("TxnCas", "ephemeral OCC staging; named Commit receipt and graph MutationBatch own authority"),
        ("TxnAddEmbedding", "ephemeral OCC staging; named Commit receipt and cross-modal MutationBatch own authority"),
        ("TxnBlobRef", "ephemeral OCC staging; named Commit receipt and cross-modal MutationBatch own authority"),
        ("TxnAddMeasurement", "ephemeral OCC staging; named Commit receipt and cross-modal MutationBatch own authority"),
        ("TxnAxiom", "ephemeral OCC staging; named Commit receipt and cross-modal MutationBatch own authority"),
        ("TxnConstruct", "ephemeral OCC staging; named Commit receipt and cross-modal MutationBatch own authority"),
        ("TxnPlanWriteback", "ephemeral OCC staging; named Commit receipt and cross-modal MutationBatch own authority"),
        ("TxnMaterializeBelief", "ephemeral OCC staging; named Commit receipt and cross-modal MutationBatch own authority"),
        ("Commit", "named control-plane receipt coordinates graph/cross-modal/2PC child MutationBatches"),
        ("Rollback", "replicated removal of encrypted OCC staging authority"),
        // ── RF-ADR-009 / RF-ADR-010: the decision and connector-pack surfaces ──
        // Each commits through the agent-library owner's own eg-transaction
        // write, not a `(ctx, plan, method, apply)` graph mutation, and each
        // has process-local ordering only (see `plan::LOCAL_ONLY_METHODS`).
        ("ConnectorPack", "native MutationBatch in agent_library.redb: pack head, members, component revisions, body holders, import record, receipt and outbox in one WTX; local-only authority"),
        ("WriteBack", "native MutationBatch in agent_library.redb: immutable source change set plus append-only attempt and reconciliation receipts; local-only authority"),
        ("DecisionCommit", "native MutationBatch in agent_library.redb: one DecisionRecord component revision, receipt and outbox in one WTX after re-derivation and catalog compare-and-set; local-only authority"),
        ("DecisionFit", "native MutationBatch in jobs.redb: decision job row and receipt; the draft artifact is an engine-held Blob CAS body; local-only authority"),
        ("DecisionEval", "native MutationBatch in jobs.redb: evaluation job row and receipt; local-only authority"),
        ("MutationOutbox", "owner-local outbox ledger rewind: bounded eg-transaction transactions with a durable control cursor; local-only authority"),
        ("ClaimWorkItem", "dedicated engine-native MutationBatch lease transition in mutation_batch.rs/redb_store.rs"),
        ("KgDelegate", "authenticated Agent Library admission lowers to the native WorkItem command-log transaction"),
        ("SubmitWorkItem", "dedicated engine-native atomic WorkItem command-log admission in mutation_batch.rs/redb_store.rs"),
        ("SubmitWorkItems", "dedicated engine-native bounded atomic WorkItem admission batch in mutation_batch.rs/redb_store.rs"),
        ("AcquireCapacity", "dedicated engine-native capacity-cell CAS/lease transaction in dispatch.rs/redb_store/capacity_lease.rs"),
        ("RenewCapacity", "dedicated engine-native fenced capacity renewal transaction in dispatch.rs/redb_store/capacity_lease.rs"),
        ("ReleaseCapacity", "dedicated engine-native fenced capacity release transaction in dispatch.rs/redb_store/capacity_lease.rs"),
        ("ReclaimExpiredCapacity", "dedicated engine-native bounded expiry reclaim transaction in dispatch.rs/redb_store/capacity_lease.rs"),
        ("UpdateCapacityCell", "dedicated engine-native controller epoch-CAS transaction in dispatch.rs/redb_store/capacity_lease.rs"),
        ("RenewWorkItemLease", "dedicated engine-native MutationBatch lease-fence transition in mutation_batch.rs/redb_store.rs"),
        ("CommitWorkItemResult", "dedicated engine-native MutationBatch result/dependency transition in mutation_batch.rs/redb_store.rs"),
        ("CancelWorkItem", "dedicated engine-native MutationBatch pending-cancellation transition in mutation_batch.rs/redb_store.rs"),
        ("DeferWorkItem", "dedicated engine-native MutationBatch fenced deferral transition in mutation_batch.rs/redb_store.rs"),
        ("CasWorkItemMetadata", "dedicated engine-native MutationBatch scheduling-metadata CAS transition in mutation_batch.rs/redb_store.rs (BUG-111)"),
        ("ReserveWorkItemResources", "dedicated engine-native MutationBatch host-reservation transaction in mutation_batch.rs/redb_store.rs"),
        ("ReleaseWorkItemResources", "dedicated engine-native MutationBatch reservation-release transaction in mutation_batch.rs/redb_store.rs"),
        ("ReclaimWorkItemResources", "dedicated engine-native MutationBatch reservation-reclaim transaction in mutation_batch.rs/redb_store.rs"),
        ("UpdateResourceHost", "dedicated engine-native MutationBatch host-telemetry transaction in mutation_batch.rs/redb_store.rs"),
        // ── Non-graph-scoped stores: no GraphCore/graph_name in scope at all;
        // each self-routes in dispatch.rs BEFORE the per-graph chain, exactly
        // like Txn*, with its OWN dedicated redb file + commit path. ──
        ("BlobBegin", "native MutationBatch in blob.redb: durable upload cursor + status/fence/idempotency/outbox in one WTX"),
        ("BlobChunkPut", "native MutationBatch in blob.redb: chunk + upload cursor + coordinator metadata in one WTX"),
        ("BlobCommit", "native MutationBatch in blob.redb: manifest/cursor transition + coordinator metadata in one WTX"),
        ("BlobGc", "native MutationBatch in blob.redb: GC rows + exact result/coordinator metadata in one WTX"),
        ("BlobRef", "native MutationBatch in blob.redb: refcount + coordinator metadata in one WTX"),
        ("BlobUnref", "native MutationBatch in blob.redb: refcount + coordinator metadata in one WTX"),
        ("KvPut", "native MutationBatch in kv.redb: KV row + status/fence/idempotency/outbox in one WTX"),
        ("KvDelete", "native MutationBatch in kv.redb: KV row + status/fence/idempotency/outbox in one WTX"),
        ("KvCas", "native MutationBatch in kv.redb: CAS decision/row + exact result/coordinator metadata in one WTX"),
        ("TsAppend", "native MutationBatch in series.redb: series rows/projection + coordinator metadata in one WTX"),
        ("TsEvict", "self-routes via dispatch.rs's tsdb block to timeseries.rs, like TsAppend above; one series.redb WTX via SeriesStore::evict_before_scoped -- no eg_transaction idempotency batch, because retention is content-idempotent (re-evicting an already-past cutoff is a safe no-op), unlike TsAppend"),
        ("TsDeleteSeries", "self-routes via dispatch.rs's tsdb block to timeseries.rs, like TsAppend above; one series.redb WTX via SeriesStore::delete_scoped -- no eg_transaction idempotency batch, because deletion is content-idempotent (re-deleting an already-gone series is a safe no-op), unlike TsAppend"),
        #[cfg(feature = "jobs")]
        ("AnalyticsJob", "native MutationBatch in jobs.redb; asynchronous claim writeback uses a staged graph MutationBatch"),
        // `Statechart` self-routes in dispatch.rs BEFORE dispatch_graph_op (see the
        // `Method::Statechart` arm there and `handlers::statechart` module docs) --
        // it never reaches this gateway's `try_handle_gateway`/`commit_mutation` at
        // all. `eg-statechart`'s `StatechartStore::instantiate`/`send_event` commit
        // through `eg-transaction` (the SAME universal MutationBatch/OCC
        // primitive `eg-jobs` uses for `AnalyticsJob`, per eg-statechart/Cargo.toml)
        // against their OWN `statecharts.redb`, keyed by def_id/instance_id, not a
        // graph -- structurally identical to `AnalyticsJob` above, just gated
        // `statechart` instead of `jobs`. Note: this is `eg-transaction` (a
        // generic per-store OCC/durable-commit primitive shared by jobs/kv/blob/
        // series/statecharts), NOT this module's `GATEWAY_ROUTED`/`commit_mutation`
        // -- the two are easily conflated by name but are different mechanisms.
        #[cfg(feature = "statechart")]
        ("Statechart", "native MutationBatch in statecharts.redb; instance define/instantiate/send_event commit status/version/fence/idempotency/outbox in one WTX via eg-transaction, exactly like AnalyticsJob in jobs.redb"),
        // RF-020. `AgentLibrary` self-routes in router.rs's
        // `dispatch_agent_library_methods` BEFORE the per-graph chain and owns
        // its own `agent_library.redb`. `AgentLibraryStore` commits publish and
        // retire through `eg-transaction`'s `MutationKernel` -- the same
        // per-store OCC primitive `AnalyticsJob` and `Statechart` use -- so the
        // revision row, kernel receipt and outbox event land in one WTX.
        ("AgentLibrary", "native MutationBatch in agent_library.redb: entry revision + head + kernel receipt/outbox in one WTX via eg-transaction, exactly like AnalyticsJob in jobs.redb"),
        // RF-ADR-008. Published into the SAME `agent_library.redb` owner as
        // `AgentLibrary` -- an agent graph is a composition of agent entries,
        // not a separate entity family, so a second physical store would split
        // one authority in two (RF-RULING-004).
        ("AgentGraph", "native MutationBatch in agent_library.redb: graph revision + head + kernel receipt/outbox in one WTX via eg-transaction, exactly like AgentLibrary alongside it"),
        ("AgentComponent", "native MutationBatch in agent_library.redb: component revision + head + kernel receipt/outbox in one WTX via eg-transaction, alongside the agents and graphs that reference it"),
        ("AgentTemplate", "native MutationBatch in agent_library.redb: template revision + head + kernel receipt/outbox in one WTX via eg-transaction, alongside the components, agents and graphs its base is assembled from"),
        // RF-019. Commits into the semantic index's OWN `OwnerLayout::SemanticIndex`
        // owner through `eg-transaction`, never a graph shard: one WTX carries the
        // stage transition, its artifact, the successor intent, the generation
        // checkpoint and the outbox lease acknowledgement together, which is what
        // makes "a stage cannot complete before its predecessor" a durable property
        // rather than an ordering convention.
        ("SemanticIndex", "native MutationBatch in the OwnerLayout::SemanticIndex owner: binding/stage row + artifact + successor intent + lease ack in one WTX via eg-transaction, exactly like AnalyticsJob in jobs.redb"),
        ("ImportSqliteFile", "native SQL-catalog MutationBatch: all imported tables + exact result/coordinator metadata in one WTX"),
        ("SqlSourceBatch", "native SQL owner MutationBatch: typed source rows, provider checkpoint, committed epoch, terminal result, replay and outbox in one WTX; LocalOnly authority refuses active Raft"),
        // ── Process-global registries on ServerState: opaque control-redb sagas,
        // no GraphCore/graph_name; dispatched directly in the top-level match. ──
        ("CreateChannel", "opaque prepared/committed session-control MutationBatch"),
        ("JoinChannel", "opaque prepared/committed session-control MutationBatch"),
        ("LeaveChannel", "opaque prepared/committed session-control MutationBatch"),
        ("CloseChannel", "opaque prepared/committed session-control MutationBatch"),
        ("SendMessage", "request-scoped opaque session-control saga; a committed retry never duplicates delivery"),
        ("RegisterIdentity", "native rbac.redb MutationBatch shares the RBAC snapshot WTX"),
        ("RbacAdmin", "native rbac.redb MutationBatch shares the RBAC snapshot WTX"),
        ("RegisterForeignSource", "opaque prepared/committed session-control MutationBatch"),
        ("RegisterUdf", "opaque prepared/committed session-control MutationBatch"),
        ("RegisterContinuousQuery", "opaque prepared/committed session-control MutationBatch"),
        ("DropContinuousQuery", "opaque prepared/committed session-control MutationBatch"),
        ("RegisterTrigger", "opaque prepared/committed session-control MutationBatch"),
        ("DropTrigger", "opaque prepared/committed session-control MutationBatch"),
        ("CepSubscribe", "opaque prepared/committed session-control MutationBatch"),
        ("CepUnsubscribe", "opaque prepared/committed session-control MutationBatch"),
        // ── Cluster-wide / cross-shard admin ops: operate across the WHOLE
        // registry/cluster under a durable control-plane saga
        // (handlers::admin::try_handle / handlers::dist_compute::try_handle), not
        // a single resolved graph's core. ──
        ("Reshard", "prepared/committed admin MutationBatch saga around cluster-wide resharding"),
        ("CatalogAssign", "prepared/committed admin MutationBatch saga around the durable tenant catalog"),
        ("CatalogReassign", "prepared/committed admin MutationBatch saga around the durable tenant catalog"),
        ("CatalogRemove", "prepared/committed admin MutationBatch saga around the durable tenant catalog"),
        ("RebalanceExecute", "prepared/committed admin MutationBatch saga around cluster-wide rebalance"),
        ("Restore", "prepared/committed admin MutationBatch saga around online restore/PITR"),
        ("RaftAddLearner", "leader-only openraft add_learner via handlers::raft_admin::try_handle against MultiRaft directly -- no GraphCore/graph_name in scope, cluster-wide like Reshard/CatalogAssign above"),
        ("RaftChangeMembership", "leader-only openraft change_membership via handlers::raft_admin::try_handle against MultiRaft directly -- no GraphCore/graph_name in scope, cluster-wide like Reshard/CatalogAssign above"),
        // W2.5 fleet server registry: self-translates into `Method::AddNode` against
        // `__commons__` from its own top-level dispatch.rs arm (see the
        // `Method::RegisterServer` match), exactly like `ApplyMultisigMutation` above
        // translates into `Method::ApplyMutation` -- by the time a mutation happens the
        // method value has already become `AddNode`, so `RegisterServer` itself never
        // reaches `commit_mutation` directly. The durable write / audit line / CDC event
        // are AddNode's (already gateway-routed); `RegisterServer`'s OWN `audit_line`/
        // `emit_for_method` marker arms (mirroring `ApplyMultisigMutation`'s) are
        // defense-in-depth only, unreachable via this single-node delegation path.
        ("RegisterServer", "validates + computes the lease fields then TRANSLATES into a Method::AddNode dispatched through the ordinary dispatch_graph_op path against __commons__ (which IS gateway-routed) -- see dispatch.rs; by the time a mutation happens the method value has already become AddNode, so this variant itself never reaches commit_mutation directly"),
        ("PlacementAdmin", "raft-replicated placement-catalog admin op (Assign/Move/AbortMove); MultiRaft::placement_assign / TenantManager::move_partition / abort_move commit through the DEFAULT group's own client_write / commit_placement to the __placement_catalog__ control graph, not this gateway's per-graph MutationBatch"),
        ("CreateMatView", "prepared/committed control-plane MutationBatch saga around the durable cross-shard view row"),
        ("RefreshMatView", "prepared/committed control-plane MutationBatch saga around the durable cross-shard view row"),
        ("PlanMatViewDefine", "prepared/committed control-plane MutationBatch saga around the durable plan definition"),
        ("PlanMatViewRefresh", "prepared/committed control-plane MutationBatch saga around derived-cache refresh"),
        ("PlanMatViewDrop", "prepared/committed control-plane MutationBatch saga around durable definition removal"),
        // ── Registry-lifecycle ops: run BEFORE/ACROSS a single graph's core
        // exists (or spans many), so there is no one EXISTING graph to route
        // "through". ──
        ("CreateGraph", "native lifecycle MutationBatch commits graph identity before registry publication"),
        ("DeleteGraph", "native lifecycle MutationBatch commits purge before registry eviction"),
        ("MultiGraphBatchUpdate", "cluster placement fanout emits one typed graph command per child; standalone mode uses a durable parent saga"),
        ("ApplyChangeEnvelope", "governed envelope coordinator commits typed graph/object/provenance rows, cursor, version, and outbox through one native MutationBatch"),
        ("ApplyChangeEnvelopes", "batch envelope coordinator groups envelopes by graph and commits each graph's page as one coalesced native MutationBatch transaction; fans out per graph like MultiGraphBatchUpdate"),
        ("SourceIngest", "RF-ADR-009 stages raw CAS and an authoritative Connector Manifest mapping, then delegates its sole canonical graph/provenance/cursor commit to ApplyChangeEnvelope"),
        ("RecomputeMaterialization", "fenced reasoning-projection coordinator resolves authoritative graph provenance and fsyncs its projection watermark"),
        // ── Server lifecycle: TxnParticipation::None, not a graph mutation. ──
        ("Shutdown", "server-lifecycle control-plane action, not a graph mutation"),
        // ── Translates away before it could ever reach the gateway. ──
        (
            "ApplyMultisigMutation",
            "validates the multisig threshold then TRANSLATES into a Method::ApplyMutation \
             dispatched through the ordinary dispatch_graph_op path (which IS gateway-routed) -- \
             see dispatch.rs; by the time a mutation happens the method value has already become \
             ApplyMutation, so this variant itself never reaches commit_mutation directly",
        ),
        // ── Native WorkItem claim capabilities: dedicated private ledger, dispatch.rs's
        // own block just above the reservation-read guard ("Native WorkItem claim
        // capabilities use a dedicated private ledger and never enter MutationBatch/
        // result/outbox/CDC projections"). See access.rs's REASON_NATIVE_CAPABILITY_LEDGER
        // for the read-side twin (VerifyWorkItemClaimCapability). ──
        (
            "MintWorkItemClaimCapability",
            "dispatch.rs's dedicated WorkItem-claim-capability block routes this to \
             redb_store::work_item_capability::mint_work_item_claim_capability against its own \
             private ledger, keyed by an AuthenticatedAuthority derived from the verified request \
             context -- never enters MutationBatch/result/outbox/CDC projections, so it does not \
             fit commit_mutation's (ctx, plan, method, apply) shape at all",
        ),
        // ── DevelopmentLane*'s 6 write methods now route through the explicit
        // `handlers::development_lane::try_handle` owner, which commits through
        // `PersistenceBackend::commit_development_lane` -> the redb writer thread ->
        // `redb_store::development_lane::commit_development_lane` -- a self-contained
        // begin_write()/commit() against the native `development_lane_*` tables. Same posture
        // as `MintWorkItemClaimCapability` above: no MutationBatch/result/outbox/CDC
        // projection, so it does not fit commit_mutation's (ctx, plan, method, apply) shape.
        // Formerly NOT_YET_AUDITED-adjacent placeholder text (push/eg-merge-artifacts, commit
        // 174c381) said "no dispatch.rs wire-routing arm exists yet" -- that arm now exists;
        // see access.rs's REASON_NATIVE_DEVELOPMENT_LANE_READ for the read-side twin
        // (DevelopmentLaneStatus/QueryDevelopmentLane). ──
        ("ReserveDevelopmentLane", "handlers::development_lane::try_handle routes this to PersistenceBackend::commit_development_lane -> redb_store::development_lane::commit_development_lane, a self-contained redb transaction against the native development_lane_* tables -- never enters MutationBatch/result/outbox/CDC projections, same posture as MintWorkItemClaimCapability above"),
        ("RenewDevelopmentLane", "handlers::development_lane::try_handle routes this to PersistenceBackend::commit_development_lane -> redb_store::development_lane::commit_development_lane, a self-contained redb transaction against the native development_lane_* tables -- never enters MutationBatch/result/outbox/CDC projections, same posture as MintWorkItemClaimCapability above"),
        ("ObserveDevelopmentLane", "handlers::development_lane::try_handle routes this to PersistenceBackend::commit_development_lane -> redb_store::development_lane::commit_development_lane, a self-contained redb transaction against the native development_lane_* tables -- never enters MutationBatch/result/outbox/CDC projections, same posture as MintWorkItemClaimCapability above"),
        ("FinishDevelopmentLane", "handlers::development_lane::try_handle routes this to PersistenceBackend::commit_development_lane -> redb_store::development_lane::commit_development_lane, a self-contained redb transaction against the native development_lane_* tables -- never enters MutationBatch/result/outbox/CDC projections, same posture as MintWorkItemClaimCapability above"),
        ("CleanupDevelopmentLane", "handlers::development_lane::try_handle routes this to PersistenceBackend::commit_development_lane -> redb_store::development_lane::commit_development_lane, a self-contained redb transaction against the native development_lane_* tables -- never enters MutationBatch/result/outbox/CDC projections, same posture as MintWorkItemClaimCapability above"),
        ("UpdateDevelopmentLaneQuota", "handlers::development_lane::try_handle routes this to PersistenceBackend::commit_development_lane -> redb_store::development_lane::commit_development_lane, a self-contained redb transaction against the native development_lane_* tables -- never enters MutationBatch/result/outbox/CDC projections, same posture as MintWorkItemClaimCapability above"),
    ];

    /// Graph-scoped methods requiring a coordinator outside this gateway. The set is
    /// **EMPTY**: all former entries now have an owning mutation kernel --
    ///   - `Sql`/`CypherQuery`/`GraphQl` are now routed via
    ///     `commit_conditional_mutation_async` at the query dispatch site (the async
    ///     twin built for exactly the `state`/`rls`-needing surfaces);
    ///   - `AddTriples`/`RemoveTriples`/`DropNamedGraph` are routed the same way at
    ///     the RDF dispatch site, with multi-valued literals included in the staged
    ///     authoritative image;
    ///   - `RunRules` was AUDITED to be genuinely read-only, so the fix was the
    ///     policy (`eg_capabilities::policy(RunRules).mutates` : true -> false), not
    ///     a route -- it left the mutating surface entirely.
    ///
    /// Kept as an explicit (empty) const, and asserted empty below, so "nothing is
    /// silently deferred" stays a machine-checked invariant: any future mutating
    /// method that is routable-but-unrouted must be listed here (a hard, visible
    /// admission), never dropped into the untracked void.
    const OPEN_NOT_JUSTIFIED: &[(&str, &str)] = &[];

    /// (d) Bypass guard, part 2: the migration surface is machine-visible. Every
    /// name in [`GATEWAY_ROUTED`] really exists in `eg_capabilities::method_policy_entries()`
    /// and really is `mutates == true` (catches a rename/typo silently un-routing
    /// a method); the COMPLEMENT (every other mutating method) must fall ENTIRELY
    /// into [`NON_GATEWAY_COORDINATED`] or [`OPEN_NOT_JUSTIFIED`] -- an undocumented name in
    /// the complement is a hard test failure (a silent skip), not a warning.
    #[test]
    fn gateway_routed_set_matches_mutating_policy_surface() {
        use std::collections::BTreeSet;

        let all_mutating: BTreeSet<&'static str> = eg_capabilities::method_policy_entries()
            .filter(|(_, p, _)| p.mutates)
            .map(|(name, _, _)| name)
            .collect();

        for routed in GATEWAY_ROUTED {
            assert!(
                all_mutating.contains(routed),
                "GATEWAY_ROUTED name '{routed}' is not a mutating method in \
                 eg_capabilities::method_policy_entries() (renamed/typo'd?)"
            );
        }

        let routed_set: BTreeSet<&'static str> = GATEWAY_ROUTED.iter().copied().collect();
        let not_yet_migrated: Vec<&'static str> =
            all_mutating.difference(&routed_set).copied().collect();

        let coordinated: std::collections::HashMap<&'static str, &'static str> =
            NON_GATEWAY_COORDINATED.iter().copied().collect();
        let open: std::collections::HashMap<&'static str, &'static str> =
            OPEN_NOT_JUSTIFIED.iter().copied().collect();

        // Every NON_GATEWAY_COORDINATED / OPEN_NOT_JUSTIFIED entry must be a real,
        // currently-non-routed, mutating method name -- catches a stale doc entry
        // (a name that got routed, renamed, or was never mutating) as loudly as an
        // undocumented one.
        for name in coordinated.keys().chain(open.keys()) {
            assert!(
                not_yet_migrated.contains(name),
                "'{name}' is documented in NON_GATEWAY_COORDINATED/OPEN_NOT_JUSTIFIED but is NOT in the \
                 current non-gateway set (already routed, renamed, or never a mutating method?) -- stale entry"
            );
        }

        let undocumented: Vec<&&'static str> = not_yet_migrated
            .iter()
            .filter(|name| !coordinated.contains_key(*name) && !open.contains_key(*name))
            .collect();
        assert!(
            undocumented.is_empty(),
            "UNDOCUMENTED non-gateway entries (silently deferred, not allowed): {undocumented:?} -- \
             add each to NON_GATEWAY_COORDINATED (native coordinator) or OPEN_NOT_JUSTIFIED (honest \
             remainder) in server::mutation::tests"
        );
        assert_eq!(
            not_yet_migrated.len(),
            coordinated.len() + open.len(),
            "NON_GATEWAY_COORDINATED + OPEN_NOT_JUSTIFIED must exactly partition the non-gateway set"
        );
        // L11 close-out invariant: the OPEN (routable-but-unrouted) bucket is EMPTY.
        // Every non-routed mutating method has a concrete native coordinator,
        // translation, or pre-commit staging protocol; none is a durability bypass.
        assert!(
            open.is_empty(),
            "OPEN_NOT_JUSTIFIED must be empty (all routable methods are owned); entries: {:?}",
            open.keys().collect::<Vec<_>>()
        );

        println!(
            "EG-P0-2/L11 gateway migration surface: {} routed / {} mutating total; \
             {} non-gateway = {} natively coordinated + \
             {} open-not-justified (MUST be 0)",
            routed_set.len(),
            all_mutating.len(),
            not_yet_migrated.len(),
            coordinated.len(),
            open.len(),
        );
    }

    /// Clustered admission has no deferred mutation family. The union of the
    /// graph state-machine gateway, bounded native commands, specialized governed
    /// envelope/modality commands, self-routed admin handlers, and process
    /// shutdown exactly equals the current mutating capability ledger.
    #[cfg(feature = "raft")]
    #[test]
    fn clustered_mutation_inventory_is_complete() {
        use std::collections::BTreeSet;

        let expected: BTreeSet<&'static str> = eg_capabilities::method_policy_entries()
            .filter(|(_, policy, _)| policy.mutates)
            .map(|(name, _, _)| name)
            .collect();
        let mut covered: BTreeSet<&'static str> = GATEWAY_ROUTED.iter().copied().collect();
        covered.extend(crate::raft::NATIVE_CONSENSUS_METHODS.iter().copied());
        covered.extend(CONSENSUS_FANOUT_METHODS.iter().copied());
        covered.extend(SELF_ROUTED_ADMIN_METHODS.iter().copied());
        covered.extend(plan::LOCAL_ONLY_METHODS.iter().copied());
        covered.insert("ApplyChangeEnvelope");
        covered.insert("SourceIngest");
        covered.insert("ServedModality");
        covered.insert("Shutdown");
        covered.insert("KgDelegate");
        // RF-020's four agent layers are no longer inserted here: X7 gave them
        // the typed `LOCAL_ONLY_METHODS` classification they always had in
        // substance, so `plan::LOCAL_ONLY_METHODS` above already covers them
        // and a second, silent insert would hide a future regression in it.
        // RF-019. Self-routes in router.rs and commits into its own semantic
        // owner through `eg-transaction`, so it never reaches a
        // `NativeMutationCommand` or the gateway -- same reason as the four
        // agent layers above.
        covered.insert("SemanticIndex");
        covered.insert("PlacementAdmin");
        // Self-translates into a gateway-routed `Method::AddNode` before this
        // classifier ever runs on it -- see `cluster_mutation_route`'s
        // `RegisterServer` arm (`VolatileControl`) for the full explanation.
        covered.insert("RegisterServer");

        let missing: Vec<_> = expected.difference(&covered).copied().collect();
        let stale: Vec<_> = covered.difference(&expected).copied().collect();
        assert!(
            missing.is_empty() && stale.is_empty(),
            "cluster mutation inventory drift: missing={missing:?}, stale={stale:?}"
        );

        // The same discriminator explained on `SELF_ROUTED_ADMIN_METHODS`: NOT
        // ALSO present in `raft::NATIVE_CONSENSUS_METHODS`, or `cluster_mutation_route`
        // would be ambiguous about which route wins.
        let native_consensus: BTreeSet<&'static str> = crate::raft::NATIVE_CONSENSUS_METHODS
            .iter()
            .copied()
            .collect();
        for name in SELF_ROUTED_ADMIN_METHODS
            .iter()
            .chain(plan::LOCAL_ONLY_METHODS)
        {
            assert!(
                !native_consensus.contains(name),
                "'{name}' is both self-routed or local-only and in \
                 raft::NATIVE_CONSENSUS_METHODS -- cluster_mutation_route would be ambiguous"
            );
        }
    }

    /// `cluster_mutation_route` must classify each `SELF_ROUTED_ADMIN_METHODS`
    /// entry as `SelfRoutedAdmin`, never `ConsensusNative` -- the exact bug
    /// (`CLUSTER_MUTATION_UNAVAILABLE: no bounded native command exists`) that
    /// made `Method::RaftAddLearner`/`RaftChangeMembership` unreachable through
    /// dispatch even though `handlers::raft_admin::try_handle` fully implements
    /// them.
    #[test]
    fn self_routed_admin_methods_bypass_the_native_mutation_router() {
        assert_eq!(
            cluster_mutation_route(&Method::RaftAddLearner {
                group: None,
                node_id: 2,
                addr: "127.0.0.1:1".to_string(),
            }),
            ClusterMutationRoute::SelfRoutedAdmin,
        );
        assert_eq!(
            cluster_mutation_route(&Method::RaftChangeMembership {
                group: None,
                voters: vec![1, 2],
            }),
            ClusterMutationRoute::SelfRoutedAdmin,
        );
    }

    #[test]
    fn clustered_mutations_are_consensus_typed() {
        assert_eq!(
            cluster_mutation_route(&Method::AddNode {
                node_id: "opaque-node".to_string(),
                properties_msgpack: Vec::new(),
            }),
            ClusterMutationRoute::ConsensusGraph,
        );
        assert_eq!(
            cluster_mutation_route(&Method::CreateGraph {
                graph_name: "opaque-graph".to_string(),
                graph_type: GraphType::Global,
            }),
            ClusterMutationRoute::ConsensusNative,
        );
        assert_eq!(
            cluster_mutation_route(&Method::ApplyMutation {
                event_type: "opaque-event".to_string(),
                query: "opaque-operation".to_string(),
            }),
            ClusterMutationRoute::ConsensusNative,
        );
        #[cfg(feature = "sparql-http")]
        assert_eq!(
            cluster_mutation_route(&Method::ApplyMutation {
                event_type: crate::server::sparql_http::SPARQL_HTTP_UPDATE_EVENT.to_string(),
                query: "CLEAR DEFAULT".to_string(),
            }),
            ClusterMutationRoute::ConsensusFanout,
        );
        assert_eq!(
            cluster_mutation_route(&Method::MultiGraphBatchUpdate {
                batches_msgpack: Vec::new(),
            }),
            ClusterMutationRoute::ConsensusFanout,
        );
        assert_eq!(
            cluster_mutation_route(&Method::Shutdown),
            ClusterMutationRoute::VolatileControl,
        );
        assert_eq!(
            cluster_mutation_route(&Method::PlacementRoute {
                request: crate::epistemic_operations::PlacementRouteRequest {
                    schema_version:
                        crate::epistemic_operations::PlacementRouteRequestSchemaVersion::V1,
                    tenant_ref: "opaque-tenant".to_string(),
                    partition_ref: "opaque-partition".to_string(),
                    client_epoch: 0,
                },
            }),
            ClusterMutationRoute::ReadOnly,
        );
    }
}
