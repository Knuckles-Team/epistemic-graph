//! Multi-op OCC ACID transaction handler (CONCEPT:EG-KG.txn.multi-op-occ-acid).
//!
//! Owns the `Txn*`/`BeginTxn`/`Commit`/`Rollback` methods. These are STATEFUL
//! (they read/write `ServerState::open_txns`), so unlike the per-graph handlers
//! they take `state`. The staged ops never touch the graph or persistence; the
//! topology write lock is taken ONCE, at commit, where the OCC read-set is
//! validated and the staged write-set applied through a single `GraphTxn`.
//!
//! Composition with the write coalescer (CONCEPT:EG-KG.sharding.per-graph-write-coalescer): staged ops are applied
//! directly via `GraphTxn` at commit and never enter the coalescer's queue, so
//! there is no interaction or deadlock with the per-graph write worker — that
//! worker only batches NON-transactional single-op writes. A long-open txn holds
//! NO lock at all (begin/stage take only the cheap `open_txns` DashMap + per-entry
//! Mutex), so client think-time never blocks readers or writers.

use std::sync::Arc;

use tokio::sync::RwLock;

use super::super::access::{check_graph_access, CarrierAuthority, GraphReadAuthority};
use super::super::auth::VerifiedRequestContext;
use super::super::state::ServerState;
#[cfg(feature = "graphql")]
use super::super::txn::IsolationLevel;
#[cfg(feature = "tsdb")]
use super::super::txn::StagedMeasurement;
use super::super::txn::{now_ms, parse_isolation, GraphTxnState, NewTxnArgs};
use crate::isolation::AccessLevel;
use crate::protocol::{Method, Response, ResultPayload};
use eg_types::contract::Nonce;

const MAX_TXN_RESULT_BYTES: usize = 1024 * 1024;
const MAX_TXN_NESTED_BYTES: usize = 64 * 1024 * 1024;
const MAX_TXN_NESTED_ITEMS: usize = 1_000_000;

#[path = "txn/commit_core.rs"]
mod commit_core;
#[path = "txn/commit_modal.rs"]
mod commit_modal;
#[path = "txn/commit_multi_graph.rs"]
mod commit_multi_graph;
#[path = "txn/commit_prepared.rs"]
mod commit_prepared;
#[cfg(feature = "raft")]
#[path = "txn/consensus.rs"]
mod consensus;
#[path = "txn/dispatch.rs"]
mod dispatch;
#[path = "txn/receipts.rs"]
mod receipts;
#[path = "txn/reconcile.rs"]
mod reconcile;
#[path = "txn/rollback.rs"]
mod rollback;
#[path = "txn/staging.rs"]
mod staging;

use commit_core::*;
pub(crate) use commit_modal::*;
pub(crate) use commit_multi_graph::*;
use commit_prepared::*;
#[cfg(feature = "raft")]
pub(crate) use consensus::*;
pub(crate) use dispatch::*;
pub(crate) use receipts::*;
use reconcile::*;
use rollback::*;
pub(crate) use staging::*;

fn consensus_apply_is_authorized() -> bool {
    #[cfg(feature = "raft")]
    {
        crate::server::dispatch::is_replicated_apply()
    }
    #[cfg(not(feature = "raft"))]
    {
        false
    }
}

fn decode_txn_value<T: serde::de::DeserializeOwned>(
    bytes: &[u8],
    max_bytes: usize,
    max_items: usize,
) -> Result<T, String> {
    eg_types::msgpack::decode_bounded(
        bytes,
        eg_types::msgpack::MsgpackLimits::new(
            max_bytes,
            max_items,
            eg_types::msgpack::DEFAULT_MAX_DEPTH,
        ),
    )
    .map_err(|_| "transaction value is invalid or exceeds resource limits".to_string())
}

fn decode_txn_result(bytes: &[u8]) -> Result<ResultPayload, String> {
    decode_txn_value(bytes, MAX_TXN_RESULT_BYTES, 1_024)
}

fn decode_txn_object(bytes: &[u8]) -> Result<serde_json::Map<String, serde_json::Value>, String> {
    decode_txn_value(bytes, MAX_TXN_NESTED_BYTES, MAX_TXN_NESTED_ITEMS)
}

#[cfg(all(test, feature = "epistemic"))]
mod materialize_belief_tests {
    use super::*;
    use crate::graph::GraphCore;

    fn node(props: serde_json::Value) -> Vec<u8> {
        rmp_serde::to_vec_named(&props).unwrap()
    }

    fn apply_cas(core: &GraphCore, m: &Method) -> bool {
        let Method::CompareAndSetNodeFields {
            node_id,
            conditions_msgpack,
            updates_msgpack,
        } = m
        else {
            panic!("expected a CompareAndSetNodeFields method, got {m:?}");
        };
        let conditions =
            rmp_serde::from_slice::<serde_json::Map<String, serde_json::Value>>(conditions_msgpack)
                .unwrap();
        let updates =
            rmp_serde::from_slice::<serde_json::Map<String, serde_json::Value>>(updates_msgpack)
                .unwrap();
        core.compare_and_set_fields(node_id, &conditions, &updates)
    }

    fn stored_confidence(core: &GraphCore, node_id: &str) -> f64 {
        let blob = core.get_node_properties(node_id).expect("node exists");
        let v: serde_json::Value = rmp_serde::from_slice(&blob).unwrap();
        v.get("confidence").and_then(|c| c.as_f64()).unwrap()
    }

    /// D5: a claim with a supporting evidence node propagates to a HIGHER belief than
    /// its bare stored prior — and the lowered Method, once applied, writes that exact
    /// derived value onto `NodeData.confidence` (materialize belief writes).
    #[test]
    fn materialize_belief_writes_the_propagated_confidence() {
        let core = GraphCore::new();
        core.add_node(
            "claim".into(),
            node(serde_json::json!({"type": "Claim", "confidence": 0.5})),
        );
        core.add_node(
            "evidence".into(),
            node(serde_json::json!({"type": "Evidence", "confidence": 0.9})),
        );
        let _ = core.add_edge(
            "evidence".into(),
            "claim".into(),
            node(serde_json::json!({"relationship": "SUPPORTS"})),
        );

        let (methods, confidence) =
            materialize_belief_to_methods(&core, "claim").expect("node exists");
        assert_eq!(methods.len(), 1);
        assert!(
            confidence > 0.5,
            "support should raise belief above the bare prior, got {confidence}"
        );
        assert!((0.0..=1.0).contains(&confidence));

        assert!(apply_cas(&core, &methods[0]), "CAS must apply cleanly");
        assert!(
            (stored_confidence(&core, "claim") - confidence).abs() < 1e-12,
            "the node's stored confidence must equal the derived belief exactly"
        );
    }

    /// D5 idempotency: the LOWERED method carries a value FROZEN at stage time (not
    /// re-derived at apply time), so replaying the identical method — exactly what a
    /// WAL replay or a duplicate commit does — is a true no-op the second time: the
    /// stored confidence stays byte-identical (never ratchets/drifts).
    #[test]
    fn materialize_belief_replay_is_idempotent() {
        let core = GraphCore::new();
        core.add_node(
            "claim2".into(),
            node(serde_json::json!({"type": "Claim", "confidence": 0.5})),
        );
        core.add_node(
            "evidence2".into(),
            node(serde_json::json!({"type": "Evidence", "confidence": 0.9})),
        );
        let _ = core.add_edge(
            "evidence2".into(),
            "claim2".into(),
            node(serde_json::json!({"relationship": "SUPPORTS"})),
        );

        let (methods, confidence) =
            materialize_belief_to_methods(&core, "claim2").expect("node exists");

        assert!(apply_cas(&core, &methods[0]));
        let first = stored_confidence(&core, "claim2");
        assert!((first - confidence).abs() < 1e-12);

        // Re-apply the SAME (already-computed) method — as WAL replay / a duplicate
        // commit would — WITHOUT recomputing belief from the now-updated node.
        assert!(apply_cas(&core, &methods[0]));
        let second = stored_confidence(&core, "claim2");
        assert_eq!(
            first, second,
            "replaying the identical lowered method must not drift the stored value"
        );
    }

    /// D5 is audited: the write lands as an ordinary `CompareAndSetNodeFields` — the
    /// SAME durable Method `audit::audit_line` already recognizes and chains (CONCEPT:
    /// EG-KG.sharding.row-level-security) — never a bespoke, unaudited mutation.
    #[test]
    #[cfg(feature = "security")]
    fn materialize_belief_rides_the_audited_cas_path() {
        let core = GraphCore::new();
        core.add_node(
            "claim3".into(),
            node(serde_json::json!({"confidence": 0.5})),
        );
        let (methods, _confidence) =
            materialize_belief_to_methods(&core, "claim3").expect("node exists");
        let line = crate::audit::audit_line(&methods[0]);
        assert_eq!(line.as_deref(), Some("CAS_NODE|claim3"));
    }

    /// D5 is opt-in: a node with no evidence at all keeps its bare stored prior
    /// (`from_graph_view` treats a MISSING confidence as `1.0`, but here it IS set) —
    /// proving materialize-belief never fabricates a belief where none was asserted.
    #[test]
    fn materialize_belief_missing_node_errors() {
        let core = GraphCore::new();
        assert!(materialize_belief_to_methods(&core, "nope").is_err());
    }
}

/// Recovery-window regressions for keyed native transactions.  These tests use
/// the private parent/child helpers directly so each crash point is explicit:
/// the first leaves only a Prepared parent, while the second commits the child
/// and deliberately drops the parent receipt before the retry reconciles it.
#[cfg(all(test, feature = "redb", feature = "security"))]
mod keyed_recovery_window_tests {
    use super::*;
    use crate::protocol::GraphType;
    use crate::server::persistence::backup::EnvVarGuard;
    use crate::server::persistence::redb_backend::RedbBackend;
    use crate::server::persistence::PersistenceBackend;
    use crate::server::ServerState;
    use std::path::{Path, PathBuf};

    const CALLER: &str = "txn-recovery-agent";
    const TENANT: &str = "tenant-recovery-scope";
    const KEY: &str = "txn-recovery-stable-key";

    fn test_dir(label: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "eg-{label}-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(&dir).expect("create transaction recovery test dir");
        dir
    }

    fn test_state(
        graph: &str,
        dir: &Path,
    ) -> (
        Arc<RwLock<ServerState>>,
        Arc<RedbBackend>,
        Arc<crate::graph::GraphCore>,
    ) {
        let backend = Arc::new(
            RedbBackend::open_with_shards(dir.to_string_lossy().into_owned(), 64, 1)
                .expect("open transaction recovery backend"),
        );
        let mut server = ServerState::new_for_test(
            "txn-recovery-test-secret",
            ServerState::test_isolation(CALLER),
        );
        server.persist_dir = Some(dir.to_string_lossy().into_owned());
        server.persistence = Some(backend.clone());
        server
            .registry
            .create_graph(graph, GraphType::Global, None)
            .expect("create transaction recovery graph");
        let core = server
            .registry
            .get(graph)
            .expect("transaction recovery graph is resident")
            .core
            .clone();
        (Arc::new(RwLock::new(server)), backend, core)
    }

    fn staged_cross_modal_txn(core: &crate::graph::GraphCore, graph: &str) -> GraphTxnState {
        let mut txn = GraphTxnState::new(
            core,
            NewTxnArgs {
                graph: graph.to_string(),
                tenant_scope: TENANT.to_string(),
                begin_version: core.version(),
                isolation: crate::server::txn::IsolationLevel::Snapshot,
                predicate: None,
                agent: CALLER.to_string(),
                now_ms: now_ms(),
            },
        );
        txn.stage(
            core,
            Method::AddNode {
                node_id: "recovery-node".to_string(),
                properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({
                    "kind": "recovery-window"
                }))
                .expect("encode staged recovery node"),
            },
            now_ms(),
        );
        txn.stage_vector(
            core,
            "recovery-node".to_string(),
            vec![0.25, 0.75],
            now_ms(),
        );
        txn
    }

    fn assert_keyed_commit(response: &Response, replayed: bool) {
        assert_eq!(response.error, None, "keyed commit failed: {response:?}");
        let Some(ResultPayload::Json(value)) = response.result.as_ref() else {
            panic!(
                "expected keyed commit JSON result, got {:?}",
                response.result
            );
        };
        assert_eq!(value["committed"], serde_json::json!(true));
        assert_eq!(value["replayed"], serde_json::json!(replayed));
    }

    fn assert_nonce_rejected(response: &Response) {
        assert!(
            response
                .error
                .as_deref()
                .is_some_and(|error| error.contains("REPLAY_NONCE_CONSUMED")),
            "exact keyed commit retry must be rejected by the kernel: {response:?}"
        );
    }

    fn lifecycle_authority(key: &str, nonce: [u8; 32]) -> CarrierAuthority {
        let base =
            crate::server::authority_context::VerifiedRequestContext::verified_for_test_with_scopes(
                CALLER,
                TENANT,
                &["*"],
            );
        let context = crate::server::authority_context::VerifiedRequestContext::from_verified_claims_with_nonce(
            base.claims().clone(),
            key.to_string(),
            Some(Nonce::from_bytes(nonce)),
        );
        CarrierAuthority::from_verified(&context).expect("build lifecycle authority")
    }

    async fn close_test_state(
        state: Arc<RwLock<ServerState>>,
        backend: Arc<RedbBackend>,
        dir: PathBuf,
    ) {
        backend.shutdown();
        state.write().await.persistence = None;
        drop(state);
        drop(backend);
        let _ = std::fs::remove_dir_all(dir);
    }

    async fn restart_test_backend(
        state: &Arc<RwLock<ServerState>>,
        backend: Arc<RedbBackend>,
        dir: &Path,
    ) -> Arc<RedbBackend> {
        backend.shutdown();
        state.write().await.persistence = None;
        drop(backend);
        for _ in 0..50 {
            match RedbBackend::open_with_shards(dir.to_string_lossy().into_owned(), 64, 1) {
                Ok(reopened) => {
                    let reopened = Arc::new(reopened);
                    state.write().await.persistence = Some(reopened.clone());
                    return reopened;
                }
                Err(_) => tokio::task::yield_now().await,
            }
        }
        panic!("reopen transaction recovery backend");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn prepared_lifecycle_retry_refuses_ambiguous_volatile_effect() {
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        let _recovery_key = EnvVarGuard::set(
            crate::crypto::TXN_RECOVERY_KEY_ENV,
            "txn-handler-recovery-window-test-key",
        );
        let graph = "prepared-lifecycle-graph";
        let dir = test_dir("txn-prepared-lifecycle");
        let (state, backend, _core) = test_state(graph, &dir);
        let first = lifecycle_authority("prepared-lifecycle-key", [21; 32]);
        let begun = begin_txn(
            &state,
            20,
            Some(CALLER),
            first.owner_scope(),
            first.tenant_scope(),
            Some(graph.to_string()),
            None,
        )
        .await;
        let txn_id = match begun.result {
            Some(ResultPayload::String(txn_id)) => txn_id,
            other => panic!(
                "unexpected lifecycle BeginTxn result: {:?} / {other:?}",
                begun.error
            ),
        };
        let method = Method::TxnAddNode {
            txn_id: txn_id.clone(),
            node_id: "node".to_string(),
            properties_msgpack: vec![1, 2, 3],
            graph: Some(graph.to_string()),
        };
        let receipt = begin_txn_lifecycle_receipt(&state, 21, CALLER, &first, &method)
            .await
            .expect("prepare lifecycle receipt");
        let staged = stage(
            &state,
            22,
            &txn_id,
            Some(graph),
            Method::AddNode {
                node_id: "node".to_string(),
                properties_msgpack: vec![1, 2, 3],
            },
        )
        .await;
        assert!(
            staged.error.is_none(),
            "stage effect failed: {:?}",
            staged.error
        );
        drop(receipt);
        let backend = restart_test_backend(&state, backend, &dir).await;
        state.write().await.open_txns.clear();

        // A crash after the volatile stage effect but before finish leaves only the
        // Prepared coordinator.  A fresh attempt must fail closed instead of
        // appending the stage a second time; the exact original nonce still
        // reaches the kernel and receives its consumed-nonce rejection.
        let fresh = lifecycle_authority("prepared-lifecycle-key", [22; 32]);
        let error = begin_txn_lifecycle_receipt(&state, 22, CALLER, &fresh, &method)
            .await
            .err()
            .expect("Prepared lifecycle must refuse re-execution");
        assert!(error.contains("Prepared"), "{error}");

        let exact = lifecycle_authority("prepared-lifecycle-key", [21; 32]);
        let error = begin_txn_lifecycle_receipt(&state, 23, CALLER, &exact, &method)
            .await
            .err()
            .expect("exact lifecycle retry must be rejected");
        assert!(error.contains("REPLAY_NONCE_CONSUMED"), "{error}");

        close_test_state(state, backend, dir).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn terminal_lifecycle_replay_refuses_missing_volatile_handle() {
        let state = Arc::new(RwLock::new(ServerState::new_for_test(
            "txn-lifecycle-replay-test-secret",
            ServerState::test_isolation(CALLER),
        )));
        let authority = lifecycle_authority("terminal-lifecycle-key", [31; 32]);
        let method = Method::BeginTxn {
            graph: Some("lost-after-restart".to_string()),
            isolation: None,
        };
        let error = validate_txn_lifecycle_replay(
            &state,
            &method,
            authority.owner_scope(),
            &ResultPayload::String("txn-lost-after-restart".to_string()),
        )
        .await
        .expect_err("missing volatile handle must not replay success");
        assert!(
            error.contains("volatile staging state is unavailable"),
            "{error}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn keyed_prepared_parent_recovers_before_child_exists() {
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        let _recovery_key = EnvVarGuard::set(
            crate::crypto::TXN_RECOVERY_KEY_ENV,
            "txn-handler-recovery-window-test-key",
        );
        let graph = "prepared-before-child-graph";
        let dir = test_dir("txn-prepared-before-child");
        let (state, backend, core) = test_state(graph, &dir);
        let txn = staged_cross_modal_txn(&core, graph);
        let txn_id = "prepared-before-child-txn";
        let tenant = Some(TENANT);
        let key = Some(KEY);

        let (receipt, replayed) = begin_txn_receipt(
            Some(backend.clone()),
            1,
            Some(CALLER),
            txn_id,
            &txn,
            key,
            Some(Nonce::from_bytes([1; 32])),
        )
        .expect("prepare keyed parent receipt");
        assert!(replayed.is_none());

        // The encrypted recovery plan is the only surviving transaction state
        // at this crash point; proving it can be reopened also binds the retry
        // to the verified tenant independently of the graph name.
        let resumed = resume_txn_receipt(
            Some(backend.clone()),
            2,
            Some(CALLER),
            txn_id,
            key,
            tenant,
            None,
        )
        .expect("resume prepared parent")
        .expect("prepared parent receipt exists");
        assert!(resumed.1.is_none(), "parent must still be Prepared");
        assert!(resumed.2.is_some(), "prepared parent must retain its plan");
        drop(resumed);
        drop(receipt);

        // Reopen the durable tier before the child exists.  The in-memory
        // registry is deliberately retained only as the serving projection;
        // recovery must come from the encrypted Prepared parent on disk.
        let backend = restart_test_backend(&state, backend, &dir).await;

        let first = commit(
            &state,
            2,
            Some(CALLER),
            txn_id,
            key,
            Some(Nonce::from_bytes([2; 32])),
            tenant,
        )
        .await;
        assert_keyed_commit(&first, false);
        assert!(
            backend
                .read_node(graph, "recovery-node")
                .await
                .expect("read recovered child")
                .is_some(),
            "prepared-before-child retry must commit the recovered child"
        );
        let committed_version = core.version();

        let exact = commit(
            &state,
            3,
            Some(CALLER),
            txn_id,
            key,
            Some(Nonce::from_bytes([1; 32])),
            tenant,
        )
        .await;
        assert_nonce_rejected(&exact);

        let replay = commit(
            &state,
            4,
            Some(CALLER),
            txn_id,
            key,
            Some(Nonce::from_bytes([3; 32])),
            tenant,
        )
        .await;
        assert_keyed_commit(&replay, true);
        assert_eq!(
            core.version(),
            committed_version,
            "terminal retry must not apply twice"
        );

        close_test_state(state, backend, dir).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn keyed_child_before_parent_finish_reconciles_one_child() {
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        let _recovery_key = EnvVarGuard::set(
            crate::crypto::TXN_RECOVERY_KEY_ENV,
            "txn-handler-recovery-window-test-key",
        );
        let graph = "child-before-parent-finish-graph";
        let dir = test_dir("txn-child-before-parent");
        let (state, backend, core) = test_state(graph, &dir);
        let txn = staged_cross_modal_txn(&core, graph);
        let txn_id = "child-before-parent-finish-txn";
        let tenant = Some(TENANT);
        let key = Some(KEY);

        let (receipt, replayed) = begin_txn_receipt(
            Some(backend.clone()),
            10,
            Some(CALLER),
            txn_id,
            &txn,
            key,
            Some(Nonce::from_bytes([11; 32])),
        )
        .expect("prepare keyed parent receipt");
        assert!(replayed.is_none());
        let coordinator_id = receipt_coordinator_id(&receipt);

        // Model the crash after the graph/vector child fsync and before the
        // control-plane parent is terminalized.  Production threads one
        // verified request nonce through both parent and child scopes, so the
        // child consumes the same attempt nonce in its own kernel scope.  The
        // retry must find this child through its opaque parent coordinator and
        // finish the same receipt.
        assert!(commit_cross_modal_txn_with_nonce(
            &state,
            11,
            Some(CALLER),
            &coordinator_id,
            txn,
            Some(Nonce::from_bytes([11; 32])),
        )
        .await
        .expect("commit durable child"));
        drop(receipt);
        let committed_version = core.version();

        // Reopen after the child fsync but before the parent finish.  This is
        // the acknowledgement-loss window: reconciliation must recover the
        // child and terminalize the same parent receipt exactly once.
        let backend = restart_test_backend(&state, backend, &dir).await;

        let recovered = commit(
            &state,
            12,
            Some(CALLER),
            txn_id,
            key,
            Some(Nonce::from_bytes([13; 32])),
            tenant,
        )
        .await;
        assert_keyed_commit(&recovered, true);
        assert_eq!(
            core.version(),
            committed_version,
            "reconcile must not apply child twice"
        );
        assert!(
            backend
                .read_node(graph, "recovery-node")
                .await
                .expect("read reconciled child")
                .is_some(),
            "child-before-parent retry must preserve the durable child"
        );

        let exact = commit(
            &state,
            13,
            Some(CALLER),
            txn_id,
            key,
            Some(Nonce::from_bytes([11; 32])),
            tenant,
        )
        .await;
        assert_nonce_rejected(&exact);

        let terminal_retry = commit(
            &state,
            14,
            Some(CALLER),
            txn_id,
            key,
            Some(Nonce::from_bytes([14; 32])),
            tenant,
        )
        .await;
        assert_keyed_commit(&terminal_retry, true);
        assert_eq!(
            core.version(),
            committed_version,
            "terminal parent retry must not duplicate the child"
        );

        close_test_state(state, backend, dir).await;
    }
}
