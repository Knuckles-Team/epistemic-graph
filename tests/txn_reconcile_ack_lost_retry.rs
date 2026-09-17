//! Ack-lost `Commit` retry reconciliation (CONCEPT:EG-KG.txn.multi-op-occ-acid),
//! covering the W1d hot-path fix in `handlers::txn::reconcile_committed_txn`.
//!
//! When a client's `Commit` response is lost after the transaction already
//! committed durably, the retried `Commit { txn_id }` finds nothing in
//! `open_txns` (the first attempt already consumed it) and falls through to
//! `reconcile_committed_txn`, which durably re-discovers the committed child and
//! replays the SAME terminal result. That reconcile path previously walked every
//! resident graph x {"txn","crossmodal"} namespace fully sequentially before
//! giving up — this test registers several resident graphs so a retry must, in
//! the pre-fix code, pay several serialized durable round-trips before finding
//! the one graph that actually committed. It proves the fanned-out lookup still
//! returns the exact same terminal result as the original (uncontested) commit,
//! for both the graph that DID commit and, transitively, that unrelated resident
//! graphs don't change the outcome.
//!
//! Driven through the REAL `dispatch` shell over an in-process `ServerState`
//! backed by a `RedbBackend` (persistence present), exactly as a client.

#![cfg(feature = "redb")]
// Boxing every `dispatch(...)` future keeps it out of the test bodies' layouts,
// but rustc still lays out the dispatch future itself in THIS crate. Under the
// `cluster` feature set that future nests past the default query depth
// ("queries overflow the depth limit!"), exactly as for the service binaries
// that drive `dispatch` (src/main.rs, src/bin/nemesis.rs), which set the same
// crate-root limit.
#![recursion_limit = "256"]

mod common;
#[path = "common/test_support.rs"]
mod test_support;

use epistemic_graph::acl::RequestContextClaims;
use epistemic_graph::mutation_batch::MutationBatchStatus;
use epistemic_graph::protocol::{GraphType, Method, Request, Response, ResultPayload};
use epistemic_graph::server::{compute_verified_envelope_token, dispatch, VerifiedEnvelopeParams};
use std::process::Command;

const SECRET: &str = "txn-reconcile-ack-lost-secret";
const FAULT_GRAPH: &str = "txn-signed-fault-graph";
const FAULT_COMMIT_KEY: &str = "txn-signed-fault-commit-key";
const FAULT_COMMIT_NONCE: &str = "txn-signed-fault-commit-nonce";
const FAULT_CHILD_ENV: &str = "E3_SIGNED_COMMIT_FAULT_CHILD";
const FAULT_PHASE_ENV: &str = "E3_SIGNED_COMMIT_FAULT_PHASE";
const FAULT_DIR_ENV: &str = "E3_SIGNED_COMMIT_FAULT_DIR";
const FAULT_ARMED_MARKER_FILE: &str = "e3-signed-commit-fault-armed";
const LIFECYCLE_CHILD_ENV: &str = "E3_SIGNED_LIFECYCLE_FAULT_CHILD";
const LIFECYCLE_MODE_ENV: &str = "E3_SIGNED_LIFECYCLE_FAULT_MODE";
const LIFECYCLE_DIR_ENV: &str = "E3_SIGNED_LIFECYCLE_FAULT_DIR";
const LIFECYCLE_ARMED_MARKER_FILE: &str = "e3-signed-lifecycle-fault-armed";
const LIFECYCLE_GRAPH: &str = "txn-signed-lifecycle-fault-graph";
const LIFECYCLE_BEGIN_KEY: &str = "txn-signed-lifecycle-begin-key";
const LIFECYCLE_BEGIN_NONCE: &str = "txn-signed-lifecycle-begin-nonce";
const LIFECYCLE_STAGE_KEY: &str = "txn-signed-lifecycle-stage-key";
const LIFECYCLE_STAGE_NONCE: &str = "txn-signed-lifecycle-stage-nonce";
const LIFECYCLE_TXN_ID_FILE: &str = "e3-lifecycle-txn-id";

// Every test in this integration binary that opens a durable backend more than
// once must keep the process-global encryption configuration stable across the
// entire restart window.  The child fault tests run in separate processes, so
// this lock covers the parent-side tests that can otherwise race each other.
static TEST_ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn pack(value: serde_json::Value) -> Vec<u8> {
    rmp_serde::to_vec_named(&value).unwrap()
}

/// The fixed integration-test JWT tenant claim `common::signed_request` embeds
/// (`tests/common/mod.rs`'s private `TEST_TENANT`). Mirrored here (not exported)
/// so this test can derive the SAME opaque `CarrierAuthority::tenant_scope()` a
/// verified request produces.
const TEST_TENANT_CLAIM: &str = "integration-test-tenant";

/// Reproduce `server::mutation_batch::opaque_coordinator_key` (crate-private) so
/// this integration test can predict the exact `CarrierAuthority::tenant_scope()`
/// a signed request resolves to: `opaque_coordinator_key("carrier-tenant",
/// "verified", <tenant claim>)`. The single-graph OCC commit path stamps a
/// durable `MutationBatch.tenant` with this same value (CONCEPT:
/// EG-KG.txn.multi-op-occ-acid). The test deliberately keeps that tenant scope
/// distinct from its target graph: recovery must compare the child batch against
/// the verified tenant, not incorrectly require `batch.tenant == batch.graph`.
fn opaque_coordinator_key(namespace: &str, graph: &str, coordinator_id: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut digest = Sha256::new();
    digest.update(namespace.as_bytes());
    digest.update([0]);
    digest.update(graph.as_bytes());
    digest.update([0]);
    digest.update(coordinator_id.as_bytes());
    format!("{namespace}:{}", hex::encode(digest.finalize()))
}

fn carrier_tenant_scope(raw_tenant: &str) -> String {
    opaque_coordinator_key("carrier-tenant", "verified", raw_tenant)
}

/// Reproduce `handlers::txn::receipts::commit_receipt_id` (crate-private) for
/// the keyed signed Commit: the durable parent receipt id is the tenant-scoped
/// operation id `transaction-receipt:<digest>` prefixed by its verifiable tenant
/// binding, `<transaction-receipt-scope:digest>:<operation id>`. The cross-modal
/// child batch id is derived from this WHOLE id, so a mirror that stops at the
/// operation id names a batch that never exists.
fn fault_parent_id() -> String {
    let tenant = carrier_tenant_scope(TEST_TENANT_CLAIM);
    let scoped_key =
        opaque_coordinator_key("transaction-receipt-tenant", &tenant, FAULT_COMMIT_KEY);
    let operation_id = opaque_coordinator_key("transaction-receipt", "idempotency", &scoped_key);
    let binding = opaque_coordinator_key("transaction-receipt-scope", &tenant, &operation_id);
    format!("{binding}:{operation_id}")
}

/// The cross-modal child batch id `reconcile_committed_txn` looks up for the
/// fault parent (`opaque_coordinator_key("crossmodal", graph, parent id)`).
fn fault_child_id() -> String {
    opaque_coordinator_key("crossmodal", FAULT_GRAPH, &fault_parent_id())
}

/// Durable status of the fault transaction's cross-modal child batch, or `None`
/// when no child batch was committed under [`fault_child_id`].
async fn durable_fault_child_status(
    backend: &test_support::SharedPersistence,
) -> Option<MutationBatchStatus> {
    backend
        .read_mutation_batch(FAULT_GRAPH, &fault_child_id())
        .await
        .expect("inspect the durable cross-modal child batch")
        .map(|record| record.status)
}

fn inspect_admin_recovery_counts(
    backend: &test_support::SharedPersistence,
    persist_dir: &std::path::Path,
    label: &str,
) -> (u64, u64) {
    let destination = persist_dir.join(format!(".e3-proof-{label}"));
    let report = backend
        .as_redb()
        .expect("fault proof requires the concrete redb backend")
        .backup(&destination, env!("CARGO_PKG_VERSION"), 0, label, &[])
        .expect("read the durable admin coordinator census");
    let counts = (
        report.admin_mutations.prepared,
        report.admin_mutations.committed,
    );
    std::fs::remove_dir_all(destination).expect("remove the temporary proof bundle");
    counts
}

fn assert_fault_abort(status: std::process::ExitStatus, label: &str) {
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        assert_eq!(
            status.signal(),
            Some(6),
            "{label} fault child must terminate by SIGABRT, not by panic or another exit: {status:?}"
        );
    }
    #[cfg(not(unix))]
    assert!(
        !status.success(),
        "{label} fault child must fail at the armed boundary"
    );
}

fn signed_fixed_request(
    id: u64,
    graph: &str,
    method: Method,
    nonce: &str,
    idempotency_key: &str,
) -> Request {
    common::configure_authority();
    let context = RequestContextClaims {
        principal: common::TEST_AGENT.to_string(),
        tenant: TEST_TENANT_CLAIM.to_string(),
        audience: "epistemic-graph-integration-tests".to_string(),
        agent_id: common::TEST_AGENT.to_string(),
        roles: Vec::new(),
        scopes: vec!["*".to_string()],
        policy_version: "integration-test-policy-v1".to_string(),
        delegation: Vec::new(),
        node: None,
        priority: None,
    };
    let mut request = Request {
        id,
        graph: graph.to_string(),
        auth_token: String::new(),
        agent_id: Some(common::TEST_AGENT.to_string()),
        method,
    };
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock is after the Unix epoch")
        .as_secs();
    request.auth_token = compute_verified_envelope_token(
        SECRET,
        &request,
        &VerifiedEnvelopeParams {
            context: &context,
            timestamp,
            nonce,
            idempotency_key,
        },
    );
    request
}

fn certification_fault_spec(request_id: u64, phase: &str) -> String {
    serde_json::json!({
        "schema_version": 1,
        "nonce": "0000000000000000000000000000000000000000000000000000000000000000",
        "request_id": request_id,
        "domain": "cross_modal",
        "phase": phase,
    })
    .to_string()
}

#[tokio::test]
async fn commit_retry_after_ack_loss_reconciles_across_resident_graphs() {
    let _env_lock = TEST_ENV_LOCK.lock().await;
    let dir = test_support::fresh_dir("eg-txn-reconcile");
    let dir_s = dir.to_string_lossy().to_string();

    // The single/multi-graph OCC `Commit` receipt is sealed via the transaction
    // recovery cipher (CONCEPT:EG-KG.txn.multi-op-occ-acid), so a durable Commit
    // requires an encryption key configured. This test is the only one in this
    // binary, so setting the process-global env var here is race-free.
    std::env::set_var(
        epistemic_graph::crypto::ENCRYPTION_KEY_ENV,
        "txn-reconcile-ack-lost-retry-test-key",
    );

    let backend = test_support::open_redb_backend(dir_s.clone()).unwrap();
    let state = test_support::state_with(
        SECRET,
        common::current_isolation(),
        Some(dir_s.clone()),
        Some(backend.clone()),
    );

    // Register several resident graphs so the retry's reconcile walk actually has
    // more than one (graph x namespace) candidate to consider — the target graph
    // is deliberately the LAST one registered/committed-to. Its name is distinct
    // from the opaque tenant scope a signed request resolves to.
    let target = "txn-reconcile-distinct-graph".to_string();
    let tenant_scope = carrier_tenant_scope(TEST_TENANT_CLAIM);
    assert_ne!(target, tenant_scope);
    let graphs: Vec<String> = vec![
        "gragone".to_string(),
        "gragtwo".to_string(),
        "gragthree".to_string(),
        target.clone(),
    ];
    for graph in &graphs {
        let cr: Response = Box::pin(dispatch(
            &state,
            test_support::request(
                SECRET,
                1,
                graph,
                Method::CreateGraph {
                    graph_name: graph.clone(),
                    graph_type: GraphType::Global,
                },
            ),
        ))
        .await;
        assert!(
            cr.error.is_none(),
            "CreateGraph {graph} failed: {:?}",
            cr.error
        );
    }
    let target = target.as_str();

    // begin → stage → commit on the target graph, exactly as a normal client.
    let begun: Response = Box::pin(dispatch(
        &state,
        test_support::request(
            SECRET,
            10,
            target,
            Method::BeginTxn {
                graph: Some(target.to_string()),
                isolation: None,
            },
        ),
    ))
    .await;
    assert!(begun.error.is_none(), "BeginTxn failed: {:?}", begun.error);
    let txn_id = match begun.result {
        Some(ResultPayload::String(value)) => value,
        other => panic!("unexpected BeginTxn result shape: {other:?}"),
    };

    let staged: Response = Box::pin(dispatch(
        &state,
        test_support::request(
            SECRET,
            11,
            target,
            Method::TxnAddNode {
                txn_id: txn_id.clone(),
                node_id: "n1".to_string(),
                properties_msgpack: pack(serde_json::json!({"kind": "widget"})),
                graph: None,
            },
        ),
    ))
    .await;
    assert!(
        staged.error.is_none(),
        "TxnAddNode failed: {:?}",
        staged.error
    );

    let first_commit: Response = Box::pin(dispatch(
        &state,
        test_support::request(
            SECRET,
            12,
            target,
            Method::Commit {
                txn_id: txn_id.clone(),
                idempotency_key: Some("txn-reconcile-stable-key".to_string()),
            },
        ),
    ))
    .await;
    assert!(
        first_commit.error.is_none(),
        "first Commit failed: {:?}",
        first_commit.error
    );
    assert!(
        matches!(
            first_commit.result.as_ref(),
            Some(ResultPayload::Json(value))
                if value.get("committed") == Some(&serde_json::Value::Bool(true))
                    && value.get("replayed") == Some(&serde_json::Value::Bool(false))
        ),
        "first keyed Commit should report a fresh success: {:?} / {:?}",
        first_commit.result,
        first_commit.error
    );
    let target_core = {
        let state_guard = state.read().await;
        state_guard
            .registry
            .get(target)
            .expect("target graph remains resident")
            .core
            .clone()
    };
    let version_after_first = target_core.version();

    // The first Commit already consumed `open_txns[txn_id]`. Reopen the durable
    // backend before retrying so this covers the crash/restart path as well as a
    // lost response: the registry remains the serving projection, while the
    // receipt and committed child are read from the reopened authority.
    // `shutdown()` stops the writer thread but does NOT close the redb
    // `Database` -- that happens only when the last owning Arc drops, and redb
    // holds its advisory per-file lock until then. `state.persistence` still held
    // a clone here, so the reopen could never succeed; the old bounded RETRY just
    // turned that permanent leak into "Database already open. Cannot acquire
    // lock." two seconds later. Clear the state's clone, hand the last one over,
    // and let the helper prove exclusivity before opening.
    state.write().await.persistence = None;
    let restarted_backend = test_support::reopen_after_sole_reference(
        backend,
        || test_support::open_redb_backend(dir_s.clone()),
        "reopen txn reconcile backend",
    )
    .await;
    state.write().await.persistence = Some(restarted_backend.clone());

    // Simulate the client never having seen that response (an ack-lost retry) by
    // resending the IDENTICAL keyed Commit request. This is the path that used to
    // scan every resident graph x namespace fully sequentially before finding
    // `target`.
    let retried_commit: Response = Box::pin(dispatch(
        &state,
        test_support::request(
            SECRET,
            13,
            target,
            Method::Commit {
                txn_id: txn_id.clone(),
                idempotency_key: Some("txn-reconcile-stable-key".to_string()),
            },
        ),
    ))
    .await;
    assert!(
        retried_commit.error.is_none(),
        "retried Commit (ack-lost reconcile) failed: {:?}",
        retried_commit.error
    );
    assert!(
        matches!(
            retried_commit.result.as_ref(),
            Some(ResultPayload::Json(value))
                if value.get("committed") == Some(&serde_json::Value::Bool(true))
                    && value.get("replayed") == Some(&serde_json::Value::Bool(true))
        ),
        "retried keyed Commit must replay the SAME terminal result as the original commit: {:?} / {:?}",
        retried_commit.result,
        retried_commit.error
    );
    assert_eq!(
        target_core.version(),
        version_after_first,
        "ack-lost keyed replay must not apply the child write a second time"
    );

    restarted_backend.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Child process for [`signed_dispatch_commit_fault_windows_recover_parent_once`].
/// The certification hook aborts the process in the actual signed Commit route,
/// after the durable parent prepare and either before the child commit or after
/// the child commit but before the parent finish. Running this as a separate
/// process makes the abort an actual restart boundary rather than a private
/// helper-level simulation.
///
/// Not a test: the parent test re-executes its own test binary with
/// `FAULT_CHILD_ENV` set, and that run of the parent enters here instead of the
/// parent body (the `src/raft/xshard_harness.rs` pattern). It never returns: the
/// armed boundary aborts the process, and anything else panics.
async fn signed_dispatch_commit_fault_child() {
    let dir_s = std::env::var(FAULT_DIR_ENV)
        .expect("fault dir from parent harness; run signed_dispatch_commit_fault_windows_recover_parent_once instead of setting the child env by hand");
    let phase = std::env::var(FAULT_PHASE_ENV).expect("fault phase from parent harness");
    std::env::set_var(
        epistemic_graph::crypto::ENCRYPTION_KEY_ENV,
        "txn-signed-commit-fault-test-key",
    );
    std::env::set_var(
        "EPISTEMIC_GRAPH_CERTIFICATION_FAULT",
        certification_fault_spec(405, &phase),
    );

    let backend = test_support::open_redb_backend(dir_s.clone()).unwrap();
    let state = test_support::state_with(
        SECRET,
        common::current_isolation(),
        Some(dir_s.clone()),
        Some(backend),
    );
    let created: Response = Box::pin(dispatch(
        &state,
        test_support::request(
            SECRET,
            401,
            FAULT_GRAPH,
            Method::CreateGraph {
                graph_name: FAULT_GRAPH.to_string(),
                graph_type: GraphType::Global,
            },
        ),
    ))
    .await;
    assert!(
        created.error.is_none(),
        "CreateGraph failed: {:?}",
        created.error
    );

    let begin = Method::BeginTxn {
        graph: Some(FAULT_GRAPH.to_string()),
        isolation: None,
    };
    let begun: Response = Box::pin(dispatch(
        &state,
        signed_fixed_request(
            402,
            FAULT_GRAPH,
            begin,
            "txn-signed-fault-begin-nonce",
            "txn-signed-fault-begin-key",
        ),
    ))
    .await;
    let txn_id = match begun.result {
        Some(ResultPayload::String(txn_id)) => txn_id,
        other => panic!(
            "unexpected fault BeginTxn result: {:?} / {other:?}",
            begun.error
        ),
    };
    let staged_node: Response = Box::pin(dispatch(
        &state,
        signed_fixed_request(
            403,
            FAULT_GRAPH,
            Method::TxnAddNode {
                txn_id: txn_id.clone(),
                node_id: "signed-fault-node".to_string(),
                properties_msgpack: pack(serde_json::json!({"kind": "signed-fault"})),
                graph: None,
            },
            "txn-signed-fault-node-nonce",
            "txn-signed-fault-node-key",
        ),
    ))
    .await;
    assert!(
        staged_node.error.is_none(),
        "fault TxnAddNode failed: {:?}",
        staged_node.error
    );
    let staged_embedding: Response = Box::pin(dispatch(
        &state,
        signed_fixed_request(
            404,
            FAULT_GRAPH,
            Method::TxnAddEmbedding {
                txn_id: txn_id.clone(),
                node_id: "signed-fault-node".to_string(),
                embedding: vec![0.25, 0.75],
                graph: None,
            },
            "txn-signed-fault-vector-nonce",
            "txn-signed-fault-vector-key",
        ),
    ))
    .await;
    assert!(
        staged_embedding.error.is_none(),
        "fault TxnAddEmbedding failed: {:?}",
        staged_embedding.error
    );

    // This marker is written only after the signed setup requests succeeded and
    // immediately before the exact Commit whose kernel boundary is armed.  The
    // parent therefore proves both that the child reached the intended dispatch
    // and that its termination was the certification abort, rather than a setup
    // panic or an unrelated nonzero exit.
    std::fs::write(
        std::path::Path::new(&dir_s).join(FAULT_ARMED_MARKER_FILE),
        format!("armed:{phase}:405\n"),
    )
    .expect("write signed Commit fault armed marker");

    // The request id is bound into the signed carrier and into the batch. The
    // exact certification phase therefore selects this Commit child boundary,
    // while the parent lifecycle prepare remains durable.
    let _ = Box::pin(dispatch(
        &state,
        signed_fixed_request(
            405,
            FAULT_GRAPH,
            Method::Commit {
                txn_id,
                idempotency_key: Some(FAULT_COMMIT_KEY.to_string()),
            },
            FAULT_COMMIT_NONCE,
            FAULT_COMMIT_KEY,
        ),
    ))
    .await;
    panic!("certification fault {phase} did not abort the signed Commit child");
}

/// Enter the child-process path when this test binary was re-executed by the
/// parent. Keeping the environment branch here leaves the already-complex
/// behavioral test unchanged under the diff-scoped complexity gate.
async fn enter_signed_dispatch_commit_fault_child_if_requested() {
    if std::env::var_os(FAULT_CHILD_ENV).is_some() {
        signed_dispatch_commit_fault_child().await;
        unreachable!("the signed Commit fault child aborts or panics");
    }
}

/// Drive the real signed dispatch route through both durable commit fault
/// windows. A fresh retry must recover the prepared parent, produce exactly one
/// child effect, and then replay the same terminal receipt; reusing the original
/// commit nonce must still be rejected by the kernel.
#[tokio::test]
async fn signed_dispatch_commit_fault_windows_recover_parent_once() {
    enter_signed_dispatch_commit_fault_child_if_requested().await;
    let _env_lock = TEST_ENV_LOCK.lock().await;
    for (label, phase, replayed_on_recovery) in [
        ("before-child", "before_commit", false),
        (
            "child-before-parent-finish",
            "after_commit_before_ack",
            true,
        ),
    ] {
        let dir = test_support::fresh_dir(&format!("eg-txn-signed-fault-{label}"));
        let dir_s = dir.to_string_lossy().into_owned();
        let child = Command::new(std::env::current_exe().expect("integration test executable"))
            .arg("--exact")
            .arg("signed_dispatch_commit_fault_windows_recover_parent_once")
            .arg("--nocapture")
            .env(FAULT_CHILD_ENV, "1")
            .env(FAULT_PHASE_ENV, phase)
            .env(FAULT_DIR_ENV, &dir_s)
            .status()
            .expect("spawn signed Commit fault child");
        assert_fault_abort(child, phase);
        assert_eq!(
            std::fs::read_to_string(std::path::Path::new(&dir_s).join(FAULT_ARMED_MARKER_FILE))
                .expect("signed Commit fault child must leave its armed marker"),
            format!("armed:{phase}:405\n"),
            "fault child must arm the requested signed Commit boundary before aborting"
        );

        std::env::set_var(
            epistemic_graph::crypto::ENCRYPTION_KEY_ENV,
            "txn-signed-commit-fault-test-key",
        );
        let backend = test_support::reopen_with_bounded_retry(
            || test_support::open_redb_backend(dir_s.clone()),
            "reopen signed Commit fault backend",
        )
        .await;
        let state = test_support::state_with(
            SECRET,
            common::current_isolation(),
            Some(dir_s.clone()),
            Some(backend.clone()),
        );
        let (prepared_before_retry, _) =
            inspect_admin_recovery_counts(&backend, &dir, &format!("{label}-before"));
        assert_eq!(
            prepared_before_retry, 1,
            "{phase} restart must expose exactly one durable Prepared transaction parent"
        );
        let child_status = durable_fault_child_status(&backend).await;
        match phase {
            "before_commit" => assert_eq!(
                child_status, None,
                "before_commit abort must leave no durable child batch"
            ),
            "after_commit_before_ack" => assert_eq!(
                child_status,
                Some(MutationBatchStatus::Committed),
                "after_commit_before_ack abort must leave the durable committed child"
            ),
            other => panic!("unexpected certification phase {other}"),
        }
        // Install the serving projection AT THE DURABLE VERSION, the way a real
        // restart does.
        //
        // `Registry::create_graph` passes `source_snapshot_version = 0`, so the
        // fresh core's OCC counter starts at 0 while the authority is already at
        // the version the pre-fault `CreateGraph` committed. Recovery then bumps
        // the counter RELATIVELY (`mark_dirty` -> `fetch_add(1)`) and lands one
        // behind the truth, while the terminal replay re-syncs it ABSOLUTELY
        // (`install_committed_snapshot` -> `version.store(committed_version)`)
        // and jumps to the truth -- so the "terminal replay must not duplicate
        // the child" comparison below measured the fixture's own version drift,
        // not a duplicate write. Adopt the durable version first and the two
        // paths agree, exactly as they do in a served process.
        let durable_version = backend
            .read_mutation_graph_version(FAULT_GRAPH)
            .await
            .expect("read the durable graph version for the restarted projection")
            .unwrap_or(0);
        {
            let mut guard = state.write().await;
            guard
                .registry
                .create_graph(FAULT_GRAPH, GraphType::Global, None)
                .expect("install graph serving projection after restart");
            if durable_version > 0 {
                guard
                    .registry
                    .get(FAULT_GRAPH)
                    .expect("restarted projection is resident")
                    .core
                    .adopt_materialized_version(durable_version)
                    .expect("a restarted projection adopts the authoritative version");
            }
        }

        let recovered: Response = Box::pin(dispatch(
            &state,
            signed_fixed_request(
                406,
                FAULT_GRAPH,
                Method::Commit {
                    txn_id: "txn-signed-fault-txn".to_string(),
                    idempotency_key: Some(FAULT_COMMIT_KEY.to_string()),
                },
                "txn-signed-fault-retry-nonce",
                FAULT_COMMIT_KEY,
            ),
        ))
        .await;
        assert_eq!(
            recovered.error, None,
            "{phase} recovery failed: {recovered:?}"
        );
        let Some(ResultPayload::Json(value)) = recovered.result.as_ref() else {
            panic!("expected keyed recovery result for {phase}: {recovered:?}");
        };
        assert_eq!(value["committed"], serde_json::json!(true));
        assert_eq!(
            value["replayed"],
            serde_json::json!(replayed_on_recovery),
            "fault phase {phase} recovery replay marker changed"
        );
        let (prepared_after_recovery, committed_after_recovery) =
            inspect_admin_recovery_counts(&backend, &dir, &format!("{label}-after"));
        assert_eq!(
            prepared_after_recovery, 0,
            "{phase} recovery must terminalize the previously inspected parent"
        );
        assert!(
            committed_after_recovery >= 1,
            "{phase} recovery must leave a durable terminal parent receipt"
        );
        // Both windows end with the one committed child under the id recovery
        // derives from the durable parent. This also proves the pre-retry
        // `None` of the before_commit window inspected the real child id rather
        // than an id that could never exist.
        assert_eq!(
            durable_fault_child_status(&backend).await,
            Some(MutationBatchStatus::Committed),
            "{phase} recovery must leave the committed cross-modal child of the durable parent"
        );

        let core = state
            .read()
            .await
            .registry
            .get(FAULT_GRAPH)
            .expect("recovered graph projection")
            .core
            .clone();
        assert!(
            core.get_node_properties("signed-fault-node").is_some(),
            "{phase} recovery must publish the child exactly once"
        );
        let version_after_recovery = core.version();

        let terminal_retry: Response = Box::pin(dispatch(
            &state,
            signed_fixed_request(
                407,
                FAULT_GRAPH,
                Method::Commit {
                    txn_id: "txn-signed-fault-txn".to_string(),
                    idempotency_key: Some(FAULT_COMMIT_KEY.to_string()),
                },
                "txn-signed-fault-terminal-retry-nonce",
                FAULT_COMMIT_KEY,
            ),
        ))
        .await;
        assert_eq!(
            terminal_retry.error, None,
            "terminal retry failed: {terminal_retry:?}"
        );
        assert!(
            matches!(
                terminal_retry.result.as_ref(),
                Some(ResultPayload::Json(value))
                    if value["committed"] == serde_json::json!(true)
                        && value["replayed"] == serde_json::json!(true)
            ),
            "terminal retry must replay the keyed terminal result: {terminal_retry:?}"
        );
        assert_eq!(
            core.version(),
            version_after_recovery,
            "{phase} terminal replay must not duplicate the child"
        );

        let exact_original: Response = Box::pin(dispatch(
            &state,
            signed_fixed_request(
                408,
                FAULT_GRAPH,
                Method::Commit {
                    txn_id: "txn-signed-fault-txn".to_string(),
                    idempotency_key: Some(FAULT_COMMIT_KEY.to_string()),
                },
                FAULT_COMMIT_NONCE,
                FAULT_COMMIT_KEY,
            ),
        ))
        .await;
        assert!(
            exact_original
                .error
                .as_deref()
                .is_some_and(|error| error.contains("REPLAY_NONCE_CONSUMED")),
            "{phase} exact original nonce must be rejected: {exact_original:?}"
        );

        backend.shutdown();
        state.write().await.persistence = None;
        drop(state);
        let _ = std::fs::remove_dir_all(&dir);
    }
}

/// Child process for [`signed_dispatch_lifecycle_fault_windows_refuse_ambiguous_replay`].
/// The dedicated lifecycle hook aborts after the signed Begin/Stage volatile
/// effect and before the saga terminal receipt, leaving a real Prepared row.
///
/// Not a test: the parent test re-executes its own test binary with
/// `LIFECYCLE_CHILD_ENV` set, and that run of the parent enters here instead of
/// the parent body (the `src/raft/xshard_harness.rs` pattern). It never returns:
/// the armed boundary aborts the process, and anything else panics.
async fn signed_dispatch_lifecycle_fault_child() {
    let dir_s = std::env::var(LIFECYCLE_DIR_ENV)
        .expect("lifecycle fault dir from parent harness; run signed_dispatch_lifecycle_fault_windows_refuse_ambiguous_replay instead of setting the child env by hand");
    let mode = std::env::var(LIFECYCLE_MODE_ENV).expect("lifecycle fault mode from parent harness");
    std::env::set_var(
        epistemic_graph::crypto::ENCRYPTION_KEY_ENV,
        "txn-signed-lifecycle-fault-test-key",
    );
    let backend = test_support::open_redb_backend(dir_s.clone()).unwrap();
    let state = test_support::state_with(
        SECRET,
        common::current_isolation(),
        Some(dir_s.clone()),
        Some(backend),
    );
    let created: Response = Box::pin(dispatch(
        &state,
        test_support::request(
            SECRET,
            490,
            LIFECYCLE_GRAPH,
            Method::CreateGraph {
                graph_name: LIFECYCLE_GRAPH.to_string(),
                graph_type: GraphType::Global,
            },
        ),
    ))
    .await;
    assert!(
        created.error.is_none(),
        "CreateGraph failed: {:?}",
        created.error
    );

    if mode == "begin" {
        std::env::set_var("EPISTEMIC_GRAPH_LIFECYCLE_EFFECT_FAULT_REQUEST_ID", "501");
        std::fs::write(
            std::path::Path::new(&dir_s).join(LIFECYCLE_ARMED_MARKER_FILE),
            "armed:begin:501\n",
        )
        .expect("write signed lifecycle Begin fault armed marker");
        let _ = Box::pin(dispatch(
            &state,
            signed_fixed_request(
                501,
                LIFECYCLE_GRAPH,
                Method::BeginTxn {
                    graph: Some(LIFECYCLE_GRAPH.to_string()),
                    isolation: None,
                },
                LIFECYCLE_BEGIN_NONCE,
                LIFECYCLE_BEGIN_KEY,
            ),
        ))
        .await;
    } else {
        let begun: Response = Box::pin(dispatch(
            &state,
            signed_fixed_request(
                502,
                LIFECYCLE_GRAPH,
                Method::BeginTxn {
                    graph: Some(LIFECYCLE_GRAPH.to_string()),
                    isolation: None,
                },
                "txn-signed-lifecycle-stage-begin-nonce",
                "txn-signed-lifecycle-stage-begin-key",
            ),
        ))
        .await;
        let txn_id = match begun.result {
            Some(ResultPayload::String(txn_id)) => txn_id,
            other => panic!(
                "unexpected lifecycle Stage Begin: {:?} / {other:?}",
                begun.error
            ),
        };
        std::fs::write(
            std::path::Path::new(&dir_s).join(LIFECYCLE_TXN_ID_FILE),
            &txn_id,
        )
        .expect("persist lifecycle stage handle for restart harness");
        std::env::set_var("EPISTEMIC_GRAPH_LIFECYCLE_EFFECT_FAULT_REQUEST_ID", "503");
        std::fs::write(
            std::path::Path::new(&dir_s).join(LIFECYCLE_ARMED_MARKER_FILE),
            "armed:stage:503\n",
        )
        .expect("write signed lifecycle Stage fault armed marker");
        let _ = Box::pin(dispatch(
            &state,
            signed_fixed_request(
                503,
                LIFECYCLE_GRAPH,
                Method::TxnAddNode {
                    txn_id,
                    node_id: "lifecycle-fault-node".to_string(),
                    properties_msgpack: pack(serde_json::json!({"kind": "lifecycle-fault"})),
                    graph: None,
                },
                LIFECYCLE_STAGE_NONCE,
                LIFECYCLE_STAGE_KEY,
            ),
        ))
        .await;
    }
    panic!("lifecycle effect fault did not abort the signed {mode} dispatch");
}

/// Enter the lifecycle child-process path when selected by the parent test.
/// The normal parent path returns immediately and acquires the shared env lock.
async fn enter_signed_dispatch_lifecycle_fault_child_if_requested() {
    if std::env::var_os(LIFECYCLE_CHILD_ENV).is_some() {
        signed_dispatch_lifecycle_fault_child().await;
        unreachable!("the signed lifecycle fault child aborts or panics");
    }
}

/// Drive signed Begin and Stage through the real prepare → volatile effect →
/// finish boundary. After the child aborts, a fresh nonce must refuse the
/// ambiguous Prepared saga and the consumed original nonce must still fail in
/// the kernel; neither retry may silently repeat a volatile effect.
#[tokio::test]
async fn signed_dispatch_lifecycle_fault_windows_refuse_ambiguous_replay() {
    enter_signed_dispatch_lifecycle_fault_child_if_requested().await;
    let _env_lock = TEST_ENV_LOCK.lock().await;
    for mode in ["begin", "stage"] {
        let dir = test_support::fresh_dir(&format!("eg-txn-signed-lifecycle-fault-{mode}"));
        let dir_s = dir.to_string_lossy().into_owned();
        let child = Command::new(std::env::current_exe().expect("integration test executable"))
            .arg("--exact")
            .arg("signed_dispatch_lifecycle_fault_windows_refuse_ambiguous_replay")
            .arg("--nocapture")
            .env(LIFECYCLE_CHILD_ENV, "1")
            .env(LIFECYCLE_MODE_ENV, mode)
            .env(LIFECYCLE_DIR_ENV, &dir_s)
            .status()
            .expect("spawn signed lifecycle fault child");
        assert_fault_abort(child, mode);
        let expected_marker = if mode == "begin" {
            "armed:begin:501\n"
        } else {
            "armed:stage:503\n"
        };
        assert_eq!(
            std::fs::read_to_string(std::path::Path::new(&dir_s).join(LIFECYCLE_ARMED_MARKER_FILE))
                .expect("signed lifecycle fault child must leave its armed marker"),
            expected_marker,
            "lifecycle child must arm the requested effect boundary before aborting"
        );

        std::env::set_var(
            epistemic_graph::crypto::ENCRYPTION_KEY_ENV,
            "txn-signed-lifecycle-fault-test-key",
        );
        let backend = test_support::reopen_with_bounded_retry(
            || test_support::open_redb_backend(dir_s.clone()),
            "reopen signed lifecycle fault backend",
        )
        .await;
        let state = test_support::state_with(
            SECRET,
            common::current_isolation(),
            Some(dir_s.clone()),
            Some(backend.clone()),
        );
        let (prepared_before_retry, _) =
            inspect_admin_recovery_counts(&backend, &dir, &format!("lifecycle-{mode}-before"));
        assert_eq!(
            prepared_before_retry, 1,
            "signed lifecycle {mode} abort must leave exactly one durable Prepared receipt"
        );
        state
            .write()
            .await
            .registry
            .create_graph(LIFECYCLE_GRAPH, GraphType::Global, None)
            .expect("install lifecycle graph serving projection after restart");

        let (method, original_nonce, key, fresh_id, exact_id) = if mode == "begin" {
            (
                Method::BeginTxn {
                    graph: Some(LIFECYCLE_GRAPH.to_string()),
                    isolation: None,
                },
                LIFECYCLE_BEGIN_NONCE,
                LIFECYCLE_BEGIN_KEY,
                504,
                505,
            )
        } else {
            let txn_id =
                std::fs::read_to_string(std::path::Path::new(&dir_s).join(LIFECYCLE_TXN_ID_FILE))
                    .expect("signed Stage child must leave its transaction handle")
                    .trim()
                    .to_string();
            (
                Method::TxnAddNode {
                    txn_id,
                    node_id: "lifecycle-fault-node".to_string(),
                    properties_msgpack: pack(serde_json::json!({"kind": "lifecycle-fault"})),
                    graph: None,
                },
                LIFECYCLE_STAGE_NONCE,
                LIFECYCLE_STAGE_KEY,
                506,
                507,
            )
        };
        let fresh: Response = Box::pin(dispatch(
            &state,
            signed_fixed_request(
                fresh_id,
                LIFECYCLE_GRAPH,
                method.clone(),
                "fresh-lifecycle-nonce",
                key,
            ),
        ))
        .await;
        assert!(
            fresh.error.as_deref().is_some_and(|error| {
                error.contains("transaction lifecycle receipt is Prepared")
                    && error.contains("volatile staging state is unavailable")
                    && error.contains("refusing to re-execute")
            }),
            "fresh {mode} retry must refuse the exact ambiguous Prepared effect: {fresh:?}"
        );
        let (prepared_after_retry, _) =
            inspect_admin_recovery_counts(&backend, &dir, &format!("lifecycle-{mode}-after"));
        assert_eq!(
            prepared_after_retry, 1,
            "refusing an ambiguous {mode} retry must leave its Prepared receipt intact"
        );

        let exact: Response = Box::pin(dispatch(
            &state,
            signed_fixed_request(exact_id, LIFECYCLE_GRAPH, method, original_nonce, key),
        ))
        .await;
        assert!(
            exact
                .error
                .as_deref()
                .is_some_and(|error| error.contains("REPLAY_NONCE_CONSUMED")),
            "exact original {mode} nonce must be rejected: {exact:?}"
        );

        backend.shutdown();
        state.write().await.persistence = None;
        drop(state);
        let _ = std::fs::remove_dir_all(&dir);
    }
}

#[tokio::test]
async fn native_lifecycle_reopen_refuses_stale_begin_and_stage_success() {
    let _env_lock = TEST_ENV_LOCK.lock().await;
    let dir = test_support::fresh_dir("eg-txn-lifecycle-restart");
    let dir_s = dir.to_string_lossy().to_string();
    std::env::set_var(
        epistemic_graph::crypto::ENCRYPTION_KEY_ENV,
        "txn-lifecycle-restart-test-key",
    );

    let backend = test_support::open_redb_backend(dir_s.clone()).unwrap();
    let state = test_support::state_with(
        SECRET,
        common::current_isolation(),
        Some(dir_s.clone()),
        Some(backend.clone()),
    );
    let target = "txn-lifecycle-restart-graph";
    let created: Response = Box::pin(dispatch(
        &state,
        test_support::request(
            SECRET,
            100,
            target,
            Method::CreateGraph {
                graph_name: target.to_string(),
                graph_type: GraphType::Global,
            },
        ),
    ))
    .await;
    assert!(
        created.error.is_none(),
        "CreateGraph failed: {:?}",
        created.error
    );

    let begin = Method::BeginTxn {
        graph: Some(target.to_string()),
        isolation: None,
    };
    let first_begin: Response = Box::pin(dispatch(
        &state,
        signed_fixed_request(
            101,
            target,
            begin.clone(),
            "native-lifecycle-begin-nonce-1",
            "native-lifecycle-begin-key",
        ),
    ))
    .await;
    let _first_txn = match first_begin.result {
        Some(ResultPayload::String(txn_id)) => txn_id,
        other => panic!(
            "unexpected BeginTxn result: {:?} / {other:?}",
            first_begin.error
        ),
    };

    // Reopen the durable tier and discard the in-process handle, exactly as a
    // process restart does.  The durable lifecycle receipt must not turn into
    // a dead txn id on a fresh stable-key retry.
    // Dropping the LOCAL handle is not enough: `state.persistence` holds another
    // clone, and redb keeps its per-file lock until the last one goes. Clearing
    // it first is what makes the reopen possible at all -- the old bounded RETRY
    // around the open could only ever report the resulting permanent
    // "Database already open. Cannot acquire lock." after 2s of retrying.
    {
        let mut guard = state.write().await;
        guard.open_txns.clear();
        guard.persistence = None;
    }
    let reopened = test_support::reopen_after_sole_reference(
        backend,
        || test_support::open_redb_backend(dir_s.clone()),
        "reopen native lifecycle backend",
    )
    .await;
    state.write().await.persistence = Some(reopened.clone());

    let stale_begin: Response = Box::pin(dispatch(
        &state,
        signed_fixed_request(
            102,
            target,
            begin.clone(),
            "native-lifecycle-begin-nonce-2",
            "native-lifecycle-begin-key",
        ),
    ))
    .await;
    assert!(
        stale_begin
            .error
            .as_deref()
            .is_some_and(|error| error.contains("volatile staging state is unavailable")),
        "restarted Begin must refuse a stale success: {:?}",
        stale_begin.error
    );
    let exact_begin: Response = Box::pin(dispatch(
        &state,
        signed_fixed_request(
            103,
            target,
            begin,
            "native-lifecycle-begin-nonce-1",
            "native-lifecycle-begin-key",
        ),
    ))
    .await;
    assert!(
        exact_begin
            .error
            .as_deref()
            .is_some_and(|error| error.contains("REPLAY_NONCE_CONSUMED")),
        "exact Begin retry must remain a kernel nonce rejection: {:?}",
        exact_begin.error
    );

    // Create one new live handle, terminalize a stage, then repeat the same
    // restart loss for Stage.  The stable-key receipt cannot claim a staged
    // write succeeded when its volatile write-set is gone.
    let second_begin: Response = Box::pin(dispatch(
        &state,
        signed_fixed_request(
            104,
            target,
            Method::BeginTxn {
                graph: Some(target.to_string()),
                isolation: None,
            },
            "native-lifecycle-begin-nonce-3",
            "native-lifecycle-begin-key-2",
        ),
    ))
    .await;
    let second_txn = match second_begin.result {
        Some(ResultPayload::String(txn_id)) => txn_id,
        other => panic!(
            "unexpected second BeginTxn result: {:?} / {other:?}",
            second_begin.error
        ),
    };
    let stage = Method::TxnAddNode {
        txn_id: second_txn.clone(),
        node_id: "restart-stage-node".to_string(),
        properties_msgpack: pack(serde_json::json!({"kind": "restart-stage"})),
        graph: None,
    };
    let first_stage: Response = Box::pin(dispatch(
        &state,
        signed_fixed_request(
            105,
            target,
            stage.clone(),
            "native-lifecycle-stage-nonce-1",
            "native-lifecycle-stage-key",
        ),
    ))
    .await;
    assert!(
        first_stage.error.is_none(),
        "TxnAddNode failed: {:?}",
        first_stage.error
    );
    state.write().await.open_txns.clear();

    let stale_stage: Response = Box::pin(dispatch(
        &state,
        signed_fixed_request(
            106,
            target,
            stage.clone(),
            "native-lifecycle-stage-nonce-2",
            "native-lifecycle-stage-key",
        ),
    ))
    .await;
    assert!(
        stale_stage
            .error
            .as_deref()
            .is_some_and(|error| error.contains("volatile staging state is unavailable")),
        "restarted Stage must refuse a stale success: {:?}",
        stale_stage.error
    );
    let exact_stage: Response = Box::pin(dispatch(
        &state,
        signed_fixed_request(
            107,
            target,
            stage,
            "native-lifecycle-stage-nonce-1",
            "native-lifecycle-stage-key",
        ),
    ))
    .await;
    assert!(
        exact_stage
            .error
            .as_deref()
            .is_some_and(|error| error.contains("REPLAY_NONCE_CONSUMED")),
        "exact Stage retry must remain a kernel nonce rejection: {:?}",
        exact_stage.error
    );

    reopened.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}
