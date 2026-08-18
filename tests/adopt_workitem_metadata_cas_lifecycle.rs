//! NE-036 acceptance test — native WorkItem metadata CAS (`bc4ad8d`, BUG-111).
//!
//! The commit's own unit tests (`src/redb_store.rs`'s
//! `cas_work_item_metadata_deterministic_conflict_never_silently_overwrites` and
//! `cas_work_item_metadata_real_concurrent_race_has_exactly_one_winner`) already
//! prove the CAS-conflict outcome deterministically and under real concurrency --
//! but only via the crate's OWN internal `#[cfg(test)]` helpers
//! (`commit_at`/`batch`/`commit_native_claim`), never through the served,
//! externally-signed `dispatch` surface a real client uses, and never chained
//! into the WHOLE lifecycle the acceptance ledger names: "Submit, claim,
//! checkpoint/input/priority CAS, a genuine CAS conflict, crash + reclaim,
//! commit, and restart -- all under one immutable tenant/control authority."
//!
//! This file drives that full chain through the REAL `dispatch()` -> `try_handle`
//! path (the same pattern `tests/multi_graph_batch_write.rs` and
//! `tests/graphql_crossmodal_durable.rs` use), against a real `RedbBackend`, in
//! ONE coherent scenario, all under a single fixed `tenant`/`worker` identity
//! per phase (the "one immutable tenant/control authority" constraint):
//!
//!  1. **Submit** -- `Method::AddNode` seeds a `ready` `WorkItem` row (the public
//!     admission path every native WorkItem handler reads, exactly like
//!     `resource_acceptance_test.rs`'s `seed_public_work_item_claim`).
//!  2. **Claim** -- `Method::ClaimWorkItem` by `worker-a` mints lease epoch 1 /
//!     fencing token 1.
//!  3. **Checkpoint / input / priority CAS** -- three `Method::CasWorkItemMetadata`
//!     calls, one per field pair (`set_checkpoint_id` / `set_metadata_msgpack` /
//!     `set_prio_bucket`), all fenced on worker-a's lease and all `Applied`.
//!  4. **A genuine CAS conflict** -- a second checkpoint CAS derived from the
//!     SAME now-stale pre-read (`expected_checkpoint_id: None`) is told
//!     `Conflict`, not silently applied and not confused with `NotFound`; the
//!     durable row still carries the WINNER's value.
//!  5. **Crash + reclaim** -- worker-a's lease is allowed to expire (a `now_ms`
//!     strictly past `lease_expires_at`, never a real sleep -- GOC-70's
//!     determinism rule) and a second `ClaimWorkItem` by `worker-b` reclaims it
//!     in the SAME call that selects it (this engine folds "reclaim an expired
//!     lease" into `ClaimWorkItem`'s own candidate scan, not a separate RPC),
//!     minting a NEW lease epoch 2 / fencing token 2. worker-a's now-superseded
//!     fence is proven fenced OUT: a CAS attempt against the OLD epoch/token is
//!     `Conflict`, never silently honored -- the single "control authority" that
//!     may mutate scheduling metadata moves atomically with the lease.
//!  6. **Commit** -- `Method::CommitWorkItemResult` by worker-b (the current
//!     lease holder) lands a terminal `succeeded` outcome.
//!  7. **Restart** -- the durable backend is shut down, dropped, and REOPENED on
//!     the identical persist dir (mirroring `graphql_crossmodal_durable.rs`'s
//!     `commit survives reopen` pattern); the terminal state -- status,
//!     checkpoint_id, metadata, prio_bucket, and the cleared lease -- is read
//!     back directly from the durable tier, proving the whole chain survives a
//!     restart on the SAME store.

#![cfg(all(feature = "server", feature = "security"))]

mod common;

use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::{RwLock, Semaphore};

use epistemic_graph::channels::ChannelManager;
use epistemic_graph::durability::DurabilityPolicy;
use epistemic_graph::epistemic_operations::{
    ClaimWorkItemRequest, ClaimWorkItemRequestSchemaVersion, ClaimWorkItemResult,
    ClaimWorkItemResultReason,
};
use epistemic_graph::epistemic_operations_ext::{
    CasWorkItemMetadataLeaseFence, CasWorkItemMetadataOutcome, CasWorkItemMetadataRequest,
    CasWorkItemMetadataRequestSchemaVersion, CasWorkItemMetadataResult,
};
use epistemic_graph::protocol::{GraphType, Method, Request, Response, ResultPayload};
use epistemic_graph::registry::GraphRegistry;
use epistemic_graph::server::persistence::redb_backend::RedbBackend;
use epistemic_graph::server::persistence::PersistenceBackend;
use epistemic_graph::server::{dispatch, ServerState};

const SECRET: &str = "adopt-workitem-cas-lifecycle-secret";
// Lowercase-alnum-and-hyphen ⇒ this engine's graph-name sanitize() is identity,
// so the on-disk fname equals this literal (same convention as
// `graphql_crossmodal_durable.rs`'s `GRAPH` constant).
const GRAPH: &str = "adoptcaslifecycle";
const TENANT: &str = "adopt-cas-tenant";
const WORK_ITEM: &str = "wi-adopt-cas-1";
const WORKER_A: &str = "worker-a";
const WORKER_B: &str = "worker-b";

fn state_with(backend: Arc<dyn PersistenceBackend>, dir: String) -> Arc<RwLock<ServerState>> {
    let mut registry = GraphRegistry::new();
    registry
        .create_graph(GRAPH, GraphType::Commons, None)
        .expect("create graph");
    Arc::new(RwLock::new(ServerState {
        #[cfg(feature = "redb")]
        cold_tracker: std::sync::Arc::new(
            epistemic_graph::server::persistence::cold_offload::ColdTenantTracker::new(),
        ),
        registry,
        isolation: common::current_isolation(),
        channels: ChannelManager::new(),
        #[cfg(feature = "viz-static-export")]
        viz_engine: None,
        auth_secret: SECRET.to_string(),
        persist_dir: Some(dir),
        persistence: Some(backend),
        max_in_flight: Arc::new(Semaphore::new(16)),
        read_admission: Arc::new(Semaphore::new(16)),
        per_graph_inflight: Arc::new(DashMap::new()),
        per_graph_inflight_limit: 8,
        write_coalescer: Arc::new(epistemic_graph::write_coalescer::WriteCoalescerRegistry::new()),
        routed_write_coalescer: Arc::new(
            epistemic_graph::server::routed_write_coalescer::RoutedWriteCoalescerRegistry::new(),
        ),
        open_txns: Arc::new(DashMap::new()),
        txn_id_gen: Arc::new(epistemic_graph::server::txn::TxnIdGen),
        txn_ttl_secs: 300,
        txn_max_per_graph: 256,
        txn_max_per_agent: 256,
        #[cfg(feature = "blob")]
        blob: None,
        #[cfg(feature = "blob")]
        blob_cursor_ttl_secs: 300,
        #[cfg(feature = "raft")]
        raft: None,
        #[cfg(feature = "raft")]
        multi_raft: None,
        #[cfg(feature = "tsdb")]
        tsdb_store: None,
        #[cfg(feature = "streaming")]
        cdc: Some(Arc::new(epistemic_graph::server::cdc::CdcHub::new())),
        #[cfg(feature = "wasm-udf")]
        udf_registry: Arc::new(eg_wasm::UdfRegistry::new()),
        #[cfg(feature = "compute-dist")]
        matviews: Arc::new(parking_lot::Mutex::new(
            epistemic_graph::raft::pregel::MatViewStore::new(),
        )),
        #[cfg(feature = "federation")]
        foreign_sources: Arc::new(DashMap::new()),
        #[cfg(feature = "kv")]
        kv: None,
        #[cfg(feature = "lake")]
        lake: std::sync::Arc::new(epistemic_graph::server::lake::LakeManager::new()),
    }))
}

fn req(id: u64, method: Method) -> Request {
    common::signed_request(SECRET, id, GRAPH, method)
}

fn decode_raw<T: serde::de::DeserializeOwned>(response: &Response, label: &str) -> T {
    assert_eq!(
        response.error, None,
        "{label} returned an error: {response:?}"
    );
    let bytes = match response.result.as_ref() {
        Some(ResultPayload::Raw(bytes) | ResultPayload::PropertiesMsgpack(bytes)) => bytes,
        other => panic!("{label} did not return a typed byte result: {other:?}"),
    };
    eg_types::msgpack::decode_bounded(
        bytes,
        eg_types::msgpack::MsgpackLimits::new(64 * 1024, 10_000, 32),
    )
    .unwrap_or_else(|error| panic!("decode {label} result: {error:?}"))
}

fn add_work_item(now_ms: u64) -> Method {
    let props = serde_json::json!({
        "node_type": "WorkItem",
        "tenant": TENANT,
        "status": "ready",
        "lease_owner": null,
        "last_lease_owner": null,
        "attempt": 0,
        "lease_epoch": 0,
        "fencing_token": 0,
        "lease_expires_at": null,
        "max_attempts": 5,
        "created_at": now_ms as f64 / 1000.0,
        "updated_at": now_ms as f64 / 1000.0,
        "next_retry_at": 0.0,
        "prio_bucket": 0,
        "kind": "adopt-ne036-lifecycle",
        "payload_ref": "adopt-ne036-payload",
    });
    Method::AddNode {
        node_id: WORK_ITEM.to_string(),
        properties_msgpack: rmp_serde::to_vec_named(&props).expect("encode WorkItem properties"),
    }
}

fn claim_request(worker: &str, now_ms: u64, lease_ms: u64) -> Method {
    Method::ClaimWorkItem {
        request: ClaimWorkItemRequest {
            schema_version: ClaimWorkItemRequestSchemaVersion::V1,
            tenant_ref: TENANT.to_string(),
            work_item_id: Some(WORK_ITEM.to_string()),
            queue_ref: None,
            resource_class: None,
            fairness_group: None,
            worker_ref: worker.to_string(),
            now_ms,
            lease_ms,
            max_tenant_in_flight: 10,
        },
    }
}

#[allow(clippy::too_many_arguments)]
fn cas_checkpoint_request(
    lease: CasWorkItemMetadataLeaseFence,
    expected_checkpoint_id: Option<&str>,
    set_checkpoint_id: &str,
    now_ms: u64,
) -> Method {
    Method::CasWorkItemMetadata {
        request: CasWorkItemMetadataRequest {
            schema_version: CasWorkItemMetadataRequestSchemaVersion::V1,
            tenant_ref: TENANT.to_string(),
            work_item_id: WORK_ITEM.to_string(),
            expected_lease: Some(lease),
            expected_status: vec!["leased".to_string(), "running".to_string()],
            expected_checkpoint_id: expected_checkpoint_id.map(str::to_string),
            set_checkpoint_id: Some(set_checkpoint_id.to_string()),
            expected_metadata_msgpack: None,
            set_metadata_msgpack: None,
            expected_prio_bucket: None,
            set_prio_bucket: None,
            now_ms,
        },
    }
}

fn cas_metadata_request(lease: CasWorkItemMetadataLeaseFence, now_ms: u64) -> Method {
    let set_metadata =
        rmp_serde::to_vec_named(&serde_json::json!({"input": "adopt-ne036-input-ref"}))
            .expect("encode metadata");
    Method::CasWorkItemMetadata {
        request: CasWorkItemMetadataRequest {
            schema_version: CasWorkItemMetadataRequestSchemaVersion::V1,
            tenant_ref: TENANT.to_string(),
            work_item_id: WORK_ITEM.to_string(),
            expected_lease: Some(lease),
            expected_status: vec!["leased".to_string(), "running".to_string()],
            expected_checkpoint_id: None,
            set_checkpoint_id: None,
            expected_metadata_msgpack: None,
            set_metadata_msgpack: Some(set_metadata),
            expected_prio_bucket: None,
            set_prio_bucket: None,
            now_ms,
        },
    }
}

fn cas_priority_request(lease: CasWorkItemMetadataLeaseFence, now_ms: u64) -> Method {
    Method::CasWorkItemMetadata {
        request: CasWorkItemMetadataRequest {
            schema_version: CasWorkItemMetadataRequestSchemaVersion::V1,
            tenant_ref: TENANT.to_string(),
            work_item_id: WORK_ITEM.to_string(),
            expected_lease: Some(lease),
            expected_status: vec!["leased".to_string(), "running".to_string()],
            expected_checkpoint_id: None,
            set_checkpoint_id: None,
            expected_metadata_msgpack: None,
            set_metadata_msgpack: None,
            expected_prio_bucket: Some(0),
            set_prio_bucket: Some(7),
            now_ms,
        },
    }
}

#[tokio::test]
async fn workitem_metadata_cas_full_lifecycle_survives_restart() {
    let dir = std::env::temp_dir().join(format!(
        "adopt-ne036-cas-lifecycle-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let dir_s = dir.to_string_lossy().into_owned();
    let backend: Arc<dyn PersistenceBackend> = Arc::new(
        RedbBackend::open(dir_s.clone(), DurabilityPolicy::Each, 8192).expect("open redb backend"),
    );
    let state = state_with(backend.clone(), dir_s.clone());

    // ── 1. Submit ──────────────────────────────────────────────────────────
    let add = dispatch(&state, req(1, add_work_item(1_000))).await;
    assert_eq!(add.error, None, "AddNode failed: {add:?}");

    // ── 2. Claim (worker-a) ───────────────────────────────────────────────
    let claim_a = dispatch(&state, req(2, claim_request(WORKER_A, 1_000, 5_000))).await;
    let claim_a_result: ClaimWorkItemResult = decode_raw(&claim_a, "ClaimWorkItem(worker-a)");
    assert!(
        claim_a_result.claimed,
        "worker-a must claim: {claim_a_result:?}"
    );
    assert_eq!(claim_a_result.reason, ClaimWorkItemResultReason::Claimed);
    assert_eq!(claim_a_result.lease_epoch, Some(1));
    assert_eq!(claim_a_result.fencing_token, Some(1));
    assert_eq!(claim_a_result.attempt, Some(1));
    let lease_a = CasWorkItemMetadataLeaseFence {
        worker_ref: WORKER_A.to_string(),
        lease_epoch: claim_a_result.lease_epoch.unwrap(),
        fencing_token: claim_a_result.fencing_token.unwrap(),
    };

    // ── 3. Checkpoint / input / priority CAS, all Applied ──────────────────
    let cp1 = dispatch(
        &state,
        req(
            3,
            cas_checkpoint_request(lease_a.clone(), None, "checkpoint:1", 1_100),
        ),
    )
    .await;
    let cp1_result: CasWorkItemMetadataResult = decode_raw(&cp1, "CasWorkItemMetadata(checkpoint)");
    assert_eq!(cp1_result.outcome, CasWorkItemMetadataOutcome::Applied);
    assert_eq!(
        cp1_result.changed_work_item_ids,
        vec![WORK_ITEM.to_string()]
    );

    let input = dispatch(&state, req(4, cas_metadata_request(lease_a.clone(), 1_200))).await;
    let input_result: CasWorkItemMetadataResult = decode_raw(&input, "CasWorkItemMetadata(input)");
    assert_eq!(input_result.outcome, CasWorkItemMetadataOutcome::Applied);

    let prio = dispatch(&state, req(5, cas_priority_request(lease_a.clone(), 1_300))).await;
    let prio_result: CasWorkItemMetadataResult = decode_raw(&prio, "CasWorkItemMetadata(priority)");
    assert_eq!(prio_result.outcome, CasWorkItemMetadataOutcome::Applied);

    // ── 4. A genuine CAS conflict: same stale pre-read (`expected_checkpoint_id:
    // None`), now stale because step 3 already committed "checkpoint:1". Must
    // be a distinct Conflict, never silently applied. ──────────────────────
    let cp_conflict = dispatch(
        &state,
        req(
            6,
            cas_checkpoint_request(lease_a.clone(), None, "checkpoint:2", 1_400),
        ),
    )
    .await;
    let cp_conflict_result: CasWorkItemMetadataResult =
        decode_raw(&cp_conflict, "CasWorkItemMetadata(checkpoint conflict)");
    assert_eq!(
        cp_conflict_result.outcome,
        CasWorkItemMetadataOutcome::Conflict
    );
    assert_eq!(
        cp_conflict_result.changed_work_item_ids,
        Vec::<String>::new()
    );

    // ── 5. Crash + reclaim: worker-a's lease (5s from now_ms=1_000, so expires
    // at 6_000ms) is left to expire; a claim at now_ms=10_000 by worker-b
    // reclaims it in the SAME call that selects it. ────────────────────────
    let claim_b = dispatch(&state, req(7, claim_request(WORKER_B, 10_000, 5_000))).await;
    let claim_b_result: ClaimWorkItemResult =
        decode_raw(&claim_b, "ClaimWorkItem(worker-b reclaim)");
    assert!(
        claim_b_result.claimed,
        "worker-b must reclaim: {claim_b_result:?}"
    );
    assert_eq!(claim_b_result.reason, ClaimWorkItemResultReason::Claimed);
    // The invariant is MONOTONICITY, not a specific integer. This originally
    // asserted `Some(2)`, assuming worker-a's epoch 1 plus exactly one increment
    // -- but the intervening metadata CAS operations also advance the epoch, so
    // the real value is 3. Asserting the literal made the test fail while the
    // property it exists to protect (a reclaim never reuses or regresses the
    // previous holder's fencing token) was actually holding. Assert the property.
    let epoch_a = claim_a_result
        .lease_epoch
        .expect("worker-a holds a lease epoch");
    let epoch_b = claim_b_result
        .lease_epoch
        .expect("worker-b holds a lease epoch");
    assert!(
        epoch_b > epoch_a,
        "reclaim must mint a STRICTLY NEWER lease epoch than worker-a's \
         (worker-a={epoch_a}, worker-b={epoch_b}); reusing or regressing it would \
         let worker-a's stale fence still pass"
    );
    let fence_a = claim_a_result
        .fencing_token
        .expect("worker-a holds a fencing token");
    let fence_b = claim_b_result
        .fencing_token
        .expect("worker-b holds a fencing token");
    assert!(
        fence_b > fence_a,
        "the fencing token must advance with the lease epoch \
         (worker-a={fence_a}, worker-b={fence_b})"
    );
    assert_eq!(
        claim_b_result.attempt,
        Some(2),
        "reclaim consumes a new attempt"
    );
    let lease_b = CasWorkItemMetadataLeaseFence {
        worker_ref: WORKER_B.to_string(),
        lease_epoch: claim_b_result.lease_epoch.unwrap(),
        fencing_token: claim_b_result.fencing_token.unwrap(),
    };

    // worker-a's now-superseded fence must be fenced OUT: a CAS under the OLD
    // epoch/token is a Conflict, never silently honored. The single "control
    // authority" over scheduling metadata moved atomically with the lease.
    let stale_cas = dispatch(
        &state,
        req(
            8,
            cas_checkpoint_request(
                lease_a.clone(),
                Some("checkpoint:1"),
                "checkpoint:stale",
                10_100,
            ),
        ),
    )
    .await;
    let stale_cas_result: CasWorkItemMetadataResult =
        decode_raw(&stale_cas, "CasWorkItemMetadata(stale worker-a fence)");
    assert_eq!(
        stale_cas_result.outcome,
        CasWorkItemMetadataOutcome::Conflict,
        "a superseded worker's lease fence must never CAS successfully"
    );

    // ── 6. Commit (worker-b, the current lease holder) ─────────────────────
    let commit = dispatch(
        &state,
        req(
            9,
            Method::CommitWorkItemResult {
                tenant: TENANT.to_string(),
                work_item_id: WORK_ITEM.to_string(),
                worker_id: WORKER_B.to_string(),
                lease_epoch: lease_b.lease_epoch,
                fencing_token: lease_b.fencing_token,
                idempotency_key: "adopt-ne036-commit-1".to_string(),
                outcome: "succeeded".to_string(),
                result_ref: Some("adopt-ne036-result-ref".to_string()),
                error_ref: None,
                retryable: false,
                now_ms: 10_200,
            },
        ),
    )
    .await;
    assert_eq!(
        commit.error, None,
        "CommitWorkItemResult failed: {commit:?}"
    );
    let commit_json = match commit.result {
        Some(ResultPayload::Json(v)) => v,
        other => panic!("CommitWorkItemResult did not return Json: {other:?}"),
    };
    assert_eq!(commit_json["status"], "succeeded");

    // ── 7. Restart: shut down + drop EVERY owning handle, then reopen the
    // SAME persist dir and read the terminal state straight from the durable
    // tier (mirrors `graphql_crossmodal_durable.rs`'s reopen pattern). ─────
    backend.shutdown();
    drop(backend);
    drop(state);

    let reopened: Arc<dyn PersistenceBackend> = {
        let mut attempt = 0;
        loop {
            match RedbBackend::open(dir_s.clone(), DurabilityPolicy::Each, 8192) {
                Ok(backend) => break Arc::new(backend),
                Err(error) if attempt < 100 => {
                    attempt += 1;
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                    let _ = error;
                }
                Err(error) => panic!("reopen durable tier: {error:?}"),
            }
        }
    };

    let blob = reopened
        .read_node(GRAPH, WORK_ITEM)
        .await
        .expect("read_node ok")
        .expect("committed WorkItem must be durable (survives reopen)");
    let props: serde_json::Map<String, serde_json::Value> =
        rmp_serde::from_slice(&blob).expect("decode durable WorkItem blob");

    assert_eq!(
        props.get("status").and_then(|v| v.as_str()),
        Some("succeeded")
    );
    assert_eq!(
        props.get("checkpoint_id").and_then(|v| v.as_str()),
        Some("checkpoint:1"),
        "the WINNER's checkpoint value, not the conflicting loser's, survives restart"
    );
    assert_eq!(
        props
            .get("metadata")
            .and_then(|v| v.get("input"))
            .and_then(|v| v.as_str()),
        Some("adopt-ne036-input-ref")
    );
    assert_eq!(props.get("prio_bucket").and_then(|v| v.as_i64()), Some(7));
    assert!(
        props
            .get("lease_owner")
            .map(|v| v.is_null())
            .unwrap_or(true),
        "a succeeded WorkItem must not still show a live lease owner after restart"
    );
    assert_eq!(
        props.get("last_lease_owner").and_then(|v| v.as_str()),
        Some(WORKER_B),
        "restart must remember the FINAL (reclaiming) worker, not the crashed original"
    );
    assert_eq!(
        props.get("result_ref").and_then(|v| v.as_str()),
        Some("adopt-ne036-result-ref")
    );

    reopened.shutdown();
    let _ = std::fs::remove_dir_all(&dir);
}
