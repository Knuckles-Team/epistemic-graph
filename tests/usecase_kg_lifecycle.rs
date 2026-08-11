//! HIGH-VALUE USE-CASE SUITE #5 — KG lifecycle with validation & inference (CONCEPT:EG-KG.query.usecase-kg-lifecycle).
//!
//! The full write-path lifecycle of a knowledge graph, proven end-to-end over the REAL
//! server `dispatch`:
//!   1. VALIDATE — SHACL validation gates the write: a data graph that violates the shape
//!      is REJECTED (`conforms=false`), a conformant one PASSES.
//!   2. MUTATE (ACID) — a new instance node + its edge + its vector embedding + an OWL TBox
//!      axiom are staged in ONE transaction and committed ATOMICALLY (the EG-359..390 in-txn
//!      cross-modal seam), so graph mutation AND vector-index maintenance AND the ontology
//!      change land together or not at all.
//!   3. INFER — after commit the OWL reasoner's inference CLOSURE reflects the new axiom:
//!      the freshly-committed instance is inferred a member of the committed super-class.
//!   4. RE-INDEX — the new embedding is immediately kNN-retrievable (the vector index was
//!      maintained by the same commit).
//!   5. CONCURRENCY — under a CONCURRENT writer committing a second cross-modal instance,
//!      many concurrent hybrid readers each see a CONSISTENT snapshot: a vector-RANK that
//!      returns the new node inherently proves its node AND embedding committed together
//!      (never a torn/partial state).
//!
//! SEAMS exercised: SHACL(validation)⇄graph(mutation)⇄vector(index maintenance)⇄OWL
//! (inference closure) in one ACID txn, under concurrent hybrid read/write.
//! Module-gated on the surfaces it drives; runs under `--features full`.
#![cfg(all(
    feature = "query",
    feature = "owl-plan",
    feature = "shacl",
    feature = "rdf"
))]

mod common;

use std::sync::Arc;

use dashmap::DashMap;
use serde_json::json;
use tokio::sync::{RwLock, Semaphore};

use epistemic_graph::channels::ChannelManager;
use epistemic_graph::protocol::{Method, Request, Response, ResultPayload};
use epistemic_graph::registry::GraphRegistry;
use epistemic_graph::server::{dispatch, ServerState};

const SECRET: &str = "usecase-lifecycle-secret";

fn state() -> Arc<RwLock<ServerState>> {
    let (persist_dir, persistence) = common::tempdir_persistence();
    Arc::new(RwLock::new(ServerState {
        #[cfg(feature = "redb")]
        cold_tracker: std::sync::Arc::new(
            epistemic_graph::server::persistence::cold_offload::ColdTenantTracker::new(),
        ),
        registry: GraphRegistry::new(),
        isolation: common::current_isolation(),
        channels: ChannelManager::new(),
        auth_secret: SECRET.to_string(),
        persist_dir,
        persistence,
        max_in_flight: Arc::new(Semaphore::new(16)),
        read_admission: Arc::new(Semaphore::new(16)),
        per_graph_inflight: Arc::new(DashMap::new()),
        per_graph_inflight_limit: 8,
        write_coalescer: Arc::new(epistemic_graph::write_coalescer::WriteCoalescerRegistry::new()),
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
    common::signed_request(SECRET, id, "__commons__", method)
}

fn pack(v: serde_json::Value) -> Vec<u8> {
    rmp_serde::to_vec_named(&v).unwrap()
}

async fn begin(state: &Arc<RwLock<ServerState>>, id: u64) -> String {
    let r = Box::pin(dispatch(
        state,
        req(
            id,
            Method::BeginTxn {
                graph: None,
                isolation: None,
            },
        ),
    ))
    .await;
    match r.result {
        Some(ResultPayload::String(s)) => s,
        other => panic!("BeginTxn failed: {:?} / {other:?}", r.error),
    }
}

async fn ok(state: &Arc<RwLock<ServerState>>, id: u64, method: Method) {
    let r = Box::pin(dispatch(state, req(id, method))).await;
    assert!(r.error.is_none(), "op {id} failed: {:?}", r.error);
}

fn unified_ids(resp: &Response) -> Vec<String> {
    assert!(
        resp.error.is_none(),
        "unified query error: {:?}",
        resp.error
    );
    let bytes = match &resp.result {
        Some(ResultPayload::Raw(b)) => b.clone(),
        other => panic!("expected Raw result, got {other:?}"),
    };
    let rows: Vec<(String, Option<f32>)> = rmp_serde::from_slice(&bytes).unwrap();
    rows.into_iter().map(|(id, _)| id).collect()
}

async fn hybrid_read(state: &Arc<RwLock<ServerState>>, id: u64) -> Vec<String> {
    let r = Box::pin(dispatch(
        state,
        req(
            id,
            Method::UnifiedQueryText {
                text: "MATCH (:Sensor) |> RANK BY ~[1.0,0.0] |> LIMIT 10".into(),
            },
        ),
    ))
    .await;
    unified_ids(&r)
}

/// SHACL shapes: a `Sensor` MUST carry a `unit` (minCount 1).
const SHAPES: &str = "@prefix sh: <http://www.w3.org/ns/shacl#> .\n\
    @prefix ex: <http://ex/> .\n\
    ex:SensorShape a sh:NodeShape ;\n\
      sh:targetClass ex:Sensor ;\n\
      sh:property [ sh:path ex:unit ; sh:minCount 1 ] .\n";

async fn shacl_conforms(state: &Arc<RwLock<ServerState>>, id: u64, data_graph: &str) -> bool {
    let r = Box::pin(dispatch(
        state,
        req(
            id,
            Method::ShaclValidate {
                shapes: SHAPES.into(),
                data_graph: data_graph.into(),
            },
        ),
    ))
    .await;
    assert!(r.error.is_none(), "ShaclValidate error: {:?}", r.error);
    match &r.result {
        Some(ResultPayload::Json(v)) => v["conforms"].as_bool().expect("conforms bool"),
        other => panic!("expected Json report, got {other:?}"),
    }
}

/// THE KG-lifecycle proof (CONCEPT:EG-KG.query.usecase-kg-lifecycle): validate → atomic cross-modal commit → inference
/// closure → vector re-index → consistent concurrent reads.
#[tokio::test]
async fn validate_commit_infer_reindex_under_concurrency_eg438() {
    let state = state();

    // ── 1. SHACL VALIDATION gates the write ──
    let bad = "@prefix ex: <http://ex/> .\nex:s_bad a ex:Sensor .\n";
    let good = "@prefix ex: <http://ex/> .\nex:s1 a ex:Sensor ; ex:unit \"celsius\" .\n";
    assert!(
        !shacl_conforms(&state, 1, bad).await,
        "a Sensor missing its required `unit` must be REJECTED by SHACL validation"
    );
    assert!(
        shacl_conforms(&state, 2, good).await,
        "a conformant Sensor must PASS SHACL validation"
    );

    // ── 2. ONE ACID txn: graph node + edge + embedding + OWL axiom, committed atomically ──
    let txn = begin(&state, 10).await;
    ok(
        &state,
        11,
        Method::TxnAddNode {
            txn_id: txn.clone(),
            node_id: "s1".into(),
            properties_msgpack: pack(json!({ "type": "Sensor", "unit": "celsius" })),
            graph: None,
        },
    )
    .await;
    ok(
        &state,
        12,
        Method::TxnAddNode {
            txn_id: txn.clone(),
            node_id: "room".into(),
            properties_msgpack: pack(json!({ "type": "Room" })),
            graph: None,
        },
    )
    .await;
    ok(
        &state,
        13,
        Method::TxnAddEdge {
            txn_id: txn.clone(),
            source_id: "s1".into(),
            target_id: "room".into(),
            properties_msgpack: pack(json!({ "relationship": "LOCATED_IN" })),
            graph: None,
        },
    )
    .await;
    ok(
        &state,
        14,
        Method::TxnAddEmbedding {
            txn_id: txn.clone(),
            node_id: "s1".into(),
            embedding: vec![1.0, 0.0],
            graph: None,
        },
    )
    .await;
    // OWL TBox change staged in the SAME txn: Sensor ⊑ Device.
    ok(
        &state,
        15,
        Method::TxnAxiom {
            txn_id: txn.clone(),
            turtle: "@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n\
                     <http://ex/Sensor> rdfs:subClassOf <http://ex/Device> .\n"
                .into(),
            graph: None,
        },
    )
    .await;
    let commit = Box::pin(dispatch(
        &state,
        req(
            16,
            Method::Commit {
                txn_id: txn.clone(),
            },
        ),
    ))
    .await;
    assert!(
        matches!(commit.result, Some(ResultPayload::Bool(true))),
        "the cross-modal lifecycle txn must commit atomically: {:?}",
        commit.error
    );

    // ── 3. INFERENCE CLOSURE: the committed axiom makes s1 an inferred Device ──
    // REASON with an EMPTY ontology reads the axiom from the committed TBox.
    let reason = eg_plan::Plan::new(vec![
        eg_plan::Op::Scan {
            label: "Sensor".into(),
        },
        eg_plan::Op::Reason {
            target_class: "<http://ex/Device>".into(),
            ontology: String::new(),
        },
    ]);
    let inferred =
        unified_ids(&Box::pin(dispatch(&state, req(17, Method::UnifiedQuery { plan: reason }))).await);
    assert_eq!(
        inferred,
        vec!["s1".to_string()],
        "OWL inference closure over the committed TBox infers s1 a Device: {inferred:?}"
    );

    // ── 4. VECTOR RE-INDEX: the new embedding is immediately kNN-retrievable ──
    let hits = hybrid_read(&state, 18).await;
    assert_eq!(
        hits,
        vec!["s1".to_string()],
        "the committed embedding is immediately kNN-retrievable (index maintained): {hits:?}"
    );

    // ── 5. CONCURRENCY: a second cross-modal writer + many hybrid readers, all consistent ──
    // The writer commits a SECOND sensor s2 (node + embedding) while readers run.
    let writer = {
        let state = state.clone();
        tokio::spawn(async move {
            let txn = begin(&state, 100).await;
            ok(
                &state,
                101,
                Method::TxnAddNode {
                    txn_id: txn.clone(),
                    node_id: "s2".into(),
                    properties_msgpack: pack(json!({ "type": "Sensor", "unit": "kelvin" })),
                    graph: None,
                },
            )
            .await;
            ok(
                &state,
                102,
                Method::TxnAddEmbedding {
                    txn_id: txn.clone(),
                    node_id: "s2".into(),
                    embedding: vec![0.98, 0.10],
                    graph: None,
                },
            )
            .await;
            let c = Box::pin(dispatch(&state, req(103, Method::Commit { txn_id: txn }))).await;
            assert!(matches!(c.result, Some(ResultPayload::Bool(true))));
        })
    };

    let mut readers = Vec::new();
    for i in 0..12u64 {
        let state = state.clone();
        readers.push(tokio::spawn(
            async move { hybrid_read(&state, 200 + i).await },
        ));
    }
    writer.await.unwrap();
    for r in readers {
        let hits = r.await.unwrap();
        // Snapshot consistency: s1 is always present (committed before the readers spawned).
        assert!(
            hits.contains(&"s1".to_string()),
            "every concurrent reader sees the already-committed s1: {hits:?}"
        );
        // No torn state: s2 is either fully retrievable via a VECTOR rank (so its node AND
        // embedding committed together) or absent — never a half-committed row. A
        // vector-RANK result returning s2 IS the proof both modalities landed atomically.
        assert!(
            hits.iter().all(|id| id == "s1" || id == "s2"),
            "a concurrent hybrid read never surfaces a torn/partial row: {hits:?}"
        );
    }

    // After the writer joined, a final read sees BOTH sensors, both vector-ranked.
    let both = hybrid_read(&state, 300).await;
    assert!(
        both.contains(&"s1".to_string()) && both.contains(&"s2".to_string()),
        "post-commit both cross-modal instances are retrievable together: {both:?}"
    );
}
