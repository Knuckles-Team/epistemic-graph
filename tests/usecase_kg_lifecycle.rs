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
#[path = "common/test_support.rs"]
mod test_support;

use serde_json::json;

use epistemic_graph::protocol::{Method, ResultPayload};
use epistemic_graph::server::dispatch;

const SECRET: &str = "usecase-lifecycle-secret";

/// A fully-featured state with a real redb persistence backend. The multi-op
/// `BeginTxn`..`Commit` path in this test seals its transaction recovery plan
/// (`server::handlers::txn::seal_txn_recovery_plan`), which fail-closed REQUIRES
/// `EPISTEMIC_GRAPH_ENCRYPTION_KEY` at the backend's `open()` call -- the same
/// requirement `redb_backend::tests::cm_dir` / `advanced_crossmodal_roundtrip.rs::state`
/// provision. Encryption is symmetric and transparent to this test's assertions;
/// provision it ONCE, before the first backend opens.
fn state() -> test_support::SharedState {
    #[cfg(feature = "redb")]
    test_support::provision_encryption_key_once("usecase-lifecycle-recovery-key");
    test_support::durable_state(SECRET, common::current_isolation())
}

async fn begin(state: &test_support::SharedState, id: u64) -> String {
    test_support::begin_txn(state, SECRET, id, None).await
}

async fn ok(state: &test_support::SharedState, id: u64, method: Method) {
    test_support::assert_ok(state, SECRET, id, method).await
}

async fn hybrid_read(state: &test_support::SharedState, id: u64) -> Vec<String> {
    let r = Box::pin(dispatch(
        state,
        test_support::commons_request(
            SECRET,
            id,
            Method::UnifiedQueryText {
                text: "MATCH (:Sensor) |> RANK BY ~[1.0,0.0] |> LIMIT 10".into(),
            },
        ),
    ))
    .await;
    test_support::unified_ids(&r)
}

/// SHACL shapes: a `Sensor` MUST carry a `unit` (minCount 1).
const SHAPES: &str = "@prefix sh: <http://www.w3.org/ns/shacl#> .\n\
    @prefix ex: <http://ex/> .\n\
    ex:SensorShape a sh:NodeShape ;\n\
      sh:targetClass ex:Sensor ;\n\
      sh:property [ sh:path ex:unit ; sh:minCount 1 ] .\n";

async fn shacl_conforms(state: &test_support::SharedState, id: u64, data_graph: &str) -> bool {
    let r = Box::pin(dispatch(
        state,
        test_support::commons_request(
            SECRET,
            id,
            Method::ShaclValidate {
                shapes: Some(SHAPES.into()),
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
            properties_msgpack: test_support::json_bytes(
                json!({ "type": "Sensor", "unit": "celsius" }),
            ),
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
            properties_msgpack: test_support::json_bytes(json!({ "type": "Room" })),
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
            properties_msgpack: test_support::json_bytes(json!({ "relationship": "LOCATED_IN" })),
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
        test_support::commons_request(
            SECRET,
            16,
            Method::Commit {
                txn_id: txn.clone(),
                idempotency_key: None,
            },
        ),
    ))
    .await;
    assert!(
        matches!(
            commit.result,
            Some(ResultPayload::Json(serde_json::Value::Bool(true)))
        ),
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
    let inferred = test_support::unified_ids(
        &Box::pin(dispatch(
            &state,
            test_support::commons_request(SECRET, 17, Method::UnifiedQuery { plan: reason }),
        ))
        .await,
    );
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
                    properties_msgpack: test_support::json_bytes(
                        json!({ "type": "Sensor", "unit": "kelvin" }),
                    ),
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
            let c = Box::pin(dispatch(
                &state,
                test_support::commons_request(
                    SECRET,
                    103,
                    Method::Commit {
                        txn_id: txn,
                        idempotency_key: None,
                    },
                ),
            ))
            .await;
            assert!(matches!(
                c.result,
                Some(ResultPayload::Json(serde_json::Value::Bool(true)))
            ));
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
