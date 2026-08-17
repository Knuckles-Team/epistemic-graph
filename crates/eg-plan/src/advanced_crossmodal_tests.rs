//! Advanced cross-modal seam proofs (CONCEPT:EG-KG.compute.asof-valid-t-sources..EG-389).
//!
//! The "north-star" set: the MOST complex cross-modal plans the closed algebra can
//! express over a HAND-BUILT [`PlanCtx`] (no server, fully self-contained), each fusing
//! THREE+ modalities in ONE plan and asserting the fused result equals a hand-reasoned
//! oracle — the richest exercises of "every `Op` is `RowSet -> RowSet`, so any op's
//! output is a legal input to any op".
//!
//! Module-gated on `query` (the executor); each proof is additionally `cfg`-gated on the
//! modality feature(s) it exercises, so the module compiles under ANY feature subset of
//! `query`. The roundtrip-surface tests (in-txn 5-modality commit, RLS, encryption,
//! cross-shard Raft, pgwire/sparql snapshot, streaming CDC, KV-cache warm-fork) live in
//! `tests/advanced_crossmodal_roundtrip.rs` — they need a live server/txn/cluster surface
//! a hand-built `PlanCtx` cannot stand up.
//!
//!  * EG-384 — bitemporal multi-hop: `AsOf(source) → Rank → Reason<iri> → Traverse`
//!    (temporal × vector × OWL × graph). Only members INFERRED-and-LIVE-at-t reach their
//!    targets; a later instant re-selects a smaller live set (the "inferred-at-t" seam).
//!  * EG-385 — federation fusion: `Scan → Filter(SQL) → ForeignScan(join) → Rank`
//!    (graph × relational × foreign × vector) == the manual join; a `Named` foreign source
//!    with no registry FAILS CLOSED (the SSRF/fail-closed analog at the plan seam).
//!  * EG-386 — geo × vector × temporal: `SpatialScan(R-tree bbox) → Filter(DWithin) →
//!    Rank → AsOf` narrows spatially, refines by distance, ranks by similarity, then keeps
//!    only the temporally-live.
//!  * EG-387 — tensor × graph × vector + CAS dedup: `TensorScan → TensorOp(CAS) →
//!    Traverse → Rank` derives + content-addresses tensors (identical derivations dedup to
//!    ONE blob) while the derived-frame rows traverse into a vector rank.
//!  * EG-388 — CEP × graph × tsdb: `Scan → Cep → Traverse → Window` — a matched event
//!    session traverses to its sensor readings, then a native tsdb window aggregates them,
//!    all in ONE plan.
//!  * EG-389 — probabilistic × OWL × vector + MMR: `Reason → Rank → Probabilistic →
//!    RankMmr` reranks an OWL-reasoned, vector-filtered set by a closed-form probabilistic
//!    score, then diversifies with Maximal Marginal Relevance.

#![allow(unused_imports)]

use eg_core::compute::semantic::SemanticStore;
use eg_core::graph::{GraphCore, GraphView};
use serde_json::json;

use crate::algebra::{Op, Plan, Pred};
use crate::exec::{execute, PlanCtx, PlanExt};

fn blob(v: serde_json::Value) -> Vec<u8> {
    rmp_serde::to_vec_named(&v).unwrap()
}

// ─────────────────────────────────────────────────────────────────────────────
// EG-384 — bitemporal multi-hop: AsOf(source) → Rank → Reason<iri> → Traverse
//          (temporal × vector × OWL × graph, string→IRI bridge)
// ─────────────────────────────────────────────────────────────────────────────

/// The ontology: `<ex/Article> ⊑ <ex/Paper> ⊑ <ex/ScholarlyWork>` — a TWO-hop subclass
/// chain, so an `<ex/Article>`-typed node is an inferred `ScholarlyWork` transitively.
#[cfg(feature = "owl")]
const SCHOLARLY_ONT: &str = "@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n\
    <http://ex/Paper> rdfs:subClassOf <http://ex/ScholarlyWork> .\n\
    <http://ex/Article> rdfs:subClassOf <http://ex/Paper> .\n";

/// Four bare-string-typed docs with bitemporal validity windows (`valid_from`/
/// `valid_until`, epoch seconds) + embeddings + a `CITES` edge each to a distinct target:
///   * `p1` Paper,   live `[0,100)`      — member, dies at 100
///   * `p2` Paper,   live `[0,∞)`        — member, always live
///   * `p3` Topic,   live `[0,∞)`        — NON-member (no subclass path to ScholarlyWork)
///   * `p4` Article, live `[0,100)`      — member (2-hop), dies at 100
///
/// Bridged to IRIs by the REASON target's namespace (`<http://ex/…>`), so a plain
/// `{"type":"Paper"}` resolves to `<http://ex/Paper>` (string→IRI bridge, EG-376).
#[cfg(feature = "owl")]
fn bitemporal_scholarly() -> (GraphView, SemanticStore) {
    let core = GraphCore::new();
    core.add_node(
        "p1".into(),
        blob(json!({ "type": "Paper", "valid_from": 0, "valid_until": 100 })),
    );
    core.add_node(
        "p2".into(),
        blob(json!({ "type": "Paper", "valid_from": 0 })),
    );
    core.add_node(
        "p3".into(),
        blob(json!({ "type": "Topic", "valid_from": 0 })),
    );
    core.add_node(
        "p4".into(),
        blob(json!({ "type": "Article", "valid_from": 0, "valid_until": 100 })),
    );
    // Targets (always live: no valid_from ⇒ from=0, no valid_until ⇒ open).
    for t in ["t1", "t2", "t3", "t4"] {
        core.add_node(t.into(), blob(json!({ "type": "Target", "valid_from": 0 })));
    }
    for (s, t) in [("p1", "t1"), ("p2", "t2"), ("p3", "t3"), ("p4", "t4")] {
        core.add_edge(s.into(), t.into(), blob(json!({ "relationship": "CITES" })))
            .unwrap();
    }
    // query ≈ [1,0,0]: p2 > p1 > p4 > p3 among the p-nodes (p3 highest? no — give p3 a
    // farther vector so the rank order among MEMBERS is deterministic: p2 > p1 > p4).
    let mut semantic = SemanticStore::new();
    semantic
        .add_embedding("p1".into(), vec![0.90, 0.44, 0.0])
        .unwrap();
    semantic
        .add_embedding("p2".into(), vec![0.99, 0.10, 0.0])
        .unwrap();
    semantic
        .add_embedding("p3".into(), vec![0.70, 0.71, 0.0])
        .unwrap();
    semantic
        .add_embedding("p4".into(), vec![0.80, 0.60, 0.0])
        .unwrap();
    (core.analysis_snapshot(), semantic)
}

/// THE bitemporal cross-modal proof (CONCEPT:EG-KG.compute.asof-valid-t-sources): `AsOf{Valid}@t` SOURCES every node
/// live at `t`, a vector `Rank` orders them, mid-plan `Reason<ScholarlyWork>` keeps only
/// the OWL-inferred members (dropping the Topic `p3`), and `Traverse<CITES>` reaches their
/// targets. Re-running at a LATER instant re-selects a SMALLER live member set — so a
/// member whose validity window has closed no longer reaches its target ("only
/// inferred-AT-t"). The reasoner's per-member confidence is uniform for this HARD
/// ontology and is intentionally superseded by the downstream vector `Rank` (the
/// time-DECAY of confidence is the server `OwlReason` surface, which threads the real
/// wall-clock `now`; see `exec::reason_source`), so the surviving order is the vector-rank
/// order — asserted below.
#[cfg(feature = "owl")]
#[test]
fn bitemporal_reason_vector_traverse_asof_eg384() {
    let (view, semantic) = bitemporal_scholarly();
    let ctx = PlanCtx::new(&view, &semantic);

    let plan_at = |ts: f64| {
        Plan::new(vec![
            // SOURCE: every node live at `ts` on the Valid timeline.
            Op::AsOf {
                ts,
                axis: eg_types::wire::TimeAxis::Valid,
            },
            // VECTOR: order the live set by similarity (embedded nodes only).
            Op::Rank {
                query: vec![1.0, 0.0, 0.0],
            },
            // OWL: keep only inferred ScholarlyWork members, in rank order (p3 dropped).
            Op::Reason {
                target_class: "<http://ex/ScholarlyWork>".into(),
                ontology: SCHOLARLY_ONT.into(),
            },
            // GRAPH: reach each surviving member's CITES target.
            Op::Traverse {
                rel: "CITES".into(),
                min: 1,
                max: 1,
            },
        ])
    };

    // t = 50: all four p-nodes are live ⇒ members {p1,p2,p4} (p3 Topic dropped), in the
    // vector-rank order p2 > p1 > p4 ⇒ targets t2, t1, t4. The Topic's target t3 is
    // NEVER reached (reason filtered p3 out mid-plan).
    let early = execute(&plan_at(50.0), &ctx).unwrap().ids();
    assert_eq!(
        early,
        vec!["t2".to_string(), "t1".to_string(), "t4".to_string()],
        "at t=50 the inferred-and-live members reach their targets in vector-rank order"
    );
    assert!(
        !early.contains(&"t3".to_string()),
        "the Topic p3 is not a ScholarlyWork — mid-plan Reason drops it, so t3 is unreached"
    );

    // t = 150: p1 and p4 have expired (window `[0,100)`); only p2 is still a LIVE member,
    // so only its target t2 is reached — the "inferred-AT-t" temporal re-selection.
    let late = execute(&plan_at(150.0), &ctx).unwrap().ids();
    assert_eq!(
        late,
        vec!["t2".to_string()],
        "at t=150 only the still-live member p2 reaches its target; expired members drop out"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// EG-385 — federation fusion: Scan → Filter(SQL) → ForeignScan(join) → Rank
//          (graph × relational × foreign × vector) + fail-closed named source
// ─────────────────────────────────────────────────────────────────────────────

/// Spawn a one-shot HTTP/JSON server on an ephemeral loopback port returning `body` — the
/// stand-in for a foreign SQL / SPARQL-`SERVICE` endpoint (the SAME mock the federation
/// proofs use).
///
/// Regression note (GOC-40): the doc comment here used to claim host-level SSRF vetting
/// was enforced only at the server surface, "NOT at this dep-free plan seam, which only
/// resolves a name -> source" — stale as of `federation.rs`'s
/// `validate_http_json_target`/`HTTP_JSON_FEDERATION_ALLOW_ENV` (this SAME crate, called
/// from this SAME `HttpJsonSource` execution path), which unconditionally rejects an
/// unallowlisted loopback/private destination. The caller below now opts its own mock
/// server's exact origin in via `federation_tests::MockHttpAllowGuard`, the same
/// production allowlist mechanism (not a bypass), matching what every OTHER mock-HTTP
/// federation test in this crate (`federation_tests.rs`) already does.
#[cfg(feature = "federation")]
fn spawn_mock_json(body: &'static str) -> String {
    use std::io::{Read, Write};
    use std::net::TcpListener;
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock http");
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        if let Ok((mut stream, _)) = listener.accept() {
            let mut buf = [0u8; 1024];
            let _ = stream.read(&mut buf);
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = stream.write_all(resp.as_bytes());
        }
    });
    format!("http://{addr}/")
}

/// THE federation fusion proof (CONCEPT:EG-KG.compute.one-plan-fuses-four): ONE plan fuses FOUR modalities —
/// `Scan(Doc)` (graph) → `Filter(year>2023)` (real DataFusion / relational SQL) →
/// `ForeignScan(join)` (a foreign JSON/SQL source, foreign∩local) → `Rank(q)` (vector) —
/// and equals the hand-wired join (local-filtered ∩ foreign, vector-ranked). Reuses the
/// canonical `crate::fixture` (Docs d1..d5 with years + embeddings).
#[cfg(feature = "federation")]
#[test]
fn federation_sparql_local_vector_foreign_sql_one_plan_eg385() {
    use eg_types::wire::{ForeignSourceSpec, HttpFieldMap};
    use std::collections::HashSet;

    let fx = crate::fixture::build();
    let query = crate::fixture::query_vec();

    // The foreign source returns {d2, d4} — the "SPARQL SERVICE / foreign SQL" result set.
    let addr = spawn_mock_json(r#"[{"k":"d2"},{"k":"d4"}]"#);
    // Opt this test's own loopback mock server into the production SSRF allowlist —
    // see the `spawn_mock_json` doc comment above for why this is required now.
    let _allow = crate::federation_tests::MockHttpAllowGuard::new(&addr);
    let foreign = ForeignSourceSpec::HttpJson {
        url: addr,
        json_path: "".into(),
        field_map: HttpFieldMap {
            id: "k".into(),
            score: None,
        },
    };

    let plan = Plan::new(vec![
        Op::Scan {
            label: "Doc".into(),
        },
        Op::Filter {
            preds: vec![Pred::GtNum {
                prop: "year".into(),
                n: 2023.0,
            }],
        },
        Op::ForeignScan {
            source: Box::new(foreign),
            join: true,
        },
        Op::Rank {
            query: query.clone(),
        },
        Op::Limit { k: 10 },
    ]);
    let ctx = PlanCtx::new(&fx.view, &fx.semantic);
    let fused = execute(&plan, &ctx).unwrap().ids();

    // Oracle: local(year>2023)={d1,d2,d4,d5} ∩ foreign{d2,d4} = {d2,d4}; ranked d2 > d4.
    let local: HashSet<&str> = ["d1", "d2", "d4", "d5"].into_iter().collect();
    let fgn: HashSet<&str> = ["d2", "d4"].into_iter().collect();
    let joined: HashSet<String> = local.intersection(&fgn).map(|s| s.to_string()).collect();
    let manual: Vec<String> = fx
        .semantic
        .semantic_search(&query, 32)
        .into_iter()
        .map(|(id, _)| id)
        .filter(|id| joined.contains(id))
        .collect();
    assert_eq!(
        fused, manual,
        "federated 4-modal plan must equal the manual join"
    );
    assert_eq!(fused, vec!["d2".to_string(), "d4".to_string()]);
}

/// FAIL-CLOSED (CONCEPT:EG-KG.compute.one-plan-fuses-four, the SSRF analog at the plan seam): a `Named` foreign source
/// with NO `ForeignSourceRegistry` attached to the ctx is a CLEAN typed error, never a
/// silent pass-through — so an unbound/untrusted foreign reference cannot leak data. (The
/// host-level SSRF allowlist that refuses internal-range peers lives at the server surface.)
#[cfg(feature = "federation")]
#[test]
fn federation_named_source_fails_closed_eg385() {
    use eg_types::wire::ForeignSourceSpec;
    let fx = crate::fixture::build();
    let plan = Plan::new(vec![Op::ForeignScan {
        source: Box::new(ForeignSourceSpec::Named {
            name: "untrusted-peer".into(),
        }),
        join: false,
    }]);
    // No `.with_foreign(...)` on the ctx ⇒ the named source cannot resolve ⇒ error.
    let err = execute(&plan, &PlanCtx::new(&fx.view, &fx.semantic)).unwrap_err();
    assert!(
        err.contains("untrusted-peer") && err.contains("ForeignSourceRegistry"),
        "an unbound Named foreign source must fail closed with a precise error, got: {err}"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// EG-386 — geo × vector × temporal: SpatialScan → Filter(DWithin) → Rank → AsOf
// ─────────────────────────────────────────────────────────────────────────────

/// A `Place` layer of point geometries with bitemporal validity + embeddings:
///   * `near_live`  (1,1)  live `[0,∞)`     — inside bbox, within 5 of origin, live
///   * `near_dead`  (2,2)  live `[0,100)`   — inside bbox, within 5, but EXPIRED at t=150
///   * `far_in_box` (9,9)  live `[0,∞)`     — inside bbox but >5 from origin (DWithin drops)
///   * `outside`   (50,50) live `[0,∞)`     — OUTSIDE the bbox (R-tree scan drops)
///
/// Query ≈ [1,0] ranks `near_live` before `near_dead`.
#[cfg(feature = "geo")]
fn places() -> (GraphView, SemanticStore) {
    let core = GraphCore::new();
    let mk = |id: &str, x: f64, y: f64, until: Option<i64>| {
        let mut props =
            json!({ "type": "Place", "geometry": format!("POINT({x} {y})"), "valid_from": 0 });
        if let Some(u) = until {
            props["valid_until"] = json!(u);
        }
        core.add_node(id.into(), blob(props));
    };
    mk("near_live", 1.0, 1.0, None);
    mk("near_dead", 2.0, 2.0, Some(100));
    mk("far_in_box", 9.0, 9.0, None);
    mk("outside", 50.0, 50.0, None);

    let mut semantic = SemanticStore::new();
    semantic
        .add_embedding("near_live".into(), vec![0.99, 0.10])
        .unwrap();
    semantic
        .add_embedding("near_dead".into(), vec![0.90, 0.44])
        .unwrap();
    semantic
        .add_embedding("far_in_box".into(), vec![0.80, 0.60])
        .unwrap();
    semantic
        .add_embedding("outside".into(), vec![0.70, 0.71])
        .unwrap();
    (core.analysis_snapshot(), semantic)
}

/// THE geo cross-modal proof (CONCEPT:EG-KG.compute.spatialscan-packed-hilbert-r): `SpatialScan` (a packed-Hilbert-R-tree bbox
/// scan) narrows to the in-box places, `Filter(SpatialDWithin ≤5 of origin)` refines by
/// planar distance, `Rank` orders by vector similarity, and `AsOf{Valid}@t` keeps only the
/// temporally-live — geometry × distance × vector × time fused in ONE plan.
#[cfg(feature = "geo")]
#[test]
fn geo_vector_temporal_fusion_eg386() {
    use eg_types::wire::TimeAxis;
    let (view, semantic) = places();
    let ctx = PlanCtx::new(&view, &semantic);

    // bbox [0,0,10,10] admits near_live, near_dead, far_in_box (not `outside` at 50,50).
    let base = |ts: f64| {
        Plan::new(vec![
            Op::SpatialScan {
                layer: "Place".into(),
                bbox: [0.0, 0.0, 10.0, 10.0],
            },
            Op::Filter {
                preds: vec![Pred::SpatialDWithin {
                    column: "geometry".into(),
                    wkt: "POINT(0 0)".into(),
                    distance: 5.0,
                }],
            },
            Op::Rank {
                query: vec![1.0, 0.0],
            },
            Op::AsOf {
                ts,
                axis: TimeAxis::Valid,
            },
        ])
    };

    // t=50: DWithin keeps near_live (√2) + near_dead (√8), drops far_in_box (√162);
    // both live ⇒ ranked near_live > near_dead.
    let early = execute(&base(50.0), &ctx).unwrap().ids();
    assert_eq!(
        early,
        vec!["near_live".to_string(), "near_dead".to_string()],
        "R-tree bbox + DWithin narrow to the two near places, vector-ranked, both live at t=50"
    );

    // t=150: near_dead's window `[0,100)` has closed ⇒ only near_live survives.
    let late = execute(&base(150.0), &ctx).unwrap().ids();
    assert_eq!(
        late,
        vec!["near_live".to_string()],
        "the expired near_dead drops out at t=150 — temporal leg composes after geo+vector"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// EG-387 — tensor × graph × vector + CAS dedup:
//          TensorScan → TensorOp(CAS) → Traverse → Rank
// ─────────────────────────────────────────────────────────────────────────────

/// Three `Frame` nodes carrying the SAME 2×3 tensor, each `DERIVES` a distinct `Doc`, with
/// embeddings on the docs so a downstream vector `Rank` orders the reached set.
#[cfg(feature = "tensor")]
fn frames_deriving_docs() -> (GraphView, SemanticStore) {
    use eg_tensor::{Buffer, Tensor};
    let core = GraphCore::new();
    let t = Tensor::new(vec![2, 3], Buffer::F32(vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0])).unwrap();
    let tv = serde_json::to_value(&t).unwrap();
    for id in ["F1", "F2", "F3"] {
        core.add_node(id.into(), blob(json!({ "type": "Frame", "tensor": tv })));
    }
    for id in ["docA", "docB", "docC"] {
        core.add_node(id.into(), blob(json!({ "type": "Doc" })));
    }
    for (f, d) in [("F1", "docA"), ("F2", "docB"), ("F3", "docC")] {
        core.add_edge(
            f.into(),
            d.into(),
            blob(json!({ "relationship": "DERIVES" })),
        )
        .unwrap();
    }
    let mut semantic = SemanticStore::new();
    semantic
        .add_embedding("docA".into(), vec![0.90, 0.44])
        .unwrap();
    semantic
        .add_embedding("docB".into(), vec![0.99, 0.10])
        .unwrap(); // closest
    semantic
        .add_embedding("docC".into(), vec![0.60, 0.80])
        .unwrap();
    (core.analysis_snapshot(), semantic)
}

/// THE tensor cross-modal proof (CONCEPT:EG-KG.compute.tensorscan-frame-seeds): `TensorScan(Frame)` seeds the
/// tensor-bearing rows, `TensorOp(Reduce, CAS-write-back)` derives + content-addresses a
/// tensor per frame (three IDENTICAL derivations dedup to ONE CAS blob, EG-304),
/// `Traverse<DERIVES>` walks from the surviving frames into their docs, and `Rank`
/// vector-orders the reached docs — tensor × graph × vector fused in ONE plan, and the CAS
/// contents asserted alongside the RowSet.
#[cfg(feature = "tensor")]
#[test]
fn tensor_graph_vector_cas_dedup_eg387() {
    use eg_tensor::{content_hash, Buffer, Tensor, TensorStore};
    use eg_types::wire::{TensorOpKind, TensorReduceKind};
    use std::sync::Mutex;

    let (view, semantic) = frames_deriving_docs();
    let store = Mutex::new(TensorStore::new());
    let ctx = PlanCtx::new(&view, &semantic).with_tensor_store(&store);

    let plan = Plan::new(vec![
        Op::TensorScan {
            layer: "Frame".into(),
        },
        Op::TensorOp {
            kind: TensorOpKind::Reduce {
                axis: 1,
                kind: TensorReduceKind::Mean,
            },
        },
        Op::Traverse {
            rel: "DERIVES".into(),
            min: 1,
            max: 1,
        },
        Op::Rank {
            query: vec![1.0, 0.0],
        },
        Op::Limit { k: 10 },
    ]);
    let reached = execute(&plan, &ctx).unwrap().ids();
    assert_eq!(
        reached,
        vec!["docB".to_string(), "docA".to_string(), "docC".to_string()],
        "the tensor-derived frames traverse into their docs, vector-ranked docB > docA > docC"
    );

    // CAS: three identical Mean-over-axis-1 derivations content-address to ONE dedup'd blob.
    let derived = Tensor::new(vec![2, 3], Buffer::F32(vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0]))
        .unwrap()
        .reduce(1, eg_tensor::ReduceKind::Mean)
        .unwrap();
    let store = store.into_inner().unwrap();
    assert_eq!(
        store.len(),
        1,
        "identical derived tensors must dedup to ONE CAS blob"
    );
    assert!(
        store.contains(&content_hash(&derived.to_blob())),
        "the derived tensor must be retrievable from the CAS by its content hash"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// EG-388 — CEP × graph × tsdb: Scan → Cep → Traverse → Window
// ─────────────────────────────────────────────────────────────────────────────

/// A completed `login → logout` session (`E1@10 → E2@15`, within a 20-unit window) plus a
/// dangling login (`E3@100`, no logout within 20). Each matched event `TRIGGERS` a `Reading`
/// sensor node carrying a `valid_from` timestamp + a `value` — the tsdb series the window
/// aggregates. The dangling `E3` triggers a reading too, but E3 never matches the CEP, so
/// its reading is never reached.
#[cfg(all(feature = "stream", feature = "timeseries"))]
fn sessions_with_readings() -> (GraphView, SemanticStore) {
    let core = GraphCore::new();
    core.add_node(
        "E1".into(),
        blob(json!({ "type": "Event", "ts": 10, "key": "login" })),
    );
    core.add_node(
        "E2".into(),
        blob(json!({ "type": "Event", "ts": 15, "key": "logout" })),
    );
    core.add_node(
        "E3".into(),
        blob(json!({ "type": "Event", "ts": 100, "key": "login" })),
    );
    // Readings in one 10s bucket: r1@1s=10, r2@2s=20 (from the matched session); r3@3s=999
    // from the dangling E3 (must NOT be aggregated). valid_from is in the same unit Window
    // reads; value is the aggregated column.
    for (id, ts, val) in [("r1", 1i64, 10.0), ("r2", 2, 20.0), ("r3", 3, 999.0)] {
        core.add_node(
            id.into(),
            blob(json!({ "type": "Reading", "valid_from": ts, "value": val })),
        );
    }
    for (e, r) in [("E1", "r1"), ("E2", "r2"), ("E3", "r3")] {
        core.add_edge(
            e.into(),
            r.into(),
            blob(json!({ "relationship": "TRIGGERS" })),
        )
        .unwrap();
    }
    (core.analysis_snapshot(), SemanticStore::new())
}

/// THE CEP cross-modal proof (CONCEPT:EG-KG.compute.scan-event-seeds-stream): `Scan(Event)` seeds the stream, `Cep`
/// detects the `login→logout` session (keeping only E1,E2 — E3's login has no logout in
/// the window), `Traverse<TRIGGERS>` reaches ONLY the matched events' readings (r1,r2 — NOT
/// the dangling E3's r3), and a native eg-tsdb `Window` aggregates those readings into one
/// 10s bucket. CEP × graph × time-series in ONE plan; the dangling reading's value (999)
/// never pollutes the window because its event never matched.
#[cfg(all(feature = "stream", feature = "timeseries"))]
#[test]
fn cep_stream_traverse_tsdb_window_one_plan_eg388() {
    use eg_types::wire::{CepMatcherSpec, CepNodeSpec, CepPatternSpec, CepWindowSpec};
    let (view, semantic) = sessions_with_readings();
    let ctx = PlanCtx::new(&view, &semantic);

    let key = |k: &str| CepMatcherSpec {
        key: Some(k.into()),
        preds: vec![],
    };
    let plan = Plan::new(vec![
        Op::Scan {
            label: "Event".into(),
        },
        Op::Cep {
            pattern: CepPatternSpec {
                pattern: CepNodeSpec::Sequence(vec![key("login"), key("logout")]),
                window: CepWindowSpec::Sliding { size: 20 },
            },
        },
        Op::Traverse {
            rel: "TRIGGERS".into(),
            min: 1,
            max: 1,
        },
        Op::Window { secs: 10.0 },
    ]);
    let out = execute(&plan, &ctx).unwrap();

    // One 10s bucket (start 0) whose MEAN over r1(10)+r2(20) = 15 — r3(999) excluded
    // because E3 never matched the CEP, so r3 was never traversed into the window.
    let rows: Vec<(String, Option<f32>)> =
        out.rows().iter().map(|r| (r.id.clone(), r.score)).collect();
    assert_eq!(rows.len(), 1, "one aggregated bucket, got {rows:?}");
    assert_eq!(rows[0].0, "0", "bucket aligned to start 0");
    assert_eq!(
        rows[0].1,
        Some(15.0),
        "MEAN of the matched-session readings (10,20)=15; the dangling r3=999 is excluded"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// EG-389 — probabilistic × OWL × vector + MMR:
//          Reason → Rank → Probabilistic → RankMmr
// ─────────────────────────────────────────────────────────────────────────────

/// Inferred `ScholarlyWork` members carrying a `distribution` (a Beta/Gaussian belief) +
/// embeddings, plus a non-member `Topic`. Two members share the SAME embedding (so MMR's
/// diversity penalty is observable), a third is orthogonal.
#[cfg(all(feature = "owl", feature = "probabilistic"))]
fn beliefs_reasoned() -> (GraphView, SemanticStore) {
    use eg_types::Distribution;
    let core = GraphCore::new();
    let mk = |id: &str, ty: &str, d: Option<Distribution>| {
        let mut props = json!({ "type": ty });
        if let Some(dist) = d {
            props["distribution"] = serde_json::to_value(&dist).unwrap();
        }
        core.add_node(id.into(), blob(props));
    };
    // Members (Paper ⊑ ScholarlyWork via SCHOLARLY_ONT). Expectation(mean) orders
    // m_hi > m_mid > m_lo, but by a SMALL margin (9.0 > 8.5 > 8.0) so MMR's diversity
    // penalty for a near-duplicate (a shared vector, similarity 1.0) can outweigh the
    // 0.5-mean relevance gap and promote the orthogonal member.
    mk(
        "m_hi",
        "Paper",
        Some(Distribution::Gaussian {
            mean: 9.0,
            std: 1.0,
        }),
    );
    mk(
        "m_mid",
        "Paper",
        Some(Distribution::Gaussian {
            mean: 8.5,
            std: 1.0,
        }),
    );
    mk(
        "m_lo",
        "Paper",
        Some(Distribution::Gaussian {
            mean: 8.0,
            std: 1.0,
        }),
    );
    // Non-member Topic with a (high) distribution — must be dropped by Reason before it can
    // ever win the probabilistic rank.
    mk(
        "topic",
        "Topic",
        Some(Distribution::Gaussian {
            mean: 99.0,
            std: 1.0,
        }),
    );

    let mut semantic = SemanticStore::new();
    // m_hi and m_mid share the SAME vector (a near-duplicate pair); m_lo is orthogonal.
    semantic
        .add_embedding("m_hi".into(), vec![1.0, 0.0])
        .unwrap();
    semantic
        .add_embedding("m_mid".into(), vec![1.0, 0.0])
        .unwrap();
    semantic
        .add_embedding("m_lo".into(), vec![0.0, 1.0])
        .unwrap();
    semantic
        .add_embedding("topic".into(), vec![1.0, 0.0])
        .unwrap();
    (core.analysis_snapshot(), semantic)
}

/// THE probabilistic cross-modal proof (CONCEPT:EG-KG.compute.reason-scholarlywork-sources): `Reason<ScholarlyWork>` sources the
/// OWL-inferred members (dropping the high-distribution Topic), `Rank(q)` vector-filters/
/// orders them, `Probabilistic(Expectation)` RE-scores each surviving row by its stored
/// distribution's mean (so the belief with the highest expectation leads), and `RankMmr`
/// diversifies — demoting the near-duplicate of the leader in favour of the orthogonal
/// member. Uncertainty × OWL × vector × diversity, ONE plan.
#[cfg(all(feature = "owl", feature = "probabilistic"))]
#[test]
fn probabilistic_rerank_reasoned_vector_mmr_eg389() {
    use eg_types::wire::ProbQuery;
    let (view, semantic) = beliefs_reasoned();
    let ctx = PlanCtx::new(&view, &semantic);

    // Sanity: WITHOUT MMR, the probabilistic Expectation rank alone orders m_hi > m_mid >
    // m_lo (means 9 > 5 > 1); the Topic is absent (Reason dropped it).
    let ranked = Plan::new(vec![
        Op::Reason {
            target_class: "<http://ex/ScholarlyWork>".into(),
            ontology: SCHOLARLY_ONT.into(),
        },
        Op::Rank {
            query: vec![1.0, 0.0],
        },
        Op::Probabilistic {
            query: ProbQuery::Expectation,
        },
    ]);
    let ranked_ids = execute(&ranked, &ctx).unwrap().ids();
    assert!(
        !ranked_ids.contains(&"topic".to_string()),
        "the non-member Topic must be reasoned OUT before the probabilistic rank"
    );
    assert_eq!(
        ranked_ids.first(),
        Some(&"m_hi".to_string()),
        "highest-expectation belief leads the probabilistic rank, got {ranked_ids:?}"
    );

    // WITH MMR (λ=0.5): the leader m_hi is picked first; its near-duplicate m_mid (same
    // vector) is penalised for redundancy, so the orthogonal m_lo is promoted above it —
    // MMR diversified the reasoned+probabilistic set.
    let diversified = Plan::new(vec![
        Op::Reason {
            target_class: "<http://ex/ScholarlyWork>".into(),
            ontology: SCHOLARLY_ONT.into(),
        },
        Op::Rank {
            query: vec![1.0, 0.0],
        },
        Op::Probabilistic {
            query: ProbQuery::Expectation,
        },
        Op::RankMmr { lambda: 0.5, k: 3 },
    ]);
    let mmr_ids = execute(&diversified, &ctx).unwrap().ids();
    assert_eq!(
        mmr_ids.first(),
        Some(&"m_hi".to_string()),
        "MMR keeps the top-relevance leader first"
    );
    let pos = |id: &str| mmr_ids.iter().position(|x| x == id).unwrap();
    assert!(
        pos("m_lo") < pos("m_mid"),
        "MMR promotes the orthogonal m_lo above the near-duplicate m_mid, got {mmr_ids:?}"
    );
}
