//! EH-450 — the end-to-end oracle for an epistemic + FUSE + AS OF query, over three
//! surfaces that must agree:
//!
//!  * **UQL text** — `EVIDENCE FOR … |> AS OF … |> FUSE [RANK …] [TEXT …] |> LIMIT …
//!    WITH KNOWLEDGE (…)`, run as a statement (`uql::serve::run_statement`);
//!  * **the structured plan** — the same ops built by hand, executed by
//!    `eg_plan::execute`, its rows turned into a `KnowledgeSet` by the caller;
//!  * **separate surfaces** — the vector and the lexical ranking each run as its own
//!    single-modality plan over the same evidence/time candidate set, fused by the caller
//!    with the public RRF (`eg_text::rrf_fuse`).
//!
//! The UQL text must lower to the structured plan, all three must return the same ids
//! and scores in the same order, and every row's `WITH KNOWLEDGE` record must be the
//! caller-built `KnowledgeSet` record (projection, bitemporal window, epistemic
//! neighbourhood) — which proves `KnowledgeSet` is reachable from UQL and faithful.

use eg_core::compute::semantic::SemanticStore;
use eg_core::graph::{GraphCore, GraphView};
use eg_plan::uql::serve::run_statement;
use eg_plan::uql::{parse_statement, Body, Params};
use eg_plan::{execute, KnowledgeSet, Op, Plan, PlanCtx, RowSet};
use eg_text::TextIndex;
use eg_types::wire::{TimeAxis, UqlResult, UqlRow};
use serde_json::json;

const QUERY: &str = "EVIDENCE FOR 'c1' |> AS OF @500 \
                     |> FUSE [RANK BY ~[1, 0, 0, 0]] [TEXT 'pump pressure'] |> LIMIT 3 \
                     WITH KNOWLEDGE (note)";

fn blob(v: serde_json::Value) -> Vec<u8> {
    rmp_serde::to_vec_named(&v).unwrap()
}

/// Claim `c1` supported by five evidence nodes. At t=500, `e4` has expired and `e5`
/// is not yet valid, so the candidate set is `e1`–`e3`; the vector and the lexical
/// rankings disagree on its order. `x` contradicts `e1`, so `e1`'s knowledge record
/// carries a contradiction.
struct Fixture {
    view: GraphView,
    semantic: SemanticStore,
    text: TextIndex,
}

fn fixture() -> Fixture {
    let core = GraphCore::new();
    core.add_node("c1".into(), blob(json!({ "type": "Claim" })));
    core.add_node("x".into(), blob(json!({ "type": "Evidence" })));
    let evidence = [
        ("e1", 0, 1000, "pump pressure dropped twice"),
        ("e2", 0, 1000, "pressure logs nominal"),
        ("e3", 100, 1000, "pump pump pressure pressure alarm"),
        ("e4", 0, 100, "pump pressure (expired reading)"),
        ("e5", 2000, 3000, "pump pressure (future reading)"),
    ];
    let mut text = TextIndex::in_memory().unwrap();
    for (id, from, until, note) in evidence {
        let props = json!({
            "type": "Evidence", "note": note, "valid_from": from, "valid_until": until,
        });
        core.add_node(id.into(), blob(props));
        core.add_edge(
            id.into(),
            "c1".into(),
            blob(json!({ "relationship": "SUPPORTS" })),
        )
        .unwrap();
        text.upsert(id, note);
    }
    core.add_edge(
        "x".into(),
        "e1".into(),
        blob(json!({ "relationship": "CONTRADICTS" })),
    )
    .unwrap();
    text.commit().unwrap();
    let mut semantic = SemanticStore::new();
    for (id, v) in [
        ("e1", [0.60, 0.80, 0.0, 0.0]),
        ("e2", [0.99, 0.10, 0.0, 0.0]),
        ("e3", [0.10, 0.99, 0.0, 0.0]),
        ("e4", [1.00, 0.00, 0.0, 0.0]),
        ("e5", [1.00, 0.00, 0.0, 0.0]),
    ] {
        semantic.add_embedding(id.into(), v.to_vec()).unwrap();
    }
    Fixture {
        view: core.analysis_snapshot(),
        semantic,
        text,
    }
}

/// The candidate stages every surface shares: the evidence for `c1`, live at t=500.
fn candidates() -> Vec<Op> {
    vec![
        Op::EvidenceFor {
            claim_id: "c1".into(),
        },
        Op::AsOf {
            ts: 500.0,
            axis: TimeAxis::Valid,
        },
    ]
}

fn vector_rank() -> Op {
    Op::Rank {
        query: vec![1.0, 0.0, 0.0, 0.0],
    }
}

fn text_rank() -> Op {
    Op::RankText {
        query: "pump pressure".into(),
    }
}

fn structured() -> Plan {
    let mut ops = candidates();
    ops.push(Op::FuseRrf {
        branches: vec![vec![vector_rank()], vec![text_rank()]],
        k: 0.0,
    });
    ops.push(Op::Limit { k: 3 });
    Plan::new(ops)
}

/// One single-modality surface: the candidates, then one ranking.
fn surface(ctx: &PlanCtx, rank: Op) -> Vec<String> {
    let mut ops = candidates();
    ops.push(rank);
    execute(&Plan::new(ops), ctx).unwrap().ids()
}

fn separate_surfaces(ctx: &PlanCtx) -> RowSet {
    let vector = surface(ctx, vector_rank());
    let lexical = surface(ctx, text_rank());
    let fused = eg_text::rrf_fuse(&[&vector, &lexical], eg_text::RRF_K);
    RowSet::from_scored(fused).limit(3)
}

fn scored(rows: &RowSet) -> Vec<(String, Option<f32>)> {
    rows.rows()
        .iter()
        .map(|r| (r.id.clone(), r.score))
        .collect()
}

fn uql_rows(ctx: &PlanCtx) -> Vec<UqlRow> {
    let stmt = parse_statement(QUERY, &Params::new()).unwrap();
    assert_eq!(
        stmt.body,
        Body::Pipeline(structured()),
        "UQL must lower to the structured plan"
    );
    match run_statement(&stmt, ctx).unwrap() {
        UqlResult::Rows { rows, .. } => rows,
        other => panic!("rows expected, got {other:?}"),
    }
}

#[test]
fn uql_structured_and_separate_surfaces_agree_with_knowledge() {
    let fx = fixture();
    let ctx = PlanCtx::new(&fx.view, &fx.semantic).with_text(&fx.text);

    let from_plan = execute(&structured(), &ctx).unwrap();
    let from_surfaces = separate_surfaces(&ctx);
    assert_eq!(
        scored(&from_plan),
        scored(&from_surfaces),
        "the fused plan must equal the caller-fused single-modality surfaces"
    );
    let ids = from_plan.ids();
    assert!(!ids.iter().any(|id| id == "e4" || id == "e5"), "{ids:?}");
    assert!(ids.len() >= 2, "the fixture must exercise fusion: {ids:?}");

    let rows = uql_rows(&ctx);
    let from_uql: Vec<(String, Option<f32>)> =
        rows.iter().map(|r| (r.id.clone(), r.score)).collect();
    assert_eq!(from_uql, scored(&from_plan));

    let expected = KnowledgeSet::from_rowset(&from_plan, &fx.view, &["note"]);
    for (row, record) in rows.iter().zip(&expected.rows) {
        let knowledge = row.knowledge.as_ref().expect("WITH KNOWLEDGE row record");
        assert_eq!(knowledge.projection, record.projection, "{}", row.id);
        assert_eq!(
            (knowledge.valid_from, knowledge.valid_until),
            record.valid_time
        );
        assert_eq!(knowledge.confidence, record.confidence);
        assert_eq!(knowledge.contradiction_ids, record.contradiction_ids);
        assert_eq!(knowledge.source_refs, record.source_refs);
        assert!(knowledge.epistemic_resolved);
        assert!(row.proof.is_none(), "no WITH PROOF was asked");
    }
    let e1 = rows
        .iter()
        .find(|r| r.id == "e1")
        .expect("e1 is live at t=500");
    let e1 = e1.knowledge.as_ref().unwrap();
    assert_eq!(e1.contradiction_ids, vec!["x".to_string()]);
    assert_eq!(
        e1.projection,
        Some(json!({ "note": "pump pressure dropped twice" }))
    );
}
