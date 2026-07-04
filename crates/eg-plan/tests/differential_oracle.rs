//! Differential multi-surface oracle (CONCEPT:EG-400) — the highest-ROI proof from the
//! external review: **modality interchangeability**. Each curated hybrid query is
//! expressed through MULTIPLE query surfaces and every surface must lower to the SAME
//! plan AND execute to the byte-identical [`RowSet`]. If any seam lowers one surface to
//! a different plan/result, this trips.
//!
//! Surfaces exercised here (all in-process against eg-plan's public API):
//!  * **UQL text** — `eg_plan::uql::parse(text)`
//!  * **Rust builder** — a hand-built `Plan { ops: vec![Op::…] }`
//!  * **the separate-surfaces oracle** — `eg_plan::oracle::separate_surfaces`, the
//!    three-siloed-round-trip baseline the fused planner must reproduce (cross-checked
//!    for the classic MATCH→WHERE→TRAVERSE→RANK→LIMIT shape).
//!
//! The pgwire SQL/UQL WIRE surface is a fourth leg proved in the top-level
//! `tests/query_harness_pgwire.rs` (it needs the `pgwire` listener from the facade
//! crate, not reachable from an eg-plan integration test).
//!
//! Covered composed pipelines (per the review):
//!  * `MATCH → WHERE → TRAVERSE → RANK → AS OF → LIMIT`
//!  * `REASON → RANK → TRAVERSE`  (owl)
//!
//! SKIPPED legs (documented, not faked):
//!  * **UQL `FUSE`** — the UQL front-end grammar lists `FUSE` as a seam but the parser
//!    implements NO `FUSE` stage (see `uql/parser.rs::parse_stage`), so RRF fusion is
//!    only expressible via the Rust builder / wire. Its builder path is exercised in
//!    `composition_matrix.rs` (text feature). No fake UQL assertion is made.

mod common;

use common::*;
use eg_core::compute::semantic::SemanticStore;
use eg_core::graph::GraphView;
use eg_plan::{execute, Op, Plan, PlanCtx, Pred, RowSet};
use eg_types::wire::TimeAxis;

/// Run one query through the UQL front-end AND the Rust builder, assert the two lower
/// to the IDENTICAL plan AST, and that they EXECUTE to the identical `RowSet` (ids +
/// vector scores, in order). Returns the shared `RowSet` for further assertions.
fn agree(uql: &str, builder: &Plan, view: &GraphView, semantic: &SemanticStore) -> RowSet {
    let parsed =
        eg_plan::uql::parse(uql).unwrap_or_else(|e| panic!("UQL parse `{uql}`: {}", e.msg));
    assert_eq!(
        &parsed, builder,
        "UQL and the Rust builder must lower to the IDENTICAL plan AST\n  UQL: {uql}"
    );

    let ctx = PlanCtx::new(view, semantic);
    let from_uql = execute(&parsed, &ctx).expect("execute UQL-lowered plan");
    let from_builder = execute(builder, &ctx).expect("execute builder plan");
    assert_eq!(
        stable_rows(&from_uql),
        stable_rows(&from_builder),
        "UQL and builder must execute to the byte-identical RowSet\n  UQL: {uql}"
    );
    from_uql
}

/// The canonical hybrid: relational FILTER → graph TRAVERSE → vector RANK → LIMIT, over
/// all three surfaces (UQL, builder, AND the separate-surfaces oracle). This is the
/// load-bearing interchangeability proof: the human-writable text, the programmatic
/// builder, and the three-round-trip siloed baseline all agree.
#[test]
fn classic_filter_traverse_rank_all_surfaces() {
    let (view, semantic) = build_docs();

    let builder = Plan::new(vec![
        Op::Scan {
            label: "Doc".into(),
        },
        Op::Filter {
            preds: vec![Pred::GtNum {
                prop: "year".into(),
                n: 2024.0,
            }],
        },
        Op::Traverse {
            rel: "CITES".into(),
            min: 1,
            max: 2,
        },
        Op::Rank { query: query_vec() },
        Op::Limit { k: 10 },
    ]);
    let uql = "MATCH (:Doc) WHERE year > 2024 \
               |> TRAVERSE -[:CITES]->{1,2} \
               |> RANK BY ~[1.0, 0.0, 0.0, 0.0] |> LIMIT 10";

    let rs = agree(uql, &builder, &view, &semantic);
    assert_eq!(
        ids(&rs),
        vec!["d2", "d4", "d3"],
        "expected the ranked frontier"
    );

    // Third surface: the separate-surfaces oracle (three siloed round-trips).
    let oracle = eg_plan::oracle::separate_surfaces(
        &view,
        &semantic,
        "Doc",
        &[Pred::GtNum {
            prop: "year".into(),
            n: 2024.0,
        }],
        "CITES",
        1,
        2,
        &query_vec(),
        10,
    )
    .unwrap();
    assert_eq!(
        stable_rows(&rs),
        stable_rows(&oracle),
        "the fused surfaces must equal the three-round-trip siloed oracle"
    );
}

/// The full composed chain the review names: MATCH → WHERE → TRAVERSE → RANK → AS OF →
/// LIMIT. The `AS OF` pins a valid-time instant; the Doc nodes carry no validity window
/// so they are live at every instant (`live_at` defaults `valid_from`→0, open end), so
/// the answer equals the AsOf-free classic — and BOTH surfaces must still lower the
/// `AS OF` op identically and execute identically. Proves the time op threads through
/// the mid-pipeline seam without perturbing the graph+vector legs.
#[test]
fn composed_traverse_rank_asof_limit_interchangeable() {
    let (view, semantic) = build_docs();

    let builder = Plan::new(vec![
        Op::Scan {
            label: "Doc".into(),
        },
        Op::Filter {
            preds: vec![Pred::GtNum {
                prop: "year".into(),
                n: 2024.0,
            }],
        },
        Op::Traverse {
            rel: "CITES".into(),
            min: 1,
            max: 2,
        },
        Op::Rank { query: query_vec() },
        Op::AsOf {
            ts: 1_700_000_000.0,
            axis: TimeAxis::Valid,
        },
        Op::Limit { k: 10 },
    ]);
    let uql = "MATCH (:Doc) WHERE year > 2024 \
               |> TRAVERSE -[:CITES]->{1,2} \
               |> RANK BY ~[1.0, 0.0, 0.0, 0.0] \
               |> AS OF @1700000000 |> LIMIT 10";

    let rs = agree(uql, &builder, &view, &semantic);
    assert_eq!(
        ids(&rs),
        vec!["d2", "d4", "d3"],
        "AS OF over always-live Docs keeps the ranked frontier order"
    );
}

/// `AS OF` that ACTUALLY filters (bi-temporal Event fixture), across UQL + builder. At
/// @175 e1 and e2 are live, e3 (starts 300) is not — proving the interchangeable surface
/// isn't a no-op: both lower the same `AsOf{Valid}` and drop the same row.
#[test]
fn asof_valid_time_interchangeable_and_selective() {
    let (view, semantic) = build_events();

    let builder = Plan::new(vec![
        Op::Scan {
            label: "Event".into(),
        },
        Op::AsOf {
            ts: 175.0,
            axis: TimeAxis::Valid,
        },
    ]);
    let uql = "MATCH (:Event) |> AS OF @175";
    let rs = agree(uql, &builder, &view, &semantic);
    assert_eq!(ids_sorted(&rs), vec!["e1".to_string(), "e2".to_string()]);

    // Transaction axis via `AS OF TX` is the SAME surface knob — proves the axis
    // keyword round-trips through both surfaces AND selects the OTHER timeline: these
    // Events carry no tx window, so their belief window defaults to `[0,∞)` and ALL are
    // "believed" at @175 (distinct from the valid axis above, which dropped e3). The two
    // surfaces must agree on this different answer just the same.
    let builder_tx = Plan::new(vec![
        Op::Scan {
            label: "Event".into(),
        },
        Op::AsOf {
            ts: 175.0,
            axis: TimeAxis::Transaction,
        },
    ]);
    let rs_tx = agree(
        "MATCH (:Event) |> AS OF TX @175",
        &builder_tx,
        &view,
        &semantic,
    );
    assert_eq!(
        ids_sorted(&rs_tx),
        vec!["e1".to_string(), "e2".to_string(), "e3".to_string()],
        "the transaction axis keeps all Events (tx window defaults to [0,∞))"
    );
}

/// A vector RANK followed by a graph-native RERANK (MENTIONS) — proves a two-ranker
/// hybrid is interchangeable across surfaces. `RERANK MENTIONS` re-orders the ranked set
/// by incoming-edge salience.
#[test]
fn rank_then_mentions_rerank_interchangeable() {
    let (view, semantic) = build_docs();
    let builder = Plan::new(vec![
        Op::Scan {
            label: "Doc".into(),
        },
        Op::Rank { query: query_vec() },
        Op::RankMentions {},
        Op::Limit { k: 5 },
    ]);
    let uql = "MATCH (:Doc) |> RANK BY ~[1.0,0.0,0.0,0.0] |> RERANK MENTIONS |> LIMIT 5";
    let rs = agree(uql, &builder, &view, &semantic);
    assert!(!rs.is_empty(), "the rerank chain produced rows");
}

/// The reasoning-seeded composed chain the review names: `REASON → RANK → TRAVERSE`.
/// `REASON <ScholarlyWork>` seeds the OWL-inferred members; a vector RANK orders them;
/// a graph TRAVERSE then expands the CITES frontier — all interchangeable UQL↔builder.
/// (UQL `REASON <iri>` lowers to `ontology: ""`, i.e. classify the in-graph axioms —
/// the fixture loads the TBox into the graph so both surfaces classify identically.)
#[cfg(feature = "owl")]
#[test]
fn reason_rank_traverse_interchangeable() {
    let (view, semantic) = build_reason_graph();
    let target = "<http://example.org/ScholarlyWork>";

    let builder = Plan::new(vec![
        Op::Reason {
            target_class: target.into(),
            ontology: String::new(),
        },
        Op::Rank {
            query: vec![1.0, 0.0, 0.0],
        },
        Op::Traverse {
            rel: "CITES".into(),
            min: 1,
            max: 1,
        },
        Op::Limit { k: 10 },
    ]);
    let uql = "MATCH (:ScholarlyWork) \
               |> REASON <http://example.org/ScholarlyWork> \
               |> RANK BY ~[1.0,0.0,0.0] \
               |> TRAVERSE -[:CITES]->{1,1} |> LIMIT 10";
    // NB: the UQL MATCH label is inert here — REASON is a SOURCE op that REPLACES the
    // input, so the leading Scan is overwritten by the reasoner's member set. To keep
    // the plan ASTs identical we compare against a builder that OMITS the Scan (REASON
    // is the true source), and prove the parsed UQL's REASON-onward tail matches.
    let parsed = eg_plan::uql::parse(uql).unwrap();
    // Drop the leading Scan the MATCH emits; REASON is the real source.
    let tail: Vec<Op> = parsed.ops.iter().skip(1).cloned().collect();
    assert_eq!(
        tail, builder.ops,
        "the REASON-onward pipeline must match the builder"
    );

    let ctx = PlanCtx::new(&view, &semantic);
    let from_uql = execute(&parsed, &ctx).unwrap();
    let from_builder = execute(&builder, &ctx).unwrap();
    assert_eq!(
        stable_rows(&from_uql),
        stable_rows(&from_builder),
        "REASON→RANK→TRAVERSE must execute identically whether the Scan precedes REASON or not"
    );
    // Concretely: inferred {p1,p2,p4}; ranked; CITES 1-hop reaches {c1,c2,c3}.
    let got = ids_sorted(&from_builder);
    assert_eq!(
        got,
        vec![
            "<http://example.org/c1>".to_string(),
            "<http://example.org/c2>".to_string(),
            "<http://example.org/c3>".to_string(),
        ],
        "REASON→RANK→TRAVERSE reaches the CITES frontier of the inferred ScholarlyWorks"
    );
}

/// A cost-model reorder must NOT change the answer set: filter-first and vector-first
/// lowerings of the SAME `(Filter, Rank)` pair produce the identical result set. This is
/// the optimizer half of interchangeability — the planner may pick either order but the
/// two surfaces (the two plans) must agree as sets.
#[test]
fn cost_reorder_is_answer_preserving() {
    use eg_plan::{CostModel, Order, Stats};
    let (view, semantic) = build_docs();
    let ctx = PlanCtx::new(&view, &semantic);

    let plan = vec![
        Op::Scan {
            label: "Doc".into(),
        },
        Op::Filter {
            preds: vec![Pred::GtNum {
                prop: "year".into(),
                n: 2022.0,
            }],
        },
        Op::Rank { query: query_vec() },
    ];
    let selective = Stats::estimate(10_000, 0.01, 10, semantic.len());
    let broad = Stats::estimate(10_000, 0.98, 10, semantic.len());
    assert_eq!(CostModel::order(&selective), Order::FilterFirst);
    assert_eq!(CostModel::order(&broad), Order::VectorFirst);

    let sel = CostModel::reorder_filter_rank(plan.clone(), &selective);
    let brd = CostModel::reorder_filter_rank(plan, &broad);
    let a = ids_sorted(&execute(&Plan::new(sel), &ctx).unwrap());
    let b = ids_sorted(&execute(&Plan::new(brd), &ctx).unwrap());
    assert_eq!(
        a, b,
        "the cost reorder must be answer-preserving across surfaces"
    );
}
