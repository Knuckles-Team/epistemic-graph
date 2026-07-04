//! Plan-snapshot regression (CONCEPT:EG-403). Snapshots the LOGICAL plan for a curated
//! set of hybrid queries so BOTH a lowering regression (the UQL front-end starts
//! producing a different AST) AND an optimizer-quality regression (a selective filter
//! silently stops being pushed ahead of a vector RANK) trip a snapshot diff. Each curated
//! query is ALSO asserted to lower identically from UQL and the Rust builder — the
//! snapshot is the shared, reviewed artifact both surfaces must match.
//!
//! There is no separate PHYSICAL plan IR exposed by eg-plan (a `Plan` is the logical
//! `Vec<Op>`; the cost model REWRITES that logical plan in place). So the "optimizer"
//! snapshots below capture the cost-reordered logical plan — the artifact whose quality
//! we guard against regression.

mod common;

use eg_plan::{CostModel, Op, Pred, Stats};

/// Render a plan's ops as a stable, reviewable multi-line string.
fn render(ops: &[Op]) -> String {
    format!("{ops:#?}")
}

/// Assert a curated UQL query lowers to `builder`, then snapshot that shared plan under
/// `name`. The snapshot catches a lowering drift; the equality catches a surface skew.
fn snap(name: &str, uql: &str, builder: Vec<Op>) {
    let parsed = eg_plan::uql::parse(uql).unwrap_or_else(|e| panic!("UQL `{uql}`: {}", e.msg));
    assert_eq!(
        parsed.ops, builder,
        "UQL and builder must lower to the same plan for `{name}`"
    );
    insta::assert_snapshot!(format!("plan_{name}"), render(&parsed.ops));
}

#[test]
fn curated_hybrid_plans_are_stable() {
    // 1. classic relational→graph→vector→limit
    snap(
        "classic_filter_traverse_rank",
        "MATCH (:Doc) WHERE year > 2024 |> TRAVERSE -[:CITES]->{1,2} |> RANK BY ~[1.0,0.0,0.0,0.0] |> LIMIT 10",
        vec![
            Op::Scan { label: "Doc".into() },
            Op::Filter { preds: vec![Pred::GtNum { prop: "year".into(), n: 2024.0 }] },
            Op::Traverse { rel: "CITES".into(), min: 1, max: 2 },
            Op::Rank { query: vec![1.0, 0.0, 0.0, 0.0] },
            Op::Limit { k: 10 },
        ],
    );

    // 2. traverse→rank→AS OF→limit (the composed time chain)
    snap(
        "traverse_rank_asof_limit",
        "MATCH (:Doc) |> TRAVERSE -[:CITES]->{1,2} |> RANK BY ~[1.0,0.0,0.0,0.0] |> AS OF @1700000000 |> LIMIT 5",
        vec![
            Op::Scan { label: "Doc".into() },
            Op::Traverse { rel: "CITES".into(), min: 1, max: 2 },
            Op::Rank { query: vec![1.0, 0.0, 0.0, 0.0] },
            Op::AsOf { ts: 1_700_000_000.0, axis: Default::default() },
            Op::Limit { k: 5 },
        ],
    );

    // 3. rank then graph-native MENTIONS rerank
    snap(
        "rank_then_mentions",
        "MATCH (:Doc) |> RANK BY ~[1.0,0.0,0.0,0.0] |> RERANK MENTIONS",
        vec![
            Op::Scan {
                label: "Doc".into(),
            },
            Op::Rank {
                query: vec![1.0, 0.0, 0.0, 0.0],
            },
            Op::RankMentions {},
        ],
    );

    // 4. rank then node-distance rerank
    snap(
        "rank_then_node_distance",
        "MATCH (:Doc) |> RANK BY ~[1.0,0.0,0.0,0.0] |> RERANK NODE_DISTANCE FROM d1",
        vec![
            Op::Scan {
                label: "Doc".into(),
            },
            Op::Rank {
                query: vec![1.0, 0.0, 0.0, 0.0],
            },
            Op::RankNodeDistance {
                center: "d1".into(),
            },
        ],
    );

    // 5. transaction-axis AS OF
    snap(
        "asof_transaction_axis",
        "MATCH (:Event) |> AS OF TX @150",
        vec![
            Op::Scan {
                label: "Event".into(),
            },
            Op::AsOf {
                ts: 150.0,
                axis: eg_types::wire::TimeAxis::Transaction,
            },
        ],
    );

    // 6. window + foreign + traverse (context ops threaded mid-plan)
    snap(
        "window_foreign_traverse",
        "MATCH (:Event) |> FOREIGN \"peer-west\" |> WINDOW 1 h |> TRAVERSE -[:CAUSED]->{1,2} |> LIMIT 10",
        vec![
            Op::Scan { label: "Event".into() },
            Op::Foreign { name: "peer-west".into() },
            Op::Window { secs: 3600.0 },
            Op::Traverse { rel: "CAUSED".into(), min: 1, max: 2 },
            Op::Limit { k: 10 },
        ],
    );

    // 7. MMR diversity rerank behind a vector rank
    snap(
        "rank_then_mmr",
        "MATCH (:Doc) |> RANK BY ~[1.0,0.0,0.0,0.0] |> RERANK MMR 0.3 5",
        vec![
            Op::Scan {
                label: "Doc".into(),
            },
            Op::Rank {
                query: vec![1.0, 0.0, 0.0, 0.0],
            },
            Op::RankMmr { lambda: 0.3, k: 5 },
        ],
    );

    // 8. scan + inline where sugar (a scan-only-ish shape)
    snap(
        "scan_inline_where",
        "MATCH (:Doc) WHERE lang = 'en'",
        vec![
            Op::Scan {
                label: "Doc".into(),
            },
            Op::Filter {
                preds: vec![Pred::Eq {
                    prop: "lang".into(),
                    value: "en".into(),
                }],
            },
        ],
    );
}

/// OPTIMIZER-quality regression: the cost model must PUSH a selective filter ahead of the
/// vector RANK, and choose vector-first for a broad filter. Snapshot BOTH reordered plans
/// so a silent optimizer regression (the selective filter stops being pushed) trips the
/// diff, not just a hidden perf loss.
#[test]
fn cost_reordered_plans_are_stable() {
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
        Op::Rank {
            query: vec![1.0, 0.0, 0.0, 0.0],
        },
    ];

    let selective = Stats::estimate(10_000, 0.01, 10, 6);
    let broad = Stats::estimate(10_000, 0.98, 10, 6);

    let sel = CostModel::reorder_filter_rank(plan.clone(), &selective);
    let brd = CostModel::reorder_filter_rank(plan, &broad);

    // Guard the actual optimizer decision, not just the render: selective ⇒ filter pushed.
    assert!(
        matches!(sel[1], Op::Filter { .. }) && matches!(sel[2], Op::Rank { .. }),
        "selective regime must push the filter ahead of RANK"
    );
    assert!(
        matches!(brd[1], Op::Rank { .. }) && matches!(brd[2], Op::Filter { .. }),
        "broad regime must run the vector RANK first"
    );

    insta::assert_snapshot!("optimizer_selective_filter_pushed", render(&sel));
    insta::assert_snapshot!("optimizer_broad_vector_first", render(&brd));
}
