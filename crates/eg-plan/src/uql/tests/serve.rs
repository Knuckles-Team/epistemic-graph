//! Running statements (UQL-07/08/09): channels coexist, EXPLAIN/PROFILE, DAGs (with
//! EXPLAIN/PROFILE/RETURN, EH-449), budgets.

use eg_core::compute::semantic::SemanticStore;
use eg_core::graph::GraphCore;
use eg_types::wire::{UqlParam, UqlResult};
use serde_json::json;

use crate::budget::Budget;
use crate::exec::PlanCtx;
use crate::uql::serve::run_statement;
use crate::uql::{parse_statement, Params};

fn blob(v: serde_json::Value) -> Vec<u8> {
    rmp_serde::to_vec_named(&v).unwrap()
}

/// d1 → d2 → d3 (CITES), d4 isolated; d2 and d3 are cited once each.
fn fixture() -> (eg_core::graph::GraphView, SemanticStore) {
    let core = GraphCore::new();
    for (id, year) in [("d1", 2020), ("d2", 2021), ("d3", 2022), ("d4", 2023)] {
        core.add_node(id.into(), blob(json!({ "type": "Doc", "year": year })));
    }
    for (s, t) in [("d1", "d2"), ("d2", "d3")] {
        core.add_edge(s.into(), t.into(), blob(json!({ "relationship": "CITES" })))
            .unwrap();
    }
    (core.analysis_snapshot(), SemanticStore::new())
}

fn run(src: &str, params: &Params, ctx: &PlanCtx) -> Result<UqlResult, String> {
    let stmt = parse_statement(src, params).map_err(|e| e.render(src))?;
    run_statement(&stmt, ctx)
}

fn provenance(result: UqlResult) -> String {
    match result {
        UqlResult::Rows {
            provenance_digest, ..
        }
        | UqlResult::Profile {
            provenance_digest, ..
        } => provenance_digest,
        UqlResult::Explain { .. } => panic!("row result expected"),
    }
}

#[test]
fn row_provenance_binds_query_parameters_and_rows() {
    let (view, semantic) = fixture();
    let ctx = PlanCtx::new(&view, &semantic);
    let query = "MATCH (:Doc) WHERE year >= $min |> RETURN mentions";
    let low: Params = [("min".into(), UqlParam::Num(2021.0))].into();
    let high: Params = [("min".into(), UqlParam::Num(2022.0))].into();
    let first = provenance(run(query, &low, &ctx).unwrap());
    assert!(first.starts_with("sha256:") && first.len() == 71);
    assert_eq!(first, provenance(run(query, &low, &ctx).unwrap()));
    assert_eq!(
        first,
        provenance(run(&format!("PROFILE {query}"), &low, &ctx).unwrap())
    );
    assert_ne!(first, provenance(run(query, &high, &ctx).unwrap()));
    assert_ne!(
        first,
        provenance(run("MATCH (:Doc)", &Params::new(), &ctx).unwrap())
    );
}

#[test]
fn dag_row_provenance_is_stable_across_profile_mode() {
    let (view, semantic) = fixture();
    let ctx = PlanCtx::new(&view, &semantic);
    let plain = provenance(run(PROGRAM, &Params::new(), &ctx).unwrap());
    let profiled = provenance(run(&format!("PROFILE {PROGRAM}"), &Params::new(), &ctx).unwrap());
    assert_eq!(plain, profiled);
}

#[test]
fn two_scoring_stages_keep_both_channels() {
    let (view, semantic) = fixture();
    let ctx = PlanCtx::new(&view, &semantic);
    let out = run(
        "MATCH (:Doc) |> RERANK NODE_DISTANCE FROM 'd1' |> RERANK MENTIONS \
         |> RETURN node_distance, mentions",
        &Params::new(),
        &ctx,
    )
    .unwrap();
    let UqlResult::Rows { columns, rows, .. } = out else {
        panic!("rows expected")
    };
    assert_eq!(columns, vec!["node_distance", "mentions"]);
    let d2 = rows.iter().find(|r| r.id == "d2").expect("d2 row");
    // d2 is one hop from d1 (1/(1+1)) and cited once (the set max), so both channels
    // survive the second rerank overwriting `score`.
    assert_eq!(d2.channels[0], Some(0.5));
    assert_eq!(d2.channels[1], Some(1.0));
    assert_eq!(d2.score, Some(1.0));
}

#[test]
fn explain_reports_stages_estimates_and_incrementality() {
    let (view, semantic) = fixture();
    let ctx = PlanCtx::new(&view, &semantic);
    let out = run(
        "EXPLAIN MATCH (:Doc) WHERE year > 2020 |> LIMIT 2",
        &Params::new(),
        &ctx,
    )
    .unwrap();
    let UqlResult::Explain {
        canonical,
        stages,
        incremental,
        ..
    } = out
    else {
        panic!("explain expected")
    };
    assert!(canonical.starts_with("MATCH (:Doc)"));
    assert_eq!(stages.len(), 3);
    assert!(stages.iter().all(|s| s.rows.is_none()));
    assert!(
        incremental,
        "Scan|Filter|Limit is incrementally maintainable"
    );
    let traversal = run(
        "EXPLAIN MATCH (:Doc) |> TRAVERSE CITES",
        &Params::new(),
        &ctx,
    )
    .unwrap();
    assert!(matches!(
        traversal,
        UqlResult::Explain {
            incremental: false,
            ..
        }
    ));
}

#[test]
fn profile_counts_rows_per_stage_and_params_bind() {
    let (view, semantic) = fixture();
    let ctx = PlanCtx::new(&view, &semantic);
    let params: Params = [("min".to_string(), UqlParam::Num(2021.0))].into();
    let out = run(
        "PROFILE MATCH (:Doc) WHERE year >= $min |> LIMIT 2",
        &params,
        &ctx,
    )
    .unwrap();
    let UqlResult::Profile { rows, stages, .. } = out else {
        panic!("profile expected")
    };
    assert_eq!(rows.len(), 2);
    assert_eq!(stages.last().and_then(|s| s.rows), Some(2));
    assert!(stages.iter().all(|s| s.micros.is_some()));
}

#[test]
fn a_let_program_runs_as_a_dag() {
    let (view, semantic) = fixture();
    let ctx = PlanCtx::new(&view, &semantic);
    let out = run(
        "LET docs = MATCH (:Doc); LET cited = FROM docs |> TRAVERSE CITES; \
         JOIN docs, cited |> LIMIT 10",
        &Params::new(),
        &ctx,
    )
    .unwrap();
    let UqlResult::Rows { rows, .. } = out else {
        panic!("rows expected")
    };
    let mut ids: Vec<&str> = rows.iter().map(|r| r.id.as_str()).collect();
    ids.sort_unstable();
    assert_eq!(ids, vec!["d2", "d3"]);
}

#[test]
fn budgets_refuse_with_a_typed_error() {
    let (view, semantic) = fixture();
    let ctx = PlanCtx::new(&view, &semantic).with_budget(Budget {
        max_result_rows: 1,
        max_traversal_visits: 100,
    });
    let err = run("MATCH (:Doc)", &Params::new(), &ctx).unwrap_err();
    assert!(err.starts_with(crate::budget::BUDGET_EXCEEDED), "{err}");
}

/// docs → cited (TRAVERSE) → JOIN docs, cited → RERANK MENTIONS → RETURN mentions: four
/// nodes, the third joining the first two.
const PROGRAM: &str = "LET docs = MATCH (:Doc); LET cited = FROM docs |> TRAVERSE CITES; \
                       JOIN docs, cited |> RERANK MENTIONS |> RETURN mentions";

#[test]
fn a_program_explains_node_by_node() {
    let (view, semantic) = fixture();
    let ctx = PlanCtx::new(&view, &semantic);
    let out = run(&format!("EXPLAIN {PROGRAM}"), &Params::new(), &ctx).unwrap();
    let UqlResult::Explain {
        canonical,
        optimized,
        stages,
        incremental,
        incremental_note,
        ..
    } = out
    else {
        panic!("explain expected")
    };
    assert_eq!(canonical, optimized, "a program runs as written");
    let reparsed = parse_statement(&canonical, &Params::new()).unwrap();
    assert!(matches!(reparsed.body, crate::uql::Body::Dag(_)));
    let names: Vec<&str> = stages.iter().map(|s| s.stage.as_str()).collect();
    assert_eq!(names.len(), 4, "{names:?}");
    assert!(names[0].starts_with("#0 MATCH (:Doc)"), "{names:?}");
    assert!(
        names[2].starts_with("#2 <- #0,#1 RERANK MENTIONS"),
        "{names:?}"
    );
    assert!(stages
        .iter()
        .all(|s| s.rows.is_none() && s.micros.is_none()));
    assert!(!incremental && incremental_note.contains("LET"));
}

#[test]
fn a_program_profiles_and_returns_channels() {
    let (view, semantic) = fixture();
    let ctx = PlanCtx::new(&view, &semantic);
    let out = run(&format!("PROFILE {PROGRAM}"), &Params::new(), &ctx).unwrap();
    let UqlResult::Profile {
        columns,
        rows,
        stages,
        ..
    } = out
    else {
        panic!("profile expected")
    };
    assert_eq!(columns, vec!["mentions"]);
    let actual: Vec<Option<u64>> = stages.iter().map(|s| s.rows).collect();
    assert_eq!(actual, vec![Some(4), Some(2), Some(2), Some(2)]);
    assert!(stages.iter().all(|s| s.micros.is_some()));
    let run_rows = match run(PROGRAM, &Params::new(), &ctx).unwrap() {
        UqlResult::Rows { rows, .. } => rows,
        other => panic!("rows expected, got {other:?}"),
    };
    assert_eq!(
        run_rows, rows,
        "RETURN channels are the same with and without PROFILE"
    );
    // d2 and d3 are each cited once — the most any row is — so both score 1.0.
    assert!(
        rows.iter().all(|r| r.channels == vec![Some(1.0)]),
        "{rows:?}"
    );
}
