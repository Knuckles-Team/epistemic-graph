//! HIGH-VALUE USE-CASE SUITE #2 — codebase / repository intelligence (CONCEPT:EG-KG.query.usecase-codebase-intel).
//!
//! A repository is ingested as a code-structure GRAPH (File/Function nodes, CALLS/IMPORTS
//! edges) + VECTOR embeddings of function signatures + a small language/framework OWL
//! ontology + per-node change-time metadata (valid-time). ONE hybrid query then answers
//! the archetypal code-intelligence question:
//!
//!   "functions SIMILAR to a signature X (vector), RELATED via the call-graph to a focal
//!    entry point Y (graph traverse), CHANGED RECENTLY (bi-temporal AS OF), and MATCHING
//!    ontology concept Z (OWL inference — e.g. a compiled-language function)."
//!
//! Driven through the REAL query engine (`eg_plan::execute` over a live `PlanCtx`). Each
//! leg is asserted to genuinely filter/rank: the call-graph leg excludes an equally-
//! similar function OUTSIDE the neighborhood, OWL excludes a wrong-language function, and
//! AS OF excludes a stale version (and re-includes it at an earlier instant).
//!
//! SEAMS exercised: vector⇄graph(call-graph)⇄OWL(language ontology)⇄bi-temporal fusion.
//! Module-gated on `query` + `owl-plan`; runs under `--features full`.
#![cfg(all(feature = "query", feature = "owl-plan"))]

use eg_core::compute::semantic::SemanticStore;
use eg_core::graph::{GraphCore, GraphView};
use eg_plan::{execute, Op, Plan, PlanCtx};
use eg_types::wire::TimeAxis;
use serde_json::json;

fn blob(v: serde_json::Value) -> Vec<u8> {
    rmp_serde::to_vec_named(&v).unwrap()
}

/// Language/framework ontology: `RustFn ⊑ CompiledFunction` (a compiled-language function),
/// while `PyFn` is NOT — so `Reason <CompiledFunction>` infers the Rust functions and drops
/// the Python one. The concept "Z" the query matches on.
const CODE_ONT: &str = "@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n\
    <http://code/RustFn> rdfs:subClassOf <http://code/CompiledFunction> .\n";

/// A repository's call-graph. A focal entry point `main_handler` CALLS the parsing layer;
/// signature embeddings (query ≈ a parsing signature `[1,0,0]`) + change-time windows.
///
///  * `parse_config` RustFn — in `main_handler`'s call-graph, sig-similar, changed recently.
///  * `load_file` RustFn — reached at 2 hops via `parse_config`, moderately similar.
///  * `stale_fn` RustFn — reached via the call-graph, but its version window `[0,100)` has
///    closed (not "changed recently") so an AS OF now drops it.
///  * `py_helper` PyFn — reached at 1 hop, MOST sig-similar, but wrong language so OWL
///    `Reason<CompiledFunction>` drops it.
///  * `old_parse` RustFn — the MOST sig-similar overall and recent, but NOT in the focal
///    call-graph, so the graph leg excludes it.
fn build_repo() -> (GraphView, SemanticStore) {
    let core = GraphCore::new();
    let mk = |id: &str, ty: &str, until: Option<i64>| {
        let mut p = json!({ "type": ty, "valid_from": 0 });
        if let Some(u) = until {
            p["valid_until"] = json!(u);
        }
        core.add_node(id.into(), blob(p));
    };
    mk("main_handler", "EntryPoint", None); // the focal seed (a distinct label)
    mk("parse_config", "RustFn", None);
    mk("load_file", "RustFn", None);
    mk("stale_fn", "RustFn", Some(100));
    mk("py_helper", "PyFn", None);
    mk("old_parse", "RustFn", None);

    // Call-graph: caller -CALLS-> callee (forward BFS follows outgoing edges).
    for (caller, callee) in [
        ("main_handler", "parse_config"),
        ("main_handler", "py_helper"),
        ("parse_config", "load_file"),
        ("parse_config", "stale_fn"),
    ] {
        core.add_edge(
            caller.into(),
            callee.into(),
            blob(json!({ "relationship": "CALLS" })),
        )
        .unwrap();
    }

    // Signature embeddings (query ≈ [1,0,0]).
    let mut s = SemanticStore::new();
    s.add_embedding("old_parse".into(), vec![0.99, 0.10, 0.0]); // most similar overall (but unreachable)
    s.add_embedding("py_helper".into(), vec![0.97, 0.20, 0.0]); // most similar in-neighborhood (wrong lang)
    s.add_embedding("parse_config".into(), vec![0.95, 0.30, 0.0]); // the answer
    s.add_embedding("stale_fn".into(), vec![0.90, 0.40, 0.0]);
    s.add_embedding("load_file".into(), vec![0.80, 0.55, 0.0]);
    // main_handler (the focal seed) is excluded from results by the `min:1` traverse.
    (core.analysis_snapshot(), s)
}

/// THE codebase-intelligence proof (CONCEPT:EG-KG.query.usecase-codebase-intel): a single fused plan =
/// `Traverse(call-graph of the focal entry) → Rank(signature) → Reason<CompiledFunction>
/// (OWL) → AsOf(recent)` returns exactly the sig-similar, call-related, recently-changed,
/// compiled-language function — and every leg is shown to filter.
#[test]
fn hybrid_codebase_query_honors_call_graph_vector_owl_time_eg435() {
    let (view, semantic) = build_repo();
    let ctx = PlanCtx::new(&view, &semantic);

    let query_sig = vec![1.0_f32, 0.0, 0.0];
    let plan = |ts: f64| {
        Plan::new(vec![
            // SEED from the single focal entry point (its own distinct label).
            Op::Scan {
                label: "EntryPoint".into(),
            },
            // GRAPH (call-graph leg): expand to the focal entry's call-graph neighborhood
            // (functions reachable via CALLS within 1..3 hops).
            Op::Traverse {
                rel: "CALLS".into(),
                min: 1,
                max: 3,
            },
            // VECTOR: order the neighborhood by signature similarity to X.
            Op::Rank {
                query: query_sig.clone(),
            },
            // OWL: keep only inferred CompiledFunction members (drops the PyFn).
            Op::Reason {
                target_class: "<http://code/CompiledFunction>".into(),
                ontology: CODE_ONT.into(),
            },
            // bi-temporal: keep only the versions live (recently changed) at `ts`.
            Op::AsOf {
                ts,
                axis: TimeAxis::Valid,
            },
            Op::Limit { k: 10 },
        ])
    };

    // At "now" (ts=200): neighborhood = {parse_config, py_helper, load_file, stale_fn};
    // vector orders py_helper > parse_config > stale_fn > load_file; OWL drops py_helper;
    // AS OF drops the stale version → parse_config is the answer, ahead of load_file.
    let now = execute(&plan(200.0), &ctx).unwrap().ids();
    assert_eq!(
        now.first().map(String::as_str),
        Some("parse_config"),
        "the fused answer is the sig-similar, call-related, recent, compiled-lang fn: {now:?}"
    );
    // Call-graph leg: the MOST sig-similar function overall is excluded because it is not
    // in the focal entry's call-graph.
    assert!(
        !now.contains(&"old_parse".to_string()),
        "the call-graph leg excludes an equally-similar fn OUTSIDE the neighborhood: {now:?}"
    );
    // OWL leg: the wrong-language (PyFn) function — most similar in-neighborhood — is dropped.
    assert!(
        !now.contains(&"py_helper".to_string()),
        "the OWL leg drops the wrong-language PyFn even though it is the most similar: {now:?}"
    );
    // Temporal leg: the stale version is dropped at now.
    assert!(
        !now.contains(&"stale_fn".to_string()),
        "the AS OF leg drops the stale (not recently-changed) version: {now:?}"
    );

    // bi-temporal re-selection: at an earlier instant the stale version WAS live.
    let past = execute(&plan(50.0), &ctx).unwrap().ids();
    assert!(
        past.contains(&"stale_fn".to_string()),
        "at ts=50 the (then-current) stale_fn version IS returned — AS OF re-selects: {past:?}"
    );
}
