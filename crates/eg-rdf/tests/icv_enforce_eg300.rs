//! ICV write-path enforcement, exercised end-to-end through the eg-rdf SPARQL UPDATE /
//! commit path (CONCEPT:EG-KG.ontology.rdf-update-guard).
//!
//! The enforcement WIRING lives in `eg-rdf` (the [`eg_rdf::guard::WriteGuard`] hook +
//! [`eg_rdf::update::execute`]); the ICV POLICY that implements the hook lives in
//! `eg-shacl` (which sits ABOVE eg-rdf in the DAG and is used here as a DEV-dependency).
//! This test drives a real [`eg_rdf::update::MapStore`] through the mandatory guard and
//! asserts:
//!
//!  * the policy REJECTS a change set that would introduce an integrity violation, and the
//!    store is left UNCHANGED (nothing committed) — the rejection carries the introduced
//!    violation(s) with their SPARQL witness;
//!  * the policy ACCEPTS a clean change set (it commits);
//!  * an empty registry fails closed.

use eg_rdf::sparql::{execute, Dataset, Projection, QueryOutcome};
use eg_rdf::update::{execute_str, MapStore, UpdateError};
use eg_shacl::{IcvPolicy, IcvPolicyRegistry};

const PREFIXES: &str = r#"
@prefix sh:   <http://www.w3.org/ns/shacl#> .
@prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix ex:   <http://example.org/> .
"#;

// A person may have at most one manager — a RESOURCE-valued maxCount, which survives the
// property-graph mapping (a duplicate literal would fold into the key-unique blob).
const SHAPES: &str = r#"
ex:PersonShape a sh:NodeShape ;
    sh:targetClass ex:Person ;
    sh:property [ sh:path ex:manager ; sh:maxCount 1 ] .
"#;

fn policy() -> IcvPolicyRegistry {
    let p = IcvPolicy::from_turtle(&format!("{PREFIXES}{SHAPES}")).unwrap();
    IcvPolicyRegistry::new().with(None, p)
}

/// Seed a conforming base: one Person with exactly one manager.
fn seed(store: &MapStore) {
    execute_str(
        "PREFIX ex: <http://example.org/>
         INSERT DATA { ex:a a ex:Person ; ex:manager ex:m1 }",
        store,
        &Projection::raw(),
        &policy(),
    )
    .expect("seed");
}

fn manager_count(store: &MapStore) -> usize {
    let view = store.core_of(None).analysis_snapshot();
    match execute(
        &Dataset::new(&view, Vec::new()),
        "SELECT ?m WHERE { <http://example.org/a> <http://example.org/manager> ?m }",
        &Projection::raw(),
        None,
    )
    .unwrap()
    {
        QueryOutcome::Solutions(r) => r.solutions.len(),
        _ => panic!("expected solutions"),
    }
}

const ADD_SECOND_MANAGER: &str =
    "PREFIX ex: <http://example.org/> INSERT DATA { ex:a ex:manager ex:m2 }";

#[test]
fn eg300_enforce_rejects_violating_change_and_leaves_store_unchanged() {
    let store = MapStore::new();
    seed(&store);
    assert_eq!(manager_count(&store), 1);

    let err = execute_str(ADD_SECOND_MANAGER, &store, &Projection::raw(), &policy())
        .expect_err("Enforce must reject a maxCount-breaking change");

    match err {
        UpdateError::Rejected(rej) => {
            assert_eq!(rej.graph, None, "default-graph rejection");
            assert!(rej.message.contains("EG-KG.ontology.rdf-update-guard"));
            // Structured detail lists the introduced violation(s) with SPARQL witness.
            let arr = rej.details.as_array().expect("details array");
            assert!(!arr.is_empty(), "at least one introduced violation");
            assert!(
                rej.details.to_string().contains("witness"),
                "each violation carries its SPARQL witness"
            );
        }
        other => panic!("expected Rejected, got {other:?}"),
    }

    // The commit was aborted — the second manager was NOT written.
    assert_eq!(
        manager_count(&store),
        1,
        "rejected change must not touch the store"
    );
}

#[test]
fn eg300_enforce_accepts_clean_change() {
    let store = MapStore::new();
    seed(&store);

    let report = execute_str(
        "PREFIX ex: <http://example.org/> INSERT DATA { ex:a ex:nickname \"Ay\" }",
        &store,
        &Projection::raw(),
        &policy(),
    )
    .expect("clean change under Enforce must commit");
    assert_eq!(report.inserted, 1);

    // And an in-range change (still one manager) also commits.
    assert_eq!(manager_count(&store), 1);
}

#[test]
fn eg300_missing_policy_rejects_the_write() {
    let store = MapStore::new();
    seed(&store);

    let error = execute_str(
        ADD_SECOND_MANAGER,
        &store,
        &Projection::raw(),
        &IcvPolicyRegistry::new(),
    )
    .expect_err("missing integrity policy must reject");
    assert!(matches!(error, UpdateError::Rejected(_)));
    assert_eq!(manager_count(&store), 1);
}
