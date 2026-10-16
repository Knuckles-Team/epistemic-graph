//! EG-DURABLE-KERNEL-R024 — rollup proof: every named external backend is
//! present in this repository as a `ForeignSourceSpec` federation source or a
//! `MirrorTargetSpec` mirror target, and each entry point is exercised with a
//! representative OFFLINE operation: the typed explicit refusal the unbound
//! drivers (Neo4j/FalkorDb Cypher, Spark batch, brain-guarded) must return,
//! the bound AGE binding's endpoint-match refusal (under `federation-sql`),
//! and fail-closed mirror sends through the `MirrorTargetRegistry`.
//!
//! The representative QUERY operations of the live-probing bindings (the AGE
//! graph probe, the Trino statement pages) are proved by the per-backend
//! children (R024.1 / R024.7); this rollup test covers exactly what can be
//! exercised without a live backend, offline.

use eg_plan::federation::ForeignSourceRegistry;
use eg_plan::federation_brain_guarded::register_brain_guarded;
use eg_plan::federation_cypher_unbound::{register_falkordb, register_neo4j};
use eg_plan::federation_spark::register_spark;
use eg_plan::mirror_target::{target_for, MirrorTargetRegistry};
use eg_types::wire::{ForeignCypherBackend, ForeignSourceSpec, MirrorTargetSpec};

fn rollup_cypher_spec(backend: ForeignCypherBackend) -> ForeignSourceSpec {
    ForeignSourceSpec::Cypher {
        backend,
        endpoint: "bolt://rollup.example.invalid:7687".into(),
        graph: "rollup_graph".into(),
        query: "MATCH (r) RETURN r.key AS id".into(),
        id_field: "id".into(),
        score_field: None,
    }
}

// spec: EG-DURABLE-KERNEL-R024
#[test]
fn unbound_cypher_backends_refuse_typed_and_keep_the_registry_empty() {
    let mut registry = ForeignSourceRegistry::new();
    let neo4j = register_neo4j(
        &mut registry,
        "rollup-neo4j",
        &rollup_cypher_spec(ForeignCypherBackend::Neo4j),
    )
    .unwrap_err();
    assert!(neo4j.contains("not yet bound"), "Neo4j: {neo4j}");
    let falkor = register_falkordb(
        &mut registry,
        "rollup-falkor",
        &rollup_cypher_spec(ForeignCypherBackend::FalkorDb),
    )
    .unwrap_err();
    assert!(falkor.contains("not yet bound"), "FalkorDb: {falkor}");
    assert!(
        registry.is_empty(),
        "a refused registration must not be stored"
    );
}

// spec: EG-DURABLE-KERNEL-R024
#[test]
fn unbound_spark_and_brain_guarded_backends_refuse_typed() {
    let mut registry = ForeignSourceRegistry::new();
    let spark = ForeignSourceSpec::SparkBatch {
        artifact_ref: "artifact://rollup-batch".into(),
        id_field: "id".into(),
        score_field: None,
    };
    let spark_err = register_spark(&mut registry, "rollup-spark", &spark).unwrap_err();
    assert!(
        spark_err.contains("no bound artifact-store driver"),
        "Spark: {spark_err}"
    );
    let guarded = ForeignSourceSpec::BrainGuarded {
        endpoint: "https://rollup.example.invalid".into(),
        guard_policy: "rollup-policy".into(),
        query: "SELECT key FROM rollup_table".into(),
        id_field: "id".into(),
        score_field: None,
    };
    let guarded_err =
        register_brain_guarded(&mut registry, "rollup-guarded", &guarded).unwrap_err();
    assert!(
        guarded_err.contains("never run unguarded"),
        "brain-guarded: {guarded_err}"
    );
    assert!(
        registry.is_empty(),
        "a refused registration must not be stored"
    );
}

// spec: EG-DURABLE-KERNEL-R024
#[test]
fn mirror_targets_fail_closed_and_unknown_names_error_cleanly() {
    let mut targets = MirrorTargetRegistry::new();
    targets
        .register(
            "rollup-fan",
            MirrorTargetSpec::FanOut {
                targets: vec!["a".into(), "b".into()],
            },
        )
        .unwrap();
    let fan = targets.send_to("rollup-fan", b"row").unwrap_err();
    assert!(
        fan.contains("requires a verified registration"),
        "fan-out: {fan}"
    );
    let named_spec = MirrorTargetSpec::Named {
        name: "rollup-sink".into(),
    };
    let named = target_for(&named_spec).unwrap();
    let named_err = named.send(b"row").unwrap_err();
    assert!(
        named_err.contains("requires a verified registration"),
        "named: {named_err}"
    );
    let outbox_spec = MirrorTargetSpec::Outbox {
        consumer: "rollup-consumer".into(),
    };
    let outbox = target_for(&outbox_spec).unwrap();
    let outbox_err = outbox.send(b"row").unwrap_err();
    assert!(
        outbox_err.contains("requires a verified registration"),
        "outbox: {outbox_err}"
    );
    let unknown = targets.send_to("rollup-unknown", b"row").unwrap_err();
    assert!(
        unknown.contains("no mirror target registered"),
        "unknown name: {unknown}"
    );
}

#[cfg(feature = "federation-sql")]
fn rollup_age_spec() -> ForeignSourceSpec {
    ForeignSourceSpec::Cypher {
        backend: ForeignCypherBackend::Age,
        endpoint: "postgres://rollup.example.invalid:5432/age_graph".into(),
        graph: "rollup_graph".into(),
        query: "MATCH (r) RETURN r.key AS id".into(),
        id_field: "id".into(),
        score_field: None,
    }
}

// spec: EG-DURABLE-KERNEL-R024
#[cfg(feature = "federation-sql")]
#[test]
fn age_binding_with_a_mismatched_dsn_is_refused_offline() {
    let mut registry = ForeignSourceRegistry::new();
    let err = eg_plan::federation_age::register_age(
        &mut registry,
        "rollup-age",
        &rollup_age_spec(),
        "postgres://other.example.invalid:5432/age_graph".into(),
    )
    .unwrap_err();
    assert!(err.contains("does not match"), "AGE: {err}");
    assert!(registry.is_empty(), "a refused binding must not be stored");
}
