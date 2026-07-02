//! Federation NAME-RESOLUTION proofs (CONCEPT:EG-073): the `ForeignSourceRegistry`
//! resolves a foreign source BY NAME so a `Named` `Op::ForeignScan` (and the UQL
//! `FOREIGN "<name>"` marker `Op::Foreign`) materialize the registered source's rows,
//! compose with a local Join, error cleanly on an unbound name, and — crucially — leave
//! every existing plan unchanged when NO registry is attached (the default ctx).
//!
//! These sources are OFFLINE (a fixed `TableSource` / a closure), so the proofs exercise
//! the resolution seam without standing up a live external endpoint — the network kinds
//! (`RemoteEngine`/`HttpJson`/`Sql`) are already proven in the sibling federation tests.

use std::collections::HashSet;

use eg_types::wire::{ForeignSourceSpec, Pred};

use crate::algebra::Op;
use crate::exec::{execute, PlanCtx};
use crate::federation::ForeignSourceRegistry;
use crate::rowset::RowSet;
use crate::Plan;

/// Build a `(String, Option<f32>)` row list from `(&str, Option<f32>)` pairs.
fn rows(pairs: &[(&str, Option<f32>)]) -> Vec<(String, Option<f32>)> {
    pairs.iter().map(|(id, s)| (id.to_string(), *s)).collect()
}

/// CONCEPT:EG-073 — a registered source resolves by name: a `Named` `Op::ForeignScan`
/// (a pure source, `join=false`) materializes the registered rows into the RowSet.
#[test]
fn eg073_named_foreign_scan_materializes_registered_rows() {
    let mut registry = ForeignSourceRegistry::new();
    registry.register_table("peer-east", rows(&[("d2", Some(0.7)), ("d4", Some(0.4))]));
    assert_eq!(registry.len(), 1);
    assert!(!registry.is_empty());

    let plan = Plan::new(vec![Op::ForeignScan {
        source: ForeignSourceSpec::Named {
            name: "peer-east".into(),
        },
        join: false,
    }]);

    let fx = crate::fixture::build();
    let ctx = PlanCtx::new(&fx.view, &fx.semantic).with_foreign(&registry);
    let out = execute(&plan, &ctx).unwrap();

    assert_eq!(out.ids(), vec!["d2", "d4"], "named source rows materialized");
    assert_eq!(out.rows()[0].score, Some(0.7), "scores are projected");
}

/// CONCEPT:EG-073 — a `Named` `Op::ForeignScan{join:true}` composes with the LOCAL graph:
/// `Scan -> Filter(year>2023) -> ForeignScan{Named, join} -> Rank -> Limit` equals the
/// MANUAL join (local-filtered ids ∩ registered foreign ids, vector-ranked, top-k).
#[test]
fn eg073_named_foreign_scan_composes_with_a_local_join() {
    let fx = crate::fixture::build();
    let query = crate::fixture::query_vec(); // [1,0,0,0] → ranks d2 > d4

    let mut registry = ForeignSourceRegistry::new();
    registry.register_table("peer-east", rows(&[("d2", None), ("d4", None)]));

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
            source: ForeignSourceSpec::Named {
                name: "peer-east".into(),
            },
            join: true,
        },
        Op::Rank {
            query: query.clone(),
        },
        Op::Limit { k: 10 },
    ]);
    let ctx = PlanCtx::new(&fx.view, &fx.semantic).with_foreign(&registry);
    let fused = execute(&plan, &ctx).unwrap();

    // Oracle: local filter year>2023 → {d1,d2,d4,d5}; foreign {d2,d4}; ∩ → {d2,d4}; rank.
    let local: HashSet<&str> = ["d1", "d2", "d4", "d5"].into_iter().collect();
    let foreign: HashSet<&str> = ["d2", "d4"].into_iter().collect();
    let joined: HashSet<String> = local
        .intersection(&foreign)
        .map(|s| s.to_string())
        .collect();
    let manual: Vec<String> = fx
        .semantic
        .semantic_search(&query, 32)
        .into_iter()
        .map(|(id, _)| id)
        .filter(|id| joined.contains(id))
        .collect();

    assert_eq!(fused.ids(), manual, "named foreign∩local == the manual join");
    assert_eq!(fused.ids(), vec!["d2", "d4"], "ranked d2 then d4");
}

/// CONCEPT:EG-073 — the UQL `FOREIGN "<name>"` marker (`Op::Foreign`) resolves through
/// the registry too: with a registry attached the named source's rows REPLACE the input.
#[test]
fn eg073_op_foreign_marker_resolves_through_registry() {
    let fx = crate::fixture::build();
    let mut registry = ForeignSourceRegistry::new();
    registry.register_closure("peer-west", || Ok(RowSet::from_ids(["d3".into(), "d4".into()])));

    // Scan would seed all Docs; the resolved FOREIGN marker replaces it with {d3,d4}.
    let plan = Plan::new(vec![
        Op::Scan {
            label: "Doc".into(),
        },
        Op::Foreign {
            name: "peer-west".into(),
        },
        Op::Limit { k: 10 },
    ]);
    let ctx = PlanCtx::new(&fx.view, &fx.semantic).with_foreign(&registry);
    let mut ids = execute(&plan, &ctx).unwrap().ids();
    ids.sort();
    assert_eq!(ids, vec!["d3", "d4"], "closure source replaced the local scan");
}

/// CONCEPT:EG-073 — an unbound name is a CLEAN typed error (naming the source + what IS
/// registered), for BOTH the `Named` `Op::ForeignScan` and the `Op::Foreign` marker.
#[test]
fn eg073_unbound_named_source_errors_cleanly() {
    let fx = crate::fixture::build();
    let mut registry = ForeignSourceRegistry::new();
    registry.register_table("peer-east", rows(&[("d2", None)]));

    let scan = Plan::new(vec![Op::ForeignScan {
        source: ForeignSourceSpec::Named {
            name: "ghost".into(),
        },
        join: false,
    }]);
    let ctx = PlanCtx::new(&fx.view, &fx.semantic).with_foreign(&registry);
    let err = execute(&scan, &ctx).unwrap_err();
    assert!(
        err.contains("no foreign source registered") && err.contains("ghost"),
        "clean unbound-name error, got: {err}"
    );

    let marker = Plan::new(vec![Op::Foreign {
        name: "ghost".into(),
    }]);
    let err2 = execute(&marker, &ctx).unwrap_err();
    assert!(err2.contains("ghost"), "marker unbound error, got: {err2}");
}

/// CONCEPT:EG-073 — a `Named` `Op::ForeignScan` with NO registry attached to the ctx is a
/// clean error (rather than a panic or a silent empty set): the plan references a
/// registry that was never wired.
#[test]
fn eg073_named_scan_without_a_registry_errors() {
    let fx = crate::fixture::build();
    let plan = Plan::new(vec![Op::ForeignScan {
        source: ForeignSourceSpec::Named {
            name: "peer-east".into(),
        },
        join: false,
    }]);
    // Default ctx — no `.with_foreign(..)`.
    let ctx = PlanCtx::new(&fx.view, &fx.semantic);
    let err = execute(&plan, &ctx).unwrap_err();
    assert!(
        err.contains("no ForeignSourceRegistry is attached"),
        "clean no-registry error, got: {err}"
    );
}

/// CONCEPT:EG-073 — the CRITICAL invariant: with NO registry attached (the default ctx),
/// the `Op::Foreign` marker keeps its PRIOR pass-through behavior, so every existing plan
/// is byte-for-byte unchanged. A registry is strictly additive.
#[test]
fn eg073_empty_registry_preserves_existing_foreign_marker_passthrough() {
    let fx = crate::fixture::build();
    let plan = Plan::new(vec![
        Op::Scan {
            label: "Doc".into(),
        },
        Op::Foreign {
            name: "peer-east".into(),
        },
    ]);

    // No registry attached → the marker passes the local scan through unchanged.
    let ctx = PlanCtx::new(&fx.view, &fx.semantic);
    let mut with_marker = execute(&plan, &ctx).unwrap().ids();
    with_marker.sort();

    // The SAME scan without the marker at all.
    let bare = Plan::new(vec![Op::Scan {
        label: "Doc".into(),
    }]);
    let mut without = execute(&bare, &ctx).unwrap().ids();
    without.sort();

    assert_eq!(
        with_marker, without,
        "no-registry FOREIGN marker is a pass-through (existing behavior preserved)"
    );
}

/// CONCEPT:EG-073 — a `Named` spec registered as a *spec-backed* source recurses into the
/// clean guard rather than looping: `source_for` cannot resolve a `Named`, so a
/// `register_spec(Named)` surfaces the guard error, not a hang or empty set.
#[test]
fn eg073_spec_source_rejects_a_nested_named_spec() {
    let fx = crate::fixture::build();
    let mut registry = ForeignSourceRegistry::new();
    registry.register_spec(
        "loopy",
        ForeignSourceSpec::Named {
            name: "loopy".into(),
        },
    );
    let plan = Plan::new(vec![Op::Foreign {
        name: "loopy".into(),
    }]);
    let ctx = PlanCtx::new(&fx.view, &fx.semantic).with_foreign(&registry);
    let err = execute(&plan, &ctx).unwrap_err();
    assert!(
        err.contains("resolves through the ForeignSourceRegistry"),
        "nested Named spec hits the source_for guard, got: {err}"
    );
}
