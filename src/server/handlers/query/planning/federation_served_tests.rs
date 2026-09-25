//! EH-563 served-path proofs: every served plan that touches a foreign source runs through
//! the federation optimizer (`run_unified_with` binds a session), and a foreign∩local join over a
//! registered source keeps its exact answer. No network: the refusals below happen before
//! any request, which the naive full-fetch path could not produce.

#[cfg(feature = "tsdb")]
use super::TsdbLegBind;
use super::{execute_rows, run_unified_with, ServedIndexes};
use eg_core::compute::semantic::SemanticStore;
use eg_core::graph::GraphCore;
use eg_plan::federation::ForeignSourceRegistry;
use eg_plan::Op;
use eg_types::wire::{ForeignSourceSpec, HttpFieldMap};

fn blob(v: serde_json::Value) -> Vec<u8> {
    rmp_serde::to_vec_named(&v).unwrap()
}

fn run(
    ops: Vec<Op>,
    registry: Option<&ForeignSourceRegistry>,
) -> Result<Vec<(String, Option<f32>)>, String> {
    let core = GraphCore::new();
    for id in ["d1", "d2", "d3"] {
        core.add_node(id.into(), blob(serde_json::json!({ "type": "Doc" })));
    }
    let view = core.analysis_snapshot();
    let semantic = SemanticStore::new();
    let served = ServedIndexes {
        foreign: registry,
        ..ServedIndexes::default()
    };
    run_unified_with(
        eg_plan::Plan::new(ops),
        &view,
        &semantic,
        served,
        #[cfg(feature = "tsdb")]
        TsdbLegBind {
            tsdb: None,
            tsdb_tenant: None,
            tsdb_graph: None,
            staged_series: None,
        },
        execute_rows,
    )
}

fn key_only_source() -> ForeignSourceSpec {
    ForeignSourceSpec::HttpJson {
        url: "https://api.example.invalid/items?ids={keys}".into(),
        json_path: "data".into(),
        field_map: HttpFieldMap {
            id: "ref".into(),
            score: None,
        },
    }
}

#[test]
fn a_served_scan_of_a_key_only_source_is_refused_by_the_optimizer() {
    let err = run(
        vec![Op::ForeignScan {
            source: Box::new(key_only_source()),
            join: false,
        }],
        None,
    )
    .unwrap_err();
    assert!(
        err.starts_with(eg_plan::federation_opt::REQUIRES_KEYS),
        "the served path must plan with source capabilities: {err}"
    );
}

#[test]
fn a_served_named_join_keeps_the_exact_intersection() {
    let mut registry = ForeignSourceRegistry::new();
    registry.register_table(
        "catalog",
        ["d3", "x1", "d1"].map(|id| (id.to_string(), None)),
    );
    let rows = run(
        vec![
            Op::Scan {
                label: "Doc".into(),
            },
            Op::ForeignScan {
                source: Box::new(ForeignSourceSpec::Named {
                    name: "catalog".into(),
                }),
                join: true,
            },
        ],
        Some(&registry),
    )
    .unwrap();
    let mut ids: Vec<String> = rows.into_iter().map(|(id, _)| id).collect();
    ids.sort();
    assert_eq!(ids, vec!["d1", "d3"]);
}
