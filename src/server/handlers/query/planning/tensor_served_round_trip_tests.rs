use super::*;
use eg_core::compute::semantic::SemanticStore;
use eg_core::graph::GraphCore;
use eg_tensor::{Buffer, Tensor};
use eg_types::wire::{TensorOpKind, TensorReduceKind};

fn blob(v: serde_json::Value) -> Vec<u8> {
    rmp_serde::to_vec_named(&v).unwrap()
}

/// A `Frame` layer of three nodes each holding the same dense 2×3 tensor in
/// their conventional `tensor` property, mirroring
/// `eg_plan::tensor_tests::frames()`.
fn frames_view() -> crate::graph::GraphView {
    let core = GraphCore::new();
    let t = Tensor::new(vec![2, 3], Buffer::F32(vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0])).unwrap();
    let tv = serde_json::to_value(&t).unwrap();
    for id in ["F1", "F2", "F3"] {
        core.add_node(
            id.into(),
            blob(serde_json::json!({ "type": "Frame", "tensor": tv })),
        );
    }
    core.analysis_snapshot()
}

fn served_indexes() -> ServedIndexes<'static> {
    ServedIndexes {
        #[cfg(feature = "text")]
        text: None,
        #[cfg(feature = "geo")]
        spatial: None,
        #[cfg(feature = "federation")]
        foreign: None,
        #[cfg(all(feature = "shacl", feature = "owl-plan"))]
        shapes: None,
        #[cfg(not(any(feature = "text", feature = "geo")))]
        _marker: std::marker::PhantomData,
    }
}

fn call_run_unified(plan: eg_plan::Plan) -> Result<Vec<(String, Option<f32>)>, String> {
    let view = frames_view();
    let semantic = SemanticStore::new();
    run_unified_with(
        plan,
        &view,
        &semantic,
        served_indexes(),
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

#[test]
fn served_tensor_scan_and_op_executes_instead_of_erroring() {
    let plan = eg_plan::Plan::new(vec![
        eg_plan::Op::TensorScan {
            layer: "Frame".into(),
        },
        eg_plan::Op::TensorOp {
            kind: TensorOpKind::Reduce {
                axis: 1,
                kind: TensorReduceKind::Mean,
            },
        },
    ]);
    let rows = call_run_unified(plan).expect(
        "served TensorOp must execute now that run_unified binds a tensor store, \
         not error with 'TensorOp requires a bound tensor store'",
    );
    let mut ids: Vec<&str> = rows.iter().map(|(id, _)| id.as_str()).collect();
    ids.sort();
    assert_eq!(ids, vec!["F1", "F2", "F3"]);
}

/// Before the fix, `run_unified` had no `tensor_store` binding at all, so this
/// exact plan deterministically failed with "TensorOp requires a bound tensor
/// store" regardless of input — the gap this test closes.
#[test]
fn served_tensor_op_without_the_fix_would_have_errored() {
    let plan = eg_plan::Plan::new(vec![
        eg_plan::Op::TensorScan {
            layer: "Frame".into(),
        },
        eg_plan::Op::TensorOp {
            kind: TensorOpKind::Elementwise {
                op: eg_types::wire::TensorElementwiseOp::Mul,
                scalar: 2.0,
            },
        },
    ]);
    assert!(
        call_run_unified(plan).is_ok(),
        "TensorOp over the served path must not deterministically error"
    );
}
