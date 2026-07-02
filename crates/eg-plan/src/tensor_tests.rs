//! Array / tensor modality executor proofs (CONCEPT:EG-085).
//!
//! A small `Frame` layer, each node carrying a dense N-D array in a `tensor` property
//! (the serde form of `eg_tensor::Tensor`), drives the two tensor surfaces end-to-end
//! through the fused executor:
//!  * `Op::TensorScan { layer }` — seed the RowSet with the layer's tensor-bearing nodes.
//!  * `Op::TensorOp { kind }` — apply an eg-tensor slice/reduce/elementwise per-row,
//!    dropping rows whose tensor is missing/invalid or where the op fails.
//!
//! And a compose proof: `TensorScan` then a `TensorOp` (a valid slice) in ONE plan.

use eg_core::compute::semantic::SemanticStore;
use eg_core::graph::{GraphCore, GraphView};
use eg_tensor::{Buffer, Tensor};
use eg_types::wire::{TensorElementwiseOp, TensorOpKind, TensorReduceKind};
use serde_json::json;

use crate::algebra::{Op, Plan};
use crate::exec::PlanCtx;
use crate::PlanExt;

fn blob(v: serde_json::Value) -> Vec<u8> {
    rmp_serde::to_vec_named(&v).unwrap()
}

/// A layer of `Frame` nodes each holding a 2×3 tensor, plus a non-tensor `Doc`
/// distractor and a `Frame` with a malformed `tensor` property.
///
///   F1,F2,F3 carry a valid 2×3 F32 tensor; Doc nd0 has none; F4 has garbage.
fn frames() -> GraphView {
    let core = GraphCore::new();
    let t = Tensor::new(vec![2, 3], Buffer::F32(vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0])).unwrap();
    let tv = serde_json::to_value(&t).unwrap();
    for id in ["F1", "F2", "F3"] {
        core.add_node(id.into(), blob(json!({ "type": "Frame", "tensor": tv })));
    }
    // A non-tensor distractor that must never appear in a tensor result.
    core.add_node("nd0".into(), blob(json!({ "type": "Doc", "year": 2025 })));
    // A Frame whose `tensor` property is malformed — must be dropped by the scan.
    core.add_node(
        "F4".into(),
        blob(json!({ "type": "Frame", "tensor": "not-a-tensor" })),
    );
    core.analysis_snapshot()
}

fn run(plan: &Plan, view: &GraphView) -> Vec<String> {
    let sem = SemanticStore::new();
    let c = PlanCtx::new(view, &sem);
    let mut ids = plan.execute(&c).unwrap().ids();
    ids.sort();
    ids
}

#[test]
fn tensor_scan_selects_tensor_bearing_layer() {
    let view = frames();
    // Only F1,F2,F3 carry a valid tensor; nd0 is a Doc, F4's tensor is malformed.
    let plan = Plan::new(vec![Op::TensorScan {
        layer: "Frame".into(),
    }]);
    assert_eq!(run(&plan, &view), vec!["F1", "F2", "F3"]);
}

#[test]
fn tensor_op_valid_slice_keeps_rows() {
    let view = frames();
    // A slice within bounds succeeds for every 2×3 tensor → all rows survive.
    let plan = Plan::new(vec![
        Op::TensorScan {
            layer: "Frame".into(),
        },
        Op::TensorOp {
            kind: TensorOpKind::Slice {
                ranges: vec![(0, 2), (1, 3)],
            },
        },
    ]);
    assert_eq!(run(&plan, &view), vec!["F1", "F2", "F3"]);
}

#[test]
fn tensor_op_out_of_bounds_slice_drops_rows() {
    let view = frames();
    // A slice out of bounds fails for every tensor → all rows dropped.
    let plan = Plan::new(vec![
        Op::TensorScan {
            layer: "Frame".into(),
        },
        Op::TensorOp {
            kind: TensorOpKind::Slice {
                ranges: vec![(0, 2), (0, 9)],
            },
        },
    ]);
    assert_eq!(run(&plan, &view), Vec::<String>::new());
}

#[test]
fn tensor_op_reduce_and_elementwise_pass_through() {
    let view = frames();
    // A reduce over a valid axis + an elementwise op both succeed → rows survive.
    let plan = Plan::new(vec![
        Op::TensorScan {
            layer: "Frame".into(),
        },
        Op::TensorOp {
            kind: TensorOpKind::Reduce {
                axis: 1,
                kind: TensorReduceKind::Mean,
            },
        },
        Op::TensorOp {
            kind: TensorOpKind::Elementwise {
                op: TensorElementwiseOp::Mul,
                scalar: 2.0,
            },
        },
        Op::Limit { k: 10 },
    ]);
    assert_eq!(run(&plan, &view), vec!["F1", "F2", "F3"]);
}

#[test]
fn tensor_op_reduce_bad_axis_drops_rows() {
    let view = frames();
    // Axis 5 is out of range for a rank-2 tensor → reduce fails → rows dropped.
    let plan = Plan::new(vec![
        Op::TensorScan {
            layer: "Frame".into(),
        },
        Op::TensorOp {
            kind: TensorOpKind::Reduce {
                axis: 5,
                kind: TensorReduceKind::Sum,
            },
        },
    ]);
    assert_eq!(run(&plan, &view), Vec::<String>::new());
}
