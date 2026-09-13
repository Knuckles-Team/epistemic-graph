"""Static, non-engine proofs for the current-only dispatch decomposition."""

from __future__ import annotations

import copy
import importlib.util
from pathlib import Path

import pytest

pytestmark = pytest.mark.no_engine


def _gate_module():
    gate_path = Path(__file__).resolve().parents[1] / "scripts" / "check_dispatch_decomposition.py"
    spec = importlib.util.spec_from_file_location("dispatch_decomposition_gate", gate_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_dispatch_decomposition_gate() -> None:
    _gate_module().main()


def test_route_ownership_drift_fails_closed() -> None:
    module = _gate_module()
    parts = module.sources()
    owner = module.ROUTE_OWNERS["dispatch_channel_methods"]
    parts[owner] = parts[owner].replace(
        "dispatch_channel_methods", "removed_channel_methods", 1
    )
    with pytest.raises(SystemExit, match="dispatch_channel_methods ownership changed"):
        module.check_inventory(parts)


def test_route_call_removal_fails_closed() -> None:
    module = _gate_module()
    parts = module.sources()
    target = "src/server/dispatch/router.rs"
    parts[target] = parts[target].replace(
        "dispatch_service_control_methods(ctx, method).await?",
        "removed_dispatch_service_control_methods(ctx, method).await?",
        1,
    )
    with pytest.raises(SystemExit, match="dispatch_service_control_methods call missing"):
        module.check_inventory(parts)


def test_graph_router_call_removal_fails_closed() -> None:
    module = _gate_module()
    parts = module.sources()
    target = "src/server/dispatch/graph_pipeline/graph_dispatch.rs"
    parts[target] = parts[target].replace(
        "route_graph_op_method(routing, method).await",
        "removed_route_graph_op_method(routing, method).await",
        1,
    )
    with pytest.raises(SystemExit, match="route_graph_op_method call missing"):
        module.check_inventory(parts)


def test_test_and_assertion_inventory_drift_fails_closed() -> None:
    module = _gate_module()
    parts = module.sources()
    target = "src/server/dispatch/consensus/routing.rs"
    parts[target] = parts[target].replace("#[test]", "#[removed_test]", 1)
    with pytest.raises(SystemExit, match="test inventory changed"):
        module.check_inventory(parts)


def test_cfg_boundary_drift_fails_closed() -> None:
    module = _gate_module()
    parts = module.sources()
    target = "src/server/dispatch/graph_pipeline/pipeline.rs"
    parts[target] = parts[target].replace('feature = "query"', 'feature = "removed-query"', 1)
    with pytest.raises(SystemExit, match="cfg boundary set changed"):
        module.check_cfg_contract(parts)


def test_legacy_coalescer_subset_proof_fails_closed() -> None:
    module = _gate_module()
    parts = copy.deepcopy(module.sources())
    gateway_path = module.ROOT / "src/server/handlers/graph_ops/gateway_graph.rs"
    original_root = module.ROOT
    try:
        # Supply the gateway source through the same dictionary seam used by the
        # checker so this mutation never touches the checkout.
        parts["src/server/handlers/graph_ops/gateway_graph.rs"] = gateway_path.read_text().replace(
            "Method::AddNode", "Method::RemovedAddNode"
        )
        with pytest.raises(SystemExit, match="gateway arm AddNode"):
            module.check_routing_and_coalescing(parts)
    finally:
        module.ROOT = original_root


def test_route_order_ignores_comments_and_detects_reversed_calls() -> None:
    module = _gate_module()
    parts = copy.deepcopy(module.sources())
    target = "src/server/dispatch/graph_pipeline/pipeline.rs"
    compute = "route_pipeline_compute(&ctx, method)"
    surfaces = "route_pipeline_surfaces(&ctx, method)"
    source = parts[target]
    swapped = source.replace(compute, "__route_compute__", 1)
    swapped = swapped.replace(surfaces, compute, 1).replace("__route_compute__", surfaces, 1)
    parts[target] = (
        swapped
        + "\n// route_pipeline_compute(&ctx, method) must precede "
        + "route_pipeline_surfaces(&ctx, method)\n"
    )
    with pytest.raises(SystemExit, match="graph gateway/terminal route order changed"):
        module.check_routing_and_coalescing(parts)


def test_post_lock_router_order_swap_fails_closed() -> None:
    module = _gate_module()
    parts = copy.deepcopy(module.sources())
    target = "src/server/dispatch/graph_pipeline/native_routes.rs"
    change = "route_change_envelope_ops(ctx, method).await"
    store = "route_native_store_ops(ctx, method).await"
    source = parts[target]
    swapped = source.replace(change, "__route_change__", 1)
    swapped = swapped.replace(store, change, 1).replace("__route_change__", store, 1)
    parts[target] = swapped
    with pytest.raises(SystemExit, match="post-lock graph router order changed"):
        module.check_routing_and_coalescing(parts)


def test_compile_shape_regression_fails_closed() -> None:
    module = _gate_module()
    parts = module.sources()
    graph = "src/server/dispatch/graph_pipeline/dispatch_helpers.rs"
    parts[graph] = parts[graph].replace(
        "let method = Method::ServedModality { op: op.clone() };",
        "let removed_method = Method::ServedModality { op: op.clone() };",
        1,
    )
    with pytest.raises(SystemExit, match="ServedModality lost"):
        module.check_compile_shapes(parts)


def test_compiler_family_child_omission_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _gate_module()
    omitted = "src/server/dispatch/change_envelope/multi_graph.rs"
    monkeypatch.setattr(module, "EXPECTED_PATHS", module.EXPECTED_PATHS - {omitted})

    with pytest.raises(SystemExit, match="compiler module family or orphan set changed"):
        module.sources()


def test_consensus_fail_open_catch_all_fails_closed() -> None:
    module = _gate_module()
    parts = module.sources()
    target = "src/server/dispatch/consensus/routing.rs"
    parts[target] = parts[target].replace(
        'Some(other) => unreachable!("unclassified native consensus domain: {other}")',
        "Some(other) => request_graph.to_string()",
        1,
    )
    with pytest.raises(SystemExit, match="fail-open catch-all"):
        module.check_consensus_exhaustiveness(parts)
