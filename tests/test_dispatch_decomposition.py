"""Static, non-engine proofs for the current-only dispatch decomposition."""

from __future__ import annotations

import copy
import importlib.util
from pathlib import Path

import pytest

pytestmark = pytest.mark.no_engine


def _gate_module():
    gate_path = (
        Path(__file__).resolve().parents[1]
        / "scripts"
        / "check_dispatch_decomposition.py"
    )
    spec = importlib.util.spec_from_file_location(
        "dispatch_decomposition_gate", gate_path
    )
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


def test_test_and_assertion_inventory_drift_fails_closed() -> None:
    module = _gate_module()
    parts = module.sources()
    target = "src/server/dispatch/consensus.rs"
    parts[target] = parts[target].replace("#[test]", "#[removed_test]", 1)
    with pytest.raises(SystemExit, match="test inventory changed"):
        module.check_inventory(parts)


def test_cfg_boundary_drift_fails_closed() -> None:
    module = _gate_module()
    parts = module.sources()
    target = "src/server/dispatch/graph_pipeline.rs"
    parts[target] = parts[target].replace(
        'feature = "query"', 'feature = "removed-query"', 1
    )
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
        parts["src/server/handlers/graph_ops/gateway_graph.rs"] = (
            gateway_path.read_text().replace(
                "Method::AddNode", "Method::RemovedAddNode"
            )
        )
        with pytest.raises(SystemExit, match="gateway arm AddNode"):
            module.check_routing_and_coalescing(parts)
    finally:
        module.ROOT = original_root


def test_compile_shape_regression_fails_closed() -> None:
    module = _gate_module()
    parts = module.sources()
    graph = "src/server/dispatch/graph_pipeline.rs"
    parts[graph] = parts[graph].replace(
        "let method = Method::ServedModality { op: op.clone() };",
        "let removed_method = Method::ServedModality { op: op.clone() };",
        1,
    )
    with pytest.raises(SystemExit, match="ServedModality lost"):
        module.check_compile_shapes(parts)
