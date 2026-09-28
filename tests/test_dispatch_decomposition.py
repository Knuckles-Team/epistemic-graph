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


def _replace_once(source: str, old: str, new: str) -> str:
    """Build a non-vacuous known-bad source perturbation."""

    assert old in source, f"fixture target is absent: {old}"
    broken = source.replace(old, new, 1)
    assert broken != source
    return broken


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
    with pytest.raises(
        SystemExit, match="dispatch_service_control_methods call missing"
    ):
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


def test_route_order_ignores_comments_and_detects_reversed_calls() -> None:
    module = _gate_module()
    parts = copy.deepcopy(module.sources())
    target = "src/server/dispatch/graph_pipeline/pipeline.rs"
    compute = "route_pipeline_compute(&ctx, method)"
    surfaces = "route_pipeline_surfaces(&ctx, method)"
    source = parts[target]
    swapped = source.replace(compute, "__route_compute__", 1)
    swapped = swapped.replace(surfaces, compute, 1).replace(
        "__route_compute__", surfaces, 1
    )
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


def test_undeclared_dispatch_module_is_an_orphan(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    module = _gate_module()
    family = module.read_compiler_family(
        module.ROOT / f"{module.DISPATCH_ROOT}.rs", module.ROOT
    )
    root = tmp_path
    for path in family.all_paths:
        target = root / path.relative_to(module.ROOT)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(path.read_text(encoding="utf-8"), encoding="utf-8")
    (root / module.DISPATCH_ROOT / "forgotten.rs").write_text("fn dead() {}\n")
    monkeypatch.setattr(module, "ROOT", root)

    with pytest.raises(SystemExit, match="orphan Rust module files"):
        module.sources()
