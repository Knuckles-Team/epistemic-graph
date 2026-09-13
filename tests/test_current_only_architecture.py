"""CI entry point for the audited strict-current architecture gate."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

# Pure/static test -- never needs the shared native engine (see
# conftest.py's session-scoped `start_epistemic_graph_server` fixture,
# which this marker exempts this module from triggering).
pytestmark = pytest.mark.no_engine


def test_current_only_architecture_gate() -> None:
    root = Path(__file__).resolve().parents[1]
    gate_path = root / "scripts" / "check_current_only_architecture.py"
    spec = importlib.util.spec_from_file_location("current_only_gate", gate_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.main()


def test_broker_expiry_guard_rejects_none_as_expired() -> None:
    root = Path(__file__).resolve().parents[1]
    gate_path = root / "scripts" / "check_current_only_architecture.py"
    spec = importlib.util.spec_from_file_location("current_only_broker_gate", gate_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    broker = (root / "crates/eg-core/src/broker.rs").read_text(encoding="utf-8")
    module._require_broker_expiry_sweep(broker)

    broken = broker.replace("lease_until.is_some_and", "lease_until.is_none_or", 1)
    with pytest.raises(SystemExit, match="non-expiring zero-duration claim"):
        module._require_broker_expiry_sweep(broken)


def test_graph_fencing_follows_compiler_declared_children() -> None:
    root = Path(__file__).resolve().parents[1]
    gate_path = root / "scripts" / "check_current_only_architecture.py"
    spec = importlib.util.spec_from_file_location("current_only_graph_gate", gate_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    graph = module.read_module_tree("crates/eg-core/src/graph.rs", root_dir=root)

    module._check_graph_fencing(graph)
    broken = graph.replace("pub fn create_node_if_absent(", "fn create_node_if_absent(")
    with pytest.raises(SystemExit, match="native atomic create"):
        module._check_graph_fencing(broken)
