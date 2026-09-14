"""CI entry point for the audited strict-current architecture gate."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

# Pure/static test -- never needs the shared native engine (see
# conftest.py's session-scoped `start_epistemic_graph_server` fixture,
# which this marker exempts this module from triggering).
pytestmark = pytest.mark.no_engine


def _gate_module():
    root = Path(__file__).resolve().parents[1]
    gate_path = root / "scripts" / "check_current_only_architecture.py"
    spec = importlib.util.spec_from_file_location("current_only_test_gate", gate_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_current_only_architecture_gate() -> None:
    root = Path(__file__).resolve().parents[1]
    gate_path = root / "scripts" / "check_current_only_architecture.py"
    spec = importlib.util.spec_from_file_location("current_only_gate", gate_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.main()


def test_protocol_child_variant_is_composed_and_required() -> None:
    root = Path(__file__).resolve().parents[1]
    gate_path = root / "scripts" / "check_current_only_architecture.py"
    spec = importlib.util.spec_from_file_location(
        "current_only_protocol_family", gate_path
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    protocol = module.protocol_source()
    wire = module.read_module_tree("crates/eg-types/src/wire.rs", root_dir=root)
    assert "CreateNodeIfAbsent {" in protocol
    module._check_protocol(protocol, wire)

    omitted = protocol.replace("CreateNodeIfAbsent {", "CreateNodeIfAbsentRemoved {", 1)
    with pytest.raises(
        SystemExit, match="missing contract variant: CreateNodeIfAbsent"
    ):
        module._check_protocol(omitted, wire)


def test_rdf_integrity_check_follows_compiler_declared_children(
    tmp_path: Path, monkeypatch
) -> None:
    module = _gate_module()
    handler_dir = tmp_path / "src/server/handlers"
    child_dir = handler_dir / "rdf"
    child_dir.mkdir(parents=True)
    (handler_dir / "rdf.rs").write_text("mod triples;\n", encoding="utf-8")
    child = child_dir / "triples.rs"
    child.write_text('const RDF_CHILD_MARKER: &str = "reachable";\n', encoding="utf-8")
    monkeypatch.setattr(module, "ROOT", tmp_path)

    assert "RDF_CHILD_MARKER" in module.rdf_handler_source()
    child.unlink()
    with pytest.raises(SystemExit, match="resolve to exactly one file"):
        module.rdf_handler_source()


def test_rdf_update_check_follows_compiler_declared_children(
    tmp_path: Path, monkeypatch
) -> None:
    module = _gate_module()
    update_dir = tmp_path / "crates/eg-rdf/src/update"
    update_dir.mkdir(parents=True)
    (update_dir.parent / "update.rs").write_text("mod engine;\n", encoding="utf-8")
    child = update_dir / "engine.rs"
    child.write_text("fn guarded_update_marker() {}\n", encoding="utf-8")
    monkeypatch.setattr(module, "ROOT", tmp_path)

    assert "guarded_update_marker" in module.rdf_update_source()
    child.unlink()
    with pytest.raises(SystemExit, match="resolve to exactly one file"):
        module.rdf_update_source()


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


def test_identity_order_follows_consensus_child_and_fails_closed() -> None:
    root = Path(__file__).resolve().parents[1]
    gate_path = root / "scripts" / "check_current_only_architecture.py"
    spec = importlib.util.spec_from_file_location(
        "current_only_identity_gate", gate_path
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    dispatch = module.read_module_tree("src/server/dispatch.rs", root_dir=root)

    module._check_identity_order(dispatch)
    broken = dispatch.replace(
        'Some("Identity") => "__commons__".to_string()',
        'Some("Identity") => request_graph.to_string()',
        1,
    )
    with pytest.raises(SystemExit, match="not totally ordered"):
        module._check_identity_order(broken)
