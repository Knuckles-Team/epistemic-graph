"""CI entry point and known-bad perturbations for the current-only ban gate."""

from __future__ import annotations

import importlib.util
import re
from pathlib import Path

import pytest

# Pure/static test -- never needs the shared native engine (see
# conftest.py's session-scoped `start_epistemic_graph_server` fixture,
# which this marker exempts this module from triggering).
pytestmark = pytest.mark.no_engine

ROOT = Path(__file__).resolve().parents[1]


def _gate_module():
    gate_path = ROOT / "scripts" / "check_current_only_architecture.py"
    spec = importlib.util.spec_from_file_location("current_only_test_gate", gate_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def gate():
    return _gate_module()


def test_current_only_architecture_gate(gate) -> None:
    gate.main()


_PROTOCOL = """\
#[serde(deny_unknown_fields)]
pub struct Request {
    #[serde(deserialize_with = "deserialize_required_option")]
    pub agent_id: Option<String>,
}
#[derive(Debug, Clone)]
pub enum CausalQueryModeWire { Intervene }
enum Method {
    DsSoftmax {
        logits: Vec<f64>,
        temperature: f64,
    },
}
fn uses(m: Method) {
    match m {
        Method::DsSoftmax { .. } => {}
    }
}
"""


def _check_fixture(gate, protocol: str) -> None:
    body = gate.variant_body(protocol, "DsSoftmax")
    gate.require_explicit_field(body, "DsSoftmax", "temperature")
    attrs, request = gate.item(protocol, "struct", "Request")
    assert "deny_unknown_fields" in attrs
    gate.require_explicit_field(request, "Request", "agent_id")
    causal, _ = gate.item(protocol, "enum", "CausalQueryModeWire")
    assert not gate.derives(causal, "Default")


def test_required_field_fixture_passes_and_ignores_match_patterns(gate) -> None:
    _check_fixture(gate, _PROTOCOL)


@pytest.mark.parametrize(
    ("old", "new", "message"),
    [
        (
            "        temperature: f64,",
            '        #[serde(default = "one")]\n        temperature: f64,',
            "accepts an omitted legacy value",
        ),
        (
            "        temperature: f64,",
            '        #[serde(skip_serializing_if = "is_one")]\n'
            "        temperature: f64,",
            "can disappear from the canonical encoding",
        ),
        (
            '    #[serde(deserialize_with = "deserialize_required_option")]\n',
            "",
            "conflates an omitted field with explicit null",
        ),
        ("        temperature: f64,", "        temp: f64,", "is missing"),
    ],
)
def test_required_field_bans_fail_closed(gate, old, new, message) -> None:
    broken = _PROTOCOL.replace(old, new, 1)
    assert broken != _PROTOCOL
    with pytest.raises(SystemExit, match=message):
        _check_fixture(gate, broken)


def test_required_field_ban_is_not_fooled_by_a_commented_attribute(gate) -> None:
    commented = _PROTOCOL.replace(
        "        temperature: f64,",
        "        // #[serde(default)]\n        temperature: f64,",
        1,
    )
    _check_fixture(gate, commented)


def test_live_protocol_rejects_a_new_serde_default(gate) -> None:
    protocol = gate.protocol_source()
    wire = gate.tree("crates/eg-types/src/wire.rs")
    gate.check_protocol(protocol, wire)
    broken, count = re.subn(
        r"(?m)^(\s*CreateNodeIfAbsent\s*\{\s*)node_id:",
        r"\1#[serde(default)] node_id:",
        protocol,
        count=1,
    )
    assert count == 1
    with pytest.raises(SystemExit, match="CreateNodeIfAbsent.node_id accepts"):
        gate.check_protocol(broken, wire)


def test_causal_mode_default_derive_is_rejected(gate) -> None:
    protocol = gate.protocol_source()
    wire = gate.tree("crates/eg-types/src/wire.rs")
    broken = protocol.replace(
        "pub enum CausalQueryModeWire",
        "#[derive(Default)]\npub enum CausalQueryModeWire",
        1,
    )
    with pytest.raises(SystemExit, match="implicit historical default"):
        gate.check_protocol(broken, wire)


def test_legacy_default_helper_is_rejected_but_not_in_a_comment(gate) -> None:
    protocol = gate.protocol_source()
    wire = gate.tree("crates/eg-types/src/wire.rs")
    gate.check_protocol(protocol + "\n// fn default_temperature() {}\n", wire)
    with pytest.raises(SystemExit, match="default_temperature"):
        gate.check_protocol(protocol + "\nfn default_temperature() {}\n", wire)


def test_create_if_absent_toctou_and_prepublish_bans(gate) -> None:
    graph = "impl G { pub fn create_node_if_absent(&self) -> bool { self.txn() } }"
    prepublish = "fn prepublish_success(m: &Method) { match m { _ => None } }"
    gate.check_graph_fencing(graph, prepublish)
    with pytest.raises(SystemExit, match="TOCTOU"):
        gate.check_graph_fencing(
            graph.replace("self.txn()", "self.has_node(id)"), prepublish
        )
    with pytest.raises(SystemExit, match="predicted before"):
        gate.check_graph_fencing(
            graph, prepublish.replace("_ =>", "Method::BrokerAckTag { .. } =>")
        )


def test_raft_snapshot_rejects_a_retired_plaintext_field(gate) -> None:
    store = gate.raft_store_source()
    gate.check_raft_snapshot(store)
    broken = store.replace(
        "struct GraphSnapshot {", "struct GraphSnapshot {\n    nodes: Vec<u8>,", 1
    )
    with pytest.raises(SystemExit, match=r"duplicate decoded/plaintext.*nodes"):
        gate.check_raft_snapshot(broken)


def test_served_index_rejects_the_version_only_coverage_check(gate) -> None:
    pregel = gate.read("src/raft/pregel.rs")
    served = gate.tree("src/server/secondary_indexes.rs")
    gate.check_read_authority(pregel, served)
    broken = served.replace("covers_source(", "covers_version(", 1)
    with pytest.raises(SystemExit, match="covers_version"):
        gate.check_read_authority(pregel, broken)


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


def test_raft_store_family_rejects_orphan_children(tmp_path: Path, monkeypatch) -> None:
    module = _gate_module()
    store_dir = tmp_path / "src" / "raft"
    child_dir = store_dir / "store"
    child_dir.mkdir(parents=True)
    (store_dir / "store.rs").write_text(
        "struct GraphSnapshot {\n    durable: RawGraphRows,\n}\nmod snapshot;\n",
        encoding="utf-8",
    )
    (child_dir / "snapshot.rs").write_text("fn list() {}\n", encoding="utf-8")
    monkeypatch.setattr(module, "ROOT", tmp_path)

    module.check_raft_snapshot(module.raft_store_source())
    (store_dir / "store.rs").write_text(
        "struct GraphSnapshot {\n    durable: RawGraphRows,\n}\n", encoding="utf-8"
    )
    with pytest.raises(SystemExit, match="orphan Rust module files"):
        module.raft_store_source()
