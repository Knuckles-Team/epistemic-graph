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


def _replace_once(source: str, old: str, new: str) -> str:
    """Build a non-vacuous known-bad source perturbation."""

    assert source.count(old) == 1, f"fixture target is not unique: {old}"
    broken = source.replace(old, new, 1)
    assert broken != source
    return broken


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

    # Target the variant declaration itself (four-space indent), not a family
    # predicate pattern that also names the variant.
    declaration = "\n    CreateNodeIfAbsent {"
    assert protocol.count(declaration) == 1
    omitted = protocol.replace(declaration, "\n    CreateNodeIfAbsentRemoved {", 1)
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


def test_raft_snapshot_check_follows_compiler_declared_children(
    tmp_path: Path, monkeypatch
) -> None:
    module = _gate_module()
    store_dir = tmp_path / "src" / "raft"
    child_dir = store_dir / "store"
    child_dir.mkdir(parents=True)
    (store_dir / "store.rs").write_text(
        "const RAFT_SNAPSHOT_SCHEMA_VERSION: u16 = 4;\n"
        "struct GraphSnapshot {\n"
        "    durable: crate::server::persistence::online_reshard::RawGraphRows,\n"
        "}\n"
        "mod snapshot;\n",
        encoding="utf-8",
    )
    child = child_dir / "snapshot.rs"
    child.write_text(
        "fn export_graph_raw_for_snapshot() {}\n"
        "fn read_authoritative_graph_snapshot() {}\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(module, "ROOT", tmp_path)

    store = module.raft_store_source()
    module._check_raft_snapshot_shape(store)

    child.write_text(
        "/*\n"
        "fn export_graph_raw_for_snapshot() {}\n"
        "fn read_authoritative_graph_snapshot() {}\n"
        "*/\n",
        encoding="utf-8",
    )
    with pytest.raises(SystemExit, match="duplicate decoded/plaintext"):
        module._check_raft_snapshot_shape(module.raft_store_source())

    (store_dir / "store.rs").write_text(
        (store_dir / "store.rs")
        .read_text(encoding="utf-8")
        .replace("mod snapshot;\n", ""),
        encoding="utf-8",
    )
    with pytest.raises(SystemExit, match="orphan Rust module files"):
        module.raft_store_source()


def test_raft_snapshot_replacement_rejects_dead_helper_declaration() -> None:
    module = _gate_module()
    store = module.raft_store_source()
    module._check_raft_snapshot_replacement(store)

    broken = _replace_once(
        store,
        "self.remove_stale_snapshot_graph(&name).await?;",
        "let _stale_graph_is_incorrectly_retained = name;",
    )
    assert "fn remove_stale_snapshot_graph(" in broken
    with pytest.raises(SystemExit, match="merges with stale graph authority"):
        module._check_raft_snapshot_replacement(broken)


def test_raft_snapshot_replacement_rejects_string_literal_call_spoof() -> None:
    module = _gate_module()
    store = module.raft_store_source()
    module._check_raft_snapshot_replacement(store)

    broken = _replace_once(
        store,
        "self.remove_stale_snapshot_graph(&name).await?;",
        'let _spoof = "self.remove_stale_snapshot_graph(&name).await?;";',
    )
    with pytest.raises(SystemExit, match="merges with stale graph authority"):
        module._check_raft_snapshot_replacement(broken)


def test_raft_snapshot_replacement_rejects_global_marker_spoof() -> None:
    module = _gate_module()
    store = module.raft_store_source()
    module._check_raft_snapshot_replacement(store)

    broken = _replace_once(
        store,
        'if stale.iter().any(|name| name == "__commons__") {',
        "if false {",
    )
    assert "Raft snapshot omits the mandatory commons graph" in broken
    with pytest.raises(SystemExit, match="merges with stale graph authority"):
        module._check_raft_snapshot_replacement(broken)


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


def _client_batch_sources(module):
    return (
        module.read("epistemic_graph/client.py"),
        module.read("epistemic_graph/generated/graph.py"),
        module.read("epistemic_graph/generated/messaging.py"),
    )


def test_client_batch_contract_passes_on_current_sources() -> None:
    module = _gate_module()
    client, generated_graph, generated_messaging = _client_batch_sources(module)
    module._check_client_batch_contract(client, generated_graph, generated_messaging)


def test_client_create_if_absent_contract_rejects_missing_binary_pack() -> None:
    module = _gate_module()
    client, generated_graph, _ = _client_batch_sources(module)
    module._check_client_create_if_absent_contract(client, generated_graph)
    broken = _replace_once(
        client,
        "def _pack_binary_msgpack(value: Any) -> bytes:",
        "def _pack_binary_msgpack_renamed(value: Any) -> bytes:",
    )
    with pytest.raises(SystemExit, match="native binary MessagePack"):
        module._check_client_create_if_absent_contract(broken, generated_graph)


def test_client_create_if_absent_contract_rejects_lost_generated_transport() -> None:
    module = _gate_module()
    client, generated_graph, _ = _client_batch_sources(module)
    broken_graph = generated_graph.replace(
        "CreateNodeIfAbsentRequest", "RenamedRequest"
    )
    with pytest.raises(SystemExit, match="binary create-if-absent contract"):
        module._check_client_create_if_absent_contract(client, broken_graph)


def test_client_tag_ack_nack_contract_rejects_unfenced_ack() -> None:
    module = _gate_module()
    client, _, _ = _client_batch_sources(module)
    module._check_client_tag_ack_nack_contract(client)
    broken = _replace_once(
        client,
        '"delivery_tag": int(delivery_tag), "consumer": consumer',
        '"delivery_tag": int(delivery_tag)',
    )
    with pytest.raises(SystemExit, match="tag acknowledgement is not owner-fenced"):
        module._check_client_tag_ack_nack_contract(broken)


def test_client_tag_ack_nack_contract_rejects_nack_missing_clock() -> None:
    module = _gate_module()
    client, _, _ = _client_batch_sources(module)
    # The check is a whole-file substring test (not scoped to nack_tag's own
    # body), and `"now_ms": int(now_ms)` recurs across several client methods
    # (nack, renew, ...) -- so every occurrence must be removed to make the
    # marker actually absent from `client`, matching what the check tests.
    assert '"now_ms": int(now_ms)' in client
    broken = client.replace('"now_ms": int(now_ms)', '"now_ms": now_ms_value')
    with pytest.raises(SystemExit, match="tag nack omits its owner or explicit clock"):
        module._check_client_tag_ack_nack_contract(broken)


def test_client_renew_tag_contract_rejects_missing_lease_field() -> None:
    module = _gate_module()
    client, _, _ = _client_batch_sources(module)
    module._check_client_renew_tag_contract(client)
    broken = _replace_once(
        client, "send_broker_renew_tag(", "send_broker_renew_tag_renamed("
    )
    with pytest.raises(SystemExit, match="lease renewal is not owner-fenced"):
        module._check_client_renew_tag_contract(broken)


def test_generated_broker_transport_contract_rejects_missing_owner_field() -> None:
    module = _gate_module()
    _, _, generated_messaging = _client_batch_sources(module)
    module._check_generated_broker_transport_contract(generated_messaging)
    broken = generated_messaging.replace("BrokerRenewTagRequest", "RenamedRequest")
    with pytest.raises(SystemExit, match="broker transport lost owner and clock"):
        module._check_generated_broker_transport_contract(broken)
