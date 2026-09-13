"""Focused tests for the exact fault/restart static checker."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

pytestmark = pytest.mark.no_engine


def _checker_module():
    path = (
        Path(__file__).resolve().parents[1]
        / "scripts"
        / "check_exact_fault_restart_harness.py"
    )
    spec = importlib.util.spec_from_file_location("exact_fault_restart_gate", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_exact_fault_checker_reads_decomposed_contracts() -> None:
    module = _checker_module()

    assert module._check_fault_seam_contract() == []
    assert module._check_store_contract() == []


def test_fault_checker_rejects_comment_or_string_marker_spoof(monkeypatch) -> None:
    module = _checker_module()
    relative = "crates/eg-types/src/mutation_batch.rs"
    source = module._read_rust_module(relative)
    marker = "pub enum MutationCommitPhase"
    source = source.replace(marker, "pub enum RenamedCommitPhase", 1)
    source += f'\n// {marker}\nconst SPOOF: &str = "{marker}";\n'
    monkeypatch.setattr(module, "_read_rust_module", lambda _relative: source)

    errors = module._check_fault_seam_contract()

    assert "fault seam: missing 'pub enum MutationCommitPhase'" in errors


def test_fault_checker_rejects_retired_request_binding_even_with_decoys(
    monkeypatch,
) -> None:
    module = _checker_module()
    relative = "crates/eg-types/src/mutation_batch.rs"
    source = module._read_rust_module(relative)
    source = source.replace(
        "batch_matches_request(batch, spec.request_id)",
        "spec.request_id == batch.context.request_id",
        1,
    )
    source = source.replace(
        "envelope.authority.request_id.as_str()",
        "batch.context.request_id.as_str()",
        1,
    )
    source += """
fn decoy_fault_markers() {
    let _ = batch_matches_request(batch, spec.request_id);
    let _ = batch.envelope.operation();
    let _ = envelope.authority.request_id.as_str();
    let _ = crate::mutation_batch::request_opaque_id(request_id);
}
"""
    monkeypatch.setattr(module, "_read_rust_module", lambda _relative: source)

    errors = module._check_fault_seam_contract()

    assert (
        "fault seam apply path: missing "
        "'batch_matches_request(batch, spec.request_id)'"
    ) in errors
    assert (
        "fault seam request identity helper: missing "
        "'envelope.authority.request_id.as_str()'"
    ) in errors


def test_store_checker_binds_commit_to_saga_call_path(monkeypatch) -> None:
    module = _checker_module()
    original = module._read
    relative = "crates/eg-transaction/src/saga.rs"
    source = original(relative)
    marker = "commit(write, batch)?;"
    source = source.replace(marker, "finish_removed(write, batch)?;")

    def read(path: str):
        return source if path == relative else original(path)

    monkeypatch.setattr(module, "_read", read)

    errors = module._check_store_contract()

    assert "native commit helper and saga: missing 'commit(write, batch)?;'" in errors


def test_store_checker_rejects_an_omitted_graph_store_child(monkeypatch) -> None:
    """The graph-store phase inventory must include every declared child."""

    module = _checker_module()
    facade = "src/redb_store.rs"
    omitted = "src/redb_store/store_batch.rs"
    original_reader = module._read_rust_module
    assembled = original_reader(facade)
    child = module._read(omitted)
    assert child in assembled

    def read_module(relative: str) -> str:
        if relative == facade:
            return assembled.replace(child, "", 1)
        return original_reader(relative)

    monkeypatch.setattr(module, "_read_rust_module", read_module)

    errors = module._check_store_contract()

    assert (
        "graph mutation store: missing "
        "'MutationCommitPhase::BeforeRows'"
    ) in errors
