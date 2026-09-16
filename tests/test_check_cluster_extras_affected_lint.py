"""Behavior tests for scripts/check_cluster_extras_affected_lint.py's `main`
and the helpers it was decomposed into (`_reuse_previous_selection`,
`_rust_relevant_files`, `_decide_extras_reachability`).

`main` drives real `cargo metadata`/`cargo clippy` and reads
`push_gate_evidence`'s on-disk state, neither of which this pure-Python lane
should invoke -- every external boundary is monkeypatched with a
deterministic fake, and `run_heavy` itself is replaced with a stub that
records its reason and returns a sentinel code, so a change that regresses
any of `main`'s fail-closed/skip decisions fails this test.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]

# Pure/static test -- never needs the shared native engine (see
# conftest.py's session-scoped `start_epistemic_graph_server` fixture,
# which this marker exempts this module from triggering).
pytestmark = pytest.mark.no_engine

_HEAVY_SENTINEL = 77


def _load():
    path = ROOT / "scripts" / "check_cluster_extras_affected_lint.py"
    spec = importlib.util.spec_from_file_location(
        "check_cluster_extras_affected_lint", path
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["check_cluster_extras_affected_lint"] = module
    spec.loader.exec_module(module)
    return module


def _stub_run_heavy(monkeypatch, module, reasons: list[str]) -> None:
    def fake_run_heavy(reason: str) -> int:
        reasons.append(reason)
        return _HEAVY_SENTINEL

    monkeypatch.setattr(module, "run_heavy", fake_run_heavy)


def _disable_reuse(monkeypatch, module) -> None:
    monkeypatch.setattr(module, "_reuse_previous_selection", lambda: False)


def test_main_runs_heavy_when_changed_files_cannot_be_determined(monkeypatch):
    module = _load()
    _disable_reuse(monkeypatch, module)
    reasons: list[str] = []
    _stub_run_heavy(monkeypatch, module, reasons)
    monkeypatch.setattr(module, "_changed_files", lambda: None)

    assert module.main() == _HEAVY_SENTINEL
    assert "could not determine the push's changed-file range" in reasons[0]


def test_main_skips_heavy_when_no_rust_relevant_files_changed(monkeypatch, capsys):
    module = _load()
    _disable_reuse(monkeypatch, module)
    reasons: list[str] = []
    _stub_run_heavy(monkeypatch, module, reasons)
    monkeypatch.setattr(module, "_changed_files", lambda: ["README.md", "docs/x.md"])

    assert module.main() == 0
    assert reasons == []
    assert "no Rust-relevant files changed" in capsys.readouterr().out


def test_main_runs_heavy_when_cargo_toml_changed(monkeypatch):
    module = _load()
    _disable_reuse(monkeypatch, module)
    reasons: list[str] = []
    _stub_run_heavy(monkeypatch, module, reasons)
    monkeypatch.setattr(module, "_changed_files", lambda: ["Cargo.toml"])

    assert module.main() == _HEAVY_SENTINEL
    assert "the dependency graph itself moved" in reasons[0]


def test_main_runs_heavy_when_cargo_lock_changed(monkeypatch):
    module = _load()
    _disable_reuse(monkeypatch, module)
    reasons: list[str] = []
    _stub_run_heavy(monkeypatch, module, reasons)
    monkeypatch.setattr(
        module, "_changed_files", lambda: ["Cargo.lock", "crates/eg-core/src/lib.rs"]
    )

    assert module.main() == _HEAVY_SENTINEL
    assert "the dependency graph itself moved" in reasons[0]


def test_main_runs_heavy_when_reachability_cannot_be_computed(monkeypatch):
    module = _load()
    _disable_reuse(monkeypatch, module)
    reasons: list[str] = []
    _stub_run_heavy(monkeypatch, module, reasons)
    monkeypatch.setattr(module, "_changed_files", lambda: ["crates/eg-core/src/lib.rs"])

    def boom() -> set[str]:
        raise RuntimeError("cargo metadata exploded")

    monkeypatch.setattr(module, "reachable_from_extras", boom)

    assert module.main() == _HEAVY_SENTINEL
    assert "could not compute the cluster/full-extras-reachable crate set" in reasons[0]


def test_main_runs_heavy_when_touched_crate_reaches_extras(monkeypatch):
    module = _load()
    _disable_reuse(monkeypatch, module)
    reasons: list[str] = []
    _stub_run_heavy(monkeypatch, module, reasons)
    monkeypatch.setattr(module, "_changed_files", lambda: ["crates/eg-raft/src/lib.rs"])
    monkeypatch.setattr(module, "reachable_from_extras", lambda: {"eg-raft"})

    assert module.main() == _HEAVY_SENTINEL
    assert "reachable from cluster/full-extras" in reasons[0]
    assert "eg-raft" in reasons[0]


def test_main_skips_heavy_when_touched_crate_does_not_reach_extras(monkeypatch, capsys):
    module = _load()
    _disable_reuse(monkeypatch, module)
    reasons: list[str] = []
    _stub_run_heavy(monkeypatch, module, reasons)
    monkeypatch.setattr(module, "_changed_files", lambda: ["crates/eg-core/src/lib.rs"])
    monkeypatch.setattr(module, "reachable_from_extras", lambda: {"eg-raft"})

    assert module.main() == 0
    assert reasons == []
    out = capsys.readouterr().out
    assert "do not reach cluster/full-extras" in out
    assert "exhaustive leg still runs" in out


def test_main_returns_zero_when_previous_selection_is_reused(monkeypatch):
    module = _load()
    monkeypatch.setattr(module, "_reuse_previous_selection", lambda: True)

    def unexpected(*args: object, **kwargs: object) -> None:
        raise AssertionError("must not touch changed files when reuse applies")

    monkeypatch.setattr(module, "_changed_files", unexpected)

    assert module.main() == 0


def test_rust_relevant_files_keeps_only_cargo_and_rust_paths():
    module = _load()
    files = [
        "README.md",
        "Cargo.toml",
        "Cargo.lock",
        "build.rs",
        "crates/eg-core/src/lib.rs",
        "src/main.rs",
        "docs/notes.md",
    ]

    assert module._rust_relevant_files(files) == [
        "Cargo.toml",
        "Cargo.lock",
        "build.rs",
        "crates/eg-core/src/lib.rs",
        "src/main.rs",
    ]


def test_owning_crate_maps_crates_and_root_facade_paths():
    module = _load()
    assert module._owning_crate("crates/eg-raft/src/lib.rs") == "eg-raft"
    assert module._owning_crate("src/raft/placement.rs") == "epistemic-graph"
    assert module._owning_crate("build.rs") == "epistemic-graph"
    assert module._owning_crate("README.md") is None


def test_reuse_previous_selection_is_false_without_prior_evidence(monkeypatch):
    module = _load()
    monkeypatch.setattr(
        module.push_gate_evidence, "local_build_environment", lambda: None
    )
    monkeypatch.setattr(
        module.push_gate_evidence.EvidenceStore,
        "current",
        classmethod(lambda cls: None),
    )

    assert module._reuse_previous_selection() is False


def test_reuse_previous_selection_is_true_when_store_consumes_the_selection(
    monkeypatch, capsys
):
    module = _load()
    monkeypatch.setattr(
        module.push_gate_evidence, "local_build_environment", lambda: None
    )

    class _FakeStore:
        def consume(self, selection: object) -> bool:
            return True

    monkeypatch.setattr(
        module.push_gate_evidence.EvidenceStore,
        "current",
        classmethod(lambda cls: _FakeStore()),
    )

    assert module._reuse_previous_selection() is True
    assert "reusing the successful advisory" in capsys.readouterr().out


def test_reuse_previous_selection_is_false_on_evidence_error(monkeypatch):
    module = _load()
    monkeypatch.setattr(
        module.push_gate_evidence, "local_build_environment", lambda: None
    )

    class _FakeStore:
        def consume(self, selection: object) -> bool:
            raise module.push_gate_evidence.EvidenceError("corrupt evidence file")

    monkeypatch.setattr(
        module.push_gate_evidence.EvidenceStore,
        "current",
        classmethod(lambda cls: _FakeStore()),
    )

    assert module._reuse_previous_selection() is False
