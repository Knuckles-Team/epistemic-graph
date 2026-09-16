"""Behavior tests for scripts/check_dupehound.py's `main` and the helpers it
was decomposed into (`_run_dupehound`, `_load_reviewed_register`,
`_report_rotted_register`, `_report_clean_or_unregistered`).

`main` drives a real `dupehound` binary and a real git diff, neither of which
this pure-Python lane should invoke, so every external boundary (path
selection, the binary invocation, and the reviewed-distinct register) is
monkeypatched with deterministic fakes -- a change that regresses any of
`main`'s decision branches (no changed source / clean / rotted register /
unregistered clone) must fail this test.
"""

from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]

# Pure/static test -- never needs the shared native engine (see
# conftest.py's session-scoped `start_epistemic_graph_server` fixture,
# which this marker exempts this module from triggering).
pytestmark = pytest.mark.no_engine


def _load():
    path = ROOT / "scripts" / "check_dupehound.py"
    spec = importlib.util.spec_from_file_location("check_dupehound", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["check_dupehound"] = module
    spec.loader.exec_module(module)
    return module


def _completed(returncode: int, stdout: str) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(
        args=["dupehound"], returncode=returncode, stdout=stdout, stderr=""
    )


def _no_findings_payload() -> str:
    import json

    return json.dumps({"schema_version": 1, "findings": []})


def _one_finding_payload() -> str:
    import json

    finding = {
        "file": "src/new.py",
        "line": 4,
        "name": "new",
        "similarity": 0.91,
        "original_file": "src/old.py",
        "original_line": 4,
        "original_name": "old",
    }
    return json.dumps({"schema_version": 1, "findings": [finding]})


def _stub_environment(dupehound, monkeypatch, *, paths=("src/new.py",)):
    """Bypass git/tool resolution: force a fixed changed-path set and a fake
    binary, so `main` reaches its own decision logic."""
    monkeypatch.setattr(dupehound, "changed_paths", lambda base_ref: list(paths))
    monkeypatch.setattr(
        dupehound,
        "selected_paths",
        lambda changed, config: sorted(changed) if paths else [],
    )
    monkeypatch.setattr(
        dupehound, "_resolve_dupehound", lambda config: "/fake/dupehound"
    )
    monkeypatch.setattr(dupehound, "_check_version", lambda executable, config: None)


def test_main_reports_ok_when_no_changed_supported_source(monkeypatch, capsys):
    dupehound = _load()
    _stub_environment(dupehound, monkeypatch, paths=())

    code = dupehound.main([])

    assert code == 0
    assert "no changed supported-language source" in capsys.readouterr().out


def test_main_reports_ok_when_dupehound_finds_nothing(monkeypatch, capsys):
    dupehound = _load()
    _stub_environment(dupehound, monkeypatch)
    monkeypatch.setattr(
        dupehound,
        "_run_dupehound",
        lambda executable, config, args: _completed(0, _no_findings_payload()),
    )
    monkeypatch.setattr(dupehound.dupehound_ledger, "load_register", lambda: [])
    # Real `partition` calls `resolved_reason`, which stats the finding's
    # (real) files -- there are none, so pin the wiring in isolation with a
    # direct fake rather than depending on filesystem-dependent rot checks.
    monkeypatch.setattr(
        dupehound.dupehound_ledger,
        "partition",
        lambda findings, register: ([], [], [], []),
    )

    code = dupehound.main([])

    assert code == 0
    assert "no changed function reimplementation" in capsys.readouterr().out


def test_main_reports_fail_on_unregistered_clone(monkeypatch, capsys):
    dupehound = _load()
    _stub_environment(dupehound, monkeypatch)
    monkeypatch.setattr(
        dupehound,
        "_run_dupehound",
        lambda executable, config, args: _completed(1, _one_finding_payload()),
    )
    monkeypatch.setattr(dupehound.dupehound_ledger, "load_register", lambda: [])
    finding = {
        "file": "src/new.py",
        "line": 4,
        "name": "new",
        "similarity": 0.91,
        "original_file": "src/old.py",
        "original_line": 4,
        "original_name": "old",
    }
    monkeypatch.setattr(
        dupehound.dupehound_ledger,
        "partition",
        lambda findings, register: ([finding], [], [], []),
    )

    code = dupehound.main([])

    out = capsys.readouterr().out
    assert code == 1
    assert "FAIL: 1 structural clone(s)" in out
    assert "src/new.py:4 new reimplements src/old.py:4 old" in out


def test_main_reports_fail_on_rotted_unused_register_entry(monkeypatch, capsys):
    dupehound = _load()
    _stub_environment(dupehound, monkeypatch)
    monkeypatch.setattr(
        dupehound,
        "_run_dupehound",
        lambda executable, config, args: _completed(0, _no_findings_payload()),
    )
    monkeypatch.setattr(
        dupehound.dupehound_ledger, "load_register", lambda: ["fake-pair"]
    )
    monkeypatch.setattr(
        dupehound.dupehound_ledger,
        "partition",
        lambda findings, register: ([], [], [], ["fake-pair"]),
    )

    code = dupehound.main([])

    out = capsys.readouterr().out
    assert code == 1
    assert "reviewed-distinct entr(ies) no longer" in out


def test_main_reports_fail_on_rotted_changed_register_entry(monkeypatch, capsys):
    dupehound = _load()
    _stub_environment(dupehound, monkeypatch)
    monkeypatch.setattr(
        dupehound,
        "_run_dupehound",
        lambda executable, config, args: _completed(0, _no_findings_payload()),
    )
    monkeypatch.setattr(
        dupehound.dupehound_ledger, "load_register", lambda: ["fake-pair"]
    )
    changed_finding = {
        "file": "src/new.py",
        "line": 4,
        "name": "new",
        "similarity": 0.91,
        "original_file": "src/old.py",
        "original_line": 4,
        "original_name": "old",
    }
    monkeypatch.setattr(
        dupehound.dupehound_ledger,
        "partition",
        lambda findings, register: ([], [changed_finding], [], []),
    )

    code = dupehound.main([])

    out = capsys.readouterr().out
    assert code == 1
    assert "reviewed-distinct pair(s) changed" in out


def test_main_reports_ok_with_still_holding_register(monkeypatch, capsys):
    dupehound = _load()
    _stub_environment(dupehound, monkeypatch)
    monkeypatch.setattr(
        dupehound,
        "_run_dupehound",
        lambda executable, config, args: _completed(0, _no_findings_payload()),
    )
    monkeypatch.setattr(
        dupehound.dupehound_ledger, "load_register", lambda: ["fake-pair"]
    )
    monkeypatch.setattr(
        dupehound.dupehound_ledger,
        "partition",
        lambda findings, register: ([], [], [], []),
    )

    code = dupehound.main([])

    out = capsys.readouterr().out
    assert code == 0
    assert "1 reviewed-distinct pair(s) still hold" in out
