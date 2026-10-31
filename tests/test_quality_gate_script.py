"""Static proofs for the combined quality-gate script (MEDULLA-SDLC-R007)."""

from __future__ import annotations

import importlib.machinery
import types
from pathlib import Path

import pytest

pytestmark = pytest.mark.no_engine

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "check_quality_gates.py"


def _gate_module():
    loader = importlib.machinery.SourceFileLoader(
        "check_quality_gates_under_test", str(SCRIPT)
    )
    module = types.ModuleType(loader.name)
    module.__file__ = str(SCRIPT)
    loader.exec_module(module)
    return module


def _hook_entries() -> dict[str, str]:
    """Map hook id -> entry from .config/pre-commit.yaml without a YAML dependency."""

    entries: dict[str, str] = {}
    hook_id = None
    for line in (ROOT / ".config" / "pre-commit.yaml").read_text().splitlines():
        stripped = line.strip()
        if stripped.startswith("- id:"):
            hook_id = stripped.split(":", 1)[1].strip()
        elif stripped.startswith("entry:") and hook_id is not None:
            entries[hook_id] = stripped.split(":", 1)[1].strip()
    return entries


def test_combined_gate_covers_every_staged_scanner_with_hook_commands() -> None:
    gate = _gate_module()
    names = [name for name, _ in gate.SCANNERS]
    assert names == [
        "complexity-staged",
        "kiss-changed-rust",
        "dupehound-changed-functions",
        "jscpd-differential",
        "guardrail-tracked-privacy",
    ]
    entries = _hook_entries()
    for name, argv in gate.SCANNERS:
        expected = entries[name]
        if argv[:2] == ("bash", "-c"):
            assert expected == f"bash -c '{argv[2]}'", name
        else:
            assert expected == " ".join(argv), name
    assert (
        "entry: python3 scripts/check_quality_gates.py"
        in (ROOT / ".config" / "pre-commit.yaml").read_text()
    )


@pytest.mark.spec("MEDULLA-SDLC-R007")
def test_exit_code_follows_every_scanner(tmp_path: Path) -> None:
    gate = _gate_module()
    ok = ("python3", "-c", "raise SystemExit(0)")
    marker = tmp_path / "ran"
    late = ("python3", "-c", f"open({str(marker)!r}, 'w').close()")
    assert gate.run_gates((("a", ok), ("b", ok)), cwd=tmp_path)[0] == 0
    code, results = gate.run_gates(
        (("a", ("python3", "-c", "raise SystemExit(1)")), ("b", late)), cwd=tmp_path
    )
    assert code == 1 and results == [("a", 1), ("b", 0)]
    assert marker.exists(), "later scanners still run after a failure"
    code, results = gate.run_gates(
        (("a", ok), ("b", ("/nonexistent/scanner",))), cwd=tmp_path
    )
    assert code == 1 and results[1] == ("b", 2)
    assert gate.run_gates((), cwd=tmp_path)[0] == 1
