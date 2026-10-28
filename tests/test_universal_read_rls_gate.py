"""Bind the universal read-RLS architecture gate to EG-DURABLE-KERNEL-R029."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.no_engine

ROOT = Path(__file__).resolve().parents[1]


def _gate_module():
    """Load the gate the same way its direct hook invocation loads it."""

    scripts = str(ROOT / "scripts")
    if scripts not in sys.path:
        sys.path.insert(0, scripts)
    path = ROOT / "scripts" / "check_universal_read_rls.py"
    spec = importlib.util.spec_from_file_location("universal_read_rls_gate", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.mark.spec("EG-DURABLE-KERNEL-R029")
def test_universal_read_rls_gate_checks_current_ts_scan_dispatch(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Run the gate against the current executor and require its success record."""

    _gate_module().main()
    result = json.loads(capsys.readouterr().out)

    assert result["ok"] is True
    assert result["coverage_scope"] == "universal_served_reads"
