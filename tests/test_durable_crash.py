"""Exact-artifact process-fault and restart certification.

The certification implementation lives in
``scripts/certify_exact_fault_restart.py`` so release pipelines can run it
without pytest or the suite's shared engine fixture.  This wrapper is opt-in: no
artifact means the ordinary source suite skips it, while a partially configured
or invalid explicit artifact is a hard failure rather than a skip or fallback.

The opt-in signal is the DEDICATED ``EPISTEMIC_GRAPH_EXACT_CERTIFICATION=1``
flag, deliberately independent of ``EPISTEMIC_GRAPH_TEST_BINARY``/``_SHA256``.
Those two are also the routine perf-optimization pair BUG-045
(``tests/conftest.py``'s ``_prebuilt_test_binary()``) documents setting on
EVERY local/CI run to skip the shared session engine's own ``cargo build`` --
gating this expensive, opt-in release-gate certification on their mere
presence made an ordinary fast run hard-fail here by accident (every private
per-file ``cargo build`` this suite used to pay for masked the interaction
behind a 60s pytest-timeout error until those were fixed to honor the shared
prebuilt binary too).
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.exact_artifact

ROOT = Path(__file__).resolve().parents[1]
HARNESS = ROOT / "scripts" / "certify_exact_fault_restart.py"


@pytest.mark.timeout(3600)
def test_exact_binary_fault_restart_matrix(tmp_path: Path) -> None:
    if os.environ.get("EPISTEMIC_GRAPH_EXACT_CERTIFICATION", "").strip() != "1":
        pytest.skip(
            "exact-artifact certification was not requested "
            "(set EPISTEMIC_GRAPH_EXACT_CERTIFICATION=1 to opt in)"
        )
    binary = str(os.environ.get("EPISTEMIC_GRAPH_TEST_BINARY", "") or "").strip()
    digest = str(
        os.environ.get("EPISTEMIC_GRAPH_TEST_BINARY_SHA256", "") or ""
    ).strip()
    assert binary, "EPISTEMIC_GRAPH_TEST_BINARY is required for exact certification"
    assert digest, (
        "EPISTEMIC_GRAPH_TEST_BINARY_SHA256 is required for exact certification"
    )

    evidence_path = tmp_path / "exact-fault-restart.json"
    completed = subprocess.run(  # noqa: S603 - fixed harness and explicit artifact
        [
            sys.executable,
            str(HARNESS),
            "--binary",
            binary,
            "--binary-sha256",
            digest,
            "--output",
            str(evidence_path),
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
        timeout=3500,
    )
    assert completed.returncode == 0, "exact artifact certification failed"
    evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
    assert evidence["binary"] == {"sha256": digest}
    assert evidence["summary"] == {
        "matrix_cases": 60,
        "passed": 60,
        "status": "pass",
    }
    assert evidence["g05"]["tenant_qualified_time_series"]["passed"] is True
    assert evidence["g05"]["spatial_restart_lazy_open"]["passed"] is True
