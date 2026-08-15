"""Meta-test: prove scripts/ci_gate_replica.py's anti-drift guard actually fires.

The whole point of parsing .github/workflows/release.yml at run time (see that
script's module docstring) instead of hand-copying its steps is that a NEW job
release.yml adds can never be silently skipped — --consistency-check must fail
loudly instead. This test proves that claim rather than merely asserting it in
a comment: it takes the real, live release.yml, injects one new top-level job
the script has never seen, and checks that consistency_check() rejects it (and
that the ORIGINAL, unmodified file still passes).
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT_PATH = REPO_ROOT / "scripts" / "ci_gate_replica.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("ci_gate_replica", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    return module


def test_real_release_yaml_passes_consistency_check():
    m = _load_module()
    doc = m.load_workflow(m.DEFAULT_WORKFLOW_PATH)
    assert m.consistency_check(doc, verbose=False) is True


def test_injected_new_job_fails_consistency_check():
    m = _load_module()
    doc = m.load_workflow(m.DEFAULT_WORKFLOW_PATH)
    doc["jobs"]["totally-new-release-job"] = {
        "runs-on": "ubuntu-latest",
        "steps": [{"name": "do a new blocking thing", "run": "echo hi"}],
    }
    assert m.consistency_check(doc, verbose=False) is False


def test_removing_a_configured_job_fails_consistency_check():
    """The opposite drift direction: EXECUTABLE_JOBS/JOB_SKIP_REASONS naming a
    job that no longer exists in release.yml (renamed or deleted) must also
    fail, not silently pass with less coverage than the config claims."""
    m = _load_module()
    doc = m.load_workflow(m.DEFAULT_WORKFLOW_PATH)
    assert "gates" in m.EXECUTABLE_JOBS
    del doc["jobs"]["gates"]
    assert m.consistency_check(doc, verbose=False) is False


def test_gates_job_run_steps_include_the_numeric_kernel_parity_chain():
    """Regression guard for the exact drift class this script was built to
    close: a hand-copied step list previously omitted the eg-numeric kernel
    build + numpy-parity gate steps from the `gates` job. Parsing must always
    surface them as RUN steps."""
    m = _load_module()
    doc = m.load_workflow(m.DEFAULT_WORKFLOW_PATH)
    plan, _, _ = m.build_plan(doc)
    gates_run_names = {p["name"] for p in plan if p["job"] == "gates" and p["mode"] == "RUN"}
    for expected in (
        "Build eg-numeric Surface-A wheel (feature python)",
        "numpy-parity gate (compiled kernel vs numpy)",
        "Audit numeric wheel for retained build identity",
    ):
        assert expected in gates_run_names, f"missing from parsed gates RUN plan: {expected!r}"
