"""EG-CONTRACT-R009: the release workflow runs the full root Python suite.

``release.yml`` already ran the crate tests (``gates``) and the duplication
census (``scanner-quality``), but historically had no step that ran the
~2,800 cases under root ``tests/`` -- so client/contract/connector-pack
regressions in that suite could reach ``main`` unexecuted (EH-468). The
``python-suite`` job closes that gap: it builds the numeric kernel and the
engine binary once, then runs the whole ``tests/`` suite under pytest-xdist
(parallel workers) with a per-test timeout. Its ``timeout-minutes`` is the
same pull_request/push-gated expression every other gating job in this
workflow uses, so the identical step runs both as the pre-merge gate replica
(on a pull request) and as part of the real release (on push/tag) -- there is
only one job definition, not a separate copy that could drift.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

REPO = Path(__file__).resolve().parents[1]
WORKFLOW = REPO / ".github" / "workflows" / "release.yml"


def _workflow() -> dict:
    return yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))


def _python_suite_job() -> dict:
    return _workflow()["jobs"]["python-suite"]


def _test_step(job: dict) -> dict:
    for step in job["steps"]:
        if str(step.get("name", "")).startswith("Test (root Python suite"):
            return step
    raise AssertionError("python-suite job has no root-suite Test step")


@pytest.mark.spec("EG-CONTRACT-R009")
def test_python_suite_job_runs_the_full_root_tests_directory() -> None:
    step = _test_step(_python_suite_job())
    run = step["run"]
    assert "pytest tests/" in run
    # Parallel execution (pytest-xdist worker count) ...
    assert " -n " in run
    # ... and a per-test timeout, not just an overall job timeout.
    assert "--timeout=900" in run


@pytest.mark.spec("EG-CONTRACT-R009")
def test_python_suite_job_builds_the_real_artifacts_before_testing() -> None:
    job = _python_suite_job()
    step_names = [str(step.get("name", "")) for step in job["steps"]]
    assert any("numeric kernel" in name for name in step_names)
    assert any("pyengine extension" in name for name in step_names)
    assert any("engine binary" in name for name in step_names)


@pytest.mark.spec("EG-CONTRACT-R009")
def test_pre_merge_gate_replica_runs_the_identical_suite() -> None:
    """The same job is the pre-merge gate on a PR and the release gate on push:
    one `timeout-minutes` expression, gated on `github.event_name ==
    'pull_request'`, with no second copy of the Test step for either event."""
    job = _python_suite_job()
    timeout_expr = job["timeout-minutes"]
    assert "github.event_name == 'pull_request'" in timeout_expr
    assert job["steps"].count(_test_step(job)) == 1


@pytest.mark.spec("EG-CONTRACT-R009")
def test_crate_and_duplication_census_jobs_still_run_alongside_it() -> None:
    """R009 adds the root suite ALONGSIDE the existing crate tests (`gates`)
    and duplication census (`scanner-quality`), not instead of them."""
    jobs = _workflow()["jobs"]
    assert "gates" in jobs
    assert "scanner-quality" in jobs
    scanner_step_names = [
        str(step.get("name", "")) for step in jobs["scanner-quality"]["steps"]
    ]
    assert any("test_dupehound_census" in str(step.get("run", "")) for step in jobs["scanner-quality"]["steps"]) or any(
        "dupehound" in name.lower() for name in scanner_step_names
    )
