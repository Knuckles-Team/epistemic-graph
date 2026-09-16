"""Proves ``scripts/ci_gate_replica.py::_run_step`` still does its three jobs
after being decomposed into ``_resolved_cargo_build_jobs``, ``_build_step_env``,
``_execute_step``, ``_absorb_github_env``, and ``_absorb_github_path``:

  1. runs the given shell text and reports its exit status/elapsed time;
  2. threads ``$GITHUB_ENV``/``$GITHUB_PATH`` writes made by the step back
     into the caller's ``job_env``, the same way GitHub Actions threads state
     between steps of one job;
  3. rejects a non-positive/non-int ``cargo_build_jobs`` override and clamps
     an oversized one to the local hard maximum.

None of this was covered by an existing test before this refactor -- these
tests are new, not moved, and each one is written so it fails against the
pre-refactor behavior it pins if that behavior regresses.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT_PATH = REPO_ROOT / "scripts" / "ci_gate_replica.py"

# Shells out only to `bash -c` for short, deterministic, offline commands --
# never touches the compiled engine.
pytestmark = pytest.mark.no_engine


def _load_module():
    spec = importlib.util.spec_from_file_location(
        "ci_gate_replica_run_step", SCRIPT_PATH
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def module():
    return _load_module()


def test_run_step_reports_exit_status_and_elapsed(module):
    status, elapsed = module._run_step("exit 7", job_env={}, cargo_build_jobs=1)
    assert status == 7
    assert elapsed >= 0.0


def test_run_step_threads_github_env_into_job_env(module):
    job_env: dict = {"PATH": "/usr/bin"}
    status, _ = module._run_step(
        'echo "GREETING=hello there" >> "$GITHUB_ENV"',
        job_env=job_env,
        cargo_build_jobs=1,
    )
    assert status == 0
    assert job_env["GREETING"] == "hello there"


def test_run_step_threads_github_path_into_job_env(module):
    job_env: dict = {"PATH": "/usr/bin"}
    status, _ = module._run_step(
        'echo "/opt/fixture-bin" >> "$GITHUB_PATH"',
        job_env=job_env,
        cargo_build_jobs=1,
    )
    assert status == 0
    assert job_env["PATH"].split(module.os.pathsep)[0] == "/opt/fixture-bin"


def test_run_step_sets_cargo_build_jobs_env_var(module):
    job_env: dict = {}
    status, _ = module._run_step(
        'test "$CARGO_BUILD_JOBS" = "2"',
        job_env=job_env,
        cargo_build_jobs=2,
    )
    assert status == 0


def test_resolved_cargo_build_jobs_clamps_to_local_maximum(module):
    assert module._resolved_cargo_build_jobs(999) == module.MAX_LOCAL_CARGO_BUILD_JOBS


@pytest.mark.parametrize("bad_value", [0, -1, "4", True, 1.5])
def test_resolved_cargo_build_jobs_rejects_non_positive_int(module, bad_value):
    with pytest.raises(ValueError):
        module._resolved_cargo_build_jobs(bad_value)
