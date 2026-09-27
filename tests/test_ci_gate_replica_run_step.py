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


# ── The replica as the shared landing gate (--jobs / --base-ref / --all-blocking,
# step-scoped `env:`, and tool-installation steps replaced by verification). ──


def test_step_env_is_applied_and_resolves_the_replicated_push(module, monkeypatch):
    monkeypatch.setattr(module, "_GITHUB_CONTEXT", module.push_event_context("abc123"))
    job_env: dict = {"PATH": "/usr/bin:/bin"}
    step_env = {
        "PUSH_BEFORE_SHA": "${{ github.event.before }}",
        "PR_BASE_SHA": "${{ github.event.pull_request.base.sha }}",
    }
    status, _ = module._run_step(
        'test "$PUSH_BEFORE_SHA" = abc123 && test -z "$PR_BASE_SHA" '
        '&& test "${{ github.event.before }}" = abc123',
        job_env=job_env,
        cargo_build_jobs=1,
        step_env=step_env,
    )
    assert status == 0
    # Step env is scoped to its step, never leaked into the job.
    assert "PUSH_BEFORE_SHA" not in job_env


def test_without_a_base_ref_push_expressions_still_strip_to_empty(module, monkeypatch):
    monkeypatch.setattr(module, "_GITHUB_CONTEXT", {})
    status, _ = module._run_step(
        'test -z "$B" && test -z "${{ github.event.before }}"',
        job_env={"PATH": "/usr/bin:/bin"},
        cargo_build_jobs=1,
        step_env={"B": "${{ github.event.before }}"},
    )
    assert status == 0


def test_select_plan_filters_jobs_fails_closed_and_can_make_everything_blocking(module):
    plan = [
        {"job": "gates", "blocking": True},
        {"job": "feature-matrix#full", "blocking": False},
        {"job": "quality-advisory", "blocking": False},
    ]
    selected = module.select_plan(plan, "quality-advisory,feature-matrix")
    assert [row["job"] for row in selected] == [
        "feature-matrix#full",
        "quality-advisory",
    ]
    assert [row["blocking"] for row in selected] == [False, False]
    landing = module.select_plan(plan, None, all_blocking=True)
    assert all(row["blocking"] for row in landing) and len(landing) == 3
    with pytest.raises(ValueError, match="nope"):
        module.select_plan(plan, "gates,nope")
    one_leg = module.select_plan(plan, "feature-matrix#full")
    assert [row["job"] for row in one_leg] == ["feature-matrix#full"]
    with pytest.raises(ValueError, match="feature-matrix#absent"):
        module.select_plan(plan, "feature-matrix#absent")


def test_resolve_commit_rejects_options_and_unknown_refs(module):
    with pytest.raises(ValueError):
        module.resolve_commit("--all")
    with pytest.raises(ValueError):
        module.resolve_commit("refs/heads/no-such-branch-gate-speed")


def test_apt_setup_step_is_verified_from_its_own_package_list():
    import ci_replica.workflow_plan as plan

    check = plan.apt_verification(
        "sudo apt-get update\n"
        "sudo apt-get install -y --no-install-recommends libsasl2-dev \\\n"
    )
    assert "libsasl2-dev" in check and "-y" not in check.split(";")[0]
    # A package provided by an installed one (pkg-config <- pkgconf) is present.
    assert "${Provides}" in check
    status = (
        __import__("subprocess")
        .run(
            [
                "bash",
                "-c",
                plan.apt_verification("apt-get install -y gate-speed-no-such-pkg"),
            ],
            capture_output=True,
        )
        .returncode
    )
    assert status == 1


def test_a_renamed_setup_step_is_reported_as_drift(module):
    import ci_replica.drift as drift

    doc = {
        "jobs": {"security": {"steps": [{"name": "Renamed install", "run": "true"}]}}
    }
    spec = module.WORKFLOW_REGISTRY["release.yml"]
    report = drift._setup_step_drift([("release.yml", spec, doc)])
    assert any("Install pinned cargo-deny" in bullet for bullet in report.bullets)


def test_run_step_honours_the_step_working_directory(module):
    status, _ = module._run_step(
        'test "$(basename "$PWD")" = js && test -f index.mjs',
        job_env={"PATH": "/usr/bin:/bin"},
        cargo_build_jobs=1,
        working_directory="clients/js",
    )
    assert status == 0


def test_plan_rows_carry_the_workflow_working_directory(module):
    doc = module.load_workflow(module.WORKFLOWS_DIR / "release.yml")
    plan, _, _ = module.build_plan_for_workflow(
        module.WORKFLOW_REGISTRY["release.yml"], doc
    )
    rows = {row["name"]: row for row in plan if row["job"] == "scanner-quality"}
    assert rows["Install locked thin-client dependencies"]["working_directory"] == (
        "clients/js"
    )
    assert rows["Verify scanner versions"]["working_directory"] == ""


# ── AF_UNIX: pytest's temp stays short enough for the engine socket. ──


def test_run_step_leads_pytest_addopts_with_a_short_basetemp(module):
    status, _ = module._run_step(
        'case "$PYTEST_ADDOPTS" in "--basetemp=/"*" -q") exit 0;; esac; exit 1',
        job_env={"PYTEST_ADDOPTS": "-q"},
        cargo_build_jobs=1,
    )
    assert status == 0


def test_readonly_preferred_temp_falls_back_to_short_writable_temp(monkeypatch):
    sys.path.insert(0, str(SCRIPT_PATH.parent))
    from ci_replica import pytest_temp

    previous = pytest_temp._BASETEMP
    real_mkdtemp = pytest_temp.tempfile.mkdtemp
    preferred, fallback = pytest_temp._parents()
    attempted = []

    def mkdtemp(*, prefix, dir):
        attempted.append(dir)
        if dir == preferred:
            raise PermissionError("read-only replica sandbox")
        return real_mkdtemp(prefix=prefix, dir=dir)

    monkeypatch.setattr(pytest_temp.tempfile, "mkdtemp", mkdtemp)
    monkeypatch.setattr(pytest_temp, "_BASETEMP", None)
    try:
        basetemp = pytest_temp.short_basetemp()
        assert attempted == [preferred, fallback]
        assert basetemp.is_relative_to(fallback)
        assert pytest_temp.fits_socket_limit(basetemp)
    finally:
        if pytest_temp._BASETEMP is not None:
            pytest_temp.shutil.rmtree(pytest_temp._BASETEMP.parent)
        pytest_temp._BASETEMP = previous


def test_no_writable_short_temp_root_fails_closed(monkeypatch):
    sys.path.insert(0, str(SCRIPT_PATH.parent))
    from ci_replica import pytest_temp

    def readonly(*, prefix, dir):
        raise PermissionError(f"{dir} is read-only")

    monkeypatch.setattr(pytest_temp.tempfile, "mkdtemp", readonly)
    monkeypatch.setattr(pytest_temp, "_BASETEMP", None)
    with pytest.raises(RuntimeError, match="no writable short pytest temp root"):
        pytest_temp.short_basetemp()


def test_the_deepest_engine_socket_under_the_short_basetemp_binds(tmp_path):
    import socket

    sys.path.insert(0, str(SCRIPT_PATH.parent))
    from ci_replica import pytest_temp

    basetemp = pytest_temp.short_basetemp()
    assert pytest_temp.fits_socket_limit(basetemp)
    target = Path(str(basetemp) + pytest_temp.SOCKET_TAIL)
    target.parent.mkdir(parents=True, exist_ok=True)
    with socket.socket(socket.AF_UNIX) as server:
        server.bind(str(target))
    target.unlink()


def test_the_limit_check_agrees_with_the_kernel_on_a_long_base(tmp_path):
    import socket

    sys.path.insert(0, str(SCRIPT_PATH.parent))
    from ci_replica import pytest_temp

    long_base = tmp_path / ("x" * 60)
    assert not pytest_temp.fits_socket_limit(long_base)
    target = Path(str(long_base) + pytest_temp.SOCKET_TAIL)
    target.parent.mkdir(parents=True, exist_ok=True)
    with socket.socket(socket.AF_UNIX) as server, pytest.raises(OSError):
        server.bind(str(target))
