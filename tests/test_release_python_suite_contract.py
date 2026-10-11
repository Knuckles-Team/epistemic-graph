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

import os
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest
import tomllib
import yaml

from scripts import _kernel_build

REPO = Path(__file__).resolve().parents[1]
WORKFLOW = REPO / ".github" / "workflows" / "release.yml"
# These tests inspect workflow configuration only, without building an engine.
pytestmark = pytest.mark.no_engine


def _workflow() -> dict:
    return yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))


def _python_suite_job() -> dict:
    return _workflow()["jobs"]["python-suite"]


def _test_step(job: dict) -> dict:
    for step in job["steps"]:
        if str(step.get("name", "")).startswith("Test (root Python suite"):
            return step
    raise AssertionError("python-suite job has no root-suite Test step")


@pytest.mark.parametrize(
    "job_name",
    ["gates", "gates-facade", "gates-variants", "gates-crates", "python-suite"],
)
def test_capability_dependencies_bootstrap_uv_for_the_test_interpreter(
    job_name: str,
) -> None:
    steps = _workflow()["jobs"][job_name]["steps"]
    install = next(
        step
        for step in steps
        if step.get("name") == "Install locked capability-test Python dependencies"
    )
    setup = next(
        step
        for step in steps[: steps.index(install)]
        if step.get("uses", "").startswith("astral-sh/setup-uv@")
    )
    assert setup["uses"] == (
        "astral-sh/setup-uv@d0cc045d04ccac9d8b7881df0226f9e82c39688e"
    )
    assert setup["with"]["version"] == "0.11.7"
    assert "if" not in setup
    assert 'uv pip install --python "$(command -v python)" ' in install["run"]
    python_setup = next(
        step
        for step in steps[: steps.index(install)]
        if step.get("uses", "").startswith("actions/setup-python@")
        or step.get("with", {}).get("activate-environment") is True
    )
    assert "if" not in python_setup


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
    assert any(
        "test_dupehound_census" in str(step.get("run", ""))
        for step in jobs["scanner-quality"]["steps"]
    ) or any("dupehound" in name.lower() for name in scanner_step_names)


CODEGEN_ENV = {
    "CARGO_PROFILE_RELEASE_OPT_LEVEL": "0",
    "CARGO_PROFILE_RELEASE_LTO": "off",
    "CARGO_PROFILE_RELEASE_CODEGEN_UNITS": "256",
}


def _codegen_step() -> dict:
    return next(
        step
        for step in _python_suite_job()["steps"]
        if step.get("name") == "Use fast native codegen for Python PR validation"
    )


def test_pr_codegen_runs_before_cache_and_preserves_build_and_test_commands(
    tmp_path: Path,
) -> None:
    steps = _python_suite_job()["steps"]
    step = _codegen_step()
    assert step["if"] == "github.event_name == 'pull_request'"
    cache = next(
        s for s in steps if s.get("uses", "").startswith("Swatinem/rust-cache@")
    )
    assert steps.index(step) < steps.index(cache)
    for command in (
        "python3 scripts/build_numeric_kernel.py",
        "python3 scripts/build_pyengine_kernel.py",
        "cargo build --locked --features full --bin epistemic-graph-server",
    ):
        build = next(s for s in steps if s.get("run") == command)
        assert (
            steps.index(cache)
            < steps.index(build)
            < steps.index(_test_step(_python_suite_job()))
        )
        assert "if" not in build
    assert "if" not in _test_step(_python_suite_job())
    output = tmp_path / "github-env"
    subprocess.run(
        ["bash", "-e", "-c", step["run"]],
        env={**os.environ, "GITHUB_ENV": str(output)},
        check=True,
    )
    assert (
        dict(line.split("=", 1) for line in output.read_text().splitlines())
        == CODEGEN_ENV
    )


def test_main_and_tag_pushes_keep_optimized_release_defaults() -> None:
    workflow = _workflow()
    assert _codegen_step()["if"] == "github.event_name == 'pull_request'"
    assert not CODEGEN_ENV.keys() & workflow.get("env", {}).keys()
    for name, job in workflow["jobs"].items():
        assert not CODEGEN_ENV.keys() & job.get("env", {}).keys()
        for step in job.get("steps", []):
            if any(key in str(step) for key in CODEGEN_ENV):
                assert name == "python-suite"
                assert step == _codegen_step()
    profile = tomllib.loads((REPO / "Cargo.toml").read_text())["profile"]["release"]
    assert (profile["opt-level"], profile["lto"], profile["codegen-units"]) == (
        3,
        "thin",
        1,
    )


@pytest.mark.parametrize(
    ("module", "crate", "features"),
    [
        ("numeric", "eg-numeric", ("python",)),
        ("engine", "eg-pyengine", ("python", "security")),
    ],
)
@pytest.mark.parametrize("fast_codegen", [True, False])
def test_mounted_kernel_command_propagates_codegen_without_changing_release_contract(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    module: str,
    crate: str,
    features: tuple[str, ...],
    fast_codegen: bool,
) -> None:
    for key, value in CODEGEN_ENV.items():
        if fast_codegen:
            monkeypatch.setenv(key, value)
        else:
            monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("CARGO_TARGET_DIR", "/unrelated-worktree")
    monkeypatch.setattr(_kernel_build, "TARGET_PACKAGE", tmp_path)
    monkeypatch.setattr(_kernel_build, "TARGET_DIR", tmp_path / "target-isolated")
    monkeypatch.setattr(_kernel_build.shutil, "which", lambda _: "/fake/maturin")
    spec = _kernel_build.KernelSpec(
        module, crate, "unused", "unused", crate, "unused", features
    )

    def run(
        command: list[str], *, env: dict, check: bool
    ) -> subprocess.CompletedProcess:
        assert command[:7] == [
            "maturin",
            "build",
            "--release",
            "-m",
            str(spec.manifest),
            "--features",
            ",".join(features),
        ]
        assert command[7:9] == ["--target-dir", str(tmp_path / "target-isolated")]
        assert {key: env[key] for key in CODEGEN_ENV if key in env} == (
            CODEGEN_ENV if fast_codegen else {}
        )
        assert "CARGO_TARGET_DIR" not in env
        assert not check
        (Path(command[-1]) / "kernel.whl").write_bytes(b"wheel")
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.setattr(_kernel_build.subprocess, "run", run)
    monkeypatch.setattr(
        _kernel_build,
        "_load_injector",
        lambda _: SimpleNamespace(
            _find_kernel_extension=lambda _: (b"kernel", 0o755, f"{module}.so")
        ),
    )
    destination = _kernel_build.build_kernel(spec)
    assert destination.read_bytes() == b"kernel"
    assert destination.stat().st_mode & 0o777 == 0o755
