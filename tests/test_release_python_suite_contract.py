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
import re
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
    job: dict = {
        "steps": sum(
            (
                _workflow()["jobs"][name]["steps"]
                for name in ("python-kernels", "python-server", "python-suite")
            ),
            [],
        )
    }
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


def test_parallel_preparation_is_exact_revision_and_same_run() -> None:
    jobs = _workflow()["jobs"]
    assert jobs["python-suite"]["needs"] == [
        "changes",
        "python-kernels",
        "python-server",
    ]
    for name in ("python-kernels", "python-server"):
        job = jobs[name]
        assert job["needs"] == "changes"
        assert job["if"] == "needs.changes.outputs.code == 'true'"
        assert job["steps"][0]["with"]["ref"] == "${{ github.sha }}"
        upload = next(
            s
            for s in job["steps"]
            if s.get("uses", "").startswith("actions/upload-artifact@")
        )
        assert upload["with"]["if-no-files-found"] == "error"
        assert upload["with"]["name"] == name + "-${{ github.sha }}"
    consumer = jobs["python-suite"]
    assert consumer["if"] == "always() && needs.changes.outputs.code == 'true'"
    assert (
        consumer["steps"][0]["run"]
        == 'test "$KERNELS_RESULT" = success && test "$SERVER_RESULT" = success'
    )
    assert consumer["steps"][1]["with"]["ref"] == "${{ github.sha }}"
    downloads = [
        s
        for s in consumer["steps"]
        if s.get("uses", "").startswith("actions/download-artifact@")
    ]
    assert len(downloads) == 2
    assert {s["with"]["name"] for s in downloads} == {
        "python-kernels-${{ github.sha }}",
        "python-server-${{ github.sha }}",
    }
    assert all(set(s["with"]) == {"name", "path"} for s in downloads)
    runs = "\n".join(s.get("run", "") for s in consumer["steps"])
    assert "cargo build" not in runs
    assert "unpack kernels" in runs and "unpack server" in runs
    assert "assert e.__engine__ == 'eg-pyengine'" in runs
    assert "maturin pep517 write-dist-info" in runs
    assert "if" not in _test_step(consumer)


def test_validation_profile_is_warmed_without_affecting_release_wheels() -> None:
    workflow = _workflow()
    assert not CODEGEN_ENV.keys() & workflow.get("env", {}).keys()
    for name, job in workflow["jobs"].items():
        if name == "python-kernels":
            assert job["env"] == CODEGEN_ENV
        else:
            assert not CODEGEN_ENV.keys() & job.get("env", {}).keys()
        for step in job.get("steps", []):
            assert not any(key in str(step) for key in CODEGEN_ENV)
    profile = tomllib.loads((REPO / "Cargo.toml").read_text())["profile"]
    assert (
        profile["release"]["opt-level"],
        profile["release"]["lto"],
        profile["release"]["codegen-units"],
    ) == (3, "thin", 1)
    assert profile["dev"]["opt-level"] == 1
    builds = [
        s["run"] for s in workflow["jobs"]["python-server"]["steps"] if "run" in s
    ]
    assert "cargo build --locked --features full --bin epistemic-graph-server" in builds


def test_compiler_cache_is_installed_before_precisely_two_step_local_wrappers() -> None:
    workflow = _workflow()
    jobs = workflow["jobs"]
    commands = {
        "python-server": (
            "Build the full-featured engine binary once",
            "cargo build --locked --features full --bin epistemic-graph-server",
        ),
        "gates-facade": (
            "Test (facade full)",
            "python3 scripts/cargo_test_rescue.py cargo test -p epistemic-graph "
            "--features full --no-fail-fast --timings",
        ),
    }
    cache_backend = {
        "SCCACHE_IDLE_TIMEOUT": "7200",
        "SCCACHE_GHA_ENABLED": "true",
        "SCCACHE_GHA_RW_MODE": (
            "${{ github.ref == 'refs/heads/main' && 'READ_WRITE' || 'READ_ONLY' }}"
        ),
    }
    wrapped = []
    setups = []
    for job_name, (step_name, command) in commands.items():
        job = jobs[job_name]
        assert job["timeout-minutes"] == (
            "${{ github.event_name == 'pull_request' && 10 || 60 }}"
        )
        assert int(cache_backend["SCCACHE_IDLE_TIMEOUT"]) > 60 * 60
        steps = job["steps"]
        setup = next(
            s for s in steps if s.get("name") == "Install validation compiler cache"
        )
        build = next(s for s in steps if s.get("name") == step_name)
        setups.append(setup)
        wrapped.append(build)
        assert setup["uses"] == (
            "mozilla-actions/sccache-action@fc920bf0ec8de6ee65d409111f7ec508035751ba"
        )
        assert setup["with"] == {"version": "v0.16.0"}
        assert steps.index(setup) < steps.index(build)
        assert "if" not in setup and "if" not in build
        assert build["run"] == command
        assert build["env"] == {"RUSTC_WRAPPER": "sccache"}
        assert job["env"] == cache_backend
        stats = next(s for s in steps if "sccache --show-stats" in s.get("run", ""))
        assert stats["if"] == "always()"
        assert stats["run"].splitlines() == [
            "echo 'Cache statistics cover the current daemon only; "
            "a restart resets its counters.'",
            "sccache --show-stats",
        ]
        assert steps.index(stats) > steps.index(build)
        # With no reachable daemon, stats synthesize zeroes from caller config.
        # Both reporting paths must inherit the compilation backend settings.
        for cache_step in (setup, build, stats):
            effective_env = {
                **workflow["env"],
                **job["env"],
                **cache_step.get("env", {}),
            }
            assert {key: effective_env[key] for key in cache_backend} == cache_backend
    # The shared anchors keep both installations and cache policies identical.
    assert setups[0] is setups[1]
    assert wrapped[0]["env"] is wrapped[1]["env"]
    assert jobs["python-server"]["env"] is jobs["gates-facade"]["env"]
    steps = jobs["python-server"]["steps"]
    pack = next(
        s for s in steps if s.get("name") == "Package exact-revision server binary"
    )
    assert steps.index(wrapped[0]) < steps.index(pack)
    assert "if" not in pack
    assert workflow["env"]["CARGO_INCREMENTAL"] == "0"
    assert "RUSTC_WRAPPER" not in workflow["env"]
    assert not cache_backend.keys() & workflow["env"].keys()
    for job_name, job in jobs.items():
        assert "RUSTC_WRAPPER" not in job.get("env", {})
        if job_name not in commands:
            assert not cache_backend.keys() & job.get("env", {}).keys()
        for step in job.get("steps", []):
            if all(step is not build for build in wrapped):
                assert "RUSTC_WRAPPER" not in step.get("env", {})


@pytest.mark.spec("EG-CONTRACT-R009")
def test_facade_timings_are_current_run_evidence_without_replacing_tests() -> None:
    workflow = _workflow()
    job = workflow["jobs"]["gates-facade"]
    steps = job["steps"]
    config = tomllib.loads((REPO / ".cargo/config.toml").read_text())
    assert config["build"]["target-dir"] == "target-isolated"
    assert "CARGO_TARGET_DIR" not in workflow["env"]
    assert "CARGO_TARGET_DIR" not in job["env"]
    assert all("CARGO_TARGET_DIR" not in s.get("env", {}) for s in steps)
    clear = next(
        s for s in steps if s.get("name") == "Clear previous facade build timings"
    )
    build = next(s for s in steps if s.get("name") == "Test (facade full)")
    upload = next(s for s in steps if s.get("name") == "Upload facade build timings")
    cache = next(
        s for s in steps if s.get("uses", "").startswith("Swatinem/rust-cache@")
    )
    assert (
        steps.index(cache)
        < steps.index(clear)
        < steps.index(build)
        < steps.index(upload)
    )
    assert clear["run"] == "rm -rf target-isolated/cargo-timings"
    assert "if" not in clear and "continue-on-error" not in build
    assert "--timings" in build["run"] and "--no-run" not in build["run"]
    assert upload["if"] == "always()"
    assert (
        upload["uses"]
        == "actions/upload-artifact@ea165f8d65b6e75b540449e92b4886f43607fa02"
    )
    assert upload["with"] == {
        "name": (
            "facade-build-timings-${{ github.sha }}-"
            "${{ github.run_id }}-${{ github.run_attempt }}"
        ),
        "path": "target-isolated/cargo-timings/",
        "if-no-files-found": "warn",
        "retention-days": 1,
    }


@pytest.mark.spec("EG-CONTRACT-R009")
def test_only_verified_empty_helper_bins_disable_default_test_harnesses() -> None:
    manifest = tomllib.loads((REPO / "Cargo.toml").read_text())
    disabled = {
        target["name"]: target["path"]
        for target in manifest["bin"]
        if target.get("test") is False
    }
    assert disabled == {
        "epistemic-graph-rf019-observe": "src/bin/rf019_observe.rs",
        "migrate-shards": "src/bin/migrate_shards.rs",
        "restore": "src/bin/restore.rs",
        "lake-fixture-export": "src/bin/lake_fixture_export.rs",
    }
    assert manifest["package"].get("autotests", True) is True
    assert manifest.get("lib", {}).get("test", True) is True
    assert all(target.get("test", True) for target in manifest.get("test", []))
    for path in disabled.values():
        # Fail closed if a helper gains test attributes or includes other source.
        # Re-enable its harness before adding tests or a module with possible tests.
        source = (REPO / path).read_text()
        assert not re.search(
            r"#\s*\[[^\]]*\btest\b|\bmod\s+\w+|\binclude\s*!", source
        ), path


@pytest.mark.parametrize("kind", ["kernels", "server"])
@pytest.mark.parametrize("damage", [None, "digest", "revision", "source", "missing"])
def test_native_artifact_roundtrip_fails_closed(
    tmp_path: Path, kind: str, damage: str | None
) -> None:
    script = REPO / "scripts/python_suite_artifact.sh"
    root = tmp_path / "checkout"
    root.mkdir()
    subprocess.run(["git", "init", "-q", str(root)], check=True)
    subprocess.run(
        [
            "git",
            "-C",
            str(root),
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.com",
            "commit",
            "--allow-empty",
            "-qm",
            "fixture",
        ],
        check=True,
    )
    sha = subprocess.check_output(
        ["git", "-C", str(root), "rev-parse", "HEAD"], text=True
    ).strip()
    env = {**os.environ, "RUNNER_TEMP": str(tmp_path / "artifacts"), "GITHUB_SHA": sha}
    paths = (
        ["epistemic_graph/numeric.test.so", "epistemic_graph/engine.test.so"]
        if kind == "kernels"
        else ["target-isolated/debug/epistemic-graph-server"]
    )
    for name in paths:
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"native fixture")
        path.chmod(0o755)
    subprocess.run(["bash", str(script), "pack", kind], cwd=root, env=env, check=True)
    bundle = Path(env["RUNNER_TEMP"]) / f"python-{kind}"
    for name in paths:
        (root / name).unlink()
    if damage == "digest":
        (bundle / "payload.tar").write_bytes(b"corrupt")
    elif damage == "revision":
        env["GITHUB_SHA"] = "0" * 40
    elif damage == "source":
        (bundle / "source.sha").write_text("0" * 40 + "\n")
        with (bundle / "SHA256SUMS").open("w") as manifest:
            subprocess.run(
                ["sha256sum", "source.sha", "payload.tar"],
                cwd=bundle,
                stdout=manifest,
                check=True,
            )
    elif damage == "missing":
        (bundle / "payload.tar").unlink()
    result = subprocess.run(
        ["bash", str(script), "unpack", kind], cwd=root, env=env, check=False
    )
    assert (result.returncode == 0) == (damage is None)
    if damage is None:
        assert all((root / name).read_bytes() == b"native fixture" for name in paths)
        assert all(os.access(root / name, os.X_OK) for name in paths)


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


def test_python_suite_installs_native_scorer_tracing_before_full_suite() -> None:
    job = _python_suite_job()
    steps = job["steps"]
    install = next(
        step
        for step in steps
        if step.get("name") == "Install native scorer tracing prerequisite"
    )
    assert "apt-get install -y --no-install-recommends strace" in install["run"]
    assert "strace --help | grep -F -- '--kill-on-exit'" in install["run"]
    assert "if" not in install
    assert steps.index(install) < steps.index(_test_step(job))
    assert " -m " not in _test_step(job)["run"].split("pytest tests/", 1)[1], (
        "native acceptance must not be silently deselected"
    )
