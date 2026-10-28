"""Topology contract for the central reusable lightweight-gates caller."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest
import yaml

pytestmark = pytest.mark.no_engine

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github/workflows/ci.yml"
CENTRAL = "Knuckles-Team/pipelines/.github/workflows/medulla_light_gates.yml@main"
RUNNER = (
    "${{ vars.ARBORA_TRUSTED_CI_RUNNER == 'true' && "
    "'\"arbora-ci-trusted\"' || '\"ubuntu-latest\"' }}"
)

# Every blocking job, including additive full-native pull-request opt-in.
REQUIRED_BLOCKING_JOBS = [
    "light-gates",
    "changes",
    "security",
    "gates-contract",
    "gates-wheel",
    "gates-whisper",
    "gates-crates",
    "gates-storage",
    "gates-pyo3",
    "python-kernels",
    "python-server",
    "python-suite",
    "tts-piper-inference",
    "language-clients",
    "quality-advisory",
    "gates",
    "gates-plan-fuzz",
    "gates-facade",
    "gates-variants",
    "clippy-release-profiles",
    "candidate-server-wheel",
]
FULL_NATIVE_JOBS = {
    "gates",
    "gates-plan-fuzz",
    "gates-facade",
    "gates-variants",
    "clippy-release-profiles",
}
PATH_GATED_JOBS = {
    "gates-wheel": "packaging",
    "gates-pyo3": "packaging",
    "gates-whisper": "whisper",
    "tts-piper-inference": "tts",
    "language-clients": "clients",
}
NON_BLOCKING_JOBS = {
    "feature-matrix",
    "benchmarks",
    "wheel-cache",
    "server-wheel-cache",
    "engine-main-wheel",
}


def _jobs() -> dict:
    return yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))["jobs"]


def _needs(job: dict) -> set[str]:
    value = job.get("needs", [])
    return {value} if isinstance(value, str) else set(value)


def test_light_gates_is_the_single_central_light_gate_call() -> None:
    jobs = _jobs()
    assert jobs["light-gates"] == {"uses": CENTRAL, "with": {"runner": RUNNER}}
    # Native jobs read scope from the local job, never from the reusable
    # call, so they do not wait behind the central pre-commit leg.
    assert "uses" not in jobs["changes"]
    for name, job in jobs.items():
        if name not in {"ci-required", "engine-main-wheel", "owner-format-repair"}:
            assert "light-gates" not in _needs(job), name
    assert "lint-and-architecture" not in jobs
    assert "spec-gates" not in jobs


def test_all_native_scope_consumers_keep_the_changes_output_edge() -> None:
    jobs = _jobs()
    expected = {
        "security",
        "gates",
        "gates-contract",
        "owner-format-repair",
        "gates-plan-fuzz",
        "gates-wheel",
        "gates-facade",
        "gates-whisper",
        "gates-variants",
        "gates-crates",
        "gates-storage",
        "gates-pyo3",
        "python-kernels",
        "python-server",
        "python-suite",
        "tts-piper-inference",
        "language-clients",
        "quality-advisory",
        "clippy-release-profiles",
        "engine-main-wheel",
        "candidate-server-wheel",
    }
    actual = {
        name
        for name, job in jobs.items()
        if name != "ci-required" and "changes" in _needs(job)
    }
    assert actual == expected
    for name in expected:
        assert "needs.changes.outputs.code" in str(
            jobs[name].get("if", "")
        ) or name in {
            "security",
            "engine-main-wheel",
            "owner-format-repair",
        }


def test_full_native_opt_in_adds_jobs_without_changing_default_pr_scope() -> None:
    jobs = _jobs()
    for name in FULL_NATIVE_JOBS:
        assert jobs[name]["if"] == (
            "needs.changes.outputs.code == 'true' && "
            "(github.event_name != 'pull_request' || "
            "needs.changes.outputs.full_native == 'true')"
        ), name
    for name, gate in PATH_GATED_JOBS.items():
        assert jobs[name]["if"] == (
            "needs.changes.outputs.code == 'true' && "
            "(github.event_name != 'pull_request' || "
            "needs.changes.outputs.full_native == 'true' || "
            f"needs.changes.outputs.{gate} == 'true')"
        ), name
    assert "scanner-quality" not in jobs


def test_publication_waits_for_the_light_gate_call_without_deleted_job_names() -> None:
    jobs = _jobs()
    needs = _needs(jobs["engine-main-wheel"])
    assert {"light-gates", "changes"} <= needs
    assert FULL_NATIVE_JOBS <= needs
    assert "lint-and-architecture" not in needs
    assert "spec-gates" not in needs
    for job in jobs.values():
        assert "lint-and-architecture" not in _needs(job)
        assert "spec-gates" not in _needs(job)


def test_ci_required_is_the_fail_closed_branch_protection_aggregate() -> None:
    jobs = _jobs()
    required = jobs["ci-required"]
    assert required["name"] == "CI required"
    assert required["if"] == "always()"
    assert required["runs-on"] == "ubuntu-latest"
    assert required["needs"] == REQUIRED_BLOCKING_JOBS
    assert not NON_BLOCKING_JOBS & set(required["needs"])

    step = required["steps"][0]
    assert step["name"] == "Require every blocking CI result"
    assert step["env"]["CODE_SCOPE"] == "${{ needs.changes.outputs.code }}"
    for job_name in REQUIRED_BLOCKING_JOBS:
        env_name = job_name.upper().replace("-", "_") + "_RESULT"
        assert step["env"][env_name] == f"${{{{ needs.{job_name}.result }}}}"

    run = step["run"]
    assert "set -euo pipefail" in run
    assert 'result="${result:-MISSING}"' in run
    assert '[[ "$result" == success ]]' in run
    assert '[[ "$CODE_SCOPE" == false ]]' in run
    for job_name, gate in PATH_GATED_JOBS.items():
        assert f'[{job_name}]="${gate.upper()}_SCOPE"' in run
        assert step["env"][f"{gate.upper()}_SCOPE"] == (
            f"${{{{ needs.changes.outputs.{gate} }}}}"
        )
    assert '"$EVENT_NAME" == pull_request' in run
    assert "accepted path-gate skip" in run
    assert "expected skipped for documentation-only scope" in run
    assert "failures=()" in run
    assert "exit 1" in run


def test_owner_format_repair_failure_or_full_native_cannot_publish() -> None:
    jobs = _jobs()
    repair = jobs["owner-format-repair"]
    condition = repair["if"]
    assert "!cancelled()" in condition
    assert "github.event_name == 'pull_request'" in condition
    assert "needs.changes.result == 'failure'" in condition
    assert "needs.light-gates.result == 'failure'" in condition
    assert set(_needs(repair)) == {"changes", "light-gates"}
    assert " ".join(condition.split()) == (
        "${{ !cancelled() && github.event_name == 'pull_request' && "
        "(needs.changes.result == 'failure' || "
        "needs.light-gates.result == 'failure' || "
        "(needs.changes.outputs.code == 'true' && "
        "needs.changes.outputs.full_native == 'true')) }}"
    )
    assert "owner-format-repair" not in _needs(jobs["ci-required"])
    assert repair["permissions"] == {"contents": "read"}
    assert repair["env"]["CARGO_BUILD_JOBS"] == 1
    assert repair["env"]["CARGO_INCREMENTAL"] == 0
    assert repair["steps"][-1]["with"]["if-no-files-found"] == "error"
    assert "if" not in repair["steps"][-1]
    generation = next(
        step["run"]
        for step in repair["steps"]
        if step.get("name") == "Generate exact owner-format repair evidence"
    )
    for token in (
        "gen_owner_store_formats",
        "--locked",
        "source_revision",
        "source_tree",
        "recipe_sha256",
        "size_bytes",
        "check=True",
        "exist_ok=False",
    ):
        assert token in generation
    assert "owner-format-repair" not in _needs(jobs["engine-main-wheel"])
    assert jobs["gates-storage"]["if"] == "needs.changes.outputs.code == 'true'"
    assert jobs["gates-contract"]["if"] == "needs.changes.outputs.code == 'true'"


def _aggregate_env(
    *, event: str = "pull_request", full: bool = False, code: bool = True
) -> dict[str, str]:
    env = {
        "EVENT_NAME": event,
        "CODE_SCOPE": str(code).lower(),
        "FULL_NATIVE": str(full).lower(),
        **{f"{gate.upper()}_SCOPE": "false" for gate in PATH_GATED_JOBS.values()},
    }
    for job in REQUIRED_BLOCKING_JOBS:
        result = "success"
        if job not in {"light-gates", "changes", "security"}:
            if not code or (
                event == "pull_request"
                and not full
                and job in FULL_NATIVE_JOBS | PATH_GATED_JOBS.keys()
            ):
                result = "skipped"
        if job == "candidate-server-wheel":
            result = (
                "success" if code and full and event == "pull_request" else "skipped"
            )
        env[job.upper().replace("-", "_") + "_RESULT"] = result
    return env


def _run_aggregate(env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    # Execute the actual checked-in shell, not a Python reimplementation of it.
    run = _jobs()["ci-required"]["steps"][0]["run"]
    return subprocess.run(
        ["bash", "--noprofile", "--norc", "-c", run],
        env={"PATH": os.defpath, **env},
        text=True,
        capture_output=True,
        timeout=10,
        check=False,
    )


@pytest.mark.parametrize(
    "event", ["pull_request", "push", "schedule", "workflow_dispatch"]
)
@pytest.mark.parametrize("full", [False, True])
@pytest.mark.parametrize("code", [False, True])
def test_aggregate_accepts_exact_expected_matrix(
    event: str, full: bool, code: bool
) -> None:
    result = _run_aggregate(_aggregate_env(event=event, full=full, code=code))
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize(
    "job",
    sorted(FULL_NATIVE_JOBS | PATH_GATED_JOBS.keys() | {"candidate-server-wheel"}),
)
@pytest.mark.parametrize("outcome", ["failure", "cancelled", "skipped", "", None])
def test_full_native_aggregate_refuses_every_unproven_job(
    job: str, outcome: str | None
) -> None:
    env = _aggregate_env(full=True)
    key = job.upper().replace("-", "_") + "_RESULT"
    if outcome is None:
        del env[key]
    else:
        env[key] = outcome
    assert _run_aggregate(env).returncode != 0


@pytest.mark.parametrize(
    "key",
    [
        "CODE_SCOPE",
        "FULL_NATIVE",
        "PACKAGING_SCOPE",
        "WHISPER_SCOPE",
        "TTS_SCOPE",
        "CLIENTS_SCOPE",
    ],
)
@pytest.mark.parametrize("value", ["", "TRUE", "null", None])
@pytest.mark.parametrize("full", [False, True])
def test_aggregate_refuses_malformed_scope_outputs(
    key: str, value: str | None, full: bool
) -> None:
    env = _aggregate_env(full=full)
    if value is None:
        del env[key]
    else:
        env[key] = value
    assert _run_aggregate(env).returncode != 0


@pytest.mark.parametrize("job,gate", sorted(PATH_GATED_JOBS.items()))
def test_default_pr_path_skip_requires_false_gate(job: str, gate: str) -> None:
    env = _aggregate_env()
    env[f"{gate.upper()}_SCOPE"] = "true"
    assert _run_aggregate(env).returncode != 0
    # A shared packaging gate requires both of its jobs, not only the first.
    for gated_job, own_gate in PATH_GATED_JOBS.items():
        if own_gate == gate:
            env[gated_job.upper().replace("-", "_") + "_RESULT"] = "success"
    result = _run_aggregate(env)
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize("event", ["push", "schedule", "workflow_dispatch"])
@pytest.mark.parametrize("job", sorted(FULL_NATIVE_JOBS | PATH_GATED_JOBS.keys()))
def test_non_pr_aggregate_never_accepts_native_skip(event: str, job: str) -> None:
    env = _aggregate_env(event=event)
    env[job.upper().replace("-", "_") + "_RESULT"] = "skipped"
    assert _run_aggregate(env).returncode != 0


@pytest.mark.parametrize("job", REQUIRED_BLOCKING_JOBS)
def test_docs_aggregate_requires_exact_skips_and_successful_foundation(
    job: str,
) -> None:
    env = _aggregate_env(code=False, full=True)
    key = job.upper().replace("-", "_") + "_RESULT"
    env[key] = "skipped" if env[key] == "success" else "success"
    assert _run_aggregate(env).returncode != 0


def test_full_native_label_does_not_expand_events_or_permissions() -> None:
    workflow = yaml.load(WORKFLOW.read_text(), Loader=yaml.BaseLoader)
    assert set(workflow["on"]) == {
        "push",
        "pull_request",
        "schedule",
        "workflow_dispatch",
    }
    assert workflow["on"]["pull_request"] == ""
    assert workflow["on"]["push"] == {"branches": ["main"]}
    assert workflow["permissions"] == {"contents": "read"}
    assert workflow["jobs"]["changes"]["outputs"]["full_native"] == (
        "${{ github.event_name == 'pull_request' && "
        "contains(github.event.pull_request.labels.*.name, 'ci:full-native') }}"
    )
    # Native opt-in neither uses pull_request_target nor grants token write scopes.
    for name in (
        FULL_NATIVE_JOBS
        | PATH_GATED_JOBS.keys()
        | {"changes", "gates-crates", "ci-required"}
    ):
        job = workflow["jobs"][name]
        assert all(
            value in {"read", "none"} for value in job.get("permissions", {}).values()
        )
        assert "secrets" not in job
        assert "pull_request.head.repo.fork" not in job.get("if", "")


def test_full_native_workspace_uses_full_sweep_not_pr_subset() -> None:
    job = _jobs()["gates-crates"]
    assert job["timeout-minutes"] == (
        "${{ github.event_name == 'pull_request' && "
        "needs.changes.outputs.full_native != 'true' && 10 || 60 }}"
    )
    steps = {step.get("name"): step for step in job["steps"]}
    full = steps[
        "Test (every other workspace crate, all features; push to main / release tag)"
    ]
    assert full["if"] == (
        "github.job == 'gates-crates' && (github.event_name != 'pull_request' || "
        "needs.changes.outputs.full_native == 'true')"
    )
    assert "cargo test --locked --workspace" in full["run"]
    assert "--all-features --no-fail-fast" in full["run"]
    for name in [
        "Resolve the published base",
        "Resolve pull-request test scope (workspace crates)",
        "Test (every other workspace crate -- pull request fast subset)",
    ]:
        assert steps[name]["if"] == (
            "github.job == 'gates-crates' && github.event_name == 'pull_request' && "
            "needs.changes.outputs.full_native != 'true'"
        )
