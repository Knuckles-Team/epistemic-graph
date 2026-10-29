"""Topology contract for the central reusable lightweight-gates caller."""

from __future__ import annotations

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github/workflows/ci.yml"
CENTRAL = "Knuckles-Team/pipelines/.github/workflows/medulla_light_gates.yml@main"
RUNNER = (
    "${{ vars.ARBORA_TRUSTED_CI_RUNNER == 'true' && "
    "'\"arbora-ci-trusted\"' || '\"ubuntu-latest\"' }}"
)

# Every job that can run on a pull request. Main-only jobs fail the workflow
# run release.yml requires, not the branch-protection aggregate.
REQUIRED_BLOCKING_JOBS = [
    "light-gates",
    "changes",
    "security",
    "gates-contract",
    "gates-wheel",
    "gates-whisper",
    "gates-ontology",
    "gates-crates",
    "gates-storage",
    "gates-pyo3",
    "python-kernels",
    "python-server",
    "python-suite",
    "tts-piper-inference",
    "language-clients",
    "quality-advisory",
]
MAIN_ONLY_JOBS = {
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
    "gates-ontology": "ontology",
    "tts-piper-inference": "tts",
    "language-clients": "clients",
}
NON_BLOCKING_JOBS = {
    *MAIN_ONLY_JOBS,
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
        if name not in {"ci-required", "engine-main-wheel"}:
            assert "light-gates" not in _needs(job), name
    assert "lint-and-architecture" not in jobs
    assert "spec-gates" not in jobs


def test_all_native_scope_consumers_keep_the_changes_output_edge() -> None:
    jobs = _jobs()
    expected = {
        "security",
        "gates",
        "gates-contract",
        "gates-plan-fuzz",
        "gates-wheel",
        "gates-facade",
        "gates-whisper",
        "gates-ontology",
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
        }


def test_main_only_and_path_gated_jobs_skip_on_pull_requests() -> None:
    jobs = _jobs()
    for name in MAIN_ONLY_JOBS:
        assert jobs[name]["if"] == (
            "needs.changes.outputs.code == 'true' && "
            "github.event_name != 'pull_request'"
        ), name
    for name, gate in PATH_GATED_JOBS.items():
        assert jobs[name]["if"] == (
            "needs.changes.outputs.code == 'true' && "
            "(github.event_name != 'pull_request' || "
            f"needs.changes.outputs.{gate} == 'true')"
        ), name
    assert "scanner-quality" not in jobs


def test_publication_waits_for_the_light_gate_call_without_deleted_job_names() -> None:
    jobs = _jobs()
    needs = _needs(jobs["engine-main-wheel"])
    assert {"light-gates", "changes"} <= needs
    assert MAIN_ONLY_JOBS <= needs
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


def test_auto_merge_is_the_shared_pipelines_caller() -> None:
    source = (WORKFLOW.parent / "auto_merge.yml").read_text(encoding="utf-8")
    workflow = yaml.safe_load(source)
    events = workflow.get("on", workflow.get(True))
    assert set(events) == {"check_run"}
    assert "actions/checkout@" not in source
    assert workflow["permissions"] == {}
    job = workflow["jobs"]["auto-merge"]
    assert job["permissions"]["contents"] == "write"
    assert job["permissions"]["pull-requests"] == "write"
    assert job["uses"].endswith("/auto_merge_pipeline.yml@main")
    assert job["secrets"] == {"merge-token": "${{ secrets.GIT_TOKEN }}"}
