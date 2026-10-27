"""Topology contract for the central reusable lightweight-gates caller."""

from __future__ import annotations

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

REQUIRED_BLOCKING_JOBS = [
    "changes",
    "security",
    "gates",
    "gates-contract",
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
]
NON_BLOCKING_JOBS = {
    "scanner-quality",
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


def test_changes_is_the_single_central_light_gate_call() -> None:
    jobs = _jobs()
    assert jobs["changes"] == {"uses": CENTRAL, "with": {"runner": RUNNER}}
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
        "scanner-quality",
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
            "owner-format-repair",
        }


def test_publication_waits_for_the_light_gate_call_without_deleted_job_names() -> None:
    jobs = _jobs()
    needs = _needs(jobs["engine-main-wheel"])
    assert "changes" in needs
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
    assert '"$job" == gates-variants' in run
    assert '"$EVENT_NAME" == pull_request' in run
    assert "expected skipped for documentation-only scope" in run
    assert "failures=()" in run
    assert "exit 1" in run


def test_owner_format_repair_is_failure_only_and_cannot_publish() -> None:
    jobs = _jobs()
    repair = jobs["owner-format-repair"]
    condition = repair["if"]
    assert "!cancelled()" in condition
    assert "github.event_name == 'pull_request'" in condition
    assert "needs.changes.result == 'failure'" in condition
    assert "needs.changes.outputs" not in condition
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
