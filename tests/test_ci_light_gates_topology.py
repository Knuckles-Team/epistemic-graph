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
        "backward-python",
        "python-suite",
        "tts-piper-inference",
        "language-clients",
        "quality-advisory",
        "scanner-quality",
        "engine-main-wheel",
    }
    actual = {name for name, job in jobs.items() if "changes" in _needs(job)}
    assert actual == expected
    for name in expected:
        assert "needs.changes.outputs.code" in str(
            jobs[name].get("if", "")
        ) or name in {
            "security",
            "engine-main-wheel",
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
