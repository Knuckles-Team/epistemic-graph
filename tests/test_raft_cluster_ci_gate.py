"""EG-DURABLE-KERNEL-R008: raft unit tests pass under cluster features and run
in CI.

Collected by the existing python-suite job. This checks parses the release
workflow configuration; it does not replace the native raft suite's own
compilation and test evidence, which the `gates-variants` job in
`.github/workflows/release.yml` provides on every push to `main` and every
pull request.
"""

from __future__ import annotations

import shlex
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def release_workflow() -> dict:
    return yaml.safe_load((ROOT / ".github/workflows/release.yml").read_text())


def _run_tokens(step: dict) -> list[str]:
    return shlex.split(step.get("run", ""), comments=True)


@pytest.mark.spec("EG-DURABLE-KERNEL-R008")
def test_ci_runs_on_every_push_and_pull_request(release_workflow: dict) -> None:
    triggers = release_workflow[True]  # YAML 1.1 parses the bare `on:` key as boolean True
    assert "push" in triggers and "main" in triggers["push"]["branches"]
    assert "pull_request" in triggers


@pytest.mark.spec("EG-DURABLE-KERNEL-R008")
def test_gates_variants_job_builds_and_tests_raft_under_cluster_features(
    release_workflow: dict,
) -> None:
    steps = release_workflow["jobs"]["gates-variants"]["steps"]

    raft_lib_step = next(
        step
        for step in steps
        if "raft::" in step.get("run", "") and "--lib" in _run_tokens(step)
    )
    tokens = _run_tokens(raft_lib_step)
    assert "cargo" in tokens and "test" in tokens
    features_idx = tokens.index("--features")
    features = set(tokens[features_idx + 1].split(","))
    assert {"cluster", "harness", "calvin"} <= features


@pytest.mark.spec("EG-DURABLE-KERNEL-R008")
def test_gates_variants_job_runs_txn_reconcile_ack_lost_retry(
    release_workflow: dict,
) -> None:
    steps = release_workflow["jobs"]["gates-variants"]["steps"]

    retry_step = next(
        step
        for step in steps
        if "txn_reconcile_ack_lost_retry" in step.get("run", "")
    )
    tokens = _run_tokens(retry_step)
    assert "cargo" in tokens and "test" in tokens
    assert "--test" in tokens
    test_idx = tokens.index("--test")
    assert tokens[test_idx + 1] == "txn_reconcile_ack_lost_retry"
    features_idx = tokens.index("--features")
    features = set(tokens[features_idx + 1].split(","))
    assert {"cluster", "harness", "calvin"} <= features
