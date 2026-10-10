"""EG-DURABLE-KERNEL-R010: the jscpd clone gate runs on every commit.

Collected by the existing python-suite job. These checks parse configuration;
they do not replace the gate's own structural-clone detection, which
`scripts/check_duplication.py` (exercised separately by
`tests/test_check_duplication_no_suppression.py` for its no-suppression
contract) provides.

Two wiring facts prove "runs on every commit":

1. The local pre-commit configuration declares the jscpd-differential hook,
   so a developer's committed tree is checked before it reaches a PR.
2. The hosted `scanner-quality` job in `.github/workflows/release.yml` runs
   `check_duplication.py enforce` on every push to `main` and every pull
   request -- the actual per-commit backstop, since the local hook's
   `stages: [manual]` means it is not auto-invoked by a bare `git commit`.
"""

from __future__ import annotations

import shlex
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.spec("EG-DURABLE-KERNEL-R010")
def test_precommit_config_declares_the_jscpd_differential_hook() -> None:
    config = yaml.safe_load((ROOT / ".config/pre-commit.yaml").read_text())
    hook_ids = {
        hook["id"]
        for repo in config["repos"]
        for hook in repo.get("hooks", [])
    }
    assert "jscpd-differential" in hook_ids
    assert "jscpd-census" in hook_ids


@pytest.mark.spec("EG-DURABLE-KERNEL-R010")
def test_ci_runs_jscpd_enforce_on_every_push_and_pull_request() -> None:
    workflow = yaml.safe_load((ROOT / ".github/workflows/release.yml").read_text())
    triggers = workflow[True]  # YAML 1.1 parses the bare `on:` key as boolean True
    assert "push" in triggers and "main" in triggers["push"]["branches"]
    assert "pull_request" in triggers

    steps = workflow["jobs"]["scanner-quality"]["steps"]
    enforce_step = next(
        step
        for step in steps
        if "check_duplication.py" in step.get("run", "")
        and "enforce" in shlex.split(step.get("run", ""), comments=True)
    )
    tokens = shlex.split(enforce_step["run"], comments=True)
    assert "--base-ref" in tokens
