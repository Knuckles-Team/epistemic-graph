"""EG-DURABLE-KERNEL-R010: the jscpd clone gate runs on every commit.

Collected by the existing python-suite job. These checks parse configuration;
they do not replace the gate's own structural-clone detection, which
`scripts/check_duplication.py` (exercised separately by
`tests/test_check_duplication_no_suppression.py` for its no-suppression
contract) provides.

Two wiring facts prove "runs on every commit":

1. The local pre-commit configuration declares the jscpd-differential hook,
   so a developer's committed tree is checked before it reaches a PR.
2. The manual gate profile (`scripts/ci_parity.sh`, which runs the
   `manual` hook stage) runs `check_duplication.py enforce` against a base
   ref. The hosted advisory scanner job was removed to bound pull-request CI.
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
        hook["id"] for repo in config["repos"] for hook in repo.get("hooks", [])
    }
    assert "jscpd-differential" in hook_ids
    assert "jscpd-census" in hook_ids


@pytest.mark.spec("EG-DURABLE-KERNEL-R010")
def test_manual_gate_profile_runs_jscpd_enforce_against_a_base() -> None:
    config = yaml.safe_load((ROOT / ".config/pre-commit.yaml").read_text())
    hook = next(
        hook
        for repo in config["repos"]
        for hook in repo.get("hooks", [])
        if hook["id"] == "jscpd-differential"
    )
    assert hook["stages"] == ["manual"]
    tokens = shlex.split(hook["entry"], comments=True)
    assert tokens[:2] == ["bash", "-c"]
    inner = shlex.split(tokens[2], comments=True)
    assert "scripts/check_duplication.py" in inner and "enforce" in inner
    assert "--base-ref" in inner
    parity = (ROOT / "scripts/ci_parity.sh").read_text(encoding="utf-8")
    assert "--hook-stage manual" in parity
    # The advisory hosted scanner job was removed (EG-CONTRACT-R053.7).
    workflow = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())
    assert "scanner-quality" not in workflow["jobs"]
