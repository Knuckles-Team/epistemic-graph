"""Static proofs for landed MEDULLA-CONTRACT rows whose acceptance is a configuration fact.

Collected by the python-suite job. Each test parses the committed artifact the
row names; the native gates keep their own compilation evidence.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import tomllib
import yaml

pytestmark = pytest.mark.no_engine

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.spec("MEDULLA-CONTRACT-R024")
def test_precommit_and_codespell_config_are_locked_and_consistent() -> None:
    config = yaml.safe_load((ROOT / ".config/pre-commit.yaml").read_text())
    hooks = [hook for repo in config["repos"] for hook in repo.get("hooks", [])]
    codespell = next(hook for hook in hooks if hook["id"] == "codespell")
    ignore_args = [a for a in codespell["args"] if a.startswith("--ignore-words=")]
    assert ignore_args, "codespell must consume the committed ignore list"
    ignore_path = ROOT / ignore_args[0].split("=", 1)[1]
    assert ignore_path == ROOT / ".config/codespell.ignore"
    assert ignore_path.is_file()
    # Every repo-local config the hooks name must exist, so no gate points at a
    # file that was renamed or dropped during the config merge.
    referenced = set()
    for hook in hooks:
        text = " ".join([str(hook.get("entry", "")), *map(str, hook.get("args", []))])
        referenced.update(
            re.findall(
                r"\.config/[A-Za-z0-9_./-]+\.(?:toml|ini|ignore|yaml|yml|json|txt)",
                text,
            )
        )
    assert referenced, "pre-commit config should reference .config files"
    missing = sorted(p for p in referenced if not (ROOT / p).exists())
    assert missing == []


@pytest.mark.spec("MEDULLA-CONTRACT-R030")
def test_constrained_gate_build_step_is_not_cpu_restricted() -> None:
    lines = (ROOT / "scripts/constrained_parallelism_gate.sh").read_text().splitlines()
    code = [ln for ln in lines if not ln.lstrip().startswith("#")]
    build = [ln for ln in code if "--no-run" in ln and "cargo test" in ln]
    run = [ln for ln in code if "taskset -c" in ln and "cargo test" in ln]
    assert build, "the unconstrained build step must exist"
    assert run, "the constrained run step must exist"
    assert all("taskset" not in ln for ln in build)
    assert all("--no-run" not in ln for ln in run)
    # The constrained step keeps its own bound; it is not relaxed to compensate.
    assert any("bounded_test" in ln for ln in run)


@pytest.mark.spec("MEDULLA-CONTRACT-R033")
def test_wire_method_features_move_in_lockstep_with_the_facade() -> None:
    caps = tomllib.loads((ROOT / "crates/medulla-capabilities/Cargo.toml").read_text())
    root = tomllib.loads((ROOT / "Cargo.toml").read_text())["features"]
    mirrored = {
        name: values
        for name, values in caps["features"].items()
        if values == [f"medulla-types/{name}"]
    }
    assert mirrored, "medulla-capabilities must mirror Method-gating medulla-types features"
    for name in mirrored:
        facades = [
            key
            for key, values in root.items()
            if f"medulla-capabilities/{name}" in {v.replace("?", "") for v in values}
        ]
        assert facades, f"no facade feature enables medulla-capabilities/{name}"
        for key in facades:
            enabled = {v.replace("?", "") for v in root[key]}
            assert f"medulla-types/{name}" in enabled, (key, name)


@pytest.mark.spec("MEDULLA-CONTRACT-R039")
def test_duplication_gate_compares_the_combined_diff_to_a_base_ref() -> None:
    config = yaml.safe_load((ROOT / ".config/pre-commit.yaml").read_text())
    hook = next(
        hook
        for repo in config["repos"]
        for hook in repo.get("hooks", [])
        if hook["id"] == "jscpd-differential"
    )
    entry = hook["entry"]
    assert "check_duplication.py enforce" in entry
    assert "--base-ref" in entry
    assert "origin/main" in entry or "gate_base_ref" in entry
