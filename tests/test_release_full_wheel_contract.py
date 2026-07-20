"""Release wheels must all carry the full engine and folded numeric kernel."""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import tomllib
import yaml

pytestmark = pytest.mark.no_engine

REPO = Path(__file__).resolve().parents[1]
WORKFLOW = REPO / ".github" / "workflows" / "release-build.yml"


def test_maturin_default_and_python_extra_are_full() -> None:
    project = tomllib.loads((REPO / "pyproject.toml").read_text(encoding="utf-8"))
    assert project["tool"]["maturin"]["features"] == ["full", "ast-extended"]
    assert project["project"]["optional-dependencies"]["full"] == [
        "epistemic-graph[owl,lmcache,numeric]"
    ]


def test_agent_skills_have_one_canonical_owner() -> None:
    """The engine must not republish skills consolidated in Agent Utilities."""

    project = tomllib.loads((REPO / "pyproject.toml").read_text(encoding="utf-8"))
    entry_points = project["project"].get("entry-points", {})
    assert "agent_utilities.skill_providers" not in entry_points
    skills = REPO / "epistemic_graph" / "skills"
    assert not (skills / "__init__.py").exists()
    assert not list(skills.rglob("SKILL.md"))


def test_every_supported_release_target_uses_one_full_wheel_pipeline() -> None:
    raw = WORKFLOW.read_text(encoding="utf-8")
    workflow = yaml.safe_load(raw)
    matrix = workflow["jobs"]["wheels"]["strategy"]["matrix"]["include"]
    targets = {entry["name"]: entry["target"] for entry in matrix}
    assert targets == {
        "linux-aarch64": "aarch64-unknown-linux-gnu",
        "linux-x86_64": "x86_64-unknown-linux-gnu",
        "macos-aarch64": "aarch64-apple-darwin",
        "macos-x86_64": "x86_64-apple-darwin",
        "windows-x86_64": "x86_64-pc-windows-msvc",
    }
    assert 'MATURIN_FEATURES: "full,ast-extended"' in raw
    assert "--no-default-features" not in raw
    assert "scripts/inject_numeric_kernel.py" in raw
    assert "scripts/normalize_wheel_build_paths.py" in raw
    assert "import epistemic_graph.numeric" in raw


def test_release_wheels_are_rebuilt_and_compared_reproducibly() -> None:
    raw = WORKFLOW.read_text(encoding="utf-8")
    toolchain = tomllib.loads(
        (REPO / "rust-toolchain.toml").read_text(encoding="utf-8")
    )["toolchain"]["channel"]

    assert re.fullmatch(r"[0-9]+[.][0-9]+[.][0-9]+", toolchain)
    assert "toolchain: ${{ steps.rust-toolchain.outputs.channel }}" in raw
    assert "toolchain: stable" not in raw
    assert 'CARGO_BUILD_JOBS: "1"' in raw
    assert 'CARGO_INCREMENTAL: "0"' in raw
    assert "max-parallel: 1" in raw
    assert "SOURCE_DATE_EPOCH=" in raw
    for output in (
        "dist-primary",
        "numdist-primary",
        "dist-reproduction",
        "numdist-reproduction",
    ):
        assert f"--out {output}" in raw
    assert raw.count("scripts/inject_numeric_kernel.py") == 2
    assert raw.count("scripts/normalize_wheel_sbom.py") == 2
    assert raw.count("scripts/normalize_wheel_build_paths.py") == 2
    assert raw.count("scripts/check_wheel_privacy.py") == 2
    assert raw.count("sccache: 'false'") == 4
    assert (
        raw.count("CARGO_TARGET_DIR: ${{ runner.temp }}/epistemic-graph-release-target")
        == 4
    )
    assert "Remove primary native build state" in raw
    assert "release wheel digest mismatch" in raw


def test_incomplete_fallback_artifacts_cannot_be_published() -> None:
    raw = WORKFLOW.read_text(encoding="utf-8")
    assert "command: sdist" not in raw
    assert "dist/*.tar.gz" not in raw
    assert not (REPO / ".github" / "workflows" / "pipeline.yml").exists()
