"""Release wheels must all carry the full engine and folded numeric kernel."""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

import pytest
import tomllib
import yaml

pytestmark = pytest.mark.no_engine

REPO = Path(__file__).resolve().parents[1]
WORKFLOW = REPO / ".github" / "workflows" / "release.yml"


def _build_job_source(raw: str) -> str:
    """Isolate the `build` job's YAML text from the rest of `release.yml`.

    `release.yml` now also carries the `gates` job (the former rust-ci.yml
    test suite), which legitimately uses `toolchain: stable` (to run cargo
    test/clippy) and `--no-default-features` (the slim-server build check) —
    neither describes the release wheel matrix. Callers asserting on
    wheel-build-only behavior must check this slice, not the whole file, or
    they'll trip on the unrelated `gates` job content.
    """
    start = raw.index("\n  build:\n")
    end = raw.index("\n  docker-image:\n", start)
    return raw[start:end]


def test_maturin_default_and_python_extra_are_full() -> None:
    project = tomllib.loads((REPO / "pyproject.toml").read_text(encoding="utf-8"))
    assert project["tool"]["maturin"]["features"] == ["full", "ast-extended"]
    dependencies = project["project"]["dependencies"]
    assert not any(
        re.split(r"[\[<>=!~;]", dependency, maxsplit=1)[0].lower() == "numpy"
        for dependency in dependencies
    )
    assert "pyoxigraph>=0.3.22" in dependencies
    assert "httpx>=0.24.0" in dependencies
    optional = project["project"]["optional-dependencies"]
    assert optional["quant"] == []
    assert all(
        "numpy" not in requirement.lower()
        for requirements in optional.values()
        for requirement in requirements
    )
    assert optional["full"] == []
    assert optional["all"] == []
    assert optional["lake-parity"] == []
    lake_requirements = (
        REPO / "tests" / "lake-parity-requirements.txt"
    ).read_text(encoding="utf-8")
    assert "pyiceberg[pyarrow]>=0.7.0" in lake_requirements
    assert "deltalake>=0.18.0" in lake_requirements


def test_agent_skills_have_one_canonical_owner() -> None:
    """The engine wheel owns and publishes its operator skills exactly once."""

    project = tomllib.loads((REPO / "pyproject.toml").read_text(encoding="utf-8"))
    entry_points = project["project"].get("entry-points", {})
    assert entry_points["agent_utilities.skill_providers"] == {
        "epistemic-graph": "epistemic_graph.skills"
    }
    skills = REPO / "epistemic_graph" / "skills"
    assert (skills / "__init__.py").is_file()
    try:
        out = subprocess.run(
            ["git", "-C", str(skills), "ls-files", "--", "SKILL.md", "*/SKILL.md"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout
        skill_md_files = [skills / line for line in out.splitlines() if line]
    except (subprocess.CalledProcessError, FileNotFoundError):
        skill_md_files = []
    if not skill_md_files:
        # BUG-043: prefer the git-tracked set (a raw rglob also picks up
        # gitignored, generated build output); fall back to a filesystem
        # walk only when this checkout is not inside a git working tree.
        skill_md_files = list(skills.rglob("SKILL.md"))
    assert {path.parent.name for path in skill_md_files} == {
        "epistemic-graph-deploy",
        "epistemic-graph-migrations",
        "epistemic-graph-troubleshooting",
        "kg-modality-consensus",
        "kg-modality-reasoning",
        "kg-modality-sparql",
        "kg-modality-sql",
    }


def test_every_supported_release_target_uses_one_full_wheel_pipeline() -> None:
    raw = WORKFLOW.read_text(encoding="utf-8")
    workflow = yaml.safe_load(raw)
    matrix = workflow["jobs"]["build"]["strategy"]["matrix"]["include"]
    targets = {entry["name"]: entry["target"] for entry in matrix}
    assert targets == {
        "linux-aarch64": "aarch64-unknown-linux-gnu",
        "linux-x86_64": "x86_64-unknown-linux-gnu",
        "macos-aarch64": "aarch64-apple-darwin",
        "macos-x86_64": "x86_64-apple-darwin",
        "windows-x86_64": "x86_64-pc-windows-msvc",
    }
    assert 'MATURIN_FEATURES: "full,ast-extended"' in raw
    assert "--no-default-features" not in _build_job_source(raw)
    assert "scripts/inject_numeric_kernel.py" in raw
    assert "scripts/normalize_wheel_build_paths.py" in raw
    assert "import epistemic_graph.numeric" in raw
    # The release smoke test must exercise the native module without importing
    # or installing NumPy; parity keeps its reference dependency isolated in the
    # separate gates job.
    assert "import numpy" not in _build_job_source(raw)


def test_release_wheels_are_rebuilt_and_compared_reproducibly() -> None:
    raw = WORKFLOW.read_text(encoding="utf-8")
    toolchain = tomllib.loads(
        (REPO / "rust-toolchain.toml").read_text(encoding="utf-8")
    )["toolchain"]["channel"]

    assert re.fullmatch(r"[0-9]+[.][0-9]+[.][0-9]+", toolchain)
    build_job_raw = _build_job_source(raw)
    assert "toolchain: ${{ steps.rust-toolchain.outputs.channel }}" in build_job_raw
    assert "toolchain: stable" not in build_job_raw
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
    # These three scripts each run 3x, not 2x: once in the `build` job for
    # each of the primary/reproduction release-wheel passes, PLUS once more
    # in the `gates` job (normalizing/auditing the standalone eg-numeric
    # Surface-A parity wheel built there for the numpy-parity gate). That
    # third call is a legitimate, unrelated wheel and doesn't affect the
    # primary/reproduction reproducibility this test protects — a drop below
    # 3 still means one of those calls went missing.
    assert raw.count("scripts/normalize_wheel_sbom.py") == 3
    assert raw.count("scripts/normalize_wheel_build_paths.py") == 3
    assert raw.count("scripts/check_wheel_privacy.py") == 3
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
    # The two-workflow redesign folded the former release-build.yml (this
    # test's wheel-build workflow, pre-rename) and rust-ci.yml into
    # release.yml/advisory.yml and deleted both. A resurrected
    # release-build.yml would be exactly the kind of unguarded fallback
    # release path this test exists to rule out.
    assert not (REPO / ".github" / "workflows" / "release-build.yml").exists()
    assert not (REPO / ".github" / "workflows" / "rust-ci.yml").exists()
