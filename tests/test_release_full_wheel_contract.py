"""Release wheels must all carry the full engine and folded numeric kernel."""

from __future__ import annotations

import hashlib
import re
import subprocess
from pathlib import Path

import pytest
import tomllib
import yaml

pytestmark = pytest.mark.no_engine

REPO = Path(__file__).resolve().parents[1]
WORKFLOW = REPO / ".github" / "workflows" / "release.yml"
WHEEL_PASS_ACTION = REPO / ".github" / "actions" / "folded-wheel" / "action.yml"
NUMERIC_CONTRACT_DOCS = (
    REPO / "AGENTS.md",
    REPO / "README.md",
    REPO / "docs" / "architecture" / "numeric_kernel.md",
    REPO / "docs" / "architecture" / "analytics_program.md",
    REPO / "docs" / "capabilities.md",
    REPO / "docs" / "concepts.md",
)


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
    lake_requirements = (REPO / "tests" / "lake-parity-requirements.txt").read_text(
        encoding="utf-8"
    )
    assert "msgpack>=1.2.1" in lake_requirements
    assert "pyiceberg[pyarrow]>=0.7.0" in lake_requirements
    assert "deltalake>=0.18.0" in lake_requirements


def test_current_numeric_docs_match_the_builtin_boundary_contract() -> None:
    """Current docs must not revive removed NumPy/native-ABI promises."""

    docs = {path: path.read_text(encoding="utf-8") for path in NUMERIC_CONTRACT_DOCS}
    numeric = docs[REPO / "docs" / "architecture" / "numeric_kernel.md"]
    assert "bounded built-in" in numeric
    assert "scalar↔nested-list PyO3 contract" in numeric
    assert "isolated NumPy parity oracle" in numeric
    assert "Arrow `KnowledgeBatch` currency" in numeric
    forbidden_current_claims = (
        "rust-numpy",
        "numpy-shim",
        "kernel-owned numpy tail",
        "numeric interoperability dependencies",
        "zero-copy + allow_threads",
    )
    for path, text in docs.items():
        lowered = text.lower()
        for claim in forbidden_current_claims:
            assert claim not in lowered, f"stale numeric claim in {path}: {claim}"


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


def _verified_x86_workflow() -> dict:
    """Bind the local structural proof to the reviewed immutable workflow."""
    caller = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    expected = (
        "Knuckles-Team/epistemic-graph/.github/workflows/eg-release-x86.yml@"
        "bc4448160e8d16deb8210ce8aeb058b2da293d93"
    )
    assert caller["jobs"]["build-x86"]["uses"] == expected
    source = (WORKFLOW.parent / "eg-release-x86.yml").read_bytes()
    assert hashlib.sha256(source).hexdigest() == (
        "5379e1939b5e4972923ef9961b902fc6d5425a517daf8d23ff22725fd0aa4ee3"
    )
    return yaml.safe_load(source)


def test_every_supported_release_target_uses_one_full_wheel_pipeline() -> None:
    raw = WORKFLOW.read_text(encoding="utf-8")
    workflow = yaml.safe_load(raw)
    matrix = workflow["jobs"]["build"]["strategy"]["matrix"]["include"]
    targets = {entry["name"]: entry["target"] for entry in matrix}
    assert len(matrix) == len(targets) == 4
    x86 = _verified_x86_workflow()["jobs"]["wheel"]
    assert x86["env"]["MATURIN_FEATURES"] == "full,ast-extended"
    upload = next(
        step
        for step in x86["steps"]
        if step.get("uses", "").startswith("actions/upload-artifact@")
    )
    target_name = upload["with"]["name"].removeprefix("wheel-")
    assert target_name == "linux-x86_64" and target_name not in targets
    targets[target_name] = x86["env"]["EG_WHEEL_TARGET"]
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
    # Wheel legs (thin LTO, zig/qemu cross builds) keep a low job count.
    # The byte-identical reproduction pass is a manual (workflow_dispatch) check,
    # so tag releases build each wheel once; the publish candidate is always the
    # primary pass.
    assert "Build reproduction wheel" in build_job_raw
    assert build_job_raw.count("uses: ./.github/actions/folded-wheel") == 2
    assert "pass: primary" in build_job_raw
    assert "pass: reproduction" in build_job_raw
    assert build_job_raw.count("github.event_name == 'workflow_dispatch'") >= 3
    assert "cp dist-primary/epistemic_graph-*.whl dist/" in build_job_raw
    assert 'CARGO_INCREMENTAL: "0"' in raw
    assert "max-parallel: 1" in raw
    assert "SOURCE_DATE_EPOCH=" in raw
    # One pass (.github/actions/folded-wheel) builds the server wheel, the
    # numeric kernel and the pyengine kernel into one shared target directory,
    # folds both kernels in, then normalizes, audits and checks the result.
    wheel_pass = WHEEL_PASS_ACTION.read_text(encoding="utf-8")
    for output in ("dist", "numdist", "enginedist"):
        assert f"--out {output}-${{{{ inputs.pass }}}}" in wheel_pass
    assert wheel_pass.count("sccache: 'false'") == 3
    assert "CARGO_TARGET_DIR=$RUNNER_TEMP/epistemic-graph-release-target" in wheel_pass
    for script in (
        "scripts/inject_numeric_kernel.py",
        "scripts/inject_pyengine.py",
        "scripts/normalize_wheel_sbom.py",
        "scripts/normalize_wheel_build_paths.py",
        "scripts/check_wheel_privacy.py",
        "scripts/check_wheel_completeness.py --require-engine-kernel",
    ):
        assert wheel_pass.count(script) == 1, script
    # The `gates` job folds its OWN numeric kernel into a real wheel
    # (CONCEPT:EG-346) and normalizes/audits its numeric kernel, pyengine
    # kernel and folded wheel, rather than `pip install`ing the standalone
    # `eg-numeric` build (the 2026-08-21 stale-install incident).
    assert raw.count("scripts/inject_numeric_kernel.py") == 1
    assert raw.count("scripts/normalize_wheel_sbom.py") == 3
    assert raw.count("scripts/normalize_wheel_build_paths.py") == 3
    assert raw.count("scripts/check_wheel_privacy.py") == 3
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


def _runner_labels(job: dict) -> list[str]:
    """Every runner label a job can resolve to, across its matrix legs."""

    runs_on = job.get("runs-on", [])
    if isinstance(runs_on, str) and "matrix.runner" in runs_on:
        legs = job["strategy"]["matrix"]["include"]
        values = [leg["runner"] for leg in legs]
    else:
        values = [runs_on]
    labels: list[str] = []
    for value in values:
        labels.extend(value if isinstance(value, list) else [value])
    return labels


def test_self_hosted_runners_are_unreachable_from_pull_requests() -> None:
    """Only a tag push or a manual dispatch in this repository reaches the
    project's own runners; a pull request, including one from a fork, never
    runs code on them."""

    for workflow in sorted((REPO / ".github" / "workflows").glob("*.yml")):
        doc = yaml.safe_load(workflow.read_text(encoding="utf-8"))
        for name, job in doc["jobs"].items():
            if "self-hosted" not in _runner_labels(job):
                continue
            condition = " ".join(str(job.get("if", "")).split())
            assert (
                "github.repository == 'Knuckles-Team/epistemic-graph'" in condition
            ), f"{workflow.name}:{name}"
            assert "pull_request" not in condition, f"{workflow.name}:{name}"
            assert "startsWith(github.ref, 'refs/tags/v')" in condition, (
                f"{workflow.name}:{name}"
            )


def test_runner_jobs_keep_build_budget_and_bounded_authorization() -> None:
    for workflow in sorted((REPO / ".github" / "workflows").glob("*.yml")):
        doc = yaml.safe_load(workflow.read_text(encoding="utf-8"))
        for name, job in doc["jobs"].items():
            if "uses" in job:  # reusable-workflow call: GitHub rejects a timeout here
                continue
            if (workflow.name, name) == ("eg-release-x86.yml", "authorize"):
                assert job == _verified_x86_workflow()["jobs"]["authorize"]
                assert job["runs-on"] == "ubuntu-latest"
                assert job.get("timeout-minutes") == 5
            else:
                assert job.get("timeout-minutes") == 360, f"{workflow.name}:{name}"
