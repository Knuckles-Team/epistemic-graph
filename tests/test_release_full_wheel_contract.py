"""Release wheels must all carry the full engine and folded numeric kernel."""

from __future__ import annotations

import hashlib
import io
import re
import subprocess
import tarfile
from pathlib import Path

import pytest
import tomllib
import yaml
from check_sdist_completeness import required_sources, validate

pytestmark = pytest.mark.no_engine

REPO = Path(__file__).resolve().parents[1]
WORKFLOW = REPO / ".github" / "workflows" / "release.yml"
CI_WORKFLOW = REPO / ".github" / "workflows" / "ci.yml"
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
    end = raw.index("\n  build-x86:\n", start)
    return raw[start:end]


def test_maturin_default_and_python_extra_are_full() -> None:
    project = tomllib.loads((REPO / "pyproject.toml").read_text(encoding="utf-8"))
    maturin = project["tool"]["maturin"]
    # The public wheel is the eg-pyengine extension crate, not the server bin.
    assert maturin["features"] == ["python", "security"]
    assert maturin["manifest-path"] == "crates/eg-pyengine/Cargo.toml"
    assert maturin["module-name"] == "epistemic_graph.engine"
    assert maturin.get("bindings") != "bin"
    dependencies = project["project"]["dependencies"]
    assert not any(
        re.split(r"[\[<>=!~;]", dependency, maxsplit=1)[0].lower() == "numpy"
        for dependency in dependencies
    )
    assert "pyoxigraph>=0.5.11" in dependencies
    assert "httpx>=0.28.1" in dependencies
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
    """Every authored skill has one canonical identity in the engine provider."""

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
    assert skill_md_files, "the engine provider must not publish an empty catalog"
    identities: set[str] = set()
    for path in skill_md_files:
        assert path.resolve().parent.parent == skills.resolve(), (
            f"skill must have one canonical directory in the engine package: {path}"
        )
        document = path.read_text(encoding="utf-8")
        assert document.startswith("---\n"), f"missing skill frontmatter: {path}"
        frontmatter = yaml.safe_load(document.split("---", 2)[1])
        assert isinstance(frontmatter, dict), f"invalid skill frontmatter: {path}"
        identity = frontmatter.get("name")
        assert identity == path.parent.name, f"skill identity/path mismatch: {path}"
        assert identity.casefold() not in identities, f"duplicate skill: {identity}"
        identities.add(identity.casefold())
    assert "epistemic-graph-development" in identities, (
        "the development skill must ship from the engine provider, not a sibling"
    )


def _verified_x86_workflow() -> dict:
    """Bind the local structural proof to the reviewed immutable workflow."""
    caller = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    expected = (
        "Knuckles-Team/epistemic-graph/.github/workflows/eg-release-x86.yml@"
        "35fe8e492acedce6c31c399e98a8c62c26ee2cd3"
    )
    assert caller["jobs"]["build-x86"]["uses"] == expected
    source = (WORKFLOW.parent / "eg-release-x86.yml").read_bytes()
    assert hashlib.sha256(source).hexdigest() == (
        "28d6e43039e000b1aaf16bc4677c2cd598e10209d0659c7be512573c01ce4e17"
    )
    return yaml.safe_load(source)


def test_every_supported_release_target_uses_one_full_wheel_pipeline() -> None:
    raw = WORKFLOW.read_text(encoding="utf-8")
    workflow = yaml.safe_load(raw)
    matrix = workflow["jobs"]["build"]["strategy"]["matrix"]["include"]
    targets = {entry["name"]: entry["target"] for entry in matrix}
    assert len(matrix) == len(targets) == 3
    arm = next(entry for entry in matrix if entry["name"] == "linux-aarch64")
    assert arm["runner"] == "ubuntu-24.04-arm"
    assert arm["jobs"] == 1
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
        "windows-x86_64": "x86_64-pc-windows-msvc",
    }
    assert "MATURIN_FEATURES" not in raw
    assert "--no-default-features" not in _build_job_source(raw)
    both = raw + CI_WORKFLOW.read_text(encoding="utf-8")
    assert "scripts/inject_numeric_kernel.py" in both
    assert "scripts/normalize_wheel_build_paths.py" in both
    assert "import epistemic_graph.numeric" in both
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
    assert "uses: ./.github/actions/pinned-rust-toolchain" in build_job_raw
    pinned = (REPO / ".github/actions/pinned-rust-toolchain/action.yml").read_text(
        encoding="utf-8"
    )
    assert "tomllib.load(open('rust-toolchain.toml','rb'))" in pinned
    assert "toolchain: ${{ steps.pin.outputs.channel }}" in pinned
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
    # One pass (.github/actions/folded-wheel) builds the engine extension wheel
    # and the numeric module into one shared target directory, folds the numeric
    # module in, then normalizes, audits and checks the result. No server crate.
    wheel_pass = WHEEL_PASS_ACTION.read_text(encoding="utf-8")
    for output in ("dist", "numdist"):
        assert f"--out {output}-${{{{ inputs.pass }}}}" in wheel_pass
    assert "enginedist" not in wheel_pass
    assert "full,ast-extended" not in wheel_pass
    assert wheel_pass.count("uses: ./.github/actions/release-maturin") == 2
    shared = yaml.safe_load(
        (REPO / ".github/actions/release-maturin/action.yml").read_text()
    )["runs"]["steps"]
    assert len(shared) == 1
    assert shared[0]["uses"] == (
        "PyO3/maturin-action@e83996d129638aa358a18fbd1dfb82f0b0fb5d3b"
    )
    assert shared[0]["with"]["sccache"] == "false"
    assert (
        "CARGO_TARGET_DIR=$RUNNER_TEMP/epistemic-graph-release-target"
    ) in wheel_pass
    for script in (
        "scripts/inject_numeric_kernel.py",
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
    both = raw + CI_WORKFLOW.read_text(encoding="utf-8")
    assert both.count("scripts/inject_numeric_kernel.py") == 1
    assert both.count("scripts/normalize_wheel_sbom.py") == 2
    assert both.count("scripts/normalize_wheel_build_paths.py") == 2
    assert both.count("scripts/check_wheel_privacy.py") == 2
    assert "Remove primary native build state" in raw
    assert "release wheel digest mismatch" in both


def test_incomplete_fallback_artifacts_cannot_be_published() -> None:
    raw = WORKFLOW.read_text(encoding="utf-8")
    assert "command: sdist" not in raw
    jobs = yaml.safe_load(raw)["jobs"]
    assert "publish-pypi" not in jobs
    publish = jobs["publish-x86"]
    assert publish["needs"] == "build-x86"
    assert publish["permissions"] == {"contents": "read"}
    steps = publish["steps"]
    gate = next(
        step["run"]
        for step in steps
        if step.get("name") == "Require a complete wheel before upload"
    )
    assert "check_wheel_completeness.py --require-engine-kernel dist/*.whl" in gate
    names = [step.get("name", "") for step in steps]
    assert names.index("Require a complete wheel before upload") < names.index(
        "Publish linux-x86_64 to PyPI"
    )
    assert set(jobs["verify-pypi"]["needs"]) == {
        "build",
        "publish-x86",
        "build-sdist",
        "build-server",
    }
    source_steps = jobs["build-sdist"]["steps"]
    assert any(
        "check_sdist_completeness.py" in step.get("run", "") for step in source_steps
    )
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
            _assert_workflow_job_budget(workflow, name, job)


def _assert_runner_budget(workflow: Path, name: str, job: dict) -> None:
    if "uses" in job:  # reusable-workflow call: GitHub rejects a timeout here
        return
    if (workflow.name, name) == ("eg-release-x86.yml", "authorize"):
        assert job == _verified_x86_workflow()["jobs"]["authorize"]
        assert job["runs-on"] == "ubuntu-latest"
        assert job.get("timeout-minutes") == 5
    elif (workflow.name, name) == ("arm64-linker-probe.yml", "probe"):
        # This compiles tiny fixtures, never the full release workspace.
        assert job["runs-on"] == "ubuntu-24.04-arm"
        assert job.get("timeout-minutes") == 10
    elif (workflow.name, name) == ("macos-release-probe.yml", "probe"):
        assert job["runs-on"] == "${{ matrix.runner }}"
        assert job.get("timeout-minutes") == 15
        assert {row["runner"] for row in job["strategy"]["matrix"]["include"]} == {
            "macos-14",
        }
    else:
        # Only full-engine jobs retain the longer PR compilation budget.
        # Classification, lint, security and independently split jobs stay short.
        # python-server/python-suite build/run the full engine suite and exceed
        # the short budget on cold hosted PR runners; gates-crates' per-crate
        # narrowing falls back to the FULL workspace on a broad/unmapped diff
        # (release version bumps), which cold-builds past the short budget too
        # (release PR #666 cancelled at ~10:15 on 3/3 runs).
        if workflow.name in {"ci.yml", "release.yml"} and name in {
            "gates",
            "gates-wheel",
            "gates-facade",
            "gates-whisper",
            "gates-plan-fuzz",
            "python-server",
            "python-suite",
            "gates-pyo3",
            "gates-crates",
        }:
            expected = "${{ github.event_name == 'pull_request' && 45 || 60 }}"
            assert job.get("timeout-minutes") == expected
        else:
            pr_scoped = "${{ github.event_name == 'pull_request' && 10 || 60 }}"
            assert job.get("timeout-minutes") in (360, 5, pr_scoped), (
                f"{workflow.name}:{name}"
            )


def _assert_workflow_job_budget(workflow: Path, name: str, job: dict) -> None:
    if (workflow.name, name) == ("release-publication-recovery.yml", "recover"):
        # This verifies and publishes existing wheels; it never builds them.
        assert job["runs-on"] == "ubuntu-latest"
        assert job.get("timeout-minutes") == 30
        assert "strategy" not in job
        assert job["env"]["SOURCE_COMMIT"] == (
            "7f5179650d87e6f763b129cfeea6dfb4f8b0273b"
        )
        assert any(
            step.get("run") == "python scripts/release_publication_recovery.py prepare"
            for step in job["steps"]
        )
    else:
        _assert_runner_budget(workflow, name, job)


def test_release_concurrency_never_cancels_a_publication() -> None:
    concurrency = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))["concurrency"]
    assert concurrency["group"] == "release-${{ github.ref }}"
    # Keep GitHub's bounded default: one running plus the latest pending run.
    assert concurrency.get("queue", "single") == "single"
    assert concurrency["cancel-in-progress"] is False


def test_ci_concurrency_supersedes_older_runs_per_pr_and_per_ref() -> None:
    """spec EG-CONTRACT-R053.7: main pushes and PR pushes cancel the older run."""
    doc = yaml.safe_load(CI_WORKFLOW.read_text(encoding="utf-8"))
    concurrency = doc["concurrency"]
    assert concurrency["cancel-in-progress"] is True
    group = concurrency["group"]
    assert "github.workflow" in group
    assert "github.event.pull_request.number" in group
    assert "github.ref" in group


def _source_archive(
    tmp_path, *, omitted="", version="2.27.1", unsafe=False, extra_members=()
):
    path = tmp_path / "epistemic_graph-2.27.1.tar.gz"
    entries = {
        "Cargo.toml": '[package]\nversion = "2.27.1"\n',
        "PKG-INFO": f"Name: epistemic-graph\nVersion: {version}\n",
        "crates/leaf/src/lib.rs": "pub fn leaf() {}",
        "build.rs": "fn main() {}",
        "crates/leaf/assets/viz_interactive_client.html": "<main>client</main>",
    }
    if unsafe:
        entries["../escape"] = "outside"
    with tarfile.open(path, "w:gz") as output:
        for name, text in entries.items():
            if name == omitted:
                continue
            data = text.encode()
            item = tarfile.TarInfo(f"epistemic_graph-2.27.1/{name}")
            item.size = len(data)
            output.addfile(item, io.BytesIO(data))
        for name, text, kind in extra_members:
            item = tarfile.TarInfo(f"epistemic_graph-2.27.1/{name}")
            item.type = kind
            data = text.encode()
            item.size = len(data) if item.isfile() else 0
            output.addfile(item, io.BytesIO(data) if item.isfile() else None)
    return path


def test_accepts_complete_sources(tmp_path):
    validate(
        _source_archive(tmp_path), {"Cargo.toml", "crates/leaf/src/lib.rs"}, "2.27.1"
    )


@pytest.mark.parametrize(
    "kwargs,match",
    [
        ({"omitted": "crates/leaf/src/lib.rs"}, "missing source"),
        ({"version": "2.27.0"}, "metadata identity"),
        ({"unsafe": True}, "unsafe"),
    ],
)
def test_rejects_incomplete_or_misidentified_sources(tmp_path, kwargs, match):
    with pytest.raises(ValueError, match=match):
        validate(
            _source_archive(tmp_path, **kwargs),
            {"Cargo.toml", "crates/leaf/src/lib.rs"},
            "2.27.1",
        )


@pytest.mark.parametrize("shipped", [False, True])
def test_sdist_declared_license_file_must_ship(tmp_path, shipped):
    # PyPI answers 400 to an sdist whose PKG-INFO `License-File` is absent.
    path = tmp_path / "epistemic_graph-2.27.1.tar.gz"
    entries = {
        "Cargo.toml": '[package]\nversion = "2.27.1"\n',
        "PKG-INFO": "Name: epistemic-graph\nVersion: 2.27.1\nLicense-File: LICENSE\n",
    }
    if shipped:
        entries["LICENSE"] = "MIT"
    with tarfile.open(path, "w:gz") as output:
        for name, text in entries.items():
            item = tarfile.TarInfo(f"epistemic_graph-2.27.1/{name}")
            item.size = len(text.encode())
            output.addfile(item, io.BytesIO(text.encode()))
    if shipped:
        validate(path, {"Cargo.toml"}, "2.27.1")
    else:
        with pytest.raises(ValueError, match="License-File"):
            validate(path, {"Cargo.toml"}, "2.27.1")


@pytest.mark.parametrize(
    "alias",
    [
        "./Cargo.toml",
        "nested/../Cargo.toml",
        "nested//Cargo.toml",
        "nested/./Cargo.toml",
        "nested\\Cargo.toml",
        "/Cargo.toml",
        "",
    ],
)
def test_sdist_rejects_noncanonical_member_aliases(tmp_path, alias):
    archive = _source_archive(
        tmp_path,
        extra_members=[
            (alias, '[package]\nversion = "0.0.0"\n', tarfile.REGTYPE),
        ],
    )
    with pytest.raises(ValueError, match="noncanonical"):
        validate(archive, {"Cargo.toml"}, "2.27.1")


@pytest.mark.parametrize(
    "extras",
    [
        [("Cargo.toml", "", tarfile.DIRTYPE)],
        [("folder", "", tarfile.DIRTYPE), ("folder", "", tarfile.DIRTYPE)],
        [("folder", "", tarfile.DIRTYPE), ("folder", "file", tarfile.REGTYPE)],
        [
            ("folder", "file", tarfile.REGTYPE),
            ("folder/child", "child", tarfile.REGTYPE),
        ],
        [
            ("folder/child", "child", tarfile.REGTYPE),
            ("folder", "file", tarfile.REGTYPE),
        ],
    ],
)
def test_sdist_rejects_directory_identity_collisions(tmp_path, extras):
    with pytest.raises(ValueError, match="duplicate|collision"):
        validate(
            _source_archive(tmp_path, extra_members=extras), {"Cargo.toml"}, "2.27.1"
        )


def test_sdist_accepts_canonical_directory_entries(tmp_path):
    archive = _source_archive(
        tmp_path,
        extra_members=[
            ("", "", tarfile.DIRTYPE),
            ("folder", "", tarfile.DIRTYPE),
            ("folder/source.rs", "pub fn source() {}", tarfile.REGTYPE),
        ],
    )
    validate(archive, {"Cargo.toml", "folder/source.rs"}, "2.27.1")


def test_sdist_requires_root_build_script(tmp_path):
    # A root crate that maturin builds ships its build script ...
    (tmp_path / "Cargo.toml").write_text(
        '[package]\nname = "fixture"\nversion = "1.0.0"\n'
    )
    (tmp_path / "build.rs").write_text("fn main() {}")
    assert "build.rs" in required_sources(tmp_path)
    with pytest.raises(ValueError, match="build.rs"):
        validate(_source_archive(tmp_path, omitted="build.rs"), {"build.rs"}, "2.27.1")


def test_extensions_only_sdist_inputs_are_the_real_build_inputs():
    # maturin builds crates/eg-pyengine, so the root crate is not an sdist input,
    # but the in-tree PEP 517 backend and the helpers it imports are.
    required = required_sources(REPO)
    assert not {"build.rs", "src/lib.rs", "src/main.rs"} & required
    assert not any(name.startswith(("benches/", ".cargo/")) for name in required)
    assert {
        "build_backend.py",
        "scripts/inject_numeric_kernel.py",
        "scripts/strip_wheel_executables.py",
        "crates/eg-pyengine/Cargo.toml",
    } <= required


def test_server_wheel_build_adds_swap_and_thin_links_aarch64():
    # Swap and the aarch64 link profile live in the shared action, so the release
    # leg and ci.yml's warm-cache build both get them (the cache job had no swap
    # and was OOM-killed on main).
    action = REPO / ".github/actions/build-server-wheel/action.yml"
    steps = yaml.safe_load(action.read_text(encoding="utf-8"))["runs"]["steps"]
    names = [step.get("name", "") for step in steps]
    assert names.index("Add swap for the server link") < names.index(
        "Build the wheel in the manylinux_2_28 container"
    )
    build = steps[names.index("Build the wheel in the manylinux_2_28 container")]["run"]
    assert 'image="$ARM_IMAGE"; jobs=1; lto=thin; cgu=16' in build
    assert '"CARGO_PROFILE_RELEASE_SERVER_LTO=$lto"' in build
    release = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))["jobs"][
        "build-server"
    ]
    uses = [step.get("uses", "") for step in release["steps"]]
    assert "./.github/actions/build-server-wheel" in uses


def test_sdist_inventory_includes_explicit_manifest_inputs(tmp_path):
    manifest = tmp_path / "Cargo.toml"
    manifest.write_text("""[package]
name = "fixture"
version = "1.0.0"
build = "tools/prepare.input"
readme = "PACKAGE.txt"
license-file = "LICENSE.txt"
[lib]
path = "library/entry.input"
[[bin]]
name = "server"
path = "programs/server.input"
[[example]]
name = "example"
path = "examples/entry.input"
[[bench]]
name = "bench"
path = "benches/entry.input"
[[test]]
name = "test"
path = "integration/entry.input"
""")
    leaf = tmp_path / "crates/leaf"
    leaf.mkdir(parents=True)
    (leaf / "Cargo.toml").write_text('[package]\nname = "leaf"\nversion = "1.0.0"\n')
    (leaf / "build.rs").write_text("fn main() {}")
    expected = {
        "build.rs",
        "tools/prepare.input",
        "PACKAGE.txt",
        "LICENSE.txt",
        "library/entry.input",
        "programs/server.input",
        "examples/entry.input",
        "benches/entry.input",
        "integration/entry.input",
        "crates/leaf/build.rs",
    }
    assert expected <= required_sources(tmp_path)


def test_sdist_requires_compiled_html_asset(tmp_path):
    asset = "crates/leaf/assets/viz_interactive_client.html"
    leaf = tmp_path / "crates/leaf/assets"
    leaf.mkdir(parents=True)
    (leaf / "viz_interactive_client.html").write_text("<main>client</main>")
    (tmp_path / "crates/leaf/Cargo.toml").write_text(
        '[package]\nname = "leaf"\nversion = "1.0.0"\n'
    )
    (tmp_path / "Cargo.toml").write_text("[workspace]\n")
    assert asset in required_sources(tmp_path)
    validate(_source_archive(tmp_path), {asset}, "2.27.1")
    with pytest.raises(ValueError, match="viz_interactive_client.html"):
        validate(_source_archive(tmp_path, omitted=asset), {asset}, "2.27.1")


@pytest.mark.parametrize("asset", ["client.html", "shader.wgsl", "lookup.bin"])
def test_sdist_inventory_has_no_native_asset_suffix_filter(tmp_path, asset):
    (tmp_path / "Cargo.toml").write_text(
        '[package]\nname = "fixture"\nversion = "1.0.0"\n'
    )
    directory = tmp_path / "src"
    directory.mkdir()
    (directory / asset).write_bytes(b"build input")
    assert f"src/{asset}" in required_sources(tmp_path)


def test_sdist_requires_engine_composition_helper(tmp_path):
    required = required_sources(REPO)
    assert "scripts/inject_numeric_kernel.py" in required
    assert "scripts/inject_pyengine.py" not in required
    with pytest.raises(ValueError, match="inject_numeric_kernel.py"):
        validate(
            _source_archive(tmp_path),
            {"scripts/inject_numeric_kernel.py"},
            "2.27.1",
        )


@pytest.mark.spec("EG-CONTRACT-R053.7")
def test_tag_pipeline_builds_checks_and_publishes_only() -> None:
    """A tag publishes only a commit main CI already passed (spec R053.7)."""
    doc = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    triggers = doc[True] if True in doc else doc["on"]
    assert set(triggers) == {"push", "workflow_dispatch"}
    assert triggers["push"] == {"tags": ["v*"]}
    assert triggers["workflow_dispatch"]["inputs"]["force_untested"]["default"] is False
    jobs = doc["jobs"]
    assert set(jobs) == {
        "guard",
        "build",
        "build-x86",
        "build-sdist",
        "publish-x86",
        "build-server",
        "server-image",
        "server-manifest",
        "verify-pypi",
        "github-release",
    }
    for name in ("build", "build-x86", "build-sdist", "build-server"):
        assert jobs[name]["needs"] == "guard"
    guard = jobs["guard"]
    assert guard["permissions"]["actions"] == "read"
    script = "\n".join(step.get("run", "") for step in guard["steps"])
    assert "actions/workflows/ci.yml/runs?head_sha=${SHA}&status=success" in script
    assert "force_untested" in str(guard["steps"])
    assert "REFUSING to publish" in script
    # Every gate, feature build and benchmark lives in ci.yml.
    ci = yaml.safe_load(CI_WORKFLOW.read_text(encoding="utf-8"))
    ci_triggers = ci[True] if True in ci else ci["on"]
    assert set(ci_triggers) == {"push", "pull_request", "workflow_dispatch"}
    assert ci_triggers["push"] == {"branches": ["main"]}
    assert {"gates", "feature-matrix", "benchmarks", "engine-main-wheel"} <= set(
        ci["jobs"]
    )
    assert not {"build", "build-x86", "build-sdist", "publish-pypi"} & set(ci["jobs"])
