"""The server package builds and publishes here (EG-CONTRACT-R053.9-12)."""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import tomllib
import yaml

pytestmark = pytest.mark.no_engine

ROOT = Path(__file__).resolve().parents[1]
PACKAGE = ROOT / "packaging/epistemic-graph-server"
RELEASE = ROOT / ".github/workflows/release.yml"
CI = ROOT / ".github/workflows/ci.yml"
ACTION = ROOT / ".github/actions/build-server-wheel/action.yml"
DOCKERFILE = ROOT / "docker/Dockerfile"


def _yaml(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _script(job: dict) -> str:
    return "\n".join(str(step.get("run", "")) for step in job["steps"])


def _version() -> str:
    return tomllib.loads((ROOT / "Cargo.toml").read_text())["package"]["version"]


@pytest.mark.spec("EG-CONTRACT-R053.9")
def test_package_wraps_the_root_server_bin_with_the_full_feature_set() -> None:
    project = tomllib.loads((PACKAGE / "pyproject.toml").read_text())
    maturin = project["tool"]["maturin"]
    assert project["project"]["name"] == "epistemic-graph-server"
    assert maturin["bindings"] == "bin"
    assert maturin["manifest-path"] == "../../Cargo.toml"
    assert maturin["features"] == ["full", "ast-extended"]
    root = tomllib.loads((ROOT / "Cargo.toml").read_text())
    assert "epistemic-graph-server" in {b["name"] for b in root["bin"]}
    assert "--profile release-server" in ACTION.read_text()


@pytest.mark.spec("EG-CONTRACT-R053.9")
def test_release_builds_every_platform_natively_and_uploads_each() -> None:
    jobs = _yaml(RELEASE)["jobs"]
    job = jobs["build-server"]
    assert job["needs"] == "guard"
    legs = {leg["arch"]: leg["runner"] for leg in job["strategy"]["matrix"]["include"]}
    assert legs == {
        "x86_64": "ubuntu-latest",
        "aarch64": "ubuntu-24.04-arm",
        "macos-arm64": "macos-14",
        "windows-x64": "windows-latest",
    }
    assert job["name"] == "server wheel ${{ matrix.label }}"
    assert job["defaults"]["run"]["shell"] == "bash"
    # Swap lives in the shared build action, so the release leg and ci.yml's
    # warm-cache build both get it (the cache build had none and was OOM-killed).
    action = _yaml(ACTION)["runs"]["steps"]
    swap = next(s for s in action if "swap" in s.get("name", ""))
    assert swap["if"] == "runner.os == 'Linux'"
    script = _script(job)
    assert '"$smoke/$exe" --help' in script
    assert "Scripts" in script and "epistemic-graph-server.exe" in script
    assert "binary_path" in script
    upload = next(s for s in job["steps"] if s.get("name", "").startswith("Publish"))
    assert "twine upload --skip-existing" in upload["run"]
    assert "PYPI_API_TOKEN" in str(upload["env"])
    # Smoke steps run before the upload.
    names = [s.get("name", "") for s in job["steps"]]
    assert names.index("Smoke test the built wheel") < names.index(upload["name"])
    verify = _script(jobs["verify-pypi"])
    assert "--package epistemic-graph-server" in verify
    assert "build-server" in jobs["verify-pypi"]["needs"]
    for tag in (
        "manylinux_2_28_x86_64",
        "manylinux_2_28_aarch64",
        "macosx_11_0_arm64",
        "win_amd64",
    ):
        assert tag in verify


@pytest.mark.spec("EG-CONTRACT-R053.9")
def test_compile_cache_is_saved_by_main_ci_and_only_restored_by_release() -> None:
    action = ACTION.read_text()
    assert "shared-key: egs-${{ steps.env.outputs.key }}" in action
    assert "save-if: ${{ inputs.cache-save == 'true' }}" in action
    cache_job = _yaml(CI)["jobs"]["server-wheel-cache"]
    assert "github.ref == 'refs/heads/main'" in cache_job["if"]
    assert cache_job["strategy"]["matrix"]["arch"] == ["x86_64", "aarch64"]
    assert "ubuntu-24.04-arm" in cache_job["runs-on"]
    assert "cache-save: 'true'" in yaml.safe_dump(cache_job)
    release_uses = [
        step
        for step in _yaml(RELEASE)["jobs"]["build-server"]["steps"]
        if "uses" in step
    ]
    build = next(s for s in release_uses if s["uses"].endswith("build-server-wheel"))
    assert "cache-save" not in build.get("with", {})


@pytest.mark.spec("EG-CONTRACT-R053.10")
def test_package_ships_binary_path_and_its_tests() -> None:
    init = (PACKAGE / "epistemic_graph_server/__init__.py").read_text()
    assert "def binary_path()" in init
    assert "class BinaryNotFoundError(FileNotFoundError)" in init
    assert (PACKAGE / "tests/test_binary_path.py").is_file()
    server_binary = (ROOT / "epistemic_graph/server_binary.py").read_text()
    assert "from epistemic_graph_server import binary_path" in server_binary


@pytest.mark.spec("EG-CONTRACT-R053.11")
def test_image_is_built_from_the_wheel_and_published_as_one_manifest() -> None:
    dockerfile = DOCKERFILE.read_text()
    assert "AS runtime" in dockerfile
    assert "docker/wheels/" in dockerfile
    assert "EGS_VERSION must be numeric" in dockerfile
    assert 'ENTRYPOINT ["epistemic-graph-server"]' in dockerfile
    assert "EXPOSE 9100 9101" in dockerfile
    assert "USER 10001:10001" in dockerfile
    jobs = _yaml(RELEASE)["jobs"]
    image = jobs["server-image"]
    assert image["needs"] == ["guard", "build-server"]
    platforms = {leg["platform"] for leg in image["strategy"]["matrix"]["include"]}
    assert platforms == {"linux/amd64", "linux/arm64"}
    assert "push-by-digest=true" in _script(image)
    manifest = jobs["server-manifest"]
    assert manifest["needs"] == "server-image"
    script = _script(manifest)
    assert "docker buildx imagetools create" in script
    assert "epistemic-graph-server" in str(manifest["steps"])


@pytest.mark.spec("EG-CONTRACT-R053.12")
def test_package_version_is_bumped_with_the_engine() -> None:
    version = _version()
    project = tomllib.loads((PACKAGE / "pyproject.toml").read_text())["project"]
    assert project["version"] == version
    init = (PACKAGE / "epistemic_graph_server/__init__.py").read_text()
    assert f'__version__ = "{version}"' in init
    assert f"ARG EGS_VERSION={version}" in DOCKERFILE.read_text()
    cfg = (ROOT / ".config/bumpversion.cfg").read_text()
    for target in (
        "packaging/epistemic-graph-server/pyproject.toml",
        "packaging/epistemic-graph-server/epistemic_graph_server/__init__.py",
        "docker/Dockerfile",
    ):
        assert f"[bumpversion:file:{target}]" in cfg
    assert "ARG EGS_VERSION={current_version}" in cfg


@pytest.mark.spec("EG-CONTRACT-R053.12")
def test_dispatch_can_package_a_tagged_version_behind_the_guard() -> None:
    doc = _yaml(RELEASE)
    triggers = doc[True] if True in doc else doc["on"]
    inputs = triggers["workflow_dispatch"]["inputs"]
    assert inputs["source_ref"]["default"] == ""
    assert inputs["packages"]["options"] == ["all", "server"]
    guard = doc["jobs"]["guard"]
    script = _script(guard)
    assert "commits/refs/tags/${SOURCE_REF}" in script
    assert "source_ref is only valid with packages=server" in script
    assert guard["outputs"]["sha"] == "${{ steps.guard.outputs.sha }}"
    assert "head_sha=${SHA}&status=success" in script
    build = doc["jobs"]["build-server"]
    checkout = [s for s in build["steps"] if s.get("with", {}).get("path") == "source"]
    assert checkout[0]["with"]["ref"] == "${{ needs.guard.outputs.sha }}"
    for name in ("build", "build-x86", "build-sdist"):
        assert "inputs.packages != 'server'" in doc["jobs"][name]["if"]
    assert re.search(r"source .* != Cargo.toml version", _script(build))


def test_tag_release_creates_the_github_release_after_pypi() -> None:
    """The GitHub release follows the PyPI check and links both packages."""
    job = _yaml(RELEASE)["jobs"]["github-release"]
    assert job["needs"] == ["verify-pypi"]
    assert job["permissions"] == {"contents": "write"}
    script = _script(job)
    for package in ("epistemic-graph", "epistemic-graph-server"):
        assert package in script
    assert "https://pypi.org/project/$package/$version/" in script
    assert "--latest" in script
