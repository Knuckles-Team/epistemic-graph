"""The server package builds and publishes here (MEDULLA-CONTRACT-R053.9-12)."""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import tomllib
import yaml

pytestmark = pytest.mark.no_engine

ROOT = Path(__file__).resolve().parents[1]
PACKAGE = ROOT / "packaging/medulla-server"
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


@pytest.mark.spec("MEDULLA-CONTRACT-R053.9")
def test_package_wraps_the_root_server_bin_with_the_full_feature_set() -> None:
    project = tomllib.loads((PACKAGE / "pyproject.toml").read_text())
    maturin = project["tool"]["maturin"]
    assert project["project"]["name"] == "medulla-stem"
    assert maturin["bindings"] == "bin"
    assert maturin["manifest-path"] == "../../Cargo.toml"
    assert maturin["features"] == ["full", "ast-extended"]
    root = tomllib.loads((ROOT / "Cargo.toml").read_text())
    assert "medulla-server" in {b["name"] for b in root["bin"]}
    assert "--profile release-server" in ACTION.read_text()


@pytest.mark.spec("MEDULLA-CONTRACT-R053.9")
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
    assert "Scripts" in script and "medulla-server.exe" in script
    assert "binary_path" in script
    upload = next(s for s in job["steps"] if s.get("name", "").startswith("Publish"))
    assert "twine upload --skip-existing" in upload["run"]
    assert "PYPI_API_TOKEN" in str(upload["env"])
    # Smoke steps run before the upload.
    names = [s.get("name", "") for s in job["steps"]]
    assert names.index("Smoke test the built wheel") < names.index(upload["name"])
    verify = _script(jobs["verify-pypi"])
    assert "--package medulla-stem" in verify
    assert "build-server" in jobs["verify-pypi"]["needs"]
    for tag in (
        "manylinux_2_28_x86_64",
        "manylinux_2_28_aarch64",
        "macosx_11_0_arm64",
        "win_amd64",
    ):
        assert tag in verify


@pytest.mark.spec("MEDULLA-CONTRACT-R053.9")
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


@pytest.mark.spec("MEDULLA-CONTRACT-R053.10")
def test_package_ships_binary_path_and_its_tests() -> None:
    init = (PACKAGE / "medulla_server/__init__.py").read_text()
    assert "def binary_path()" in init
    assert "class BinaryNotFoundError(FileNotFoundError)" in init
    assert (PACKAGE / "tests/test_binary_path.py").is_file()
    server_binary = (ROOT / "medulla/server_binary.py").read_text()
    assert "from medulla_server import binary_path" in server_binary


@pytest.mark.spec("MEDULLA-CONTRACT-R053.11")
def test_image_is_built_from_the_wheel_and_published_as_one_manifest() -> None:
    dockerfile = DOCKERFILE.read_text()
    assert "AS runtime" in dockerfile
    assert "docker/wheels/" in dockerfile
    assert "EGS_VERSION must be numeric" in dockerfile
    assert 'ENTRYPOINT ["medulla-server"]' in dockerfile
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
    assert "medulla-server" in str(manifest["steps"])


@pytest.mark.spec("MEDULLA-CONTRACT-R053.12")
def test_package_version_is_bumped_with_the_engine() -> None:
    version = _version()
    project = tomllib.loads((PACKAGE / "pyproject.toml").read_text())["project"]
    assert project["version"] == version
    init = (PACKAGE / "medulla_server/__init__.py").read_text()
    assert f'__version__ = "{version}"' in init
    assert f"ARG EGS_VERSION={version}" in DOCKERFILE.read_text()
    cfg = (ROOT / ".config/bumpversion.cfg").read_text()
    for target in (
        "packaging/medulla-server/pyproject.toml",
        "packaging/medulla-server/medulla_server/__init__.py",
        "docker/Dockerfile",
    ):
        assert f"[bumpversion:file:{target}]" in cfg
    assert "ARG EGS_VERSION={current_version}" in cfg


@pytest.mark.spec("MEDULLA-CONTRACT-R053.12")
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
    for package in ("medulla", "medulla-stem"):
        assert package in script
    assert "https://pypi.org/project/$package/$version/" in script
    assert "--latest" in script


@pytest.mark.spec("MEDULLA-CONTRACT-R053.9")
def test_server_sdist_is_built_proven_published_and_verified() -> None:
    jobs = _yaml(RELEASE)["jobs"]
    steps = jobs["build-server"]["steps"]
    names = [s.get("name", "") for s in steps]
    build = steps[names.index("Build the server source distribution")]
    proof = steps[
        names.index("Prove the server source distribution by an isolated rebuild")
    ]
    publish = steps[names.index("Publish the server source distribution to PyPI")]
    upload = next(
        s for s in steps if (s.get("with") or {}).get("name") == "server-sdist"
    )
    for step in (build, proof, upload, publish):
        assert "matrix.arch == 'x86_64'" in step["if"]
    assert "maturin sdist" in build["run"]
    assert "medulla_stem-${VERSION}.tar.gz" in build["run"]
    assert "pip wheel --isolated --no-deps" in proof["run"]
    assert '"$work/venv/bin/medulla-server" --help' in proof["run"]
    assert (
        "twine upload --skip-existing source/dist-server-sdist/*.tar.gz"
        in publish["run"]
    )
    order = [
        names.index(n)
        for n in (
            "Smoke test the built wheel",
            "Build the server source distribution",
            "Prove the server source distribution by an isolated rebuild",
            "Publish the server source distribution to PyPI",
        )
    ]
    assert order == sorted(order)
    assert steps.index(upload) < names.index(
        "Publish the server source distribution to PyPI"
    )
    verify = jobs["verify-pypi"]["steps"]
    assert any((s.get("with") or {}).get("name") == "server-sdist" for s in verify)
    assert 'medulla_stem-"${GITHUB_REF_NAME#v}".tar.gz' in _script(jobs["verify-pypi"])


def _path_crates(manifest_dir: Path, seen: dict[Path, None]) -> None:
    manifest = tomllib.loads((manifest_dir / "Cargo.toml").read_text())
    tables = [manifest, *manifest.get("target", {}).values()]
    for table in tables:
        for kind in ("dependencies", "dev-dependencies", "build-dependencies"):
            for spec in table.get(kind, {}).values():
                if isinstance(spec, dict) and "path" in spec:
                    crate = (manifest_dir / spec["path"]).resolve()
                    if crate not in seen:
                        seen[crate] = None
                        _path_crates(crate, seen)


@pytest.mark.spec("MEDULLA-CONTRACT-R053.9")
def test_sdist_carries_every_path_crate_cargo_needs_to_load_the_manifest() -> None:
    """maturin drops path crates reached only through unselected optional features,
    but `cargo metadata` still loads their manifests, so the isolated rebuild fails
    unless each crate tree is an explicit sdist include. maturin rejects `..` in
    include patterns, so the package dir carries `crates` and `vendor` symlinks to
    workspace trees and the patterns are relative to them."""
    seen: dict[Path, None] = {}
    _path_crates(ROOT, seen)
    members = tomllib.loads((ROOT / "Cargo.toml").read_text())["workspace"]["members"]
    expected = {str(c.relative_to(ROOT)) for c in seen} | set(members)
    project = tomllib.loads((PACKAGE / "pyproject.toml").read_text())
    includes = project["tool"]["maturin"]["include"]
    assert all(i["format"] == "sdist" for i in includes)
    included = {i["path"].removesuffix("/**/*") for i in includes}
    link = PACKAGE / "crates"
    assert link.is_symlink() and link.resolve() == ROOT / "crates"
    vendor = PACKAGE / "vendor"
    assert vendor.is_symlink() and vendor.resolve() == ROOT / "vendor"
    assert expected - included == set(), "crates missing from the sdist include list"
    assert "crates/medulla-asr-whisper" in included
    assert "crates/medulla-tts-piper" in included
    assert {
        "vendor/zstd-parent-patches",
        "vendor/security-patches",
    } <= included
