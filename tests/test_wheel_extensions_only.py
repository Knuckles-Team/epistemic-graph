"""The public wheel compiles only the extension modules (EG-CONTRACT-R053.8)."""

from __future__ import annotations

from pathlib import Path

import pytest
import tomllib
import yaml

pytestmark = pytest.mark.no_engine

ROOT = Path(__file__).resolve().parents[1]
RELEASE = ROOT / ".github/workflows/release.yml"
CI = ROOT / ".github/workflows/ci.yml"
ACTION = ROOT / ".github/actions/folded-wheel/action.yml"


def _yaml(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


@pytest.mark.spec("EG-CONTRACT-R053.8")
def test_wheel_base_is_the_extension_crate_not_the_server_bin() -> None:
    maturin = tomllib.loads((ROOT / "pyproject.toml").read_text())["tool"]["maturin"]
    assert maturin["manifest-path"] == "crates/eg-pyengine/Cargo.toml"
    assert maturin["module-name"] == "epistemic_graph.engine"
    assert maturin.get("bindings") != "bin"
    root = tomllib.loads((ROOT / "Cargo.toml").read_text())
    pyengine = tomllib.loads((ROOT / "crates/eg-pyengine/Cargo.toml").read_text())
    # maturin takes the wheel version from the manifest it builds.
    assert pyengine["package"]["version"] == root["package"]["version"]
    bump = (ROOT / ".config/bumpversion.cfg").read_text()
    assert "[bumpversion:file:crates/eg-pyengine/Cargo.toml]" in bump
    assert "[bumpversion:file(eg-pyengine):Cargo.lock]" in bump
    backend = (ROOT / "build_backend.py").read_text()
    assert "ENGINE_MANIFEST" not in backend
    assert "NUMERIC_MANIFEST" in backend


@pytest.mark.spec("EG-CONTRACT-R053.8")
def test_no_wheel_path_compiles_a_server_target() -> None:
    steps = _yaml(ACTION)["runs"]["steps"]
    commands = "\n".join(
        str(step.get("with", {}).get("args", "")) + step.get("run", "")
        for step in steps
    )
    assert "--features" in commands  # the numeric component only
    assert "full,ast-extended" not in commands
    assert "--bins" not in commands
    ci_gate = next(
        step
        for step in _yaml(CI)["jobs"]["gates-wheel"]["steps"]
        if "dist-gates" in step.get("run", "") and "maturin build" in step["run"]
    )
    assert "--features server" not in ci_gate["run"]
    assert "--no-default-features" not in ci_gate["run"]
    # The safety nets stay.
    assert "scripts/strip_wheel_executables.py" in ACTION.read_text()
    assert "--require-engine-kernel" in ACTION.read_text()


@pytest.mark.spec("EG-CONTRACT-R053.8")
def test_each_platform_publishes_on_its_own_behind_the_guard() -> None:
    jobs = _yaml(RELEASE)["jobs"]
    assert "publish-pypi" not in jobs
    for name in ("build", "build-x86", "build-sdist"):
        assert jobs[name]["needs"] == "guard"
    legs = [s.get("name", "") for s in jobs["build"]["steps"]]
    smoke = next(i for i, n in enumerate(legs) if n.startswith("Smoke test"))
    publish = next(i for i, n in enumerate(legs) if n.startswith("Publish "))
    assert smoke < publish
    assert jobs["publish-x86"]["needs"] == "build-x86"
    sdist_uses = [s.get("uses") for s in jobs["build-sdist"]["steps"]]
    assert "./.github/actions/publish-distribution" in sdist_uses
    # No uploader waits on another platform; only the verifier joins them.
    assert set(jobs["verify-pypi"]["needs"]) == {"build", "publish-x86", "build-sdist"}
    assert "twine upload" not in "\n".join(
        s.get("run", "") for s in jobs["verify-pypi"]["steps"]
    )
    action = (ROOT / ".github/actions/publish-distribution/action.yml").read_text()
    assert "twine upload --skip-existing" in action


@pytest.mark.spec("EG-CONTRACT-R053.8")
def test_compile_cache_is_saved_from_main_and_restored_on_tags() -> None:
    cache = next(
        s for s in _yaml(ACTION)["runs"]["steps"] if "rust-cache" in s.get("uses", "")
    )
    assert cache["with"]["shared-key"] == "eg-wheel-${{ env.EG_WHEEL_TARGET }}"
    assert cache["with"]["save-if"] == "${{ inputs.cache-save == 'true' }}"
    ci = _yaml(CI)["jobs"]["wheel-cache"]
    assert "github.event_name == 'push'" in ci["if"]
    assert "refs/heads/main" in ci["if"]
    warm = next(s for s in ci["steps"] if s.get("uses", "").endswith("folded-wheel"))
    assert warm["with"]["cache-save"] == "true"
    release = RELEASE.read_text()
    assert "cache-save: 'true'" not in release and "cache-save: true" not in release
