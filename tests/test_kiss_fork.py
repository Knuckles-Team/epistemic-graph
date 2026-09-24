"""The kiss fork pin: CI installs exactly the probed rev, and the probe
rejects the crates.io 0.4.10 build that prints the same version line."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest
import yaml

REPO = Path(__file__).resolve().parents[1]


def _load_kiss_fork():
    spec = importlib.util.spec_from_file_location(
        "eg_kiss_fork", REPO / "scripts/kiss_fork.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


kiss_fork = _load_kiss_fork()
pytestmark = pytest.mark.no_engine

UPSTREAM = "#!/bin/sh\necho 'missing module helper declared from src/a.rs'\nexit 1\n"
FORK = "#!/bin/sh\ntest -f src/a/tests/helper.rs || exit 3\necho 'NO VIOLATIONS'\n"


def _fake(tmp_path: Path, body: str) -> str:
    binary = tmp_path / "kiss"
    binary.write_text(body, encoding="utf-8")
    binary.chmod(0o755)
    return str(binary)


def _scanner_steps() -> dict[str, dict]:
    workflow = yaml.safe_load((REPO / ".github/workflows/release.yml").read_text())
    return {
        step.get("name"): step for step in workflow["jobs"]["scanner-quality"]["steps"]
    }


def test_probe_rejects_the_upstream_build(tmp_path: Path) -> None:
    with pytest.raises(kiss_fork.NotForkBuild, match="not the pinned kiss fork"):
        kiss_fork.require_fork_build(_fake(tmp_path, UPSTREAM))
    assert kiss_fork.main([_fake(tmp_path, UPSTREAM)]) == 2


def test_probe_accepts_a_build_that_resolves_the_inline_module(tmp_path: Path) -> None:
    kiss_fork.require_fork_build(_fake(tmp_path, FORK))
    assert kiss_fork.main([_fake(tmp_path, FORK)]) == 0


def test_ci_installs_and_probes_exactly_the_pinned_fork_rev() -> None:
    steps = _scanner_steps()
    install = steps["Provision pinned scanner toolchain"]["run"]
    assert (
        f"cargo install --locked --git {kiss_fork.KISS_FORK_GIT} "
        f"--rev {kiss_fork.KISS_FORK_REV} --root" in install
    )
    assert "--version 0.4.10" not in install
    verify = steps["Verify scanner versions"]["run"]
    assert 'python3 scripts/kiss_fork.py "$(command -v kiss)"' in verify
    key = steps["Restore pinned scanner toolchain"]["with"]["key"]
    assert f"kiss-ai-{kiss_fork.KISS_FORK_REV}" in key
