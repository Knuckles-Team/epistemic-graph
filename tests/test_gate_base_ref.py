"""MEDULLA-CONTRACT-R039: the duplication gate's base is origin/main, not the pushed tip."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

pytestmark = pytest.mark.no_engine

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "gate_base_ref.sh"


def _git(cwd: Path, *args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=cwd, check=True, capture_output=True, text=True
    ).stdout.strip()


def _commit(cwd: Path, name: str) -> str:
    (cwd / name).write_text(name)
    _git(cwd, "add", "--", name)
    _git(cwd, "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-qm", name)
    return _git(cwd, "rev-parse", "HEAD")


@pytest.mark.spec("MEDULLA-CONTRACT-R039")
def test_base_is_origin_main_not_pushed_tip_or_upstream(tmp_path: Path) -> None:
    env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
    env.pop("CX_DUP_BASE_REF", None)
    _git(tmp_path, "init", "-q", "-b", "main")
    main_sha = _commit(tmp_path, "a")
    _git(tmp_path, "update-ref", "refs/remotes/origin/main", main_sha)
    _git(tmp_path, "checkout", "-qb", "lane")
    pushed_sha = _commit(tmp_path, "b")
    _git(tmp_path, "update-ref", "refs/remotes/origin/lane", pushed_sha)
    _git(tmp_path, "config", "remote.origin.url", str(tmp_path))
    _git(
        tmp_path, "config", "remote.origin.fetch", "+refs/heads/*:refs/remotes/origin/*"
    )
    _git(tmp_path, "branch", "-q", "--set-upstream-to=origin/lane")
    _commit(tmp_path, "c")
    env["PRE_COMMIT_FROM_REF"] = pushed_sha
    out = subprocess.run(
        ["bash", str(SCRIPT)],
        cwd=tmp_path,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert out == main_sha
    env["CX_DUP_BASE_REF"] = pushed_sha
    explicit = subprocess.run(
        ["bash", str(SCRIPT)],
        cwd=tmp_path,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert explicit == pushed_sha
