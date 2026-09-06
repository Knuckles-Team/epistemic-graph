"""Regression tests for the changed-Rust KISS hook's process context."""

from __future__ import annotations

import os
import stat
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
HOOK = REPO / "scripts/check_kiss_staged.sh"
KISS_VERSION = "0.4.10"
pytestmark = pytest.mark.no_engine


def _temporary_kiss(path: Path, log: Path) -> None:
    path.write_text(
        "#!/bin/sh\n"
        f"printf '%s\\n' \"$PWD\" >> '{log}'\n"
        "if [ \"$1\" = '--version' ]; then\n"
        f"  printf 'kiss {KISS_VERSION}\\n'\n"
        "  exit 0\n"
        "fi\n"
        "if [ \"$1\" = 'check' ]; then\n"
        "  printf 'NO VIOLATIONS\\n'\n"
        "  exit 0\n"
        "fi\n"
        "exit 2\n",
        encoding="utf-8",
    )
    path.chmod(path.stat().st_mode | stat.S_IXUSR)


def test_hook_resolves_its_worktree_when_called_outside_checkout(
    tmp_path: Path,
) -> None:
    """The hook must retain changed-file checking outside the caller's cwd."""

    kiss = tmp_path / "kiss"
    log = tmp_path / "kiss.cwd"
    _temporary_kiss(kiss, log)

    # Build a private staged index containing one Rust path without changing
    # this checkout's index or worktree.  The fake KISS binary makes the test
    # independent of optional scanner installation while still proving that
    # the changed-file invocation runs from the resolved repository root.
    index = tmp_path / "index"
    env = os.environ.copy()
    env.update(
        {
            "GIT_INDEX_FILE": str(index),
            "GIT_DIR": str(REPO / ".git"),
            "GIT_WORK_TREE": str(tmp_path),
            "GIT_COMMON_DIR": str(REPO / ".git"),
            "KISS_BIN": str(kiss),
        }
    )
    index_env = {
        key: value
        for key, value in env.items()
        if not key.startswith("GIT_") or key == "GIT_INDEX_FILE"
    }
    subprocess.run(
        ["git", "read-tree", "HEAD"],
        cwd=REPO,
        env=index_env,
        check=True,
    )
    source_blob = subprocess.run(
        ["git", "rev-parse", "HEAD:src/lib.rs"],
        cwd=REPO,
        env=index_env,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    module_walker_blob = subprocess.run(
        ["git", "hash-object", "-w", "scripts/rust_module_tree.py"],
        cwd=REPO,
        env=index_env,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    subprocess.run(
        [
            "git",
            "update-index",
            "--add",
            "--cacheinfo",
            f"100644,{source_blob},src/main.rs",
        ],
        cwd=REPO,
        env=index_env,
        check=True,
    )
    subprocess.run(
        [
            "git",
            "update-index",
            "--add",
            "--cacheinfo",
            f"100644,{module_walker_blob},scripts/rust_module_tree.py",
        ],
        cwd=REPO,
        env=index_env,
        check=True,
    )

    result = subprocess.run(
        ["bash", str(HOOK)],
        cwd=tmp_path,
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert "src/main.rs" in result.stdout
    assert "kiss(staged): 0 violation(s) across 1 changed file(s)" in result.stdout
    cwds = log.read_text(encoding="utf-8").splitlines()
    assert cwds
    # The `--version` probe runs at the repository root.  The `check` invocation
    # deliberately runs inside the private staged-source tree the hook
    # materializes, so the scanner reads the bytes that will be committed rather
    # than the working tree.  Neither may ever be the caller's cwd.
    assert str(REPO) in cwds
    assert str(tmp_path) not in cwds
