"""Focused regressions for the changed-Rust KISS wrapper."""

from __future__ import annotations

import os
import shutil
import stat
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.no_engine


def _run(*args: str, cwd: Path, env: dict[str, str] | None = None) -> None:
    subprocess.run(args, cwd=cwd, env=env, check=True, capture_output=True, text=True)


@pytest.fixture
def repository(tmp_path: Path) -> tuple[Path, Path, Path]:
    repo = tmp_path / "repo"
    (repo / "scripts").mkdir(parents=True)
    (repo / ".kiss").mkdir()
    (repo / "src").mkdir()
    for name in ("check_kiss_staged.sh", "scanner_contract.py"):
        shutil.copy2(ROOT / "scripts" / name, repo / "scripts" / name)
    shutil.copy2(ROOT / "pyproject.toml", repo / "pyproject.toml")
    shutil.copy2(ROOT / ".kiss/kiss.toml", repo / ".kiss/kiss.toml")
    source = repo / "src/example.rs"
    source.write_text("committed\n", encoding="utf-8")

    kiss = tmp_path / "kiss"
    log = tmp_path / "kiss.log"
    kiss.write_text(
        "#!/usr/bin/env bash\n"
        "set -eu\n"
        'if [ "${1:-}" = --version ]; then\n'
        "  printf 'kiss 0.4.10\\n'\n"
        "  exit 0\n"
        "fi\n"
        '[ "${1:-}" = check ]\n'
        "source=${6:?missing source path}\n"
        'cat -- "$source" >> "${KISS_TEST_LOG:?missing test log}"\n'
        "printf 'Analyzed: 1 files, 0 code_units, 0 statements, 1 graph_nodes, 0 graph_edges\\n'\n"
        'if grep -q violation "$source"; then\n'
        "  printf 'VIOLATION:lines_per_file:%s:1:example.rs: File has 901 lines (threshold: 900) Split it.\\n' \"$source\"\n"
        "  exit 1\n"
        "fi\n"
        "printf 'NO VIOLATIONS\\n'\n",
        encoding="utf-8",
    )
    kiss.chmod(kiss.stat().st_mode | stat.S_IXUSR)

    _run("git", "init", "-q", cwd=repo)
    _run("git", "config", "user.name", "KISS Test", cwd=repo)
    _run("git", "config", "user.email", "kiss@example.invalid", cwd=repo)
    _run(
        "git",
        "add",
        "--",
        ".kiss/kiss.toml",
        "pyproject.toml",
        "scripts/check_kiss_staged.sh",
        "scripts/scanner_contract.py",
        "src/example.rs",
        cwd=repo,
    )
    _run("git", "commit", "-qm", "fixture", cwd=repo)
    return repo, kiss, log


def _hook_env(kiss: Path, log: Path) -> dict[str, str]:
    env = {
        key: value for key, value in os.environ.items() if not key.startswith("GIT_")
    }
    env["KISS_BIN"] = str(kiss)
    env["KISS_TEST_LOG"] = str(log)
    return env


def test_hook_scans_staged_bytes_not_unstaged_worktree_bytes(
    repository: tuple[Path, Path, Path],
) -> None:
    repo, kiss, log = repository
    source = repo / "src/example.rs"
    source.write_text("staged violation\n", encoding="utf-8")
    _run("git", "add", "--", "src/example.rs", cwd=repo)
    source.write_text("unstaged clean\n", encoding="utf-8")

    result = subprocess.run(
        ["bash", "scripts/check_kiss_staged.sh"],
        cwd=repo,
        env=_hook_env(kiss, log),
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1, result.stderr
    assert log.read_text(encoding="utf-8") == "staged violation\n"
    assert "VIOLATION:lines_per_file:" in result.stdout
    assert "1 changed file(s)" in result.stdout


def test_hook_accepts_an_environment_with_no_git_selector_arguments(
    repository: tuple[Path, Path, Path],
) -> None:
    repo, kiss, log = repository

    result = subprocess.run(
        ["bash", "scripts/check_kiss_staged.sh"],
        cwd=repo,
        env=_hook_env(kiss, log),
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert "no staged Rust source" in result.stdout
    assert not log.exists()
