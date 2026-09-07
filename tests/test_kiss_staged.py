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
    for name in (
        "check_kiss_staged.sh",
        "rust_lexer.py",
        "rust_module_tree.py",
        "scanner_contract.py",
    ):
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
        'if [ -n "${KISS_EXPECT_CONFIG_TEXT:-}" ]; then\n'
        '  grep -Fq -- "$KISS_EXPECT_CONFIG_TEXT" "$3" || exit 9\n'
        "fi\n"
        "source=${6:?missing source path}\n"
        'if [ -n "${KISS_EXPECT_PATHS:-}" ]; then\n'
        "  old_ifs=$IFS\n"
        "  IFS=:\n"
        "  for expected in $KISS_EXPECT_PATHS; do\n"
        '    [ -f "$expected" ] || exit 7\n'
        '    cat -- "$expected" >> "${KISS_TEST_LOG:?missing test log}"\n'
        "  done\n"
        "  IFS=$old_ifs\n"
        "else\n"
        '  cat -- "$source" >> "${KISS_TEST_LOG:?missing test log}"\n'
        "fi\n"
        'if [ -n "${KISS_REJECT_PATH:-}" ] && [ -e "$KISS_REJECT_PATH" ]; then\n'
        "  exit 8\n"
        "fi\n"
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
        "scripts/rust_lexer.py",
        "scripts/rust_module_tree.py",
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


def _commit_files(repo: Path, *paths: str) -> None:
    _run("git", "add", "--", *paths, cwd=repo)
    _run("git", "commit", "-qm", "module fixture", cwd=repo)


def _run_hook(repo: Path, kiss: Path, log: Path, **updates: str):
    env = _hook_env(kiss, log)
    env.update(updates)
    return subprocess.run(
        ["bash", "scripts/check_kiss_staged.sh"],
        cwd=repo,
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )


def test_hook_materializes_nested_cfg_path_and_include_closure(
    repository: tuple[Path, Path, Path],
) -> None:
    repo, kiss, log = repository
    (repo / "src/root").mkdir()
    (repo / "src/special").mkdir()
    (repo / "src/root.rs").write_text(
        "mod child;\n"
        "#[cfg(feature = \"extra\")]\n#[path = \"special/extra.rs\"]\nmod extra;\n"
        'include!("included.inc");\n',
        encoding="utf-8",
    )
    (repo / "src/root/child.rs").write_text("mod nested;\n", encoding="utf-8")
    (repo / "src/root/child/nested.rs").parent.mkdir()
    (repo / "src/root/child/nested.rs").write_text("nested clean\n", encoding="utf-8")
    (repo / "src/special/extra.rs").write_text("extra clean\n", encoding="utf-8")
    (repo / "src/included.inc").write_text("included clean\n", encoding="utf-8")
    _commit_files(
        repo,
        "src/root.rs",
        "src/root/child.rs",
        "src/root/child/nested.rs",
        "src/special/extra.rs",
        "src/included.inc",
    )
    (repo / "src/root.rs").write_text(
        "// staged root\n" + (repo / "src/root.rs").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    _run("git", "add", "--", "src/root.rs", cwd=repo)

    result = _run_hook(
        repo,
        kiss,
        log,
        KISS_EXPECT_PATHS=(
            "src/root.rs:src/root/child.rs:src/root/child/nested.rs:"
            "src/special/extra.rs:src/included.inc"
        ),
    )

    assert result.returncode == 0, result.stderr
    scanned = log.read_text(encoding="utf-8")
    for marker in ("staged root", "nested clean", "extra clean", "included clean"):
        assert marker in scanned


def test_hook_fails_closed_for_missing_declared_child(
    repository: tuple[Path, Path, Path],
) -> None:
    repo, kiss, log = repository
    source = repo / "src/example.rs"
    source.write_text("mod missing;\n", encoding="utf-8")
    _run("git", "add", "--", "src/example.rs", cwd=repo)

    result = _run_hook(repo, kiss, log)

    assert result.returncode == 2
    assert "could not prove staged Rust module closure" in result.stderr
    assert not log.exists()


def test_hook_fails_closed_for_path_traversal(
    repository: tuple[Path, Path, Path], tmp_path: Path
) -> None:
    repo, kiss, log = repository
    outside = tmp_path / "outside.rs"
    outside.write_text("outside spoof\n", encoding="utf-8")
    source = repo / "src/example.rs"
    source.write_text(
        f'#[path = "{outside}"]\nmod escaped;\n',
        encoding="utf-8",
    )
    _run("git", "add", "--", "src/example.rs", cwd=repo)

    result = _run_hook(repo, kiss, log)

    assert result.returncode == 2
    assert "escapes root" in result.stderr
    assert not log.exists()


def test_hook_rejects_symlink_source_spoof(
    repository: tuple[Path, Path, Path], tmp_path: Path
) -> None:
    repo, kiss, log = repository
    outside = tmp_path / "outside-root.rs"
    outside.write_text("outside spoof\n", encoding="utf-8")
    source = repo / "src/spoof.rs"
    source.symlink_to(outside)
    _run("git", "add", "--", "src/spoof.rs", cwd=repo)

    result = _run_hook(repo, kiss, log)

    assert result.returncode == 2
    assert "staged Rust tree contains a symlink" in result.stderr
    assert not log.exists()


def test_hook_excludes_unrelated_unstaged_child_bytes(
    repository: tuple[Path, Path, Path],
) -> None:
    repo, kiss, log = repository
    (repo / "src/example.rs").write_text("mod child;\n", encoding="utf-8")
    (repo / "src/example").mkdir()
    child = repo / "src/example/child.rs"
    child.write_text("indexed child clean\n", encoding="utf-8")
    _commit_files(repo, "src/example.rs", "src/example/child.rs")
    (repo / "src/example.rs").write_text(
        "// staged root\nmod child;\n", encoding="utf-8"
    )
    _run("git", "add", "--", "src/example.rs", cwd=repo)
    child.write_text("unstaged child violation\n", encoding="utf-8")

    result = _run_hook(
        repo,
        kiss,
        log,
        KISS_EXPECT_PATHS="src/example.rs:src/example/child.rs",
    )

    assert result.returncode == 0, result.stderr
    assert log.read_text(encoding="utf-8") == (
        "// staged root\nmod child;\nindexed child clean\n"
    )


def test_hook_handles_newline_in_staged_rust_path(
    repository: tuple[Path, Path, Path],
) -> None:
    repo, kiss, log = repository
    source = repo / "src/evasion\nunit.rs"
    source.write_text("newline path clean\n", encoding="utf-8")
    _run("git", "add", "--", str(source.relative_to(repo)), cwd=repo)

    result = _run_hook(repo, kiss, log)

    assert result.returncode == 0, result.stderr
    assert "1 changed file(s)" in result.stdout
    assert log.read_text(encoding="utf-8") == "newline path clean\n"


@pytest.mark.parametrize(
    ("declaration", "child_path"),
    [("mod child;\n", "src/root/child.rs"), ('include!("child.rs");\n', "src/child.rs")],
)
def test_hook_rejects_nested_module_or_include_symlink(
    repository: tuple[Path, Path, Path], declaration: str, child_path: str
) -> None:
    repo, kiss, log = repository
    (repo / "src/root.rs").write_text(declaration, encoding="utf-8")
    (repo / "src/real.rs").write_text("real clean\n", encoding="utf-8")
    child = repo / child_path
    child.parent.mkdir(exist_ok=True)
    child.symlink_to(repo / "src/real.rs")
    _commit_files(repo, "src/root.rs", child_path, "src/real.rs")
    (repo / "src/root.rs").write_text(
        "// staged root\n" + declaration, encoding="utf-8"
    )
    _run("git", "add", "--", "src/root.rs", cwd=repo)

    result = _run_hook(repo, kiss, log)

    assert result.returncode == 2
    assert "staged Rust tree contains a symlink" in result.stderr
    assert not log.exists()


def test_hook_uses_staged_config_and_scanner_contract_module(
    repository: tuple[Path, Path, Path],
) -> None:
    repo, kiss, log = repository
    source = repo / "src/example.rs"
    source.write_text("staged clean\n", encoding="utf-8")
    config = repo / ".kiss/kiss.toml"
    staged_config = config.read_text(encoding="utf-8") + "\n# staged-policy-marker\n"
    config.write_text(staged_config, encoding="utf-8")
    _run("git", "add", "--", "src/example.rs", ".kiss/kiss.toml", cwd=repo)
    config.write_text("# unstaged policy spoof\n", encoding="utf-8")
    (repo / "scripts/scanner_contract.py").write_text(
        'raise RuntimeError("unstaged module spoof")\n', encoding="utf-8"
    )

    result = _run_hook(
        repo, kiss, log, KISS_EXPECT_CONFIG_TEXT="staged-policy-marker"
    )

    assert result.returncode == 0, result.stderr
    assert log.read_text(encoding="utf-8") == "staged clean\n"


def test_hook_uses_staged_pyproject_scanner_version(
    repository: tuple[Path, Path, Path],
) -> None:
    repo, kiss, log = repository
    source = repo / "src/example.rs"
    source.write_text("staged clean\n", encoding="utf-8")
    pyproject = repo / "pyproject.toml"
    original = pyproject.read_text(encoding="utf-8")
    pyproject.write_text(
        original.replace('kiss_version = "0.4.10"', 'kiss_version = "0.4.11"'),
        encoding="utf-8",
    )
    _run("git", "add", "--", "src/example.rs", "pyproject.toml", cwd=repo)
    pyproject.write_text(original, encoding="utf-8")

    result = _run_hook(repo, kiss, log)

    assert result.returncode == 2
    assert "expected 'kiss 0.4.11'" in result.stderr
    assert not log.exists()


@pytest.mark.parametrize("policy_path", ["pyproject.toml", "scripts/scanner_contract.py"])
def test_hook_rejects_staged_policy_input_symlink(
    repository: tuple[Path, Path, Path], tmp_path: Path, policy_path: str
) -> None:
    repo, kiss, log = repository
    source = repo / "src/example.rs"
    source.write_text("staged clean\n", encoding="utf-8")
    outside = tmp_path / Path(policy_path).name
    outside.write_bytes((repo / policy_path).read_bytes())
    target = repo / policy_path
    target.unlink()
    target.symlink_to(outside)
    _run("git", "add", "--", "src/example.rs", policy_path, cwd=repo)

    result = _run_hook(repo, kiss, log)

    assert result.returncode == 2
    assert "staged policy input is missing or not a regular file" in result.stderr
    assert not log.exists()
