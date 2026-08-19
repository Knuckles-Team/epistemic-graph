"""Regression fixtures for the check-only, scoped Rustfmt guard."""

from __future__ import annotations

from pathlib import Path

import pytest

import scripts.check_rustfmt_scope as rustfmt_scope
from scripts.check_rustfmt_scope import (
    GuardError,
    _explicit_paths,
    _is_scope_path,
    _scope_requires_full_check,
    build_rustfmt_command,
)


def test_guard_uses_pinned_check_only_rustfmt_without_cargo_workspace_sweep() -> None:
    command = build_rustfmt_command("1.96.0", "2021", ["src/server/mod.rs"])

    assert command == [
        "rustup",
        "run",
        "1.96.0",
        "rustfmt",
        "--edition",
        "2021",
        "--check",
        "--",
        "src/server/mod.rs",
    ]
    assert "cargo" not in command
    assert "--all" not in command


def test_formatting_configuration_change_widens_only_the_read_only_check() -> None:
    assert _scope_requires_full_check(["rust-toolchain.toml"])
    assert _scope_requires_full_check(["crates/eg-core/Cargo.toml"])
    assert _scope_requires_full_check(["scripts/check_rustfmt_scope.py"])
    assert not _scope_requires_full_check(["src/server/mod.rs"])


def test_hook_and_queue_authority_files_are_in_scope_and_widen_the_check() -> None:
    for path in (
        ".pre-commit-config.yaml",
        ".mergequeue.yaml",
        "scripts/check_rustfmt_scope.py",
    ):
        assert _is_scope_path(path)
        assert _scope_requires_full_check([path])


def test_explicit_scope_rejects_unowned_files_and_parent_escape() -> None:
    with pytest.raises(GuardError, match="unsupported path"):
        _explicit_paths(["README.md"])
    with pytest.raises(GuardError, match="escapes the repository"):
        _explicit_paths(["../outside.rs"])


def test_scope_rejects_symlink_even_when_target_stays_inside_checkout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "source.rs"
    source.write_text("fn main() {}\n", encoding="utf-8")
    (tmp_path / "linked.rs").symlink_to(source)
    monkeypatch.setattr(rustfmt_scope, "ROOT", tmp_path)

    with pytest.raises(GuardError, match="symlinked path"):
        rustfmt_scope._normalise_path("linked.rs")
