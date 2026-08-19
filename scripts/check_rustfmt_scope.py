#!/usr/bin/env python3
"""Check only the Rust files in the current change with the pinned toolchain.

This guard is deliberately check-only.  In particular, it must never call
``cargo fmt --all``: a formatter hook that writes the workspace can make an
unrelated track appear dirty and can silently rewrite files outside its scope.

The pre-commit hook passes changed paths directly.  The merge queue uses
``--base`` to derive the candidate's changed paths from git.  A formatter or
toolchain manifest change widens the *read-only* check to every tracked Rust
file because that configuration can affect formatting outside the diff.
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from collections import defaultdict
from collections.abc import Iterable
from pathlib import Path

import tomllib

ROOT = Path(__file__).resolve().parents[1]
CONFIGURATION_FILES = frozenset({"Cargo.toml", "rust-toolchain.toml", "rustfmt.toml"})
GUARD_FILES = frozenset(
    {
        ".mergequeue.yaml",
        ".pre-commit-config.yaml",
        "scripts/check_rustfmt_scope.py",
    }
)
PINNED_CHANNEL = re.compile(r"\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?\Z")
EDITION = re.compile(r"\d{4}\Z")


class GuardError(RuntimeError):
    """A malformed or unavailable guard input; callers must fail closed."""


def _git_output(arguments: list[str]) -> str:
    try:
        result = subprocess.run(
            ["git", *arguments],
            cwd=ROOT,
            check=False,
            capture_output=True,
        )
    except OSError as exc:
        raise GuardError(f"cannot execute git: {exc}") from exc
    if result.returncode:
        detail = result.stderr.decode(errors="replace").strip()
        raise GuardError(detail or f"git {' '.join(arguments)} failed")
    return result.stdout.decode(errors="replace")


def _split_nul(output: str) -> list[str]:
    return [entry for entry in output.split("\0") if entry]


def _normalise_path(raw: str) -> str:
    """Return a safe repository-relative path or fail closed."""

    if not raw:
        raise GuardError("empty path was supplied")
    candidate = Path(raw)
    if candidate.is_absolute():
        raise GuardError(f"absolute path is outside the change scope: {raw}")
    normalised = Path(os.path.normpath(raw))
    if normalised == Path(".") or ".." in normalised.parts:
        raise GuardError(f"path escapes the repository: {raw}")
    if normalised.parts and normalised.parts[0] == ".git":
        raise GuardError(f"git metadata is outside the Rust scope: {raw}")
    source = ROOT / normalised
    if source.is_symlink():
        raise GuardError(f"symlinked path is outside the Rustfmt contract: {raw}")
    # Refuse a path that traverses a symlink outside the checkout instead of
    # allowing the formatter to inspect an arbitrary host file.
    try:
        resolved = (ROOT / normalised).resolve(strict=False)
    except (OSError, RuntimeError) as exc:
        raise GuardError(f"cannot resolve path {raw}: {exc}") from exc
    if not resolved.is_relative_to(ROOT):
        raise GuardError(f"symlinked path is outside the repository: {raw}")
    return normalised.as_posix()


def _is_scope_path(path: str) -> bool:
    normalised = Path(path)
    return (
        normalised.suffix == ".rs"
        or normalised.name in CONFIGURATION_FILES
        or path in GUARD_FILES
    )


def _explicit_paths(raw_paths: Iterable[str]) -> list[str]:
    paths: set[str] = set()
    for raw in raw_paths:
        path = _normalise_path(raw)
        if not _is_scope_path(path):
            raise GuardError(
                f"unsupported path {path!r}; pass Rust files or a Rustfmt guard/configuration file"
            )
        paths.add(path)
    return sorted(paths)


def _changed_paths(base: str) -> list[str]:
    output = _git_output(
        [
            "diff",
            "--name-only",
            "--diff-filter=ACMRUXB",
            "-z",
            f"{base}...HEAD",
            "--",
        ]
    )
    # Non-Rust paths are intentionally ignored here: the queue gate is selected
    # by its when_changed patterns, while this command owns only Rust formatting.
    return sorted(
        {
            path
            for raw in _split_nul(output)
            for path in [_normalise_path(raw)]
            if _is_scope_path(path)
        }
    )


def _all_tracked_rust_files() -> list[str]:
    output = _git_output(["ls-files", "-z", "--", "*.rs"])
    return sorted({_normalise_path(path) for path in _split_nul(output)})


def _read_toml(path: Path) -> dict[str, object]:
    try:
        with path.open("rb") as handle:
            document = tomllib.load(handle)
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise GuardError(f"cannot read {path.relative_to(ROOT)}: {exc}") from exc
    if not isinstance(document, dict):
        raise GuardError(f"{path.relative_to(ROOT)} must contain a TOML table")
    return document


def _pinned_channel() -> str:
    path = ROOT / "rust-toolchain.toml"
    if not path.is_file():
        raise GuardError("rust-toolchain.toml is required to pin the Rustfmt toolchain")
    document = _read_toml(path)
    toolchain = document.get("toolchain")
    if not isinstance(toolchain, dict):
        raise GuardError("rust-toolchain.toml has no [toolchain] table")
    channel = toolchain.get("channel")
    if not isinstance(channel, str) or not PINNED_CHANNEL.fullmatch(channel):
        raise GuardError(
            "rust-toolchain.toml channel must be an exact numeric release (for example 1.96.0)"
        )
    return channel


def _manifest_for(path: str) -> Path:
    current = (ROOT / path).parent
    while True:
        manifest = current / "Cargo.toml"
        if manifest.is_file():
            return manifest
        if current == ROOT:
            break
        current = current.parent
    raise GuardError(f"no Cargo.toml found for Rust file {path}")


def _edition_for(path: str) -> str:
    manifest = _manifest_for(path)
    document = _read_toml(manifest)
    package = document.get("package")
    edition: object = package.get("edition") if isinstance(package, dict) else None
    if edition is None:
        workspace = document.get("workspace")
        workspace_package = (
            workspace.get("package") if isinstance(workspace, dict) else None
        )
        edition = (
            workspace_package.get("edition")
            if isinstance(workspace_package, dict)
            else None
        )
    if not isinstance(edition, str) or not EDITION.fullmatch(edition):
        relative = manifest.relative_to(ROOT)
        raise GuardError(f"{relative} has no supported numeric package edition")
    return edition


def _scope_requires_full_check(paths: Iterable[str]) -> bool:
    return any(
        Path(path).name in CONFIGURATION_FILES or path in GUARD_FILES for path in paths
    )


def build_rustfmt_command(
    channel: str, edition: str, paths: Iterable[str]
) -> list[str]:
    """Build the only formatter command this guard is allowed to execute."""

    return [
        "rustup",
        "run",
        channel,
        "rustfmt",
        "--edition",
        edition,
        "--check",
        "--",
        *paths,
    ]


def _run_rustfmt(channel: str, paths: list[str]) -> int:
    by_edition: defaultdict[str, list[str]] = defaultdict(list)
    for path in paths:
        if (ROOT / path).is_file():
            by_edition[_edition_for(path)].append(path)
    if not by_edition:
        print("rustfmt-scope: no existing Rust files in scope")
        return 0

    for edition, edition_paths in sorted(by_edition.items()):
        command = build_rustfmt_command(channel, edition, edition_paths)
        print(
            f"rustfmt-scope: checking {len(edition_paths)} changed Rust file(s) "
            f"with Rust {channel} (edition {edition}; check-only)"
        )
        try:
            result = subprocess.run(command, cwd=ROOT, check=False)
        except OSError as exc:
            raise GuardError(
                f"cannot execute pinned Rustfmt via rustup: {exc}"
            ) from exc
        if result.returncode:
            return result.returncode
    return 0


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Check changed Rust files with the pinned Rustfmt; never rewrite the tree."
    )
    parser.add_argument(
        "--base",
        help="derive the scoped paths from BASE...HEAD (used by the merge queue)",
    )
    parser.add_argument(
        "paths", nargs="*", help="pre-commit supplied repository-relative paths"
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    if arguments.base and arguments.paths:
        _parser().error("--base cannot be combined with explicit paths")
    try:
        paths = (
            _changed_paths(arguments.base)
            if arguments.base
            else _explicit_paths(arguments.paths)
        )
        if not paths:
            print("rustfmt-scope: no Rust or formatting-configuration changes")
            return 0
        if _scope_requires_full_check(paths):
            paths = _all_tracked_rust_files()
        return _run_rustfmt(_pinned_channel(), paths)
    except GuardError as exc:
        print(f"rustfmt-scope: error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
