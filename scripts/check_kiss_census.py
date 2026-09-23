#!/usr/bin/env python3
"""Run the advisory KISS census as ONE whole-tree invocation.

KISS 0.4.10 reports a false clean when more than one path is passed to a
single ``check`` invocation, so every call passes exactly one path.  The
census used to pass each tracked file as its own path: that treats every file
as a separate one-file codebase (the cross-file duplication, orphan-module,
dependency-depth and cycle rules can never fire) and pays KISS's multi-second
Rust role scan once per file -- 137 minutes of the hosted scanner job.  One
``kiss check .`` over the repository root analyses the whole tree once.

Measured 2026-09-22 over the 1,920-file manifest: the per-file census found
430 findings, every one of which the whole-tree run also reports; the
whole-tree run adds 72 cross-file findings (31 orphan modules, 29
duplications, 12 dependency-depth, 1 cycle).  KISS honours ``.gitignore``;
untracked, non-ignored Rust files are passed to ``--ignore`` so the census
universe stays the tracked tree.

Exit 0 means the complete advisory census ran (findings are allowed).  Exit 2
means the census could not run or a native report contradicted its exit status.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import NoReturn

from scanner_contract import (
    ScannerContractError,
    load_contract,
    run_git,
    sanitized_env,
)

ROOT = Path(__file__).resolve().parent.parent
CENSUS_ROOT = "."


@dataclass(frozen=True)
class ScanResult:
    """Validated inputs from one native KISS process."""

    path: str
    status: int
    output: bytes
    violation_count: int


def fail(message: str, output: bytes | None = None) -> NoReturn:
    """Report a fail-closed census error using the gate's exit contract."""

    if output:
        sys.stderr.buffer.write(output)
        if not output.endswith(b"\n"):
            sys.stderr.buffer.write(b"\n")
    print(f"kiss census: {message}", file=sys.stderr)
    raise SystemExit(2)


def _decode_manifest(output: bytes) -> list[str]:
    """Decode and validate the NUL-delimited tracked-source response."""

    if not output:
        fail("no tracked Rust source files")
    records = output.split(b"\0")
    if records[-1] != b"":
        fail("tracked-source manifest is not NUL terminated")
    try:
        paths = [record.decode("utf-8") for record in records[:-1]]
    except UnicodeDecodeError as exc:
        fail(f"tracked-source manifest is not UTF-8: {exc}")
    if not paths or any(not path for path in paths):
        fail("tracked-source manifest contains an empty path")
    return paths


def _manifest(env: dict[str, str]) -> list[str]:
    try:
        result = subprocess.run(
            [sys.executable, "scripts/list_scanner_sources.py", "kiss"],
            cwd=ROOT,
            env=env,
            capture_output=True,
            check=False,
        )
    except OSError as exc:
        fail(f"could not start tracked-source manifest: {exc}")
    if result.returncode != 0:
        fail("tracked-source manifest failed", result.stderr or result.stdout)
    return _decode_manifest(result.stdout)


def untracked_rust_sources() -> list[str]:
    """Untracked, non-ignored Rust files: outside the tracked census universe."""

    try:
        result = run_git(
            ["ls-files", "-z", "--others", "--exclude-standard", "--", "*.rs"],
            cwd=ROOT,
        )
    except RuntimeError as exc:
        fail(str(exc))
    if result.returncode != 0:
        fail("could not list untracked Rust sources", result.stderr.encode())
    return [path for path in result.stdout.split("\0") if path]


def census_command(kiss_bin: str, ignored: list[str]) -> list[str]:
    """The single whole-tree invocation: exactly one path, the repository root."""

    command = [kiss_bin, "check", "--config", ".config/kiss.toml", "--lang", "rust"]
    for path in ignored:
        command += ["--ignore", path]
    return [*command, CENSUS_ROOT]


def scan_tree(kiss_bin: str, ignored: list[str], env: dict[str, str]) -> ScanResult:
    """Run the whole tracked tree through exactly one pinned KISS process."""

    try:
        result = subprocess.run(
            census_command(kiss_bin, ignored),
            cwd=ROOT,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=False,
        )
    except OSError as exc:
        message = f"could not start KISS: {exc}\n".encode()
        return ScanResult(CENSUS_ROOT, 2, message, 0)
    count = sum(line.startswith(b"VIOLATION:") for line in result.stdout.splitlines())
    return ScanResult(CENSUS_ROOT, result.returncode, result.stdout, count)


def validate(result: ScanResult) -> int:
    """Validate native status/report agreement and return its finding count."""

    if b"Unknown config key" in result.output:
        fail("KISS rejected a config key", result.output)
    if result.status not in (0, 1):
        fail(f"KISS failed on {result.path} with exit {result.status}", result.output)
    if (result.status == 0 and result.violation_count != 0) or (
        result.status == 1 and result.violation_count == 0
    ):
        fail(f"exit status and report disagree for {result.path}", result.output)
    return result.violation_count


def _resolve_binary(raw: str) -> str:
    resolved = shutil.which(raw)
    if resolved is None:
        fail("KISS is not installed; install the pinned tool before running hooks")
    return resolved


def _require_pinned_version(kiss_bin: str, env: dict[str, str]) -> None:
    try:
        expected = load_contract().kiss_version
    except ScannerContractError as exc:
        fail(f"invalid scanner contract: {exc}")
    try:
        version = subprocess.run(
            [kiss_bin, "--version"],
            cwd=ROOT,
            env=env,
            capture_output=True,
            check=False,
        )
    except OSError as exc:
        fail(f"could not start kiss --version: {exc}")
    if version.returncode != 0:
        fail("kiss --version failed", version.stderr or version.stdout)
    got = version.stdout.decode("utf-8", errors="replace").strip()
    if got != f"kiss {expected}":
        fail(f"expected kiss {expected}, got {got}")


def main() -> int:
    kiss_bin = _resolve_binary(os.environ.get("KISS_BIN", "kiss"))
    env = sanitized_env(preserve_index=True)
    _require_pinned_version(kiss_bin, env)
    if not (ROOT / ".config/kiss.toml").is_file():
        fail("missing .config/kiss.toml")
    if (ROOT / ".kissconfig").exists() or (ROOT / ".kissconfig").is_symlink():
        fail(".kissconfig is forbidden because it disables measured rules")

    paths = _manifest(env)
    total = validate(scan_tree(kiss_bin, untracked_rust_sources(), env))

    print(
        f"kiss census: {len(paths)} tracked Rust file(s) in one whole-tree run, "
        f"{total} violation(s); findings are advisory"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
