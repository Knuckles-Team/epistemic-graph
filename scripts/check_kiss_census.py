#!/usr/bin/env python3
"""Run the advisory KISS census: one whole-tree run UNIONED with one run per package.

KISS 0.4.10 reports a false clean when more than one path is passed to a single
``check`` invocation, so every call passes exactly one path. That path is the
codebase root KISS analyses, and the choice changes what it can see:

* one tracked FILE per call (the census until 2026-09-22): each file is its own
  one-file codebase, so the cross-file duplication, orphan-module, dependency-
  depth and cycle rules never fire, and the multi-second Rust role scan is paid
  per file (137 minutes in the hosted scanner job);
* the repository root: the whole workspace in one graph -- cross-crate
  duplication and dependency depth become visible, but KISS resolves `crate::`
  paths less precisely across a multi-crate workspace, so some intra-crate
  cycles and depths are lost (and 29 `orphan_module` findings appear for files
  the compiler-derived `orphan-modules` gate proves are compiled);
* one PACKAGE root (`src`, `crates/<name>`) per call: exact intra-crate graphs,
  but no cross-crate view.

The census is the de-duplicated UNION of the whole-tree run and every package
run, so it reports every finding any of the three shapes reports. Measured
2026-09-22 over the 1,920-file manifest: per-file 430, whole-tree 502, per-
package 473, union 515; per-file, whole-tree and per-package are each subsets.
Wall time is about 80 s (per-package runs in parallel).

KISS honours ``.gitignore``; untracked, non-ignored Rust files are passed to
``--ignore`` so the census universe stays the tracked tree.

Exit 0 means the complete advisory census ran (findings are allowed).  Exit 2
means the census could not run or a native report contradicted its exit status.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
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
PACKAGE_WORKERS = 4
_VIOLATION = re.compile(rb"^VIOLATION:([^:]+):(.+?):(\d+):([^:]*):", re.M)


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


def package_roots(paths: list[str]) -> list[str]:
    """One codebase root per package: `src` (the facade) and each `crates/<name>`."""

    roots = set()
    for path in paths:
        parts = path.split("/")
        if parts[0] == "src":
            roots.add("src")
        elif parts[0] == "crates" and len(parts) > 2:
            roots.add("/".join(parts[:2]))
        else:
            fail(f"tracked-source manifest has a path outside src/ and crates/: {path}")
    return sorted(roots)


def census_command(
    kiss_bin: str, ignored: list[str], root: str = CENSUS_ROOT
) -> list[str]:
    """One invocation over exactly one codebase root."""

    command = [kiss_bin, "check", "--config", ".config/kiss.toml", "--lang", "rust"]
    for path in ignored:
        command += ["--ignore", path]
    return [*command, root]


def scan_tree(
    kiss_bin: str, ignored: list[str], env: dict[str, str], root: str = CENSUS_ROOT
) -> ScanResult:
    """Run one codebase root through exactly one pinned KISS process."""

    try:
        result = subprocess.run(
            census_command(kiss_bin, ignored, root),
            cwd=ROOT,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=False,
        )
    except OSError as exc:
        message = f"could not start KISS: {exc}\n".encode()
        return ScanResult(root, 2, message, 0)
    count = sum(line.startswith(b"VIOLATION:") for line in result.stdout.splitlines())
    return ScanResult(root, result.returncode, result.stdout, count)


def findings(result: ScanResult) -> set[tuple[str, str, int, str]]:
    """(rule, repository-relative path, line, unit) for every reported violation."""

    found = set()
    for rule, path, line, unit in _VIOLATION.findall(result.output):
        text = path.decode("utf-8", "replace")
        relative = os.path.relpath(text, ROOT) if os.path.isabs(text) else text
        found.add((rule.decode(), os.path.normpath(relative), int(line), unit.decode()))
    return found


def union_census(
    kiss_bin: str, roots: list[str], ignored: list[str], env: dict[str, str]
) -> set[tuple[str, str, int, str]]:
    """Validate every run, then return the de-duplicated union of their findings."""

    with ThreadPoolExecutor(max_workers=PACKAGE_WORKERS) as executor:
        results = list(
            executor.map(lambda root: scan_tree(kiss_bin, ignored, env, root), roots)
        )
    union: set[tuple[str, str, int, str]] = set()
    for result in results:
        validate(result)
        union |= findings(result)
    return union


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
    roots = [CENSUS_ROOT, *package_roots(paths)]
    union = union_census(kiss_bin, roots, untracked_rust_sources(), env)

    print(
        f"kiss census: {len(paths)} tracked Rust file(s); whole-tree run + "
        f"{len(roots) - 1} package run(s); {len(union)} distinct violation(s); "
        "findings are advisory"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
