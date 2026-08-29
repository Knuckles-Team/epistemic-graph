#!/usr/bin/env python3
"""Emit the tracked source universe for the native scanner censuses.

The release workflow and local pre-push hooks must feed the same files to a
scanner.  Keeping the Git/index handling and suffix policy in one small,
standard-library-only helper prevents the two surfaces from drifting (and
ensures an empty or malformed Git result is never mistaken for a clean scan).

Output is NUL-delimited repository-relative paths so filenames cannot be split
on whitespace.  The helper intentionally lists tracked files only; callers
must not widen a census with ignored or untracked build output.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import NoReturn

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent))

from scanner_contract import (  # noqa: E402
    CCCC_SUPPORTED_SUFFIXES,
    ScannerContract,
    ScannerContractError,
    load_contract,
    relative_to_root,
    run_git,
)


def fail(message: str) -> NoReturn:
    """Report an environment failure and use the scanner gate exit code."""

    print(f"scanner source manifest: CANNOT RUN: {message}", file=sys.stderr)
    raise SystemExit(2)


def _git_files() -> list[str]:
    try:
        result = run_git(("ls-files", "-z"), cwd=ROOT, preserve_index=True)
    except RuntimeError as exc:
        fail(str(exc))
    if result.returncode != 0:
        fail(f"git ls-files failed: {(result.stderr or '').strip()[:500]}")
    if not isinstance(result.stdout, str):
        fail("git ls-files returned no text output")
    paths = []
    for raw in result.stdout.split("\x00"):
        if not raw:
            continue
        try:
            paths.append(relative_to_root(raw, ROOT))
        except ValueError as exc:
            fail(f"git reported an unsafe path: {exc}")
    return paths


def _is_cccc_source(path: str, config: ScannerContract) -> bool:
    return Path(
        path
    ).suffix.lower() in CCCC_SUPPORTED_SUFFIXES and not config.is_excluded(path)


def _is_kiss_source(path: str, config: ScannerContract) -> bool:
    return (
        Path(path).suffix.lower() == ".rs"
        and (path.startswith("src/") or path.startswith("crates/"))
        and not config.is_excluded(path)
    )


def select(paths: list[str], scanner: str, config: ScannerContract) -> list[str]:
    """Filter repository-relative tracked paths for one scanner census."""

    if scanner == "cccc":
        predicate = _is_cccc_source
    elif scanner == "kiss":
        predicate = _is_kiss_source
    else:
        fail(f"unsupported scanner {scanner!r}")
    return sorted(path for path in paths if predicate(path, config))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("scanner", choices=("cccc", "kiss"))
    args = parser.parse_args(argv)
    try:
        config = load_contract()
    except ScannerContractError as exc:
        fail(str(exc))
    for path in select(_git_files(), args.scanner, config):
        sys.stdout.buffer.write(path.encode("utf-8") + b"\0")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
