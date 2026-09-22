#!/usr/bin/env python3
"""Run the tracked-tree KISS census once per Rust package root.

KISS 0.4.10 can report a false clean when several paths are supplied to one
invocation.  Running it once for every source file is correct but repeatedly
pays the tool's multi-second Rust-role startup cost.  This runner derives the
complete package-root set from the exact tracked source manifest and scans one
directory per invocation with bounded parallelism.  A directory remains the
single path required by KISS and lets Rust module resolution see generated
support modules that are deliberately absent from the scanner manifest.
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


@dataclass(frozen=True)
class CensusResult:
    root: str
    status: int
    violations: int
    output: str


def load_manifest(repository: Path) -> list[Path]:
    """Return the canonical, NUL-delimited tracked KISS source manifest."""

    result = subprocess.run(
        [sys.executable, "scripts/list_scanner_sources.py", "kiss"],
        cwd=repository,
        check=False,
        capture_output=True,
    )
    if result.returncode != 0:
        message = result.stderr.decode(errors="replace").strip()
        raise RuntimeError(f"source manifest generation failed: {message}")
    paths = [Path(raw.decode()) for raw in result.stdout.split(b"\0") if raw]
    if not paths:
        raise RuntimeError("source manifest contains no tracked Rust files")
    return paths


def package_root(path: Path) -> Path:
    """Map a canonical source path to its independently scanned package root."""

    parts = path.parts
    if parts[0] == "src":
        return Path("src")
    if len(parts) >= 2 and parts[0] == "crates":
        return Path("crates") / parts[1]
    raise RuntimeError(f"source manifest contains an unsupported path: {path}")


def package_roots(repository: Path, paths: list[Path]) -> list[Path]:
    """Validate manifest sources and return their unique package roots."""

    roots: set[Path] = set()
    for relative in paths:
        if not (repository / relative).is_file():
            raise RuntimeError(f"manifest source is missing: {relative}")
        roots.add(package_root(relative))
    return sorted(roots)


def run_group(kiss: str, config: Path, repository: Path, root: Path) -> CensusResult:
    """Run one single-path KISS invocation and validate its result contract."""

    completed = subprocess.run(
        [
            kiss,
            "check",
            "--config",
            str(config),
            "--lang",
            "rust",
            str(repository / root),
        ],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    output = completed.stdout
    violations = sum(line.startswith("VIOLATION:") for line in output.splitlines())
    status = completed.returncode
    if status > 1:
        raise RuntimeError(f"KISS failed on {root} with exit {status}:\n{output}")
    if (status == 0 and violations != 0) or (status == 1 and violations == 0):
        raise RuntimeError(
            f"KISS exit status and report disagree for {root}: "
            f"status={status}, violations={violations}\n{output}"
        )
    return CensusResult(str(root), status, violations, output)


def run_census(repository: Path, jobs: int) -> tuple[int, int]:
    """Run the census and return ``(source_count, violation_count)``."""

    config = repository / ".kiss" / "kiss.toml"
    if not config.is_file():
        raise RuntimeError("missing .kiss/kiss.toml")
    if (repository / ".kissconfig").exists():
        raise RuntimeError(".kissconfig is forbidden")
    kiss = shutil.which("kiss")
    if kiss is None:
        raise RuntimeError("kiss is not installed")
    paths = load_manifest(repository)
    roots = package_roots(repository, paths)
    with ThreadPoolExecutor(max_workers=jobs) as executor:
        results = list(
            executor.map(lambda root: run_group(kiss, config, repository, root), roots)
        )
    return len(paths), sum(result.violations for result in results)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", type=Path, default=ROOT)
    parser.add_argument(
        "--jobs",
        type=int,
        default=min(4, max(1, os.cpu_count() or 1)),
        help="maximum concurrent package-root scans (default: up to four)",
    )
    args = parser.parse_args(argv)
    if args.jobs < 1:
        parser.error("--jobs must be positive")
    repository = args.repository.resolve()
    try:
        sources, violations = run_census(repository, args.jobs)
    except RuntimeError as exc:
        print(f"KISS census: {exc}", file=sys.stderr)
        return 2
    print(
        f"KISS census: {sources} tracked Rust file(s), "
        f"{violations} violation(s); findings are advisory"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
