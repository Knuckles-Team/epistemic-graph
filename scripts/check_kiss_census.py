#!/usr/bin/env python3
"""Run the advisory KISS census with bounded process-level parallelism.

KISS 0.4.10 can report a false clean when more than one path is passed to a
single ``check`` invocation.  This adapter therefore preserves one process per
tracked Rust path, while allowing a small number of independent processes to
run concurrently.  Each child is restricted to one Rayon worker so the census
cannot multiply the host CPU count by the process count.

Exit 0 means the complete advisory census ran (findings are allowed).  Exit 2
means the census could not run or a native report contradicted its exit status.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from pathlib import Path
from typing import NoReturn

from scanner_contract import ScannerContractError, load_contract, sanitized_env

ROOT = Path(__file__).resolve().parent.parent
MAX_WORKERS = 4


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


def worker_count(raw: str | None, available_cpus: int | None = None) -> int:
    """Resolve a deliberately small, operator-reducible worker bound."""

    cpus = available_cpus
    if cpus is None:
        try:
            cpus = len(os.sched_getaffinity(0))
        except AttributeError:
            cpus = os.cpu_count() or 1
    default = min(MAX_WORKERS, max(1, cpus))
    if raw is None:
        return default
    try:
        requested = int(raw)
    except ValueError as exc:
        raise ValueError("KISS_CENSUS_WORKERS must be an integer") from exc
    if not 1 <= requested <= MAX_WORKERS:
        raise ValueError(f"KISS_CENSUS_WORKERS must be between 1 and {MAX_WORKERS}")
    return min(requested, max(1, cpus))


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


def scan_one(kiss_bin: str, path: str, env: dict[str, str]) -> ScanResult:
    """Run exactly one path through exactly one pinned KISS process."""

    try:
        result = subprocess.run(
            [
                kiss_bin,
                "check",
                "--config",
                ".kiss/kiss.toml",
                "--lang",
                "rust",
                path,
            ],
            cwd=ROOT,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=False,
        )
    except OSError as exc:
        return ScanResult(path, 2, f"could not start KISS: {exc}\n".encode(), 0)
    count = sum(line.startswith(b"VIOLATION:") for line in result.stdout.splitlines())
    return ScanResult(path, result.returncode, result.stdout, count)


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


def scan_paths(
    kiss_bin: str, paths: list[str], env: dict[str, str], workers: int
) -> int:
    """Scan all paths while retaining at most ``workers`` native reports."""

    total = 0
    next_path = iter(paths)
    with ThreadPoolExecutor(max_workers=workers) as executor:
        pending: set[Future[ScanResult]] = set()
        for _ in range(workers):
            try:
                path = next(next_path)
            except StopIteration:
                break
            pending.add(executor.submit(scan_one, kiss_bin, path, env))

        while pending:
            completed, pending = wait(pending, return_when=FIRST_COMPLETED)
            for future in completed:
                total += validate(future.result())
                try:
                    path = next(next_path)
                except StopIteration:
                    continue
                pending.add(executor.submit(scan_one, kiss_bin, path, env))
    return total


def _resolve_binary(raw: str) -> str:
    resolved = shutil.which(raw)
    if resolved is None:
        fail("KISS is not installed; install the pinned tool before running hooks")
    return resolved


def main() -> int:
    try:
        expected = load_contract().kiss_version
    except ScannerContractError as exc:
        fail(f"invalid scanner contract: {exc}")

    kiss_bin = _resolve_binary(os.environ.get("KISS_BIN", "kiss"))
    env = sanitized_env(preserve_index=True)
    # A KISS process is already the unit of parallelism.  Prevent its internal
    # Rayon pool from multiplying the bounded process count by the host's CPUs.
    env["RAYON_NUM_THREADS"] = "1"

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
    if not (ROOT / ".kiss/kiss.toml").is_file():
        fail("missing .kiss/kiss.toml")
    if (ROOT / ".kissconfig").exists() or (ROOT / ".kissconfig").is_symlink():
        fail(".kissconfig is forbidden because it disables measured rules")

    paths = _manifest(env)
    try:
        workers = worker_count(os.environ.get("KISS_CENSUS_WORKERS"))
    except ValueError as exc:
        fail(str(exc))

    total = scan_paths(kiss_bin, paths, env, workers)

    print(
        f"kiss census: {len(paths)} tracked Rust file(s), {total} violation(s); "
        f"findings are advisory (workers={workers}, child_threads=1)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
