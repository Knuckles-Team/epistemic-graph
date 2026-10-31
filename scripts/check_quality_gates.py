#!/usr/bin/env python3
"""Combined quality gate: every staged scanner in one run, one verdict.

Runs the staged complexity (cccc), KISS and Dupehound checks, the jscpd
differential scan of the current changes and the tracked-file privacy scan
together. Every scanner runs even when an earlier one fails, so one run shows
the whole picture. The exit code is 0 only when every scanner exits 0;
otherwise it is 1. Each scanner keeps its own fail-closed behaviour (a scanner
that cannot run exits non-zero and so fails the combined gate).

The commands are the ``entry`` of the matching hooks in
``.config/pre-commit.yaml``; ``tests/test_check_quality_gates.py`` keeps the
two in step. Run it with ``python3 scripts/check_quality_gates.py`` or the
manual ``quality-gates`` pre-commit hook.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

# (pre-commit hook id, argv). Order is report order only.
SCANNERS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("complexity-staged", ("python3", "scripts/check_complexity_staged.py")),
    ("kiss-changed-rust", ("bash", "scripts/check_kiss_staged.sh")),
    ("dupehound-changed-functions", ("python3", "scripts/check_dupehound.py")),
    (
        "jscpd-differential",
        (
            "bash",
            "-c",
            'base="$(bash scripts/gate_base_ref.sh)" && '
            'exec python3 scripts/check_duplication.py enforce --base-ref "$base"',
        ),
    ),
    ("guardrail-tracked-privacy", ("python3", "scripts/check_tracked_privacy.py")),
)


def run_gates(
    scanners: tuple[tuple[str, tuple[str, ...]], ...] = SCANNERS,
    cwd: Path = ROOT,
) -> tuple[int, list[tuple[str, int]]]:
    """Run every scanner and return (combined exit code, per-scanner codes)."""

    results: list[tuple[str, int]] = []
    for name, argv in scanners:
        print(f"== {name}", flush=True)
        try:
            code = subprocess.run(argv, cwd=cwd, check=False).returncode
        except OSError as exc:
            print(f"{name}: cannot run: {exc}", file=sys.stderr)
            code = 2
        results.append((name, code))
    print("== quality gates")
    for name, code in results:
        print(f"{'PASS' if code == 0 else 'FAIL'} {name} (exit {code})")
    combined = 0 if results and all(code == 0 for _, code in results) else 1
    print("PASS all quality gates" if combined == 0 else "FAIL quality gates")
    return combined, results


def main() -> int:
    return run_gates()[0]


if __name__ == "__main__":
    sys.exit(main())
