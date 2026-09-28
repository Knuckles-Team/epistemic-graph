#!/usr/bin/env python3
"""Run a gate script that lives in the sibling agent-utilities checkout.

Several shared gates (lane guard, stub check, sprawl scan) are owned by
agent-utilities and read from a sibling checkout (``$AGENT_UTILITIES_ROOT`` or
``../agent-utilities`` next to this repository's main worktree). A fresh clone
has no sibling, so a missing checkout must not block local commits: outside CI
the gate reports ``SKIPPED`` and passes; in CI (``CI`` set) it fails closed,
because CI is expected to provide the sibling.

Usage: ``python3 scripts/sibling_gate.py <script-name> [args...]``
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def agent_utilities_script(name: str) -> Path:
    """Where the sibling agent-utilities checkout keeps ``scripts/<name>``."""

    configured = os.environ.get("AGENT_UTILITIES_ROOT")
    if configured:
        return Path(configured) / "scripts" / name
    common = subprocess.check_output(
        ["git", "rev-parse", "--path-format=absolute", "--git-common-dir"],
        cwd=ROOT,
        text=True,
    ).strip()
    return Path(common).parent.parent / "agent-utilities" / "scripts" / name


def unavailable(gate: str, reason: str) -> int:
    """Fail closed in CI; skip visibly everywhere else."""

    if os.environ.get("CI"):
        print(f"{gate}: CANNOT RUN in CI: {reason}", file=sys.stderr)
        return 2
    print(
        f"SKIPPED ({gate}): {reason}; set AGENT_UTILITIES_ROOT or clone "
        "agent-utilities next to this repository to run it"
    )
    return 0


def main(argv: list[str]) -> int:
    if not argv:
        print(__doc__, file=sys.stderr)
        return 2
    name, *args = argv
    script = agent_utilities_script(name)
    if not script.is_file():
        return unavailable(name, f"sibling script missing: {script}")
    # Prefer the sibling's own environment: its scripts import agent_utilities.
    sibling_python = script.parents[1] / ".venv" / "bin" / "python"
    python = str(sibling_python) if sibling_python.is_file() else sys.executable
    result = subprocess.run(
        [python, str(script), *args], cwd=ROOT, capture_output=True, text=True
    )
    sys.stdout.write(result.stdout)
    if result.returncode and "ModuleNotFoundError" in result.stderr:
        missing = result.stderr.strip().splitlines()[-1]
        return unavailable(name, f"sibling environment incomplete ({missing})")
    sys.stderr.write(result.stderr)
    return result.returncode


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
