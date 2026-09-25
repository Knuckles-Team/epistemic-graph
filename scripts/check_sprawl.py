#!/usr/bin/env python3
"""Run the fleet sprawl checks with EG's verified generated-artifact ledger.

The Go and JavaScript packages must ship their generated method codec.  Only a
binary whose bytes match the reviewed ledger pin may pass the binary size cap;
all other fleet sprawl findings remain blocking.
"""

from __future__ import annotations

import os
import re
import runpy
import subprocess
import sys
from pathlib import Path

try:
    from generated_artifacts import verify
except ModuleNotFoundError:  # imported as scripts.check_sprawl by focused tests
    from scripts.generated_artifacts import verify

ROOT = Path(__file__).resolve().parents[1]
LARGE_BINARY = re.compile(r"^tracked binary > \d+ bytes: (.+) \(\d+ bytes\)$")


def fleet_checker() -> Path:
    """Resolve the same sibling AU checkout used by the pre-commit hook."""
    configured = os.environ.get("AGENT_UTILITIES_ROOT")
    if configured:
        return Path(configured) / "scripts/check_sprawl.py"
    common = subprocess.check_output(
        ["git", "rev-parse", "--path-format=absolute", "--git-common-dir"],
        cwd=ROOT,
        text=True,
    ).strip()
    return Path(common).parent.parent / "agent-utilities/scripts/check_sprawl.py"


def remaining(findings: list[str], verified: set[str]) -> list[str]:
    """Keep every finding except a size finding for a byte-verified binary."""
    return [
        finding
        for finding in findings
        if (match := LARGE_BINARY.fullmatch(finding)) is None
        or match.group(1) not in verified
    ]


def main() -> int:
    checker = fleet_checker()
    if not checker.is_file():
        print(
            f"Anti-sprawl gate FAILED: fleet checker missing: {checker}",
            file=sys.stderr,
        )
        return 1
    findings = runpy.run_path(str(checker))["scan"](ROOT)
    verified, ledger_problems = verify(ROOT)
    failures = [*remaining(findings, verified), *ledger_problems]
    if failures:
        print("Anti-sprawl gate FAILED:", file=sys.stderr)
        for failure in sorted(failures):
            print(f"  - {failure}", file=sys.stderr)
        return 1
    print("OK: no sprawl/hygiene violations; generated artifacts match their pins.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
