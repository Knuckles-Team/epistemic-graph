#!/usr/bin/env python3
"""Assert docs/status.md's displayed totals match its source files.

Lightweight CI gate (wired into .github/workflows/advisory.yml, report-only)
for the Codex/status page: fails when `docs/status.md` is stale relative to
`docs/capabilities.md`, `docs/capabilities.generated.md`, or
`docs/concept_reservations.yaml`. Reuses `scripts/build_status_page.py`'s own
render function rather than re-implementing the parsing/rendering logic — a
second independent implementation is exactly how agent-utilities'
1216/1203/1196 concept-count drift happened.

Run:  python scripts/check_status_page.py
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

from build_status_page import STATUS_PATH, render  # noqa: E402


def main() -> int:
    rendered = render()
    if not STATUS_PATH.is_file():
        print(
            "check_status_page: FAIL: docs/status.md is missing. "
            "Run: python scripts/build_status_page.py --write",
            file=sys.stderr,
        )
        return 1
    current = STATUS_PATH.read_text(encoding="utf-8")
    if current != rendered:
        print(
            "check_status_page: FAIL: docs/status.md is stale relative to "
            "docs/capabilities.md / docs/capabilities.generated.md / "
            "docs/concept_reservations.yaml. "
            "Run: python scripts/build_status_page.py --write",
            file=sys.stderr,
        )
        return 1
    print("check_status_page: PASS (docs/status.md matches its sources)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
