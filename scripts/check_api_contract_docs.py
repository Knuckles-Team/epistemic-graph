#!/usr/bin/env python3
"""Assert the generated API docs (D7/D8) are current relative to contract/.

Lightweight advisory-shaped gate mirroring `scripts/check_status_page.py`'s
own relationship to `scripts/build_status_page.py`: reuses
`gen_api_docs.py`'s own render functions rather than re-implementing them, so
there is exactly one way this repository turns `contract/` into
`docs/api/*.md` + `docs/openapi.json` + and it is checked, not reimplemented.

Run:  python3 scripts/check_api_contract_docs.py
"""

from __future__ import annotations

import sys
from pathlib import Path

from gen_api_docs import Contract, rendered_files

ROOT = Path(__file__).resolve().parent.parent


def main() -> int:
    contract = Contract()
    files = rendered_files(contract)

    stale = []
    for path, content in files.items():
        if not path.is_file() or path.read_text(encoding="utf-8") != content:
            stale.append(path.relative_to(ROOT))
    if stale:
        print(
            "check_api_contract_docs: FAIL: stale relative to contract/: "
            + ", ".join(str(p) for p in stale)
            + ". Run: python3 scripts/gen_api_docs.py --write",
            file=sys.stderr,
        )
        return 1
    print(
        f"check_api_contract_docs: PASS ({len(files)} generated files match contract/)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
