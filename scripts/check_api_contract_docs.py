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

from gen_api_docs import Contract, check_rendered, rendered_files


def main() -> int:
    return check_rendered(rendered_files(Contract()), "check_api_contract_docs")


if __name__ == "__main__":
    raise SystemExit(main())
