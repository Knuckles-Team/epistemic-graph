#!/usr/bin/env python3
"""Check the bounded, source-anchored scaling documentation register.

The checker deliberately validates only claims in ``scale_claims.md``. It does
not infer deployment or benchmark results from prose. A source anchor plus a
required source phrase makes stale documentation fail closed, while the
status vocabulary keeps implementation, lab, live, and certification evidence
distinct.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CLAIMS = ROOT / "docs/architecture/scale_claims.md"
STATUSES = frozenset(
    {"DESIGNED", "IMPLEMENTED", "UNIT-PROVEN", "LAB-PROVEN", "LIVE", "1M-CERTIFIED"}
)


def _cells(line: str) -> list[str] | None:
    if not line.lstrip().startswith("|"):
        return None
    cells = [cell.strip().strip("`") for cell in line.strip().strip("|").split("|")]
    if len(cells) != 5 or all(set(cell) <= {"-", ":", " "} for cell in cells):
        return None
    return cells


def _claims(text: str) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    header_seen = False
    for line in text.splitlines():
        cells = _cells(line)
        if cells is None:
            continue
        if cells[0].lower() == "id" and cells[1].lower() == "status":
            header_seen = True
            continue
        if not header_seen:
            continue
        rows.append(
            {
                "id": cells[0],
                "status": cells[1],
                "anchor": cells[2],
                "required": cells[3],
                "evidence": cells[4],
            }
        )
    return rows


def _claim_field_errors(row: dict[str, str]) -> list[str]:
    """Errors visible in one claim row's own cells."""
    errors = []
    if row["status"] not in STATUSES:
        errors.append(f"{row['id']}: unknown status {row['status']!r}")
    if not row["evidence"]:
        errors.append(f"{row['id']}: evidence/scope is empty")
    return errors


def _claim_anchor_errors(root: Path, row: dict[str, str]) -> list[str]:
    """Errors from resolving one claim's `path#text` source anchor."""
    claim_id, anchor = row["id"], row["anchor"]
    if "#" not in anchor:
        return [f"{claim_id}: source anchor must be path#text"]
    relative, needle = anchor.split("#", 1)
    source = (root / relative).resolve()
    try:
        source.relative_to(root.resolve())
    except ValueError:
        return [f"{claim_id}: source anchor escapes repository: {relative}"]
    if not source.is_file():
        return [f"{claim_id}: source file is missing: {relative}"]
    source_text = source.read_text(encoding="utf-8")
    errors = []
    if needle not in source_text:
        errors.append(f"{claim_id}: anchor text is missing from {relative}: {needle!r}")
    if row["required"] and row["required"] not in source_text:
        errors.append(
            f"{claim_id}: required source text is missing from {relative}: {row['required']!r}"
        )
    if row["status"] in {"LIVE", "1M-CERTIFIED"} and "reports/" not in row["evidence"]:
        errors.append(
            f"{claim_id}: {row['status']} requires a versioned reports/ evidence reference"
        )
    return errors


def check_claims(root: Path = ROOT, claims_path: Path = DEFAULT_CLAIMS) -> list[str]:
    """Return deterministic errors for a claim register, without executing code."""

    try:
        text = claims_path.read_text(encoding="utf-8")
    except OSError as exc:
        return [f"cannot read claims register {claims_path}: {exc}"]

    rows = _claims(text)
    if not rows:
        return ["claims register has no five-column claim table"]
    errors: list[str] = []
    seen: set[str] = set()
    for row in rows:
        claim_id = row["id"]
        if not claim_id or claim_id in seen:
            errors.append(f"duplicate or empty claim id: {claim_id!r}")
        seen.add(claim_id)
        errors.extend(_claim_field_errors(row))
        errors.extend(_claim_anchor_errors(root, row))
    return errors


def main(argv: list[str] | None = None) -> int:
    claims_path = Path(argv[0]).resolve() if argv else DEFAULT_CLAIMS
    errors = check_claims(ROOT, claims_path)
    if errors:
        for error in errors:
            print(f"scale documentation claim check: {error}", file=sys.stderr)
        return 1
    print(f"scale documentation claim check: {claims_path.relative_to(ROOT)} OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
