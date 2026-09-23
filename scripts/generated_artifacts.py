#!/usr/bin/env python3
"""Reviewed, byte-verified generated artifacts (`.config/generated-artifacts.toml`).

Some generated files must be committed: Go's ``//go:embed`` cannot reach outside
its module, so the eg2 method-body codec ships as a committed ``.wasm`` in each
client. Hygiene gates would otherwise reject such a file (too large a binary,
not inspectable as text). The ledger is how a gate tells a REVIEWED generated
artifact from an arbitrary binary:

* every entry names the path, the exact sha256 of the committed bytes, the
  command that reproduces them (``reproducer``), the CI job that runs that
  command (``proven_by``) and a ``reason``;
* a gate skips its binary checks for a listed path ONLY when the file's sha256
  equals the pinned value;
* a listed file that is missing, or whose bytes changed, is a hard failure --
  the ledger detects its own rot instead of silently widening, so it is not a
  ratchet and not a path allowlist.

``tests/test_generated_artifacts_ledger.py`` additionally requires every
entry's reproducer to run in its ``proven_by`` job, so no entry outlives the
proof that its bytes are reproducible.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path

import tomllib

LEDGER = Path(".config/generated-artifacts.toml")
FIELDS = ("path", "sha256", "reproducer", "proven_by", "reason")


class LedgerError(ValueError):
    """The ledger itself is malformed (fail closed: no entry is honoured)."""


@dataclass(frozen=True)
class Entry:
    path: str
    sha256: str
    reproducer: str
    proven_by: str
    reason: str


def _entry(raw: object, index: int) -> Entry:
    if not isinstance(raw, dict) or set(raw) != set(FIELDS):
        raise LedgerError(f"entry {index} must have exactly the fields {FIELDS}")
    if not all(isinstance(raw[field], str) and raw[field].strip() for field in FIELDS):
        raise LedgerError(f"entry {index}: every field must be a non-empty string")
    digest = raw["sha256"]
    if len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
        raise LedgerError(f"entry {index}: sha256 must be 64 lowercase hex digits")
    relative = Path(raw["path"])
    if relative.is_absolute() or ".." in relative.parts:
        raise LedgerError(f"entry {index}: path must be repository-relative")
    return Entry(**{field: raw[field] for field in FIELDS})


def load(root: Path) -> dict[str, Entry]:
    """The ledger's entries by repository-relative POSIX path ({} when absent)."""
    ledger = root / LEDGER
    if not ledger.is_file():
        return {}
    try:
        document = tomllib.loads(ledger.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, tomllib.TOMLDecodeError) as exc:
        raise LedgerError(f"{LEDGER} is unreadable: {exc}") from exc
    raws = document.get("artifact", [])
    if set(document) - {"artifact"} or not isinstance(raws, list):
        raise LedgerError(f"{LEDGER} holds only [[artifact]] tables")
    entries = [_entry(raw, index) for index, raw in enumerate(raws)]
    by_path = {Path(entry.path).as_posix(): entry for entry in entries}
    if len(by_path) != len(entries):
        raise LedgerError(f"{LEDGER} lists a path twice")
    return by_path


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def verify(root: Path) -> tuple[set[str], list[str]]:
    """(paths whose bytes match their pin, problems). A problem is a gate failure."""
    try:
        entries = load(root)
    except LedgerError as exc:
        return set(), [f"generated-artifact ledger: {exc}"]
    verified: set[str] = set()
    problems: list[str] = []
    for rel, entry in sorted(entries.items()):
        path = root / rel
        if not path.is_file() or path.is_symlink():
            problems.append(f"generated artifact listed in {LEDGER} is missing: {rel}")
        elif sha256(path) != entry.sha256:
            problems.append(
                f"generated artifact {rel} does not match its pinned sha256 in "
                f"{LEDGER} -- regenerate with `{entry.reproducer}` and update the pin"
            )
        else:
            verified.add(rel)
    return verified, problems
