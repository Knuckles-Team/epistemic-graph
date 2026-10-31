"""Crate-split manifest loading and file-to-crate ownership (MEDULLA-CONTRACT-R056)."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import tomllib

DELETE = "delete"
DEFAULT_MANIFEST = Path(__file__).resolve().parent / "manifest.toml"


@dataclass(frozen=True)
class Crate:
    name: str
    kind: str
    exists: bool
    allowed_deps: tuple[str, ...]
    paths: tuple[str, ...]


@dataclass(frozen=True)
class Manifest:
    crates: dict[str, Crate]
    delete_paths: tuple[str, ...]
    delete_enforced: bool
    extra: dict[str, object] = field(default_factory=dict)

    def entries(self) -> list[tuple[str, str]]:
        """All (path entry, owner crate) pairs, the delete set owning as `delete`."""
        pairs = [(p, c.name) for c in self.crates.values() for p in c.paths]
        return pairs + [(p, DELETE) for p in self.delete_paths]


def load_manifest(path: Path = DEFAULT_MANIFEST) -> Manifest:
    data = tomllib.loads(path.read_text(encoding="utf-8"))
    crates = {
        name: Crate(
            name=name,
            kind=str(body.get("kind", "")),
            exists=bool(body.get("exists", False)),
            allowed_deps=tuple(body.get("allowed_deps", [])),
            paths=tuple(body.get("paths", [])),
        )
        for name, body in data.get("crates", {}).items()
    }
    delete = data.get("delete", {})
    return Manifest(
        crates=crates,
        delete_paths=tuple(delete.get("paths", {}).get("entries", [])),
        delete_enforced=bool(delete.get("enforce", False)),
    )


def entry_matches(entry: str, rel: str) -> bool:
    """Whether manifest `entry` covers the repo-relative file `rel`."""
    if entry.endswith(".rs"):
        return rel == entry
    return rel == entry + ".rs" or rel.startswith(entry + "/")


def owners_of(manifest: Manifest, rel: str) -> list[str]:
    """Owning crates of `rel` at the longest matching entry length (0, 1 or more)."""
    best = 0
    found: list[str] = []
    for entry, crate in manifest.entries():
        if not entry_matches(entry, rel):
            continue
        if len(entry) > best:
            best, found = len(entry), [crate]
        elif len(entry) == best and crate not in found:
            found.append(crate)
    return found


def list_src_files(root: Path) -> list[str]:
    src = root / "src"
    return sorted(p.relative_to(root).as_posix() for p in src.rglob("*.rs"))


def build_owner_map(manifest: Manifest, files: list[str]) -> dict[str, str | None]:
    """Unambiguous owner per file; None when unassigned or ambiguous."""
    result: dict[str, str | None] = {}
    for rel in files:
        found = owners_of(manifest, rel)
        result[rel] = found[0] if len(found) == 1 else None
    return result
