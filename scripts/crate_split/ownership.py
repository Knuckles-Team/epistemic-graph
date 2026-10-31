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
    level: int
    paths: tuple[str, ...]


@dataclass(frozen=True)
class Manifest:
    crates: dict[str, Crate]
    delete_paths: tuple[str, ...]
    delete_enforced: bool
    scaffold_done: bool = False
    extra: dict[str, object] = field(default_factory=dict)

    def level(self, crate: str) -> int:
        """Layer of a crate; the delete set sits outside the layering."""
        return self.crates[crate].level if crate in self.crates else -1

    def may_depend(self, crate: str, dep: str) -> bool:
        """Whether `crate` may use `dep`: itself or any strictly lower layer."""
        if crate == dep or crate == DELETE:
            return True
        if dep == DELETE or dep not in self.crates:
            return False
        return self.crates[dep].level < self.crates[crate].level

    def allowed_deps(self, crate: str) -> list[str]:
        return sorted(
            c for c in self.crates if c != crate and self.may_depend(crate, c)
        )

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
            level=int(body["level"]),
            paths=tuple(body.get("paths", [])),
        )
        for name, body in data.get("crates", {}).items()
    }
    delete = data.get("delete", {})
    return Manifest(
        crates=crates,
        delete_paths=tuple(delete.get("paths", {}).get("entries", [])),
        delete_enforced=bool(delete.get("enforce", False)),
        scaffold_done=bool(data.get("scaffold", {}).get("done", False)),
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


DEFAULT_PLACEMENT = Path(__file__).resolve().parent / "placement.toml"


def load_placement(path: Path = DEFAULT_PLACEMENT) -> dict[str, str]:
    """File -> crate table written by resolve.py (the file-to-crate authority)."""
    data = tomllib.loads(path.read_text(encoding="utf-8"))
    return dict(data.get("files", {}))


def load_test_moves(path: Path = DEFAULT_PLACEMENT) -> list[str]:
    data = tomllib.loads(path.read_text(encoding="utf-8"))
    return list(data.get("test_moves", {}).get("files", []))
