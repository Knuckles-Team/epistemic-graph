#!/usr/bin/env python3
"""Check the real crate graph against the crate-split layering (MEDULLA-CONTRACT-R056).

Fails on: (a) a crate depending on a manifest crate that is not strictly below it in the
layering, (b) a dependency edge between two sibling handler or wire crates, (c) a
remaining contract module once the manifest's delete set is enforced, (d) a placement
path that no longer exists, or a `src/` file assigned twice or never. Without a
placement file the manifest path entries are checked instead. Crates not created yet are
skipped for (a) and (b).
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import tomllib

sys.path.insert(0, str(Path(__file__).resolve().parent))

from crate_split.ownership import (
    DEFAULT_MANIFEST,
    DEFAULT_PLACEMENT,
    DELETE,
    Manifest,
    entry_matches,
    list_src_files,
    load_manifest,
    load_placement,
    owners_of,
)

ROOT_CRATE = "medulla-engine"
DEP_TABLES = ("dependencies", "dev-dependencies", "build-dependencies")


def cargo_path(root: Path, name: str) -> Path:
    """Cargo.toml of a crate; the facade is the root package."""
    return (
        root / "Cargo.toml"
        if name == ROOT_CRATE
        else root / "crates" / name / "Cargo.toml"
    )


def dep_names(table: dict) -> set[str]:
    names = set()
    for key, spec in table.items():
        names.add(
            spec["package"] if isinstance(spec, dict) and "package" in spec else key
        )
    return names


def read_deps(cargo_toml: Path) -> set[str]:
    """Every dependency name in all dependency tables, target-specific included."""
    data = tomllib.loads(cargo_toml.read_text(encoding="utf-8"))
    found: set[str] = set()
    for table in DEP_TABLES:
        found |= dep_names(data.get(table, {}))
    for target in data.get("target", {}).values():
        for table in DEP_TABLES:
            found |= dep_names(target.get(table, {}))
    return found


def existing_deps(manifest: Manifest, root: Path) -> dict[str, set[str]]:
    """Manifest-crate dependencies of every manifest crate that exists on disk."""
    result: dict[str, set[str]] = {}
    for name in manifest.crates:
        path = cargo_path(root, name)
        if path.is_file():
            result[name] = read_deps(path) & set(manifest.crates)
    return result


def check_allowed(manifest: Manifest, deps: dict[str, set[str]]) -> list[str]:
    problems = []
    for name, found in sorted(deps.items()):
        for dep in sorted(d for d in found if not manifest.may_depend(name, d)):
            problems.append(
                f"{name} depends on {dep}, which is not below it in the layering"
            )
    return problems


def check_siblings(manifest: Manifest, deps: dict[str, set[str]]) -> list[str]:
    """handler_crates_have_no_sibling_edges (wire is a sibling of the handlers)."""
    handlers = {n for n, c in manifest.crates.items() if c.kind in ("handler", "wire")}
    return [
        f"handler crate {name} depends on sibling handler crate {dep}"
        for name, found in sorted(deps.items())
        if name in handlers
        for dep in sorted(found & handlers - {name})
    ]


def check_scaffold(manifest: Manifest, root: Path) -> list[str]:
    """Once the scaffold is done every manifest crate must exist."""
    if not manifest.scaffold_done:
        return []
    return [
        f"scaffold incomplete: crate {name} is missing"
        for name in sorted(manifest.crates)
        if not cargo_path(root, name).is_file()
    ]


def check_no_contract_modules(manifest: Manifest, root: Path) -> list[str]:
    """no_contract_modules: nothing from the delete set may remain once enforced."""
    if not manifest.delete_enforced:
        return []
    files = list_src_files(root)
    return [
        f"contract module still present: {rel}"
        for rel in files
        if any(entry_matches(e, rel) for e in manifest.delete_paths)
    ]


def dangling_entries(manifest: Manifest, root: Path, files: list[str]) -> list[str]:
    problems = []
    for entry, crate in manifest.entries():
        if any(entry_matches(entry, rel) for rel in files):
            continue
        if not (root / entry).exists() and not (root / (entry + ".rs")).exists():
            problems.append(f"manifest path does not exist: {entry} ({crate})")
    return problems


def check_manifest_ownership(manifest: Manifest, root: Path) -> list[str]:
    files = list_src_files(root)
    problems = dangling_entries(manifest, root, files)
    for rel in files:
        found = owners_of(manifest, rel)
        if not found:
            problems.append(f"src file assigned to no crate: {rel}")
        elif len(found) > 1:
            problems.append(
                f"src file assigned twice: {rel} -> {', '.join(sorted(found))}"
            )
    return problems


def crate_dir_of(rel: str) -> str | None:
    parts = rel.split("/")
    return parts[1] if parts[0] == "crates" and len(parts) > 2 else None


def check_placement(
    manifest: Manifest, root: Path, placement: dict[str, str]
) -> list[str]:
    """Every src file is placed once, every placed path exists, crates are known."""
    problems = []
    for rel, crate in sorted(placement.items()):
        if not (root / rel).is_file():
            problems.append(f"placement path does not exist: {rel}")
        if crate != DELETE and crate not in manifest.crates:
            problems.append(f"placement names unknown crate {crate}: {rel}")
        if crate_dir_of(rel) not in (None, crate):
            problems.append(f"placement puts {rel} in {crate}, not its crate directory")
    problems += [
        f"src file assigned to no crate: {rel}"
        for rel in list_src_files(root)
        if rel not in placement
    ]
    return problems


def check_ownership(
    manifest: Manifest, root: Path, placement: Path | None
) -> list[str]:
    if placement is not None and placement.is_file():
        return check_placement(manifest, root, load_placement(placement))
    return check_manifest_ownership(manifest, root)


def run_checks(
    manifest: Manifest, root: Path, placement: Path | None = None
) -> list[str]:
    deps = existing_deps(manifest, root)
    return (
        check_allowed(manifest, deps)
        + check_siblings(manifest, deps)
        + check_scaffold(manifest, root)
        + check_no_contract_modules(manifest, root)
        + check_ownership(manifest, root, placement)
    )


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    ap.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    ap.add_argument("--placement", type=Path, default=DEFAULT_PLACEMENT)
    args = ap.parse_args(sys.argv[1:] if argv is None else argv)
    problems = run_checks(load_manifest(args.manifest), args.root, args.placement)
    for problem in problems:
        print(f"check_crate_graph: {problem}", file=sys.stderr)
    if problems:
        return 1
    print("check_crate_graph: ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
