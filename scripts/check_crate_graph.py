#!/usr/bin/env python3
"""Check the real crate graph against the crate-split manifest (MEDULLA-CONTRACT-R056).

Fails on: (a) a crate depending on a manifest crate outside its `allowed_deps`,
(b) a dependency edge between two handler crates, (c) a remaining contract module once
the manifest's delete set is enforced, (d) a manifest path that no longer exists or a
`src/` file assigned twice or never. Crates not created yet are skipped for (a) and (b).
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import tomllib

sys.path.insert(0, str(Path(__file__).resolve().parent))

from crate_split.ownership import (
    DEFAULT_MANIFEST,
    Manifest,
    entry_matches,
    list_src_files,
    load_manifest,
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
        allowed = set(manifest.crates[name].allowed_deps)
        for dep in sorted(found - allowed):
            problems.append(
                f"{name} depends on {dep}, which is not in its allowed_deps"
            )
    return problems


def check_siblings(manifest: Manifest, deps: dict[str, set[str]]) -> list[str]:
    """handler_crates_have_no_sibling_edges."""
    handlers = {n for n, c in manifest.crates.items() if c.kind == "handler"}
    return [
        f"handler crate {name} depends on sibling handler crate {dep}"
        for name, found in sorted(deps.items())
        if name in handlers
        for dep in sorted(found & handlers - {name})
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


def check_ownership(manifest: Manifest, root: Path) -> list[str]:
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


def run_checks(manifest: Manifest, root: Path) -> list[str]:
    deps = existing_deps(manifest, root)
    return (
        check_allowed(manifest, deps)
        + check_siblings(manifest, deps)
        + check_no_contract_modules(manifest, root)
        + check_ownership(manifest, root)
    )


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    ap.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    args = ap.parse_args(sys.argv[1:] if argv is None else argv)
    problems = run_checks(load_manifest(args.manifest), args.root)
    for problem in problems:
        print(f"check_crate_graph: {problem}", file=sys.stderr)
    if problems:
        return 1
    print("check_crate_graph: ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
