#!/usr/bin/env python3
"""Fail the build if any Rust dependency drifts off crates.io.

Enforces the crates.io-only dependency edict (AGENTS.md, "crates.io-only Rust
dependency edict"): every Rust dependency must resolve from the official
registry. Three things fail this gate:

  1. A `git = "..."` key on any dependency in any workspace `Cargo.toml`
     (root or `crates/*/Cargo.toml`), in `[dependencies]`,
     `[dev-dependencies]`, `[build-dependencies]`, `[workspace.dependencies]`,
     or a `[target.'cfg(...)'.*dependencies]` table.
  2. A `path = "..."` dependency that resolves OUTSIDE this workspace
     checkout (a `path` to a sibling crate under `crates/` -- the normal
     in-workspace pattern -- is fine; a `path` that escapes the repo root is
     not). This does not touch `[[bin]]`/`[[bench]]`/`[[example]]` target
     `path`s -- those are source-file locations, not dependencies, and are
     parsed from a different table.
  3. A `git+` source in the committed `Cargo.lock` -- the mechanical proof
     that no git dependency is actually being resolved, even if some
     manifest edit slipped past checks 1/2 (e.g. a `[patch]` section).

The only sanctioned exception is a not-yet-released security fix: an
immutable `rev = "<full sha>"` pin, commented with the advisory ID and the
crates.io version that will replace it, removed the moment that version
publishes (see the `object_store` history in AGENTS.md for the worked
example). That is a conscious, reviewed, temporary state -- not something
this automated gate can special-case, so lifting it back to a plain
`version = "..."` dependency is the only way to turn this gate green again.
"""

from __future__ import annotations

import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

_DEPENDENCY_TABLE_PREFIXES = (
    "dependencies",
    "dev-dependencies",
    "build-dependencies",
)


def _dependency_tables(doc: dict) -> list[tuple[str, dict]]:
    """Yield (label, table) for every dependency table in a parsed Cargo.toml."""

    tables: list[tuple[str, dict]] = []
    for prefix in _DEPENDENCY_TABLE_PREFIXES:
        table = doc.get(prefix)
        if isinstance(table, dict):
            tables.append((prefix, table))
    workspace = doc.get("workspace")
    if isinstance(workspace, dict):
        table = workspace.get("dependencies")
        if isinstance(table, dict):
            tables.append(("workspace.dependencies", table))
    target = doc.get("target")
    if isinstance(target, dict):
        for cfg_name, cfg_table in target.items():
            if not isinstance(cfg_table, dict):
                continue
            for prefix in _DEPENDENCY_TABLE_PREFIXES:
                table = cfg_table.get(prefix)
                if isinstance(table, dict):
                    tables.append((f"target.{cfg_name}.{prefix}", table))
    return tables


def check_manifest(manifest_path: Path, failures: list[str]) -> None:
    doc = tomllib.loads(manifest_path.read_text(encoding="utf-8"))
    manifest_dir = manifest_path.parent
    relative_manifest = manifest_path.relative_to(ROOT)

    for label, table in _dependency_tables(doc):
        for dep_name, spec in table.items():
            if not isinstance(spec, dict):
                continue
            if "git" in spec:
                failures.append(
                    f"{relative_manifest} [{label}] '{dep_name}' has a git = source "
                    f"({spec.get('git')!r}). Rust dependencies must come from crates.io "
                    "-- see AGENTS.md, 'crates.io-only Rust dependency edict'."
                )
            path_value = spec.get("path")
            if isinstance(path_value, str):
                resolved = (manifest_dir / path_value).resolve()
                try:
                    resolved.relative_to(ROOT)
                except ValueError:
                    failures.append(
                        f"{relative_manifest} [{label}] '{dep_name}' has a path = "
                        f"dependency ({path_value!r}) that resolves OUTSIDE this "
                        f"workspace ({resolved}). Only in-workspace path dependencies "
                        "are allowed -- see AGENTS.md, 'crates.io-only Rust dependency "
                        "edict'."
                    )


def check_lockfile(failures: list[str]) -> None:
    lock_path = ROOT / "Cargo.lock"
    if not lock_path.exists():
        return
    for line_number, line in enumerate(lock_path.read_text(encoding="utf-8").splitlines(), 1):
        stripped = line.strip()
        if stripped.startswith("source") and "git+" in stripped:
            failures.append(
                f"Cargo.lock line {line_number} resolves a dependency from a git+ "
                f"source ({stripped!r}). Every locked dependency must resolve from "
                "crates.io -- see AGENTS.md, 'crates.io-only Rust dependency edict'."
            )


def main() -> int:
    failures: list[str] = []

    manifests = [ROOT / "Cargo.toml"]
    manifests.extend(sorted((ROOT / "crates").glob("*/Cargo.toml")))
    for manifest_path in manifests:
        check_manifest(manifest_path, failures)

    check_lockfile(failures)

    if failures:
        print("FAIL: crates.io-only dependency edict violated:", file=sys.stderr)
        for failure in failures:
            print(f"  - {failure}", file=sys.stderr)
        print(
            "\nThe only sanctioned exception is an unreleased security fix pinned to an "
            "immutable rev = \"<full sha>\", commented with the advisory ID and the "
            "crates.io version that will replace it, removed the moment that version "
            "publishes. See AGENTS.md, 'crates.io-only Rust dependency edict'.",
            file=sys.stderr,
        )
        return 1

    print(f"OK: {len(manifests)} Cargo.toml manifest(s) and Cargo.lock are crates.io-only.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
