#!/usr/bin/env python3
"""Fail closed when a tracked `tests/` file has no owner in the component registry.

Why this exists
----------------
`architecture/component-registry.yml`'s owner-manifest layout checker
(`plans/refactor/scripts/architecture_component_registry.py`'s
`validate_layout_records`) validates the PATH STRINGS a component declares --
canonical lexical form, no overlap between components, source/test roots
paired -- but it never reads this repository's filesystem. It has no way to
notice that a real, tracked file exists and is not named by any component's
`owned_source_roots`/`test_roots`/etc. Confirmed by reading its source: none
of `validate_layout_records`, `_layout_path`, `_paths_overlap`, or
`_require_architecture_label` call `os.path`/`Path.exists`/`Path.is_file` or
enumerate a directory anywhere. A file the registry never mentions is neither
flagged nor even visible to it.

`eg.binary-composition`'s `test_roots` in particular is an explicit
enumeration of the repository root `tests/` directory's entries (needed so
carved-out per-component test files -- e.g. `eg.persistence`'s
`tests/sharded_startup_open_timing_d_cdx_65.rs` -- do not overlap its
blanket claim). That enumeration is a snapshot: the next NEW file added
directly under `tests/` is not automatically covered by it or by anything
else, and the registry checker will not notice. This gate is the completeness
check the checker structurally cannot be: it reads the real, tracked
`tests/` tree and fails if any file there is not covered, exactly, by some
component's declared paths.

Ownership rule
--------------
A tracked path under `tests/` is OWNED if it equals, or is nested one or more
directories under, some path listed in any implementation_component's
`owned_source_roots`, `public_contract_roots`, `test_roots`, `generated_roots`,
`shared_roots`, `shared_files`, or `owned_source_roots_omissions[].path`.
Nesting uses the same boundary rule the registry checker itself uses
(`_paths_overlap`): a path is a descendant of a claimed root only when it
matches at a `/` boundary, so `tests/foo.rs` is not accidentally claimed by a
sibling `tests/foo` directory entry.

Exit 0 = every tracked `tests/` file is owned. Exit 1 = at least one is not
(each is printed, with a pointer to fix it). Exit 2 = cannot run (missing
registry file, invalid YAML, git failure).
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]

PATH_FIELDS = (
    "owned_source_roots",
    "public_contract_roots",
    "test_roots",
    "generated_roots",
    "shared_roots",
    "shared_files",
)


def die(message: str) -> None:
    print(f"registry-test-ownership: CANNOT RUN: {message}", file=sys.stderr)
    raise SystemExit(2)


def parse_claimed_paths(document: dict) -> list[str]:
    """The union of every path-bearing field across implementation components."""

    claimed: list[str] = []
    for component in document.get("components", []):
        if component.get("component_kind") != "implementation_component":
            continue
        for field in PATH_FIELDS:
            claimed.extend(component.get(field) or [])
        for omission in component.get("owned_source_roots_omissions") or []:
            path = omission.get("path")
            if path:
                claimed.append(path)
    return claimed


def load_claimed_paths(registry_path: Path) -> list[str]:
    if not registry_path.is_file():
        die(f"missing {registry_path}")
    try:
        document = yaml.safe_load(registry_path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        die(f"invalid YAML in {registry_path}: {exc}")
    return parse_claimed_paths(document)


def is_owned(path: str, claimed_paths: list[str]) -> bool:
    for claim in claimed_paths:
        claim = claim.rstrip("/")
        if path == claim or path.startswith(claim + "/"):
            return True
    return False


def tracked_test_files(root: Path) -> list[str]:
    result = subprocess.run(
        ["git", "ls-files", "--", "tests"],
        cwd=root,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        die(f"git ls-files failed: {result.stderr.strip()}")
    return [line for line in result.stdout.splitlines() if line]


def unowned_test_files(root: Path, registry_path: Path) -> tuple[list[str], list[str]]:
    """Return `(all tracked tests/ files, the ones with no owner)`."""

    claimed_paths = load_claimed_paths(registry_path)
    files = tracked_test_files(root)
    if not files:
        die("git ls-files -- tests returned no files; the registry claims the directory exists")
    unowned = [f for f in files if not is_owned(f, claimed_paths)]
    return files, unowned


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root",
        type=Path,
        default=ROOT,
        help="Repository root to check (default: this script's own checkout). "
        "Exists so this gate is testable against a throwaway fixture repo "
        "without touching the real one.",
    )
    args = parser.parse_args(argv)
    registry_path = args.root / "architecture" / "component-registry.yml"

    files, unowned = unowned_test_files(args.root, registry_path)

    if unowned:
        print(
            f"registry-test-ownership: {len(unowned)} of {len(files)} tracked tests/ "
            "file(s) have no owner in architecture/component-registry.yml:",
            file=sys.stderr,
        )
        for f in unowned:
            print(f"  {f}", file=sys.stderr)
        print(
            "\nFix: add the exact path to the owning component's `test_roots` "
            "(or `owned_source_roots`, if it is not a test file) in "
            "architecture/component-registry.yml. If it belongs under the shared "
            "root and no more specific component owns it, add it to "
            "eg.binary-composition's `test_roots` enumeration instead of widening "
            "that enumeration back into a directory glob (a glob would re-overlap "
            "every per-component carve-out the layout checker requires).",
            file=sys.stderr,
        )
        return 1

    print(f"registry-test-ownership: OK: all {len(files)} tracked tests/ file(s) are owned")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
