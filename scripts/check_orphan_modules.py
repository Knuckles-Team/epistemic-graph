#!/usr/bin/env python3
"""Fail closed when a tracked Rust file is not compiled by any cargo target.

Why this exists
---------------
Every shape scanner in this repository -- CCCC, KISS, dupehound, jscpd, clippy,
arch-lint -- measures source that the compiler reads.  A file with no ``mod``
declaration is read by none of them, so it is simultaneously short, simple,
unduplicated, correctly layered and *entirely absent from the binary*.  163 KB
of tiered-ingestion pipeline sat in this tree in exactly that state: added,
reviewed, merged, never wired, and green on every gate.

The property
------------
For every cargo TARGET in the workspace (lib, bin, test, bench, example, build
script, and the fuzz targets of a nested fuzz package), walk the
compiler-declared module closure of its root file.  The union of those closures
is the set of files rustc actually reads.  Any tracked ``.rs`` file outside that
union is unreachable and is reported.

This asserts the property, not a spelling: nothing here keys on a module NAME,
a directory NAME, a line number or a byte offset.  A file that moves, is
renamed, or is redeclared through ``#[path]`` stays reachable because the walk
follows the same declarations the compiler does.

Traps this gate is built to avoid (each one produced a wrong answer in a
throwaway first version):

* cargo AUTO-DISCOVERS targets in ``tests/``, ``benches/``, ``examples/``,
  ``src/bin/`` and ``fuzz/fuzz_targets/``.  Those roots need no ``mod``
  declaration anywhere and must never be reported.  Auto-discovery is
  suppressed per-manifest by ``autobins``/``autotests``/``autobenches``/
  ``autoexamples``.
* ``mod`` declarations bind PER DIRECTORY.  ``mod semantic_index;`` in
  ``eg-types/src/lib.rs`` declares ``eg-types/src/semantic_index.rs`` and says
  nothing whatever about ``src/server/semantic_index.rs``.  A global
  match-by-name produced 95 false positives here and still missed a true orphan.
* Both module layouts (``dir/mod.rs`` and the 2018 ``dir.rs`` + ``dir/``),
  ``#[path = "..."]`` overrides and ``include!`` inputs are all real
  declarations.
* A ``mod`` behind ``#[cfg(...)]`` -- a feature, ``test``, a target predicate --
  IS reachable.  The walk runs in test-inclusive mode, where no cfg is
  evaluated, so a module declared under any predicate counts as declared.

Reporting
---------
An orphan whose own module tree pulls in further files is an orphan ROOT; the
files it would have declared are reported as collateral rather than as separate
findings, so the count names distinct wiring gaps rather than file counts.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path

import tomllib
from rust_module_tree import read_module_paths
from scanner_contract import run_git

ROOT = Path(__file__).resolve().parents[1]
SCHEMA = "eg-orphan-module-gate/v1"


class GateError(RuntimeError):
    """The gate could not establish its universe and must not report green."""


@dataclass(frozen=True)
class Target:
    """One cargo compilation root and how it was established."""

    manifest: str
    kind: str
    path: str
    discovery: str


def tracked_rust_files() -> list[str]:
    result = run_git(("ls-files", "-z", "--", "*.rs"), cwd=ROOT)
    if result.returncode != 0:
        raise GateError(f"git ls-files failed: {(result.stderr or '').strip()}")
    return sorted(entry for entry in result.stdout.split("\0") if entry)


def tracked_manifests() -> list[str]:
    result = run_git(("ls-files", "-z", "--", "Cargo.toml", "*/Cargo.toml"), cwd=ROOT)
    if result.returncode != 0:
        raise GateError(f"git ls-files failed: {(result.stderr or '').strip()}")
    return sorted(entry for entry in result.stdout.split("\0") if entry)


def _manifest_table(path: Path) -> dict:
    try:
        with path.open("rb") as handle:
            return tomllib.load(handle)
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise GateError(f"unreadable manifest {path}: {exc}") from exc


def _declared(section: object, kind: str, manifest_dir: Path, rel: str) -> list[Target]:
    """Targets written out explicitly in the manifest."""

    entries: list[dict]
    if isinstance(section, dict):
        entries = [section]
    elif isinstance(section, list):
        entries = [entry for entry in section if isinstance(entry, dict)]
    else:
        return []
    targets: list[Target] = []
    for entry in entries:
        declared_path = entry.get("path")
        if isinstance(declared_path, str):
            candidate = (manifest_dir / declared_path).resolve()
            targets.append(Target(rel, kind, _relative(candidate), "manifest [path]"))
    return targets


def _relative(path: Path) -> str:
    try:
        return path.resolve().relative_to(ROOT).as_posix()
    except ValueError as exc:
        raise GateError(f"cargo target escapes the repository: {path}") from exc


def _auto_directory(directory: Path, rel: str, kind: str) -> list[Target]:
    """cargo's auto-discovery rule for tests/, benches/, examples/, src/bin/.

    A target is ``<dir>/<name>.rs`` or ``<dir>/<name>/main.rs``.  Nothing
    deeper is a target; deeper files are ordinary modules and must be
    DECLARED by one of these roots.
    """

    if not directory.is_dir():
        return []
    targets: list[Target] = []
    for child in sorted(directory.iterdir()):
        if child.is_file() and child.suffix == ".rs":
            targets.append(Target(rel, kind, _relative(child), "cargo auto-discovery"))
        elif child.is_dir() and (child / "main.rs").is_file():
            targets.append(
                Target(rel, kind, _relative(child / "main.rs"), "cargo auto-discovery")
            )
    return targets


def _fuzz_targets(manifest_dir: Path, rel: str) -> list[Target]:
    """cargo-fuzz discovers every file under fuzz_targets/ as its own binary."""

    directory = manifest_dir / "fuzz_targets"
    if not directory.is_dir():
        return []
    return [
        Target(rel, "fuzz", _relative(path), "cargo-fuzz auto-discovery")
        for path in sorted(directory.rglob("*.rs"))
    ]


def _library_target(
    table: dict, package: dict, manifest_dir: Path, rel: str
) -> list[Target]:
    declared = _declared(table.get("lib"), "lib", manifest_dir, rel)
    if declared:
        return declared
    default = manifest_dir / "src" / "lib.rs"
    if not default.is_file():
        return []
    return [Target(rel, "lib", _relative(default), "cargo default src/lib.rs")]


def _build_target(package: dict, manifest_dir: Path, rel: str) -> list[Target]:
    build = package.get("build")
    if isinstance(build, str):
        return [Target(rel, "build", _relative(manifest_dir / build), "manifest build")]
    if build is False:
        return []
    default = manifest_dir / "build.rs"
    if not default.is_file():
        return []
    return [Target(rel, "build", _relative(default), "cargo default build.rs")]


def _binary_targets(package: dict, manifest_dir: Path, rel: str) -> list[Target]:
    if package.get("autobins", True) is False:
        return []
    default = manifest_dir / "src" / "main.rs"
    targets = _auto_directory(manifest_dir / "src" / "bin", rel, "bin")
    if default.is_file():
        targets.append(
            Target(rel, "bin", _relative(default), "cargo default src/main.rs")
        )
    return targets


def package_targets(rel: str) -> list[Target]:
    manifest_path = ROOT / rel
    manifest_dir = manifest_path.parent
    table = _manifest_table(manifest_path)
    package = table.get("package")
    if not isinstance(package, dict):
        # A pure `[workspace]` manifest compiles nothing of its own.
        return []

    targets = _library_target(table, package, manifest_dir, rel)
    targets.extend(_build_target(package, manifest_dir, rel))
    targets.extend(_declared(table.get("bin"), "bin", manifest_dir, rel))
    targets.extend(_binary_targets(package, manifest_dir, rel))
    for kind, directory in (
        ("test", manifest_dir / "tests"),
        ("bench", manifest_dir / "benches"),
        ("example", manifest_dir / "examples"),
    ):
        targets.extend(_declared(table.get(kind), kind, manifest_dir, rel))
        if package.get(f"auto{kind}s", True) is not False:
            targets.extend(_auto_directory(directory, rel, kind))
    targets.extend(_fuzz_targets(manifest_dir, rel))

    seen: dict[str, Target] = {}
    for target in targets:
        seen.setdefault(target.path, target)
    return sorted(seen.values(), key=lambda target: (target.path, target.kind))


def closure(relative: str, *, crate_root: bool = False) -> set[str]:
    """Every file the compiler reads when it compiles this root."""

    try:
        paths = read_module_paths(
            relative, ROOT, include_tests=True, crate_root=crate_root
        )
    except (ValueError, OSError) as exc:
        raise GateError(f"module walk failed for {relative}: {exc}") from exc
    return {_relative(path) for path in paths}


def _targets() -> list[Target]:
    manifests = tracked_manifests()
    if not manifests:
        raise GateError("no tracked Cargo manifest: the gate has no universe")
    targets: list[Target] = []
    for manifest in manifests:
        targets.extend(package_targets(manifest))
    if not targets:
        raise GateError("no cargo target resolved: the gate has no universe")
    for target in targets:
        if not (ROOT / target.path).is_file():
            raise GateError(f"declared cargo target is absent: {target.path}")
    return targets


def _orphan_roots(unreachable: list[str], tracked: set[str]) -> list[dict]:
    """Group unreachable files under the one whose wiring would fix them.

    An unreachable file may itself declare further unreachable files, and one
    `mod` statement wires the whole sub-tree, so the sub-tree is ONE finding.
    """

    collateral: dict[str, set[str]] = {}
    for path in unreachable:
        collateral[path] = {
            member for member in closure(path) - {path} if member in tracked
        }
    claimed = {member for members in collateral.values() for member in members}
    return [
        {
            "path": path,
            "bytes": (ROOT / path).stat().st_size,
            "collateral": sorted(collateral[path]),
        }
        for path in unreachable
        if path not in claimed
    ]


def run_gate() -> dict:
    tracked = tracked_rust_files()
    if not tracked:
        raise GateError("no tracked Rust source: the gate has no universe")
    targets = _targets()

    reachable: set[str] = set()
    for target in targets:
        reachable |= closure(target.path, crate_root=True)

    tracked_set = set(tracked)
    unreachable = sorted(tracked_set - reachable)
    return {
        "schema": SCHEMA,
        "targets": len(targets),
        "target_kinds": {
            kind: sum(1 for target in targets if target.kind == kind)
            for kind in sorted({target.kind for target in targets})
        },
        "tracked_rust_files": len(tracked),
        "compiler_reachable_files": len(tracked_set & reachable),
        "orphan_roots": _orphan_roots(unreachable, tracked_set),
        "orphan_files": len(unreachable),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--json", action="store_true", help="emit the full receipt as JSON"
    )
    args = parser.parse_args(argv)
    try:
        receipt = run_gate()
    except GateError as exc:
        print(f"orphan-module gate: CANNOT RUN: {exc}", file=sys.stderr)
        return 2
    if args.json:
        print(json.dumps(receipt, indent=2, sort_keys=True))
    roots = receipt["orphan_roots"]
    if not roots:
        print(
            "orphan-module gate: OK: "
            f"{receipt['compiler_reachable_files']} tracked Rust file(s) are "
            f"compiled by {receipt['targets']} cargo target(s)"
        )
        return 0
    print(
        f"orphan-module gate: FAIL: {len(roots)} orphan root(s), "
        f"{receipt['orphan_files']} tracked Rust file(s) in total, are not "
        f"compiled by any cargo target "
        f"({receipt['compiler_reachable_files']}/{receipt['tracked_rust_files']} "
        "reachable)",
        file=sys.stderr,
    )
    for finding in roots:
        print(
            f"  {finding['path']} ({finding['bytes']} bytes) — no `mod` "
            "declaration reaches it from any crate root",
            file=sys.stderr,
        )
        for member in finding["collateral"]:
            print(f"      also unreachable via it: {member}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
