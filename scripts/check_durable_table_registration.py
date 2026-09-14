#!/usr/bin/env python3
"""Fail closed when a durable table is defined but not registered.

Why this exists
---------------
A redb table is DEFINED by one ``TableDefinition::new("name")``.  Three closed
registries then have to know about it:

* the **owner access registry** (``owner::table_api::owner_table_access``),
  which answers each table's cutover disposition and ends in
  ``unreachable!("table outside closed owner access registry: {table}")``;
* the **owner manifest** (``owner::contract``), whose key/value type resolvers
  end in ``unreachable!("table outside closed owner manifest: {name}")`` and
  whose hash is the manifest digest a backup and a recovery census are checked
  against;
* the **declared-table census** (``owner_table_names`` + the ledger census +
  the graph-shard census), which is what a strict backup and the recovery
  validator enumerate.

A table missing from any of them is a durable table that no manifest covers, no
backup enumerates, and whose first real access panics.  ``agent_component``
shipped in exactly that state.

The existing mechanism -- the frozen-cardinality assertions in
``crates/eg-storage/src/owner/tests.rs`` -- only notices when a test happens to
OPEN the table.  This gate asserts it at the DEFINITION, which is the point a
new table actually enters the tree.

The property
------------
Take the definition sites from the compiler-reachable PRODUCTION closure of
every cargo library target in the workspace -- not from a list of files, and
not from test code, which legitimately defines scratch tables.

Every name so defined must be answered by the owner manifest and by the
declared-table census.  A name that is ALSO an owner table -- it is in
``owner_table_names`` or the graph-shard census -- must additionally carry a
cutover disposition in the owner access registry.  A ledger table has no owner
and no disposition (its manifest contract is ``TableOwnership::Ledger`` and its
census is the ledger census), so requiring it there would be requiring the
wrong registry to know about it.

A registry answers a name either by an explicit entry or by a declared RULE
(the ``__sql_`` prefix family; the graph-shard census).  The rules are read out
of the registry source itself, so deleting one shows up as every table in that
family going unregistered rather than as a silent widening.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

import tomllib
from rust_lexer import _balanced_span_from, _rust_code_mask, _rust_comments_mask
from rust_module_tree import read_module_tree
from scanner_contract import run_git

ROOT = Path(__file__).resolve().parents[1]
SCHEMA = "eg-durable-table-registration-gate/v1"

ACCESS_REGISTRY = "crates/eg-storage/src/owner/table_api.rs"
ACCESS_FUNCTION = "owner_table_access"
OWNER_MANIFEST = "crates/eg-storage/src/owner/contract.rs"
OWNER_CENSUS = "crates/eg-storage/src/owner/registry.rs"
OWNER_CENSUS_FUNCTION = "owner_table_names"
LEDGER_CENSUS = "crates/eg-storage/src/tables.rs"
SHARD_CENSUS = "crates/eg-storage/src/owner/graph_shard.rs"
SHARD_CENSUS_CONSTANT = "GRAPH_SHARD_TABLES"

_DEFINITION = re.compile(
    r"\b(?:Multimap)?TableDefinition\s*::\s*new\s*\(\s*\"(?P<table>[^\"]+)\"\s*\)"
)
_LITERAL = re.compile(r"\"(?P<value>[^\"\\\n]*)\"")
_FN = r"(?:pub(?:\s*\([^)]*\))?\s+)?(?:const\s+)?(?:unsafe\s+)?fn\s+"
_CONSTANT = re.compile(
    r"\bconst\s+(?P<name>[A-Z][A-Z0-9_]*)\s*:(?P<rest>[^=]*)=\s*(?P<value>.*?);",
    re.S,
)
_VISIT = re.compile(r"\$visit!\(\s*\$crate::tables::(?P<name>[A-Z][A-Z0-9_]*)\s*\)")


class GateError(RuntimeError):
    """The gate could not establish its universe and must not report green."""


def read(relative: str) -> str:
    path = ROOT / relative
    if not path.is_file():
        raise GateError(f"required source is absent: {relative}")
    return path.read_text(encoding="utf-8")


def _library_root(manifest: str) -> str | None:
    """The library root of one cargo manifest, or None if it declares no lib."""

    path = ROOT / manifest
    try:
        with path.open("rb") as handle:
            table = tomllib.load(handle)
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise GateError(f"unreadable manifest {manifest}: {exc}") from exc
    if not isinstance(table.get("package"), dict):
        return None
    library = table.get("lib")
    declared = library.get("path") if isinstance(library, dict) else None
    candidate = (
        path.parent / declared
        if isinstance(declared, str)
        else path.parent / "src" / "lib.rs"
    )
    if not candidate.is_file():
        return None
    return candidate.resolve().relative_to(ROOT).as_posix()


def library_roots() -> list[str]:
    """Every cargo library root in the workspace, from the manifests."""

    result = run_git(("ls-files", "-z", "--", "Cargo.toml", "*/Cargo.toml"), cwd=ROOT)
    if result.returncode != 0:
        raise GateError(f"git ls-files failed: {(result.stderr or '').strip()}")
    manifests = sorted(entry for entry in result.stdout.split("\0") if entry)
    roots = [root for manifest in manifests if (root := _library_root(manifest))]
    if not roots:
        raise GateError("no cargo library root resolved: the gate has no universe")
    return roots


def definition_sites() -> dict[str, list[str]]:
    """table name -> the crate roots whose production closure defines it."""

    sites: dict[str, list[str]] = {}
    for root in library_roots():
        source = read_module_tree(root, root_dir=ROOT)
        for match in _DEFINITION.finditer(_rust_comments_mask(source)):
            sites.setdefault(match.group("table"), []).append(root)
    if not sites:
        raise GateError("no durable table definition found: the gate has no universe")
    return sites


def _function_body(relative: str, name: str) -> str:
    source = read(relative)
    mask = _rust_code_mask(source)
    header = re.search(_FN + re.escape(name) + r"\s*[(<]", mask)
    if header is None:
        raise GateError(f"registry function is absent: {relative}::{name}")
    opener = mask.find("{", header.end())
    if opener < 0:
        raise GateError(f"registry function has no body: {relative}::{name}")
    closer = _balanced_span_from(mask, opener, "{", "}")
    return _rust_comments_mask(source)[opener : closer + 1]


def _constant_body(relative: str, name: str) -> str:
    text = _rust_comments_mask(read(relative))
    match = re.search(r"\bconst\s+" + re.escape(name) + r"\s*:[^=]*=\s*&\s*\[", text)
    if match is None:
        raise GateError(f"registry constant is absent: {relative}::{name}")
    opener = match.end() - 1
    closer = _balanced_span_from(text, opener, "[", "]")
    return text[opener : closer + 1]


def _literals(body: str) -> set[str]:
    return {match.group("value") for match in _LITERAL.finditer(body)}


def ledger_census() -> set[str]:
    """The ledger tables the `visit_ledger_tables!` macro enumerates."""

    text = _rust_comments_mask(read(LEDGER_CENSUS))
    constants: dict[str, set[str]] = {}
    for match in _CONSTANT.finditer(text):
        names = {
            definition.group("table")
            for definition in _DEFINITION.finditer(match.group("value"))
        }
        if names:
            constants[match.group("name")] = names
    marker = "macro_rules! visit_ledger_tables"
    start = text.find(marker)
    if start < 0:
        raise GateError("the ledger census macro is absent")
    opener = text.find("{", start + len(marker))
    closer = _balanced_span_from(text, opener, "{", "}")
    visited = {match.group("name") for match in _VISIT.finditer(text[opener:closer])}
    if not visited:
        raise GateError("the ledger census macro enumerates nothing")
    missing = sorted(visited - set(constants))
    if missing:
        raise GateError(
            "the ledger census names constants this gate cannot resolve to a "
            f"table: {missing}"
        )
    return {name for constant in visited for name in constants[constant]}


def registries() -> tuple[dict[str, set[str]], dict[str, list[str]]]:
    """The three registries' explicit entries, and their declared rules."""

    access = _function_body(ACCESS_REGISTRY, ACCESS_FUNCTION)
    shard = set(_literals(_constant_body(SHARD_CENSUS, SHARD_CENSUS_CONSTANT)))
    rules: dict[str, list[str]] = {}

    # The access registry answers two whole families by RULE rather than by
    # entry.  Read the rules out of its source: a deleted rule must show up as
    # every table in that family going unregistered, not as a silent widening.
    access_rules: list[str] = []
    prefix = re.search(r"starts_with\(\s*\"(?P<prefix>[^\"]+)\"\s*\)", access)
    if prefix is not None:
        access_rules.append(f"prefix:{prefix.group('prefix')}")
    if f"{SHARD_CENSUS_CONSTANT}.contains" in access:
        access_rules.append(f"census:{SHARD_CENSUS_CONSTANT}")
    rules["owner_access_registry"] = access_rules

    manifest = _rust_comments_mask(read(OWNER_MANIFEST))
    manifest_rules = [
        f"prefix:{match.group('prefix')}"
        for match in re.finditer(
            r"starts_with\(\s*\"(?P<prefix>[^\"]+)\"\s*\)", manifest
        )
    ]
    if "graph_shard::key_type" in manifest:
        manifest_rules.append(f"census:{SHARD_CENSUS_CONSTANT}")
    rules["owner_manifest"] = sorted(set(manifest_rules))

    owner_body = _function_body(OWNER_CENSUS, OWNER_CENSUS_FUNCTION)
    owner_census = _literals(owner_body)
    census_rules: list[str] = []
    if SHARD_CENSUS_CONSTANT in owner_body:
        census_rules.append(f"census:{SHARD_CENSUS_CONSTANT}")
    rules["declared_table_census"] = census_rules
    rules["owner_table_census"] = list(census_rules)

    return (
        {
            "owner_access_registry": _literals(access),
            "owner_manifest": _literals(manifest),
            "declared_table_census": owner_census | ledger_census(),
            "owner_table_census": owner_census,
        },
        rules | {"graph_shard_census": sorted(shard)},
    )


def _answered(table: str, entries: set[str], rules: list[str], shard: set[str]) -> bool:
    if table in entries:
        return True
    for rule in rules:
        kind, _, value = rule.partition(":")
        if kind == "prefix" and table.startswith(value):
            return True
        if kind == "census" and table in shard:
            return True
    return False


def run_gate() -> dict:
    sites = definition_sites()
    entries, rules = registries()
    shard = set(rules.pop("graph_shard_census"))

    findings: list[dict] = []
    for table in sorted(sites):
        required = ["owner_manifest", "declared_table_census"]
        # The access registry answers the CUTOVER DISPOSITION of an owner
        # table.  A ledger table has no owner and no disposition: its contract
        # is `TableOwnership::Ledger` in the manifest and its census is the
        # ledger census, so requiring it here would be requiring the wrong
        # registry to know about it.
        if _answered(
            table,
            entries["owner_table_census"],
            rules["owner_table_census"],
            shard,
        ):
            required.insert(0, "owner_access_registry")
        unregistered = [
            registry
            for registry in required
            if not _answered(table, entries[registry], rules[registry], shard)
        ]
        if unregistered:
            findings.append(
                {
                    "table": table,
                    "defined_in": sorted(set(sites[table])),
                    "unregistered_in": unregistered,
                }
            )

    return {
        "schema": SCHEMA,
        "definitions": len(sites),
        "registry_sizes": {name: len(value) for name, value in sorted(entries.items())},
        "graph_shard_census": len(shard),
        "rules": rules,
        "unregistered_tables": findings,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true", help="emit the full receipt")
    args = parser.parse_args(argv)
    try:
        receipt = run_gate()
    except GateError as exc:
        print(f"durable-table-registration gate: CANNOT RUN: {exc}", file=sys.stderr)
        return 2
    if args.json:
        print(json.dumps(receipt, indent=2, sort_keys=True))
    findings = receipt["unregistered_tables"]
    if not findings:
        print(
            "durable-table-registration gate: OK: "
            f"{receipt['definitions']} durable table(s) are answered by the owner "
            "access registry, the owner manifest and the declared-table census"
        )
        return 0
    print(
        f"durable-table-registration gate: FAIL: {len(findings)} durable table(s) "
        "are defined but not registered",
        file=sys.stderr,
    )
    for finding in findings:
        print(
            f"  {finding['table']}: missing from "
            f"{', '.join(finding['unregistered_in'])} "
            f"(defined in {', '.join(finding['defined_in'])})",
            file=sys.stderr,
        )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
