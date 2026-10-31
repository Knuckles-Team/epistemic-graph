"""Module units of the engine sources (MEDULLA-CONTRACT-R056).

A unit is a module together with all its descendants. The crate roots `lib.rs`,
`server/mod.rs` and `server/handlers/mod.rs` are hubs that dissolve: each direct child
module is its own unit and the hub files stay behind as single-file units.
"""

from __future__ import annotations

import re
from collections import defaultdict

from crate_split.graph import module_path

HUB_MODS = {(), ("server",), ("server", "handlers")}
HUB_FILES = {"src/lib.rs", "src/server/mod.rs", "src/server/handlers/mod.rs"}
TEST_NAME = re.compile(r"(_tests|tests|_fixtures|test_support)\.rs$")


def is_test_file(rel: str) -> bool:
    return bool(TEST_NAME.search(rel)) or "/tests/" in rel


def subject_of(rel: str, files: set[str]) -> str | None:
    """The source file a test file belongs to, when it can be inferred."""
    parent, _, name = rel.rpartition("/")
    if name.endswith("_tests.rs"):
        stem = name[: -len("_tests.rs")]
        candidates = [f"{parent}/{stem}.rs", f"{parent}/{stem}/mod.rs"]
    else:
        base = parent.split("/tests")[0] if "/tests" in rel else parent
        candidates = [base + ".rs", base + "/mod.rs"]
    return next((c for c in candidates if c in files and c != rel), None)


def module_unit(rel: str) -> str:
    """Unit key of a file by module path alone (tests are re-attached later)."""
    path = module_path(rel)
    if path is None or rel in HUB_FILES:
        return rel
    k = 1
    while tuple(path[:k]) in HUB_MODS:
        k += 1
    return "/".join(path[:k])


def build_units(files: list[str]) -> tuple[dict[str, str], dict[str, list[str]]]:
    """(file -> unit, unit -> files), with test files following their subject."""
    fileset = set(files)
    unit_of = {f: module_unit(f) for f in files}
    for rel in files:
        subject = subject_of(rel, fileset) if is_test_file(rel) else None
        if subject:
            unit_of[rel] = unit_of[subject]
    members: dict[str, list[str]] = defaultdict(list)
    for rel in files:
        members[unit_of[rel]].append(rel)
    return unit_of, dict(members)


def tarjan(nodes: list[str], succ: dict[str, set[str]]) -> list[list[str]]:
    """Strongly connected components, dependencies before dependents."""
    index: dict[str, int] = {}
    low: dict[str, int] = {}
    stack: list[str] = []
    on_stack: set[str] = set()
    out: list[list[str]] = []

    def visit(v: str) -> None:
        index[v] = low[v] = len(index)
        stack.append(v)
        on_stack.add(v)
        for w in sorted(succ.get(v, ())):
            if w not in index:
                visit(w)
                low[v] = min(low[v], low[w])
            elif w in on_stack:
                low[v] = min(low[v], index[w])
        if low[v] == index[v]:
            comp = []
            while True:
                w = stack.pop()
                on_stack.discard(w)
                comp.append(w)
                if w == v:
                    break
            out.append(sorted(comp))

    for node in nodes:
        if node not in index:
            visit(node)
    return out
