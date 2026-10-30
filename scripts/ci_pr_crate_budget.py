#!/usr/bin/env python3
"""Pull-request crate-sweep budget guard.

Reads the pr-test-scope crate list (stdin, one crate per line) and
``cargo metadata`` JSON (file argument), computes the set of workspace crates
that must be rebuilt because of the scoped crates (the scoped crates plus all
their transitive workspace dependents), and prints one verdict line:

* ``RUN`` followed by the crates -- the rebuild closure fits the PR budget.
* ``DEFER <reason>`` -- the closure exceeds ``--max-rebuild``; the caller must
  skip the sweep with that message (main CI owns the full sweep) rather than
  silently running a cold rebuild that outlasts the pull-request budget.
"""

from __future__ import annotations

import argparse
import json
import sys


def rebuild_closure(metadata: dict, scoped: set[str]) -> set[str]:
    members = {
        p["id"]: p["name"]
        for p in metadata["packages"]
        if p["id"] in set(metadata["workspace_members"])
    }
    dependents: dict[str, set[str]] = {name: set() for name in members.values()}
    for pkg in metadata["packages"]:
        if pkg["id"] not in members:
            continue
        for dep in pkg["dependencies"]:
            if dep["name"] in dependents:
                dependents[dep["name"]].add(pkg["name"])
    closure: set[str] = set()
    stack = [c for c in scoped if c in dependents]
    while stack:
        crate = stack.pop()
        if crate in closure:
            continue
        closure.add(crate)
        stack.extend(dependents[crate])
    return closure


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "metadata", help="path to `cargo metadata --format-version 1 --no-deps` JSON"
    )
    parser.add_argument("--max-rebuild", type=int, default=12)
    args = parser.parse_args(argv)
    with open(args.metadata, encoding="utf-8") as handle:
        metadata = json.load(handle)
    scoped = {line.strip() for line in sys.stdin if line.strip()}
    closure = rebuild_closure(metadata, scoped)
    if len(closure) > args.max_rebuild:
        print(
            f"DEFER the diff touches {sorted(scoped)}; rebuilding "
            f"{len(closure)} dependent workspace crates exceeds the "
            f"pull-request budget of {args.max_rebuild}; main CI owns this sweep"
        )
        return 0
    print("RUN " + " ".join(sorted(scoped)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
