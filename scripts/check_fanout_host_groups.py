#!/usr/bin/env python3
"""EG-CONTRACT-R011.1: validate the land gate's fan-out host-group manifest.

The land gate's default execution (EG-CONTRACT-R011) fans work out across
independent build-host groups, each with a persistent warm target and a
reserved headroom fraction on its gate host. Before any work is dispatched,
the manifest describing those groups must be structurally sound: every group
needs exactly one warm target and a headroom reservation in (0, 1), and no
host may be claimed by more than one group (a double-booked host would let
two fan-out lanes race for the same warm target).

This script is the standalone, fail-closed check for that manifest shape. It
takes no action on any host; it only validates the declared groups.

Exit codes: 0 valid, 1 a structural violation was found (printed), 2 cannot
run (missing/unreadable/malformed-YAML manifest).
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_MANIFEST = ROOT / ".config" / "fanout-host-groups.yml"


def die(message: str) -> None:
    print(f"fanout-host-groups: CANNOT RUN: {message}", file=sys.stderr)
    raise SystemExit(2)


def load_manifest(path: Path) -> dict[str, Any]:
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError as exc:
        die(f"could not read {path}: {exc}")
    try:
        document = yaml.safe_load(raw)
    except yaml.YAMLError as exc:
        die(f"{path} is not valid YAML: {exc}")
    if not isinstance(document, dict) or not isinstance(document.get("groups"), list):
        die(f"{path} has no top-level 'groups' list")
    return document


def validate_groups(groups: list[Any]) -> list[str]:
    """Return a list of human-readable problems; empty means valid."""

    problems: list[str] = []
    seen_hosts: dict[str, str] = {}
    seen_names: set[str] = set()
    for index, group in enumerate(groups):
        label = f"groups[{index}]"
        if not isinstance(group, dict):
            problems.append(f"{label} is not a mapping")
            continue
        name = group.get("name")
        if not isinstance(name, str) or not name.strip():
            problems.append(f"{label} has no non-empty 'name'")
            name = label
        elif name in seen_names:
            problems.append(f"group '{name}' is declared more than once")
        seen_names.add(name)

        warm_target = group.get("warm_target")
        if not isinstance(warm_target, str) or not warm_target.strip():
            problems.append(f"group '{name}' has no non-empty 'warm_target'")

        headroom = group.get("headroom_reservation")
        if (
            isinstance(headroom, bool)
            or not isinstance(headroom, (int, float))
            or not 0.0 < float(headroom) < 1.0
        ):
            problems.append(
                f"group '{name}' has an invalid 'headroom_reservation' "
                "(must be a number strictly between 0 and 1)"
            )

        hosts = group.get("hosts")
        if not isinstance(hosts, list) or not hosts:
            problems.append(f"group '{name}' has no non-empty 'hosts' list")
            hosts = []
        for host in hosts:
            if not isinstance(host, str) or not host.strip():
                problems.append(f"group '{name}' has a non-string host entry")
                continue
            if host in seen_hosts:
                problems.append(
                    f"host '{host}' is claimed by both '{seen_hosts[host]}' "
                    f"and '{name}'"
                )
            else:
                seen_hosts[host] = name
    return problems


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "manifest",
        nargs="?",
        type=Path,
        default=DEFAULT_MANIFEST,
        help="path to the fan-out host-group manifest (default: %(default)s)",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    document = load_manifest(args.manifest)
    problems = validate_groups(document["groups"])
    if not problems:
        print(
            f"fanout-host-groups: PASS: {len(document['groups'])} disjoint "
            "group(s), each with a warm target and headroom reservation"
        )
        return 0
    print("fanout-host-groups: FAIL:")
    for problem in problems:
        print(f"  {problem}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
