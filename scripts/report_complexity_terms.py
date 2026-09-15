#!/usr/bin/env python3
"""Split the CCCC census into "accepted by rule" and "real backlog".

WHY
---
A census that prints one undifferentiated number is noise: every run says
"hundreds of findings", nobody can tell whether today's number is worse than
yesterday's for a reason, and the report stops being read. This splits the same
findings by the terms of acceptance in `scripts/rust_exhaustive_match.py`, so
the census reports clean-with-known-exceptions and the backlog number is the
one a burndown lane can actually own.

It is a REPORT, not a gate: it never fails on a count, holds no threshold, and
writes nothing. `check_complexity_staged.py` is what enforces, on the diff, and
it applies the identical rule from the identical module -- one authority, so
the census and the hook can never disagree about what is accepted.

Exit codes: 0 printed a report, 2 the report could not be produced (an
ENVIRONMENT fact -- never reported as "no findings").
"""

from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any, NoReturn

from rust_exhaustive_match import dispatch_shape
from scanner_contract import (
    CCCC_MAX_COGNITIVE,
    CCCC_MAX_CYCLOMATIC,
)

ROOT = Path(__file__).resolve().parent.parent

#: Every disposition a cyclomatic-only over-cap function can land in, with the
#: reason the terms of acceptance give for it. Order is report order.
DISPOSITIONS = (
    ("accepted", "exhaustive dispatch, residual within cap -- ACCEPTED BY RULE"),
    ("catch_all", "has a catch-all arm, so the match is NOT exhaustive"),
    ("no_match", "no `match` in the body: branching that is not dispatch"),
    ("residual", "dispatch discounted, the rest still exceeds the cap"),
    ("unattributable", "arm count exceeds cyclomatic: attribution unproven"),
    ("not_rust", "not Rust: rustc exhaustiveness does not apply"),
    ("unreadable", "source could not be lexed or located"),
)


def fail(message: str) -> NoReturn:
    print(f"complexity terms: CANNOT RUN: {message}", file=sys.stderr)
    raise SystemExit(2)


def _rows(document: dict[str, Any]) -> list[tuple[str, str, int, int, int]]:
    files = document.get("files")
    if not isinstance(files, list) or not files:
        fail("report has no files array")
    out: list[tuple[str, str, int, int, int]] = []

    def walk(fn: object, path: str, prefix: str) -> None:
        if not isinstance(fn, dict):
            fail("report contains a function that is not an object")
        for field in ("name", "line", "cyclomatic", "cognitive"):
            if not isinstance(fn.get(field), (str, int)) or isinstance(
                fn.get(field), bool
            ):
                fail(f"report contains a function without a valid {field}")
        name = f"{prefix}{fn['name']}"
        out.append((name, path, int(fn["line"]), fn["cyclomatic"], fn["cognitive"]))
        children = fn.get("children", [])
        if not isinstance(children, list):
            fail(f"report function {name} has invalid children")
        for kid in children:
            walk(kid, path, f"{name}.")

    for entry in files:
        if not isinstance(entry, dict) or not isinstance(entry.get("path"), str):
            fail("report contains a file without a path")
        functions = entry.get("functions")
        if not isinstance(functions, list):
            fail(f"report file {entry['path']} has no functions array")
        for fn in functions:
            walk(fn, entry["path"], "")
    return out


def _source(path: str, cache: dict[str, str | None]) -> str | None:
    if path not in cache:
        candidate = ROOT / path
        if candidate.suffix.lower() != ".rs":
            cache[path] = None
        else:
            try:
                cache[path] = candidate.read_text(encoding="utf-8", errors="replace")
            except OSError:
                cache[path] = None
    return cache[path]


def classify(
    path: str, line: int, cyclomatic: int, cache: dict[str, str | None], max_cyc: int
) -> str:
    """Which disposition the terms of acceptance give one over-cap function."""
    source = _source(path, cache)
    if source is None:
        return "not_rust" if not path.endswith(".rs") else "unreadable"
    shape = dispatch_shape(source, line)
    if shape is None:
        return "unreadable"
    if shape.arms == 0:
        return "no_match"
    if shape.catch_alls:
        return "catch_all"
    residual = cyclomatic - shape.arms
    if residual < 1:
        return "unattributable"
    return "accepted" if residual <= max_cyc else "residual"


def _document(path: Path) -> dict[str, Any]:
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        fail(f"cannot read report {path}: {exc}")
    if not isinstance(document, dict):
        fail(f"report {path} is not a JSON object")
    return document


def _print_report(measured: int, over: int, cognitive: int, tally: Counter) -> int:
    accepted = tally["accepted"]
    backlog = over - accepted
    print(
        f"complexity terms: {measured} function(s) measured, "
        f"{over} over cyclomatic {CCCC_MAX_CYCLOMATIC} "
        f"or cognitive {CCCC_MAX_COGNITIVE}"
    )
    print(
        f"  cognitive over cap        {cognitive:5d}  "
        "genuinely complex -- never accepted"
    )
    for key, why in DISPOSITIONS:
        print(f"  {key:<25} {tally[key]:5d}  {why}")
    print(f"  ACCEPTED BY RULE          {accepted:5d}")
    print(f"  REAL BACKLOG              {backlog:5d}")
    return backlog


def report(path: Path) -> int:
    rows = _rows(_document(path))
    over = [
        row
        for row in rows
        if row[3] > CCCC_MAX_CYCLOMATIC or row[4] > CCCC_MAX_COGNITIVE
    ]
    cyclomatic_only = [row for row in over if row[4] <= CCCC_MAX_COGNITIVE]
    cache: dict[str, str | None] = {}
    tally = Counter(
        classify(row[1], row[2], row[3], cache, CCCC_MAX_CYCLOMATIC)
        for row in cyclomatic_only
    )
    return _print_report(len(rows), len(over), len(over) - len(cyclomatic_only), tally)


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    if len(argv) != 1:
        fail("usage: report_complexity_terms.py REPORT.json")
    report(Path(argv[0]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
