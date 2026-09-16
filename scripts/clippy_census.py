#!/usr/bin/env python3
"""Count EVERY clippy lint in the workspace, in one pass.

The enforcing gate runs `cargo clippy ... -- -D warnings`, which is the right
way to FAIL a build and the wrong way to MEASURE one. `-D warnings` promotes
each lint to an error, so a crate stops at its own first lint and -- worse --
every crate that depends on it is never linted at all. The number that comes
back is therefore "lints in the first crate that had any", not "lints in the
workspace", and it moves unpredictably as fixes land: clearing one crate can
uncover a hundred lints in three crates behind it.

This separates the two jobs. It runs the same lint set WITHOUT `-D warnings` and
with `--keep-going`, so nothing is promoted to an error, nothing short-circuits,
and every crate is linted in a single pass. Diagnostics come back as JSON, so
each one is attributed to its lint name, crate and file rather than counted from
console text.

Reporting only. It never edits, never writes a baseline, and never gates.
"""

from __future__ import annotations

import argparse
import collections
import json
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def run(cargo: str, target_dir: str | None, jobs: int | None) -> list[dict]:
    command = [
        cargo,
        "clippy",
        "--workspace",
        "--all-targets",
        "--all-features",
        "--locked",
        "--keep-going",
        "--message-format=json",
    ]
    if jobs:
        command.extend(("-j", str(jobs)))
    environment = None
    if target_dir:
        import os

        environment = dict(os.environ, CARGO_TARGET_DIR=target_dir)
    process = subprocess.run(
        command,
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        env=environment,
        check=False,
    )
    messages = []
    for line in process.stdout.splitlines():
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        if record.get("reason") == "compiler-message":
            messages.append(record)
    return messages


def _finding_row(record: dict) -> dict | None:
    """The census row for one compiler message, or None if it isn't a finding.

    rustc's own errors are not clippy findings; keeping them out here (rather
    than filtering post hoc) means a compile failure can never be mistaken
    for a lint count.
    """
    message = record.get("message", {})
    if message.get("level") not in {"warning", "error"}:
        return None
    code = (message.get("code") or {}).get("code") or "(uncoded)"
    crate = record.get("target", {}).get("name", "?")
    spans = message.get("spans") or []
    primary = next((s for s in spans if s.get("is_primary")), None)
    file = primary.get("file_name") if primary else "?"
    line = primary.get("line_start") if primary else 0
    return {"lint": code, "crate": crate, "file": file, "line": line}


def _tally(
    messages: list[dict],
) -> tuple[collections.Counter[str], collections.Counter[str], list[dict]]:
    by_lint: collections.Counter[str] = collections.Counter()
    by_crate: collections.Counter[str] = collections.Counter()
    rows: list[dict] = []
    for record in messages:
        row = _finding_row(record)
        if row is None:
            continue
        by_lint[row["lint"]] += 1
        by_crate[row["crate"]] += 1
        rows.append(row)
    return by_lint, by_crate, rows


def _print_report(
    rows: list[dict],
    by_lint: collections.Counter[str],
    by_crate: collections.Counter[str],
) -> None:
    print(
        f"clippy census: {len(rows)} finding(s) across {len(by_crate)} crate target(s)"
    )
    print("\nby lint:")
    for code, count in by_lint.most_common():
        print(f"  {count:5d}  {code}")
    print("\nby crate target (top 20):")
    for crate, count in by_crate.most_common(20):
        print(f"  {count:5d}  {crate}")


def _write_json_out(path: str, rows: list[dict]) -> None:
    Path(path).write_text(json.dumps(rows, indent=2), encoding="utf-8")
    print(f"\nper-finding rows written to {path}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cargo", default="cargo")
    parser.add_argument("--target-dir")
    parser.add_argument("--jobs", type=int)
    parser.add_argument("--json-out")
    arguments = parser.parse_args()

    messages = run(arguments.cargo, arguments.target_dir, arguments.jobs)
    by_lint, by_crate, rows = _tally(messages)
    _print_report(rows, by_lint, by_crate)
    if arguments.json_out:
        _write_json_out(arguments.json_out, rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
