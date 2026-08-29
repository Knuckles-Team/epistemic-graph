#!/usr/bin/env python3
"""Validate the machine-readable CCCC census report.

CCCC omits a clean file's ``parse_errors`` field, but its top-level summary
always carries the required ``parse_error_count``.  A missing or malformed
summary is an environment failure, never an advisory clean result.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, NoReturn


def fail(message: str) -> NoReturn:
    print(f"cccc census: CANNOT RUN: {message}", file=sys.stderr)
    raise SystemExit(2)


def _read_report(path: Path) -> dict[str, Any]:
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        fail(f"cannot read report {path}: {exc}")
    if not isinstance(document, dict):
        fail(f"report {path} is not a JSON object")
    return document


def _summary(document: dict[str, Any], path: Path) -> dict[str, Any]:
    summary = document.get("summary")
    if not isinstance(summary, dict):
        fail(f"report {path} has no top-level summary")
    parse_count = summary.get("parse_error_count")
    if (
        isinstance(parse_count, bool)
        or not isinstance(parse_count, int)
        or parse_count < 0
    ):
        fail(f"report {path} has no valid summary.parse_error_count")
    if parse_count:
        fail(f"report {path} has {parse_count} top-level parse error(s)")
    return summary


def _files(document: dict[str, Any], path: Path) -> list[dict[str, Any]]:
    files = document.get("files")
    if not isinstance(files, list) or not files:
        fail(f"report {path} has no files array")
    result = []
    for index, entry in enumerate(files):
        if not isinstance(entry, dict):
            fail(f"report {path} file {index} is not an object")
        result.append(entry)
    return result


def _validate_file_parse_errors(entry: dict[str, Any], index: int, path: Path) -> None:
    errors = entry.get("parse_errors", [])
    if not isinstance(errors, list):
        fail(f"report {path} file {index} has invalid parse_errors")
    if any(not isinstance(error, str) or not error for error in errors):
        fail(f"report {path} file {index} has invalid parse_errors")
    if errors:
        fail(f"report {path} file {index} has parse errors")


def validate_report(path: Path) -> int:
    document = _read_report(path)
    _summary(document, path)
    files = _files(document, path)
    for index, entry in enumerate(files):
        _validate_file_parse_errors(entry, index, path)
    print(f"cccc census: {len(files)} tracked source file(s), findings are advisory")
    return len(files)


def main(argv: list[str] | None = None) -> int:
    if argv is None:
        argv = sys.argv[1:]
    if len(argv) != 1:
        fail("usage: validate_cccc_census.py REPORT.json")
    validate_report(Path(argv[0]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
