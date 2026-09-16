#!/usr/bin/env python3
"""Validate the machine-readable CCCC census report.

CCCC omits a clean file's ``parse_errors`` field, but its top-level summary
always carries the required counts and recursive function metrics.  A missing,
malformed, or internally inconsistent native report is an environment failure,
never an advisory clean result.  Empty ``functions`` arrays are valid when the
file and summary metrics truthfully report zero functions.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path
from typing import Any, NamedTuple, NoReturn

SUMMARY_METRICS = ("sum", "max", "median", "p90", "p95")
U32_MAX = (1 << 32) - 1


class ValidatedFunction(NamedTuple):
    """One native function row after schema and tree validation."""

    name: str
    path: str
    line: int
    cognitive: int
    cyclomatic: int


class ValidatedReport(NamedTuple):
    """Native report data shared by the validator and terms classifier."""

    files: list[dict[str, Any]]
    functions: list[ValidatedFunction]


def _integer(
    value: object, label: str, *, minimum: int, maximum: int | None = None
) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or value < minimum
        or (maximum is not None and value > maximum)
    ):
        bound = (
            f" between {minimum} and {maximum}"
            if maximum is not None
            else f" >= {minimum}"
        )
        fail(f"{label} is not an integer{bound}")
    return value


def _u32(value: object, label: str, *, minimum: int) -> int:
    return _integer(value, label, minimum=minimum, maximum=U32_MAX)


def _count(value: object, label: str) -> int:
    return _integer(value, label, minimum=0)


def _nonempty_string(value: object, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        fail(f"{label} is not a nonempty string")
    return value


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


def _validate_summary_metric(summary: dict[str, Any], metric: str, path: Path) -> None:
    values = summary.get(metric)
    if not isinstance(values, dict):
        fail(f"report {path} summary.{metric} is not an object")
    for field in SUMMARY_METRICS:
        _u32(values.get(field), f"report {path} summary.{metric}.{field}", minimum=0)


def _summary(document: dict[str, Any], path: Path) -> dict[str, Any]:
    summary = document.get("summary")
    if not isinstance(summary, dict):
        fail(f"report {path} has no top-level summary")
    for field in (
        "file_count",
        "function_count",
        "parse_error_count",
        "parse_error_file_count",
    ):
        _count(summary.get(field), f"report {path} summary.{field}")
    for metric in ("cognitive", "cyclomatic"):
        _validate_summary_metric(summary, metric, path)
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


def _validate_function(
    fn: object, index: int, path: Path, prefix: str
) -> tuple[str, int, int, int, list[object]]:
    if not isinstance(fn, dict):
        fail(f"report {path} function {index} is not an object")
    name = _nonempty_string(fn.get("name"), f"report {path} function {index}.name")
    _nonempty_string(fn.get("kind"), f"report {path} function {index}.kind")
    line = _u32(fn.get("line"), f"report {path} function {index}.line", minimum=1)
    cognitive = _u32(
        fn.get("cognitive"),
        f"report {path} function {index}.cognitive",
        minimum=0,
    )
    cyclomatic = _u32(
        fn.get("cyclomatic"),
        f"report {path} function {index}.cyclomatic",
        minimum=1,
    )
    children = fn.get("children", [])
    if not isinstance(children, list):
        fail(f"report {path} function {index}.children is not an array")
    return f"{prefix}{name}", line, cognitive, cyclomatic, children


def _parse_error_count(entry: dict[str, Any], index: int, path: Path) -> int:
    errors = entry.get("parse_errors", [])
    if not isinstance(errors, list):
        fail(f"report {path} file {index} has invalid parse_errors")
    if any(not isinstance(error, str) or not error for error in errors):
        fail(f"report {path} file {index} has invalid parse_errors")
    return len(errors)


def _walk_function_tree(
    functions: list[object], file_path: str, path: Path
) -> list[ValidatedFunction]:
    rows: list[ValidatedFunction] = []
    stack: list[tuple[object, str, int]] = [
        (fn, "", child_index)
        for child_index, fn in reversed(list(enumerate(functions)))
    ]
    while stack:
        fn, prefix, function_index = stack.pop()
        qualified, line, cognitive, cyclomatic, children = _validate_function(
            fn, function_index, path, prefix
        )
        rows.append(
            ValidatedFunction(qualified, file_path, line, cognitive, cyclomatic)
        )
        for child_index, child in reversed(list(enumerate(children))):
            stack.append((child, f"{qualified}.", child_index))
    return rows


def _validate_file_totals(
    file_cognitive: int,
    file_cyclomatic: int,
    rows: list[ValidatedFunction],
    index: int,
    path: Path,
) -> None:
    cognitive_sum = sum(row.cognitive for row in rows)
    cyclomatic_sum = sum(row.cyclomatic for row in rows)
    if file_cognitive < cognitive_sum:
        fail(
            f"report {path} file {index}.cognitive={file_cognitive} is below "
            f"recursive function total {cognitive_sum}"
        )
    if file_cyclomatic < cyclomatic_sum:
        fail(
            f"report {path} file {index}.cyclomatic={file_cyclomatic} is below "
            f"recursive function total {cyclomatic_sum}"
        )


def _validate_file(
    entry: dict[str, Any], index: int, path: Path
) -> tuple[str, int, int, list[ValidatedFunction], int]:
    file_path = _nonempty_string(entry.get("path"), f"report {path} file {index}.path")
    file_cognitive = _u32(
        entry.get("cognitive"), f"report {path} file {index}.cognitive", minimum=0
    )
    file_cyclomatic = _u32(
        entry.get("cyclomatic"), f"report {path} file {index}.cyclomatic", minimum=0
    )
    functions = entry.get("functions")
    if not isinstance(functions, list):
        fail(f"report {path} file {index}.functions is not an array")
    rows = _walk_function_tree(functions, file_path, path)
    parse_error_count = _parse_error_count(entry, index, path)
    _validate_file_totals(file_cognitive, file_cyclomatic, rows, index, path)
    return file_path, file_cognitive, file_cyclomatic, rows, parse_error_count


def _percentile(values: list[int], fraction: float) -> int:
    if not values:
        return 0
    ordered = sorted(values)
    rank = max(1, math.ceil(len(ordered) * fraction))
    return ordered[rank - 1]


def _metric_summary(values: list[int]) -> dict[str, int]:
    return {
        "sum": sum(values),
        "max": max(values, default=0),
        "median": _percentile(values, 0.50),
        "p90": _percentile(values, 0.90),
        "p95": _percentile(values, 0.95),
    }


def _validate_summary_counts(
    summary: dict[str, Any],
    path: Path,
    file_count: int,
    rows: list[ValidatedFunction],
    parse_error_count: int,
    parse_error_file_count: int,
) -> None:
    expected_counts = {
        "file_count": file_count,
        "function_count": len(rows),
        "parse_error_count": parse_error_count,
        "parse_error_file_count": parse_error_file_count,
    }
    for field, expected in expected_counts.items():
        actual = summary[field]
        if actual != expected:
            fail(
                f"report {path} summary.{field}={actual} does not match "
                f"native report value {expected}"
            )
    for field, values in (
        ("cognitive", [row.cognitive for row in rows]),
        ("cyclomatic", [row.cyclomatic for row in rows]),
    ):
        expected_metrics = _metric_summary(values)
        if summary[field] != expected_metrics:
            fail(
                f"report {path} summary.{field} does not match recursive "
                f"native function metrics: expected {expected_metrics!r}"
            )


def validate_document(
    document: dict[str, Any],
    path: Path,
    source_manifest: Path | None = None,
) -> ValidatedReport:
    summary = _summary(document, path)
    files = _files(document, path)
    rows: list[ValidatedFunction] = []
    file_paths: list[str] = []
    parse_error_count = 0
    parse_error_file_count = 0
    for index, entry in enumerate(files):
        file_path, _, _, file_rows, file_parse_errors = _validate_file(
            entry, index, path
        )
        file_paths.append(file_path)
        rows.extend(file_rows)
        parse_error_count += file_parse_errors
        parse_error_file_count += int(bool(file_parse_errors))
    if len(set(file_paths)) != len(file_paths):
        fail(f"report {path} contains duplicate measured paths")
    _validate_summary_counts(
        summary,
        path,
        len(files),
        rows,
        parse_error_count,
        parse_error_file_count,
    )
    if parse_error_count:
        fail(f"report {path} has {parse_error_count} parse error(s)")
    if source_manifest is not None:
        _validate_exact_paths(files, source_manifest, path)
    return ValidatedReport(files, rows)


def _read_source_manifest(path: Path) -> list[str]:
    try:
        raw = path.read_bytes()
    except OSError as exc:
        fail(f"cannot read source manifest {path}: {exc}")
    if not raw or not raw.endswith(b"\0"):
        fail(f"source manifest {path} is not a nonempty NUL-delimited file")
    encoded_paths = raw[:-1].split(b"\0")
    if any(not encoded_path for encoded_path in encoded_paths):
        fail(f"source manifest {path} contains an empty path")
    try:
        paths = [encoded_path.decode("utf-8") for encoded_path in encoded_paths]
    except UnicodeDecodeError as exc:
        fail(f"source manifest {path} is not valid UTF-8: {exc}")
    if len(set(paths)) != len(paths):
        fail(f"source manifest {path} contains duplicate paths")
    return paths


def _validate_exact_paths(
    files: list[dict[str, Any]], manifest: Path, report: Path
) -> None:
    requested = _read_source_manifest(manifest)
    measured: list[str] = []
    for index, entry in enumerate(files):
        path = entry.get("path")
        if not isinstance(path, str) or not path:
            fail(f"report {report} file {index} has no valid path")
        measured.append(path)
    if len(set(measured)) != len(measured):
        fail(f"report {report} contains duplicate measured paths")
    missing = sorted(set(requested) - set(measured))
    extra = sorted(set(measured) - set(requested))
    if missing or extra:
        details = []
        if missing:
            details.append(f"missing={missing!r}")
        if extra:
            details.append(f"extra={extra!r}")
        message = ", ".join(details)
        fail(f"report {report} does not exactly cover source manifest: {message}")


def validate_report(path: Path, source_manifest: Path | None = None) -> int:
    document = _read_report(path)
    validated = validate_document(document, path, source_manifest)
    print(
        f"cccc census: {len(validated.files)} tracked source file(s), "
        "findings are advisory"
    )
    return len(validated.files)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source-manifest",
        type=Path,
        help="require the report's measured paths to exactly match this NUL manifest",
    )
    parser.add_argument("report", type=Path, metavar="REPORT.json")
    args = parser.parse_args(sys.argv[1:] if argv is None else argv)
    validate_report(args.report, args.source_manifest)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
