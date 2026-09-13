#!/usr/bin/env python3
"""Diff-scope filter for `scripts/check_kiss_staged.sh` (BUG-CX-136 follow-up, F6).

Bare KISS runs whole-file. That makes `kiss-changed-rust` fail a commit for
debt the commit did not touch -- a one-line comment added to a large
pre-existing file fails because KISS re-reports every violation already in
that file, not just what the diff changed. Lanes worked around this with
`git commit --no-verify`, which silently disables every other pre-commit
hook too.

This module narrows a KISS violation report to the findings the STAGED diff
is actually responsible for, by re-running KISS a second time on the HEAD
blob of the same file (in a separate temporary tree) and comparing:

  * Rules that live inside one function body (`statements_per_function`,
    `returns_per_function`, `calls_per_function`, ...): a violation counts
    only if the enclosing function is NEW (no same-named function existed at
    HEAD) or MODIFIED (the same-named function's exact source text differs
    from its HEAD version). An untouched pre-existing violator is not
    re-reported. This is a CONTENT comparison, not a line-number or bare
    symbol-name comparison -- extraction and mechanical merges routinely
    shift both without changing meaning (see `symbol-keyed-baselines-
    break-under-extraction` / `architecture-gates-key-on-byte-offsets`).
  * Rules that aggregate an entire file, or an entire type's method count
    across every `impl` block in the file (`AGGREGATE_RULES` below): there
    is no single contiguous span to diff, so these are compared by
    MAGNITUDE against the same (rule, item_name) key in the HEAD report. A
    violation counts only if the rule is newly crossed (absent from the HEAD
    report) or the reported count is strictly larger than the HEAD count
    (worsened). An unchanged or improved count is pre-existing debt and does
    not count.

No baseline file, no self-updating count, no allowlist: every comparison is
computed at run time from two ephemeral `kiss check` reports (the staged
tree and the HEAD blob), never from a stored number.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from rust_lexer import _balanced_span_from, _rust_code_mask  # noqa: E402

VIOLATION_RE = re.compile(
    r"^VIOLATION:(?P<rule>[^:]+):(?P<path>[^:]*):(?P<line>\d+):(?P<name>[^:]*):"
    r"\s?(?P<message>.*)$"
)

# Rules whose count is a property of the WHOLE file (or, for
# methods_per_class, of one type's method count summed across every `impl`
# block scattered through the file) rather than of one contiguous function
# body. These cannot be attributed by comparing "the enclosing item's
# content" -- there is no single span whose text identity proves the metric
# is unchanged -- so they are compared by MAGNITUDE instead, keyed by
# (rule, item_name) between the staged and HEAD reports for the same file.
AGGREGATE_RULES = frozenset(
    {
        "statements_per_file",
        "lines_per_file",
        "functions_per_file",
        "interface_types_per_file",
        "concrete_types_per_file",
        "imported_names_per_file",
        "methods_per_class",
    }
)

_INT_RE = re.compile(r"\d+")
_FN_PATTERN_CACHE: dict[str, re.Pattern[str]] = {}


def _fn_pattern(name: str) -> re.Pattern[str]:
    pattern = _FN_PATTERN_CACHE.get(name)
    if pattern is None:
        pattern = re.compile(r"\bfn\s+" + re.escape(name) + r"\s*[(<]")
        _FN_PATTERN_CACHE[name] = pattern
    return pattern


def _line_start(source: str, index: int) -> int:
    return source.rfind("\n", 0, index) + 1


def _line_number(source: str, index: int) -> int:
    return source.count("\n", 0, index) + 1


def _find_body_open(masked: str, start: int) -> int | None:
    """First top-level `{` after a `fn name` match, or `None` for a body-less item."""

    depth = 0
    index = start
    while index < len(masked):
        char = masked[index]
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        elif char == "{" and depth <= 0:
            return index
        elif char == ";" and depth <= 0:
            return None
        index += 1
    return None


def function_spans(source: str, name: str) -> list[tuple[int, int, str]]:
    """Every `fn <name>` occurrence in `source`: `(start_line, end_line, text)`.

    Comment/string content is masked (via the shared `rust_lexer` authority
    every other Rust scanner in this repository uses) before the regex and
    brace search run, so a comment or string literal that happens to spell
    `fn <name>` is never mistaken for a definition. Occurrences are returned
    in file order; a same-named function found N times is matched to the
    N-th same-named function in the other tree by this ordinal position.
    """

    masked = _rust_code_mask(source)
    spans: list[tuple[int, int, str]] = []
    for match in _fn_pattern(name).finditer(masked):
        start = _line_start(source, match.start())
        start_line = _line_number(source, match.start())
        body_open = _find_body_open(masked, match.end())
        if body_open is None:
            terminator = masked.find(";", match.end())
            end = terminator if terminator != -1 else len(source) - 1
        else:
            end = _balanced_span_from(masked, body_open, "{", "}")
        end_line = _line_number(source, end)
        spans.append((start_line, end_line, source[start : end + 1]))
    return spans


def _first_int(message: str) -> int | None:
    match = _INT_RE.search(message)
    return int(match.group()) if match else None


def parse_report(text: str | None) -> list[dict[str, str]]:
    if not text:
        return []
    violations = []
    for line in text.splitlines():
        match = VIOLATION_RE.match(line)
        if match:
            violations.append(match.groupdict())
    return violations


def _select_span(
    spans: list[tuple[int, int, str]], line: int
) -> tuple[int, tuple[int, int, str]] | None:
    """The span containing `line`, plus its ordinal position among `spans`."""

    for index, span in enumerate(spans):
        start_line, end_line, _ = span
        if start_line <= line <= end_line:
            return index, span
    return None


def _head_aggregate_magnitudes(
    head_violations: list[dict[str, str]],
) -> dict[tuple[str, str], int]:
    """The largest reported count per (rule, item_name), for aggregate rules only."""

    magnitudes: dict[tuple[str, str], int] = {}
    for violation in head_violations:
        if violation["rule"] not in AGGREGATE_RULES:
            continue
        magnitude = _first_int(violation["message"])
        if magnitude is None:
            continue
        key = (violation["rule"], violation["name"])
        magnitudes[key] = max(magnitudes.get(key, -1), magnitude)
    return magnitudes


def _aggregate_is_attributable(
    violation: dict[str, str], head_magnitudes: dict[tuple[str, str], int]
) -> bool:
    """A file-/type-aggregate finding counts iff newly crossed or worsened."""

    head_magnitude = head_magnitudes.get((violation["rule"], violation["name"]))
    if head_magnitude is None:
        return True  # rule absent from the HEAD report: newly crossed
    magnitude = _first_int(violation["message"])
    return magnitude is not None and magnitude > head_magnitude


class _SpanLookup:
    """Cached, by-name `function_spans` lookup over one source tree."""

    def __init__(self, source: str | None) -> None:
        self._source = source
        self._cache: dict[str, list[tuple[int, int, str]]] = {}

    def __call__(self, name: str) -> list[tuple[int, int, str]]:
        if self._source is None:
            return []
        if name not in self._cache:
            self._cache[name] = function_spans(self._source, name)
        return self._cache[name]


def _function_is_attributable(
    violation: dict[str, str], staged_spans: _SpanLookup, head_spans: _SpanLookup
) -> bool:
    """A function-scoped finding counts iff the enclosing item is new or changed."""

    selected = _select_span(staged_spans(violation["name"]), int(violation["line"]))
    if selected is None:
        # Defensive: KISS reported this rule at a line the extractor could
        # not re-locate in the staged source. Fail closed rather than
        # silently drop a finding.
        return True

    ordinal, (_, _, staged_text) = selected
    candidates = head_spans(violation["name"])
    if ordinal >= len(candidates):
        return True  # no same-named function at HEAD: newly added
    return candidates[ordinal][2] != staged_text  # same name, body changed?


def attributable_violations(
    staged_source: str,
    staged_report: str,
    head_source: str | None,
    head_report: str | None,
) -> list[dict[str, str]]:
    """The subset of `staged_report`'s violations the staged diff caused.

    `head_source`/`head_report` are `None` for a file with no HEAD blob (a
    newly added file) -- every staged violation is then attributable, since
    there is no prior state to have been pre-existing debt against.
    """

    staged_violations = parse_report(staged_report)
    if head_source is None:
        return staged_violations

    head_magnitudes = _head_aggregate_magnitudes(parse_report(head_report))
    staged_spans = _SpanLookup(staged_source)
    head_spans = _SpanLookup(head_source)

    return [
        violation
        for violation in staged_violations
        if (
            _aggregate_is_attributable(violation, head_magnitudes)
            if violation["rule"] in AGGREGATE_RULES
            else _function_is_attributable(violation, staged_spans, head_spans)
        )
    ]


def _format(violation: dict[str, str]) -> str:
    return (
        f"VIOLATION:{violation['rule']}:{violation['path']}:{violation['line']}:"
        f"{violation['name']}: {violation['message']}"
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--staged-source", required=True, type=Path)
    parser.add_argument("--staged-report", required=True, type=Path)
    parser.add_argument("--head-source", type=Path)
    parser.add_argument("--head-report", type=Path)
    args = parser.parse_args(argv)

    staged_source = args.staged_source.read_text(encoding="utf-8")
    staged_report = args.staged_report.read_text(encoding="utf-8")
    head_source = (
        args.head_source.read_text(encoding="utf-8")
        if args.head_source is not None and args.head_source.is_file()
        else None
    )
    head_report = (
        args.head_report.read_text(encoding="utf-8")
        if args.head_report is not None and args.head_report.is_file()
        else None
    )

    for violation in attributable_violations(
        staged_source, staged_report, head_source, head_report
    ):
        print(_format(violation))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
