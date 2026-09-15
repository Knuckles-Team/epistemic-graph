"""Fixtures for the BUG-CX-136 diff-scoping filter (F6).

`scripts/kiss_diff_scope.py` narrows a raw KISS report to the findings a
staged diff is actually responsible for. These are the four scenarios the
program brief calls out by letter:

  (a) a comment-only change to a file with a pre-existing violation -> PASS
      (the violating function/item is untouched, so its finding is
      pre-existing debt, not something this diff caused).
  (b) a new function that violates -> FAIL (no same-named function existed
      at HEAD, so the finding is entirely attributable to the diff).
  (c) modifying a function that already violates -> FAIL ("touching debt
      means fixing it": the enclosing item's content differs from HEAD).
  (d) a file-level threshold newly crossed -> FAIL (the aggregate rule was
      absent from the HEAD report for this file).

Each is proven directly against `attributable_violations` with synthetic
Rust source pairs and synthetic KISS report text -- no real `kiss` binary,
no git, no subprocess -- so the test is fast, deterministic, and exercises
exactly the comparison logic the hook depends on. `tests/test_kiss_staged.py`
separately covers the bash-level wiring (materializing HEAD_ROOT, invoking
this module, propagating its exit status) with the existing fake-`kiss`-stub
pattern that file already uses for the rest of the hook.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "scripts"))

from kiss_diff_scope import (  # noqa: E402
    attributable_violations,
    function_spans,
    parse_report,
)

pytestmark = pytest.mark.no_engine


def _violation(
    rule: str = "statements_per_function",
    line: int = 1,
    name: str = "target_fn",
    message: str = "Function 'target_fn' has 90 statements (threshold: 35)",
    path: str = "/tmp/example.rs",
) -> str:
    return f"VIOLATION:{rule}:{path}:{line}:{name}: {message}"


# ---------------------------------------------------------------------------
# (a) comment-only change to a file with a pre-existing violation -> PASS
# ---------------------------------------------------------------------------


def test_comment_only_change_suppresses_pre_existing_function_violation() -> None:
    head_source = (
        "fn target_fn() {\n    let a = 1;\n    let b = 2;\n    let _ = a + b;\n}\n"
    )
    staged_source = "// a harmless top-of-file comment\n" + head_source
    # The function itself did not move relative to the comment addition in
    # this fixture except by one line -- the point under test is that its
    # BODY TEXT is identical, which is what the matcher actually keys on.
    staged_report = _violation(line=2)
    head_report = _violation(line=1)

    result = attributable_violations(
        staged_source, staged_report, head_source, head_report
    )

    assert result == []


def test_multiple_pre_existing_violations_on_the_same_untouched_function_all_pass() -> (
    None
):
    head_source = "fn target_fn() {\n    let a = 1;\n    let _ = a;\n}\n"
    staged_source = "// comment\n" + head_source
    staged_report = "\n".join(
        [
            _violation(rule="statements_per_function", line=2),
            _violation(rule="returns_per_function", line=2),
        ]
    )
    head_report = "\n".join(
        [
            _violation(rule="statements_per_function", line=1),
            _violation(rule="returns_per_function", line=1),
        ]
    )

    result = attributable_violations(
        staged_source, staged_report, head_source, head_report
    )

    assert result == []


# ---------------------------------------------------------------------------
# (b) a new function that violates -> FAIL
# ---------------------------------------------------------------------------


def test_new_function_violation_is_attributable() -> None:
    head_source = "fn other_fn() {\n    let a = 1;\n    let _ = a;\n}\n"
    staged_source = head_source + (
        "\nfn target_fn() {\n    let a = 1;\n    let _ = a;\n}\n"
    )
    staged_report = _violation(line=5)
    head_report = ""  # no violation existed at HEAD; target_fn did not exist

    result = attributable_violations(
        staged_source, staged_report, head_source, head_report
    )

    assert len(result) == 1
    assert result[0]["name"] == "target_fn"


def test_brand_new_file_is_fully_attributable() -> None:
    staged_source = "fn target_fn() {\n    let a = 1;\n    let _ = a;\n}\n"
    staged_report = _violation(line=1)

    result = attributable_violations(staged_source, staged_report, None, None)

    assert result == parse_report(staged_report)


# ---------------------------------------------------------------------------
# (c) modifying a function that already violates -> FAIL
# ---------------------------------------------------------------------------


def test_modified_violating_function_is_attributable() -> None:
    head_source = (
        "fn target_fn() {\n    let a = 1;\n    let b = 2;\n    let _ = a + b;\n}\n"
    )
    staged_source = (
        "fn target_fn() {\n"
        "    let a = 1;\n"
        "    let b = 2;\n"
        "    let c = 3;\n"  # the diff touches this function's body
        "    let _ = a + b + c;\n"
        "}\n"
    )
    staged_report = _violation(line=1)
    head_report = _violation(line=1)

    result = attributable_violations(
        staged_source, staged_report, head_source, head_report
    )

    assert len(result) == 1
    assert result[0]["name"] == "target_fn"


def test_second_same_named_occurrence_is_matched_positionally() -> None:
    # Two impls each define `fn new`. Only the SECOND one is modified by the
    # diff; the finding on the first (untouched) occurrence must still pass.
    head_source = (
        "impl A {\n    fn new() {\n        let a = 1;\n        let _ = a;\n    }\n}\n"
        "impl B {\n    fn new() {\n        let a = 1;\n        let _ = a;\n    }\n}\n"
    )
    staged_source = (
        "impl A {\n    fn new() {\n        let a = 1;\n        let _ = a;\n    }\n}\n"
        "impl B {\n    fn new() {\n        let a = 2;\n        let _ = a;\n    }\n}\n"
    )
    staged_report = "\n".join(
        [
            _violation(rule="returns_per_function", line=2, name="new"),
            _violation(rule="returns_per_function", line=8, name="new"),
        ]
    )
    head_report = "\n".join(
        [
            _violation(rule="returns_per_function", line=2, name="new"),
            _violation(rule="returns_per_function", line=8, name="new"),
        ]
    )

    result = attributable_violations(
        staged_source, staged_report, head_source, head_report
    )

    assert len(result) == 1
    assert result[0]["line"] == "8"


# ---------------------------------------------------------------------------
# (d) a file-level threshold newly crossed -> FAIL
# ---------------------------------------------------------------------------


def test_newly_crossed_file_level_threshold_is_attributable() -> None:
    head_source = "fn a() {}\n"
    staged_source = head_source + "// padding\n" * 900
    staged_report = _violation(
        rule="lines_per_file",
        line=1,
        name="example.rs",
        message="File has 901 lines (threshold: 900) Split the file roughly in half.",
    )
    head_report = ""  # file was under threshold at HEAD; rule absent

    result = attributable_violations(
        staged_source, staged_report, head_source, head_report
    )

    assert len(result) == 1
    assert result[0]["rule"] == "lines_per_file"


def test_file_level_finding_already_present_and_not_worsened_is_suppressed() -> None:
    head_source = "fn a() {}\n" + "// padding\n" * 950
    staged_source = head_source  # unrelated change elsewhere does not grow it
    staged_report = _violation(
        rule="lines_per_file",
        line=1,
        name="example.rs",
        message="File has 951 lines (threshold: 900) Split the file roughly in half.",
    )
    head_report = _violation(
        rule="lines_per_file",
        line=1,
        name="example.rs",
        message="File has 951 lines (threshold: 900) Split the file roughly in half.",
    )

    result = attributable_violations(
        staged_source, staged_report, head_source, head_report
    )

    assert result == []


def test_file_level_finding_worsened_is_attributable() -> None:
    head_source = "fn a() {}\n" + "// padding\n" * 950
    staged_source = head_source + "// padding\n" * 10
    staged_report = _violation(
        rule="lines_per_file",
        line=1,
        name="example.rs",
        message="File has 961 lines (threshold: 900) Split the file roughly in half.",
    )
    head_report = _violation(
        rule="lines_per_file",
        line=1,
        name="example.rs",
        message="File has 951 lines (threshold: 900) Split the file roughly in half.",
    )

    result = attributable_violations(
        staged_source, staged_report, head_source, head_report
    )

    assert len(result) == 1


def test_methods_per_class_is_treated_as_aggregate_not_content_matched() -> None:
    # methods_per_class counts a type's methods across every impl block in
    # the file -- there is no single contiguous span to diff, so it must be
    # magnitude-compared like a file-level rule, keyed by (rule, item_name).
    head_source = "impl Foo {\n    fn a() {}\n}\n"
    staged_source = head_source + "impl Foo {\n    fn b() {}\n}\n"
    staged_report = _violation(
        rule="methods_per_class",
        line=1,
        name="Foo",
        message="Type 'Foo' has 14 methods (threshold: 13)",
    )
    head_report = ""

    result = attributable_violations(
        staged_source, staged_report, head_source, head_report
    )

    assert len(result) == 1
    assert result[0]["rule"] == "methods_per_class"


# ---------------------------------------------------------------------------
# Extraction mechanics
# ---------------------------------------------------------------------------


def test_function_spans_ignores_the_name_inside_a_comment_or_string() -> None:
    source = (
        "// fn target_fn() looks like a definition but is not\n"
        'const NOTE: &str = "fn target_fn() also not a definition";\n'
        "fn target_fn() {\n"
        "    let a = 1;\n"
        "    let _ = a;\n"
        "}\n"
    )

    spans = function_spans(source, "target_fn")

    assert len(spans) == 1
    start_line, end_line, text = spans[0]
    assert start_line == 3
    assert text.startswith("fn target_fn()")


def test_parse_report_ignores_non_violation_lines() -> None:
    report = (
        "Analyzed: 1 files, 3 code_units, 5 statements, 1 graph_nodes, 0 graph_edges\n"
        + _violation()
        + "\n"
        "Run 'kiss rules' for more information about fixing violations.\n"
        "kiss: 1.23s\n"
    )

    parsed = parse_report(report)

    assert len(parsed) == 1
    assert parsed[0]["name"] == "target_fn"
