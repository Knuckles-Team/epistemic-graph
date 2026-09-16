"""Tests for the clippy-diagnostics tallying/reporting in
``scripts/clippy_census.py``.

This script had no test coverage before the ``main()`` decomposition into
``_finding_row`` / ``_tally`` / ``_print_report`` / ``_write_json_out``. These
tests pin the behavior those helpers now carry: rustc-only messages (no
``code``) and non-warning/error levels are excluded from the census, each
kept finding is attributed to its lint code and crate target, and
``--json-out`` writes exactly the per-finding rows to disk.
"""

from __future__ import annotations

import json

import pytest

from scripts.clippy_census import _finding_row, _tally, main

pytestmark = pytest.mark.no_engine


def _message_record(
    *,
    level: str = "warning",
    code: str | None = "clippy::needless_return",
    crate: str = "eg-modality",
    file: str = "src/lib.rs",
    line: int = 12,
    is_primary: bool = True,
) -> dict:
    spans = (
        [{"is_primary": is_primary, "file_name": file, "line_start": line}]
        if file is not None
        else []
    )
    return {
        "reason": "compiler-message",
        "target": {"name": crate},
        "message": {
            "level": level,
            "code": {"code": code} if code else None,
            "spans": spans,
        },
    }


def test_finding_row_extracts_lint_crate_file_line():
    row = _finding_row(_message_record())
    assert row == {
        "lint": "clippy::needless_return",
        "crate": "eg-modality",
        "file": "src/lib.rs",
        "line": 12,
    }


def test_finding_row_uncoded_message_reports_uncoded():
    row = _finding_row(_message_record(code=None))
    assert row is not None
    assert row["lint"] == "(uncoded)"


@pytest.mark.parametrize("level", ["note", "help", "failure-note"])
def test_finding_row_excludes_non_warning_error_levels(level):
    # A plain rustc note/help is not a clippy finding -- must not be counted.
    assert _finding_row(_message_record(level=level)) is None


def test_finding_row_no_primary_span_reports_unknown_location():
    row = _finding_row(_message_record(is_primary=False))
    assert row is not None
    assert row["file"] == "?"
    assert row["line"] == 0


def test_tally_counts_by_lint_and_crate_and_skips_non_findings():
    messages = [
        _message_record(code="clippy::needless_return", crate="eg-modality"),
        _message_record(code="clippy::needless_return", crate="eg-modality"),
        _message_record(code="clippy::too_many_arguments", crate="eg-server"),
        _message_record(level="note"),  # excluded
    ]
    by_lint, by_crate, rows = _tally(messages)
    assert by_lint["clippy::needless_return"] == 2
    assert by_lint["clippy::too_many_arguments"] == 1
    assert by_crate["eg-modality"] == 2
    assert by_crate["eg-server"] == 1
    assert len(rows) == 3


def test_main_writes_json_out_with_exact_rows(tmp_path, monkeypatch, capsys):
    messages = [
        _message_record(code="clippy::needless_return", crate="eg-modality"),
        _message_record(level="note"),
    ]
    monkeypatch.setattr(
        "scripts.clippy_census.run", lambda cargo, target_dir, jobs: messages
    )
    out_path = tmp_path / "clippy-rows.json"
    monkeypatch.setattr("sys.argv", ["clippy_census.py", "--json-out", str(out_path)])

    exit_code = main()

    assert exit_code == 0
    rows = json.loads(out_path.read_text(encoding="utf-8"))
    assert rows == [
        {
            "lint": "clippy::needless_return",
            "crate": "eg-modality",
            "file": "src/lib.rs",
            "line": 12,
        }
    ]
    captured = capsys.readouterr()
    assert "clippy census: 1 finding(s) across 1 crate target(s)" in captured.out
    assert f"per-finding rows written to {out_path}" in captured.out


def test_main_without_json_out_does_not_write_file(monkeypatch, tmp_path):
    monkeypatch.setattr("scripts.clippy_census.run", lambda cargo, target_dir, jobs: [])
    monkeypatch.setattr("sys.argv", ["clippy_census.py"])
    monkeypatch.chdir(tmp_path)

    exit_code = main()

    assert exit_code == 0
    assert list(tmp_path.iterdir()) == []
