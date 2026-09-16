"""Tests for ``scripts/dupehound_ledger.py``.

This script had NO test coverage before ``normalized_function_text`` and
``partition`` were decomposed (the former into
``_find_declaration_start``/``_collect_brace_balanced_block``, the latter into
``_pair_key``/``_pair_note``/``_pair_rot_reason``/``_classify_registered_finding``).
These tests pin the pre-existing, user-facing behaviour of both functions
directly against real files on disk, so a regression in either decomposition
fails a test rather than surfacing only via the pre-commit dupehound hook.
"""

from __future__ import annotations

import pytest

from scripts.dupehound_ledger import (
    DistinctPair,
    digest_of,
    normalized_function_text,
    partition,
)

# Static tests against real files on a tmp_path -- never touches the compiled
# engine.
pytestmark = pytest.mark.no_engine


FOO_BODY = "fn foo(x: i32) -> i32 {\n    x + 1\n}\n"
BAR_BODY = "fn bar(x: i32) -> i32 {\n    x + 1\n}\n"


def _write(tmp_path, name: str, content: str):
    path = tmp_path / name
    path.write_text(content, encoding="utf-8")
    return path


# ── normalized_function_text ────────────────────────────────────────────────


def test_normalized_function_text_finds_declaration_near_hint(tmp_path):
    path = _write(tmp_path, "a.rs", "// header\n" + FOO_BODY)
    text = normalized_function_text(path, 2, "foo")
    assert text == "fn foo(x: i32) -> i32 { x + 1 }"


def test_normalized_function_text_falls_back_to_whole_file_search(tmp_path):
    # The hint line is far from the real declaration -- must still find it by
    # scanning the whole file, not just the +/-3 window around the hint.
    padding = "\n".join(f"// filler line {i}" for i in range(40))
    path = _write(tmp_path, "a.rs", padding + "\n" + FOO_BODY)
    text = normalized_function_text(path, 1, "foo")
    assert text == "fn foo(x: i32) -> i32 { x + 1 }"


def test_normalized_function_text_missing_function_returns_none(tmp_path):
    path = _write(tmp_path, "a.rs", BAR_BODY)
    assert normalized_function_text(path, 1, "foo") is None


def test_normalized_function_text_missing_file_returns_none(tmp_path):
    assert normalized_function_text(tmp_path / "missing.rs", 1, "foo") is None


def test_normalized_function_text_collapses_rewrapped_whitespace(tmp_path):
    # rustfmt re-wrapping onto more lines (same tokens, different whitespace
    # runs) must not change the pinned digest.
    one_line = _write(tmp_path, "a.rs", FOO_BODY)
    wrapped = _write(
        tmp_path,
        "b.rs",
        "fn foo(x: i32)\n    -> i32\n{\n    x\n    +\n    1\n}\n",
    )
    assert normalized_function_text(one_line, 1, "foo") == normalized_function_text(
        wrapped, 1, "foo"
    )


def test_normalized_function_text_out_of_range_hint_still_finds_function(tmp_path):
    # A HEAD-relative line hint can point past the end of the worktree file;
    # the lookup must still succeed via the whole-file fallback.
    path = _write(tmp_path, "a.rs", FOO_BODY)
    assert normalized_function_text(path, 999, "foo") is not None


# ── partition ────────────────────────────────────────────────────────────────


def _finding(file: str, name: str, original_file: str, original_name: str) -> dict:
    return {
        "file": file,
        "name": name,
        "line": 1,
        "original_file": original_file,
        "original_name": original_name,
        "original_line": 1,
    }


def _pinned_digest(path, name: str) -> str:
    text = normalized_function_text(path, 1, name)
    assert text is not None, f"fixture function {name!r} not found in {path}"
    return digest_of(text)


def _pin(tmp_path, left_file, left_name, left_body, right_file, right_name, right_body):
    """A DistinctPair pinned against whatever's on disk right now."""
    _write(tmp_path, left_file, left_body)
    _write(tmp_path, right_file, right_body)
    left_digest = _pinned_digest(tmp_path / left_file, left_name)
    right_digest = _pinned_digest(tmp_path / right_file, right_name)
    return DistinctPair(
        left_file=left_file,
        left_name=left_name,
        left_digest=left_digest,
        right_file=right_file,
        right_name=right_name,
        right_digest=right_digest,
        reason="two structurally identical functions over disjoint domains",
        reviewed_on="2026-01-01",
    )


def test_partition_reports_unregistered_finding_as_a_gate_failure(tmp_path):
    _write(tmp_path, "a.rs", FOO_BODY)
    _write(tmp_path, "b.rs", BAR_BODY)
    finding = _finding("a.rs", "foo", "b.rs", "bar")

    unregistered, changed, notes, rotted = partition([finding], [], root=tmp_path)

    assert unregistered == [finding]
    assert changed == []
    assert rotted == []


def test_partition_registered_unchanged_pair_is_neither_unregistered_nor_changed(
    tmp_path,
):
    pair = _pin(tmp_path, "a.rs", "foo", FOO_BODY, "b.rs", "bar", BAR_BODY)
    finding = _finding("a.rs", "foo", "b.rs", "bar")

    unregistered, changed, notes, rotted = partition([finding], [pair], root=tmp_path)

    assert unregistered == []
    assert changed == []
    assert rotted == []


def test_partition_registered_pair_reverse_key_also_matches(tmp_path):
    # partition() must match a finding against a pair regardless of which
    # side dupehound reported as "original".
    pair = _pin(tmp_path, "a.rs", "foo", FOO_BODY, "b.rs", "bar", BAR_BODY)
    finding = _finding("b.rs", "bar", "a.rs", "foo")

    unregistered, changed, notes, rotted = partition([finding], [pair], root=tmp_path)

    assert unregistered == []
    assert changed == []


def test_partition_reports_changed_when_source_no_longer_matches_pinned_digest(
    tmp_path,
):
    pair = _pin(tmp_path, "a.rs", "foo", FOO_BODY, "b.rs", "bar", BAR_BODY)
    # The source has since been edited -- the register entry is now stale for
    # THIS finding.
    _write(tmp_path, "a.rs", "fn foo(x: i32) -> i32 {\n    x + 2\n}\n")
    finding = _finding("a.rs", "foo", "b.rs", "bar")

    unregistered, changed, notes, rotted = partition([finding], [pair], root=tmp_path)

    assert unregistered == []
    assert changed == [finding]
    assert any("source has changed since" in note for note in notes)


def test_partition_reports_rotted_entry_whose_function_was_deleted(tmp_path):
    pair = _pin(tmp_path, "a.rs", "foo", FOO_BODY, "b.rs", "bar", BAR_BODY)
    # Delete `foo` entirely -- the register entry describes code that is gone.
    _write(tmp_path, "a.rs", "// foo was removed\n")

    unregistered, changed, notes, rotted = partition([], [pair], root=tmp_path)

    assert rotted == [pair]
    assert any("delete this entry" in note for note in notes)


def test_partition_excludes_resolved_findings_from_both_gate_lists(tmp_path):
    # The "original" implementation no longer exists in the tree at all --
    # resolved_reason() must exclude this from both unregistered and changed,
    # reporting it only as an informational note.
    _write(tmp_path, "a.rs", FOO_BODY)
    finding = _finding("a.rs", "foo", "b.rs", "bar")  # b.rs never created

    unregistered, changed, notes, rotted = partition([finding], [], root=tmp_path)

    assert unregistered == []
    assert changed == []
    assert any("resolved:" in note for note in notes)
