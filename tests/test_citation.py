"""Citation resolution must preserve ambiguity across re-ingestion."""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from epistemic_graph.ingestion.citation import citation_status, resolve_fragment

pytestmark = pytest.mark.no_engine


@dataclass(frozen=True)
class _Fragment:
    fragment_id: str
    address: str
    content_hash: str
    text: str


def test_unique_moved_content_beats_stale_address() -> None:
    current_at_address = _Fragment("a", "p:1", "new", "replacement")
    moved_original = _Fragment("b", "p:2", "old", "original")
    status = citation_status(
        [current_at_address, moved_original], fragment_id="a", content_hash="old"
    )
    assert status["status"] == "moved"
    assert status["fragment_id"] == "b"
    assert status["text"] == "original"


def test_duplicate_content_never_guesses_new_address() -> None:
    fragments = [
        _Fragment("b", "p:2", "old", "duplicate"),
        _Fragment("c", "p:3", "old", "duplicate"),
    ]
    assert resolve_fragment(fragments, content_hash="old") is None
    assert (
        citation_status(fragments, fragment_id="a", content_hash="old")["status"]
        == "lost"
    )
