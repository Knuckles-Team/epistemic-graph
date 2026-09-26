"""Engine evidence fragmenters preserve stable addresses across source changes."""

from __future__ import annotations

import pytest

from epistemic_graph.ingestion.evidence_fragmentation import (
    fragment_markdown,
    fragment_pdf,
    fragment_record,
    fragment_rowset,
)

pytestmark = pytest.mark.no_engine


def test_markdown_heading_address_survives_unrelated_paragraph_edit() -> None:
    before = fragment_markdown(
        "# Alpha\n\nFirst.\n\n## Beta\n\nSecond.", artifact_id="a"
    )
    after = fragment_markdown(
        "# Alpha\n\nChanged.\n\n## Beta\n\nSecond.", artifact_id="a"
    )
    before_beta = next(
        item for item in before if item.kind == "heading" and item.label == "Beta"
    )
    after_beta = next(
        item for item in after if item.kind == "heading" and item.label == "Beta"
    )
    assert before_beta.fragment_id == after_beta.fragment_id
    assert before_beta.content_hash == after_beta.content_hash


def test_pdf_pages_and_record_fields_have_distinct_addresses() -> None:
    pages = fragment_pdf(["First page", "Second page"], artifact_id="a")
    assert len({item.fragment_id for item in pages}) == len(pages)
    assert {item.kind for item in pages} == {"page", "paragraph"}

    record = fragment_record({"left": 1, "right": 2}, artifact_id="a")
    assert len({item.fragment_id for item in record}) == len(record)
    assert {item.kind for item in record} == {"record", "field"}


def test_rowset_primary_keys_survive_reordering() -> None:
    rows = [{"id": "one", "value": 1}, {"id": "two", "value": 2}]
    first = fragment_rowset(rows, artifact_id="a")
    second = fragment_rowset(list(reversed(rows)), artifact_id="a")
    assert {item.fragment_id for item in first} == {item.fragment_id for item in second}
