"""Source identity derivation stays consistent across graph ingestion paths."""

from epistemic_graph.source_provenance import stamp_source


def test_source_stamp_canonicalizes_and_preserves_caller_values():
    row = {"domain": "already-set"}
    assert stamp_source(row, " Egeria ") is row
    assert row == {"domain": "already-set", "source_system": "egeria"}


def test_internal_write_has_no_source_stamp():
    row: dict[str, str] = {}
    assert stamp_source(row, None) is row
    assert row == {}
