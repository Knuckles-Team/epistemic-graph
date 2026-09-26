"""Pure lifecycle closure parity for bounded assimilation reads."""

import pytest

from epistemic_graph.assimilation_lifecycle_derivation import (
    assimilation_edge_properties,
    closed_feature_ids,
    feature_ledger_row,
    feature_properties,
    relation_closes,
    status_is_closed,
)

pytestmark = pytest.mark.no_engine


def test_status_and_direction_are_explicit() -> None:
    assert status_is_closed("IMPLEMENTED")
    assert not status_is_closed("open")
    assert relation_closes("SATISFIED_BY", incoming=False)
    assert not relation_closes("SATISFIED_BY", incoming=True)
    assert relation_closes("SUPERSEDES", incoming=True)
    assert not relation_closes("SUPERSEDES", incoming=False)


def test_closed_ids_obey_feature_direction_and_ignore_external_nodes() -> None:
    features = {
        "status": {"status": "done"},
        "out": {"status": "open"},
        "in": {"status": "open"},
        "still-open": {"status": "open"},
    }
    edges = [
        ("out", "external", "DERIVED_FROM_RESEARCH"),
        ("external", "in", "SUPERSEDES"),
        ("external", "still-open", "SATISFIED_BY"),
    ]
    assert closed_feature_ids(features, edges) == {"status", "out", "in"}


def test_feature_row_and_properties_preserve_unknown_and_empty_rules() -> None:
    assert feature_ledger_row({"id": "  "}) is None
    row = feature_ledger_row(
        {"id": " f1 ", "concept": "UNKNOWN", "source": "paper", "status": ""}
    )
    assert row == {
        "feature_id": "f1",
        "name": "f1",
        "concept_ids": [],
        "research_sources": ["paper"],
        "status": "open",
        "sdd_path": "",
    }
    assert feature_properties(name="f1", research_sources=["paper"])["codebase"] == ""
    assert assimilation_edge_properties("implemented", "2026-09-26") == {
        "_rel": "ASSIMILATED_INTO",
        "status": "implemented",
        "concept": "AU-KG.query.vendor-agnostic-traversal",
        "assimilation_date": "2026-09-26",
    }
