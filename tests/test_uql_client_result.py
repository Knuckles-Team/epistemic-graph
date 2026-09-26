"""``client.query.uql`` flattens a ``UqlResult`` (UQL-07; EH-448/EH-450 annotations)."""

from __future__ import annotations

import pytest

from epistemic_graph.client import _uql_result

pytestmark = pytest.mark.no_engine


def test_rows_carry_channels_and_only_the_requested_annotations() -> None:
    proof = {"coverage": "partial", "steps": [{"Unproved": {"stage": "MATCH ()"}}]}
    knowledge = {"kind": "", "confidence": 1.0, "projection": {"year": 2021}}
    wire = {
        "Rows": {
            "columns": ["similarity"],
            "rows": [
                {
                    "id": "d1",
                    "score": 0.5,
                    "channels": [0.5],
                    "knowledge": None,
                    "proof": None,
                },
                {
                    "id": "d2",
                    "score": None,
                    "channels": [None],
                    "knowledge": knowledge,
                    "proof": proof,
                },
            ],
            "warnings": [],
        }
    }
    out = _uql_result(wire)
    assert out["kind"] == "rows"
    plain, annotated = out["rows"]
    assert plain == {"id": "d1", "score": 0.5, "channels": {"similarity": 0.5}}
    assert annotated["knowledge"] == knowledge
    assert annotated["proof"] == proof


def test_explain_has_no_rows() -> None:
    wire = {"Explain": {"canonical": "MATCH ()", "optimized": "MATCH ()", "stages": []}}
    assert _uql_result(wire)["kind"] == "explain"
