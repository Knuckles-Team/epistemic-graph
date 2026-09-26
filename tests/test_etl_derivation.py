"""ETL wire-result derivation is engine-owned and independent of AU."""

from __future__ import annotations

import pytest

from epistemic_graph.etl_derivation import (
    aggregate_counts,
    lineage_direction,
    step_result,
)
from epistemic_graph.etl_result import EtlResult

pytestmark = pytest.mark.no_engine


def test_step_result_preserves_strict_fields_and_isolates_connector_details():
    result = step_result(
        {"status": "ok", "counts": {"nodes": 3}, "vendor_cursor": "next"},
        source="servicenow",
        mode="delta",
    )
    assert result.source == "servicenow"
    assert result.mode == "delta"
    assert result.counts == {"nodes": 3}
    assert result.details == {"vendor_cursor": "next"}


def test_step_result_preserves_existing_typed_result():
    result = EtlResult(status="skipped", source="leanix")
    assert step_result(result, source="other") is result


def test_step_result_merges_existing_details_with_connector_fields():
    result = step_result({"details": {"first": 1}, "second": 2})
    assert result.details == {"first": 1, "second": 2}


def test_aggregate_counts_adds_completed_steps_in_order():
    inbound = EtlResult(counts={"nodes": 2, "edges": 1})
    outbound = EtlResult(counts={"nodes": 3})
    assert aggregate_counts(None, inbound, outbound) == {"nodes": 5, "edges": 1}


@pytest.mark.parametrize(
    ("source", "sink", "expected"),
    [
        ("source", "sink", "through"),
        ("source", None, "inbound"),
        (None, "sink", "outbound"),
    ],
)
def test_lineage_direction(source, sink, expected):
    assert lineage_direction(source, sink) == expected
