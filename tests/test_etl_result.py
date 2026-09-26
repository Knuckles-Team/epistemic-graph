"""Unit tests for the engine-owned ETL step contract."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from epistemic_graph.etl_result import EtlResult

pytestmark = pytest.mark.no_engine


def test_default_status_and_counts():
    result = EtlResult()
    assert result.status == "ok"
    assert result.counts == {}
    assert result.source is None


def test_connector_fields_are_namespaced_under_details():
    result = EtlResult(
        status="ok",
        counts={"nodes": 7},
        details={"instances": [{"name": "a"}]},
    )
    dumped = result.model_dump()
    assert dumped["counts"] == {"nodes": 7}
    assert dumped["details"] == {"instances": [{"name": "a"}]}


def test_unknown_top_level_fields_are_rejected():
    with pytest.raises(ValidationError):
        EtlResult.model_validate({"status": "ok", "nodes_hydrated": 4})


def test_nested_steps_use_the_same_contract():
    inbound = EtlResult(status="materialized", source="camunda", counts={"nodes": 4})
    result = EtlResult(status="ok", inbound=inbound)
    assert result.inbound == inbound
    assert result.model_dump()["inbound"]["counts"] == {"nodes": 4}


def test_counts_are_explicit():
    result = EtlResult(counts={"nodes": 1, "edges": 2})
    assert result.counts == {"nodes": 1, "edges": 2}
