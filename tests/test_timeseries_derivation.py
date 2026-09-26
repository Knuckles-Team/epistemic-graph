"""Deterministic time-series keys and query literals."""

from datetime import UTC, datetime

import pytest

from epistemic_graph.timeseries_derivation import (
    cypher_string,
    from_nanoseconds,
    series_id,
    to_nanoseconds,
)

pytestmark = pytest.mark.no_engine


def test_tag_order_does_not_change_series_identity():
    assert series_id("cpu", {"host": "a", "unit": "pct"}) == series_id(
        "cpu", {"unit": "pct", "host": "a"}
    )
    assert series_id("cpu", None) == "ts:cpu"
    assert series_id("cpu", {"host": "b"}) != series_id("cpu", {"host": "a"})


def test_timestamp_key_round_trip_at_microsecond_resolution():
    point = datetime(2026, 9, 26, 12, 0, 1, 123456, tzinfo=UTC)
    timestamp_ns = to_nanoseconds(point)
    assert timestamp_ns % 1_000_000_000 == 123_456_000
    assert from_nanoseconds(timestamp_ns) == point
    assert from_nanoseconds(to_nanoseconds(point.replace(tzinfo=None))) == point
    before_epoch = datetime(1969, 12, 31, 23, 59, 59, 999999, tzinfo=UTC)
    assert to_nanoseconds(before_epoch) == -1_000
    assert from_nanoseconds(-1_000) == before_epoch


def test_cypher_symbol_rejects_controls_and_escapes_quotes():
    assert cypher_string("x' RETURN s //") == "'x\\' RETURN s //'"
    with pytest.raises(ValueError):
        cypher_string("x\nMATCH (n)")
    with pytest.raises(ValueError):
        cypher_string("")
