"""Source positions preserve cursor replay and content-version semantics."""

from __future__ import annotations

import pytest

from epistemic_graph.ingestion.source_positions import (
    checkpoint_from_position,
    content_position,
    cursor_partition,
    position_advances,
    typed_position,
)

pytestmark = pytest.mark.no_engine


def test_typed_cursor_round_trips_sequence_timestamp_and_opaque() -> None:
    assert typed_position("2", content=False) == {"kind": "sequence", "value": 2}
    assert checkpoint_from_position(typed_position("2", content=False)) == "2"
    timestamp = typed_position("2026-09-26T00:00:00Z", content=False)
    assert timestamp["kind"] == "timestamp_millis"
    assert checkpoint_from_position(timestamp) == "2026-09-26T00:00:00.000Z"
    opaque = typed_position("page-7", content=False)
    assert opaque == {
        "kind": "opaque",
        "value": {"cursor_type": "connector_opaque_v1", "value": "page-7"},
    }
    assert checkpoint_from_position(opaque) == "page-7"


def test_position_advances_only_within_compatible_types() -> None:
    assert position_advances(
        typed_position("3", content=False), typed_position("2", content=False)
    )
    assert not position_advances(
        typed_position("2", content=False), typed_position("2", content=False)
    )
    assert not position_advances(
        typed_position("page-7", content=False), typed_position("2", content=False)
    )
    assert position_advances(
        typed_position("page-8", content=False),
        typed_position("page-7", content=False),
    )


def test_content_position_advances_prior_or_uses_explicit_version() -> None:
    assert content_position(
        None, {"source_version": {"kind": "sequence", "value": 4}}, "digest"
    ) == {
        "kind": "sequence",
        "value": 5,
    }
    assert content_position(
        "7", {"source_version": {"kind": "sequence", "value": 4}}, "digest"
    ) == {
        "kind": "sequence",
        "value": 7,
    }
    assert content_position(None, None, "digest") == {
        "kind": "opaque",
        "value": {"version_type": "connector_opaque_v1", "value": "digest"},
    }


def test_cursor_partition_is_stable_and_source_scoped() -> None:
    assert cursor_partition("") == ""
    assert cursor_partition("instance-a") == cursor_partition("instance-a")
    assert cursor_partition("instance-a") != cursor_partition("instance-b")
