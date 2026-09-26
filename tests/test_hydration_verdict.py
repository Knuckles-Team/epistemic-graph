"""Hydration verdict distinguishes absent sources from tenant-hidden rows."""

from __future__ import annotations

import pytest

from epistemic_graph.ingestion.hydration_verdict import fuse_hydration_verdict

pytestmark = pytest.mark.no_engine


@pytest.mark.parametrize(
    ("expected", "serving", "service", "attempted", "verdict"),
    [
        (62, 0, 62, True, "blocked"),
        (62, 0, 0, True, "not_started"),
        (62, 0, None, False, "blocked"),
        (62, None, None, False, "blocked"),
        (62, 30, 30, True, "partial"),
        (62, 62, 62, True, "complete"),
        (None, 0, None, False, "not_started"),
        (None, 5, None, False, "complete"),
    ],
)
def test_verdict_follows_both_read_authorities(
    expected: int | None,
    serving: int | None,
    service: int | None,
    attempted: bool,
    verdict: str,
) -> None:
    actual, reason = fuse_hydration_verdict(expected, serving, service, attempted)
    assert actual == verdict
    assert reason


def test_hidden_rows_are_reported_as_visibility_gap() -> None:
    verdict, reason = fuse_hydration_verdict(62, 0, 62, True)
    assert verdict == "blocked"
    assert "RLS visibility gap" in reason
