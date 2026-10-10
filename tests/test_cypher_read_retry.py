"""EG-CONTRACT-R049: cypher_read retries once on a dropped connection."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any, cast

import pytest

from epistemic_graph import client as client_mod

pytestmark = pytest.mark.no_engine


def _make(monkeypatch, outcomes):
    calls: list[dict] = []

    async def fake_send(_client, params):
        calls.append(params)
        out = outcomes[len(calls) - 1]
        if isinstance(out, Exception):
            raise out
        return SimpleNamespace(payload=out)

    monkeypatch.setattr(
        client_mod._gen, "query", SimpleNamespace(send_cypher_query=fake_send)
    )
    c = object.__new__(client_mod.QueryClient)
    c._client = cast(Any, object())
    monkeypatch.setattr(c, "_rows_to_dicts", lambda r: [{"r": r}], raising=False)
    return c, calls


@pytest.mark.spec("EG-CONTRACT-R049")
def test_read_retried_once_then_succeeds(monkeypatch):
    c, calls = _make(monkeypatch, [ConnectionError("dropped"), "ok"])
    assert asyncio.run(c.cypher_read("MATCH (n) RETURN n")) == [{"r": "ok"}]
    assert len(calls) == 2


@pytest.mark.spec("EG-CONTRACT-R049")
def test_second_drop_surfaces(monkeypatch):
    c, calls = _make(monkeypatch, [ConnectionError("a"), ConnectionError("b")])
    with pytest.raises(ConnectionError, match="b"):
        asyncio.run(c.cypher_read("MATCH (n) RETURN n"))
    assert len(calls) == 2


@pytest.mark.spec("EG-CONTRACT-R049")
def test_write_not_retried(monkeypatch):
    c, calls = _make(monkeypatch, [ConnectionError("dropped"), "ok"])
    with pytest.raises(ConnectionError):
        asyncio.run(c.cypher_write("CREATE (n)"))
    assert len(calls) == 1
