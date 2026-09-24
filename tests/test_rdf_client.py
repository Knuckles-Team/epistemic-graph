"""RDF namespace wire-contract tests."""

from __future__ import annotations

from typing import Any

import pytest

from epistemic_graph.client import EpistemicGraphClient, RdfClient

# Fake-client unit tests only -- never needs the shared native engine (see
# conftest.py's session-scoped `start_epistemic_graph_server` fixture,
# which this marker exempts this module from triggering).
pytestmark = pytest.mark.no_engine


class _FakeClient(EpistemicGraphClient):
    def __init__(self, result: Any) -> None:
        self.result = result
        self.sent: list[tuple[str, dict[str, Any] | None]] = []

    async def _send(
        self,
        method: str,
        params: dict[str, Any] | None = None,
        graph: str | None = None,
        *,
        idempotency_key: str | None = None,
    ) -> Any:
        self.sent.append((method, params))
        return self.result


@pytest.mark.asyncio
async def test_validate_shacl_sends_both_inline_graphs() -> None:
    report = {"conforms": True, "results": [], "schema_digests": []}
    fake = _FakeClient(report)
    rdf = RdfClient(fake)

    result = await rdf.validate_shacl("shapes", "data")

    # The decoded report carries every model field, the optional digest included.
    assert result == {**report, "composed_digest": None}
    assert fake.sent == [("ShaclValidate", {"shapes": "shapes", "data_graph": "data"})]


@pytest.mark.asyncio
async def test_validate_committed_sends_typed_triples_without_shapes() -> None:
    digest = "33" * 32
    report = {
        "conforms": True,
        "results": [],
        "schema_digests": [digest],
        "composed_digest": digest,
    }
    fake = _FakeClient(report)
    rdf = RdfClient(fake)
    triples = [
        {
            "subject": "http://knuckles.team/kg#a",
            "predicate": "http://knuckles.team/kg#name",
            "object": {"kind": "literal", "lexical": "a"},
        }
    ]

    result = await rdf.validate_committed(data_triples=triples)

    assert result == report
    [(method, params)] = fake.sent
    assert method == "ShaclValidate"
    assert "shapes" not in (params or {})
    assert (params or {})["data_triples"][0]["subject"] == "http://knuckles.team/kg#a"
