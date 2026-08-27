"""CA-13 — OBDA (`.obda`) namespace wire-contract tests.

`ObdaClient` is a client-side-only Load/Evaluate ergonomic wrapper over the existing,
stateless `Method.SparqlVirtual` wire call (`RdfClient.sparql_virtual`) -- see the W02
design decision recorded in `src/server/obda/mod.rs`. These tests exercise it against a
real `RdfClient` wired to a fake low-level `_send`, so the exact `SparqlVirtual` params
`ObdaClient.evaluate` produces are asserted the same way `test_rdf_client.py` asserts
`RdfClient`'s own wire shape -- no live engine required.
"""

from __future__ import annotations

from typing import Any

import pytest

from epistemic_graph.client import ObdaClient, RdfClient

# Fake-client unit tests only -- never needs the shared native engine (see
# conftest.py's session-scoped `start_epistemic_graph_server` fixture, which this
# marker exempts this module from triggering).
pytestmark = pytest.mark.no_engine


class _FakeLowLevelClient:
    """Records every `_send` call and returns a canned `SparqlResult`-shaped payload."""

    def __init__(self, result: Any) -> None:
        self.result = result
        self.sent: list[tuple[str, dict[str, Any] | None]] = []

    async def _send(self, method: str, params: dict[str, Any] | None = None) -> Any:
        self.sent.append((method, params))
        return self.result


class _FakeClient:
    """Stands in for `EpistemicGraphClient`: exposes `.rdf`, the one namespace
    `ObdaClient` reaches through."""

    def __init__(self, result: Any) -> None:
        self.low_level = _FakeLowLevelClient(result)
        self.rdf = RdfClient(self.low_level)  # type: ignore[arg-type]


PEOPLE_MAPPING = """
    SOURCE  people
    SUBJECT http://example.org/person/{id}
    CLASS   http://example.org/Person
    COLUMN  http://example.org/name  name
"""

RESULT_ONE_ROW = {"vars": ["name"], "rows": [["Alice"]]}


@pytest.mark.asyncio
async def test_evaluate_sends_sparql_virtual_scoped_to_the_loaded_source() -> None:
    fake = _FakeClient(RESULT_ONE_ROW)
    obda = ObdaClient(fake)  # type: ignore[arg-type]

    await obda.load(PEOPLE_MAPPING, "people")
    rows = await obda.evaluate(
        "PREFIX ex: <http://example.org/> SELECT ?name WHERE { ?p ex:name ?name }"
    )

    assert rows == [{"name": "Alice"}]
    assert fake.low_level.sent == [
        (
            "SparqlVirtual",
            {
                "query": (
                    "PREFIX ex: <http://example.org/> "
                    "SELECT ?name WHERE { ?p ex:name ?name }"
                ),
                "mapping": PEOPLE_MAPPING,
                "tables": ["people"],
                "external_sources": [],
            },
        )
    ]


@pytest.mark.asyncio
async def test_reload_is_idempotent_last_load_wins() -> None:
    fake = _FakeClient(RESULT_ONE_ROW)
    obda = ObdaClient(fake)  # type: ignore[arg-type]

    await obda.load(PEOPLE_MAPPING, "people")
    replacement = PEOPLE_MAPPING.replace("name", "fullname")
    await obda.load(replacement, "people")  # re-load, same source_name
    await obda.evaluate("SELECT * WHERE { ?s ?p ?o }")

    # Only ONE mapping is ever sent for "people" -- the LATEST one, never both.
    sent_mappings = [params["mapping"] for _method, params in fake.low_level.sent]
    assert sent_mappings == [replacement]


@pytest.mark.asyncio
async def test_evaluate_unregistered_source_raises_typed_error_not_empty() -> None:
    """P6 negative case (client-side half): an unregistered mapping never silently
    returns an empty result -- it fails fast, locally, with no engine round trip."""
    fake = _FakeClient(RESULT_ONE_ROW)
    obda = ObdaClient(fake)  # type: ignore[arg-type]

    with pytest.raises(KeyError, match="never loaded"):
        await obda.evaluate("SELECT * WHERE { ?s ?p ?o }", source_name="ghost")

    # No wire call was made -- the rejection is entirely local.
    assert fake.low_level.sent == []


@pytest.mark.asyncio
async def test_evaluate_requires_source_name_when_multiple_are_loaded() -> None:
    fake = _FakeClient(RESULT_ONE_ROW)
    obda = ObdaClient(fake)  # type: ignore[arg-type]

    await obda.load(PEOPLE_MAPPING, "people")
    await obda.load(PEOPLE_MAPPING, "other")

    with pytest.raises(KeyError, match="source_name is required"):
        await obda.evaluate("SELECT * WHERE { ?s ?p ?o }")

    # Disambiguating with an explicit source_name works.
    await obda.evaluate("SELECT * WHERE { ?s ?p ?o }", source_name="people")
    assert fake.low_level.sent[-1][1]["tables"] == ["people"]


@pytest.mark.asyncio
async def test_evaluate_with_exactly_one_loaded_source_omits_source_name() -> None:
    fake = _FakeClient(RESULT_ONE_ROW)
    obda = ObdaClient(fake)  # type: ignore[arg-type]

    await obda.load(PEOPLE_MAPPING, "people")
    await obda.evaluate("SELECT * WHERE { ?s ?p ?o }")  # no source_name needed

    assert fake.low_level.sent[-1][1]["tables"] == ["people"]


@pytest.mark.asyncio
async def test_load_rejects_empty_source_name_or_mapping() -> None:
    fake = _FakeClient(RESULT_ONE_ROW)
    obda = ObdaClient(fake)  # type: ignore[arg-type]

    with pytest.raises(ValueError):
        await obda.load(PEOPLE_MAPPING, "")
    with pytest.raises(ValueError):
        await obda.load("", "people")
