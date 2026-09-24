"""RDF namespace wire-contract tests.

EH-377: the SPARQL reads decode through the contract model -- `sparql_result`
returns the typed `SparqlResult`, and `sparql`, `sparql_explain` and
`sparql_virtual` shape their rows from it, so a payload the contract does not
declare raises `ContractViolation` instead of reaching a caller (agent-utilities'
`GraphComputeEngine.sparql`) as an untyped dict.
"""

from __future__ import annotations

from typing import Any

import pytest

from epistemic_graph.client import EpistemicGraphClient, RdfClient
from epistemic_graph.generated import ContractViolation, models

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


WITNESS = {
    "subject": "http://example.org/alice",
    "predicate": "http://example.org/name",
    "object": "Alice",
    "object_kind": "literal",
}
RESULT = {
    "vars": ["name", "nick"],
    "rows": [["Alice", None]],
    "proofs": [{"row": 0, "witnesses": [WITNESS], "coverage": "complete"}],
}
QUERY = "SELECT ?name ?nick WHERE { ?p <http://example.org/name> ?name }"


def _rdf(result: Any) -> tuple[RdfClient, _FakeClient]:
    low_level = _FakeClient(result)
    return RdfClient(low_level), low_level


@pytest.mark.asyncio
async def test_sparql_result_is_the_typed_contract_model() -> None:
    rdf, low_level = _rdf(RESULT)
    result = await rdf.sparql_result(QUERY)
    assert isinstance(result, models.SparqlResult)
    assert result.proofs[0].coverage is models.SparqlProofCoverage.COMPLETE
    assert low_level.sent == [
        ("Sparql", {"query": QUERY, "base_iri": "", "type_convention": ""})
    ]


@pytest.mark.asyncio
async def test_sparql_rows_come_from_the_decoded_result() -> None:
    rdf, _ = _rdf(RESULT)
    assert await rdf.sparql(QUERY) == [{"name": "Alice", "nick": None}]


@pytest.mark.asyncio
async def test_sparql_explain_asks_for_proofs_and_maps_them_per_row() -> None:
    rdf, low_level = _rdf(RESULT)
    explained = await rdf.sparql_explain(QUERY)
    assert explained == [
        {
            "row": {"name": "Alice", "nick": None},
            "witnesses": [WITNESS],
            "coverage": "complete",
        }
    ]
    assert low_level.sent[0][1] == {
        "query": QUERY,
        "base_iri": "",
        "type_convention": "",
        "explain": True,
    }


@pytest.mark.asyncio
async def test_sparql_virtual_rows_come_from_the_decoded_result() -> None:
    rdf, _ = _rdf(RESULT)
    rows = await rdf.sparql_virtual(QUERY, "SOURCE people", ["people"])
    assert rows == [{"name": "Alice", "nick": None}]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload",
    [
        None,
        {"vars": ["name"], "rows": [["Alice"]]},
        {**RESULT, "rows": [[1]]},
        {**RESULT, "extra": True},
    ],
)
async def test_an_undeclared_payload_is_a_contract_violation(payload: Any) -> None:
    rdf, _ = _rdf(payload)
    with pytest.raises(ContractViolation):
        await rdf.sparql(QUERY)
