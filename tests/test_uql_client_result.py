"""``client.query.uql`` flattens a ``UqlResult`` (UQL-07; EH-448/EH-450 annotations)."""

from __future__ import annotations

import pytest

from epistemic_graph.client import (
    EngineResponseError,
    EpistemicGraphClient,
    QueryClient,
    _uql_result,
)


class _FakeClient(EpistemicGraphClient):
    def __init__(self) -> None:
        self.sent: list[tuple[str, dict[str, object] | None]] = []

    async def _send(
        self,
        method: str,
        params: dict[str, object] | None = None,
        graph: str | None = None,
        *,
        idempotency_key: str | None = None,
    ) -> object:
        self.sent.append((method, params))
        return {"Rows": {"columns": [], "rows": [], "warnings": []}}


@pytest.mark.no_engine
@pytest.mark.asyncio
async def test_uql_keeps_hostile_values_out_of_query_text() -> None:
    client = _FakeClient()
    text = "MATCH (:Doc) WHERE title = $title"
    hostile = "x' |> DELETE ALL //"

    await QueryClient(client).uql(text, {"title": hostile})

    assert client.sent == [
        (
            "Uql",
            {"text": text, "params": {"title": {"Str": hostile}}},
        )
    ]


@pytest.mark.spec("EG-FEDERATED-QUERY-R025")
def test_uql_binds_hostile_value_from_python_client_through_engine(clean_graph) -> None:
    hostile = "needle' |> MATCH (:Secret) |> LIMIT 1000 //"
    clean_graph.nodes.add("uql-hostile-value", {"type": "Doc", "title": hostile})
    clean_graph.nodes.add("uql-decoy-value", {"type": "Doc", "title": "needle"})

    result = clean_graph.query.uql(
        "MATCH (:Doc) WHERE title = $title", {"title": hostile}
    )

    assert result["kind"] == "rows"
    assert [row["id"] for row in result["rows"]] == ["uql-hostile-value"]


@pytest.mark.parametrize(
    ("text", "params", "expected_code"),
    [
        ("MATCH (:Doc) |> LIMIT $k", {}, "UQL_UNBOUND_PARAMETER"),
        (
            "MATCH (:Doc) |> LIMIT $k",
            {"k": "five"},
            "UQL_PARAMETER_TYPE",
        ),
        (
            "MATCH (:Doc) |> LIMIT $k",
            {"k": 1, "typo": 2},
            "UQL_UNUSED_PARAMETER",
        ),
    ],
)
@pytest.mark.spec("EG-FEDERATED-QUERY-R025")
def test_uql_rejects_invalid_parameters_through_python_client(
    clean_graph, text, params, expected_code
) -> None:
    with pytest.raises(EngineResponseError) as error:
        clean_graph.query.uql(text, params)

    assert error.value.code == expected_code


@pytest.mark.no_engine
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


@pytest.mark.no_engine
def test_explain_has_no_rows() -> None:
    wire = {
        "Explain": {
            "canonical": "MATCH ()",
            "optimized": "MATCH ()",
            "stages": [],
            "plan_digest": "a" * 64,
            "query_digest": "b" * 64,
        }
    }
    result = _uql_result(wire)
    assert result["kind"] == "explain"
    assert result["plan_digest"] == "a" * 64
    assert result["query_digest"] == "b" * 64
