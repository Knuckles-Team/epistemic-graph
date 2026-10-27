"""``client.query.uql`` flattens a ``UqlResult`` (UQL-07; EH-448/EH-450 annotations)."""

from __future__ import annotations

import pytest

from epistemic_graph.client import EpistemicGraphClient, QueryClient, _uql_result

pytestmark = pytest.mark.no_engine


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


def test_explain_has_no_rows() -> None:
    wire = {"Explain": {"canonical": "MATCH ()", "optimized": "MATCH ()", "stages": []}}
    assert _uql_result(wire)["kind"] == "explain"
