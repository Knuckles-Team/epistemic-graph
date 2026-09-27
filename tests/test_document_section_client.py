"""EH-509 Python binding never downgrades a stored read to legacy rows."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

import pytest

from epistemic_graph import generated as contract
from epistemic_graph.client import QueryClient


def test_missing_contract_method_fails_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delattr(
        contract.query, "send_retrieve_document_sections", raising=False
    )
    client = QueryClient(object())  # type: ignore[arg-type]
    with pytest.raises(RuntimeError, match="lacks RetrieveDocumentSections"):
        asyncio.run(client.retrieve_document_sections("doc:1", "heading"))


def test_generated_sender_receives_only_bounded_query_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: list[dict[str, Any]] = []

    async def sender(client: object, params: dict[str, Any]) -> Any:
        seen.append(params)
        return SimpleNamespace(
            payload={"document_id": "doc:1", "citations": [{"node_id": "sec:1"}]}
        )

    monkeypatch.setattr(
        contract.query, "send_retrieve_document_sections", sender, raising=False
    )
    client = QueryClient(object())  # type: ignore[arg-type]
    result = asyncio.run(client.retrieve_document_sections("doc:1", "heading", top_k=3))
    assert result["citations"] == [{"node_id": "sec:1"}]
    assert seen == [
        {"document_id": "doc:1", "query": "heading", "top_k": 3, "beam_width": 16}
    ]


def test_invalid_result_is_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    async def sender(_client: object, _params: dict[str, Any]) -> Any:
        return SimpleNamespace(payload={"document_id": "other", "citations": []})

    monkeypatch.setattr(
        contract.query, "send_retrieve_document_sections", sender, raising=False
    )
    client = QueryClient(object())  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="invalid RetrieveDocumentSections response"):
        asyncio.run(client.retrieve_document_sections("doc:1", "heading"))
