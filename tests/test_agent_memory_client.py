"""Typed memory client preserves the EG wire contract for AU callers."""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import patch

import msgpack

from epistemic_graph import generated
from epistemic_graph.client import GraphOperationsClient


class _Wire:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []


def test_agent_memory_client_uses_contract_params_and_native_results() -> None:
    wire = _Wire()
    memory = GraphOperationsClient(wire)  # type: ignore[arg-type]

    async def call() -> tuple[str, str, tuple[int, list[str]]]:
        summary = await memory.create_summary_node(
            child_ids=["ep:1", "ep:2"],
            summary_text="durable fact",
            metadata={"source": "agent", "content": "untrusted override"},
        )
        semantic = await memory.consolidate_memories(
            episodic_ids=["ep:1", "ep:2"],
            summary_id=summary,
            summary_text="durable fact",
        )
        maintained = await memory.maintain_memories(
            ids=["ep:1", "ep:2"],
            now_ms=1000,
            half_life_ms=604800000,
            evict_threshold=0.05,
        )
        return summary, semantic, maintained

    async def send_summary(_client: Any, params: dict[str, Any]) -> str:
        wire.calls.append(("CreateSummaryNode", params))
        return "summary:1"

    async def send_consolidate(_client: Any, params: dict[str, Any]) -> str:
        wire.calls.append(("Consolidate", params))
        return "semantic:1"

    async def send_maintain(_client: Any, params: dict[str, Any]) -> object:
        wire.calls.append(("Maintain", params))
        return object()

    with (
        patch.object(generated.graph, "send_create_summary_node", send_summary),
        patch.object(generated.graph, "send_consolidate", send_consolidate),
        patch.object(generated.graph, "send_maintain", send_maintain),
        patch.object(generated.graph, "decode_maintain", return_value=(2, ["old:1"])),
    ):
        assert asyncio.run(call()) == ("summary:1", "semantic:1", (2, ["old:1"]))
    assert [method for method, _ in wire.calls] == [
        "CreateSummaryNode",
        "Consolidate",
        "Maintain",
    ]
    assert wire.calls[0][1]["child_ids"] == ["ep:1", "ep:2"]
    assert msgpack.unpackb(wire.calls[0][1]["props_msgpack"], raw=False) == {
        "content": "durable fact",
        "memory_type": "semantic",
        "source": "agent",
    }
    assert msgpack.unpackb(wire.calls[1][1]["semantic_props_msgpack"], raw=False) == {
        "content": "durable fact",
        "memory_type": "semantic",
        "summary_id": "summary:1",
    }
    assert wire.calls[2][1] == {
        "ids": ["ep:1", "ep:2"],
        "now_ms": 1000,
        "half_life_ms": 604800000,
        "evict_threshold": 0.05,
        "delete": False,
    }
