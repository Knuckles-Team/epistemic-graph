from __future__ import annotations

import asyncio
from typing import Any

from epistemic_graph.generated.graph_schema import (
    GraphSchemaCommitted,
    GraphSchemaOpAttach,
    GraphSchemaOpDetach,
    GraphSchemaSourcesView,
    SchemaSourceOriginViewCore,
)

from epistemic_graph.generated import reasoning


class _Client:
    def __init__(self, payloads: dict[str, dict[str, Any]]) -> None:
        self.payloads = payloads
        self.calls: list[tuple[str, dict[str, Any] | None, str | None, str | None]] = []

    async def _send(
        self,
        method: str,
        params: dict[str, Any] | None,
        graph: str | None,
        *,
        idempotency_key: str | None,
    ) -> dict[str, Any]:
        self.calls.append((method, params, graph, idempotency_key))
        return self.payloads[method]


def test_generated_graph_schema_write_and_list_are_typed() -> None:
    digest = "07" * 32
    client = _Client(
        {
            "GraphSchema": {
                "schema_version": 1,
                "graph": "tenant",
                "composed_digest": digest,
                "graph_version": 8,
                "changed": True,
            },
            "GraphSchemaList": {
                "schema_version": 1,
                "graph": "tenant",
                "core_catalog_digest": digest,
                "composed_digest": digest,
                "core_sources": [
                    {
                        "source_id": "core:capability@1",
                        "origin": {
                            "origin": "core",
                            "module": "capability",
                            "version": 1,
                            "set_digest": digest,
                        },
                        "shapes_sha256": None,
                        "ontology_sha256": digest,
                        "shapes_bytes": 0,
                        "ontology_bytes": 335,
                        "attached_at_ms": 0,
                    }
                ],
                "dynamic_sources": [],
            },
        }
    )
    attach = GraphSchemaOpAttach(
        op="attach",
        source_id="admin:local",
        shapes_ttl=None,
        ontology_ttl="@prefix owl: <http://www.w3.org/2002/07/owl#> .",
        if_composed_digest=None,
    )
    committed = asyncio.run(
        reasoning.send_graph_schema(
            client,
            {"op": attach.model_dump(mode="json")},
            "tenant",
            idempotency_key="schema-1",
        )
    )
    listed = asyncio.run(reasoning.send_graph_schema_list(client, {}, "tenant"))

    assert isinstance(committed, GraphSchemaCommitted)
    assert isinstance(listed, GraphSchemaSourcesView)
    assert isinstance(listed.core_sources[0].origin, SchemaSourceOriginViewCore)
    assert client.calls == [
        (
            "GraphSchema",
            {"op": attach.model_dump(mode="json")},
            "tenant",
            "schema-1",
        ),
        ("GraphSchemaList", {}, "tenant", None),
    ]


def test_generated_graph_schema_detach_preserves_the_discriminator() -> None:
    detach = GraphSchemaOpDetach(
        op="detach",
        source_id="admin:local",
        if_composed_digest="00" * 32,
    )
    assert detach.model_dump(mode="json") == {
        "op": "detach",
        "source_id": "admin:local",
        "if_composed_digest": "00" * 32,
    }
