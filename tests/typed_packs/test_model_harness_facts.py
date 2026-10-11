"""Signed pack import/readback coverage for measured model facts."""

from __future__ import annotations

import asyncio
import os
from typing import Any

import pytest
from conftest import TEST_TENANT, request_context
from pydantic import ValidationError
from test_connector_pack_client import _catalog
from test_connector_pack_planted import IMPORTER, _bind, _context

from epistemic_graph import (
    ConnectorPackArchiveBuilder,
    ConnectorPackClient,
    ConnectorPackEntryContent,
)
from epistemic_graph.client import EpistemicGraphClient
from epistemic_graph.generated.connector_pack import (
    AgentLibraryMutationContext,
    PackAnnotations,
    PackEntryKind,
    PackModelFacts,
    PackProducer,
)


@pytest.mark.spec("EG-TYPED-PACKS-R048.5")
def test_model_profile_round_trip() -> None:
    model: dict[str, Any] = {
        "provider": "test-provider",
        "model_identity": "test-model",
        "context_window_tokens": 128_000,
        "max_output_tokens": 8192,
        "supports_tools": True,
        "supports_structured_output": True,
        "supports_vision": False,
        "shell_proficiency": {
            "value": "high",
            "evaluation_run_ref": {
                "evaluation_id": "eval:shell",
                "digest": "shell-evidence",
            },
        },
        "stopping_behaviour": {
            "value": "balanced",
            "evaluation_run_ref": {
                "evaluation_id": "eval:stop",
                "digest": "stop-evidence",
            },
        },
    }
    facts = PackModelFacts.model_validate(model)
    for field in ("shell_proficiency", "stopping_behaviour"):
        with pytest.raises(ValidationError) as refusal:
            PackModelFacts.model_validate(
                {**model, field: {**model[field], "value": "unknown"}}
            )
        assert any(
            error["type"] == "enum" and field in error["loc"]
            for error in refusal.value.errors()
        )

    async def exercise() -> None:
        connector = "model-harness-facts"
        client = await EpistemicGraphClient.connect(
            socket_path=os.environ["GRAPH_SERVICE_SOCKET"],
            verified_context=request_context(),
        )
        try:
            await _bind(client, connector, IMPORTER)
            server = ConnectorPackEntryContent(
                kind=PackEntryKind.MCP_SERVER,
                uri=f"mcp://{connector}/server",
                name=connector,
                media_type="application/json",
                body=b'{"name":"model-harness-facts"}',
                annotations=PackAnnotations(contract_version="1.0.0"),
            )
            entry = ConnectorPackEntryContent(
                kind=PackEntryKind.MODEL_PROFILE,
                uri=f"model-profile://{connector}/model",
                name="model",
                media_type="application/json",
                body=b'{"name":"model"}',
                annotations=PackAnnotations(model=facts),
            )
            result = await ConnectorPackClient(client).import_pack(
                ConnectorPackArchiveBuilder.build(server, [entry]),
                connector=connector,
                server_package_version="1.0.0",
                producer=PackProducer(name="model-test", version="1"),
                catalog=_catalog(),
                context=AgentLibraryMutationContext.model_validate(
                    _context("model-import")
                ),
            )
            assert result.result == "imported", result
            stored = await client._send(
                "AgentComponent",
                {
                    "op": {
                        "op": "current",
                        "tenant_id": TEST_TENANT,
                        "component_id": f"mcp:{connector}/model_profile/model",
                    }
                },
                None,
            )
            assert stored is not None
            assert (
                stored["facts"]["context_window_tokens"]
                == model["context_window_tokens"]
            )
            for field in ("shell_proficiency", "stopping_behaviour"):
                assert stored["facts"][field] == model[field]
        finally:
            await client.close()

    asyncio.run(exercise())
