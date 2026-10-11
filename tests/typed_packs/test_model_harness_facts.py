"""Signed pack import/readback coverage for measured model facts."""

from __future__ import annotations

import asyncio
import copy
import os
from typing import Any

import msgpack
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
from epistemic_graph.client import (
    EpistemicGraphClient,
    _canonical_method_body,
    _decode_send_result,
)
from epistemic_graph.connector_pack import connector_pack_import_key
from epistemic_graph.generated.connector_pack import (
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
                uri=f"mcp-server://{connector}",
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
            packs = ConnectorPackClient(client)
            archive = ConnectorPackArchiveBuilder.build(server, [entry])
            blob = await packs.upload_archive(
                archive, operation_key="model-harness-upload"
            )
            index = archive.index(
                connector=connector,
                blob_digest=blob,
                server_package_version="1.0.0",
                producer=PackProducer(name="model-test", version="1"),
                catalog=_catalog(),
            )
            key = connector_pack_import_key(connector, index.pack_digest, None)
            request = client._build_send_request(
                "ConnectorPack",
                {
                    "op": {
                        "op": "import",
                        "request": {
                            "context": _context(key),
                            "index": index.model_dump(mode="json"),
                            "expected_head": None,
                            "allow_mass_withdrawal": False,
                        },
                    }
                },
                req_id=client._next_id(),
                target_graph=client._graph_name,
                idempotency_key=key,
            )
            canonical = _canonical_method_body("ConnectorPack", request["params"])
            wire_model = request["params"]["op"]["request"]["index"]["entries"][0][
                "annotations"
            ]["model"]
            for snake, camel in [
                ("context_window_tokens", "contextWindowTokens"),
                ("shell_proficiency", "shellProficiency"),
                ("stopping_behaviour", "stoppingBehaviour"),
            ]:
                wire_model[camel] = wire_model.pop(snake)
            # Aliases decode to the identical signed body and pack digest.
            assert (
                _canonical_method_body("ConnectorPack", request["params"]) == canonical
            )
            signed_token = request["auth_token"]
            signed_digest = request["params"]["op"]["request"]["index"]["pack_digest"]
            for field in ("shellProficiency", "stoppingBehaviour"):
                malformed = copy.deepcopy(request)
                malformed["params"]["op"]["request"]["index"]["entries"][0][
                    "annotations"
                ]["model"][field]["value"] = "unknown"
                assert malformed["auth_token"] == signed_token
                assert (
                    malformed["params"]["op"]["request"]["index"]["pack_digest"]
                    == signed_digest
                )
                # Closed enum decoding precedes authentication and pack digest
                # checks (transport/framing.rs::decode_request_payload). An
                # invalid enum has no typed canonical body to authenticate.
                # This raw frame must reach that server boundary, not Pydantic.
                refused = await client._roundtrip(
                    msgpack.packb(malformed, use_bin_type=True),
                    req_id=request["id"],
                    method="ConnectorPack",
                )
                assert refused["id"] == request["id"]
                assert refused["error"] == "INVALID_ARGUMENT", refused
                assert refused["error_detail"] == "invalid request encoding", refused
                assert refused.get("result") is None
                status = await packs.status(tenant_id=TEST_TENANT, connector=connector)
                assert status.head is None
                assert status.last_receipt is None
                for suffix in (f"mcp_server/{connector}", "model_profile/model"):
                    assert (
                        await client._send(
                            "AgentComponent",
                            {
                                "op": {
                                    "op": "current",
                                    "tenant_id": TEST_TENANT,
                                    "component_id": f"mcp:{connector}/{suffix}",
                                }
                            },
                        )
                        is None
                    )
            # The exact original signature still works: malformed frames did
            # not consume its nonce or create an import receipt. This also
            # proves the alias input survives the real signed import path.
            accepted = await client._roundtrip(
                msgpack.packb(request, use_bin_type=True),
                req_id=request["id"],
                method="ConnectorPack",
            )
            assert not accepted.get("error"), accepted
            imported = _decode_send_result("ConnectorPack", accepted.get("result"))
            assert imported["result"] == "imported", imported
            assert imported["receipt"]["pack_digest"] == signed_digest
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
