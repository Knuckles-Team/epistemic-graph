"""The stable AgentComponent.Content method returns the imported body bytes."""

from __future__ import annotations

import asyncio
import hashlib
import os
import uuid
from typing import Any

from conftest import TEST_TENANT, request_context
from test_connector_pack_client import _catalog
from test_connector_pack_planted import IMPORTER, _bind, _context

from epistemic_graph import (
    ConnectorPackArchiveBuilder,
    ConnectorPackClient,
    ConnectorPackEntryContent,
)
from epistemic_graph.client import EpistemicGraphClient
from epistemic_graph.connector_pack import connector_pack_import_key
from epistemic_graph.generated.connector_pack import (
    PackAnnotations,
    PackEntryKind,
    PackProducer,
)


def test_stable_content_returns_body() -> None:
    """A generated-client content read must return the verified pack body.

    The fixture imports through the same Blob CAS and ConnectorPack.Import
    boundaries used by production, then reads through the generated facade.
    A dispatch-only refusal does not satisfy this acceptance.
    """

    async def exercise() -> None:
        connector = f"stable-content-{uuid.uuid4().hex[:10]}"
        body = b"---\nname: body\ndescription: stable content fixture\n---\n\nbody from the signed pack\n"
        client = await EpistemicGraphClient.connect(
            socket_path=os.environ["GRAPH_SERVICE_SOCKET"],
            verified_context=request_context(),
        )
        try:
            await _bind(client, connector, IMPORTER)
            archive = ConnectorPackArchiveBuilder.build(
                ConnectorPackEntryContent(
                    kind=PackEntryKind.MCP_SERVER,
                    uri=f"mcp-server://{connector}",
                    name=connector,
                    media_type="application/json",
                    body=b'{"name":"stable-content"}',
                    annotations=PackAnnotations(contract_version="1.0.0"),
                ),
                [
                    ConnectorPackEntryContent(
                        kind=PackEntryKind.SKILL,
                        uri=f"skill://{connector}/body",
                        name="body",
                        media_type="text/markdown",
                        body=body,
                        annotations=PackAnnotations(contract_version="1.0.0"),
                    )
                ],
            )
            packs = ConnectorPackClient(client)
            blob = await packs.upload_archive(
                archive,
                operation_key=f"r094.1:{connector}",
            )
            index = archive.index(
                connector=connector,
                blob_digest=blob,
                server_package_version="1.0.0",
                producer=PackProducer(name="r094.1-test", version="1"),
                catalog=_catalog(),
            )
            key = connector_pack_import_key(connector, index.pack_digest, None)
            result: dict[str, Any] = await client._send(
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
                None,
                idempotency_key=key,
            )
            assert result["result"] == "imported", result

            served = await packs.content(
                tenant_id=TEST_TENANT,
                component_id=f"mcp:{connector}/skill/body",
            )
            assert served.body == body
            assert served.media_type == "text/markdown"
            assert served.content_digest == "sha256:" + hashlib.sha256(body).hexdigest()
        finally:
            await client.close()

    asyncio.run(exercise())
