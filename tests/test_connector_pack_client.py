from __future__ import annotations

import asyncio
import hashlib
import json
from pathlib import Path
from typing import Any

import pytest

from epistemic_graph import (
    ConnectorPackArchiveBuilder,
    ConnectorPackClient,
    ConnectorPackEntryContent,
    ConnectorPackWriteError,
    pack_digest,
)
from epistemic_graph import (
    PackImportResultRejected as ExportedRejected,
)
from epistemic_graph import (
    PackImportResultUnchanged as ExportedUnchanged,
)
from epistemic_graph.connector_pack import connector_pack_import_key
from epistemic_graph.generated.connector_pack import (
    AgentLibraryMutationContext,
    DeclaredCost,
    DeclaredLatency,
    McpCatalogSnapshotBinding,
    PackAnnotations,
    PackEntry,
    PackEntryKind,
    PackImportResultRejected,
    PackImportResultUnchanged,
    PackModelFacts,
    PackProducer,
    PackRef,
    PackSection,
    PackWriteErrorCode,
)

pytestmark = pytest.mark.no_engine

ROOT = Path(__file__).resolve().parents[1]


def _digest(byte: int) -> str:
    return f"{byte:02x}" * 32


def _section(offset: int, byte: int) -> PackSection:
    return PackSection(offset=offset, length=128, sha256=_digest(byte))


def _annotations() -> PackAnnotations:
    return PackAnnotations(
        provides=["eg:capability/retrieval"],
        requires_capabilities=["eg:capability/action"],
        modalities_in=["eg:modality/text"],
        modalities_out=["eg:modality/text"],
        required_scopes=["kg:read"],
        read_only_hint=True,
        destructive_hint=False,
        idempotent_hint=None,
        open_world_hint=True,
        contract_version="1.4.0",
        cost=DeclaredCost(
            currency="USD",
            per_call_micros=2_500,
            input_per_mtok_micros=300_000,
        ),
        latency_declared=DeclaredLatency(p50_ms=120, p95_ms=900),
        model=PackModelFacts(
            provider="vendor",
            model_identity="vendor/model-1",
            context_window_tokens=128_000,
            max_output_tokens=8_192,
            supports_tools=True,
            supports_structured_output=True,
            supports_vision=False,
        ),
        sdk_contract_pin="sdk-d18-1",
    )


def _entry(kind: PackEntryKind, name: str) -> PackEntry:
    return PackEntry(
        kind=kind,
        uri=f"mcp://connector-a/{name}",
        name=name,
        media_type="application/json",
        body=_section(0, 0xC1),
        input_schema=_section(128, 0xC2),
        output_schema=_section(256, 0xC3),
        annotations=_annotations(),
        references=[
            PackRef(uri="mcp://connector-a/server", kind=PackEntryKind.MCP_SERVER)
        ],
    )


def _catalog() -> McpCatalogSnapshotBinding:
    return McpCatalogSnapshotBinding(
        configuration_revision=7,
        catalog_generation=11,
        snapshot_digest=_digest(0xC8),
        child_connection_generation=3,
        authorization_scope_digest=_digest(0xC9),
    )


def _vector_entries() -> list[PackEntry]:
    kinds = [
        PackEntryKind.TOOL,
        PackEntryKind.SKILL,
        PackEntryKind.PROMPT,
        PackEntryKind.RESOURCE,
        PackEntryKind.RESOURCE_TEMPLATE,
        PackEntryKind.ONTOLOGY,
        PackEntryKind.SHAPES,
        PackEntryKind.MODEL_PROFILE,
        PackEntryKind.A2A_CARD,
        PackEntryKind.MANIFEST,
    ]
    return [_entry(kind, f"entry-{position}") for position, kind in enumerate(kinds)]


def _expected_vector(name: str) -> str:
    document = json.loads(
        (ROOT / "contract/fixtures/connector_pack_digest_vectors.json").read_text()
    )
    return next(row["sha256"] for row in document["vectors"] if row["name"] == name)


def test_pack_digest_matches_the_rust_generated_golden_vector() -> None:
    assert pack_digest(
        "connector-a",
        _catalog(),
        _entry(PackEntryKind.MCP_SERVER, "server"),
        list(reversed(_vector_entries())),
    ) == _expected_vector("pack_digest/one_entry_per_kind")


def _built_archive():
    annotations = PackAnnotations(provides=["eg:capability/retrieval"])
    server = ConnectorPackEntryContent(
        kind=PackEntryKind.MCP_SERVER,
        uri="mcp://connector-a/server",
        name="connector-a",
        media_type="application/json",
        body=b'{"name":"connector-a"}',
    )
    tool = ConnectorPackEntryContent(
        kind=PackEntryKind.TOOL,
        uri="tool://connector-a/search",
        name="search",
        media_type="application/json",
        body=b'{"name":"search"}',
        input_schema=b'{"type":"object"}',
        annotations=annotations,
        references=(
            PackRef(uri="mcp://connector-a/server", kind=PackEntryKind.MCP_SERVER),
        ),
    )
    return ConnectorPackArchiveBuilder.build(server, [tool])


def test_archive_builder_emits_gap_free_digest_pinned_sections() -> None:
    built = _built_archive()
    sections = [
        built.server.body,
        built.entries[0].body,
        built.entries[0].input_schema,
    ]
    cursor = 0
    for section in sections:
        assert section is not None
        assert section.offset == cursor
        content = built.data[section.offset : section.offset + section.length]
        assert hashlib.sha256(content).hexdigest() == section.sha256
        cursor += section.length
    assert cursor == len(built.data)


def _context() -> AgentLibraryMutationContext:
    return AgentLibraryMutationContext(
        request_id=1,
        principal="principal-a",
        caller_principal="caller-a",
        attempt_nonce="07" * 32,
        tenant_id="tenant-a",
        actor_scope="scope-a",
        purpose_id="purpose-a",
        policy_revision="policy-1",
        policy_digest="sha256:" + _digest(0x22),
        policy_decision_id="decision-1",
        idempotency_key="caller-must-not-win",
        expected_revision=1,
        trace_id="trace-1",
        created_at_ms=1_700_000_000_000,
    )


class _Client:
    def __init__(self) -> None:
        self.sent: list[tuple[str, Any, Any, Any]] = []

    async def _send(
        self,
        method: str,
        params: dict[str, Any] | None,
        graph: str | None,
        *,
        idempotency_key: str | None,
    ) -> Any:
        self.sent.append((method, params, graph, idempotency_key))
        if method == "BlobBegin":
            return 91
        if method == "BlobChunkPut":
            return 1
        if method == "BlobCommit":
            return "blob-manifest-a"
        if method == "AgentComponent":
            return {
                "schema_version": 1,
                "component_id": "mcp:connector-a/tool/search",
                "entry_revision": 3,
                "definition_digest": "sha256:" + _digest(0x31),
                "content_digest": "sha256:" + _digest(0x32),
                "media_type": "application/json",
                "body": b'{"name":"search"}',
            }
        if method == "ConnectorPack" and params is not None:
            operation = params["op"]["op"]
            if operation == "status":
                return {
                    "schema_version": 2,
                    "tenant_id": "tenant-a",
                    "connector": "connector-a",
                    "members": {"published": 0, "withdrawn": 0, "retired": 0},
                    "warnings": [],
                    "projection": {"projection": "none"},
                }
            if operation == "import":
                request = params["op"]["request"]
                return {
                    "result": "unchanged",
                    "pack_digest": request["index"]["pack_digest"],
                    "binding_revision": 4,
                }
        raise AssertionError((method, params))


def test_typed_status_content_and_import_use_only_generated_wire_shapes() -> None:
    async def exercise() -> tuple[_Client, PackImportResultUnchanged]:
        transport = _Client()
        client = ConnectorPackClient(transport)
        status = await client.status(tenant_id="tenant-a", connector="connector-a")
        assert status.connector == "connector-a"
        content = await client.content(
            tenant_id="tenant-a", component_id="mcp:connector-a/tool/search"
        )
        assert content.body == b'{"name":"search"}'
        result = await client.import_pack(
            _built_archive(),
            connector="connector-a",
            server_package_version="2.3.1",
            producer=PackProducer(name="agent-connector-sdk", version="0.1.0"),
            catalog=_catalog(),
            context=_context(),
        )
        assert isinstance(result, PackImportResultUnchanged)
        return transport, result

    transport, result = asyncio.run(exercise())
    pack_call = next(
        call
        for call in transport.sent
        if call[0] == "ConnectorPack" and call[1]["op"]["op"] == "import"
    )
    request = pack_call[1]["op"]["request"]
    expected_key = connector_pack_import_key(
        "connector-a", result.pack_digest, expected_head=None
    )
    assert pack_call[3] == expected_key
    assert request["context"]["idempotency_key"] == expected_key
    assert request["index"]["archive"]["blob_digest"] == "blob-manifest-a"
    assert [call[3] for call in transport.sent if call[0].startswith("Blob")] == [
        f"connector-pack:connector-a:archive:{result.pack_digest}:begin:1048576",
        f"connector-pack:connector-a:archive:{result.pack_digest}:chunk:0",
        f"connector-pack:connector-a:archive:{result.pack_digest}:commit",
    ]


def test_rejected_import_is_a_typed_result_not_a_fabricated_exception() -> None:
    class Rejected(_Client):
        async def _send(
            self, method: str, params: Any, graph: Any, **kwargs: Any
        ) -> Any:
            if method == "ConnectorPack":
                return {
                    "result": "rejected",
                    "pack_digest": None,
                    "violations": [
                        {
                            "code": "MALFORMED_INDEX",
                            "detail": "fixture",
                        }
                    ],
                    "budget_exhausted": False,
                }
            return await super()._send(method, params, graph, **kwargs)

    async def exercise() -> Any:
        return await ConnectorPackClient(Rejected()).import_pack(
            _built_archive(),
            connector="connector-a",
            server_package_version="2.3.1",
            producer=PackProducer(name="agent-connector-sdk", version="0.1.0"),
            catalog=_catalog(),
            context=_context(),
            blob_digest="already-uploaded",
        )

    result = asyncio.run(exercise())
    assert isinstance(result, PackImportResultRejected)
    assert result.violations[0].code.value == "MALFORMED_INDEX"


def test_head_conflict_is_a_typed_closed_write_error() -> None:
    class Conflict(_Client):
        async def _send(
            self, method: str, params: Any, graph: Any, **kwargs: Any
        ) -> Any:
            if method == "ConnectorPack":
                self.sent.append((method, params, graph, kwargs["idempotency_key"]))
                raise RuntimeError("PACK_HEAD_CONFLICT: connector pack head changed")
            return await super()._send(method, params, graph, **kwargs)

    async def exercise() -> tuple[Conflict, ConnectorPackWriteError]:
        transport = Conflict()
        client = ConnectorPackClient(transport)
        try:
            await client.import_pack(
                _built_archive(),
                connector="connector-a",
                server_package_version="2.3.1",
                producer=PackProducer(name="agent-connector-sdk", version="0.1.0"),
                catalog=_catalog(),
                context=_context(),
                blob_digest="already-uploaded",
            )
        except ConnectorPackWriteError as error:
            return transport, error
        raise AssertionError("the engine conflict was not surfaced")

    _, error = asyncio.run(exercise())
    assert error.code is PackWriteErrorCode.PACK_HEAD_CONFLICT
    assert error.detail == "connector pack head changed"


def test_transport_loss_retry_reuses_the_exact_import_operation_key() -> None:
    class LostResponse(_Client):
        import_attempts = 0

        async def _send(
            self, method: str, params: Any, graph: Any, **kwargs: Any
        ) -> Any:
            if method == "ConnectorPack":
                self.sent.append((method, params, graph, kwargs["idempotency_key"]))
                self.import_attempts += 1
                if self.import_attempts == 1:
                    raise ConnectionError("response lost after commit")
                request = params["op"]["request"]
                return {
                    "result": "unchanged",
                    "pack_digest": request["index"]["pack_digest"],
                    "binding_revision": 4,
                }
            return await super()._send(method, params, graph, **kwargs)

    async def exercise() -> LostResponse:
        transport = LostResponse()
        client = ConnectorPackClient(transport)
        arguments: dict[str, Any] = {
            "connector": "connector-a",
            "server_package_version": "2.3.1",
            "producer": PackProducer(name="agent-connector-sdk", version="0.1.0"),
            "catalog": _catalog(),
            "context": _context(),
            "blob_digest": "already-uploaded",
        }
        with pytest.raises(ConnectionError, match="response lost after commit"):
            await client.import_pack(_built_archive(), **arguments)
        result = await client.import_pack(_built_archive(), **arguments)
        assert isinstance(result, PackImportResultUnchanged)
        return transport

    transport = asyncio.run(exercise())
    import_keys = [call[3] for call in transport.sent]
    assert len(import_keys) == 2
    assert import_keys[0] == import_keys[1]


def test_public_wheel_surface_exports_the_connector_pack_client() -> None:
    import epistemic_graph

    assert epistemic_graph.ConnectorPackClient is ConnectorPackClient
    assert epistemic_graph.ConnectorPackArchiveBuilder is ConnectorPackArchiveBuilder
    assert ExportedRejected is PackImportResultRejected
    assert ExportedUnchanged is PackImportResultUnchanged
    assert epistemic_graph.ConnectorPackWriteError is ConnectorPackWriteError
    assert epistemic_graph.PackWriteErrorCode is PackWriteErrorCode
