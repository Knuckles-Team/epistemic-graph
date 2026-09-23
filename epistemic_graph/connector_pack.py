"""Typed ConnectorPack construction and transport.

The Rust wire contract remains the only owner of pack shapes, schema versions,
digest domains, and import dispositions.  This module composes the generated
DTOs and senders into the one ergonomic client seam needed by connector-sync;
it does not introduce a second pack model.
"""

from __future__ import annotations

import hashlib
import struct
from collections.abc import Sequence
from dataclasses import dataclass, field

from ._transport import EngineTransport
from .generated.agent_component import (
    AgentComponentContentRequest,
    AgentComponentContentResult,
)
from .generated.connector_pack import (
    CONNECTOR_PACK_SCHEMA_VERSION,
    AgentLibraryMutationContext,
    ConnectorPackImportRequest,
    ConnectorPackIndex,
    ConnectorPackStatus,
    ConnectorPackStatusRequest,
    McpCatalogSnapshotBinding,
    PackAnnotations,
    PackArchiveRef,
    PackEntry,
    PackEntryKind,
    PackHeadRef,
    PackImportResult,
    PackImportResultImported,
    PackImportResultRejected,
    PackImportResultUnchanged,
    PackModelFacts,
    PackProducer,
    PackRef,
    PackSection,
    PackWriteErrorCode,
)
from .generated.digest import framed_sha256
from .generated.storage import (
    send_agent_component_content,
    send_blob_begin,
    send_blob_chunk_put,
    send_blob_commit,
    send_connector_pack_import,
    send_connector_pack_status,
)

_PACK_DIGEST_DOMAIN = b"eg/connector-pack/v2"
_ENTRY_DIGEST_DOMAIN = b"eg/connector-pack-entry/v1"
_ANNOTATIONS_DIGEST_DOMAIN = b"eg/connector-pack-annotations/v1"
_MODEL_FACTS_DIGEST_DOMAIN = b"eg/cp-model-facts/v1"
_REFERENCES_DIGEST_DOMAIN = b"eg/connector-pack-references/v1"
_CATALOG_DIGEST_DOMAIN = b"eg/mcp-catalog-binding/v1"
_PROVIDES_DOMAIN = b"eg/cp-provides/v1"
_REQUIRES_CAPABILITIES_DOMAIN = b"eg/cp-requires-capabilities/v1"
_MODALITIES_IN_DOMAIN = b"eg/cp-modalities-in/v1"
_MODALITIES_OUT_DOMAIN = b"eg/cp-modalities-out/v1"
_REQUIRED_SCOPES_DOMAIN = b"eg/cp-required-scopes/v1"


class ConnectorPackWriteError(RuntimeError):
    """A closed Rust-owned ConnectorPack write error returned by the engine."""

    def __init__(self, code: PackWriteErrorCode, detail: str = "") -> None:
        self.code = code
        self.detail = detail
        message = code.value if not detail else f"{code.value}: {detail}"
        super().__init__(message)

    @classmethod
    def from_runtime_error(cls, error: RuntimeError) -> ConnectorPackWriteError | None:
        if isinstance(error, cls):
            return error
        code_text, separator, detail = str(error).partition(":")
        try:
            code = PackWriteErrorCode(code_text.strip())
        except ValueError:
            return None
        return cls(code, detail.strip() if separator else "")


def _text(value: str | None) -> bytes:
    return b"" if value is None else value.encode("utf-8")


def _u64(value: int) -> bytes:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not 0 <= value <= 2**64 - 1
    ):
        raise ValueError("digest integer must be an unsigned 64-bit value")
    return struct.pack(">Q", value)


def _optional_u64(value: int | None) -> bytes:
    return b"" if value is None else _u64(value)


def _tri(value: bool | None) -> bytes:
    if value is None:
        return b"\x00"
    if not isinstance(value, bool):
        raise TypeError("digest tri-state must be bool or None")
    return b"\x02" if value else b"\x01"


def _raw_digest(value: str) -> bytes:
    if len(value) != 64 or value.lower() != value:
        raise ValueError("digest must be 64 lowercase hexadecimal characters")
    try:
        raw = bytes.fromhex(value)
    except ValueError as exc:
        raise ValueError("digest must be 64 lowercase hexadecimal characters") from exc
    if len(raw) != 32:
        raise ValueError("digest must be 64 lowercase hexadecimal characters")
    return raw


def _list_digest(domain: bytes, values: Sequence[str] | None) -> bytes:
    fields = sorted({value.encode("utf-8") for value in values or ()})
    return _raw_digest(framed_sha256(domain, fields))


def _model_facts_digest(model: PackModelFacts) -> bytes:
    return _raw_digest(
        framed_sha256(
            _MODEL_FACTS_DIGEST_DOMAIN,
            [
                _text(model.provider),
                _text(model.model_identity),
                _u64(model.context_window_tokens),
                _u64(model.max_output_tokens),
                _tri(model.supports_tools),
                _tri(model.supports_structured_output),
                _tri(model.supports_vision),
            ],
        )
    )


def _annotations_digest(annotations: PackAnnotations) -> bytes:
    cost = annotations.cost
    latency = annotations.latency_declared
    model = annotations.model
    return _raw_digest(
        framed_sha256(
            _ANNOTATIONS_DIGEST_DOMAIN,
            [
                _list_digest(_PROVIDES_DOMAIN, annotations.provides),
                _list_digest(
                    _REQUIRES_CAPABILITIES_DOMAIN,
                    annotations.requires_capabilities,
                ),
                _list_digest(_MODALITIES_IN_DOMAIN, annotations.modalities_in),
                _list_digest(_MODALITIES_OUT_DOMAIN, annotations.modalities_out),
                _list_digest(_REQUIRED_SCOPES_DOMAIN, annotations.required_scopes),
                _tri(annotations.read_only_hint),
                _tri(annotations.destructive_hint),
                _tri(annotations.idempotent_hint),
                _tri(annotations.open_world_hint),
                _text(annotations.contract_version),
                _text(cost.currency if cost is not None else None),
                _optional_u64(cost.per_call_micros if cost is not None else None),
                _optional_u64(cost.input_per_mtok_micros if cost is not None else None),
                _optional_u64(
                    cost.output_per_mtok_micros if cost is not None else None
                ),
                _optional_u64(latency.p50_ms if latency is not None else None),
                _optional_u64(latency.p95_ms if latency is not None else None),
                _model_facts_digest(model) if model is not None else b"",
                _text(annotations.sdk_contract_pin),
            ],
        )
    )


def _references_digest(references: Sequence[PackRef] | None) -> bytes:
    pairs = sorted(
        (
            reference.uri.encode("utf-8"),
            reference.kind.value.encode("utf-8"),
        )
        for reference in references or ()
    )
    return _raw_digest(
        framed_sha256(
            _REFERENCES_DIGEST_DOMAIN,
            [part for pair in pairs for part in pair],
        )
    )


def _entry_digest(entry: PackEntry) -> bytes:
    return _raw_digest(
        framed_sha256(
            _ENTRY_DIGEST_DOMAIN,
            [
                entry.kind.value.encode("utf-8"),
                entry.uri.encode("utf-8"),
                entry.name.encode("utf-8"),
                entry.media_type.encode("utf-8"),
                _raw_digest(entry.body.sha256),
                (
                    _raw_digest(entry.input_schema.sha256)
                    if entry.input_schema is not None
                    else b""
                ),
                (
                    _raw_digest(entry.output_schema.sha256)
                    if entry.output_schema is not None
                    else b""
                ),
                _annotations_digest(entry.annotations or PackAnnotations()),
                _references_digest(entry.references),
            ],
        )
    )


def _catalog_digest(catalog: McpCatalogSnapshotBinding) -> bytes:
    return _raw_digest(
        framed_sha256(
            _CATALOG_DIGEST_DOMAIN,
            [
                _u64(catalog.configuration_revision),
                _u64(catalog.catalog_generation),
                _raw_digest(catalog.snapshot_digest),
                _u64(catalog.child_connection_generation),
                _raw_digest(catalog.authorization_scope_digest),
            ],
        )
    )


def pack_digest(
    connector: str,
    catalog: McpCatalogSnapshotBinding,
    server: PackEntry,
    entries: Sequence[PackEntry],
) -> str:
    """Return the exact digest computed by Rust ``connector_pack::pack_digest``.

    Archive layout, archive blob identity, producer, and package version are
    intentionally absent.  Section SHA-256 values, the exact served catalog
    binding, and every entry annotation/reference are included.
    """

    ordered = sorted(entries, key=lambda entry: entry.uri.encode("utf-8"))
    return framed_sha256(
        _PACK_DIGEST_DOMAIN,
        [
            connector.encode("utf-8"),
            _catalog_digest(catalog),
            _entry_digest(server),
            _u64(len(ordered)),
            *(_entry_digest(entry) for entry in ordered),
        ],
    )


@dataclass(frozen=True)
class ConnectorPackEntryContent:
    """One generated ``PackEntry`` before its bytes receive archive offsets."""

    kind: PackEntryKind
    uri: str
    name: str
    media_type: str
    body: bytes
    input_schema: bytes | None = None
    output_schema: bytes | None = None
    annotations: PackAnnotations = field(default_factory=PackAnnotations)
    references: tuple[PackRef, ...] = ()


@dataclass(frozen=True)
class ConnectorPackArchive:
    """An immutable archive plus the generated entries whose sections address it."""

    data: bytes
    server: PackEntry
    entries: tuple[PackEntry, ...]

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.data).hexdigest()

    def index(
        self,
        *,
        connector: str,
        blob_digest: str,
        server_package_version: str,
        producer: PackProducer,
        catalog: McpCatalogSnapshotBinding,
    ) -> ConnectorPackIndex:
        digest = pack_digest(connector, catalog, self.server, self.entries)
        return ConnectorPackIndex(
            schema_version=CONNECTOR_PACK_SCHEMA_VERSION,
            connector=connector,
            server=self.server,
            server_package_version=server_package_version,
            archive=PackArchiveRef(
                blob_digest=blob_digest,
                length=len(self.data),
                sha256=self.sha256,
            ),
            entries=list(self.entries),
            producer=producer,
            catalog=catalog,
            pack_digest=digest,
        )


class ConnectorPackArchiveBuilder:
    """Build one gap-free archive from generated-contract entry content."""

    @staticmethod
    def build(
        server: ConnectorPackEntryContent,
        entries: Sequence[ConnectorPackEntryContent],
    ) -> ConnectorPackArchive:
        if server.kind is not PackEntryKind.MCP_SERVER:
            raise ValueError("the server entry kind must be mcp_server")
        if any(entry.kind is PackEntryKind.MCP_SERVER for entry in entries):
            raise ValueError("mcp_server is carried only by the server entry")
        ordered = sorted(entries, key=lambda entry: entry.uri.encode("utf-8"))
        uris = [entry.uri for entry in ordered]
        uri_set = set(uris)
        if len(uri_set) != len(uris):
            raise ValueError("connector pack entry URIs must be unique")
        if server.uri in uri_set:
            raise ValueError("the server URI must not also appear in entries")

        archive = bytearray()

        def place(value: bytes | None) -> PackSection | None:
            if value is None:
                return None
            if not isinstance(value, bytes):
                raise TypeError("connector pack sections must be immutable bytes")
            section = PackSection(
                offset=len(archive),
                length=len(value),
                sha256=hashlib.sha256(value).hexdigest(),
            )
            archive.extend(value)
            return section

        def materialize(content: ConnectorPackEntryContent) -> PackEntry:
            body = place(content.body)
            assert body is not None
            return PackEntry(
                kind=content.kind,
                uri=content.uri,
                name=content.name,
                media_type=content.media_type,
                body=body,
                input_schema=place(content.input_schema),
                output_schema=place(content.output_schema),
                annotations=PackAnnotations.model_validate(content.annotations),
                references=[PackRef.model_validate(ref) for ref in content.references],
            )

        built_server = materialize(server)
        built_entries = tuple(materialize(entry) for entry in ordered)
        return ConnectorPackArchive(bytes(archive), built_server, built_entries)


def connector_pack_import_key(
    connector: str,
    digest: str,
    expected_head: PackHeadRef | None,
) -> str:
    """Stable operation identity required by the ConnectorPack replay contract."""

    _raw_digest(digest)
    expected_revision = 0 if expected_head is None else expected_head.binding_revision
    return f"connector-pack:{connector}:import:{digest}:{expected_revision}"


class ConnectorPackClient:
    """Typed async facade over generated ConnectorPack and body-read senders."""

    DEFAULT_CHUNK_SIZE = 1 << 20

    def __init__(self, client: EngineTransport) -> None:
        self._client = client

    async def status(
        self,
        *,
        tenant_id: str,
        connector: str,
        graph: str | None = None,
    ) -> ConnectorPackStatus:
        return await send_connector_pack_status(
            self._client,
            ConnectorPackStatusRequest(tenant_id=tenant_id, connector=connector),
            graph,
        )

    async def content(
        self,
        *,
        tenant_id: str,
        component_id: str,
        entry_revision: int | None = None,
        graph: str | None = None,
    ) -> AgentComponentContentResult:
        return await send_agent_component_content(
            self._client,
            AgentComponentContentRequest(
                tenant_id=tenant_id,
                component_id=component_id,
                entry_revision=entry_revision,
            ),
            graph,
        )

    async def upload_archive(
        self,
        archive: ConnectorPackArchive,
        *,
        operation_key: str,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        graph: str | None = None,
    ) -> str:
        """Upload with a deterministic identity for begin, every chunk, and commit.

        A lost response can therefore retry the same RPC without appending a
        duplicate chunk.  ``operation_key`` must include the pack digest, so a
        later catalog generation carrying identical archive bytes starts a new
        cursor instead of replaying an already-committed one.
        """

        if not operation_key:
            raise ValueError("operation_key must not be empty")
        if not 1 <= chunk_size <= 2**31 - 1:
            raise ValueError("chunk_size must be in 1..=2147483647")
        cursor = await send_blob_begin(
            self._client,
            {"chunk_size": chunk_size},
            graph,
            idempotency_key=f"{operation_key}:begin:{chunk_size}",
        )
        for ordinal, offset in enumerate(range(0, len(archive.data), chunk_size)):
            await send_blob_chunk_put(
                self._client,
                {
                    "cursor": cursor,
                    "data": archive.data[offset : offset + chunk_size],
                },
                graph,
                idempotency_key=f"{operation_key}:chunk:{ordinal}",
            )
        return await send_blob_commit(
            self._client,
            {"cursor": cursor},
            graph,
            idempotency_key=f"{operation_key}:commit",
        )

    async def import_pack(
        self,
        archive: ConnectorPackArchive,
        *,
        connector: str,
        server_package_version: str,
        producer: PackProducer,
        catalog: McpCatalogSnapshotBinding,
        context: AgentLibraryMutationContext,
        expected_head: PackHeadRef | None = None,
        allow_mass_withdrawal: bool = False,
        blob_digest: str | None = None,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        graph: str | None = None,
    ) -> PackImportResult:
        """Upload and import one pack, returning the engine's exact disposition."""

        digest = pack_digest(connector, catalog, archive.server, archive.entries)
        operation_key = connector_pack_import_key(connector, digest, expected_head)
        if blob_digest is None:
            blob_digest = await self.upload_archive(
                archive,
                operation_key=f"connector-pack:{connector}:archive:{digest}",
                chunk_size=chunk_size,
                graph=graph,
            )
        index = archive.index(
            connector=connector,
            blob_digest=blob_digest,
            server_package_version=server_package_version,
            producer=producer,
            catalog=catalog,
        )
        request = ConnectorPackImportRequest(
            context=context.model_copy(update={"idempotency_key": operation_key}),
            index=index,
            expected_head=expected_head,
            allow_mass_withdrawal=allow_mass_withdrawal,
        )
        try:
            return await send_connector_pack_import(
                self._client,
                request,
                graph,
                idempotency_key=operation_key,
            )
        except RuntimeError as error:
            typed = ConnectorPackWriteError.from_runtime_error(error)
            if typed is None:
                raise
            raise typed from error


__all__ = [
    "ConnectorPackArchive",
    "ConnectorPackArchiveBuilder",
    "ConnectorPackClient",
    "ConnectorPackEntryContent",
    "ConnectorPackWriteError",
    "PackImportResult",
    "PackImportResultImported",
    "PackImportResultRejected",
    "PackImportResultUnchanged",
    "PackWriteErrorCode",
    "connector_pack_import_key",
    "pack_digest",
]
