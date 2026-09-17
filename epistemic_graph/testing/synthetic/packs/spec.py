"""Editable pack content, and its assembly into an index plus archive bytes."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field, replace

from .._digest import sha256_raw
from .digest import pack_digest
from .model import (
    SCHEMA_VERSION,
    Annotations,
    Archive,
    BuiltPack,
    ConnectorPackIndex,
    PackEntry,
    PackRef,
    Producer,
    Section,
)


@dataclass(frozen=True)
class EntrySpec:
    kind: str
    uri: str
    name: str
    media_type: str
    body: bytes
    input_schema: bytes | None = None
    output_schema: bytes | None = None
    annotations: Annotations = field(default_factory=Annotations)
    references: tuple[PackRef, ...] = ()


@dataclass(frozen=True)
class PackSpec:
    connector: str
    server: EntrySpec
    entries: tuple[EntrySpec, ...]
    server_package_version: str = "1.0.0"
    producer: Producer = field(
        default_factory=lambda: Producer(name="synthetic-sdk", version="1")
    )
    schema_version: int = SCHEMA_VERSION
    sort_entries: bool = True

    def with_entries(self, entries: Sequence[EntrySpec]) -> PackSpec:
        return replace(self, entries=tuple(entries))


class _Layout:
    """Appends sections back to back: no gaps, no overlap, uncompressed."""

    def __init__(self) -> None:
        self.data = bytearray()

    def place(self, content: bytes | None) -> Section | None:
        if content is None:
            return None
        section = Section(
            offset=len(self.data), length=len(content), sha256=sha256_raw(content).hex()
        )
        self.data += content
        return section


def _entry(layout: _Layout, spec: EntrySpec) -> PackEntry:
    body = layout.place(spec.body)
    assert body is not None
    return PackEntry(
        kind=spec.kind,
        uri=spec.uri,
        name=spec.name,
        media_type=spec.media_type,
        body=body,
        input_schema=layout.place(spec.input_schema),
        output_schema=layout.place(spec.output_schema),
        annotations=spec.annotations,
        references=spec.references,
    )


def assemble(spec: PackSpec) -> BuiltPack:
    """Lay out every section and compute the producer's digests honestly."""
    layout = _Layout()
    server = _entry(layout, spec.server)
    ordered = (
        sorted(spec.entries, key=lambda e: e.uri.encode())
        if spec.sort_entries
        else list(spec.entries)
    )
    entries = tuple(_entry(layout, entry) for entry in ordered)
    archive = bytes(layout.data)
    index = ConnectorPackIndex(
        schema_version=spec.schema_version,
        connector=spec.connector,
        server=server,
        server_package_version=spec.server_package_version,
        archive=Archive(length=len(archive), sha256=sha256_raw(archive).hex()),
        entries=entries,
        producer=spec.producer,
        pack_digest=pack_digest(spec.connector, archive, server, entries).hex(),
    )
    return BuiltPack(index=index, archive=archive)
