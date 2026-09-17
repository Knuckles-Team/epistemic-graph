"""The normative pack digest layout (PACK-IMPORT-DESIGN §3.1), in pure Python.

Nested digests are framed as their 32 raw bytes; strings are UTF-8; string lists
are sorted by UTF-8 bytes and de-duplicated; integers are u64 big-endian; an
absent optional is an empty field; an optional boolean is one byte (0x00 absent,
0x01 false, 0x02 true).
"""

from __future__ import annotations

from collections.abc import Iterable

from .._digest import framed, sha256_raw, u64_be
from .model import Annotations, BuiltPack, ModelAnnotation, PackEntry, PackRef, Section


def _text(value: str | None) -> bytes:
    return b"" if value is None else value.encode("utf-8")


def _int(value: int | None) -> bytes:
    return b"" if value is None else u64_be(value)


def _tri(value: bool | None) -> bytes:
    return b"\x00" if value is None else (b"\x02" if value else b"\x01")


def string_list(domain: str, items: Iterable[str]) -> bytes:
    encoded = sorted({item.encode("utf-8") for item in items})
    return framed(domain.encode(), encoded)


def model_facts_digest(model: ModelAnnotation) -> bytes:
    return framed(
        b"eg/cp-model-facts/v1",
        [
            _text(model.provider),
            _text(model.model_identity),
            u64_be(model.context_window_tokens),
            u64_be(model.max_output_tokens),
            _tri(model.supports_tools),
            _tri(model.supports_structured_output),
            _tri(model.supports_vision),
        ],
    )


def annotations_digest(a: Annotations) -> bytes:
    cost, latency = a.cost, a.latency_declared
    return framed(
        b"eg/connector-pack-annotations/v1",
        [
            string_list("eg/cp-provides/v1", a.provides),
            string_list("eg/cp-requires-capabilities/v1", a.requires_capabilities),
            string_list("eg/cp-modalities-in/v1", a.modalities_in),
            string_list("eg/cp-modalities-out/v1", a.modalities_out),
            string_list("eg/cp-required-scopes/v1", a.required_scopes),
            _tri(a.read_only_hint),
            _tri(a.destructive_hint),
            _tri(a.idempotent_hint),
            _tri(a.open_world_hint),
            _text(a.contract_version),
            _text(cost.currency if cost else None),
            _int(cost.per_call_micros if cost else None),
            _int(cost.input_per_mtok_micros if cost else None),
            _int(cost.output_per_mtok_micros if cost else None),
            _int(latency.p50_ms if latency else None),
            _int(latency.p95_ms if latency else None),
            model_facts_digest(a.model) if a.model else b"",
            _text(a.sdk_contract_pin),
        ],
    )


def references_digest(references: Iterable[PackRef]) -> bytes:
    pairs = sorted((ref.uri.encode(), ref.kind.encode()) for ref in references)
    return framed(
        b"eg/connector-pack-references/v1", [part for pair in pairs for part in pair]
    )


def _section_digest(pack_archive: bytes, section: Section | None) -> bytes:
    if section is None:
        return b""
    return sha256_raw(pack_archive[section.offset : section.offset + section.length])


def entry_digest(archive: bytes, entry: PackEntry) -> bytes:
    return framed(
        b"eg/connector-pack-entry/v1",
        [
            _text(entry.kind),
            _text(entry.uri),
            _text(entry.name),
            _text(entry.media_type),
            _section_digest(archive, entry.body),
            _section_digest(archive, entry.input_schema),
            _section_digest(archive, entry.output_schema),
            annotations_digest(entry.annotations),
            references_digest(entry.references),
        ],
    )


def pack_digest(
    connector: str, archive: bytes, server: PackEntry, entries: Iterable[PackEntry]
) -> bytes:
    ordered = sorted(entries, key=lambda entry: entry.uri.encode())
    return framed(
        b"eg/connector-pack/v1",
        [
            _text(connector),
            entry_digest(archive, server),
            u64_be(len(ordered)),
            *(entry_digest(archive, entry) for entry in ordered),
        ],
    )


def recomputed_pack_digest(pack: BuiltPack) -> str:
    index = pack.index
    return pack_digest(index.connector, pack.archive, index.server, index.entries).hex()
