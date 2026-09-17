"""Index, bound, archive, section, digest, identity and policy defects."""

from __future__ import annotations

from dataclasses import replace

from .._digest import sha256_raw
from .._rng import SeededStream
from . import content
from .digest import pack_digest
from .malformed import MalformedPack, Mutation, added, case, edited, first
from .model import BuiltPack, PackEntry, PackRef, Section
from .spec import EntrySpec, PackSpec, assemble

_MIB = 1024 * 1024
_TOO_LARGE = ("PACK_TOO_LARGE",)
_MALFORMED = ("MALFORMED_INDEX",)


def _tools(spec: PackSpec, count: int, uri_padding: int = 0) -> list[EntrySpec]:
    stream = SeededStream(0, "malformed/tools")
    pad = "p" * uri_padding
    return [
        replace(tool, uri=f"{tool.uri}{pad}")
        for tool in (
            content.tool(stream.child(str(i)), spec.connector, 1000 + i)
            for i in range(count)
        )
    ]


def _with_index(
    pack: BuiltPack, archive_bytes: bytes | None = None, **changes: object
) -> BuiltPack:
    """Change index fields after assembly, keeping the producer's pack digest honest."""
    data = pack.archive if archive_bytes is None else archive_bytes
    index = pack.index.model_copy(update=changes)
    digest = pack_digest(index.connector, data, index.server, index.entries).hex()
    updated = index.model_dump() | {"pack_digest": digest}
    return BuiltPack.model_validate({"index": updated, "archive": data})


def g1_unsorted(spec: PackSpec) -> MalformedPack:
    reordered = replace(spec, entries=tuple(reversed(spec.entries)), sort_entries=False)
    return case(
        "G1",
        "unsorted_uris",
        "entries not in UTF-8 URI order",
        assemble(reordered),
        _MALFORMED,
    )


def g1_scheme(spec: PackSpec) -> MalformedPack:
    pack = edited(spec, "tool", uri=f"foo://{spec.connector}/tool_000")
    return case(
        "G1", "scheme_mismatch", "a tool under the foo:// scheme", pack, _MALFORMED
    )


def g1_two_servers(spec: PackSpec) -> MalformedPack:
    pack = added(spec, content.server("second-server"))
    return case("G1", "two_servers", "a second mcp_server entry", pack, _MALFORMED)


def g1_unknown_kind(spec: PackSpec) -> MalformedPack:
    widget = replace(
        content.prompt(spec.connector, 9),
        kind="widget",
        uri=f"widget://{spec.connector}/w",
    )
    return case(
        "G1",
        "unknown_kind",
        "an entry kind outside the closed set",
        added(spec, widget),
        ("UNKNOWN_ENTRY_KIND",),
        permitted=_MALFORMED,
    )


def g1_schema_version(spec: PackSpec) -> MalformedPack:
    return case(
        "G1",
        "schema_version",
        "an unsupported schema version",
        assemble(replace(spec, schema_version=2)),
        _MALFORMED,
    )


def g1_connector(spec: PackSpec) -> MalformedPack:
    return case(
        "G1",
        "invalid_connector",
        "a connector that is not a resource id",
        assemble(replace(spec, connector="Bad Connector!")),
        _MALFORMED,
    )


def g1_long_name(spec: PackSpec) -> MalformedPack:
    return case(
        "G1",
        "name_over_256_bytes",
        "an entry name of 257 bytes",
        edited(spec, "prompt", name="n" * 257),
        _MALFORMED,
    )


def g2_entries(spec: PackSpec) -> MalformedPack:
    pack = assemble(spec.with_entries(_tools(spec, 1_025)))
    return case("G2", "entries_over_1024", "1,025 entries", pack, _TOO_LARGE)


def g2_body(spec: PackSpec) -> MalformedPack:
    skill = first(spec, "skill")
    body = skill.body + b"x" * (2 * _MIB + 1 - len(skill.body))
    return case(
        "G2",
        "body_over_2mib",
        "a body of 2 MiB + 1 byte",
        edited(spec, "skill", body=body),
        _TOO_LARGE,
    )


def g2_index(spec: PackSpec) -> MalformedPack:
    pack = assemble(spec.with_entries(_tools(spec, 1_024, uri_padding=1_200)))
    return case(
        "G2", "index_over_1mib", "an encoded index well over 1 MiB", pack, _TOO_LARGE
    )


def g2_references(spec: PackSpec) -> MalformedPack:
    tools = _tools(spec, 65)
    refs = tuple(PackRef(uri=t.uri, kind="tool") for t in tools)
    skill = replace(first(spec, "skill"), references=refs)
    pack = assemble(spec.with_entries([*tools, skill]))
    return case(
        "G2", "references_over_64", "a skill with 65 references", pack, _TOO_LARGE
    )


def g2_iri_list(spec: PackSpec) -> MalformedPack:
    tool = first(spec, "tool")
    provides = tuple(
        sorted(f"https://synthetic.example/capability/c{i:02d}" for i in range(65))
    )
    annotations = tool.annotations.model_copy(update={"provides": provides})
    pack = edited(spec, "tool", annotations=annotations)
    return case(
        "G2",
        "iri_list_over_64",
        "65 items in one IRI list",
        pack,
        _TOO_LARGE,
        permitted=("UNRESOLVED_CAPABILITY_IRI",),
    )


def g2_archive(spec: PackSpec) -> MalformedPack:
    big = [
        content.skill(spec.connector, 10 + i, (), size=_MIB * 19 // 10)
        for i in range(9)
    ]
    return case(
        "G2",
        "archive_over_16mib",
        "nine 1.9 MiB skills, a 17 MiB archive",
        added(spec, *big),
        _TOO_LARGE,
    )


def g2_schema(spec: PackSpec) -> MalformedPack:
    schema = (
        b'{"description":"'
        + b"d" * _MIB
        + b'","properties":{"q":{"type":"string"}},"type":"object"}'
    )
    return case(
        "G2",
        "schema_over_1mib",
        "a tool input schema over 1 MiB",
        edited(spec, "tool", input_schema=schema),
        _TOO_LARGE,
    )


def g3_length(spec: PackSpec) -> MalformedPack:
    pack = assemble(spec)
    archive = pack.index.archive.model_copy(
        update={"length": pack.index.archive.length + 1}
    )
    return case(
        "G3",
        "archive_length",
        "the declared archive length is one byte long",
        _with_index(pack, archive=archive),
        ("ARCHIVE_DIGEST_MISMATCH",),
    )


def g3_sha(spec: PackSpec) -> MalformedPack:
    pack = assemble(spec)
    archive = pack.index.archive.model_copy(
        update={"sha256": sha256_raw(b"other").hex()}
    )
    return case(
        "G3",
        "archive_sha256",
        "the declared archive SHA-256 is another file's",
        _with_index(pack, archive=archive),
        ("ARCHIVE_DIGEST_MISMATCH",),
    )


def g3_missing(spec: PackSpec) -> MalformedPack:
    return case(
        "G3",
        "archive_not_uploaded",
        "the archive was never uploaded",
        assemble(spec),
        ("ARCHIVE_MISSING",),
        upload_archive=False,
    )


def _resection(pack: BuiltPack, position: int, body: Section) -> BuiltPack:
    entries = list(pack.index.entries)
    entries[position] = entries[position].model_copy(update={"body": body})
    return _with_index(pack, entries=tuple(entries))


def g4_overlap(spec: PackSpec) -> MalformedPack:
    pack = assemble(spec)
    shared = pack.index.entries[0].body
    return case(
        "G4",
        "overlapping_sections",
        "two bodies share one section",
        _resection(pack, 1, shared),
        ("MALFORMED_SECTIONS",),
    )


def g4_gap(spec: PackSpec) -> MalformedPack:
    pack = assemble(spec)
    archive = pack.archive + b"\x00" * 16
    declared = pack.index.archive.model_copy(
        update={"length": len(archive), "sha256": sha256_raw(archive).hex()}
    )
    return case(
        "G4",
        "uncovered_bytes",
        "16 archive bytes no section covers",
        _with_index(pack, archive=declared, archive_bytes=archive),
        ("MALFORMED_SECTIONS",),
    )


def g4_overflow(spec: PackSpec) -> MalformedPack:
    pack = assemble(spec)
    body = pack.index.entries[0].body.model_copy(
        update={"offset": (1 << 64) - 1, "length": 2}
    )
    return case(
        "G4",
        "offset_overflow",
        "offset + length overflows u64",
        _resection(pack, 0, body),
        ("MALFORMED_SECTIONS",),
        permitted=("PACK_DIGEST_MISMATCH",),
    )


def g5_flipped(spec: PackSpec) -> MalformedPack:
    pack = assemble(spec)
    skill: PackEntry = next(e for e in pack.index.entries if e.kind == "skill")
    archive = bytearray(pack.archive)
    archive[skill.body.offset + skill.body.length - 2] ^= 0x01
    data = bytes(archive)
    declared = pack.index.archive.model_copy(update={"sha256": sha256_raw(data).hex()})
    index = pack.index.model_copy(update={"archive": declared})
    tampered = BuiltPack.model_validate({"index": index.model_dump(), "archive": data})
    return case(
        "G5",
        "flipped_body_byte",
        "one flipped byte in a skill body",
        tampered,
        ("PACK_DIGEST_MISMATCH",),
    )


def g5_pack_digest(spec: PackSpec) -> MalformedPack:
    pack = assemble(spec)
    index = pack.index.model_copy(update={"pack_digest": sha256_raw(b"forged").hex()})
    forged = BuiltPack.model_validate(
        {"index": index.model_dump(), "archive": pack.archive}
    )
    return case(
        "G5",
        "forged_pack_digest",
        "a claimed pack digest of other content",
        forged,
        ("PACK_DIGEST_MISMATCH",),
    )


def g6_duplicate(spec: PackSpec) -> MalformedPack:
    tool = first(spec, "tool")
    twins = [
        replace(tool, uri=f"tool://{spec.connector}/dup-{s}", name="dup") for s in "ab"
    ]
    return case(
        "G6",
        "duplicate_component_id",
        "two URIs whose derived component ids collide",
        added(spec, *twins),
        ("DUPLICATE_COMPONENT_ID",),
    )


def g6_decide_kind(spec: PackSpec) -> MalformedPack:
    head = replace(
        content.prompt(spec.connector, 8),
        kind="decision_head",
        uri=f"decision-head://{spec.connector}/h",
    )
    return case(
        "G6",
        "decide_owned_kind",
        "a DecisionHead entry",
        added(spec, head),
        ("FORBIDDEN_ENTRY_KIND",),
        permitted=("UNKNOWN_ENTRY_KIND", "MALFORMED_INDEX"),
    )


def g19_importer(spec: PackSpec) -> MalformedPack:
    return case(
        "G19",
        "unconfigured_importer",
        "a valid pack from a principal that is not the importer",
        assemble(spec),
        ("IMPORTER_MISMATCH",),
        importer="unconfigured",
    )


def g20_empty(spec: PackSpec) -> MalformedPack:
    return case(
        "G20",
        "empty_pack",
        "a pack with no entries after a full import",
        assemble(spec.with_entries(())),
        ("PACK_MASS_WITHDRAWAL",),
        prior=assemble(spec),
    )


def g20_sixty_percent(spec: PackSpec) -> MalformedPack:
    kept = spec.entries[: len(spec.entries) * 2 // 5]
    return case(
        "G20",
        "withdraw_sixty_percent",
        "a re-import that withdraws 60% of members",
        assemble(spec.with_entries(kept)),
        ("PACK_MASS_WITHDRAWAL",),
        prior=assemble(spec),
    )


def g21_budget(spec: PackSpec) -> MalformedPack:
    tools = [replace(t, input_schema=None) for t in _tools(spec, 300)]
    return case(
        "G21",
        "three_hundred_violations",
        "300 tools without input schemas",
        assemble(spec.with_entries(tools)),
        ("MISSING_TOOL_SCHEMA",),
        max_reported_violations=256,
    )


INDEX_MUTATIONS: tuple[Mutation, ...] = (
    g1_unsorted,
    g1_scheme,
    g1_two_servers,
    g1_unknown_kind,
    g1_schema_version,
    g1_connector,
    g1_long_name,
    g2_entries,
    g2_body,
    g2_index,
    g2_references,
    g2_iri_list,
    g2_archive,
    g2_schema,
    g3_length,
    g3_sha,
    g3_missing,
    g4_overlap,
    g4_gap,
    g4_overflow,
    g5_flipped,
    g5_pack_digest,
    g6_duplicate,
    g6_decide_kind,
    g19_importer,
    g20_empty,
    g20_sixty_percent,
    g21_budget,
)
