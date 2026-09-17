"""Size-realistic connector packs, measured against the fleet (design §1.1).

Profiles:
* ``typical``: a dozen tools with skills, a prompt, an ontology and shapes;
* ``largest_bytes``: 236,808 bytes of skill, prompt and Turtle bodies, like the
  largest measured connector (five `.ttl` files, 220 KB of them);
* ``largest_entries``: 95 entries, 87 of them tools, like the largest measured
  entry count;
* ``largest_bodies``: the largest single `SKILL.md` (128,732 bytes) and the
  largest `.ttl` (123,285 bytes) measured.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Literal

from .._rng import SeededStream
from . import content
from .model import BuiltPack
from .spec import EntrySpec, PackSpec, assemble

Profile = Literal["typical", "largest_bytes", "largest_entries", "largest_bodies"]
LARGEST_TOTAL_BODY_BYTES = 236_808
LARGEST_ENTRY_COUNT = 95
LARGEST_SKILL_BYTES = 128_732
LARGEST_TURTLE_BYTES = 123_285
BODY_KINDS = ("skill", "prompt", "ontology", "shapes")


def _tools(stream: SeededStream, connector: str, count: int) -> list[EntrySpec]:
    return [content.tool(stream.child(f"tool-{i}"), connector, i) for i in range(count)]


def _typical(stream: SeededStream, connector: str) -> list[EntrySpec]:
    tools = _tools(stream, connector, 12)
    return [
        *tools,
        content.skill(connector, 0, tools[:2]),
        content.skill(connector, 1, tools[2:4]),
        content.prompt(connector, 0),
        content.ontology(connector, "core", 8),
        content.shapes(connector, "core-shapes", "core", 4),
        content.model_profile(connector, 0),
        content.manifest(connector),
    ]


def _largest_bytes(stream: SeededStream, connector: str) -> list[EntrySpec]:
    tools = _tools(stream, connector, 6)
    small = [content.skill(connector, 0, tools[:2]), content.prompt(connector, 0)]
    turtle_budget = LARGEST_TOTAL_BODY_BYTES - sum(len(e.body) for e in small)
    files = ["core", "orders", "markets", "risk"]
    per_file = turtle_budget // 5
    turtle = [content.ontology(connector, f, 40, per_file) for f in files]
    last = turtle_budget - per_file * len(files)
    turtle.append(content.shapes(connector, "shapes", "core", 40, last))
    return [*tools, *small, *turtle, content.manifest(connector)]


def _largest_entries(stream: SeededStream, connector: str) -> list[EntrySpec]:
    tools = _tools(stream, connector, 87)
    skills = [content.skill(connector, i, tools[i : i + 2]) for i in range(4)]
    prompts = [content.prompt(connector, i) for i in range(2)]
    turtle = [
        content.ontology(connector, "core", 8),
        content.shapes(connector, "core-shapes", "core", 4),
    ]
    return [*tools, *skills, *prompts, *turtle]


def _largest_bodies(stream: SeededStream, connector: str) -> list[EntrySpec]:
    tools = _tools(stream, connector, 3)
    return [
        *tools,
        content.skill(connector, 0, tools, size=LARGEST_SKILL_BYTES),
        content.ontology(connector, "core", 16),
        content.shapes(connector, "core-shapes", "core", 16, LARGEST_TURTLE_BYTES),
    ]


_PROFILES: dict[str, Callable[[SeededStream, str], list[EntrySpec]]] = {
    "typical": _typical,
    "largest_bytes": _largest_bytes,
    "largest_entries": _largest_entries,
    "largest_bodies": _largest_bodies,
}


def pack_spec(
    seed: int, profile: Profile = "typical", connector: str = "synthetic-mcp"
) -> PackSpec:
    stream = SeededStream(seed, f"pack/{profile}")
    entries = _PROFILES[profile](stream, connector)
    return PackSpec(
        connector=connector, server=content.server(connector), entries=tuple(entries)
    )


def generate_pack(
    seed: int, profile: Profile = "typical", connector: str = "synthetic-mcp"
) -> BuiltPack:
    return assemble(pack_spec(seed, profile, connector))
