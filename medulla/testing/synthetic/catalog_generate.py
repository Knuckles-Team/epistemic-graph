"""Seeded construction of synthetic agent catalogs (see `catalog`)."""

from __future__ import annotations

from collections.abc import Sequence

from . import vocabulary as vocab
from ._digest import canonical_json
from ._rng import SeededStream
from .catalog import (
    FOREIGN_NAMESPACE,
    GENERATOR,
    INJECTION_SUMMARY,
    Catalog,
    CostFacts,
    LatencyFacts,
    ModelFacts,
    SyntheticComponent,
    ToolFacts,
    Trap,
    build_catalog,
    component_id,
)

_TEXT = "eg:modality/text"
_STRUCTURED = "eg:modality/structured"
_IMAGE = "eg:modality/image"
_CONTEXT_WINDOWS = (8_192, 32_768, 128_000, 200_000)
# Tool index -> planted trap. The indices are fixed so a catalog of the default
# size always carries every trap; smaller catalogs carry a prefix of them.
_TOOL_TRAPS: dict[int, Trap] = {
    1: "broader_only",
    3: "foreign_only",
    5: "injection",
    7: "unknown_cost",
}
_NON_COVERING: frozenset[Trap] = frozenset({"broader_only", "foreign_only"})


def _schema(stream: SeededStream, name: str) -> str:
    fields = stream.sample(("query", "path", "limit", "cursor", "target", "body"), 2)
    properties = {field: {"type": "string"} for field in sorted(fields)}
    document = {
        "additionalProperties": False,
        "properties": properties,
        "required": sorted(fields)[:1],
        "title": name,
        "type": "object",
    }
    return canonical_json(document).decode()


def _latency(stream: SeededStream) -> LatencyFacts:
    p50 = stream.between(5, 500)
    return LatencyFacts(p50_ms=p50, p95_ms=p50 + stream.between(0, 2_000))


def _call_cost(stream: SeededStream) -> CostFacts:
    return CostFacts(
        per_call_micros=stream.between(100, 50_000),
        price_source="synthetic-price-list",
        quality="estimated",
    )


def _tool_terms(
    trap: Trap, leaf: str, uncovered: Sequence[str]
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """(classification, declared foreign capabilities) for one tool."""
    if trap == "broader_only":
        # The broader term of a planted gap: it looks relevant and covers nothing
        # the gap needs, because claiming a parent is not evidence of a child.
        return (vocab.ancestors(uncovered[0])[0],), ()
    if trap == "foreign_only":
        return (), (FOREIGN_NAMESPACE + leaf.rsplit("/", 1)[-1],)
    return (leaf,), ()


def _tool(
    stream: SeededStream, index: int, leaf: str, uncovered: Sequence[str]
) -> SyntheticComponent:
    trap = _TOOL_TRAPS.get(index, "none")
    classification, declared = _tool_terms(trap, leaf, uncovered)
    writes = any(map(vocab.is_side_effecting_term, classification)) or stream.chance(
        1, 8
    )
    name = component_id("tool", index)
    output = _schema(stream.child("output"), name) if stream.chance(1, 2) else None
    return SyntheticComponent(
        component_id=name,
        kind="tool",
        summary=INJECTION_SUMMARY
        if trap == "injection"
        else f"Synthetic tool {index} for {leaf}.",
        classification=classification,
        declared_capabilities=declared,
        modalities_in=(_STRUCTURED,),
        modalities_out=tuple(sorted((_STRUCTURED, _TEXT))),
        tool=ToolFacts(
            effect="write" if writes else "read",
            input_schema=_schema(stream.child("input"), name),
            output_schema=output,
        ),
        cost=None if trap == "unknown_cost" else _call_cost(stream),
        latency=_latency(stream),
        body=f"synthetic tool body {index} {leaf}",
        trap=trap,
    )


def tool_components(
    stream: SeededStream, coverable: Sequence[str], uncovered: Sequence[str], count: int
) -> tuple[SyntheticComponent, ...]:
    """Tools whose honest members cover every coverable leaf at least once.

    Traps that cover nothing real (`broader_only`, `foreign_only`) do not take a
    slot in the round-robin, so they never leave a leaf uncovered.
    """
    order = stream.shuffled(coverable)
    tools: list[SyntheticComponent] = []
    slot = 0
    for index in range(count):
        covering = _TOOL_TRAPS.get(index) not in _NON_COVERING
        leaf = order[slot % len(order)] if covering else order[0]
        slot += int(covering)
        tools.append(_tool(stream.child(f"tool-{index}"), index, leaf, uncovered))
    if slot < len(order):
        raise ValueError(f"{count} tools cannot cover {len(order)} capability leaves")
    return tuple(tools)


def _model(stream: SeededStream, index: int) -> SyntheticComponent:
    window = stream.choice(_CONTEXT_WINDOWS)
    vision = stream.chance(1, 3)
    return SyntheticComponent(
        component_id=component_id("model", index),
        kind="model_profile",
        summary=f"Synthetic model profile {index}.",
        classification=("eg:capability/generation/text",),
        modalities_in=tuple(sorted((_TEXT, _IMAGE) if vision else (_TEXT,))),
        modalities_out=(_TEXT,),
        model=ModelFacts(
            provider="synthetic-provider",
            model_identity=f"synthetic-model-{index}",
            context_window_tokens=window,
            max_output_tokens=window // stream.choice((2, 4, 8)),
            supports_tools=index == 0 or stream.chance(1, 2),
            supports_structured_output=stream.chance(1, 2),
            supports_vision=vision,
        ),
        cost=CostFacts(
            input_per_mtok_micros=stream.between(100_000, 15_000_000),
            output_per_mtok_micros=stream.between(400_000, 60_000_000),
            price_source="synthetic-price-list",
            quality="estimated",
        ),
        latency=_latency(stream),
        body=f"synthetic model profile body {index}",
    )


def model_components(
    stream: SeededStream, count: int
) -> tuple[SyntheticComponent, ...]:
    return tuple(_model(stream.child(f"model-{i}"), i) for i in range(count))


def _skill(
    stream: SeededStream, index: int, tools: Sequence[SyntheticComponent]
) -> SyntheticComponent:
    usable = [tool for tool in tools if tool.trap == "none"]
    picked = sorted(
        stream.sample(usable, min(2, len(usable))), key=lambda t: t.component_id
    )
    terms = sorted({iri for tool in picked for iri in tool.classification})
    return SyntheticComponent(
        component_id=component_id("skill", index),
        kind="skill",
        summary=f"Synthetic skill {index} composing {len(picked)} tools.",
        classification=tuple(terms[:1]),
        required_capabilities=tuple(terms[1:2]),
        requires=tuple(tool.component_id for tool in picked),
        modalities_in=(_TEXT,),
        modalities_out=(_TEXT,),
        cost=_call_cost(stream),
        latency=_latency(stream),
        body=f"---\nname: synthetic-skill-{index}\ndescription: synthetic\n---\n",
    )


def skill_components(
    stream: SeededStream, tools: Sequence[SyntheticComponent], count: int
) -> tuple[SyntheticComponent, ...]:
    return tuple(_skill(stream.child(f"skill-{i}"), i, tools) for i in range(count))


def generate_catalog(
    seed: int, *, tools: int = 28, models: int = 4, skills: int = 6
) -> Catalog:
    """A catalog with every capability leaf covered except two planted gaps."""
    stream = SeededStream(seed, GENERATOR)
    leaves = vocab.leaves(vocab.CAPABILITY_ROOT)
    uncovered = tuple(sorted(stream.child("gaps").sample(leaves, 2)))
    coverable = tuple(leaf for leaf in leaves if leaf not in uncovered)
    tool_list = tool_components(stream.child("tools"), coverable, uncovered, tools)
    parts = (
        *tool_list,
        *model_components(stream.child("models"), models),
        *skill_components(stream.child("skills"), tool_list, skills),
    )
    return build_catalog(seed, parts, uncovered)
