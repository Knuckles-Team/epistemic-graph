"""Entry builders: one well-formed entry of each pack kind."""

from __future__ import annotations

from collections.abc import Sequence

from .. import vocabulary as vocab
from .._digest import canonical_json
from .._rng import SeededStream
from ..catalog import FOREIGN_NAMESPACE
from .model import Annotations, Cost, Latency, ModelAnnotation, PackRef
from .spec import EntrySpec

JSON = "application/json"
TURTLE = "text/turtle"
MARKDOWN = "text/markdown"
ONTOLOGY_BASE = "https://synthetic.example/ontology"
_PREFIXES = (
    "@prefix owl: <http://www.w3.org/2002/07/owl#> .\n"
    "@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n"
    "@prefix sh: <http://www.w3.org/ns/shacl#> .\n"
    "@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n"
)


def server(connector: str) -> EntrySpec:
    descriptor = {
        "contract_version": "1",
        "instructions": "Synthetic.",
        "name": connector,
    }
    return EntrySpec(
        kind="mcp_server",
        uri=f"mcp-server://{connector}",
        name=connector,
        media_type=JSON,
        body=canonical_json(descriptor),
    )


def input_schema(name: str) -> bytes:
    document = {
        "additionalProperties": False,
        "properties": {"query": {"type": "string"}, "limit": {"type": "integer"}},
        "required": ["query"],
        "title": name,
        "type": "object",
    }
    return canonical_json(document)


def tool(
    stream: SeededStream, connector: str, index: int, description: str | None = None
) -> EntrySpec:
    name = f"tool_{index:03d}"
    leaf = stream.choice(vocab.leaves(vocab.CAPABILITY_ROOT))
    read_only = not vocab.is_side_effecting_term(leaf)
    provides = [leaf]
    if stream.chance(1, 5):
        provides.append(FOREIGN_NAMESPACE + leaf.rsplit("/", 1)[-1])
    text = f"Synthetic tool {index}." if description is None else description
    descriptor = {"description": text, "name": name, "title": name}
    p50 = stream.between(5, 400)
    return EntrySpec(
        kind="tool",
        uri=f"tool://{connector}/{name}",
        name=name,
        media_type=JSON,
        body=canonical_json(descriptor),
        input_schema=input_schema(name),
        output_schema=input_schema(f"{name}_result") if stream.chance(1, 2) else None,
        annotations=Annotations(
            provides=tuple(sorted(provides)),
            modalities_in=("eg:modality/structured",),
            modalities_out=("eg:modality/text",),
            read_only_hint=read_only,
            destructive_hint=None if read_only else stream.chance(1, 2),
            cost=Cost(currency="USD", per_call_micros=stream.between(0, 20_000)),
            latency_declared=Latency(p50_ms=p50, p95_ms=p50 + stream.between(0, 900)),
        ),
    )


def skill(
    connector: str, index: int, tools: Sequence[EntrySpec], size: int = 0
) -> EntrySpec:
    name = f"{connector}-skill-{index:02d}"
    front = f"---\nname: {name}\ndescription: Synthetic skill {index}.\n---\n"
    text = front + "# Synthetic skill\n"
    text += "x" * max(0, size - len(text.encode()) - 1) + "\n" if size else ""
    return EntrySpec(
        kind="skill",
        uri=f"skill://{name}/SKILL.md",
        name=name,
        media_type=MARKDOWN,
        body=text.encode(),
        annotations=Annotations(provides=("eg:capability/reasoning/plan",)),
        references=tuple(PackRef(uri=t.uri, kind="tool") for t in tools),
    )


def prompt(connector: str, index: int) -> EntrySpec:
    name = f"prompt_{index:02d}"
    rendered = {"messages": [{"content": f"Synthetic prompt {index}.", "role": "user"}]}
    return EntrySpec(
        kind="prompt",
        uri=f"prompt://{connector}/{name}",
        name=name,
        media_type=JSON,
        body=canonical_json(rendered),
    )


def namespace(connector: str, file: str) -> str:
    return f"{ONTOLOGY_BASE}/{connector}/{file}#"


def _classes(connector: str, file: str, count: int) -> str:
    ns = namespace(connector, file)
    lines = [_PREFIXES, f"<{ns[:-1]}> a owl:Ontology .\n"]
    lines.append(f'<{ns}C0000> a owl:Class ; rdfs:label "C0" .\n')
    lines += [
        f"<{ns}C{i:04d}> a owl:Class ; rdfs:subClassOf <{ns}C0000> ; "
        f'rdfs:label "C{i}" .\n'
        for i in range(1, count)
    ]
    return "".join(lines)


def padded(text: str, size: int) -> bytes:
    """Pad Turtle with one comment line so the body is exactly ``size`` bytes."""
    data = text.encode()
    if not size:
        return data
    gap = size - len(data)
    if gap < 3:
        raise ValueError("turtle content already exceeds its target size")
    return data + b"#" + b"x" * (gap - 2) + b"\n"


def ontology(connector: str, file: str, classes: int, size: int = 0) -> EntrySpec:
    return EntrySpec(
        kind="ontology",
        uri=f"ontology://{connector}/{file}.ttl",
        name=f"{file}.ttl",
        media_type=TURTLE,
        body=padded(_classes(connector, file, classes), size),
    )


def shapes(
    connector: str, file: str, target_file: str, count: int, size: int = 0
) -> EntrySpec:
    ns = namespace(connector, target_file)
    lines = [_PREFIXES]
    lines += [
        f"<{ns}S{i:04d}> a sh:NodeShape ; sh:targetClass <{ns}C{i:04d}> ; "
        "sh:property [ sh:path rdfs:label ; sh:minCount 1 ; "
        "sh:datatype xsd:string ] .\n"
        for i in range(count)
    ]
    return EntrySpec(
        kind="shapes",
        uri=f"shapes://{connector}/{file}.ttl",
        name=f"{file}.ttl",
        media_type=TURTLE,
        body=padded("".join(lines), size),
    )


def model_profile(connector: str, index: int, window: int = 128_000) -> EntrySpec:
    name = f"model_{index:02d}"
    facts = ModelAnnotation(
        provider="synthetic-provider",
        model_identity=f"synthetic-model-{index}",
        context_window_tokens=window,
        max_output_tokens=window // 4,
        supports_tools=True,
        supports_structured_output=True,
        supports_vision=False,
    )
    return EntrySpec(
        kind="model_profile",
        uri=f"model-profile://{connector}/{name}",
        name=name,
        media_type=JSON,
        body=canonical_json(facts.model_dump(mode="json")),
        annotations=Annotations(
            provides=("eg:capability/generation/text",),
            model=facts,
            cost=Cost(
                currency="USD",
                input_per_mtok_micros=3_000_000,
                output_per_mtok_micros=15_000_000,
            ),
            latency_declared=Latency(p50_ms=400, p95_ms=1_800),
        ),
    )


def manifest(connector: str) -> EntrySpec:
    return EntrySpec(
        kind="manifest",
        uri="manifest://connector",
        name="manifest",
        media_type=JSON,
        body=canonical_json({"connector": connector, "synthetic": True}),
    )
