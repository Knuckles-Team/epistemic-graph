"""Annotation, Turtle, OWL, SHACL and reference defects (G12-G17)."""

from __future__ import annotations

from dataclasses import replace

from . import content
from .malformed import MalformedPack, Mutation, added, case, edited, first, swap
from .model import Cost, Latency, PackRef
from .spec import EntrySpec, PackSpec, assemble

_NS = content.ONTOLOGY_BASE + "/defects#"
_TURTLE_HEAD = (
    "@prefix owl: <http://www.w3.org/2002/07/owl#> .\n"
    "@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n"
    "@prefix sh: <http://www.w3.org/ns/shacl#> .\n"
)


def _annotated(
    spec: PackSpec,
    variant: str,
    description: str,
    codes: tuple[str, ...],
    **update: object,
) -> MalformedPack:
    tool = first(spec, "tool")
    annotations = tool.annotations.model_copy(update=update)
    return case(
        "G12",
        variant,
        description,
        edited(spec, "tool", annotations=annotations),
        codes,
    )


def g12_mistyped(spec: PackSpec) -> MalformedPack:
    return _annotated(
        spec,
        "mistyped_native_iri",
        "a typo inside the eg: namespace",
        ("UNKNOWN_CAPABILITY_IRI",),
        provides=("eg:capability/retrieval/web-searc",),
    )


def g12_modality(spec: PackSpec) -> MalformedPack:
    return _annotated(
        spec,
        "modality_in_provides",
        "a modality term where a capability belongs",
        ("UNKNOWN_CAPABILITY_IRI", "INVALID_ANNOTATION"),
        provides=("eg:modality/text",),
    )


def g12_malformed_iri(spec: PackSpec) -> MalformedPack:
    return _annotated(
        spec,
        "malformed_iri",
        "a provides value that is not an IRI",
        ("INVALID_ANNOTATION", "UNKNOWN_CAPABILITY_IRI"),
        provides=("not an iri",),
    )


def g12_latency(spec: PackSpec) -> MalformedPack:
    return _annotated(
        spec,
        "p50_over_p95",
        "declared p50 above p95",
        ("INVALID_ANNOTATION",),
        latency_declared=Latency(p50_ms=900, p95_ms=100),
    )


def g12_cost(spec: PackSpec) -> MalformedPack:
    return _annotated(
        spec,
        "cost_over_bound",
        "a per-call cost above 10^15 micros",
        ("INVALID_ANNOTATION",),
        cost=Cost(currency="USD", per_call_micros=10**15 + 1),
    )


def g12_currency(spec: PackSpec) -> MalformedPack:
    return _annotated(
        spec,
        "currency_not_iso_4217",
        "a currency that is not ISO 4217",
        ("INVALID_ANNOTATION",),
        cost=Cost(currency="usd1", per_call_micros=10),
    )


def g12_foreign(spec: PackSpec) -> MalformedPack:
    tool = first(spec, "tool")
    provides = tuple(
        sorted(
            {
                *tool.annotations.provides,
                "https://synthetic.example/capability/playback",
            }
        )
    )
    annotations = tool.annotations.model_copy(update={"provides": provides})
    pack = edited(spec, "tool", annotations=annotations)
    return case(
        "G12",
        "foreign_namespace_iri",
        "a foreign-namespace capability is kept as a claim",
        pack,
        warnings=("UNRESOLVED_CAPABILITY_IRI",),
        accepted=True,
    )


def g12_model_facts(spec: PackSpec) -> MalformedPack:
    profile = first(spec, "model_profile")
    model = profile.annotations.model
    assert model is not None
    wrong = model.model_copy(
        update={"max_output_tokens": model.context_window_tokens + 1}
    )
    annotations = profile.annotations.model_copy(update={"model": wrong})
    pack = edited(spec, "model_profile", annotations=annotations)
    return case(
        "G12",
        "max_output_over_context",
        "max_output_tokens above context_window_tokens",
        pack,
        ("INVALID_FACTS",),
    )


def _turtle(kind: str, text: str, name: str = "defect") -> EntrySpec:
    scheme = "ontology" if kind == "ontology" else "shapes"
    return EntrySpec(
        kind=kind,
        uri=f"{scheme}://synthetic-mcp/{name}.ttl",
        name=f"{name}.ttl",
        media_type=content.TURTLE,
        body=(_TURTLE_HEAD + text).encode(),
    )


def _turtle_case(
    rule: str,
    variant: str,
    description: str,
    entry: EntrySpec,
    codes: tuple[str, ...],
    **extra: object,
) -> Mutation:
    def build(spec: PackSpec) -> MalformedPack:
        return case(rule, variant, description, added(spec, entry), codes, **extra)

    build.__name__ = f"{rule.lower()}_{variant}"
    return build


_ONT = ("ONTOLOGY_INVALID",)
_SHP = ("SHAPES_INVALID",)
_LONG_IRI = "https://synthetic.example/" + "i" * (
    4 * 1024 + 1 - len("https://synthetic.example/")
)
_CHAIN = "".join(
    f"<{_NS}c{i}> rdfs:subClassOf <{_NS}c{i - 1}> .\n" for i in range(1, 30_000)
)

TURTLE_MUTATIONS: tuple[Mutation, ...] = (
    _turtle_case(
        "G13",
        "base_directive",
        "an @base directive",
        _turtle("ontology", "@base <https://synthetic.example/> .\n"),
        _ONT,
    ),
    _turtle_case(
        "G13",
        "remote_import",
        "owl:imports of a remote ontology",
        _turtle(
            "ontology",
            f"<{_NS}o> a owl:Ontology ; owl:imports <http://example.org/remote> .\n",
        ),
        _ONT,
    ),
    _turtle_case(
        "G13",
        "iri_over_4kib",
        "an IRI of 4 KiB + 1 byte",
        _turtle("ontology", f"<{_LONG_IRI}> a owl:Class .\n"),
        _ONT,
    ),
    _turtle_case(
        "G13",
        "relative_iri",
        "a relative IRI",
        _turtle("ontology", "<relative> a owl:Class .\n"),
        _ONT,
    ),
    _turtle_case(
        "G13",
        "unparseable",
        "Turtle that does not parse",
        _turtle("ontology", "<a> <b> .\n"),
        _ONT,
    ),
    _turtle_case(
        "G13",
        "shapes_base_directive",
        "an @base directive in shapes",
        _turtle("shapes", "@base <https://synthetic.example/> .\n"),
        _SHP,
    ),
    _turtle_case(
        "G14",
        "unsatisfiable_class",
        "a class under two disjoint classes",
        _turtle(
            "ontology",
            f"<{_NS}A> owl:disjointWith <{_NS}B> .\n"
            f"<{_NS}C> rdfs:subClassOf <{_NS}A> , <{_NS}B> .\n",
        ),
        ("ONTOLOGY_INCONSISTENT",),
    ),
    _turtle_case(
        "G14",
        "derivation_budget",
        "a 30,000-class subclass chain",
        _turtle("ontology", _CHAIN),
        ("VALIDATION_BUDGET_EXCEEDED",),
        budget_dependent=True,
    ),
    _turtle_case(
        "G15",
        "service_silent",
        "sh:sparql with SERVICE SILENT",
        _turtle(
            "shapes",
            f"<{_NS}S> a sh:NodeShape ; sh:targetClass <{_NS}C> ; "
            'sh:sparql [ sh:select "SELECT $this WHERE { SERVICE SILENT '
            '<http://example.org/sparql> { $this ?p ?o } }" ] .\n',
        ),
        _SHP,
    ),
    _turtle_case(
        "G15",
        "malformed_path",
        "an sh:path that is a literal",
        _turtle(
            "shapes",
            f"<{_NS}S> a sh:NodeShape ; sh:targetClass <{_NS}C> ; "
            'sh:property [ sh:path "label" ] .\n',
        ),
        _SHP,
    ),
)


def g15_scoped_blank_nodes(spec: PackSpec) -> MalformedPack:
    """Must be ACCEPTED: `_:b0` in two files names two different nodes."""
    shape = (
        f"<{_NS}S{{n}}> a sh:NodeShape ; sh:targetClass <{_NS}C{{n}}> ; "
        "sh:property _:b0 .\n_:b0 sh:path rdfs:label .\n"
    )
    files = [_turtle("shapes", shape.format(n=n), name=f"blank-{n}") for n in (1, 2)]
    return case(
        "G15",
        "scoped_blank_node_labels",
        "two shapes files reuse _:b0",
        added(spec, *files),
        accepted=True,
    )


def g15_duplicate_shape(spec: PackSpec) -> MalformedPack:
    shape = f"<{_NS}Shared> a sh:NodeShape ; sh:targetClass <{_NS}C> .\n"
    files = [_turtle("shapes", shape, name=f"dup-{n}") for n in (1, 2)]
    return case(
        "G15",
        "duplicate_shape_iri",
        "one shape IRI defined in two files",
        added(spec, *files),
        warnings=("DUPLICATE_SHAPE_IRI",),
        accepted=True,
    )


def g16_violation(spec: PackSpec) -> MalformedPack:
    ns = content.namespace(spec.connector, "core")
    ontology = first(spec, "ontology")
    body = ontology.body + f"<{ns}instance> a <{ns}C0000> .\n".encode()
    pack = assemble(swap(spec, ontology, replace(ontology, body=body)))
    return case(
        "G16",
        "instance_without_label",
        "an ontology instance the pack's shapes reject",
        pack,
        ("SHACL_VIOLATION",),
    )


def g17_missing(spec: PackSpec) -> MalformedPack:
    refs = (PackRef(uri=f"tool://{spec.connector}/missing", kind="tool"),)
    return case(
        "G17",
        "missing_reference",
        "a skill naming a tool the pack lacks",
        edited(spec, "skill", references=refs),
        ("UNRESOLVED_REFERENCE",),
    )


def g17_kind(spec: PackSpec) -> MalformedPack:
    tool = first(spec, "tool")
    refs = (PackRef(uri=tool.uri, kind="prompt"),)
    return case(
        "G17",
        "reference_kind_mismatch",
        "a reference whose kind differs from its target",
        edited(spec, "skill", references=refs),
        ("UNRESOLVED_REFERENCE",),
    )


def g17_cycle(spec: PackSpec) -> MalformedPack:
    skills = [e for e in spec.entries if e.kind == "skill"][:2]
    a, b = skills
    cyclic = [
        replace(a, references=(PackRef(uri=b.uri, kind="skill"),)),
        replace(b, references=(PackRef(uri=a.uri, kind="skill"),)),
    ]
    others = [e for e in spec.entries if e not in skills]
    return case(
        "G17",
        "reference_cycle",
        "two skills that reference each other",
        assemble(spec.with_entries([*others, *cyclic])),
        ("REFERENCE_CYCLE",),
    )


SEMANTIC_MUTATIONS: tuple[Mutation, ...] = (
    g12_mistyped,
    g12_modality,
    g12_malformed_iri,
    g12_latency,
    g12_cost,
    g12_currency,
    g12_foreign,
    g12_model_facts,
    *TURTLE_MUTATIONS,
    g15_scoped_blank_nodes,
    g15_duplicate_shape,
    g16_violation,
    g17_missing,
    g17_kind,
    g17_cycle,
)
