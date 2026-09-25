"""Typed ontology pack compilation stays in EG."""

from __future__ import annotations

import pytest

from epistemic_graph.ontology_pack import (
    compile_ontology_pack,
    compile_ontology_proposal,
)


def test_compiles_sorted_declarations_and_escapes_labels() -> None:
    ttl = compile_ontology_pack(
        source="fixture",
        classes=[
            {"local": "Widget", "label": 'A "widget"', "parent": "Thing"},
            {"local": "Part", "label": "Part", "parent": None},
        ],
        object_properties=[
            {
                "local": "hasPart",
                "label": "Has part",
                "domain": "Widget",
                "range": "Part",
            }
        ],
        datatype_properties=[
            {"local": "weight", "label": "Weight", "range": "xsd:decimal"}
        ],
    )
    assert ttl.index(":Part a owl:Class") < ttl.index(":Widget a owl:Class")
    assert 'rdfs:label "A \\"widget\\""' in ttl
    assert "rdfs:subClassOf :Thing" in ttl
    assert "rdfs:domain :Widget" in ttl
    assert "rdfs:range xsd:decimal" in ttl


@pytest.mark.parametrize(
    ("source", "classes", "datatypes"),
    [
        ("../escape", (), ()),
        ("fixture", ({"local": "Bad; owl:Class", "label": "Bad"},), ()),
        ("fixture", (), ({"local": "field", "label": "Field", "range": "owl:Thing"},)),
    ],
)
def test_rejects_untrusted_rdf_syntax(source, classes, datatypes) -> None:
    with pytest.raises(ValueError):
        compile_ontology_pack(
            source=source,
            classes=classes,
            object_properties=(),
            datatype_properties=datatypes,
        )


def test_proposal_escapes_untrusted_description_and_keeps_reservation() -> None:
    ttl = compile_ontology_proposal(
        classes=[
            {"local": "Widget", "label": "Widget", "description": 'Quoted "text"'}
        ],
        object_properties=[],
    )
    assert "CONCEPT:RESERVE-PENDING" in ttl
    assert 'rdfs:comment "Quoted \\"text\\""' in ttl


def test_empty_proposal_has_no_semantic_document() -> None:
    assert compile_ontology_proposal(classes=[], object_properties=[]) == ""
