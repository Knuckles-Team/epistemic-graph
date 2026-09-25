"""EG compiles typed interface declarations into OWL and SHACL."""

from __future__ import annotations

import pytest

from epistemic_graph.interface_pack import (
    compile_interface_implementers,
    compile_interface_shape,
)


def test_compiles_class_shape_and_implementer() -> None:
    ttl = compile_interface_shape(
        {
            "local": "Locatable",
            "name": "Locatable",
            "description": "Has a location",
            "parents": ["Identifiable"],
            "properties": [
                {
                    "path": "lat",
                    "datatype": "http://www.w3.org/2001/XMLSchema#double",
                }
            ],
            "links": [{"path": "located_in", "min_count": 1}],
        }
    )
    assert "rdfs:subClassOf :Identifiable" in ttl
    assert "sh:path :lat" in ttl and "sh:minCount 1" in ttl
    assert "sh:path :located_in" in ttl
    linked = compile_interface_implementers(
        [{"object_type": "Place", "interface": "Locatable"}]
    )
    assert ":Place rdfs:subClassOf :Locatable" in linked
    assert "sh:node :LocatableShape" in linked


def test_rejects_rdf_injection_in_paths() -> None:
    with pytest.raises(ValueError):
        compile_interface_shape(
            {
                "local": "Locatable",
                "name": "Locatable",
                "properties": [
                    {
                        "path": "lat ; sh:deactivated true",
                        "datatype": "http://www.w3.org/2001/XMLSchema#double",
                    }
                ],
            }
        )
