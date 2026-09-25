"""Value type RDF emission is owned by EG."""

from __future__ import annotations

import pytest

from epistemic_graph.value_type_pack import (
    compile_value_type_owl,
    compile_value_type_shape,
)


def _declaration() -> dict:
    return {
        "name": "Rate",
        "description": "A rate",
        "base_iri": "http://www.w3.org/2001/XMLSchema#decimal",
        "constraints": {
            "min_value": 0,
            "max_value": 1,
            "exclusive_min": True,
            "exclusive_max": False,
        },
    }


def test_shape_and_owl_share_bounds() -> None:
    declaration = _declaration()
    shape = compile_value_type_shape(
        declaration, path="completionRate", target_class="Task"
    )
    owl = compile_value_type_owl(declaration)
    assert "sh:minExclusive" in shape and "sh:maxInclusive" in shape
    assert "xsd:minExclusive" in owl and "xsd:maxInclusive" in owl
    assert "sh:targetClass :Task" in shape


def test_untrusted_path_and_base_iri_fail_closed() -> None:
    declaration = _declaration()
    with pytest.raises(ValueError):
        compile_value_type_shape(declaration, path="broken; sh:deactivated true")
    declaration["base_iri"] = "https://untrusted.invalid/datatype"
    with pytest.raises(ValueError):
        compile_value_type_owl(declaration)
