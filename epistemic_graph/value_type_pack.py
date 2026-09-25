"""EG-owned RDF, OWL and SHACL compilation for typed value declarations."""

from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal
from typing import Any

from .ontology_pack import _label, _local

VALUE_TYPE_PREFIXES = (
    "@prefix : <http://knuckles.team/kg#> .\n"
    "@prefix sh: <http://www.w3.org/ns/shacl#> .\n"
    "@prefix owl: <http://www.w3.org/2002/07/owl#> .\n"
    "@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n"
    "@prefix qudt: <http://qudt.org/schema/qudt/> .\n"
    "@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n"
)


def _base_iri(value: object) -> str:
    prefix = "http://www.w3.org/2001/XMLSchema#"
    if not isinstance(value, str) or not value.startswith(prefix):
        raise ValueError("value type base must be an XSD datatype IRI")
    _local(value[len(prefix) :])
    return value


def _literal(value: object) -> str:
    if not isinstance(value, str):
        raise ValueError("RDF literal must be a string")
    return (
        value.replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t")
    )


def _number(value: Any) -> str:
    if isinstance(value, bool) or not isinstance(value, int | float | Decimal):
        raise ValueError("numeric facet must be an integer or decimal")
    if isinstance(value, int):
        return f'"{value}"^^xsd:integer'
    decimal = Decimal(str(value))
    if not decimal.is_finite():
        raise ValueError("numeric facet must be finite")
    return f'"{decimal}"^^xsd:decimal'


def _member(value: Any) -> str:
    if isinstance(value, bool):
        return f'"{str(value).lower()}"^^xsd:boolean'
    if isinstance(value, int):
        return f'"{value}"^^xsd:integer'
    if isinstance(value, float):
        if not Decimal(str(value)).is_finite():
            raise ValueError("enumerated float must be finite")
        return f'"{value}"^^xsd:double'
    if isinstance(value, Decimal):
        return f'"{value}"^^xsd:decimal'
    return f'"{_literal(str(value))}"'


def _length(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError("length facet must be a nonnegative integer")
    return value


def _shape_constraints(decl: Mapping[str, Any], *, indent: str) -> list[str]:
    constraints = decl["constraints"]
    lines = [f"{indent}sh:datatype <{_base_iri(decl['base_iri'])}> ;"]
    pattern = constraints.get("pattern")
    if pattern is not None:
        lines.append(f'{indent}sh:pattern "{_literal(pattern)}" ;')
        if constraints.get("case_insensitive"):
            lines.append(f'{indent}sh:flags "i" ;')
    for field, inclusive, exclusive in (
        ("min_value", "minInclusive", "minExclusive"),
        ("max_value", "maxInclusive", "maxExclusive"),
    ):
        value = constraints.get(field)
        if value is not None:
            facet = (
                exclusive if constraints.get("exclusive_" + field[:3]) else inclusive
            )
            lines.append(f"{indent}sh:{facet} {_number(value)} ;")
    for field, facet in (("min_length", "minLength"), ("max_length", "maxLength")):
        value = constraints.get(field)
        if value is not None:
            lines.append(f'{indent}sh:{facet} "{_length(value)}"^^xsd:integer ;')
    members = constraints.get("allowed_values")
    if members is not None:
        lines.append(f"{indent}sh:in ( {' '.join(_member(v) for v in members)} ) ;")
    unit = constraints.get("unit")
    if unit:
        lines.append(f'{indent}qudt:unit "{_literal(unit)}" ;')
    return lines


def compile_value_type_shape(
    declaration: Mapping[str, Any],
    *,
    path: str | None = None,
    target_class: str | None = None,
) -> str:
    """Compile one typed value declaration to an EG SHACL NodeShape."""
    name = _local(declaration.get("name"))
    description = _label(declaration.get("description") or f"{name} value type.")
    if path is None:
        body = _shape_constraints(declaration, indent="    ")
        body[-1] = body[-1][:-2] + " ."
        return (
            f":{name}ValueShape a sh:NodeShape ;\n"
            f'    sh:name "{name}" ;\n'
            f'    sh:description "{description}" ;\n' + "\n".join(body) + "\n"
        )
    path = _local(path)
    target = f"    sh:targetClass :{_local(target_class)} ;\n" if target_class else ""
    body = "\n".join(_shape_constraints(declaration, indent="        "))
    return (
        f":{name}Shape a sh:NodeShape ;\n"
        f'    sh:name "{name} Shape" ;\n'
        f'    sh:description "{description}" ;\n'
        f"{target}"
        "    sh:property [\n"
        f"        sh:path :{path} ;\n"
        f"{body}\n"
        f'        sh:message "Value violates the {name} value type." ;\n'
        "    ] ;\n"
        "    sh:closed false .\n"
    )


def compile_value_type_owl(declaration: Mapping[str, Any]) -> str:
    """Compile one typed value declaration to an OWL datatype restriction."""
    name = _local(declaration.get("name"))
    base = _base_iri(declaration.get("base_iri"))
    description = _label(declaration.get("description") or f"{name} value type.")
    constraints = declaration["constraints"]
    head = (
        f":{name} a rdfs:Datatype ;\n"
        f'    rdfs:label "{name}" ;\n'
        f'    rdfs:comment "{description}" ;\n'
    )
    members = constraints.get("allowed_values")
    restrictable = any(
        constraints.get(field) is not None
        for field in ("pattern", "min_value", "max_value", "min_length", "max_length")
    )
    if members is not None and not restrictable:
        rendered = " ".join(_member(value) for value in members)
        return (
            head
            + "    owl:equivalentClass [\n        a rdfs:Datatype ;\n"
            + f"        owl:oneOf ( {rendered} )\n    ] .\n"
        )
    facets: list[str] = []
    pattern = constraints.get("pattern")
    if pattern is not None:
        facets.append(f'        [ xsd:pattern "{_literal(pattern)}" ]')
    for field, inclusive, exclusive in (
        ("min_value", "minInclusive", "minExclusive"),
        ("max_value", "maxInclusive", "maxExclusive"),
    ):
        value = constraints.get(field)
        if value is not None:
            facet = (
                exclusive if constraints.get("exclusive_" + field[:3]) else inclusive
            )
            facets.append(f"        [ xsd:{facet} {_number(value)} ]")
    for field, facet in (("min_length", "minLength"), ("max_length", "maxLength")):
        value = constraints.get(field)
        if value is not None:
            facets.append(f'        [ xsd:{facet} "{_length(value)}"^^xsd:integer ]')
    if not facets:
        return head + f"    owl:equivalentClass <{base}> .\n"
    return (
        head
        + "    owl:equivalentClass [\n"
        + "        a rdfs:Datatype ;\n"
        + f"        owl:onDatatype <{base}> ;\n"
        + "        owl:withRestrictions (\n"
        + "\n".join(facets)
        + "\n        )\n    ] .\n"
    )


__all__ = ["VALUE_TYPE_PREFIXES", "compile_value_type_shape", "compile_value_type_owl"]
