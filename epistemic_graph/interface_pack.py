"""Compile typed abstract interface declarations into EG OWL/SHACL packs."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from .ontology_pack import _label, _local


def compile_interface_shape(declaration: Mapping[str, Any]) -> str:
    """Return OWL class and SHACL shape for one typed interface declaration."""
    local = _local(declaration.get("local"))
    name = _label(declaration.get("name"))
    lines = [
        "@prefix : <http://knuckles.team/kg#> .",
        "@prefix owl: <http://www.w3.org/2002/07/owl#> .",
        "@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .",
        "@prefix sh: <http://www.w3.org/ns/shacl#> .",
        "@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .",
        "",
        f":{local} a owl:Class ;",
        f'    rdfs:label "{name}" ;',
    ]
    description = declaration.get("description")
    if description:
        lines.append(f'    rdfs:comment "{_label(description)}" ;')
    lines.append(
        '    rdfs:comment "Abstract ontology interface; no own individuals." ;'
    )
    for parent in declaration.get("parents", ()):
        lines.append(f"    rdfs:subClassOf :{_local(parent)} ;")
    lines[-1] = lines[-1][:-2] + " ."
    lines.extend(
        (
            "",
            f":{local}Shape a sh:NodeShape ;",
            f"    sh:targetClass :{local} ;",
            f'    sh:name "{name} Shape" ;',
        )
    )
    properties: list[str] = []
    for item in declaration.get("properties", ()):
        path = _local(item.get("path"))
        datatype = item.get("datatype")
        prefix = "http://www.w3.org/2001/XMLSchema#"
        if not isinstance(datatype, str) or not datatype.startswith(prefix):
            raise ValueError("interface property datatype must be XSD")
        _local(datatype[len(prefix) :])
        properties.append(
            f"    sh:property [ sh:path :{path} ;\n"
            f"            sh:datatype <{datatype}> ;\n"
            "            sh:minCount 1 ;\n"
            f'            sh:message "{name} must declare interface-property {path}." ]'
        )
    for item in declaration.get("links", ()):
        edge = _local(item.get("path"))
        min_count = item.get("min_count")
        if (
            isinstance(min_count, bool)
            or not isinstance(min_count, int)
            or min_count < 0
        ):
            raise ValueError("interface link min_count must be nonnegative")
        properties.append(
            f"    sh:property [ sh:path :{edge} ;\n"
            f"            sh:minCount {min_count} ;\n"
            f'            sh:message "{name} must expose link {edge}." ]'
        )
    if properties:
        lines.append(",\n".join(properties) + " .")
    else:
        lines[-1] = lines[-1][:-2] + " ."
    lines.append("")
    return "\n".join(lines)


def compile_interface_implementers(
    implementations: Sequence[Mapping[str, object]],
) -> str:
    """Return explicit class-to-interface subclass and shape links."""
    lines = [
        "@prefix : <http://knuckles.team/kg#> .",
        "@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .",
        "@prefix sh: <http://www.w3.org/ns/shacl#> .",
        "",
    ]
    for item in implementations:
        obj = _local(item.get("object_type"))
        iface = _local(item.get("interface"))
        lines.append(f":{obj} rdfs:subClassOf :{iface} ; sh:node :{iface}Shape .")
    return "\n".join(lines)


__all__ = ["compile_interface_shape", "compile_interface_implementers"]
