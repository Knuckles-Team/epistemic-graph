#!/usr/bin/env python3
"""Fail closed when a pinned cross-record reference has no production resolver.

Why this exists
---------------
The four agent layers -- component (L1), library entry (L2), graph (L3) and
template -- refer to each other by PIN: an id plus the exact
``definition_digest``/``shape_digest`` of the revision being referred to.
RF-ADR-008's justification for resolving one such pin is that *resolution is by
(id, digest) alone, so without it a leaked digest is an execution grant*.

That argument applies to every pin, and most of them were never resolved.
Validation of a published record was local and structural -- well-formed text,
a well-formed ``sha256:<hex>``, the right kind for the slot, no duplicates --
and never asked whether the referenced thing EXISTS.  So an entry could pin
``{component_id: "tool:other-tenant-admin", kind: Tool, digest: <any>}``, and
it published, delegated, and had that pin covered by its own
``capability_digest``.  The ``agent_component`` store had no production reader
at all: a whole layer was write-only.

Nothing detects this by shape.  The pin field is declared, the validator runs,
every unit test passes.  What is missing is a READ of the other store, and the
absence of a read is invisible to every scanner that measures the code that IS
there.

The property
------------
Discover the pins from the CONTRACT, not from a list: a pin is a field whose
type carries ``ComponentDependency``, or an ``<x>_id`` / ``*_digest`` pair
declared in one struct or enum variant.  A pin added tomorrow is enumerated
tomorrow, and lands as unresolved debt until someone resolves it.

For each pin, require a production resolver: a function that OPENS the target
layer's durable head/revision tables, lexically reachable from the source
layer's admitted write.  Both halves matter -- a read of the target store that
the admission path never calls resolves nothing, and an admission path that
calls nothing which reads the target store admits unresolvable pins.

Test code is excluded from both halves.  Every one of these edges has
``#[cfg(test)]`` uses of the pin type; that is precisely how "L1 has a reader"
looked true while production had none.

An unresolved edge is REPORTED, not allowlisted.  The four still-open edges are
the point of the gate.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from rust_lexer import _balanced_span_from, _rust_code_mask, _rust_comments_mask
from rust_module_tree import read_module_tree

ROOT = Path(__file__).resolve().parents[1]
SCHEMA = "eg-pinned-reference-resolution-gate/v1"

SERVER_ROOT = "src/server/mod.rs"
STORAGE_REGISTRY = "crates/eg-storage/src/owner/registry.rs"
ADMITTED_WRITE = "open_write"

# The contract modules that declare the pinned hierarchy, and the layer each
# one is the SOURCE of.  A module here with no pin at all fails the gate
# closed: it means the discovery stopped seeing the declarations.
CONTRACT_LAYERS = {
    "crates/eg-types/src/agent_component.rs": "COMPONENT",
    "crates/eg-types/src/agent_library.rs": "LIBRARY",
    "crates/eg-types/src/agent_graph.rs": "GRAPH",
    "crates/eg-types/src/agent_template.rs": "TEMPLATE",
    "crates/eg-types/src/delegation.rs": None,
}

# What the id half of a pin names.  Derived from the FIELD, so a new pin to an
# existing layer needs no change here; a pin to an unknown layer is reported
# rather than silently dropped.
ID_TARGETS = {
    "agent_id": "LIBRARY",
    "graph_id": "GRAPH",
    "template_id": "TEMPLATE",
    "component_id": "COMPONENT",
    "server_component_id": "COMPONENT",
}

_TABLE_CONSTANT = re.compile(r"\bAGENT_(?P<layer>[A-Z]+)_(?:HEADS|REVISIONS)\b")
# The type half runs to the comma that ENDS the declaration, not to the first
# comma in it.  `[^,\n]+` truncated every multi-parameter generic at its first
# argument, so `bindings: BTreeMap<String, ComponentDependency>` read as
# `BTreeMap<String` and its `ComponentDependency` pin was never discovered --
# which is how the whole TEMPLATE -> COMPONENT edge stayed invisible while the
# gate reported every edge resolved.  Anchored at end of line (a trailing line
# comment allowed) so the greedy match backtracks to the LAST comma rather than
# the first; verified a strict superset of what the old pattern found, 30 pin
# sites -> 34.
_FIELD = re.compile(
    r"(?m)^[ \t]*(?:pub\s+)?(?P<field>[a-z_][a-z0-9_]*)\s*:\s*"
    r"(?P<type>[^\n]+),[ \t]*(?://.*)?$"
)
_CONTAINER = re.compile(
    r"\b(?P<keyword>struct|enum)\s+(?P<name>[A-Z][A-Za-z0-9_]*)\s*(?:<[^>{]*>)?\s*\{"
)
_SCALARS = frozenset(
    {
        "String", "bool", "char",
        "u8", "u16", "u32", "u64", "u128", "usize",
        "i8", "i16", "i32", "i64", "i128", "isize",
        "f32", "f64",
    }
)
_WRAPPERS = re.compile(r"\b(?:Option|Vec|Box|BTreeSet|HashSet|BTreeMap|HashMap|Arc)\b")
_VARIANT = re.compile(r"(?m)^\s{4}(?P<name>[A-Z][A-Za-z0-9_]*)\s*\{")
_FN_HEADER = re.compile(
    r"(?m)^[ \t]*(?:pub(?:\s*\([^)]*\))?\s+)?(?:const\s+)?(?:async\s+)?"
    r"(?:unsafe\s+)?fn\s+(?P<name>[A-Za-z_][A-Za-z0-9_]*)"
)
_CALL = re.compile(r"\b([A-Za-z_][A-Za-z0-9_]*)\s*\(")


class GateError(RuntimeError):
    """The gate could not establish its universe and must not report green."""


def read(relative: str) -> str:
    path = ROOT / relative
    if not path.is_file():
        raise GateError(f"required source is absent: {relative}")
    return path.read_text(encoding="utf-8")


def durable_layers() -> set[str]:
    layers = {
        match.group("layer")
        for match in _TABLE_CONSTANT.finditer(_rust_code_mask(read(STORAGE_REGISTRY)))
    }
    if len(layers) < 2:
        raise GateError("fewer than two agent layers are durably declared")
    return layers


def _balanced(mask: str, opener: int) -> int | None:
    try:
        return _balanced_span_from(mask, opener, "{", "}")
    except SystemExit:
        return None


def _bodies(mask: str, text: str) -> list[tuple[str, str, str]]:
    """(declaring name, keyword, body text) for structs, enums and variants."""

    bodies: list[tuple[str, str, str]] = []
    for container in _CONTAINER.finditer(mask):
        opener = container.end() - 1
        closer = _balanced(mask, opener)
        if closer is None:
            continue
        name = container.group("name")
        keyword = container.group("keyword")
        bodies.append((name, keyword, text[opener + 1 : closer]))
        for variant in _VARIANT.finditer(mask[opener + 1 : closer]):
            inner = opener + 1 + variant.end() - 1
            inner_closer = _balanced(mask, inner)
            if inner_closer is not None:
                bodies.append(
                    (
                        f"{name}::{variant.group('name')}",
                        "variant",
                        text[inner + 1 : inner_closer],
                    )
                )
    return bodies


def _layer_of(field: str, layers: set[str]) -> str | None:
    """The durable layer a `<..>_id` field names, or None if it names none.

    Derived from the durable layers themselves (`AGENT_<LAYER>_HEADS`), so a
    fifth layer needs no edit here.  `agent_id` is the one name that does not
    match its layer's constant: the LIBRARY layer's records are agents.
    """

    if field == "agent_id" or field.endswith("_agent_id"):
        return "LIBRARY" if "LIBRARY" in layers else None
    for layer in layers:
        if field == f"{layer.lower()}_id" or field.endswith(f"_{layer.lower()}_id"):
            return layer
    return None


def _carried_types(declared: str) -> set[str]:
    """The type names a field declaration carries, containers stripped off."""

    return {
        token
        for token in re.sub(r"[^A-Za-z0-9_]", " ", _WRAPPERS.sub(" ", declared)).split()
        if token[:1].isupper()
    }


def _reference_candidates(
    containers: dict[str, tuple[str, dict[str, str]]], layers: set[str]
) -> dict[str, str]:
    return {
        name: target
        for name, (keyword, fields) in containers.items()
        if keyword == "struct"
        and (target := _is_reference_shape(fields, layers)) is not None
    }


def _carries_only(
    fields: dict[str, str], permitted: set[str]
) -> bool:
    carried = {
        token for declared in fields.values() for token in _carried_types(declared)
    }
    return carried <= permitted


def _reference_types(
    containers: dict[str, tuple[str, dict[str, str]]], layers: set[str]
) -> dict[str, str]:
    """Contract types that are a POINTER to a record rather than a record.

    A reference carries an identity and a digest and nothing else that is
    itself a definition: every field is a scalar, a data-free enum, or another
    reference.  A draft or an entry fails that at once -- it embeds the thing
    it describes (`base: AgentLibraryEntryDraft`, `params: Vec<TemplateParam>`,
    `shape: AgentGraphShape`) -- which is what separates the two without keying
    on a name suffix that a rename would break.

    Computed as a fixpoint, because a reference may carry references
    (`TemplateInstanceRef.bindings` is a map of `ComponentDependency`).
    """

    data_free = {
        name
        for name, (keyword, fields) in containers.items()
        if keyword == "enum" and not fields
    }
    candidates = _reference_candidates(containers, layers)
    while True:
        permitted = _SCALARS | data_free | set(candidates)
        surviving = {
            name: target
            for name, target in candidates.items()
            if _carries_only(containers[name][1], permitted)
        }
        if surviving == candidates:
            return candidates
        candidates = surviving


def _is_reference_shape(fields: dict[str, str], layers: set[str]) -> str | None:
    """The layer a PIN shape refers to: one `<layer>_id` plus a digest."""

    if not any(field.endswith("digest") for field in fields):
        return None
    targets = {
        layer
        for field, declared in fields.items()
        if declared.strip() == "String" and (layer := _layer_of(field, layers))
    }
    return targets.pop() if len(targets) == 1 else None


def _contract_modules() -> dict[str, tuple[str, str, str | None]]:
    return {
        relative: (
            _rust_code_mask(source := read_module_tree(relative, root_dir=ROOT)),
            _rust_comments_mask(source),
            source_layer,
        )
        for relative, source_layer in CONTRACT_LAYERS.items()
    }


def _declarations(
    modules: dict[str, tuple[str, str, str | None]]
) -> dict[str, list[tuple[str, dict[str, str]]]]:
    return {
        relative: [
            (
                name,
                {
                    field.group("field"): field.group("type").strip()
                    for field in _FIELD.finditer(body)
                },
            )
            for name, _keyword, body in _bodies(mask, text)
        ]
        for relative, (mask, text, _layer) in modules.items()
    }


def _containers(
    modules: dict, declarations: dict
) -> dict[str, tuple[str, dict[str, str]]]:
    return {
        name: (keyword, fields)
        for relative, (mask, text, _layer) in modules.items()
        for (name, keyword, _body), (_n, fields) in zip(
            _bodies(mask, text), declarations[relative]
        )
        if "::" not in name
    }


def _named_types(declarations: dict) -> set[str]:
    return {
        token
        for entries in declarations.values()
        for _name, fields in entries
        for declared in fields.values()
        for token in re.sub(r"[^A-Za-z0-9_]", " ", declared).split()
    }


def _discover_reference_types(
    modules: dict, declarations: dict, layers: set[str]
) -> dict[str, str]:
    named = _named_types(declarations)
    references = {
        name: target
        for name, target in _reference_types(
            _containers(modules, declarations), layers
        ).items()
        if name in named
    }
    if not references:
        raise GateError(
            "no reference type discovered in the agent contract: the discovery "
            "stopped seeing the pinned hierarchy"
        )
    return references


def _field_target(
    field: str,
    declared: str,
    fields: dict[str, str],
    variant_target: str | None,
    references: dict[str, str],
    layers: set[str],
) -> tuple[str, bool] | None:
    """(target layer, pinned) for one field, or None when it is not a reference."""

    for reference, layer in references.items():
        if re.search(rf"\b{re.escape(reference)}\b", declared):
            return layer, True
    if declared.strip() != "String":
        return None
    layer = _layer_of(field, layers)
    if layer is None:
        return None
    if variant_target is not None:
        # An enum variant that inlines the pin shape: this id IS the pin.
        return variant_target, True
    # A PREFIXED layer id in a record that does not pin it -- a cross-record
    # reference carrying no digest at all.  A bare `component_id` on the
    # component's own entry is that record's name, not a reference.
    if "_" in field.removesuffix("_id"):
        return layer, False
    return None


def pin_sites(layers: set[str]) -> list[dict]:
    """Every pinned cross-record reference the contract declares.

    Three shapes, all read off the contract rather than listed here:

    * a field whose TYPE is a reference type -- a struct that is itself one
      `<layer>_id` plus a digest, and that some container uses as a field
      (`ComponentDependency`, `AgentLibraryEntryRef`, `AgentGraphEntryRef`);
    * an enum VARIANT that inlines the same shape, which is how
      `AgentGraphNodeKind::{Agent, Template, Graph}` pin their children;
    * a PREFIXED layer id with no digest beside it -- `server_component_id` --
      which is a cross-record reference carrying no pin at all.
    """

    modules = _contract_modules()
    declarations = _declarations(modules)
    references = _discover_reference_types(modules, declarations, layers)

    sites: list[dict] = []
    for relative, entries in declarations.items():
        source_layer = modules[relative][2] or "DELEGATION"
        for name, fields in entries:
            variant_target = (
                _is_reference_shape(fields, layers) if "::" in name else None
            )
            for field, declared in fields.items():
                found = _field_target(
                    field, declared, fields, variant_target, references, layers
                )
                if found is None:
                    continue
                sites.append(
                    {
                        "module": relative,
                        "source_layer": source_layer,
                        "declared_by": name,
                        "field": field,
                        "type": declared,
                        "target_layer": found[0],
                        "pinned": found[1],
                    }
                )
    _require_discovery_intact(sites)
    return sites


def _require_discovery_intact(sites: list[dict]) -> None:
    """Fail closed on a discovery that has stopped seeing the contract.

    Not "one pin per module" -- a module may legitimately declare a reference
    type that only a SIBLING module points with, which is exactly what
    `TemplateInstanceRef` does -- but the two shapes the hierarchy is built out
    of must both still be found.
    """

    if not any("ComponentDependency" in site["type"] for site in sites):
        raise GateError(
            "no ComponentDependency pin discovered: the discovery stopped "
            "seeing the contract's component references"
        )
    if not any("::" in site["declared_by"] for site in sites):
        raise GateError(
            "no inline variant pin discovered: the discovery stopped seeing "
            "the graph node kinds that pin their children"
        )


def _functions(source: str) -> dict[str, str]:
    mask = _rust_code_mask(source)
    bodies: dict[str, str] = {}
    for header in _FN_HEADER.finditer(mask):
        opener = mask.find("{", header.end())
        if opener < 0:
            continue
        closer = _balanced(mask, opener)
        if closer is None:
            continue
        name = header.group("name")
        # A name may be defined more than once under mutually exclusive cfgs;
        # keep every definition so a call in either resolves.
        bodies[name] = bodies.get(name, "") + source[header.start() : closer + 1]
    if not bodies:
        raise GateError("no production function parsed out of the server tree")
    return bodies


# Two bounds, fixed here rather than configurable, because a gate with a
# tunable reach is a gate someone can tune until it is green.
#
# `_RESOLVER_CALL_DEPTH` is how many calls the search follows from an anchor
# before giving up.  The real resolution chains are three long
# (`retained_graph` -> `graph_revisions` -> `read_graph_history`), and the
# answer is identical at 1, 2 and 3.  At 5 it starts reporting GRAPH -> COMPONENT
# and GRAPH -> LIBRARY as resolved, which they are not: a lexical, name-keyed
# call graph over a whole server tree eventually connects everything to
# everything, so the bound is what keeps the reach honest.
#
# `_ANCHOR_CALLER_LEVELS` applies only to a source that owns no record of its
# own (delegation).  There the anchor is the code that dispatches on the pin,
# and the resolution happens in a caller: `validate_retained_target` matches
# `DelegationTarget::Graph`, but it is handed an already-resolved target by the
# admission function two levels above.  The answer is identical at 2 and 3.
_RESOLVER_CALL_DEPTH = 3
_ANCHOR_CALLER_LEVELS = 2


def _called_directly(
    bodies: dict[str, str], start: set[str], targets: set[str]
) -> set[str]:
    """Members of `targets` reachable within a BOUNDED number of calls.

    Deliberately bounded, not a transitive closure.  A lexical, name-keyed call
    graph over a whole server tree connects everything to everything: one admin
    handler that serves all four agent families makes every layer look like it
    resolves every other.  The question this gate asks is narrower and exact --
    does the source layer's own admitted write CALL a read of the target
    store -- and that is a direct call by construction, because the resolution
    has to happen inside the same write transaction that is admitting the
    record which carries the pin.
    """

    frontier = set(start)
    seen = set(start)
    hit: set[str] = set()
    for _ in range(_RESOLVER_CALL_DEPTH):
        step: set[str] = set()
        for caller in frontier:
            for name in set(_CALL.findall(bodies.get(caller, ""))):
                if name in targets:
                    hit.add(name)
                if name in bodies and name not in seen:
                    seen.add(name)
                    step.add(name)
        frontier = step
    return hit


def _store_readers(bodies: dict[str, str], layers: set[str]) -> dict[str, set[str]]:
    """Functions that open each layer's durable head/revision tables."""

    return {
        layer: {
            name
            for name, body in bodies.items()
            if re.search(rf"\bAGENT_{layer}_(?:HEADS|REVISIONS)\b", body)
        }
        for layer in layers
    }


def _admitted_writes(bodies: dict[str, str], layers: set[str]) -> dict[str, set[str]]:
    """Each layer's admitted write: where a record carrying pins is committed."""

    admission = {
        layer: {
            name
            for name, body in bodies.items()
            if f"Agent{layer.capitalize()}Entry" in body and ADMITTED_WRITE in body
        }
        for layer in layers
    }
    for layer, functions in admission.items():
        if not functions:
            raise GateError(
                f"no admitted write was found for the {layer} layer: the gate "
                "cannot tell a resolved edge from an unresolved one"
            )
    return admission


def _edges_from_sites(sites: list[dict]) -> dict[tuple[str, str], dict]:
    edges: dict[tuple[str, str], dict] = {}
    for site in sites:
        edge = edges.setdefault(
            (site["source_layer"], site["target_layer"]),
            {
                "source_layer": site["source_layer"],
                "target_layer": site["target_layer"],
                "pins": [],
                "unpinned_pins": [],
            },
        )
        label = (
            f"{site['module'].rsplit('/', 1)[-1]}::"
            f"{site['declared_by']}.{site['field']}"
        )
        edge["pins"].append(label)
        if not site["pinned"]:
            edge["unpinned_pins"].append(label)
    return edges


def _edge_anchors(
    edge: dict, bodies: dict[str, str], admission: dict[str, set[str]]
) -> set[str]:
    """Where the search for this edge's resolver starts."""

    if edge["source_layer"] in admission:
        # A record layer's admission path is exact: the function that opens the
        # admitted write for that layer's entry type.
        return admission[edge["source_layer"]]
    # A layer with no record of its own -- delegation -- is anchored on the
    # code that DISPATCHES on the pin, plus that code's callers. The caller is
    # on the same admission path by construction, and it is where the
    # resolution actually happens: the function that matches
    # `DelegationTarget::Graph` is handed an already-resolved target.
    anchors = {
        name
        for name, body in bodies.items()
        if any(pin.split("::", 1)[-1].split(".")[0] in body for pin in edge["pins"])
    }
    for _ in range(_ANCHOR_CALLER_LEVELS):
        anchors |= {
            name for name, body in bodies.items() if set(_CALL.findall(body)) & anchors
        }
    return anchors


def run_gate() -> dict:
    layers = durable_layers()
    sites = pin_sites(layers)
    bodies = _functions(read_module_tree(SERVER_ROOT, root_dir=ROOT))
    opens = _store_readers(bodies, layers)
    admission = _admitted_writes(bodies, layers)

    edges = _edges_from_sites(sites)
    for edge in edges.values():
        anchors = _edge_anchors(edge, bodies, admission)
        edge["resolvers"] = sorted(
            _called_directly(bodies, anchors, opens.get(edge["target_layer"], set()))
        )
        edge["resolved"] = bool(edge["resolvers"])
        edge["pins"] = sorted(set(edge["pins"]))
        edge["unpinned_pins"] = sorted(set(edge["unpinned_pins"]))

    ordered = sorted(
        edges.values(), key=lambda edge: (edge["source_layer"], edge["target_layer"])
    )
    return {
        "schema": SCHEMA,
        "durable_layers": sorted(layers),
        "pin_sites": len(sites),
        "edges": ordered,
        "unresolved_edges": [edge for edge in ordered if not edge["resolved"]],
        "unknown_target_edges": [
            edge for edge in ordered if edge["target_layer"] == "UNKNOWN"
        ],
    }


def _report_failures(receipt: dict, unresolved: list, unpinned: list) -> None:
    for edge in unresolved:
        print(
            f"  {edge['source_layer']} -> {edge['target_layer']}: no production "
            "read of the target store is reachable from the source layer's "
            f"admitted write; pins: {', '.join(edge['pins'])}",
            file=sys.stderr,
        )
    for edge in unpinned:
        for pin in edge["unpinned_pins"]:
            print(
                f"  {pin}: an UNPINNED cross-record reference to the "
                f"{edge['target_layer']} layer — it names a record by id with "
                "no digest beside it, so no resolver can check that the thing "
                "it names is the thing that was reviewed",
                file=sys.stderr,
            )
    for edge in receipt["unknown_target_edges"]:
        print(
            f"  {edge['source_layer']} -> <unknown>: a pin whose target layer "
            f"this gate cannot classify; pins: {', '.join(edge['pins'])}",
            file=sys.stderr,
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true", help="emit the full receipt")
    args = parser.parse_args(argv)
    try:
        receipt = run_gate()
    except GateError as exc:
        print(f"pinned-reference-resolution gate: CANNOT RUN: {exc}", file=sys.stderr)
        return 2
    if args.json:
        print(json.dumps(receipt, indent=2, sort_keys=True))

    unresolved = receipt["unresolved_edges"]
    unpinned = [edge for edge in receipt["edges"] if edge["unpinned_pins"]]
    if not unresolved and not unpinned and not receipt["unknown_target_edges"]:
        print(
            "pinned-reference-resolution gate: OK: all "
            f"{len(receipt['edges'])} pinned reference edge(s) "
            f"({receipt['pin_sites']} pin site(s)) have a production resolver"
        )
        return 0
    print(
        f"pinned-reference-resolution gate: FAIL: {len(unresolved)} of "
        f"{len(receipt['edges'])} pinned reference edge(s) have no production "
        f"resolver ({len(receipt['edges']) - len(unresolved)} resolved); "
        f"{sum(len(edge['unpinned_pins']) for edge in unpinned)} unpinned "
        "cross-record reference(s)",
        file=sys.stderr,
    )
    _report_failures(receipt, unresolved, unpinned)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
