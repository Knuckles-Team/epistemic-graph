#!/usr/bin/python
"""Relation projection — materialise the edges a node's own properties encode.

CONCEPT:AU-KG.enrichment.relation-projection — materialise the edges a node's
own properties already encode.

The knowledge graph stores several genuine *relationships* as string/array
**properties** on isolated nodes: a ``Concept``'s dotted ``concept_id``
(``AU-KG.QUERY.VENDOR-AGNOSTIC-TRAVERSAL`` — a fully specified two-level
parentage) and its ``pillar``; a ``WorkItem``'s ``depends_on`` array. The
property is a legitimate denormalisation, but writing *only* the property
can leave a node with no navigable relationship edges.

This module is the single declarative statement of *which property encodes
which edge*, and a pure function that turns one node write into the extra
node/edge rows that make the relationship navigable. It is deliberately:

* **Pure and I/O-free.** :func:`project_relations` never touches the engine, so
  it costs no round trip. Its output rides along inside the SAME native
  ``BatchUpdate`` / ``ChangeEnvelope`` as the node write that produced it.
* **Endpoint-safe.** The engine refuses an ``upsert_edge`` whose endpoints do
  not exist *at that point in the batch* (``BatchUpdate op[N] edge endpoints
  must exist``). Every edge in :attr:`ProjectedRelations.edges` therefore has
  both endpoints either already written (the node itself) or materialised by
  this same projection in :attr:`ProjectedRelations.nodes`, which callers must
  emit first. References to nodes this projection cannot guarantee are returned
  separately in :attr:`ProjectedRelations.candidate_edges` for a caller that is
  able to verify them (one batched existence check, never one per node).
* **Not the inverse error.** Only properties that hold *another node's
  identity* project to an edge. A literal (``has_size``, ``has_version``) never
  does — that inversion is what produced the ``Entity -[has_size]-> Entity``
  edges in the live graph.

The keys of :data:`TAXONOMY_RULES` / :data:`REFERENCE_RULES` are casefolded
``node_type`` values, so both the schema label (``Concept``) and the registry
value (``concept``) resolve to the same rules.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

__all__ = [
    "ProjectedRelations",
    "ReferenceRule",
    "TaxonomyRule",
    "REFERENCE_RULES",
    "TAXONOMY_RULES",
    "project_relations",
    "projects_anything",
]

#: Governance properties a materialised ancestor inherits from the node that
#: caused it to exist. ``EpistemicGraphBackend.add_node`` sits BELOW
#: ``IntelligenceGraphEngine._upsert_node``'s ``stamp_ownership`` /
#: ``stamp_classification``, so a projected row that carried none of these
#: would be written-but-unreadable (the BUG-033/BUG-039 failure mode). The
#: ancestor is part of the same taxonomy as its child, so inheriting the
#: child's already-stamped scope is the correct classification.
_GOVERNANCE_KEYS: tuple[str, ...] = (
    "tenant_id",
    "_owner_id",
    "_shared_scope",
    "_visibility",
    "classification",
)

_MAX_ID_LEN = 200
_MAX_DEPTH = 12
#: A TAXONOMY id is a dotted identifier and nothing else. Rejecting path, URL
#: and prose punctuation here is what stops a free-text property being mistaken
#: for a reference.
_FORBIDDEN_TAXONOMY = frozenset(" \t\n\r/\\:\"'<>{}[]|,;")
#: A node REFERENCE is an opaque node id, which legitimately contains ``:``
#: (``concept:AU-KG.X``, ``wi:0``). Only whitespace and quoting are rejected.
_FORBIDDEN_REFERENCE = frozenset(" \t\n\r\"'<>{}|")


@dataclass(frozen=True)
class TaxonomyRule:
    """A property holding a **dotted taxonomy id** whose prefixes are ancestors.

    ``self_id=True`` — the value IS this node's own id, so the node links to its
    own parent (``A.B.C -> A.B -> A``). ``self_id=False`` — the value names a
    DIFFERENT node in the same taxonomy, so the node links to that node and the
    named node's own ancestry is materialised behind it.
    """

    property: str
    edge_type: str
    id_prefix: str
    node_type: str
    id_property: str = ""
    self_id: bool = True
    ancestor_edge_type: str = ""
    separator: str = "."
    #: Case-canonicalisation for ids in this taxonomy. ``ingest_concepts``
    #: upper-cases ``concept_id`` when it mints a node id but leaves ``pillar``
    #: verbatim, so without normalising here a pillar edge would point at
    #: ``concept:AU-KG.platform`` while the concept it names lives at
    #: ``concept:AU-KG.PLATFORM`` — a tree that silently fails to join up.
    upper: bool = False

    def ancestor_edge(self) -> str:
        return self.ancestor_edge_type or self.edge_type

    def canonical(self, taxonomy_id: str) -> str:
        return taxonomy_id.upper() if self.upper else taxonomy_id


@dataclass(frozen=True)
class ReferenceRule:
    """A property holding the id(s) of nodes this node points at.

    The targets are written by somebody else, so this projection cannot
    guarantee they exist; they surface as
    :attr:`ProjectedRelations.candidate_edges`.
    """

    property: str
    edge_type: str


@dataclass
class ProjectedRelations:
    """What one node write implies.

    ``nodes`` must be emitted before ``edges`` — the engine validates edge
    endpoints in batch order.
    """

    nodes: list[tuple[str, dict[str, Any]]] = field(default_factory=list)
    edges: list[tuple[str, str, str]] = field(default_factory=list)
    candidate_edges: list[tuple[str, str, str]] = field(default_factory=list)

    def __bool__(self) -> bool:
        return bool(self.nodes or self.edges or self.candidate_edges)


# ── the registry — the whole property/edge convention, in one place ────────

TAXONOMY_RULES: dict[str, tuple[TaxonomyRule, ...]] = {
    "concept": (
        # ``concept_id`` is a dotted OKF-CIS id: ``AU-KG.query.vendor-agnostic``
        # names its own parents. skos:broader — :Concept is rdfs:subClassOf
        # skos:Concept (ontology.ttl:1153) and ``broader`` is in the reasoner's
        # transitive set, so this tree is also what the closure runs over.
        TaxonomyRule(
            property="concept_id",
            edge_type="BROADER",
            id_prefix="concept:",
            node_type="Concept",
            id_property="concept_id",
            self_id=True,
            upper=True,
        ),
        # ``pillar`` names ANOTHER concept this one belongs to.
        TaxonomyRule(
            property="pillar",
            edge_type="PART_OF",
            id_prefix="concept:",
            node_type="Concept",
            id_property="concept_id",
            self_id=False,
            ancestor_edge_type="BROADER",
            upper=True,
        ),
    ),
}

REFERENCE_RULES: dict[str, tuple[ReferenceRule, ...]] = {
    # ``WorkItem.depends_on`` is the AUTHORITATIVE dependency store as an array
    # (work_item.py:640) while the edge was demoted to "graph-viz"
    # (work_item.py:602). Same edge type the existing best-effort ``_link``
    # writes, so the two converge idempotently instead of duplicating.
    "workitem": (ReferenceRule(property="depends_on", edge_type="TASK_DEPENDS_ON"),),
    "work_item": (ReferenceRule(property="depends_on", edge_type="TASK_DEPENDS_ON"),),
}


def projects_anything(node_type: Any) -> bool:
    """Cheap pre-filter: does this node type have any projection rule at all?

    Lets a hot write path skip the projection entirely for the overwhelming
    majority of node types (``RuntimeSignal``, ``RunTrace``, …) at the cost of
    one dict lookup.
    """
    key = str(node_type or "").strip().casefold()
    return key in TAXONOMY_RULES or key in REFERENCE_RULES


def _clean(value: Any, forbidden: frozenset[str]) -> str:
    text = str(value or "").strip()
    if not text or len(text) > _MAX_ID_LEN:
        return ""
    if any(character in forbidden for character in text):
        return ""
    return text


def _clean_taxonomy_id(value: Any) -> str:
    return _clean(value, _FORBIDDEN_TAXONOMY)


def _clean_reference(value: Any) -> str:
    return _clean(value, _FORBIDDEN_REFERENCE)


def _ancestors(taxonomy_id: str, separator: str) -> list[str]:
    """``A.B.C`` → ``["A.B", "A"]`` (nearest first), bounded by :data:`_MAX_DEPTH`."""
    parts = [part for part in taxonomy_id.split(separator) if part]
    if len(parts) < 2 or len(parts) > _MAX_DEPTH:
        return []
    return [separator.join(parts[:index]) for index in range(len(parts) - 1, 0, -1)]


def _inherited(properties: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: properties[key]
        for key in _GOVERNANCE_KEYS
        if properties.get(key) is not None
    }


def _apply_taxonomy(
    node_id: str,
    properties: Mapping[str, Any],
    rule: TaxonomyRule,
    out: ProjectedRelations,
) -> None:
    taxonomy_id = rule.canonical(_clean_taxonomy_id(properties.get(rule.property)))
    if not taxonomy_id:
        return

    governance = _inherited(properties)

    def materialise(identifier: str) -> str:
        target = f"{rule.id_prefix}{identifier}"
        row: dict[str, Any] = {
            "id": target,
            "node_type": rule.node_type,
            **governance,
        }
        if rule.id_property:
            row[rule.id_property] = identifier
        out.nodes.append((target, row))
        return target

    if rule.self_id:
        chain = _ancestors(taxonomy_id, rule.separator)
        if not chain:
            return
        source = node_id
        edge_type = rule.edge_type
        for ancestor in chain:
            target = materialise(ancestor)
            if target != source:
                out.edges.append((source, target, edge_type))
            source = target
            edge_type = rule.ancestor_edge()
        return

    # The value names another node: link to it, then walk ITS ancestry.
    head = materialise(taxonomy_id)
    if head != node_id:
        out.edges.append((node_id, head, rule.edge_type))
    source = head
    for ancestor in _ancestors(taxonomy_id, rule.separator):
        target = materialise(ancestor)
        if target != source:
            out.edges.append((source, target, rule.ancestor_edge()))
        source = target


def _apply_reference(
    node_id: str,
    properties: Mapping[str, Any],
    rule: ReferenceRule,
    out: ProjectedRelations,
) -> None:
    raw = properties.get(rule.property)
    if isinstance(raw, str) or not isinstance(raw, Sequence):
        raw = [raw] if raw else []
    seen: set[str] = set()
    for item in raw:
        target = _clean_reference(item)
        if not target or target == node_id or target in seen:
            continue
        seen.add(target)
        out.candidate_edges.append((node_id, target, rule.edge_type))


def project_relations(
    node_id: str, properties: Mapping[str, Any]
) -> ProjectedRelations:
    """Return the nodes/edges implied by one node's own properties.

    Pure: no engine access, no round trip, no exception for malformed input
    (an unrecognised or malformed value simply projects nothing).
    """
    out = ProjectedRelations()
    node_id = str(node_id or "").strip()
    if not node_id or not isinstance(properties, Mapping):
        return out
    key = str(properties.get("node_type") or "").strip().casefold()
    if not key:
        return out
    for rule in TAXONOMY_RULES.get(key, ()):
        _apply_taxonomy(node_id, properties, rule, out)
    for reference in REFERENCE_RULES.get(key, ()):
        _apply_reference(node_id, properties, reference, out)
    # Two rules can name the same ancestor; the engine merges an ``upsert_node``
    # idempotently but sending it twice is pure waste.
    if len(out.nodes) > 1:
        deduped: dict[str, dict[str, Any]] = {}
        for identifier, row in out.nodes:
            deduped.setdefault(identifier, row)
        out.nodes = list(deduped.items())
    return out
