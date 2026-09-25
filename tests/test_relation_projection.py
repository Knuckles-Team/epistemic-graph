"""Property→edge projection (CONCEPT:AU-KG.enrichment.relation-projection).

The pure half: what a node's own properties imply, with no engine involved.
"""

from __future__ import annotations

from epistemic_graph.relation_projection import (
    project_relations,
    projects_anything,
)


def _concept(node_id: str, **props):
    return {"id": node_id, "node_type": "Concept", **props}


def test_dotted_concept_id_becomes_a_broader_chain():
    projected = project_relations(
        "concept:AU-KG.QUERY.VENDOR-AGNOSTIC-TRAVERSAL",
        _concept(
            "concept:AU-KG.QUERY.VENDOR-AGNOSTIC-TRAVERSAL",
            concept_id="AU-KG.QUERY.VENDOR-AGNOSTIC-TRAVERSAL",
        ),
    )
    assert projected.edges == [
        (
            "concept:AU-KG.QUERY.VENDOR-AGNOSTIC-TRAVERSAL",
            "concept:AU-KG.QUERY",
            "BROADER",
        ),
        ("concept:AU-KG.QUERY", "concept:AU-KG", "BROADER"),
    ]
    # Every edge endpoint is materialised by the SAME projection — the engine
    # rejects a batch whose edge endpoints do not exist at that point.
    materialised = {identifier for identifier, _ in projected.nodes}
    assert materialised == {"concept:AU-KG.QUERY", "concept:AU-KG"}
    for _source, target, _rel in projected.edges:
        assert target in materialised


def test_ancestor_rows_inherit_governance_and_carry_identity_only():
    projected = project_relations(
        "concept:AU-KG.QUERY.X",
        _concept(
            "concept:AU-KG.QUERY.X",
            concept_id="AU-KG.QUERY.X",
            name="a real name",
            tenant_id="acme",
            _shared_scope="org",
            classification="public",
        ),
    )
    rows = dict(projected.nodes)
    ancestor = rows["concept:AU-KG.QUERY"]
    # BUG-033/BUG-039: an unstamped row is written-but-unreadable.
    assert ancestor["tenant_id"] == "acme"
    assert ancestor["_shared_scope"] == "org"
    assert ancestor["classification"] == "public"
    assert ancestor["node_type"] == "Concept"
    assert ancestor["concept_id"] == "AU-KG.QUERY"
    # It must NOT carry the child's descriptive fields — an upsert field-merges,
    # so copying ``name`` would overwrite the ancestor's own real name later.
    assert "name" not in ancestor


def test_pillar_links_to_another_concept_and_its_ancestry():
    projected = project_relations(
        "concept:AU-KG.QUERY.X",
        _concept(
            "concept:AU-KG.QUERY.X",
            concept_id="AU-KG.QUERY.X",
            pillar="EG-KG.COMPUTE.BACKEND",
        ),
    )
    assert (
        "concept:AU-KG.QUERY.X",
        "concept:EG-KG.COMPUTE.BACKEND",
        "PART_OF",
    ) in projected.edges
    assert (
        "concept:EG-KG.COMPUTE.BACKEND",
        "concept:EG-KG.COMPUTE",
        "BROADER",
    ) in projected.edges


def test_single_segment_id_projects_nothing():
    projected = project_relations("concept:AU", _concept("concept:AU", concept_id="AU"))
    assert not projected


def test_literals_never_become_edges():
    """The inverse error: ``has_size``/``has_version`` are attributes."""
    projected = project_relations(
        "entity:x",
        {
            "id": "entity:x",
            "node_type": "Entity",
            "has_size": "4.2MB",
            "has_version": "1.2.3",
            "origin_url": "https://example.invalid/a",
        },
    )
    assert not projected
    assert not projects_anything("Entity")


def test_free_text_property_is_rejected_as_a_reference():
    projected = project_relations(
        "concept:AU-KG.X",
        _concept(
            "concept:AU-KG.X",
            concept_id="AU-KG.X",
            pillar="some prose, with punctuation",
        ),
    )
    # ``concept_id`` still projects; the prose ``pillar`` does not.
    assert all("prose" not in target for _s, target, _r in projected.edges)


def test_work_item_depends_on_is_a_candidate_needing_verification():
    projected = project_relations(
        "wi:1",
        {
            "id": "wi:1",
            "node_type": "WorkItem",
            "depends_on": ["wi:0", "wi:1", "", "wi:0"],
        },
    )
    # Self-reference and duplicates dropped; the rest cannot be guaranteed to
    # exist, so they are candidates, never unconditional edges.
    assert projected.edges == []
    assert projected.candidate_edges == [("wi:1", "wi:0", "TASK_DEPENDS_ON")]


def test_projection_is_pure_and_total():
    for properties in ({}, {"node_type": "Concept"}, {"node_type": None}):
        assert not project_relations("n", properties)
    assert not project_relations("", {"node_type": "Concept", "concept_id": "A.B"})
