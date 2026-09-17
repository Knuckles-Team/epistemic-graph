"""Catalog generator: planted coverage, planted traps and strict typing."""

from __future__ import annotations

import json

import pytest
from pydantic import ValidationError

from epistemic_graph.testing.synthetic import vocabulary as vocab
from epistemic_graph.testing.synthetic.catalog import Catalog, SyntheticComponent
from epistemic_graph.testing.synthetic.catalog_generate import generate_catalog

pytestmark = pytest.mark.no_engine

SEEDS = (0, 7, 2026)
CAPABILITY_TERMS = (vocab.CAPABILITY_ROOT, *vocab.under(vocab.CAPABILITY_ROOT))


@pytest.fixture(scope="module", params=SEEDS)
def catalog(request: pytest.FixtureRequest) -> Catalog:
    return generate_catalog(request.param)


def _satisfying(catalog: Catalog, capability: str) -> tuple[str, ...]:
    """Pairwise re-derivation, independent of the generator's ancestor walk."""
    return tuple(
        c.component_id
        for c in catalog.components
        if any(vocab.satisfies(term, capability) for term in c.classification)
    )


def test_same_seed_same_bytes_and_seeds_differ() -> None:
    assert (
        generate_catalog(3).model_dump_json() == generate_catalog(3).model_dump_json()
    )
    assert (
        generate_catalog(3).model_dump_json() != generate_catalog(4).model_dump_json()
    )
    assert generate_catalog(3).provenance.evidence == "synthetic"


def test_coverage_table_matches_pairwise_subsumption(catalog: Catalog) -> None:
    for capability in CAPABILITY_TERMS:
        assert catalog.satisfying(capability) == _satisfying(catalog, capability), (
            capability
        )


def test_every_leaf_but_the_planted_gaps_is_covered(catalog: Catalog) -> None:
    for leaf in vocab.leaves(vocab.CAPABILITY_ROOT):
        assert bool(_satisfying(catalog, leaf)) is (leaf not in catalog.uncovered), leaf
    assert len(catalog.uncovered) == 2


def test_traps_are_planted_and_inert(catalog: Catalog) -> None:
    by_trap = {c.trap: c for c in catalog.components if c.trap != "none"}
    assert set(by_trap) == {"broader_only", "foreign_only", "injection", "unknown_cost"}
    broader = by_trap["broader_only"].classification[0]
    assert all(not vocab.satisfies(broader, gap) for gap in catalog.uncovered)
    assert any(vocab.satisfies(gap, broader) for gap in catalog.uncovered)
    foreign = by_trap["foreign_only"]
    assert foreign.classification == () and foreign.declared_capabilities
    assert "instruction" in by_trap["injection"].summary
    assert by_trap["unknown_cost"].cost is None


def test_tools_carry_object_schemas_and_facts(catalog: Catalog) -> None:
    tools = [c for c in catalog.components if c.kind == "tool"]
    assert tools and all(c.latency and c.modalities_in for c in tools)
    for tool in tools:
        assert tool.tool is not None
        schema = json.loads(tool.tool.input_schema)
        assert schema["type"] == "object" and schema["properties"]


def test_model_profiles_carry_consistent_facts(catalog: Catalog) -> None:
    models = [c.model for c in catalog.components if c.model is not None]
    assert any(m.supports_tools for m in models)
    assert all(m.max_output_tokens <= m.context_window_tokens for m in models)


def test_skill_dependencies_resolve_and_publish_first(catalog: Catalog) -> None:
    order = [c.component_id for c in catalog.publish_order()]
    for skill in (c for c in catalog.components if c.requires):
        for dependency in skill.requires:
            assert catalog.component(dependency).kind == "tool"
            assert order.index(dependency) < order.index(skill.component_id)


@pytest.mark.parametrize("read_only", [False, True])
@pytest.mark.parametrize("task", [None, "eg:task/research", "eg:task/operate"])
def test_expected_search_matches_a_brute_force_filter(
    catalog: Catalog, task: str | None, read_only: bool
) -> None:
    capabilities = ["eg:capability/analysis"] if task is None else []
    required = [*capabilities, *vocab.capabilities_for_task(task or "")]
    brute = tuple(
        c.component_id
        for c in catalog.components
        if any(vocab.satisfies(t, r) for t in c.classification for r in required)
        and not (read_only and (c.side_effecting))
    )
    assert (
        catalog.expected_search(capabilities, task=task, read_only=read_only) == brute
    )


@pytest.mark.parametrize(
    "change",
    [
        {"classification": ("eg:capability/retrieval/web-searc",)},
        {"declared_capabilities": ("eg:capability/retrieval",)},
        {"unexpected": 1},
        {"kind": "tool"},
    ],
)
def test_strict_component_model_refuses_bad_input(change: dict[str, object]) -> None:
    component = generate_catalog(0).components[0].model_dump()
    with pytest.raises(ValidationError):
        SyntheticComponent.model_validate(component | change)
