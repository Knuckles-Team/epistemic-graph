"""Shared tagged unions keep their variant classes importable (EH-377 follow-up).

EH-377 renders a union reached by several surfaces once, in ``_shared``. A surface
module that re-exports the union must re-export its variant classes too, and so must
``models``: graph-os constructs ``ComponentProvenanceMcpServer`` from
``agent_component`` and AU checks the decision variants from ``decision``.
"""

from __future__ import annotations

import importlib

import pytest

pytestmark = pytest.mark.no_engine

CONSUMED = [
    ("agent_component", "ComponentProvenance", "ComponentProvenanceMcpServer"),
    ("decision", "AbstainReason", "AbstainReasonInsufficientConfidence"),
    ("decision", "CandidateSourceRecord", "CandidateSourceRecordAgentLibrary"),
    ("decision", "DecisionOutcome", "DecisionOutcomeAbstained"),
]


@pytest.mark.spec("EG-CONTRACT-R005")
@pytest.mark.parametrize(("module", "union", "variant"), CONSUMED)
@pytest.mark.spec("EG-CONTRACT-R005")
def test_a_consumed_variant_is_exported_where_its_union_is(
    module: str, union: str, variant: str
) -> None:
    surface = importlib.import_module(f"epistemic_graph.generated.{module}")
    models = importlib.import_module("epistemic_graph.generated.models")
    assert union in surface.__all__
    assert variant in surface.__all__
    assert variant in models.__all__
    assert getattr(surface, variant) is getattr(models, variant)
