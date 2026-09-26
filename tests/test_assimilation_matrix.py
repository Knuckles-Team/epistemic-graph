"""Pure comparative matrix rendering stays stable for AU's live artifact."""

from __future__ import annotations

import pytest

from epistemic_graph.assimilation_derivation import SynergyBundle
from epistemic_graph.assimilation_matrix import (
    FeatureMatrix,
    FeatureMatrixRow,
    render_markdown,
)

pytestmark = pytest.mark.no_engine


def _row(feature_id: str, coverage: str, leverage: float) -> FeatureMatrixRow:
    return FeatureMatrixRow(
        feature_id=feature_id,
        name=f"Feature | {feature_id}",
        pillar="KG",
        feature_type="capability",
        coverage=coverage,
        concept_id="",
        novelty_score=0.75,
        leverage_score=leverage,
        sources=["paper-1"],
    )


def test_novel_gaps_exclude_covered_and_rank_by_leverage():
    matrix = FeatureMatrix(
        rows=[
            _row("low", "novel", 1),
            _row("done", "covered", 9),
            _row("high", "related", 3),
        ]
    )
    assert [row.feature_id for row in matrix.novel_gaps()] == ["high", "low"]


def test_markdown_renders_coverage_synergy_and_source_without_table_escape():
    matrix = FeatureMatrix(
        rows=[_row("a", "novel", 2), _row("b", "related", 1)],
        bundles=[SynergyBundle(members=["a", "b"], pillars=["KG", "ORCH"])],
        source_index={"paper-1": ["a", "b"]},
        counts={"total": 2, "sources": 1, "novel": 1, "related": 1, "bundles": 1},
    )
    report = render_markdown(matrix)
    assert "Feature / a" in report
    assert "Cross-source synergies" in report
    assert "KG + ORCH" in report
    assert "paper-1" in report
