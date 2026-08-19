"""Regression fixtures for source-anchored scaling documentation claims."""

from __future__ import annotations

from pathlib import Path

import scripts.check_scale_documentation as scale_claims

ROOT = Path(__file__).resolve().parents[1]


def test_current_scale_claim_register_is_source_anchored() -> None:
    assert scale_claims.check_claims() == []


def test_planted_stale_memory_budget_claim_fails_closed() -> None:
    fixture = ROOT / "tests/fixtures/scale_claims_planted_drift.md"
    errors = scale_claims.check_claims(ROOT, fixture)
    assert any("required source text is missing" in error for error in errors)
