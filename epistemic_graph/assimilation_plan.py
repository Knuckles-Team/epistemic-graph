"""Pure grounded assimilation plan proposal and fallback template."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class PlanProposal:
    feature_id: str
    plan_id: str
    title: str
    body: str
    sources: list[str] = field(default_factory=list)
    synergies: list[str] = field(default_factory=list)
    status: str = "proposed"


def default_synth(neighborhood: dict[str, Any]) -> dict[str, str]:
    """Deterministic grounded SDD plan (no LLM) from the hydrated neighborhood."""
    n = neighborhood
    syn = f" Synergizes with: {', '.join(n['synergies'])}." if n["synergies"] else ""
    src = ", ".join(n["sources"]) or "(no linked sources)"
    title = f"Assimilate: {n['name']}"
    concepts = ", ".join(n["concept_ids"]) or "n/a"
    body = (
        f"# SDD Plan: {n['name']}\n\n"
        f"> Pillar: {n['pillar'] or 'n/a'} · Concepts: {concepts}\n\n"
        f"## Overview\nAssimilate the **{n['name']}** capability into the "
        f"agent-utilities ecosystem.{syn}\n\n"
        f"## Evidence / Sources\n{src}\n\n"
        f"## Implementation\nWire the mechanism on a live path with a concept-tagged "
        f"test; on completion, close out the source(s) via the assimilation ledger "
        f"(`ASSIMILATED_INTO`).\n"
    )
    return {"title": title, "body": body}
