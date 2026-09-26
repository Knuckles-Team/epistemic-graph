"""Deterministic comparative feature matrix data and Markdown rendering."""

from __future__ import annotations

from dataclasses import dataclass, field

from .assimilation_derivation import SynergyBundle

#: max rows persisted in the graph node / live artifact (bounded; full set stays
#: derivable by re-running build). Keeps the node property + artifact within the
#: KG-2.24 bounded-JSON envelope.
MAX_PERSISTED_ROWS = 200


@dataclass
class FeatureMatrixRow:
    """One feature's place in the comparative matrix."""

    feature_id: str
    name: str
    pillar: str
    feature_type: str
    coverage: str  # "covered" | "related" | "novel"
    concept_id: str  # the SATISFIED_BY/RELATES_TO concept, "" when fully novel
    novelty_score: float
    leverage_score: float
    sources: list[str] = field(default_factory=list)
    synergy_partners: list[str] = field(default_factory=list)
    synergy_pillars: list[str] = field(default_factory=list)


@dataclass
class FeatureMatrix:
    """The materialized comparative analysis."""

    rows: list[FeatureMatrixRow] = field(default_factory=list)
    bundles: list[SynergyBundle] = field(default_factory=list)
    source_index: dict[str, list[str]] = field(default_factory=dict)
    counts: dict[str, int] = field(default_factory=dict)
    generated_at: str = ""

    def novel_gaps(self) -> list[FeatureMatrixRow]:
        """Open (non-covered) rows, leverage-ranked — the work-list to implement."""
        gaps = [r for r in self.rows if r.coverage != "covered"]
        gaps.sort(key=lambda r: (r.leverage_score, r.feature_id), reverse=True)
        return gaps


def render_markdown(matrix: FeatureMatrix) -> str:
    """Render the matrix as a comparative-analysis markdown report."""
    c = matrix.counts
    out: list[str] = []
    out.append("# Comparative Feature / Innovation Matrix")
    if matrix.generated_at:
        out.append(f"\n_Generated: {matrix.generated_at}_")
    out.append(
        f"\n**{c.get('total', 0)} features** across **{c.get('sources', 0)} sources** "
        f"— covered: {c.get('covered', 0)} · related: {c.get('related', 0)} · "
        f"novel: {c.get('novel', 0)} · synergy bundles: {c.get('bundles', 0)}\n"
    )

    out.append("## Feature × coverage\n")
    out.append(
        "| Feature | Pillar | Coverage | Concept | Novelty | Leverage | Sources |"
    )
    out.append("|---|---|---|---|---:|---:|---:|")
    for r in matrix.rows[:MAX_PERSISTED_ROWS]:
        out.append(
            f"| {clip_feature_label(r.name, 48)} | {r.pillar or '—'} | {r.coverage} | "
            f"{r.concept_id or '—'} | {r.novelty_score:.2f} | "
            f"{r.leverage_score:.2f} | {len(r.sources)} |"
        )

    gaps = matrix.novel_gaps()
    out.append("\n## Novel gaps to implement (leverage-ranked)\n")
    if gaps:
        for r in gaps[:50]:
            tag = "novel" if r.coverage == "novel" else f"related→{r.concept_id}"
            label = clip_feature_label(r.name, 64)
            out.append(
                f"- **{label}** ({tag}) — leverage {r.leverage_score:.2f}, "
                f"novelty {r.novelty_score:.2f}, {len(r.sources)} source(s)"
            )
    else:
        out.append("_No open gaps — everything ingested is already covered._")

    out.append(
        "\n## Cross-source synergies → novel unique implementations\n"
        "_Cross-pillar bundles: ideas that are individually known but TOGETHER are "
        "new — the combine-to-surpass candidates._\n"
    )
    if matrix.bundles:
        names = {r.feature_id: r.name for r in matrix.rows}
        for i, b in enumerate(matrix.bundles, 1):
            members = ", ".join(
                clip_feature_label(names.get(m, m), 40) for m in b.members[:6]
            )
            out.append(f"{i}. **[{' + '.join(b.pillars)}]** {members}")
    else:
        out.append("_No cross-pillar synergy bundles detected this cycle._")

    out.append("\n## Per-source contribution\n")
    if matrix.source_index:
        names = {r.feature_id: r.name for r in matrix.rows}
        for src, fids in sorted(
            matrix.source_index.items(), key=lambda kv: len(kv[1]), reverse=True
        )[:40]:
            sample = ", ".join(
                clip_feature_label(names.get(f, f), 32) for f in fids[:4]
            )
            more = f" (+{len(fids) - 4} more)" if len(fids) > 4 else ""
            source_label = clip_feature_label(src, 40)
            out.append(f"- `{source_label}` → {len(fids)} feature(s): {sample}{more}")
    else:
        out.append("_No source provenance recorded on the features._")

    return "\n".join(out) + "\n"


def clip_feature_label(text: str, n: int) -> str:
    text = str(text).replace("\n", " ").replace("|", "/").strip()
    return text if len(text) <= n else text[: n - 1] + "…"
