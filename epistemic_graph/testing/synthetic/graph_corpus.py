"""(g) A multi-modal graph corpus: nodes/edges, BM25 text, vectors, a bitemporal
validity window, RDF/OWL axioms, and a time series -- one small planted fixture
that exercises every retrieval leg the served engine composes in ONE plan.

The node/edge shape mirrors the proven library-internal fixture in
`tests/usecase_agent_memory.rs` (an agent's episodic memory: a `target` episode
that wins the FUSED ranking though it tops no single modality, a `vec_only` /
`lex_only` single-modality specialist each, a non-`Episode` `chatter` node the
OWL leg must drop, and an `expired` episode a bi-temporal `AS OF` must drop) --
this module is the GENERATED, served-path counterpart: a seed reproduces the
exact same corpus, and the parts of it that are safe to vary by seed (id salt,
one extra noise episode, the analytics fan-in size, the series' numeric offset)
do so, while the delicately separated ranking signals stay fixed so the planted
outcome is provable regardless of seed.
"""

from __future__ import annotations

from pydantic import model_validator

from ._model import Provenance, SyntheticModel, sorted_unique

GENERATOR = "graph_corpus"
GENERATOR_VERSION = 1

QUERY_VECTOR: tuple[float, float, float] = (1.0, 0.0, 0.0)
QUERY_TEXT = "kubernetes deployment rollout failure"
MEMORY_CLASS_IRI = "<http://mem/Memory>"
EPISODE_TYPE = "Episode"
ONTOLOGY_TURTLE = (
    "@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n"
    "<http://mem/Episode> rdfs:subClassOf <http://mem/Memory> .\n"
)


class CorpusNode(SyntheticModel):
    node_id: str
    type: str
    text: str | None = None
    embedding: tuple[float, ...] | None = None
    valid_from: int = 0
    valid_until: int | None = None


class CorpusEdge(SyntheticModel):
    source_id: str
    target_id: str
    relationship: str


class SeriesFixture(SyntheticModel):
    """A planted (ts_ns, value) signal plus the window whose exact mean is known."""

    series_id: str
    points: tuple[tuple[int, float], ...]
    window_from: int
    window_to: int
    width_ns: int
    expected_mean: float


class AnalyticsFixture(SyntheticModel):
    """A hub with ``len(spoke_ids)`` planted incoming edges; every spoke has one
    outgoing edge and nothing else, so both centralities are exact fractions of
    the corpus's own (independently countable) total node count."""

    hub_id: str
    spoke_ids: tuple[str, ...]
    expected_hub_centrality: float
    expected_spoke_centrality: float


class MemoryCorpus(SyntheticModel):
    provenance: Provenance
    session_id: str
    target_id: str
    vec_only_id: str
    lex_only_id: str
    chatter_id: str
    expired_id: str
    noise_id: str
    nodes: tuple[CorpusNode, ...]
    edges: tuple[CorpusEdge, ...]
    ontology_turtle: str
    now_ts: float
    expired_still_live_ts: float
    analytics: AnalyticsFixture
    series: SeriesFixture

    @model_validator(mode="after")
    def _referentially_closed(self) -> MemoryCorpus:
        ids = tuple(n.node_id for n in self.nodes)
        sorted_unique(tuple(sorted(ids, key=str.encode)), "node ids")
        known = set(ids)
        for edge in self.edges:
            if edge.source_id not in known or edge.target_id not in known:
                raise ValueError(
                    f"edge {edge.source_id}->{edge.target_id} names an unknown node"
                )
        return self

    def node(self, node_id: str) -> CorpusNode:
        for candidate in self.nodes:
            if candidate.node_id == node_id:
                return candidate
        raise KeyError(node_id)

    def adjacency(self, relationship: str) -> dict[str, tuple[str, ...]]:
        """Outgoing neighbors per source, for ``relationship`` edges only."""
        out: dict[str, list[str]] = {}
        for edge in self.edges:
            if edge.relationship == relationship:
                out.setdefault(edge.source_id, []).append(edge.target_id)
        return {source: tuple(targets) for source, targets in out.items()}
