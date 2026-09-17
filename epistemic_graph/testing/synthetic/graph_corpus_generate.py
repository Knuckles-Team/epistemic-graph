"""Deterministically build a :class:`MemoryCorpus`
(CONCEPT:EG-KG.query.usecase-agent-memory, served-path counterpart).

Seed sensitivity is deliberately partial: the six core nodes' embeddings/text
carry a proven, delicately separated ranking outcome (vector-alone,
BM25-alone, and the fused fusion winner each land on a different, named node
regardless of seed), so those numbers are fixed. What varies by seed is
everything safe to vary without disturbing that proof: the node-id salt, which
extra "noise" episode is planted, the analytics fan-in width, and the time
series' numeric offset -- so two seeds never produce byte-identical corpora,
but every seed reproduces the same provable shape.
"""

from __future__ import annotations

from ._model import Provenance
from ._rng import SeededStream
from .graph_corpus import (
    EPISODE_TYPE,
    ONTOLOGY_TURTLE,
    AnalyticsFixture,
    CorpusEdge,
    CorpusNode,
    MemoryCorpus,
    SeriesFixture,
)

GENERATOR = "graph_corpus"
GENERATOR_VERSION = 1

_NOISE_TOPICS: tuple[tuple[str, str], ...] = (
    ("noise-astro", "stellar nucleosynthesis supernova remnant spectroscopy survey"),
    ("noise-culinary", "sourdough fermentation gluten hydration bench technique"),
    ("noise-botany", "xylem phloem transpiration stomatal conductance study"),
)
_SERIES_BASE: tuple[float, ...] = (10.0, 20.0, 30.0, 40.0, 50.0, 60.0)
_SERIES_STEP_NS = 1_000_000_000


def _node_id(salt: str, name: str) -> str:
    return f"mem:{salt}:{name}"


def _core_memory_nodes(
    salt: str, noise_id: str, noise_text: str
) -> tuple[CorpusNode, ...]:
    """The six-node fixture with its ranking signals fixed (see module docstring),
    plus a seed-chosen 7th "noise" episode that wins neither leg."""
    return (
        CorpusNode(node_id=_node_id(salt, "session"), type="Session", valid_from=0),
        CorpusNode(
            node_id=_node_id(salt, "target"),
            type=EPISODE_TYPE,
            valid_from=0,
            text="a kubernetes deployment whose rollout hit a failure",
            embedding=(0.90, 0.40, 0.0),
        ),
        CorpusNode(
            node_id=_node_id(salt, "vec_only"),
            type=EPISODE_TYPE,
            valid_from=0,
            text="quantum chromodynamics lattice gauge theory",
            embedding=(0.99, 0.10, 0.0),
        ),
        CorpusNode(
            node_id=_node_id(salt, "lex_only"),
            type=EPISODE_TYPE,
            valid_from=0,
            text=(
                "kubernetes kubernetes deployment deployment rollout rollout "
                "failure failure kubernetes deployment rollout failure crashloop"
            ),
            embedding=(0.0, 0.10, 0.99),
        ),
        CorpusNode(
            node_id=_node_id(salt, "chatter"),
            type="Chatter",
            valid_from=0,
            text="lunch plans and unrelated small talk",
            embedding=(0.60, 0.80, 0.0),
        ),
        CorpusNode(
            node_id=_node_id(salt, "expired"),
            type=EPISODE_TYPE,
            valid_from=0,
            valid_until=100,
            text="an old kubernetes deployment rollout note",
            embedding=(0.85, 0.52, 0.0),
        ),
        CorpusNode(
            node_id=noise_id,
            type=EPISODE_TYPE,
            valid_from=0,
            text=noise_text,
            embedding=(0.0, 0.0, 1.0),
        ),
    )


def _analytics_fixture(
    salt: str, stream: SeededStream, total_other_nodes: int
) -> tuple[tuple[CorpusNode, ...], tuple[CorpusEdge, ...], AnalyticsFixture]:
    """A hub with a seed-chosen (3..5) fan-in of spokes; each spoke has exactly one
    edge (out, to the hub) and the hub has exactly ``spoke_count`` (in, none out)."""
    spoke_count = stream.between(3, 5)
    hub_id = _node_id(salt, "hub")
    spoke_ids = tuple(_node_id(salt, f"spoke-{i}") for i in range(spoke_count))
    nodes = (CorpusNode(node_id=hub_id, type="Team"),) + tuple(
        CorpusNode(node_id=spoke, type="Contributor") for spoke in spoke_ids
    )
    edges = tuple(
        CorpusEdge(source_id=spoke, target_id=hub_id, relationship="REPORTS_TO")
        for spoke in spoke_ids
    )
    total_nodes = total_other_nodes + len(nodes)
    denom = total_nodes - 1
    return (
        nodes,
        edges,
        AnalyticsFixture(
            hub_id=hub_id,
            spoke_ids=spoke_ids,
            expected_hub_centrality=spoke_count / denom,
            expected_spoke_centrality=1 / denom,
        ),
    )


def _series_fixture(salt: str, stream: SeededStream) -> SeriesFixture:
    offset = stream.between(0, 5) * 10.0
    values = tuple(base + offset for base in _SERIES_BASE)
    points = tuple((i * _SERIES_STEP_NS, values[i]) for i in range(len(values)))
    return SeriesFixture(
        series_id=_node_id(salt, "series"),
        points=points,
        window_from=0,
        window_to=len(values) * _SERIES_STEP_NS,
        width_ns=len(values) * _SERIES_STEP_NS,
        expected_mean=sum(values) / len(values),
    )


def generate_memory_corpus(seed: int) -> MemoryCorpus:
    """Build the corpus for ``seed``. Same seed -> byte-identical corpus."""
    stream = SeededStream(seed, "graph-corpus/memory")
    salt = f"{seed & 0xFFFF:04x}"

    noise_label, noise_text = stream.child("noise").choice(_NOISE_TOPICS)
    noise_id = _node_id(salt, noise_label)
    core_nodes = _core_memory_nodes(salt, noise_id, noise_text)

    edges = (
        CorpusEdge(
            source_id=_node_id(salt, "session"),
            target_id=_node_id(salt, "target"),
            relationship="RELATES",
        ),
        CorpusEdge(
            source_id=_node_id(salt, "target"),
            target_id=_node_id(salt, "lex_only"),
            relationship="RELATES",
        ),
        CorpusEdge(
            source_id=_node_id(salt, "session"),
            target_id=_node_id(salt, "chatter"),
            relationship="RELATES",
        ),
    )

    analytics_nodes, analytics_edges, analytics = _analytics_fixture(
        salt, stream.child("analytics"), len(core_nodes)
    )
    series = _series_fixture(salt, stream.child("series"))

    return MemoryCorpus(
        provenance=Provenance(
            generator=GENERATOR, generator_version=GENERATOR_VERSION, seed=seed
        ),
        session_id=_node_id(salt, "session"),
        target_id=_node_id(salt, "target"),
        vec_only_id=_node_id(salt, "vec_only"),
        lex_only_id=_node_id(salt, "lex_only"),
        chatter_id=_node_id(salt, "chatter"),
        expired_id=_node_id(salt, "expired"),
        noise_id=noise_id,
        nodes=core_nodes + analytics_nodes,
        edges=edges + analytics_edges,
        ontology_turtle=ONTOLOGY_TURTLE,
        now_ts=200.0,
        expired_still_live_ts=50.0,
        analytics=analytics,
        series=series,
    )
