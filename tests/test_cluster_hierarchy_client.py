"""``GraphOperationsClient.cluster_hierarchy_*`` marshal the VIZ-1 hierarchical
cluster-tree RPCs (CONCEPT:EG-KG.compute.leiden-hierarchy) — server-side clustering for
million-node graph visualization: ``cluster_hierarchy_refresh`` (re)computes and
durably caches a graph's Leiden cluster tree, ``cluster_hierarchy_clusters``
serves one cached level (optionally scoped to one parent's children), and
``cluster_hierarchy_expand`` drills one cluster down (real nodes/edges at
level 1, child clusters above it).

The engine-side computation (``eg_compute::algorithms::cluster_hierarchy``,
``graph_algos::leiden_hierarchy``) is covered by the Rust unit/benchmark tests in
``crates/eg-compute/src/algorithms.rs`` and
``crates/eg-compute/src/graph_algos/leiden.rs``; this asserts the Python client
sends exactly the wire shape the engine's ``Method::ClusterHierarchy*`` variants
expect and returns the response unchanged.
"""

from __future__ import annotations

from typing import Any

import pytest

from epistemic_graph.client import GraphOperationsClient

# Fake-client unit tests only -- never needs the shared native engine (see
# conftest.py's session-scoped `start_epistemic_graph_server` fixture, which
# this marker exempts this module from triggering).
pytestmark = pytest.mark.no_engine


class _FakeClient:
    def __init__(self, ret: Any = None) -> None:
        self.sent: list[tuple[str, dict[str, Any] | None]] = []
        self._ret = ret

    async def _send(self, method: str, params: dict[str, Any] | None = None) -> Any:
        self.sent.append((method, params))
        return self._ret


@pytest.mark.asyncio
async def test_refresh_sends_label_resolution_and_seed() -> None:
    fake = _FakeClient(
        ret={
            "graph": "g1",
            "levels": 3,
            "base_node_count": 100,
            "base_edge_count": 400,
            "top_level_clusters": 5,
            "cached": True,
        }
    )
    gc = GraphOperationsClient(fake)  # type: ignore[arg-type]
    out = await gc.cluster_hierarchy_refresh(label="Doc", resolution=1.2, seed=7)
    assert fake.sent == [
        (
            "ClusterHierarchyRefresh",
            {"label": "Doc", "resolution": 1.2, "seed": 7},
        )
    ]
    assert out["levels"] == 3
    assert out["cached"] is True


@pytest.mark.asyncio
async def test_refresh_defaults_no_label_resolution_one_seed_zero() -> None:
    fake = _FakeClient(ret={})
    gc = GraphOperationsClient(fake)  # type: ignore[arg-type]
    await gc.cluster_hierarchy_refresh()
    assert fake.sent == [
        ("ClusterHierarchyRefresh", {"label": None, "resolution": 1.0, "seed": 0})
    ]


@pytest.mark.asyncio
async def test_clusters_sends_level_and_optional_parent() -> None:
    fake = _FakeClient(
        ret={
            "level": 2,
            "clusters": [{"id": "L2-0", "label": "x", "node_count": 10, "edge_count": 20.0}],
            "inter_cluster_edges": [],
        }
    )
    gc = GraphOperationsClient(fake)  # type: ignore[arg-type]
    out = await gc.cluster_hierarchy_clusters(level=2)
    assert fake.sent == [
        ("ClusterHierarchyClusters", {"level": 2, "parent_cluster_id": None})
    ]
    assert out["level"] == 2
    assert out["clusters"][0]["id"] == "L2-0"


@pytest.mark.asyncio
async def test_clusters_with_parent_scopes_to_that_parents_children() -> None:
    fake = _FakeClient(ret={"level": 1, "clusters": [], "inter_cluster_edges": []})
    gc = GraphOperationsClient(fake)  # type: ignore[arg-type]
    await gc.cluster_hierarchy_clusters(level=1, parent_cluster_id="L2-3")
    assert fake.sent == [
        ("ClusterHierarchyClusters", {"level": 1, "parent_cluster_id": "L2-3"})
    ]


@pytest.mark.asyncio
async def test_expand_sends_cluster_id() -> None:
    fake = _FakeClient(
        ret={"nodes": [{"id": "n1"}], "edges": [], "child_clusters": []}
    )
    gc = GraphOperationsClient(fake)  # type: ignore[arg-type]
    out = await gc.cluster_hierarchy_expand("L1-0")
    assert fake.sent == [("ClusterHierarchyExpand", {"cluster_id": "L1-0"})]
    assert out["nodes"][0]["id"] == "n1"
