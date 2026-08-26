"""End-to-end proof of the VIZ-1 hierarchical cluster-tree RPCs against the REAL
engine (CONCEPT:EG-KG.compute.leiden-hierarchy) — server-side clustering for million-node
graph visualization: build a small graph, refresh its cluster hierarchy, list
the cached level, and expand a cluster down to its real member nodes.

Unlike ``tests/test_cluster_hierarchy_client.py`` (fake-client wire-shape
checks), this exercises the WHOLE path: engine handler
(``src/server/handlers/graph_ops.rs``'s ``ClusterHierarchyRefresh``/
``Clusters``/``Expand`` arms) -> the durable cache
(``server::persistence::cluster_hierarchy_store``) -> back out through the
Python client -- against the real compiled `epistemic-graph-server`, via the
session-scoped ``start_epistemic_graph_server`` fixture.
"""

from __future__ import annotations

import contextlib
import os

import pytest
from conftest import request_context

from epistemic_graph.client import EpistemicGraphClient

TENANT = "viz1_cluster_hierarchy_e2e"


@pytest.mark.asyncio
async def test_refresh_list_and_expand_round_trip() -> None:
    socket_path = os.environ.get(
        "GRAPH_SERVICE_SOCKET", "/tmp/test_epistemic_graph_local.sock"
    )
    secret = os.environ.get("GRAPH_SERVICE_AUTH_SECRET", "test-epistemic-graph-secret")
    client = await EpistemicGraphClient.connect(
        socket_path=socket_path,
        auth_secret=secret,
        verified_context=request_context(),
        graph_name=TENANT,
        timeout=60.0,
        heavy_timeout=60.0,
    )
    try:
        with contextlib.suppress(RuntimeError):  # fresh tenant
            await client.tenants.create(TENANT, "Agent")
        await client.graph.clear()

        # Two dense 4-cliques joined by a single weak bridge — the textbook
        # community-detection fixture (same shape the Rust leiden.rs tests use).
        clique_a = ["a0", "a1", "a2", "a3"]
        clique_b = ["b0", "b1", "b2", "b3"]
        for node_id in clique_a + clique_b:
            await client.nodes.add(node_id, {"type": "Doc"})
        for clique in (clique_a, clique_b):
            for i in range(len(clique)):
                for j in range(i + 1, len(clique)):
                    await client.edges.add(clique[i], clique[j])
        await client.edges.add("a3", "b0")  # the one bridge

        # 1) Refresh: (re)compute + durably cache the hierarchy.
        summary = await client.graph.cluster_hierarchy_refresh(resolution=1.0, seed=0)
        assert summary["base_node_count"] == 8
        assert summary["levels"] >= 1
        assert summary["cached"] is True

        # 2) Clusters at level 1: two cliques -> two clusters, all 8 members
        # covered exactly once, each cluster's own type histogram all "Doc".
        level1 = await client.graph.cluster_hierarchy_clusters(level=1)
        assert level1["level"] == 1
        assert len(level1["clusters"]) == 2, level1["clusters"]
        total_members = sum(c["node_count"] for c in level1["clusters"])
        assert total_members == 8
        for c in level1["clusters"]:
            assert c["top_node_types"] == [["Doc", c["node_count"]]]

        # An out-of-range level is a clean error, not a crash/hang.
        with pytest.raises(Exception):
            await client.graph.cluster_hierarchy_clusters(level=999)

        # 3) Expand one level-1 cluster down to its real member nodes/edges —
        # the members must be a real subset of the 8 nodes we added, and the
        # cluster must have no children (level 1 is the finest computed level).
        first_cluster_id = level1["clusters"][0]["id"]
        expanded = await client.graph.cluster_hierarchy_expand(first_cluster_id)
        assert expanded["child_clusters"] == []
        member_ids = {n["id"] for n in expanded["nodes"]}
        assert member_ids, "expand must return at least one member node"
        assert member_ids <= set(clique_a) | set(clique_b)
        # Every member must be wholly inside one clique (the two cliques are
        # each internally complete and joined by exactly one bridge edge, so
        # Leiden must not split a clique across two clusters).
        assert member_ids <= set(clique_a) or member_ids <= set(clique_b)

        # An unknown cluster_id is a clean error, not a crash.
        with pytest.raises(Exception):
            await client.graph.cluster_hierarchy_expand("L1-9999")
    finally:
        await client.close()
