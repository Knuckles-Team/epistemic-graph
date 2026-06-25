"""End-to-end proof of the cost/efficiency autoscale surface (CONCEPT:KG-2.234).

The session server is built `--features full`, which includes `cost`, so the
`ResourceStats` Method is served. This drives it through the real transport and asserts
the snapshot's per-graph / aggregate counts are accurate.
"""

import os
import pytest
from epistemic_graph.client import SyncEpistemicGraphClient


def _client(graph_name):
    return SyncEpistemicGraphClient.connect(
        socket_path=os.environ["GRAPH_SERVICE_SOCKET"],
        graph_name=graph_name,
    )


def test_resource_stats_reports_accurate_counts():
    graph = "acmecost:rs"
    c = _client(graph)
    if not hasattr(c, "resource_stats"):
        c.close()
        pytest.skip(
            "installed epistemic_graph client predates resource_stats "
            "(CONCEPT:KG-2.234); skip until the worktree client is installed"
        )
    try:
        c.tenants.create(graph)
        for i in range(7):
            c.nodes.add(f"rs{i}", {"i": i})

        snap = c.resource_stats()
        assert isinstance(snap, dict), snap

        # Aggregate shape.
        assert snap["graph_count"] >= 1
        assert snap["tenant_count"] >= 1
        assert "total_memory_bytes" in snap
        assert "budget_evictions_total" in snap

        # Our graph appears with the exact node count + derived tenant.
        g = next((x for x in snap["graphs"] if x["graph"] == graph), None)
        assert g is not None, f"{graph} missing from snapshot graphs"
        assert g["nodes"] == 7, g
        assert g["tenant"] == "acmecost", g
        assert g["memory_bytes"] > 0, g
        assert g["hibernated"] is False, g

        # Per-tenant rollup is exact for our tenant.
        t = next((x for x in snap["tenants"] if x["tenant"] == "acmecost"), None)
        assert t is not None
        assert t["nodes"] == 7
        assert t["graphs"] == 1
    finally:
        c.close()
