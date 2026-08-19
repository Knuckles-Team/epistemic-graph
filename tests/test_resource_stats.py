"""End-to-end proof of the cost/efficiency autoscale surface (CONCEPT:EG-KG.compute.lane-v).

The session server is built `--features full`, which includes `cost`, so the
`ResourceStats` Method is served. This drives it through the real transport and asserts
the snapshot's per-graph / aggregate counts are accurate.
"""

import os

import pytest
from conftest import request_context

from epistemic_graph.client import SyncEpistemicGraphClient


def _client(graph_name):
    return SyncEpistemicGraphClient.connect(
        socket_path=os.environ["GRAPH_SERVICE_SOCKET"],
        graph_name=graph_name,
        verified_context=request_context(),
    )


def test_resource_stats_reports_accurate_counts():
    graph = "acmecost:rs"
    c = _client(graph)
    if not hasattr(c, "resource_stats"):
        c.close()
        pytest.skip(
            "installed epistemic_graph client predates resource_stats "
            "(CONCEPT:EG-KG.compute.lane-v); skip until the worktree client is installed"
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

        # Explicit detail pages are finite and keyset-advanceable.  The
        # transport/client must never fall back to the old unbounded export for
        # a caller-provided limit.
        page = c.resource_stats(limit=1)
        assert page["limit"] == 1
        assert len(page["graphs"]) <= 1
        if page["has_more"]:
            assert page["next_cursor"]
            following = c.resource_stats(cursor=page["next_cursor"], limit=1)
            assert following["cursor"] == page["next_cursor"]
            assert len(following["graphs"]) <= 1

        summary = c.resource_stats(summary=True)
        assert summary["summary"] is True
        assert summary["graphs"] == []
        assert summary["tenants"] == []
        assert "effective_cpu_cores" in summary
        assert "effective_memory_limit_bytes" in summary
        assert "coalescer_queue_depth" in summary
    finally:
        c.close()
