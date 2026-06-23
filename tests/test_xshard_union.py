"""Cross-shard scatter-gather union + affinity co-residency (CONCEPT:KG-2.181).

Fake clients only — no running engine. Mirrors tests/test_union_reads.py.
"""

from __future__ import annotations

from typing import Any

import pytest

from epistemic_graph.pool import (
    AffinityRegistry,
    ShardRouter,
    _default_affinity_groups,
)

# ── (a) affinity routing pins ingest lanes to the __commons__ shard ──────────


def test_affinity_pins_ingest_lanes_to_commons() -> None:
    reg = AffinityRegistry()
    anchor = reg.anchor_for("__commons__")
    assert reg.anchor_for("__ingest__") == anchor
    assert reg.anchor_for("__ingest_news__") == anchor
    assert reg.anchor_for("__ingest_arxiv__") == anchor
    # An unrelated graph anchors to itself (plain HRW).
    assert reg.anchor_for("agent:planner") == "agent:planner"


def test_affinity_routes_ingest_lanes_to_same_shard() -> None:
    # Several endpoints so plain HRW would scatter the lanes.
    eps = [f"tcp://shard-{i}:9000" for i in range(5)]
    router = ShardRouter(eps)
    commons_shard = router._get_shard_endpoint("__commons__")
    for lane in ("__ingest__", "__ingest_news__", "__ingest_x__", "__ingest_pdf__"):
        assert router._get_shard_endpoint(lane) == commons_shard, lane


def test_affinity_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GRAPH_AFFINITY_GROUPS", "__hot__:__hot_a__|__hot_b*")
    groups = _default_affinity_groups()
    reg = AffinityRegistry(groups)
    assert reg.anchor_for("__hot_a__") == "__hot__"
    assert reg.anchor_for("__hot_b_extra__") == "__hot__"
    # Defaults still present.
    assert reg.anchor_for("__ingest__") == "__commons__"


# ── scatter-gather fakes ─────────────────────────────────────────────────────


class _FakeNodes:
    """Records union calls and returns per-graph canned data for one shard."""

    def __init__(self, shard: _FakeShardClient) -> None:
        self._shard = shard

    async def properties_union(
        self, node_id: str, graphs: list[str]
    ) -> dict[str, Any] | None:
        self._shard.calls.append(("properties", node_id, list(graphs)))
        for g in graphs:  # first-found, in order
            props = self._shard.data.get((g, node_id))
            if props is not None:
                return props
        return None

    async def list_by_label_union(
        self, label: str, graphs: list[str], limit: int = 0
    ) -> list[tuple[str, Any]]:
        self._shard.calls.append(("label", label, list(graphs)))
        seen: set[str] = set()
        out: list[tuple[str, Any]] = []
        for g in graphs:
            for nid, lbl, props in self._shard.label_rows.get(g, []):
                if lbl == label and nid not in seen:
                    seen.add(nid)
                    out.append((nid, props))
        return out

    async def neighbors_union(self, node_id: str, graphs: list[str]) -> list[str]:
        self._shard.calls.append(("neighbors", node_id, list(graphs)))
        seen: set[str] = set()
        out: list[str] = []
        for g in graphs:
            for nbr in self._shard.neighbors.get((g, node_id), []):
                if nbr not in seen:
                    seen.add(nbr)
                    out.append(nbr)
        return out


class _FakeShardClient:
    def __init__(self) -> None:
        self._graph_name: str | None = None
        self.calls: list[tuple[str, str, list[str]]] = []
        self.data: dict[tuple[str, str], dict[str, Any]] = {}
        self.label_rows: dict[str, list[tuple[str, str, Any]]] = {}
        self.neighbors: dict[tuple[str, str], list[str]] = {}
        self.nodes = _FakeNodes(self)

    async def close(self) -> None:  # pragma: no cover - never called in tests
        pass


class _FakePool:
    def __init__(self, client: _FakeShardClient) -> None:
        self.client = client
        self.acquired = 0
        self.released = 0

    async def acquire(self) -> _FakeShardClient:
        self.acquired += 1
        return self.client

    def release(self, client: _FakeShardClient) -> None:
        self.released += 1


def _router_with_fakes(
    endpoints: list[str],
    shards: dict[str, _FakeShardClient],
    affinity: AffinityRegistry | None = None,
) -> ShardRouter:
    router = ShardRouter(endpoints, affinity=affinity)
    router.pools = {ep: _FakePool(shards[ep]) for ep in endpoints}  # type: ignore[assignment]
    return router


# ── (b) scatter-gather groups by shard, fans out, merges + dedupes ───────────


@pytest.mark.asyncio
async def test_group_by_shard_partitions() -> None:
    # No affinity → graphs scatter by HRW across endpoints.
    eps = ["tcp://a:1", "tcp://b:1", "tcp://c:1"]
    router = ShardRouter(eps, affinity=AffinityRegistry(groups={}))
    graphs = ["g1", "g2", "g3", "g4", "g5", "g6"]
    groups = router.group_by_shard(graphs)
    # Every graph accounted for exactly once.
    flat = [g for grp in groups.values() for g in grp]
    assert sorted(flat) == sorted(graphs)
    # Multi-shard spread (probabilistically; 6 graphs over 3 shards).
    assert len(groups) >= 2


@pytest.mark.asyncio
async def test_properties_union_first_found_across_shards() -> None:
    eps = ["tcp://a:1", "tcp://b:1"]
    shard_a = _FakeShardClient()
    shard_b = _FakeShardClient()
    shards = {eps[0]: shard_a, eps[1]: shard_b}
    # Disjoint affinity groups so the two graphs land on different shards.
    affinity = AffinityRegistry(groups={"ga": "ga", "gb": "gb"})
    router = _router_with_fakes(eps, shards, affinity)

    ep_a = router._get_shard_endpoint("ga")
    ep_b = router._get_shard_endpoint("gb")
    assert ep_a != ep_b  # genuinely cross-shard

    # Both shards have node B; earliest graph in caller order must win.
    shards[ep_a].data[("ga", "B")] = {"src": "a"}
    shards[ep_b].data[("gb", "B")] = {"src": "b"}

    out = await router.properties_union("B", ["ga", "gb"])
    assert out == {"src": "a"}

    # Reverse order flips the winner.
    out2 = await router.properties_union("B", ["gb", "ga"])
    assert out2 == {"src": "b"}

    # Only the owning graph found → that one wins.
    out3 = await router.properties_union("B", ["gb"])
    assert out3 == {"src": "b"}


@pytest.mark.asyncio
async def test_label_union_merges_and_dedupes_across_shards() -> None:
    eps = ["tcp://a:1", "tcp://b:1"]
    shard_a = _FakeShardClient()
    shard_b = _FakeShardClient()
    shards = {eps[0]: shard_a, eps[1]: shard_b}
    affinity = AffinityRegistry(groups={"ga": "ga", "gb": "gb"})
    router = _router_with_fakes(eps, shards, affinity)

    ep_a = router._get_shard_endpoint("ga")
    ep_b = router._get_shard_endpoint("gb")
    assert ep_a != ep_b

    shards[ep_a].label_rows["ga"] = [
        ("A", "Doc", {"s": "a"}),
        ("shared", "Doc", {"s": "a"}),
    ]
    shards[ep_b].label_rows["gb"] = [
        ("shared", "Doc", {"s": "b"}),  # duplicate id across shards
        ("C", "Doc", {"s": "b"}),
    ]

    out = await router.list_by_label_union("Doc", ["ga", "gb"])
    ids = [nid for nid, _ in out]
    assert sorted(ids) == ["A", "C", "shared"]
    assert ids.count("shared") == 1  # deduped across shards
    # First-found wins for the dup (ga ordered before gb).
    props = dict(out)
    assert props["shared"] == {"s": "a"}


@pytest.mark.asyncio
async def test_label_union_respects_global_limit() -> None:
    eps = ["tcp://a:1", "tcp://b:1"]
    shards = {eps[0]: _FakeShardClient(), eps[1]: _FakeShardClient()}
    affinity = AffinityRegistry(groups={"ga": "ga", "gb": "gb"})
    router = _router_with_fakes(eps, shards, affinity)
    ep_a = router._get_shard_endpoint("ga")
    ep_b = router._get_shard_endpoint("gb")
    shards[ep_a].label_rows["ga"] = [(f"a{i}", "Doc", {}) for i in range(5)]
    shards[ep_b].label_rows["gb"] = [(f"b{i}", "Doc", {}) for i in range(5)]
    out = await router.list_by_label_union("Doc", ["ga", "gb"], limit=3)
    assert len(out) == 3


@pytest.mark.asyncio
async def test_neighbors_union_dedupes_across_shards() -> None:
    eps = ["tcp://a:1", "tcp://b:1"]
    shards = {eps[0]: _FakeShardClient(), eps[1]: _FakeShardClient()}
    affinity = AffinityRegistry(groups={"ga": "ga", "gb": "gb"})
    router = _router_with_fakes(eps, shards, affinity)
    ep_a = router._get_shard_endpoint("ga")
    ep_b = router._get_shard_endpoint("gb")
    shards[ep_a].neighbors[("ga", "X")] = ["n1", "n2"]
    shards[ep_b].neighbors[("gb", "X")] = ["n2", "n3"]  # n2 dup
    out = await router.neighbors_union("X", ["ga", "gb"])
    assert sorted(out) == ["n1", "n2", "n3"]
    assert out.count("n2") == 1


@pytest.mark.asyncio
async def test_fan_out_groups_each_shard_once() -> None:
    # Two ingest lanes co-resident with commons (one shard) + one off-shard graph.
    eps = ["tcp://a:1", "tcp://b:1", "tcp://c:1"]
    shards = {ep: _FakeShardClient() for ep in eps}
    router = _router_with_fakes(eps, shards)  # default affinity

    graphs = ["__commons__", "__ingest__", "__ingest_news__", "agent:planner"]
    groups = router.group_by_shard(graphs)
    # Commons + 2 lanes collapse to ONE group; planner is its own.
    commons_ep = router._get_shard_endpoint("__commons__")
    assert set(groups[commons_ep]) == {"__commons__", "__ingest__", "__ingest_news__"}

    await router.list_by_label_union("Doc", graphs)
    # Each touched shard's union RPC called exactly once with its full slice.
    for ep, grp in groups.items():
        assert shards[ep].calls == [("label", "Doc", grp)]


# ── (c) single-endpoint case behaves identically to today ────────────────────


@pytest.mark.asyncio
async def test_single_endpoint_single_call() -> None:
    ep = "tcp://only:1"
    shard = _FakeShardClient()
    router = _router_with_fakes([ep], {ep: shard})

    graphs = ["__commons__", "__ingest__", "__ingest_news__", "other"]
    # Everything co-resides trivially on the single shard → one group.
    assert router.group_by_shard(graphs) == {ep: graphs}

    shard.label_rows["__commons__"] = [("A", "Doc", {})]
    shard.neighbors[("__commons__", "X")] = ["n1"]
    shard.data[("__commons__", "B")] = {"ok": True}

    labels = await router.list_by_label_union("Doc", graphs)
    nbrs = await router.neighbors_union("X", graphs)
    props = await router.properties_union("B", graphs)

    assert labels == [("A", {})]
    assert nbrs == ["n1"]
    assert props == {"ok": True}
    # Exactly one union RPC per call (single shard, full graph list each time).
    assert shard.calls == [
        ("label", "Doc", graphs),
        ("neighbors", "X", graphs),
        ("properties", "B", graphs),
    ]
