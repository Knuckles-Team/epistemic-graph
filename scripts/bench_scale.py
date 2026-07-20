#!/usr/bin/env python3
"""Multi-shard scale harness for epistemic-graph (CONCEPT:AU-KG.query.vendor-agnostic-traversal P3).

Demonstrates **server-tier linear scaling** and measures the per-agent memory
footprint, then turns the marketed "100,000,000 concurrent agents" into a
*measured* extrapolation.

Method (to isolate server scaling from a single client's ceiling):
  * Keep ``--agents-per-shard`` FIXED and grow the shard count S. If the server
    tier scales, total throughput grows ∝ S at ~constant wall-time.
  * One driver **process** per shard (not one asyncio loop for all) so the client
    is parallelised across cores and never the bottleneck.
  * Each agent works a *bounded subgraph* — its own tenant graph of
    ``--nodes-per-agent`` nodes + an edge chain.
  * RSS is the **delta over idle baseline** (graph data only, not fixed binary
    overhead), so per-agent footprint is the marginal cost.

This never claims 100M was *run*. It reports what was measured (throughput,
per-agent RSS) and the arithmetic that follows: per-agent RSS ÷ per-host RAM →
agents/host → hosts for 100M.

Run: python3 scripts/bench_scale.py --shards 1,2,4 --agents-per-shard 60
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import multiprocessing as mp
import os
import subprocess
import tempfile
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
RELEASE = REPO / "target" / "release" / "epistemic-graph-server"
DEBUG = REPO / "target" / "debug" / "epistemic-graph-server"


def _server_bin() -> tuple[Path, str]:
    if RELEASE.exists():
        return RELEASE, "release"
    if DEBUG.exists():
        return DEBUG, "debug"
    raise SystemExit(
        "server binary missing; build with `cargo build --release --features server`"
    )


def _rss_kb(pid: int) -> int:
    try:
        for line in Path(f"/proc/{pid}/status").read_text().splitlines():
            if line.startswith("VmRSS:"):
                return int(line.split()[1])  # kB
    except Exception:  # noqa: BLE001
        pass
    return 0


def _spawn_shards(binary: Path, n: int, tmp: str) -> list[subprocess.Popen]:
    procs = []
    for i in range(n):
        sock = os.path.join(tmp, f"shard{i}.sock")
        p = subprocess.Popen(
            [str(binary), "--socket-path", sock],
            env={
                **os.environ,
                # Benchmarks measure pure transport: run unauthenticated,
                # which now requires the explicit insecure opt-out.
                "GRAPH_SERVICE_AUTH_SECRET": "",
                "EPISTEMIC_GRAPH_ALLOW_INSECURE": "1",
            },
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        p._sock = sock  # type: ignore[attr-defined]
        procs.append(p)
    for p in procs:
        for _ in range(200):
            if os.path.exists(p._sock):  # type: ignore[attr-defined]
                break
            time.sleep(0.05)
    return procs


async def _run_agent(sock: str, agent: str, nodes: int) -> int:
    from epistemic_graph.client import EpistemicGraphClient

    client = await EpistemicGraphClient.connect(
        socket_path=sock, graph_name=agent, auth_secret=""
    )
    ops = 0
    try:
        await client.tenants.create(agent)
        for i in range(nodes):
            await client.nodes.add(f"{agent}:n{i}", {"i": i, "label": "agent_node"})
            ops += 1
        for i in range(nodes - 1):
            await client.edges.add(
                f"{agent}:n{i}", f"{agent}:n{i + 1}", {"type": "NEXT"}
            )
            ops += 1
    finally:
        await client.close()
    return ops


def _driver_proc(sock, lo, hi, nodes, concurrency, out_q):
    """A dedicated client process saturating one shard with agents [lo, hi)."""

    async def _drive():
        sem = asyncio.Semaphore(concurrency)

        async def _one(idx):
            async with sem:
                return await _run_agent(sock, f"agent:{idx}", nodes)

        t0 = time.perf_counter()
        counts = await asyncio.gather(*[_one(i) for i in range(lo, hi)])
        return sum(counts), time.perf_counter() - t0

    ops, wall = asyncio.run(_drive())
    out_q.put((ops, wall))


def _bench(binary, shards, per_shard, nodes, concurrency) -> dict:
    agents = per_shard * shards
    with tempfile.TemporaryDirectory() as tmp:
        procs = _spawn_shards(binary, shards, tmp)
        try:
            time.sleep(0.3)
            baseline_rss = sum(_rss_kb(p.pid) for p in procs)
            out_q: mp.Queue = mp.Queue()
            drivers = []
            for s in range(shards):
                lo, hi = s * per_shard, (s + 1) * per_shard
                d = mp.Process(
                    target=_driver_proc,
                    args=(procs[s]._sock, lo, hi, nodes, concurrency, out_q),  # type: ignore[attr-defined]
                )
                d.start()
                drivers.append(d)
            results = [out_q.get() for _ in drivers]
            for d in drivers:
                d.join()
            total_ops = sum(o for o, _ in results)
            max_wall = max(w for _, w in results)
            rss_after = sum(_rss_kb(p.pid) for p in procs)
            data_rss = max(0, rss_after - baseline_rss)
            return {
                "shards": shards,
                "agents": agents,
                "nodes_per_agent": nodes,
                "total_ops": total_ops,
                "wall_s": round(max_wall, 3),
                "ops_per_sec": round(total_ops / max_wall, 1) if max_wall else 0.0,
                "data_rss_mb": round(data_rss / 1024, 1),
                "per_agent_rss_kb": round(data_rss / agents, 1) if agents else 0.0,
            }
        finally:
            for p in procs:
                p.terminate()
            for p in procs:
                try:
                    p.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    p.kill()


def _extrapolate(per_agent_rss_kb: float, ram_budget_gb: float, target: int) -> dict:
    if per_agent_rss_kb <= 0:
        return {}
    agents_per_host = int((ram_budget_gb * 1024 * 1024) / per_agent_rss_kb)
    hosts = math.ceil(target / agents_per_host) if agents_per_host else None
    return {
        "ram_budget_gb": ram_budget_gb,
        "per_agent_rss_kb": per_agent_rss_kb,
        "agents_per_host": agents_per_host,
        "target_agents": target,
        "hosts_required": hosts,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--shards", default="1,2,4", help="comma-separated shard counts")
    ap.add_argument("--agents-per-shard", type=int, default=60)
    ap.add_argument("--nodes-per-agent", type=int, default=40)
    ap.add_argument("--concurrency", type=int, default=16)
    ap.add_argument("--ram-budget-gb", type=float, default=64.0)
    ap.add_argument("--target-agents", type=int, default=100_000_000)
    ap.add_argument("--json", default=None)
    args = ap.parse_args()

    binary, build = _server_bin()
    shard_counts = [int(s) for s in args.shards.split(",") if s.strip()]
    rows = [
        _bench(binary, s, args.agents_per_shard, args.nodes_per_agent, args.concurrency)
        for s in shard_counts
    ]
    base = next((r for r in rows if r["shards"] == 1), rows[0])
    top = rows[-1]
    speedup = (
        round(top["ops_per_sec"] / base["ops_per_sec"], 2)
        if base["ops_per_sec"] else None
    )
    # per-agent RSS: median across runs (stable, ignores per-run noise).
    rss_vals = sorted(r["per_agent_rss_kb"] for r in rows if r["per_agent_rss_kb"] > 0)
    per_agent = rss_vals[len(rss_vals) // 2] if rss_vals else 0.0
    extrap = _extrapolate(per_agent, args.ram_budget_gb, args.target_agents)
    res = {
        "build": build,
        "agents_per_shard": args.agents_per_shard,
        "nodes_per_agent": args.nodes_per_agent,
        "rows": rows,
        "scaling": {
            "from_shards": base["shards"],
            "to_shards": top["shards"],
            "throughput_speedup": speedup,
            "linear_ideal": round(top["shards"] / base["shards"], 2),
        },
        "extrapolation": extrap,
    }

    print(f"epistemic-graph scale harness ({build} build, fixed {args.agents_per_shard} agents/shard)")
    print(f"  {'shards':>6} {'agents':>7} {'ops/s':>10} {'wall_s':>7} {'dataRSS_MB':>11} {'RSS/agent_kB':>13}")
    for r in rows:
        print(
            f"  {r['shards']:>6} {r['agents']:>7} {r['ops_per_sec']:>10} "
            f"{r['wall_s']:>7} {r['data_rss_mb']:>11} {r['per_agent_rss_kb']:>13}"
        )
    sc = res["scaling"]
    print(
        f"  scaling {sc['from_shards']}→{sc['to_shards']} shards: "
        f"{sc['throughput_speedup']}× throughput (linear ideal {sc['linear_ideal']}×)"
    )
    if extrap:
        print(
            f"  extrapolation: {per_agent} kB/agent → ~{extrap['agents_per_host']:,} "
            f"agents/host @ {extrap['ram_budget_gb']}GB → {extrap['hosts_required']:,} "
            f"hosts for {extrap['target_agents']:,} agents"
        )
    if args.json:
        Path(args.json).write_text(json.dumps(res, indent=2))
        print(f"  wrote {args.json}")


if __name__ == "__main__":
    main()
