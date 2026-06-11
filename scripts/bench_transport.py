#!/usr/bin/env python3
"""Measure real AddNode / GetNodeProperties latency over UDS.

Replaces marketed latency claims with measured numbers (Plan 01 acceptance).
Spawns the release server on a private socket, drives N ops, and prints
p50/p99. Run: python3 scripts/bench_transport.py [--ops 5000]
"""

from __future__ import annotations

import argparse
import asyncio
import os
import statistics
import subprocess
import tempfile
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
SERVER = REPO / "target" / "release" / "epistemic-graph-server"


async def _run(ops: int) -> dict[str, float]:
    from epistemic_graph.client import EpistemicGraphClient

    sock = os.path.join(tempfile.mkdtemp(), "bench.sock")
    proc = subprocess.Popen(
        [str(SERVER), "--socket-path", sock],
        env={
                **os.environ,
                # Benchmarks measure pure transport: run unauthenticated,
                # which now requires the explicit insecure opt-out.
                "GRAPH_SERVICE_AUTH_SECRET": "",
                "EPISTEMIC_GRAPH_ALLOW_INSECURE": "1",
            },
    )
    try:
        # Wait for the socket to appear.
        for _ in range(100):
            if os.path.exists(sock):
                break
            await asyncio.sleep(0.05)

        client = await EpistemicGraphClient.connect(socket_path=sock, graph_name="bench")
        await client.tenants.create("bench")

        add_lat: list[float] = []
        get_lat: list[float] = []
        for i in range(ops):
            nid = f"n{i}"
            t0 = time.perf_counter()
            await client.nodes.add(nid, {"i": i, "label": "bench"})
            add_lat.append((time.perf_counter() - t0) * 1e3)

            t0 = time.perf_counter()
            await client.nodes.properties(nid)
            get_lat.append((time.perf_counter() - t0) * 1e3)

        await client.close()

        def pct(xs: list[float], p: float) -> float:
            xs = sorted(xs)
            k = max(0, min(len(xs) - 1, int(round(p / 100 * (len(xs) - 1)))))
            return xs[k]

        return {
            "ops": ops,
            "add_p50_ms": statistics.median(add_lat),
            "add_p99_ms": pct(add_lat, 99),
            "get_p50_ms": statistics.median(get_lat),
            "get_p99_ms": pct(get_lat, 99),
        }
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ops", type=int, default=5000)
    args = ap.parse_args()
    if not SERVER.exists():
        raise SystemExit(
            f"server binary missing at {SERVER}; build with "
            "`cargo build --release --features server`"
        )
    res = asyncio.run(_run(args.ops))
    print("epistemic-graph UDS transport benchmark")
    print(f"  ops:        {res['ops']}")
    print(f"  AddNode     p50={res['add_p50_ms']:.3f}ms  p99={res['add_p99_ms']:.3f}ms")
    print(f"  GetNodeProps p50={res['get_p50_ms']:.3f}ms  p99={res['get_p99_ms']:.3f}ms")


if __name__ == "__main__":
    main()
