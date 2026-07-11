#!/usr/bin/env python3
"""SCALE-P2-1 real-engine soak/chaos runner — RUNS the workload against the actual
Rust ``epistemic-graph-server`` binary (not a mock), then injects LOCAL faults.

This is the honest, single-box companion to agent-utilities'
``scripts/scale/loadgen.py`` (the SCALE-P2-1 harness): that harness's ``--engine
live`` path is CI-marked "not exercised in CI" and its 1,000,000-resident soak is
documented as ``tests/scale/soak/test_hardware_pending.py`` — real, but never run.
This script closes that gap AT ENGINE SCOPE: it drives the real server binary with
a scaled-down analogue of ``agent-utilities/docs/scaling/workload_contract.yml``
(same population/rate axes, same tenant Zipf+elephant skew, same SLO percentile
targets — the constants below are transcribed from that file, dated 2026-07,
cite it in any future update) and reports MEASURED numbers only.

Scope (read before trusting a number):

* This measures the ENGINE's native write/read/claim primitives (``AddNode``,
  ``GetNodeProperties``, ``ClaimNext``, ``CompareAndSetNodeFields``) over the real
  wire protocol against a real process. It does NOT invoke agent-utilities'
  ``WorkItem``/``AgentBus`` Python orchestration layer (a different package/repo) —
  the ``queue_latency_ms``/``end_to_end_latency_ms`` SLO axes are approximated with
  the engine-native ``ClaimNext`` claim-queue primitive, not the full dispatch stack.
* Tenant skew for the mutation/tool-call axes is the FULL Zipf-over-``tenant_count``
  distribution from the contract. Tenant skew for the turn/claim axis is
  collapsed to 3 tiers (elephant / hot-tail / ordinary-tail) to keep per-poll
  ``ClaimNext`` label-scan overhead bounded on one box — documented, not hidden.
* Population size, duration, and node-per-resident counts are chosen to complete
  in bounded minutes on ONE box, not the real 1,000,000-resident/24-72h contract —
  every run prints (and the report records) exactly what scale/duration was used.
* Chaos here is what a single box can honestly inject: backpressure/admission
  shedding, process restart / cold (redb) recovery, hot-tenant noisy-neighbor
  pressure, and per-graph eviction + durable read-through. Multi-node scenarios
  (leader failover across hosts, zone loss, cross-host rebalance, a real Kafka
  rebalance) are OUT OF SCOPE here — see docs/benchmarks-soak.md's "NOT run here"
  section for the exact command to run them on the real cluster.

Usage::

    cargo build --release --features server   # once
    python3 scripts/soak_scale.py --residents 50000 --duration-s 60 \\
        --json /tmp/soak-report.json
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import statistics
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import msgpack

REPO = Path(__file__).resolve().parents[1]

# --------------------------------------------------------------------------- #
# Workload contract constants — transcribed from agent-utilities'
# docs/scaling/workload_contract.yml (v1, 2026-07). SLO targets and the skew
# SHAPE never scale; population/rate axes scale by `residents / REFERENCE_*`.
# --------------------------------------------------------------------------- #

REFERENCE_REGISTERED_AGENTS = 1_000_000
REFERENCE_TENANT_COUNT = 5_000
SKEW_EXPONENT = 1.0
ELEPHANT_RESIDENTS_FRACTION = 0.05
ELEPHANT_ACTIVE_FRACTION = 0.10

REFERENCE_TURNS_PER_SEC = 167.0
REFERENCE_TOOL_CALLS_PER_SEC = 20_000.0
REFERENCE_MUTATIONS_PER_SEC = 40_000.0

SLO_MS: dict[str, dict[str, float]] = {
    "queue_latency_ms": {"p50": 50, "p95": 500, "p99": 2_000, "p99_9": 5_000},
    "query_latency_ms": {"p50": 2, "p95": 10, "p99": 30, "p99_9": 150},
    "write_latency_ms": {"p50": 1, "p95": 5, "p99": 20, "p99_9": 100},
    "end_to_end_latency_ms": {"p50": 2_000, "p95": 8_000, "p99": 20_000, "p99_9": 45_000},
}

_MUTATION_POOL_PER_TENANT = 200  # bounded key pool per tenant (avoids unbounded node growth)


def _server_bin() -> Path:
    env_target = os.environ.get("CARGO_TARGET_DIR")
    candidates = []
    if env_target:
        candidates.append(Path(env_target) / "release" / "epistemic-graph-server")
    candidates.append(REPO / "target" / "release" / "epistemic-graph-server")
    for c in candidates:
        if c.exists():
            return c
    raise SystemExit(
        "server binary missing; build with `cargo build --release --features server` "
        "(set CARGO_TARGET_DIR if you built out-of-tree)"
    )


def _rss_kb(pid: int) -> int:
    try:
        for line in Path(f"/proc/{pid}/status").read_text().splitlines():
            if line.startswith("VmRSS:"):
                return int(line.split()[1])
    except Exception:  # noqa: BLE001
        pass
    return 0


# --------------------------------------------------------------------------- #
# Server process management
# --------------------------------------------------------------------------- #


@dataclass
class ServerHandle:
    proc: subprocess.Popen
    sock: str
    persist_dir: str | None

    def rss_kb(self) -> int:
        return _rss_kb(self.proc.pid)

    def stop(self, *, graceful: bool = True, timeout: float = 20.0) -> None:
        if self.proc.poll() is not None:
            return
        if graceful:
            self.proc.terminate()
        else:
            self.proc.kill()
        try:
            self.proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait(timeout=5)


def start_server(
    binary: Path,
    sock: str,
    *,
    persist_dir: str | None = None,
    env_overrides: dict[str, str] | None = None,
    wait_s: float = 15.0,
) -> ServerHandle:
    env = {
        **os.environ,
        # Soak measures engine behavior, not auth — run unauthenticated like the
        # existing scripts/bench_*.py convention (explicit insecure opt-out).
        "GRAPH_SERVICE_AUTH_SECRET": "",
        "EPISTEMIC_GRAPH_ALLOW_INSECURE": "1",
    }
    if persist_dir:
        env["GRAPH_SERVICE_PERSIST_DIR"] = persist_dir
    if env_overrides:
        env.update(env_overrides)
    if os.path.exists(sock):
        os.remove(sock)
    proc = subprocess.Popen(
        [str(binary), "--socket-path", sock],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    deadline = time.monotonic() + wait_s
    while time.monotonic() < deadline:
        if os.path.exists(sock):
            break
        if proc.poll() is not None:
            raise RuntimeError(f"server exited early (code {proc.returncode}) before binding {sock}")
        time.sleep(0.05)
    else:
        proc.kill()
        raise RuntimeError(f"server never bound {sock} within {wait_s}s")
    # Give the listener a brief moment to actually accept() after the file appears.
    time.sleep(0.2)
    return ServerHandle(proc=proc, sock=sock, persist_dir=persist_dir)


# --------------------------------------------------------------------------- #
# Thin wire wrappers that target an EXPLICIT graph per call (the client's public
# NodeClient wrappers are pinned to the connection's bound graph_name; the wire
# protocol itself supports an explicit per-call `graph` override — see
# EpistemicGraphClient._send — which is exactly what multi-tenant routing over
# ONE pooled connection needs).
# --------------------------------------------------------------------------- #


async def add_node(conn: Any, graph: str, node_id: str, props: dict[str, Any]) -> None:
    await conn._send(
        "AddNode",
        {"node_id": node_id, "properties_msgpack": list(msgpack.packb(props))},
        graph=graph,
    )


async def get_node_properties(conn: Any, graph: str, node_id: str) -> dict[str, Any] | None:
    raw = await conn._send("GetNodeProperties", {"node_id": node_id}, graph=graph)
    if raw is None:
        return None
    if isinstance(raw, bytes):
        return msgpack.unpackb(raw, raw=False)
    return raw


async def claim_next(
    conn: Any, graph: str, label: str, updates: dict[str, Any]
) -> tuple[str, dict[str, Any]] | None:
    raw = await conn._send(
        "ClaimNext",
        {"label": label, "updates_msgpack": list(msgpack.packb(updates))},
        graph=graph,
    )
    if isinstance(raw, bytes):
        raw = msgpack.unpackb(raw, raw=False)
    if not raw:
        return None
    return raw[0], raw[1]


async def create_graph(conn: Any, graph: str, graph_type: str = "Agent") -> None:
    # Explicit graph= (matching graph_name) rather than the MultiTenantClient
    # wrapper, which sends the connection's bound graph_name for routing —
    # this keeps routing and the graph being created unambiguously the same.
    await conn._send("CreateGraph", {"graph_name": graph, "graph_type": graph_type}, graph=graph)


async def compare_and_set(
    conn: Any, graph: str, node_id: str, conditions: dict[str, Any], updates: dict[str, Any]
) -> bool:
    return await conn._send(
        "CompareAndSetNodeFields",
        {
            "node_id": node_id,
            "conditions_msgpack": list(msgpack.packb(conditions)),
            "updates_msgpack": list(msgpack.packb(updates)),
        },
        graph=graph,
    )


# --------------------------------------------------------------------------- #
# Tenant plan (Zipf + elephant) — mirrors agent-utilities'
# docs/scaling/workload_contract.py TenantPlan/ScaledWorkload exactly (same
# formulas), reimplemented here so this script has no cross-repo dependency.
# --------------------------------------------------------------------------- #


@dataclass
class TenantPlan:
    ids: list[str]
    weights: list[float]  # normalized, sums to 1.0 — used for BOTH mutation + tool-call axes
    residents: list[int]  # resident count assigned to each tenant (population build)
    elephant_id: str

    def sample(self, rng: random.Random) -> str:
        return rng.choices(self.ids, weights=self.weights, k=1)[0]


def build_tenant_plan(tenant_count: int, residents: int) -> TenantPlan:
    elephant_id = "tenant-elephant"
    ordinary_ids = [f"tenant-{i}" for i in range(tenant_count - 1)]
    ids = [elephant_id] + ordinary_ids

    ordinary_raw = [1.0 / ((i + 1) ** SKEW_EXPONENT) for i in range(len(ordinary_ids))]
    raw_sum = sum(ordinary_raw) or 1.0
    remaining_active = max(0.0, 1.0 - ELEPHANT_ACTIVE_FRACTION)
    weights = [ELEPHANT_ACTIVE_FRACTION] + [
        remaining_active * (w / raw_sum) for w in ordinary_raw
    ]

    elephant_residents = max(1, round(residents * ELEPHANT_RESIDENTS_FRACTION))
    remaining_residents = max(0, residents - elephant_residents)
    resident_counts = [elephant_residents]
    if ordinary_ids:
        for w in ordinary_raw:
            resident_counts.append(max(1, round(remaining_residents * (w / raw_sum))))
    return TenantPlan(ids=ids, weights=weights, residents=resident_counts, elephant_id=elephant_id)


# --------------------------------------------------------------------------- #
# Percentile helpers
# --------------------------------------------------------------------------- #


def _pct(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    idx = min(len(s) - 1, int(round(q * (len(s) - 1))))
    return s[idx]


def _percentiles_ms(values_s: list[float]) -> dict[str, float]:
    ms = [v * 1000.0 for v in values_s]
    return {
        "p50": round(_pct(ms, 0.50), 3),
        "p95": round(_pct(ms, 0.95), 3),
        "p99": round(_pct(ms, 0.99), 3),
        "p99_9": round(_pct(ms, 0.999), 3),
        "n": len(ms),
    }


def _slo_pass(measured: dict[str, float], target: dict[str, float]) -> dict[str, bool]:
    return {k: measured[k] <= target[k] for k in ("p50", "p95", "p99", "p99_9")}


# --------------------------------------------------------------------------- #
# Connection pool — a handful of persistent connections, each pipelining many
# concurrent in-flight requests (EG-KG.backend.framed-response); NOT one
# connection per tenant/resident.
# --------------------------------------------------------------------------- #


async def open_pool(sock: str, n: int) -> list[Any]:
    from epistemic_graph.client import EpistemicGraphClient

    conns = []
    for i in range(n):
        c = await EpistemicGraphClient.connect(
            socket_path=sock, graph_name="__soak__", auth_secret=""
        )
        conns.append(c)
    return conns


async def close_pool(conns: list[Any]) -> None:
    for c in conns:
        try:
            await c.close()
        except Exception:  # noqa: BLE001
            pass


# --------------------------------------------------------------------------- #
# Phase A: population build + steady-state mixed workload
# --------------------------------------------------------------------------- #


@dataclass
class Metrics:
    write_latency_s: list[float] = field(default_factory=list)
    query_latency_s: list[float] = field(default_factory=list)
    queue_latency_s: list[float] = field(default_factory=list)
    end_to_end_latency_s: list[float] = field(default_factory=list)
    turns_submitted: int = 0
    turns_succeeded: int = 0
    submit_ts: dict[str, float] = field(default_factory=dict)


async def populate(
    conns: list[Any], tenants: TenantPlan, nodes_per_resident: int, concurrency: int
) -> dict[str, Any]:
    sem = asyncio.Semaphore(concurrency)
    total_nodes = 0
    t0 = time.monotonic()

    await asyncio.gather(*(create_graph(conns[i % len(conns)], t) for i, t in enumerate(tenants.ids)))

    async def add_resident(conn: Any, graph: str, ridx: int) -> int:
        n = 0
        for k in range(nodes_per_resident):
            async with sem:
                await add_node(
                    conn, graph, f"{graph}:r{ridx}:n{k}", {"label": "Resident", "i": k}
                )
            n += 1
        return n

    tasks = []
    ci = 0
    for graph, count in zip(tenants.ids, tenants.residents):
        for r in range(count):
            tasks.append(add_resident(conns[ci % len(conns)], graph, r))
            ci += 1
    counts = await asyncio.gather(*tasks)
    total_nodes = sum(counts)
    wall = time.monotonic() - t0
    return {
        "residents": sum(tenants.residents),
        "tenants": len(tenants.ids),
        "nodes_per_resident": nodes_per_resident,
        "total_nodes": total_nodes,
        "wall_s": round(wall, 3),
        "ops_per_sec": round(total_nodes / wall, 1) if wall else 0.0,
    }


async def _mutation_producer(
    conn: Any, tenants: TenantPlan, metrics: Metrics, rate: float, stop_at: float, rng: random.Random
) -> None:
    n = 0
    while time.monotonic() < stop_at:
        tenant = tenants.sample(rng)
        key = f"{tenant}:mutation:{n % _MUTATION_POOL_PER_TENANT}"
        t0 = time.perf_counter()
        await add_node(conn, tenant, key, {"label": "SoakMutation", "i": n})
        metrics.write_latency_s.append(time.perf_counter() - t0)
        n += 1
        await asyncio.sleep(rng.expovariate(max(rate, 0.001)))


async def _tool_call_producer(
    conn: Any, tenants: TenantPlan, metrics: Metrics, rate: float, stop_at: float, rng: random.Random
) -> None:
    while time.monotonic() < stop_at:
        tenant = tenants.sample(rng)
        # Read back one of the resident nodes populated for this tenant.
        node_id = f"{tenant}:r0:n0"
        t0 = time.perf_counter()
        await get_node_properties(conn, tenant, node_id)
        metrics.query_latency_s.append(time.perf_counter() - t0)
        await asyncio.sleep(rng.expovariate(max(rate, 0.001)))


async def _turn_producer(
    conn: Any,
    turn_tiers: list[tuple[str, float]],
    metrics: Metrics,
    rate: float,
    stop_at: float,
    rng: random.Random,
) -> None:
    """Submits a SoakWorkItem into one of 3 tiers (elephant/hot-tail/ordinary-tail)."""
    n = 0
    tiers = [t for t, _ in turn_tiers]
    weights = [w for _, w in turn_tiers]
    while time.monotonic() < stop_at:
        tier = rng.choices(tiers, weights=weights, k=1)[0]
        item_id = f"{tier}:turn:{n}"
        n += 1
        submit_ts = time.monotonic()
        await add_node(
            conn, tier, item_id, {"label": "SoakWorkItem", "status": "pending", "seq": n}
        )
        metrics.submit_ts[item_id] = submit_ts
        metrics.turns_submitted += 1
        await asyncio.sleep(rng.expovariate(max(rate, 0.001)))


async def _turn_worker(
    conn: Any,
    tiers: list[str],
    metrics: Metrics,
    turn_duration_s: float,
    stop_at: float,
    drain_grace_s: float,
) -> None:
    # Gentle poll interval + a SINGLE assigned tier per worker: ClaimNext takes
    # the per-graph write guard on every call, so N workers each scanning ALL
    # tiers on a tight loop turns idle polling into a self-inflicted write-lock
    # storm that inflates the concurrent mutation-write p50 (observed while
    # building this harness — a measurement artifact, not a real system property).
    poll_interval_s = 0.025
    hard_stop = stop_at + drain_grace_s
    while time.monotonic() < hard_stop:
        claimed = None
        for tier in tiers:
            claimed = await claim_next(
                conn, tier, "SoakWorkItem", {"status": "running", "claimed_ts": time.time()}
            )
            if claimed is not None:
                break
        if claimed is None:
            await asyncio.sleep(poll_interval_s)
            continue
        item_id, props = claimed
        t_claim = time.monotonic()
        submit_ts = metrics.submit_ts.get(item_id)
        if submit_ts is not None:
            metrics.queue_latency_s.append(t_claim - submit_ts)
        if turn_duration_s > 0:
            await asyncio.sleep(turn_duration_s)
        tier = item_id.rsplit(":turn:", 1)[0]
        ok = await compare_and_set(
            conn, tier, item_id, {"status": "running"}, {"status": "succeeded"}
        )
        if ok:
            metrics.turns_succeeded += 1
            if submit_ts is not None:
                metrics.end_to_end_latency_s.append(time.monotonic() - submit_ts)


async def run_steady_state(
    conns: list[Any],
    tenants: TenantPlan,
    *,
    duration_s: float,
    turns_per_sec: float,
    tool_calls_per_sec: float,
    mutations_per_sec: float,
    num_turn_workers: int,
    turn_duration_s: float,
    seed: int,
) -> dict[str, Any]:
    rng = random.Random(seed)
    metrics = Metrics()
    start = time.monotonic()
    stop_at = start + duration_s
    n_conns = len(conns)

    # 3-tier turn skew (see module docstring): elephant tenant's own graph, one
    # "hot-tail" graph modeling the next few zipf-heavy ordinary tenants, one
    # "ordinary-tail" graph for the long tail. Fall back to whatever tenant graphs
    # actually exist at very small scales (< 3 tenants) so a tiny dev run still works.
    ordinary = [t for t in tenants.ids if t != tenants.elephant_id]
    hot_tail = ordinary[0] if ordinary else tenants.elephant_id
    long_tail = ordinary[1] if len(ordinary) > 1 else hot_tail
    turn_tiers = [
        (tenants.elephant_id, ELEPHANT_ACTIVE_FRACTION),
        (hot_tail, 0.5 * (1.0 - ELEPHANT_ACTIVE_FRACTION)),
        (long_tail, 0.5 * (1.0 - ELEPHANT_ACTIVE_FRACTION)),
    ]
    # Dedup tier names preserving order (small scales can collapse tiers) and ensure
    # each tier graph exists (populate created a graph per tenant, but guard anyway).
    tier_names: list[str] = []
    for t, _ in turn_tiers:
        if t not in tier_names:
            tier_names.append(t)
    for i, t in enumerate(tier_names):
        try:
            await create_graph(conns[i % n_conns], t)
        except RuntimeError:
            pass  # already exists

    # Fan the mutation + tool-call axes across SEVERAL pooled connections so the
    # target rate is driven by real pipelined concurrency (a single-connection
    # producer's sequential awaits cap throughput at ~1/latency, which would make
    # the driver — not the engine — the bottleneck). Each producer targets its
    # share (rate / n_producers) of the axis rate.
    n_mut = max(1, min(n_conns // 2, 8))
    n_tool = max(1, min(n_conns // 4, 4))
    producers: list[Any] = []
    for i in range(n_mut):
        producers.append(
            _mutation_producer(
                conns[i % n_conns], tenants, metrics, mutations_per_sec / n_mut, stop_at, rng
            )
        )
    for i in range(n_tool):
        producers.append(
            _tool_call_producer(
                conns[(n_mut + i) % n_conns], tenants, metrics, tool_calls_per_sec / n_tool,
                stop_at, rng,
            )
        )
    producers.append(
        _turn_producer(conns[(n_mut + n_tool) % n_conns], turn_tiers, metrics, turns_per_sec, stop_at, rng)
    )
    # Each worker is assigned ONE tier (round-robin) rather than scanning all tiers
    # every poll — see _turn_worker's note on the ClaimNext write-guard storm.
    workers = [
        _turn_worker(
            conns[(n_mut + n_tool + 1 + i) % n_conns],
            [tier_names[i % len(tier_names)]],
            metrics,
            turn_duration_s,
            stop_at,
            3.0,
        )
        for i in range(num_turn_workers)
    ]
    await asyncio.gather(*producers, *workers)
    wall = time.monotonic() - start

    latency_ms = {
        "queue_latency_ms": _percentiles_ms(metrics.queue_latency_s),
        "query_latency_ms": _percentiles_ms(metrics.query_latency_s),
        "write_latency_ms": _percentiles_ms(metrics.write_latency_s),
        "end_to_end_latency_ms": _percentiles_ms(metrics.end_to_end_latency_s),
    }
    slo_pass = {axis: _slo_pass(latency_ms[axis], SLO_MS[axis]) for axis in SLO_MS}
    return {
        "duration_s": round(wall, 3),
        "counts": {
            "turns_submitted": metrics.turns_submitted,
            "turns_succeeded": metrics.turns_succeeded,
            "mutations": len(metrics.write_latency_s),
            "tool_calls": len(metrics.query_latency_s),
        },
        "throughput": {
            "turns_per_sec_measured": round(metrics.turns_succeeded / wall, 3) if wall else 0.0,
            "mutations_per_sec_measured": round(len(metrics.write_latency_s) / wall, 3)
            if wall
            else 0.0,
            "tool_calls_per_sec_measured": round(len(metrics.query_latency_s) / wall, 3)
            if wall
            else 0.0,
        },
        "latency_ms": latency_ms,
        "slo_target_ms": SLO_MS,
        "slo_pass": slo_pass,
        "ok": all(all(v.values()) for v in slo_pass.values()),
    }


# --------------------------------------------------------------------------- #
# Phase B: restart / cold (redb) recovery
# --------------------------------------------------------------------------- #


async def phase_restart_recovery(
    binary: Path, handle: ServerHandle, conns: list[Any], probe_graph: str, probe_node: str
) -> tuple[dict[str, Any], ServerHandle, list[Any]]:
    # Confirm the probe node is really there pre-restart.
    pre = await get_node_properties(conns[0], probe_graph, probe_node)
    await close_pool(conns)

    t_stop0 = time.monotonic()
    handle.stop(graceful=True, timeout=30.0)
    stop_wall_s = time.monotonic() - t_stop0

    t_restart0 = time.monotonic()
    restarted = start_server(
        binary,
        handle.sock,
        persist_dir=handle.persist_dir,
        env_overrides={},
    )
    new_conns = await open_pool(handle.sock, len(conns))

    post = None
    first_op_s = None
    deadline = time.monotonic() + 30.0
    while time.monotonic() < deadline:
        try:
            t0 = time.monotonic()
            post = await get_node_properties(new_conns[0], probe_graph, probe_node)
            first_op_s = time.monotonic() - t0
            if post is not None:
                break
        except Exception:  # noqa: BLE001
            pass
        await asyncio.sleep(0.1)
    restart_to_first_op_s = time.monotonic() - t_restart0

    return {
        "pre_restart_probe_found": pre is not None,
        "post_restart_probe_found": post is not None,
        "data_survived_restart": pre is not None and post == pre,
        "graceful_stop_wall_s": round(stop_wall_s, 3),
        "restart_to_first_successful_op_s": round(restart_to_first_op_s, 3),
        "first_op_latency_s": round(first_op_s, 4) if first_op_s is not None else None,
    }, restarted, new_conns


# --------------------------------------------------------------------------- #
# Phase C: hot-tenant noisy-neighbor isolation
# --------------------------------------------------------------------------- #


async def phase_hot_tenant(
    conns: list[Any], elephant_graph: str, ordinary_graph: str, duration_s: float
) -> dict[str, Any]:
    stop_at = time.monotonic() + duration_s
    hot_lat: list[float] = []
    ordinary_lat: list[float] = []

    async def hammer_elephant() -> None:
        n = 0
        while time.monotonic() < stop_at:
            t0 = time.perf_counter()
            await add_node(
                conns[0], elephant_graph, f"{elephant_graph}:hammer:{n}", {"label": "Hammer"}
            )
            hot_lat.append(time.perf_counter() - t0)
            n += 1

    async def sample_ordinary() -> None:
        n = 0
        while time.monotonic() < stop_at:
            t0 = time.perf_counter()
            await add_node(
                conns[1], ordinary_graph, f"{ordinary_graph}:sample:{n}", {"label": "Sample"}
            )
            ordinary_lat.append(time.perf_counter() - t0)
            n += 1
            await asyncio.sleep(0.01)  # ordinary tenant issues a normal, unhammered rate

    # Run several concurrent hammer tasks against the SAME elephant graph
    # (simulating many concurrent sessions on one noisy tenant), one connection
    # each, all pipelined against the pool.
    hammer_tasks = [hammer_elephant() for _ in range(min(8, len(conns)))]
    await asyncio.gather(*hammer_tasks, sample_ordinary())

    return {
        "elephant_hammer_write_latency_ms": _percentiles_ms(hot_lat),
        "ordinary_tenant_write_latency_ms": _percentiles_ms(ordinary_lat),
        "ordinary_tenant_slo_pass": _slo_pass(_percentiles_ms(ordinary_lat), SLO_MS["write_latency_ms"]),
    }


# --------------------------------------------------------------------------- #
# Phase D: backpressure / admission shedding (its OWN ephemeral server so
# EPISTEMIC_GRAPH_MAX_INFLIGHT can be pinned low enough to observe on one box)
# --------------------------------------------------------------------------- #


async def phase_backpressure(binary: Path, max_inflight: int, burst_concurrency: int) -> dict[str, Any]:
    with tempfile.TemporaryDirectory() as tmp:
        sock = os.path.join(tmp, "backpressure.sock")
        handle = start_server(
            binary,
            sock,
            env_overrides={"EPISTEMIC_GRAPH_MAX_INFLIGHT": str(max_inflight)},
        )
        try:
            conns = await open_pool(sock, 1)
            conn = conns[0]
            await create_graph(conn, "bp")

            busy = 0
            ok = 0
            other_err = 0
            lat: list[float] = []

            async def one(i: int) -> None:
                nonlocal busy, ok, other_err
                t0 = time.perf_counter()
                try:
                    await add_node(conn, "bp", f"bp:n{i}", {"label": "Burst"})
                    ok += 1
                    lat.append(time.perf_counter() - t0)
                except RuntimeError as e:
                    if "BUSY" in str(e):
                        busy += 1
                    else:
                        other_err += 1
                except Exception:  # noqa: BLE001
                    other_err += 1

            t0 = time.monotonic()
            await asyncio.gather(*(one(i) for i in range(burst_concurrency)))
            wall = time.monotonic() - t0

            # Recovery: after the burst, a modest concurrency should succeed cleanly.
            recovered = 0
            for i in range(50):
                try:
                    await add_node(conn, "bp", f"bp:recover{i}", {"label": "Recover"})
                    recovered += 1
                except Exception:  # noqa: BLE001
                    pass

            await close_pool(conns)
            return {
                "max_inflight_configured": max_inflight,
                "burst_concurrency": burst_concurrency,
                "burst_wall_s": round(wall, 3),
                "succeeded": ok,
                "busy_rejections": busy,
                "other_errors": other_err,
                "shed_load_observed": busy > 0,
                "accepted_latency_ms": _percentiles_ms(lat),
                "post_burst_recovery_succeeded": recovered,
                "post_burst_recovery_attempted": 50,
            }
        finally:
            handle.stop(graceful=False, timeout=10.0)


# --------------------------------------------------------------------------- #
# Phase E: per-graph eviction + durable read-through (its OWN ephemeral server
# with a tiny EPISTEMIC_GRAPH_MAX_NODES_PER_GRAPH)
# --------------------------------------------------------------------------- #


async def phase_eviction_read_through(binary: Path, node_cap: int, total_nodes: int) -> dict[str, Any]:
    with tempfile.TemporaryDirectory() as tmp:
        sock = os.path.join(tmp, "evict.sock")
        persist_dir = os.path.join(tmp, "persist")
        os.makedirs(persist_dir, exist_ok=True)
        handle = start_server(
            binary,
            sock,
            persist_dir=persist_dir,
            env_overrides={"EPISTEMIC_GRAPH_MAX_NODES_PER_GRAPH": str(node_cap)},
        )
        try:
            conns = await open_pool(sock, 1)
            conn = conns[0]
            await create_graph(conn, "evict")

            write_lat: list[float] = []
            for i in range(total_nodes):
                t0 = time.perf_counter()
                await add_node(conn, "evict", f"evict:n{i}", {"label": "EvictProbe", "i": i})
                write_lat.append(time.perf_counter() - t0)

            # Read back the FIRST node written (most likely evicted back to redb by
            # the per-graph LRU cap, since total_nodes > node_cap) vs the LAST node
            # written (most likely still RAM-resident).
            t0 = time.perf_counter()
            first = await get_node_properties(conn, "evict", "evict:n0")
            evicted_read_s = time.perf_counter() - t0

            t0 = time.perf_counter()
            last = await get_node_properties(conn, "evict", f"evict:n{total_nodes - 1}")
            hot_read_s = time.perf_counter() - t0

            await close_pool(conns)
            return {
                "node_cap_configured": node_cap,
                "total_nodes_written": total_nodes,
                "population_exceeds_cap": total_nodes > node_cap,
                "write_latency_ms": _percentiles_ms(write_lat),
                "likely_evicted_node_found": first is not None,
                "likely_evicted_node_read_ms": round(evicted_read_s * 1000, 4),
                "likely_hot_node_found": last is not None,
                "likely_hot_node_read_ms": round(hot_read_s * 1000, 4),
                "no_data_loss": first is not None and last is not None,
            }
        finally:
            handle.stop(graceful=False, timeout=10.0)


# --------------------------------------------------------------------------- #
# Orchestration
# --------------------------------------------------------------------------- #


async def run_all(args: argparse.Namespace) -> dict[str, Any]:
    binary = _server_bin()
    scale = args.residents / REFERENCE_REGISTERED_AGENTS
    tenant_count = max(2, round(REFERENCE_TENANT_COUNT * scale))
    turns_per_sec = max(REFERENCE_TURNS_PER_SEC * scale, 0.01)
    tool_calls_per_sec = max(REFERENCE_TOOL_CALLS_PER_SEC * scale, 1.0)
    mutations_per_sec = max(REFERENCE_MUTATIONS_PER_SEC * scale, 1.0)

    tenants = build_tenant_plan(tenant_count, args.residents)

    report: dict[str, Any] = {
        "meta": {
            "binary": str(binary),
            "residents": args.residents,
            "scale_vs_1m_contract": round(scale, 6),
            "tenant_count": tenant_count,
            "nodes_per_resident": args.nodes_per_resident,
            "duration_s": args.duration_s,
            "turn_duration_s": args.turn_duration_s,
            "scaled_rates": {
                "turns_per_sec": round(turns_per_sec, 3),
                "tool_calls_per_sec": round(tool_calls_per_sec, 3),
                "graph_mutations_per_sec": round(mutations_per_sec, 3),
            },
            "cpu_count": os.cpu_count(),
            "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
    }

    with tempfile.TemporaryDirectory() as tmp:
        sock = os.path.join(tmp, "soak.sock")
        persist_dir = os.path.join(tmp, "persist")
        os.makedirs(persist_dir, exist_ok=True)
        handle = start_server(binary, sock, persist_dir=persist_dir)
        try:
            conns = await open_pool(sock, args.connections)

            print("== Phase A: population build ==", file=sys.stderr)
            pop_report = await populate(conns, tenants, args.nodes_per_resident, args.pop_concurrency)
            pop_report["rss_kb_after_populate"] = handle.rss_kb()
            report["population"] = pop_report
            print(json.dumps(pop_report, indent=2), file=sys.stderr)

            print("== Phase A2: steady-state mixed workload ==", file=sys.stderr)
            steady = await run_steady_state(
                conns,
                tenants,
                duration_s=args.duration_s,
                turns_per_sec=turns_per_sec,
                tool_calls_per_sec=tool_calls_per_sec,
                mutations_per_sec=mutations_per_sec,
                num_turn_workers=args.turn_workers,
                turn_duration_s=args.turn_duration_s,
                seed=args.seed,
            )
            steady["rss_kb_after_steady_state"] = handle.rss_kb()
            report["steady_state"] = steady
            print(json.dumps(steady, indent=2), file=sys.stderr)

            print("== Phase B: restart / cold-recovery ==", file=sys.stderr)
            probe_graph = tenants.ids[1] if len(tenants.ids) > 1 else tenants.elephant_id
            probe_node = f"{probe_graph}:r0:n0"
            restart_report, handle, conns = await phase_restart_recovery(
                binary, handle, conns, probe_graph, probe_node
            )
            report["restart_recovery"] = restart_report
            print(json.dumps(restart_report, indent=2), file=sys.stderr)

            print("== Phase C: hot-tenant noisy-neighbor isolation ==", file=sys.stderr)
            ordinary_graph = tenants.ids[-1]
            hot_tenant_report = await phase_hot_tenant(
                conns, tenants.elephant_id, ordinary_graph, args.hot_tenant_duration_s
            )
            report["hot_tenant"] = hot_tenant_report
            print(json.dumps(hot_tenant_report, indent=2), file=sys.stderr)

            await close_pool(conns)
        finally:
            handle.stop(graceful=True, timeout=20.0)

    print("== Phase D: backpressure / admission shedding ==", file=sys.stderr)
    backpressure_report = await phase_backpressure(
        binary, args.backpressure_max_inflight, args.backpressure_concurrency
    )
    report["backpressure"] = backpressure_report
    print(json.dumps(backpressure_report, indent=2), file=sys.stderr)

    print("== Phase E: eviction / durable read-through ==", file=sys.stderr)
    eviction_report = await phase_eviction_read_through(
        binary, args.eviction_node_cap, args.eviction_total_nodes
    )
    report["eviction_read_through"] = eviction_report
    print(json.dumps(eviction_report, indent=2), file=sys.stderr)

    report["meta"]["finished_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    return report


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--residents", type=int, default=50_000)
    p.add_argument("--nodes-per-resident", type=int, default=10)
    p.add_argument("--duration-s", type=float, default=60.0)
    p.add_argument("--turn-duration-s", type=float, default=0.02)
    p.add_argument("--turn-workers", type=int, default=16)
    p.add_argument("--connections", type=int, default=16)
    p.add_argument("--pop-concurrency", type=int, default=256)
    p.add_argument("--seed", type=int, default=1337)
    p.add_argument("--hot-tenant-duration-s", type=float, default=10.0)
    p.add_argument("--backpressure-max-inflight", type=int, default=64)
    p.add_argument("--backpressure-concurrency", type=int, default=2000)
    p.add_argument("--eviction-node-cap", type=int, default=500)
    p.add_argument("--eviction-total-nodes", type=int, default=5000)
    p.add_argument("--json", default=None)
    return p


def main(argv: list[str] | None = None) -> int:
    args = _build_arg_parser().parse_args(argv)
    report = asyncio.run(run_all(args))
    text = json.dumps(report, indent=2, default=str)
    if args.json:
        Path(args.json).write_text(text)
        print(f"wrote {args.json}")
    else:
        print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
