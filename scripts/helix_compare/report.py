"""Render the JSON report as the Markdown tables the benchmark page publishes."""

from __future__ import annotations

from typing import Any

HEADER = (
    "| workload | engine | c | ops | err | p50 ms | p95 ms | p99 ms | ops/s "
    "| server CPU ms/op |\n|---|---|--:|--:|--:|--:|--:|--:|--:|--:|"
)


def _row(workload: str, engine: str, summary: dict[str, Any]) -> str:
    cells = [
        workload,
        engine,
        str(summary.get("concurrency", 1)),
        str(summary["ops"]),
        str(summary["errors"]),
        str(summary["p50_ms"]),
        str(summary["p95_ms"]),
        str(summary["p99_ms"]),
        str(summary["throughput_ops_s"]),
        str(summary.get("server_cpu_ms_per_op")),
    ]
    return "| " + " | ".join(cells) + " |"


def _timed_rows(engine: dict[str, Any], section: str) -> list[str]:
    lines = []
    for workload, levels in engine.get(section, {}).items():
        if "ops" in levels:  # a cold row is one summary, not a per-level map
            lines.append(_row(workload, engine["engine"], levels))
            continue
        for summary in levels.values():
            lines.append(_row(workload, engine["engine"], summary))
    return lines


def _txn_rows(engine: dict[str, Any]) -> list[str]:
    txn = engine.get("txn_batch", {})
    return [
        _row("txn_batch", engine["engine"], summary)
        for key, summary in txn.items()
        if key != "gate"
    ]


def _gate_rows(engine: dict[str, Any]) -> list[str]:
    gates = dict(engine.get("gate", {}))
    gates["txn_batch"] = engine.get("txn_batch", {}).get("gate", {})
    return [
        f"| {name} | {engine['engine']} | {'yes' if gate.get('passed') else 'NO'} "
        f"| {gate.get('checked')} | {gate.get('failed')} "
        f"| {gate.get('recall_mean', '')} | {gate.get('recall_min', '')} "
        f"| {'; '.join(gate.get('failure_samples', []))[:300]} |"
        for name, gate in gates.items()
    ]


def _footprint_row(engine: dict[str, Any]) -> str:
    load = engine.get("load", {})
    return (
        f"| {engine['engine']} | {load.get('seconds')} | {load.get('docs_per_s')} "
        f"| {load.get('storage_bytes')} | {load.get('storage_amplification')} "
        f"| {engine.get('storage_bytes_final')} | {engine.get('idle_rss_kb')} "
        f"| {engine.get('peak_rss_kb')} | {engine.get('write_conflict_retries')} "
        f"| {engine.get('ready_empty_s')} | {engine.get('ready_after_restart_s')} |"
    )


def render(report: dict[str, Any]) -> str:
    engines = report["engines"]
    sections = [
        "### Correctness gate\n\n| workload | engine | passed | checked | failed "
        "| recall mean | recall min | samples |\n|---|---|---|--:|--:|--:|--:|---|",
        *[line for engine in engines for line in _gate_rows(engine)],
        "\n### Warm latency and throughput\n\n" + HEADER,
        *[line for engine in engines for line in _timed_rows(engine, "warm")],
        *[line for engine in engines for line in _txn_rows(engine)],
        "\n### Process-cold (first queries after restart, c=1)\n\n" + HEADER,
        *[line for engine in engines for line in _timed_rows(engine, "cold")],
        "\n### Load and footprint\n\n| engine | load s | docs/s | storage B "
        "| amplification | storage B (final) | idle RSS kB | peak RSS kB "
        "| write conflict retries | ready empty s | ready after restart s |"
        "\n|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|",
        *[_footprint_row(engine) for engine in engines],
    ]
    return "\n".join(sections) + "\n"
