#!/usr/bin/env python3
"""EH-353: correctness-gated HelixDB / Epistemic Graph comparison on one host window.

Both engines run on the same host in one invocation, pinned to the same server
CPU set, over the same seeded corpus and query set (both digests pinned in
``benches/helix_compare/spec.json``). Each engine must pass the shared oracle
(:mod:`helix_compare.oracle`) for a workload before that workload is timed. The
report records host, cgroup, load average at start and end of every phase,
binary digests and both commits.

Usage (build host; see docs/benchmarks-helix.md for the full recipe)::

    python3 scripts/bench_helix_compare.py \\
        --eg-server target/release/epistemic-graph-server \\
        --helix-server /path/to/helix/target/release/server \\
        --helix-sdk /path/to/helix/sdks/python/src \\
        --workdir /var/tmp/helix-compare --out report.json --markdown report.md

``--smoke N`` shrinks the corpus to N documents and every workload to a tenth
for harness debugging; its report is marked UNPINNED and is not evidence.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import shutil
import sys
from pathlib import Path
from typing import Any

from helix_compare import dataset as corpus
from helix_compare import queries as query_set
from helix_compare.eg_engine import EgEngine
from helix_compare.helix_engine import HelixEngine
from helix_compare.measure import host_probe, parse_cpus
from helix_compare.oracle import Oracle
from helix_compare.report import render
from helix_compare.runner import Engine, EnginePass, Plan

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SPEC = ROOT / "benches" / "helix_compare" / "spec.json"
ENGINES: dict[str, type[EgEngine] | type[HelixEngine]] = {
    "eg": EgEngine,
    "helix": HelixEngine,
}


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--spec", type=Path, default=DEFAULT_SPEC)
    parser.add_argument("--eg-server", type=Path, required=True)
    parser.add_argument("--helix-server", type=Path, required=True)
    parser.add_argument("--helix-sdk", type=Path, required=True)
    parser.add_argument("--workdir", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--markdown", type=Path)
    parser.add_argument("--engines", default="eg,helix")
    parser.add_argument("--eg-commit", default="unrecorded")
    parser.add_argument("--smoke", type=int, default=0)
    parser.add_argument("--repin", action="store_true")
    return parser


def _workloads(spec: dict[str, Any], shrink: int) -> dict[str, query_set.WorkloadSpec]:
    specs = {}
    for name, values in spec["workloads"].items():
        values = dict(values)
        values["ops"] = max(4, values["ops"] // shrink)
        specs[name] = query_set.WorkloadSpec(**values)
    return specs


def _inputs(spec: dict[str, Any], args: argparse.Namespace) -> tuple[Any, Any, dict]:
    values = dict(spec["dataset"])
    if args.smoke:
        values["docs"] = args.smoke
    data = corpus.generate(corpus.DatasetSpec.from_json(values))
    queries = query_set.generate(
        data, Oracle.of(data), _workloads(spec, 10 if args.smoke else 1)
    )
    pins = {"dataset_sha256": data.digest(), "queries_sha256": queries.digest()}
    return data, queries, pins


def _verify_pins(
    spec: dict[str, Any], pins: dict[str, str], args: argparse.Namespace
) -> bool:
    if args.smoke or args.repin:
        return False
    for name, actual in pins.items():
        if spec[name] != actual:
            raise SystemExit(f"{name} {actual} does not match the pinned {spec[name]}")
    return True


def _plan(spec: dict[str, Any], smoke: bool) -> Plan:
    plan = spec["plan"]
    shrink = 10 if smoke else 1
    return Plan(
        gate_ops=max(4, plan["gate_ops"] // shrink),
        warmup_ops=max(4, plan["warmup_ops"] // shrink),
        cold_ops=max(4, plan["cold_ops"] // shrink),
        concurrency=tuple(plan["concurrency"]),
    )


async def _run_engines(
    args: argparse.Namespace, spec: dict[str, Any], data: Any, queries: Any
) -> list[dict[str, Any]]:
    binaries = {"eg": args.eg_server, "helix": args.helix_server}
    server_cpus = parse_cpus(spec["cpus"]["server"])
    reports = []
    for key in args.engines.split(","):
        workdir = args.workdir / key
        shutil.rmtree(workdir, ignore_errors=True)
        workdir.mkdir(parents=True)
        engine: Engine = ENGINES[key](binaries[key].resolve(), workdir, server_cpus)
        engine_pass = EnginePass(engine, data, queries, _plan(spec, bool(args.smoke)))
        try:
            reports.append(await engine_pass.run())
        finally:
            await engine.stop()
    return reports


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    spec = json.loads(args.spec.read_text())
    sys.path.insert(0, str(args.helix_sdk.resolve()))
    os.sched_setaffinity(0, parse_cpus(spec["cpus"]["client"]))
    started = host_probe()
    data, queries, pins = _inputs(spec, args)
    pinned = _verify_pins(spec, pins, args)
    engines = asyncio.run(_run_engines(args, spec, data, queries))
    report = {
        "evidence": "PINNED" if pinned else "UNPINNED (smoke/repin run: not evidence)",
        "spec": spec,
        "inputs": pins,
        "eg": {"commit": args.eg_commit, "server_sha256": _sha256(args.eg_server)},
        "helix": {
            "commit": spec["helix"]["commit"],
            "server_sha256": _sha256(args.helix_server),
        },
        "host_start": started,
        "host_end": host_probe(),
        "engines": engines,
    }
    args.out.write_text(json.dumps(report, indent=1, sort_keys=True))
    markdown = render(report)
    if args.markdown:
        args.markdown.write_text(markdown)
    print(markdown)
    print(json.dumps(pins))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
