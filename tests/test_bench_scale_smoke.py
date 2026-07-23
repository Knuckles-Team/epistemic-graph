"""Smoke test for the multi-shard scale harness (CONCEPT:AU-KG.query.vendor-agnostic-traversal P3).

Runs a tiny single-shard load so regressions in the harness (or the server's
multi-tenant/sharded path) surface in CI, without a heavy benchmark.
"""

import importlib.util
import json
import pathlib
import subprocess
import sys

import pytest

_BENCH_PATH = pathlib.Path(__file__).resolve().parents[1] / "scripts" / "bench_scale.py"


def _load():
    spec = importlib.util.spec_from_file_location("bench_scale", _BENCH_PATH)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_scale_harness_smoke(tmp_path):
    # Invoke the real CLI (its own process) so multiprocessing resolves the
    # driver function normally — a true end-to-end smoke of the harness.
    out = tmp_path / "res.json"
    proc = subprocess.run(
        [
            sys.executable, str(_BENCH_PATH),
            "--shards", "1", "--agents-per-shard", "3",
            "--nodes-per-agent", "5", "--concurrency", "4",
            "--json", str(out),
        ],
        capture_output=True, text=True, timeout=120,
    )
    if "server binary missing" in (proc.stdout + proc.stderr):
        pytest.skip("server binary not built")
    assert proc.returncode == 0, proc.stderr
    data = json.loads(out.read_text())
    row = data["rows"][0]
    assert row["shards"] == 1 and row["agents"] == 3
    # 3 agents × (5 nodes + 4 edges) = 27 write ops.
    assert row["total_ops"] == 27
    assert row["ops_per_sec"] > 0
    assert row["per_agent_rss_kb"] >= 0


def test_extrapolation_math():
    bench = _load()
    ex = bench._extrapolate(per_agent_rss_kb=50.0, ram_budget_gb=64.0, target=100_000_000)
    # 64 GB / 50 kB ≈ 1.34M agents/host → ~75 hosts for 100M.
    assert ex["agents_per_host"] > 1_000_000
    assert ex["hosts_required"] == __import__("math").ceil(
        100_000_000 / ex["agents_per_host"]
    )
