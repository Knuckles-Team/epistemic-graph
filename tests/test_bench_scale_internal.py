"""Behavior tests for the internal pieces of scripts/bench_scale.py
(CONCEPT:AU-KG.query.vendor-agnostic-traversal P3): `_bench`'s per-run
measurement arithmetic and `main`'s scaling/extrapolation computation and
report shape.

These pin behavior WITHOUT needing a built `epistemic-graph-server` binary
(this lane is pure Python; building the Rust server is out of scope here and
already covered end-to-end, when a binary is present, by
`test_bench_scale_smoke.py`). `_spawn_shards`/`_rss_kb`/`_driver_proc` (for
`_bench`) and `_server_bin`/`_bench` (for `main`) are monkeypatched with
deterministic fakes so the surrounding arithmetic and control flow are
exercised for real -- a change that regresses either function's math or
report shape must fail this test.
"""

from __future__ import annotations

import importlib.util
import json
import math
import pathlib
import sys

import pytest

pytestmark = pytest.mark.no_engine

_BENCH_PATH = pathlib.Path(__file__).resolve().parents[1] / "scripts" / "bench_scale.py"


def _load():
    spec = importlib.util.spec_from_file_location("bench_scale", _BENCH_PATH)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    # Register in sys.modules so `multiprocessing`'s fork of this process can
    # re-resolve `bench_scale._driver_proc` by qualified name.
    sys.modules["bench_scale"] = mod
    spec.loader.exec_module(mod)
    return mod


class _FakeProc:
    def __init__(self, pid: int) -> None:
        self.pid = pid

    def terminate(self) -> None:
        pass

    def wait(self, timeout: float | None = None) -> None:
        pass


# `_bench` dispatches `_driver_proc` through `multiprocessing.Process`, whose
# default start method here pickles the target BY REFERENCE (module +
# qualname) -- a nested/closure function cannot be pickled, so these fakes
# must be plain module-level functions using only their call arguments.
def _fake_driver_proc_half_second(sock, lo, hi, nodes, concurrency, out_q):
    out_q.put((hi - lo, 0.5))


def _fake_driver_proc_zero_wall(sock, lo, hi, nodes, concurrency, out_q):
    out_q.put((hi - lo, 0.0))


def test_bench_measurement_arithmetic(monkeypatch):
    bench = _load()
    n_shards = 2
    per_shard = 3
    nodes = 5
    concurrency = 4

    def fake_spawn_shards(binary, n, tmp):
        assert n == n_shards
        return [
            bench.ShardProc(proc=_FakeProc(pid=1000 + i), sock=f"/fake/s{i}.sock")
            for i in range(n)
        ]

    rss_calls = {"n": 0}

    def fake_rss_kb(pid):
        rss_calls["n"] += 1
        idx = pid - 1000
        # first n_shards calls are the baseline sample, the next n_shards
        # calls are the post-run sample.
        if rss_calls["n"] <= n_shards:
            return 1000
        return 1000 + (idx + 1) * 500  # shard0 -> 1500, shard1 -> 2000

    monkeypatch.setattr(bench, "_spawn_shards", fake_spawn_shards)
    monkeypatch.setattr(bench, "_rss_kb", fake_rss_kb)
    monkeypatch.setattr(bench, "_driver_proc", _fake_driver_proc_half_second)

    result = bench._bench(
        pathlib.Path("/fake/bin"), n_shards, per_shard, nodes, concurrency
    )

    # baseline sum = 1000*2 = 2000; after sum = 1500+2000 = 3500; delta 1500
    assert result == {
        "shards": 2,
        "agents": 6,
        "nodes_per_agent": 5,
        "total_ops": 6,
        "wall_s": 0.5,
        "ops_per_sec": 12.0,
        "data_rss_mb": 1.5,
        "per_agent_rss_kb": 250.0,
    }


def test_bench_zero_wall_time_reports_zero_ops_per_sec(monkeypatch):
    bench = _load()

    def fake_spawn_shards(binary, n, tmp):
        return [
            bench.ShardProc(proc=_FakeProc(pid=2000 + i), sock=f"/fake/z{i}.sock")
            for i in range(n)
        ]

    def fake_rss_kb(pid):
        return 0

    monkeypatch.setattr(bench, "_spawn_shards", fake_spawn_shards)
    monkeypatch.setattr(bench, "_rss_kb", fake_rss_kb)
    monkeypatch.setattr(bench, "_driver_proc", _fake_driver_proc_zero_wall)

    result = bench._bench(pathlib.Path("/fake/bin"), 1, 2, 5, 4)
    assert result["ops_per_sec"] == 0.0
    assert result["per_agent_rss_kb"] == 0.0


def test_main_scaling_and_extrapolation(monkeypatch, tmp_path, capsys):
    bench = _load()

    canned_rows = {
        1: {
            "shards": 1,
            "agents": 3,
            "nodes_per_agent": 5,
            "total_ops": 27,
            "wall_s": 1.0,
            "ops_per_sec": 27.0,
            "data_rss_mb": 1.0,
            "per_agent_rss_kb": 300.0,
        },
        2: {
            "shards": 2,
            "agents": 6,
            "nodes_per_agent": 5,
            "total_ops": 54,
            "wall_s": 1.0,
            "ops_per_sec": 54.0,
            "data_rss_mb": 2.0,
            "per_agent_rss_kb": 300.0,
        },
    }

    def fake_server_bin():
        return pathlib.Path("/fake/bin"), "debug"

    def fake_bench(binary, shards, per_shard, nodes, concurrency):
        return canned_rows[shards]

    monkeypatch.setattr(bench, "_server_bin", fake_server_bin)
    monkeypatch.setattr(bench, "_bench", fake_bench)

    out_json = tmp_path / "res.json"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "bench_scale.py",
            "--shards",
            "1,2",
            "--agents-per-shard",
            "3",
            "--nodes-per-agent",
            "5",
            "--concurrency",
            "4",
            "--json",
            str(out_json),
        ],
    )

    bench.main()

    captured = capsys.readouterr()
    assert "scaling 1→2 shards: 2.0× throughput (linear ideal 2.0×)" in captured.out

    data = json.loads(out_json.read_text())
    assert data["build"] == "debug"
    assert data["rows"] == [canned_rows[1], canned_rows[2]]
    assert data["scaling"] == {
        "from_shards": 1,
        "to_shards": 2,
        "throughput_speedup": 2.0,
        "linear_ideal": 2.0,
    }
    expected_extrap = bench._extrapolate(300.0, 64.0, 100_000_000)
    assert data["extrapolation"] == expected_extrap
    assert expected_extrap["hosts_required"] == math.ceil(
        100_000_000 / expected_extrap["agents_per_host"]
    )


def test_main_no_positive_rss_skips_extrapolation(monkeypatch, tmp_path, capsys):
    bench = _load()

    row = {
        "shards": 1,
        "agents": 3,
        "nodes_per_agent": 5,
        "total_ops": 27,
        "wall_s": 1.0,
        "ops_per_sec": 27.0,
        "data_rss_mb": 0.0,
        "per_agent_rss_kb": 0.0,
    }

    monkeypatch.setattr(
        bench, "_server_bin", lambda: (pathlib.Path("/fake/bin"), "debug")
    )
    monkeypatch.setattr(bench, "_bench", lambda *a, **k: row)
    monkeypatch.setattr(
        sys,
        "argv",
        ["bench_scale.py", "--shards", "1", "--agents-per-shard", "3"],
    )

    bench.main()

    captured = capsys.readouterr()
    assert "extrapolation:" not in captured.out
