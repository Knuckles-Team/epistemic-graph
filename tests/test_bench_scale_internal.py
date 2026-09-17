"""Behavior tests for the internal pieces of scripts/bench_scale.py
(CONCEPT:AU-KG.query.vendor-agnostic-traversal P3): `_bench`'s per-run
measurement arithmetic and shard shutdown, and `main`'s scaling,
median-RSS and extrapolation computation and report shape.

These pin behavior WITHOUT needing a built `epistemic-graph-server` binary
(this lane is pure Python; building the Rust server is out of scope here and
already covered end-to-end, when a binary is present, by
`test_bench_scale_smoke.py`). `_spawn_shards`/`_rss_kb`/`_driver_proc` (for
`_bench`) and `_server_bin`/`_bench` (for `main`) are monkeypatched with
deterministic fakes so the surrounding arithmetic and control flow are
exercised for real. The fakes are chosen so each computed value is
distinguishable from its plausible mistakes: the two drivers report different
wall times (max vs min, rounding), the RSS delta differs under /1024 and /1000,
the 1-shard row is not the first row, and the median RSS differs from both the
smallest value and the median taken with the zero row included.
"""

from __future__ import annotations

import json
import pathlib
import subprocess
import sys

import pytest

pytestmark = pytest.mark.no_engine


@pytest.fixture
def bench(load_script):
    return load_script("bench_scale")


class _FakeProc:
    """Shard process fake recording every lifecycle call into ``events``."""

    def __init__(self, pid: int, events: list) -> None:
        self.pid = pid
        self._events = events

    def terminate(self) -> None:
        self._events.append(("terminate", self.pid))

    def wait(self, timeout: float) -> None:
        self._events.append(("wait", self.pid))
        self._exit_within(timeout)

    def _exit_within(self, timeout: float) -> None:
        pass

    def kill(self) -> None:
        self._events.append(("kill", self.pid))


class _HangingProc(_FakeProc):
    """A shard that does not exit within the shutdown timeout."""

    def _exit_within(self, timeout: float) -> None:
        raise subprocess.TimeoutExpired("epistemic-graph-server", timeout)


# `_bench` dispatches `_driver_proc` through `multiprocessing.Process`, which
# pickles the target BY REFERENCE (module + qualname) -- a nested/closure
# function cannot be pickled, so these fakes must be plain module-level
# functions using only their call arguments.
def _fake_driver_proc_uneven_walls(sock, lo, hi, nodes, concurrency, out_q):
    # shard 0 (lo=0) reports 0.25 s, shard 1 (lo=3) reports 0.51137 s.
    out_q.put((hi - lo, 0.25 + lo * 0.08712345))


def _fake_driver_proc_zero_wall(sock, lo, hi, nodes, concurrency, out_q):
    out_q.put((hi - lo, 0.0))


def _install_shards(monkeypatch, bench, driver, rss_samples, *, hanging_pid=None):
    """Fake two-phase RSS sampling over ``len(rss_samples[0])`` shards.

    ``rss_samples`` is ``(baseline_per_shard, after_per_shard)``; the first
    pass over the shards reads the baseline, the second the post-run values.
    Returns the lifecycle event list the fake processes append to.
    """
    events: list = []
    baseline, after = rss_samples
    reads = {"n": 0}

    def fake_spawn_shards(binary, n, tmp):
        assert n == len(baseline)
        return [
            bench.ShardProc(
                proc=(_HangingProc if 1000 + i == hanging_pid else _FakeProc)(
                    1000 + i, events
                ),
                sock=f"/fake/s{i}.sock",
            )
            for i in range(n)
        ]

    def fake_rss_kb(pid):
        reads["n"] += 1
        sample = baseline if reads["n"] <= len(baseline) else after
        return sample[pid - 1000]

    monkeypatch.setattr(bench, "_spawn_shards", fake_spawn_shards)
    monkeypatch.setattr(bench, "_rss_kb", fake_rss_kb)
    monkeypatch.setattr(bench, "_driver_proc", driver)
    return events


def test_bench_measurement_arithmetic_and_shutdown(monkeypatch, bench):
    events = _install_shards(
        monkeypatch,
        bench,
        _fake_driver_proc_uneven_walls,
        ([1000, 1000], [2000, 3000]),
        hanging_pid=1001,
    )

    result = bench._bench(pathlib.Path("/fake/bin"), 2, 3, 5, 4)

    # 6 ops over the SLOWEST driver's wall time (0.51137 s -> 0.511);
    # RSS delta 3000 kB -> 2.9 MB (not 3.0), 500 kB per agent.
    assert result == {
        "shards": 2,
        "agents": 6,
        "nodes_per_agent": 5,
        "total_ops": 6,
        "wall_s": 0.511,
        "ops_per_sec": 11.7,
        "data_rss_mb": 2.9,
        "per_agent_rss_kb": 500.0,
    }
    # Every shard is terminated, then waited on; a shard that does not exit
    # in time is killed.
    assert events == [
        ("terminate", 1000),
        ("terminate", 1001),
        ("wait", 1000),
        ("wait", 1001),
        ("kill", 1001),
    ]


def test_bench_zero_wall_zero_agents_and_shrinking_rss_report_zeros(monkeypatch, bench):
    events = _install_shards(
        monkeypatch, bench, _fake_driver_proc_zero_wall, ([5000], [1000])
    )

    result = bench._bench(pathlib.Path("/fake/bin"), 1, 0, 5, 4)

    assert result["agents"] == 0
    assert result["ops_per_sec"] == 0.0
    # RSS that shrank during the run is clamped to zero, never negative.
    assert result["data_rss_mb"] == 0.0
    assert result["per_agent_rss_kb"] == 0.0
    assert events == [("terminate", 1000), ("wait", 1000)]


def _row(shards: int, ops_per_sec: float, per_agent_rss_kb: float) -> dict:
    return {
        "shards": shards,
        "agents": 3 * shards,
        "nodes_per_agent": 5,
        "total_ops": 27 * shards,
        "wall_s": 1.0,
        "ops_per_sec": ops_per_sec,
        "data_rss_mb": 1.0,
        "per_agent_rss_kb": per_agent_rss_kb,
    }


def _assert_report(out: str, present: list[str], absent: list[str]) -> None:
    for fragment in present:
        assert fragment in out
    for fragment in absent:
        assert fragment not in out


@pytest.mark.parametrize(
    ("shards_arg", "rows", "scaling", "extrapolation", "present", "absent"),
    [
        pytest.param(
            "2,1,,4",
            {2: _row(2, 50.0, 300.0), 1: _row(1, 25.0, 100.0), 4: _row(4, 90.0, 200.0)},
            {
                "from_shards": 1,
                "to_shards": 4,
                "throughput_speedup": 3.6,
                "linear_ideal": 4.0,
            },
            {
                "ram_budget_gb": 64.0,
                "per_agent_rss_kb": 200.0,
                "agents_per_host": 335544,
                "target_agents": 100_000_000,
                "hosts_required": 299,
            },
            [
                "scaling 1→4 shards: 3.6× throughput (linear ideal 4.0×)",
                "extrapolation: 200.0 kB/agent → ~335,544 agents/host @ 64.0GB "
                "→ 299 hosts for 100,000,000 agents",
            ],
            [],
            id="one-shard-base-is-not-the-first-row",
        ),
        pytest.param(
            "2,4",
            {2: _row(2, 0.0, 0.0), 4: _row(4, 80.0, 0.0)},
            {
                "from_shards": 2,
                "to_shards": 4,
                "throughput_speedup": None,
                "linear_ideal": 2.0,
            },
            {},
            ["scaling 2→4 shards: None× throughput (linear ideal 2.0×)"],
            ["extrapolation:"],
            id="no-one-shard-row-and-no-positive-rss",
        ),
    ],
)
def test_main_scaling_and_extrapolation(
    monkeypatch,
    tmp_path,
    capsys,
    bench,
    shards_arg,
    rows,
    scaling,
    extrapolation,
    present,
    absent,
):
    benched_shards: list[int] = []
    bench_settings: set[tuple] = set()

    def fake_bench(binary, shards, per_shard, nodes, concurrency):
        benched_shards.append(shards)
        bench_settings.add((binary, per_shard, nodes, concurrency))
        return rows[shards]

    monkeypatch.setattr(
        bench, "_server_bin", lambda: (pathlib.Path("/fake/bin"), "debug")
    )
    monkeypatch.setattr(bench, "_bench", fake_bench)
    out_json = tmp_path / "res.json"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "bench_scale.py",
            "--shards",
            shards_arg,
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

    # Shards are benched in --shards order (blank entries dropped), each with
    # the parsed per-shard, node and concurrency settings.
    assert benched_shards == list(rows)
    assert bench_settings == {(pathlib.Path("/fake/bin"), 3, 5, 4)}
    _assert_report(capsys.readouterr().out, present, absent)
    assert json.loads(out_json.read_text()) == {
        "build": "debug",
        "agents_per_shard": 3,
        "nodes_per_agent": 5,
        "rows": list(rows.values()),
        "scaling": scaling,
        "extrapolation": extrapolation,
    }


@pytest.mark.parametrize(
    ("per_agent_rss", "median"),
    [
        # Positive values sort to [100, 200, 300, 400]: the median is 300, not
        # the smallest (100) and not 200 (the median with the zero included).
        ([0.0, 100.0, 300.0, 200.0, 400.0], 300.0),
        ([0.0], 0.0),
        ([], 0.0),
    ],
)
def test_median_per_agent_rss_ignores_non_positive_rows(bench, per_agent_rss, median):
    rows = [{"per_agent_rss_kb": value} for value in per_agent_rss]
    assert bench._median_per_agent_rss(rows) == median


def test_extrapolation_reports_no_host_count_when_one_agent_exceeds_a_host(bench):
    assert bench._extrapolate(1e12, 64.0, 100) == {
        "ram_budget_gb": 64.0,
        "per_agent_rss_kb": 1e12,
        "agents_per_host": 0,
        "target_agents": 100,
        "hosts_required": None,
    }
