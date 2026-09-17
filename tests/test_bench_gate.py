"""Behavior tests for scripts/bench_gate.py
(CONCEPT:EG-KG.query.perf-recall-ci-gate).

Exercises the CLI end-to-end (subprocess, like the bench_scale smoke test)
so the recall gate, the latency gate, and their combination are pinned before
`main()` is decomposed into helpers -- a change that regresses any of these
must fail this test. Both thresholds are inclusive: a recall exactly at the
floor and a p50 exactly at the ceiling pass.
"""

from __future__ import annotations

import json
import pathlib
import subprocess
import sys

import pytest

# Pure/static test -- reads only local JSON artifacts, never the shared native
# engine (see conftest.py's session-scoped `start_epistemic_graph_server`
# fixture, which this marker exempts this module from triggering).
pytestmark = pytest.mark.no_engine

_GATE_PATH = pathlib.Path(__file__).resolve().parents[1] / "scripts" / "bench_gate.py"


@pytest.mark.parametrize(
    ("ceilings", "recall_at_k", "estimates", "code", "stdout", "stderr"),
    [
        pytest.param(
            {"bench_a": 1_000_000.0},
            0.95,
            {"bench_a": 500_000.0},
            0,
            ["all perf/recall thresholds held.", "recall@10: 0.9500", "bench_a"],
            [],
            id="all-thresholds-hold",
        ),
        pytest.param(
            {"bench_a": 1_000_000.0},
            0.9,
            {"bench_a": 1_000_000.0},
            0,
            ["all perf/recall thresholds held.", "recall@10: 0.9000"],
            [],
            id="values-exactly-at-floor-and-ceiling-hold",
        ),
        pytest.param(
            {"bench_a": 1_000_000.0},
            0.5,
            {"bench_a": 500_000.0},
            1,
            [],
            ["REGRESSION", "recall@10 0.5000 < floor 0.9000"],
            id="recall-regression-fails",
        ),
        pytest.param(
            {"bench_a": 100.0},
            0.95,
            {"bench_a": 500_000.0},
            1,
            [],
            ["REGRESSION", "bench_a p50", "> ceiling"],
            id="latency-regression-fails",
        ),
        pytest.param(
            {},
            None,
            {},
            1,
            [],
            ["recall artifact missing"],
            id="missing-recall-artifact-fails",
        ),
        pytest.param(
            {"bench_missing": 100.0},
            0.95,
            {},
            0,
            ["bench_missing: (skipped"],
            [],
            id="missing-estimate-is-skipped-not-failed",
        ),
    ],
)
def test_gate_verdicts(
    tmp_path, ceilings, recall_at_k, estimates, code, stdout, stderr
):
    thresholds = tmp_path / "thresholds.json"
    thresholds.write_text(
        json.dumps({"recall_floor": 0.9, "latency_p50_ns_max": ceilings})
    )
    recall = tmp_path / "recall.json"
    if recall_at_k is not None:
        recall.write_text(json.dumps({"recall_at_k": recall_at_k, "k": 10}))
    criterion_dir = tmp_path / "criterion"
    for name, point_estimate_ns in estimates.items():
        est_dir = criterion_dir / name / "new"
        est_dir.mkdir(parents=True)
        (est_dir / "estimates.json").write_text(
            json.dumps({"median": {"point_estimate": point_estimate_ns}})
        )

    proc = subprocess.run(
        [
            sys.executable,
            str(_GATE_PATH),
            "--thresholds",
            str(thresholds),
            "--criterion-dir",
            str(criterion_dir),
            "--recall-json",
            str(recall),
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert proc.returncode == code, proc.stderr
    for fragment in stdout:
        assert fragment in proc.stdout
    for fragment in stderr:
        assert fragment in proc.stderr
