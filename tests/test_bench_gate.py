"""Behavior tests for scripts/bench_gate.py
(CONCEPT:EG-KG.query.perf-recall-ci-gate).

Exercises the CLI end-to-end (subprocess, like the bench_scale smoke test)
so the recall gate, the latency gate, and their combination are pinned before
`main()` is decomposed into helpers -- a change that regresses any of these
must fail this test.
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


def _write_thresholds(path: pathlib.Path, recall_floor: float, ceilings: dict) -> None:
    path.write_text(
        json.dumps({"recall_floor": recall_floor, "latency_p50_ns_max": ceilings})
    )


def _write_recall(path: pathlib.Path, recall_at_k: float, k: int = 10) -> None:
    path.write_text(json.dumps({"recall_at_k": recall_at_k, "k": k}))


def _write_estimate(
    criterion_dir: pathlib.Path, name: str, point_estimate_ns: float
) -> None:
    est_dir = criterion_dir / name / "new"
    est_dir.mkdir(parents=True, exist_ok=True)
    (est_dir / "estimates.json").write_text(
        json.dumps({"median": {"point_estimate": point_estimate_ns}})
    )


def _run(
    tmp_path: pathlib.Path,
    thresholds: pathlib.Path,
    recall: pathlib.Path,
    criterion_dir: pathlib.Path,
) -> subprocess.CompletedProcess:
    return subprocess.run(
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


def test_all_thresholds_hold(tmp_path):
    thresholds = tmp_path / "thresholds.json"
    recall = tmp_path / "recall.json"
    criterion_dir = tmp_path / "criterion"
    _write_thresholds(thresholds, recall_floor=0.9, ceilings={"bench_a": 1_000_000.0})
    _write_recall(recall, recall_at_k=0.95)
    _write_estimate(criterion_dir, "bench_a", 500_000.0)

    proc = _run(tmp_path, thresholds, recall, criterion_dir)

    assert proc.returncode == 0, proc.stderr
    assert "all perf/recall thresholds held." in proc.stdout
    assert "recall@10: 0.9500" in proc.stdout
    assert "bench_a" in proc.stdout


def test_recall_regression_fails(tmp_path):
    thresholds = tmp_path / "thresholds.json"
    recall = tmp_path / "recall.json"
    criterion_dir = tmp_path / "criterion"
    _write_thresholds(thresholds, recall_floor=0.9, ceilings={"bench_a": 1_000_000.0})
    _write_recall(recall, recall_at_k=0.5)
    _write_estimate(criterion_dir, "bench_a", 500_000.0)

    proc = _run(tmp_path, thresholds, recall, criterion_dir)

    assert proc.returncode == 1
    assert "REGRESSION" in proc.stderr
    assert "recall@10 0.5000 < floor 0.9000" in proc.stderr


def test_latency_regression_fails(tmp_path):
    thresholds = tmp_path / "thresholds.json"
    recall = tmp_path / "recall.json"
    criterion_dir = tmp_path / "criterion"
    _write_thresholds(thresholds, recall_floor=0.9, ceilings={"bench_a": 100.0})
    _write_recall(recall, recall_at_k=0.95)
    _write_estimate(criterion_dir, "bench_a", 500_000.0)

    proc = _run(tmp_path, thresholds, recall, criterion_dir)

    assert proc.returncode == 1
    assert "REGRESSION" in proc.stderr
    assert "bench_a p50" in proc.stderr
    assert "> ceiling" in proc.stderr


def test_missing_recall_artifact_fails(tmp_path):
    thresholds = tmp_path / "thresholds.json"
    recall = tmp_path / "recall.json"  # never written
    criterion_dir = tmp_path / "criterion"
    _write_thresholds(thresholds, recall_floor=0.9, ceilings={})

    proc = _run(tmp_path, thresholds, recall, criterion_dir)

    assert proc.returncode == 1
    assert "recall artifact missing" in proc.stderr


def test_missing_estimate_is_skipped_not_failed(tmp_path):
    thresholds = tmp_path / "thresholds.json"
    recall = tmp_path / "recall.json"
    criterion_dir = tmp_path / "criterion"
    _write_thresholds(thresholds, recall_floor=0.9, ceilings={"bench_missing": 100.0})
    _write_recall(recall, recall_at_k=0.95)
    # no estimates.json written for bench_missing

    proc = _run(tmp_path, thresholds, recall, criterion_dir)

    assert proc.returncode == 0, proc.stderr
    assert "bench_missing: (skipped" in proc.stdout
