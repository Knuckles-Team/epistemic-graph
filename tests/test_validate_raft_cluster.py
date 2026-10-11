"""R014.2 process lifecycle evidence; fake Cargo does not prove Rust clusters."""

from __future__ import annotations

import json
import os
import signal
import subprocess
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
pytestmark = [pytest.mark.no_engine, pytest.mark.spec("EG-DURABLE-KERNEL-R014.2")]


@pytest.fixture
def harness(tmp_path: Path) -> tuple[dict[str, str], Path]:
    cargo = tmp_path / "cargo"
    cargo.write_text("""#!/usr/bin/env python3
import json, os, signal, subprocess, sys, time
from pathlib import Path
record = Path(os.environ["RECORD"])
if "--no-run" in sys.argv:
    assert "RAFT_LOAD_PIDS" not in os.environ
    sys.exit(int(os.environ.get("BUILD_RC", "0")))
pids = [int(pid) for pid in os.environ.get("RAFT_LOAD_PIDS", "").split(",") if pid]
for pid in pids:
    os.kill(pid, 0)
if os.environ.get("SPAWN_CHILD"):
    child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"])
    record.with_suffix(".child").write_text(str(child.pid))
record.write_text(json.dumps(pids))
if "RAFT_LOAD_REPEATS" in os.environ:
    assert "--exact" in sys.argv
    assert sys.argv[-1] in (
        "raft::tests::multi_node_group_join_then_leader_rebalance",
        "raft::tests::placement_admin_wire_rpc::placement_admin_wire_rpcs_move_data_across_a_real_three_node_cluster",
    )
if os.environ.get("CHECK_CPU") and Path("/proc").exists():
    def ticks(pid):
        return sum(int(v) for v in Path(f"/proc/{pid}/stat").read_text().split()[13:15])
    before = [ticks(pid) for pid in pids]
    time.sleep(.2)
    assert all(ticks(pid) > old for pid, old in zip(pids, before))
if os.environ.get("SIGNAL_PARENT"):
    os.kill(os.getppid(), signal.SIGTERM)
if os.environ.get("KILL_WORKER"):
    os.kill(pids[0], signal.SIGKILL)
time.sleep(float(os.environ.get("TEST_SECONDS", ".2")))
rc = int(os.environ.get("TEST_RC", "0"))
if os.environ.get("FAIL_FIRST"):
    marker = record.with_suffix(".failed")
    if not marker.exists():
        marker.touch()
        rc = 37
        print("causal failure trace: first attempt", file=sys.stderr)
if not os.environ.get("EMPTY_SELECTION"):
    print("test result: ok. 1 passed;" if rc == 0 else "test result: FAILED. 0 passed;")
sys.exit(rc)
""")
    cargo.chmod(0o755)
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith("RAFT_LOAD_")
    }
    env.update(
        PATH=f"{tmp_path}:{env['PATH']}",
        RECORD=str(tmp_path / "record"),
        RAFT_LOAD_WORKERS="2",
    )
    return env, tmp_path / "record"


def run(env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash", str(ROOT / "scripts/validate-raft-cluster.sh")],
        env=env,
        capture_output=True,
        text=True,
        timeout=10,
    )


def assert_stopped(record: Path) -> None:
    for pid in json.loads(record.read_text()):
        with pytest.raises(ProcessLookupError):
            os.kill(pid, 0)


def test_cpu_load_runs_during_cluster_test(
    harness: tuple[dict[str, str], Path],
) -> None:
    env, record = harness
    env["CHECK_CPU"] = "1"
    result = run(env)
    assert result.returncode == 0, result.stderr
    assert len(json.loads(record.read_text())) == 2
    assert "CPU load started: workers=2 limit_seconds=300" in result.stdout
    assert "CPU load stopped: workers=2 elapsed_seconds=" in result.stdout
    assert_stopped(record)


@pytest.mark.parametrize("value", ["0", "-1", "33", "", "1.5", "999999999999999999999"])
def test_zero_load_workers_refused(
    harness: tuple[dict[str, str], Path], value: str
) -> None:
    env, record = harness
    env["RAFT_LOAD_WORKERS"] = value
    result = run(env)
    assert result.returncode == 2
    assert "RAFT_LOAD_WORKERS must be an integer in 1..32" in result.stderr
    assert not record.exists()


def test_load_workers_stop_on_failure(harness: tuple[dict[str, str], Path]) -> None:
    env, record = harness
    env["TEST_RC"] = "37"
    assert run(env).returncode == 37
    assert_stopped(record)


@pytest.mark.parametrize("signum", [signal.SIGINT, signal.SIGTERM])
def test_load_workers_stop_on_signal(
    harness: tuple[dict[str, str], Path], signum: int
) -> None:
    env, record = harness
    env["TEST_SECONDS"] = "30"
    proc = subprocess.Popen(
        ["bash", str(ROOT / "scripts/validate-raft-cluster.sh")],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        deadline = time.monotonic() + 5
        while not record.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        assert record.exists()
        proc.send_signal(signum)
        proc.communicate(timeout=5)
        assert proc.returncode == 128 + signum
        assert_stopped(record)
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()


def test_duration_is_bounded(harness: tuple[dict[str, str], Path]) -> None:
    env, record = harness
    env.update(RAFT_LOAD_SECONDS="1", TEST_SECONDS="30")
    assert run(env).returncode == 124
    assert_stopped(record)


def test_worker_death_refuses_unloaded_success(
    harness: tuple[dict[str, str], Path],
) -> None:
    env, record = harness
    env.update(KILL_WORKER="1", TEST_SECONDS="2")
    assert run(env).returncode == 125
    assert_stopped(record)


def test_unloaded_call_is_unchanged(harness: tuple[dict[str, str], Path]) -> None:
    env, record = harness
    del env["RAFT_LOAD_WORKERS"]
    result = run(env)
    assert result.returncode == 0
    assert json.loads(record.read_text()) == []
    assert "CPU load" not in result.stdout


def test_build_failure_starts_no_workers(harness: tuple[dict[str, str], Path]) -> None:
    env, record = harness
    env["BUILD_RC"] = "42"
    result = run(env)
    assert result.returncode == 42
    assert "CPU load" not in result.stdout
    assert not record.exists()


@pytest.mark.parametrize("seconds", ["0", "-1", "3601", "", "1.5"])
def test_invalid_duration_refused(
    harness: tuple[dict[str, str], Path], seconds: str
) -> None:
    env, record = harness
    env["RAFT_LOAD_SECONDS"] = seconds
    assert run(env).returncode == 2
    assert not record.exists()


def test_command_descendants_stop_on_failure(
    harness: tuple[dict[str, str], Path],
) -> None:
    env, record = harness
    env.update(SPAWN_CHILD="1", TEST_RC="37")
    assert run(env).returncode == 37
    pid = int(record.with_suffix(".child").read_text())
    # An adopted zombie has exited but may await the container init's reaper.
    stat = Path(f"/proc/{pid}/stat")
    deadline = time.monotonic() + 2
    while stat.exists() and stat.read_text().split()[2] != "Z":
        assert time.monotonic() < deadline, "Cargo descendant still running"
        time.sleep(0.01)
    assert_stopped(record)


def test_signal_with_fast_command_exit_is_not_success(
    harness: tuple[dict[str, str], Path],
) -> None:
    env, record = harness
    env.update(SIGNAL_PARENT="1", TEST_SECONDS="0")
    assert run(env).returncode == 143
    assert_stopped(record)


@pytest.mark.spec("EG-DURABLE-KERNEL-R014.3.1")
def test_repeats_both_cluster_tests(harness: tuple[dict[str, str], Path]) -> None:
    env, record = harness
    env.update(RAFT_LOAD_REPEATS="2", RAFT_LOAD_OUTPUT=str(record.parent / "runs"))
    assert run(env).returncode == 0
    rows = [
        json.loads(line)
        for line in next((record.parent / "runs").glob("*/results.jsonl"))
        .read_text()
        .splitlines()
    ]
    assert len(rows) == 4
    assert {row["repetition"] for row in rows} == {1, 2}
    assert {row["test"] for row in rows} == {
        "raft::tests::multi_node_group_join_then_leader_rebalance",
        "raft::tests::placement_admin_wire_rpc::placement_admin_wire_rpcs_move_data_across_a_real_three_node_cluster",
    }
    assert all(row["workers"] == 2 and row["returncode"] == 0 for row in rows)
    assert_stopped(record)


@pytest.mark.spec("EG-DURABLE-KERNEL-R014.3.1")
def test_repetition_preserves_failure_and_trace(
    harness: tuple[dict[str, str], Path],
) -> None:
    env, record = harness
    env.update(
        RAFT_LOAD_REPEATS="2",
        RAFT_LOAD_OUTPUT=str(record.parent / "runs"),
        FAIL_FIRST="1",
    )
    assert run(env).returncode == 37
    manifest = next((record.parent / "runs").glob("*/results.jsonl"))
    rows = [json.loads(line) for line in manifest.read_text().splitlines()]
    assert [row["returncode"] for row in rows] == [37, 0, 0, 0]
    assert (
        "causal failure trace: first attempt"
        in (manifest.parent / rows[0]["trace"]).read_text()
    )
    assert len(list(manifest.parent.glob("*.log"))) == 4
    assert_stopped(record)


@pytest.mark.spec("EG-DURABLE-KERNEL-R014.3.1")
def test_empty_selection_refused(harness: tuple[dict[str, str], Path]) -> None:
    env, record = harness
    env.update(
        RAFT_LOAD_REPEATS="2",
        RAFT_LOAD_OUTPUT=str(record.parent / "runs"),
        EMPTY_SELECTION="1",
    )
    assert run(env).returncode == 125
    assert_stopped(record)


@pytest.mark.spec("EG-DURABLE-KERNEL-R014.3.1")
@pytest.mark.parametrize("value", ["0", "1", "101", "-1", "", "1.5"])
def test_invalid_repeat_count_refused(
    harness: tuple[dict[str, str], Path], value: str
) -> None:
    env, record = harness
    env["RAFT_LOAD_REPEATS"] = value
    result = run(env)
    assert result.returncode == 2
    assert "RAFT_LOAD_REPEATS must be an integer in 2..100" in result.stderr
    assert not record.exists()


@pytest.mark.spec("EG-DURABLE-KERNEL-R014.3.1")
@pytest.mark.parametrize(
    "options, expected",
    [
        ({"SIGNAL_PARENT": "1"}, 143),
        ({"RAFT_LOAD_SECONDS": "1", "TEST_SECONDS": "30"}, 124),
    ],
)
def test_repeated_interruption_retains_result(
    harness: tuple[dict[str, str], Path], options: dict[str, str], expected: int
) -> None:
    env, record = harness
    env.update(
        RAFT_LOAD_REPEATS="2",
        RAFT_LOAD_OUTPUT=str(record.parent / "runs"),
        SPAWN_CHILD="1",
        **options,
    )
    assert run(env).returncode == expected
    manifest = next((record.parent / "runs").glob("*/results.jsonl"))
    rows = [json.loads(line) for line in manifest.read_text().splitlines()]
    assert rows[0]["returncode"] == expected
    assert (manifest.parent / rows[0]["trace"]).exists()
    assert_stopped(record)
    stat = Path(f"/proc/{record.with_suffix('.child').read_text()}/stat")
    assert not stat.exists() or stat.read_text().split()[2] == "Z"


@pytest.mark.spec("EG-DURABLE-KERNEL-R014.3.1")
def test_direct_repeat_requires_count(harness: tuple[dict[str, str], Path]) -> None:
    env, _ = harness
    result = subprocess.run(
        ["python3", str(ROOT / "scripts/raft_cpu_load.py"), "--repeat", "--", "cargo"],
        env=env,
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 2
    assert "RAFT_LOAD_REPEATS must be an integer in 2..100" in result.stderr
    assert "Traceback" not in result.stderr
