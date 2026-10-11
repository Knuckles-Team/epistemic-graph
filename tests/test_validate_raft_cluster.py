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
if os.environ.get("CHECK_CPU") and Path("/proc").exists():
    def ticks(pid):
        return sum(int(v) for v in Path(f"/proc/{pid}/stat").read_text().split()[13:15])
    before = [ticks(pid) for pid in pids]
    time.sleep(.2)
    assert all(ticks(pid) > old for pid, old in zip(pids, before))
if os.environ.get("KILL_WORKER"):
    os.kill(pids[0], signal.SIGKILL)
time.sleep(float(os.environ.get("TEST_SECONDS", ".2")))
sys.exit(int(os.environ.get("TEST_RC", "0")))
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
