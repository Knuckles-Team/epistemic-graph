#!/usr/bin/env python3
"""Opt-in, bounded CPU contention for validate-raft-cluster.sh (R014.2)."""

from __future__ import annotations

import json
import multiprocessing
import os
import signal
import subprocess
import sys
import tempfile
import time
from multiprocessing.synchronize import Event
from pathlib import Path
from typing import IO

STOP_SIGNAL = 0
CLUSTER_TESTS = (
    "raft::tests::multi_node_group_join_then_leader_rebalance",
    "raft::tests::placement_admin_wire_rpc::placement_admin_wire_rpcs_move_data_across_a_real_three_node_cluster",
)


def settings() -> tuple[int, int]:
    values = []
    for name, default, limit in (
        ("RAFT_LOAD_WORKERS", "", 32),
        ("RAFT_LOAD_SECONDS", "300", 3600),
    ):
        raw = os.environ.get(name, default)
        if not raw.isascii() or not raw.isdecimal() or not 1 <= int(raw) <= limit:
            raise ValueError(f"{name} must be an integer in 1..{limit}")
        values.append(int(raw))
    return values[0], values[1]


def cpu_worker(seconds: int, ready: Event) -> None:
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
    deadline = time.monotonic() + seconds
    value = 1
    ready.set()
    while time.monotonic() < deadline:
        value = (value * 1664525 + 1013904223) & 0xFFFFFFFF


def stop_command(command: subprocess.Popen[bytes] | None) -> None:
    if command is None:
        return
    # The group may still contain children after Cargo itself exits.
    try:
        os.killpg(command.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        command.wait(timeout=1)
    except subprocess.TimeoutExpired:
        pass
    try:
        os.killpg(command.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    command.wait()


def interrupt(signum: int, _frame: object) -> None:
    global STOP_SIGNAL
    STOP_SIGNAL = signum


def start_workers(
    workers: list[multiprocessing.Process], count: int, seconds: int, started: float
) -> int | None:
    """Start the CPU workers; return an exit code if startup must abort."""
    for _ in range(count):
        ready = multiprocessing.Event()
        worker = multiprocessing.Process(target=cpu_worker, args=(seconds, ready))
        worker.start()
        workers.append(worker)
        remaining = seconds - (time.monotonic() - started)
        if not ready.wait(timeout=max(0, min(remaining, 5))):
            return 124 if time.monotonic() - started >= seconds else 125
        if STOP_SIGNAL:
            return 128 + STOP_SIGNAL
    return None


def abort_code(
    workers: list[multiprocessing.Process], started: float, seconds: int
) -> int | None:
    """Exit code if the running command must be aborted, else None."""
    if STOP_SIGNAL:
        return 128 + STOP_SIGNAL
    if time.monotonic() - started >= seconds:
        print("CPU load duration exhausted; cluster command aborted", flush=True)
        return 124
    if not all(worker.is_alive() for worker in workers):
        print("CPU load worker exited; cluster command aborted", flush=True)
        return 125
    return None


def command_exit_code(
    command: subprocess.Popen[bytes], workers: list[multiprocessing.Process]
) -> int:
    """Map the finished command's status, failing if a load worker died."""
    if STOP_SIGNAL:
        return 128 + STOP_SIGNAL
    rc = command.returncode
    if rc == 0 and not all(worker.is_alive() for worker in workers):
        rc = 125
    return rc if rc >= 0 else 128 - rc


def watch_command(
    command: subprocess.Popen[bytes],
    workers: list[multiprocessing.Process],
    started: float,
    seconds: int,
) -> int:
    """Wait for the cluster command while the load workers stay healthy."""
    while command.poll() is None:
        aborted = abort_code(workers, started, seconds)
        if aborted is not None:
            return aborted
        time.sleep(0.02)
    return command_exit_code(command, workers)


def supervise(
    argv: list[str], count: int, seconds: int, output: IO[str] | None = None
) -> int:
    workers: list[multiprocessing.Process] = []
    command = None
    started = time.monotonic()
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, interrupt)
    try:
        early = start_workers(workers, count, seconds, started)
        if early is not None:
            return early
        pids = ",".join(str(worker.pid) for worker in workers)
        print(
            f"CPU load started: workers={count} limit_seconds={seconds} pids={pids}",
            flush=True,
        )
        command = subprocess.Popen(
            argv,
            start_new_session=True,
            env={**os.environ, "RAFT_LOAD_PIDS": pids},
            stdout=output,
            stderr=subprocess.STDOUT if output else None,
        )
        return watch_command(command, workers, started, seconds)
    finally:
        # Cleanup itself must not be interrupted before every child is reaped.
        for signum in (signal.SIGINT, signal.SIGTERM):
            signal.signal(signum, signal.SIG_IGN)
        stop_command(command)
        for worker in workers:
            worker.terminate()
        for worker in workers:
            worker.join(timeout=1)
            if worker.is_alive():
                worker.kill()
                worker.join()
        print(
            f"CPU load stopped: workers={len(workers)} "
            f"elapsed_seconds={time.monotonic() - started:.3f}",
            flush=True,
        )


def repeat_count() -> int:
    raw = os.environ.get("RAFT_LOAD_REPEATS", "")
    if not raw.isascii() or not raw.isdecimal() or not 2 <= int(raw) <= 100:
        raise ValueError("RAFT_LOAD_REPEATS must be an integer in 2..100")
    return int(raw)


def repeated(argv: list[str], count: int, seconds: int) -> int:
    root = Path(os.environ.get("RAFT_LOAD_OUTPUT", "target/raft-cluster-runs"))
    root.mkdir(parents=True, exist_ok=True)
    run = Path(tempfile.mkdtemp(prefix="run-", dir=root))
    print(f"Repeated cluster evidence: {run}", flush=True)
    failed = 0
    with (run / "results.jsonl").open("w") as results:
        for repetition in range(1, repeat_count() + 1):
            for index, name in enumerate(CLUSTER_TESTS):
                log = run / f"repeat-{repetition}-test-{index}.log"
                with log.open("w") as output:
                    rc = supervise([*argv, name], count, seconds, output)
                if rc == 0 and "test result: ok. 1 passed;" not in log.read_text():
                    rc = 125
                results.write(
                    json.dumps(
                        {
                            "repetition": repetition,
                            "test": name,
                            "returncode": rc,
                            "trace": log.name,
                            "workers": count,
                        }
                    )
                    + "\n"
                )
                results.flush()
                failed = failed or rc
                if STOP_SIGNAL:
                    return 128 + STOP_SIGNAL
    return failed


def main() -> int:
    try:
        count, seconds = settings()
        if "RAFT_LOAD_REPEATS" in os.environ or sys.argv[1:2] == ["--repeat"]:
            repeat_count()
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    if sys.argv[1:] == ["--check-config"]:
        return 0
    if sys.argv[1:3] == ["--repeat", "--"]:
        return repeated(sys.argv[3:], count, seconds)
    if len(sys.argv) < 3 or sys.argv[1] != "--":
        print("usage: raft_cpu_load.py -- command [args...]", file=sys.stderr)
        return 2
    return supervise(sys.argv[2:], count, seconds)


if __name__ == "__main__":
    sys.exit(main())
