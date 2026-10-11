#!/usr/bin/env python3
"""Opt-in, bounded CPU contention for validate-raft-cluster.sh (R014.2)."""

from __future__ import annotations

import multiprocessing
import os
import signal
import subprocess
import sys
import time
from multiprocessing.synchronize import Event

STOP_SIGNAL = 0


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


def supervise(argv: list[str], count: int, seconds: int) -> int:
    workers: list[multiprocessing.Process] = []
    command = None
    started = time.monotonic()
    rc = 125
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, interrupt)
    try:
        for _ in range(count):
            ready = multiprocessing.Event()
            worker = multiprocessing.Process(target=cpu_worker, args=(seconds, ready))
            worker.start()
            workers.append(worker)
            if not ready.wait(timeout=min(seconds, 5)):
                return 125
            if STOP_SIGNAL:
                return 128 + STOP_SIGNAL
        pids = ",".join(str(worker.pid) for worker in workers)
        print(
            f"CPU load started: workers={count} limit_seconds={seconds} pids={pids}",
            flush=True,
        )
        command = subprocess.Popen(
            argv, start_new_session=True, env={**os.environ, "RAFT_LOAD_PIDS": pids}
        )
        while command.poll() is None:
            if STOP_SIGNAL:
                return 128 + STOP_SIGNAL
            if time.monotonic() - started >= seconds:
                print(
                    "CPU load duration exhausted; cluster command aborted", flush=True
                )
                return 124
            if not all(worker.is_alive() for worker in workers):
                print("CPU load worker exited; cluster command aborted", flush=True)
                return 125
            time.sleep(0.02)
        rc = command.returncode
        if rc == 0 and not all(worker.is_alive() for worker in workers):
            rc = 125
        return rc if rc >= 0 else 128 - rc
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


def main() -> int:
    try:
        count, seconds = settings()
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    if sys.argv[1:] == ["--check-config"]:
        return 0
    if len(sys.argv) < 3 or sys.argv[1] != "--":
        print("usage: raft_cpu_load.py -- command [args...]", file=sys.stderr)
        return 2
    return supervise(sys.argv[2:], count, seconds)


if __name__ == "__main__":
    sys.exit(main())
