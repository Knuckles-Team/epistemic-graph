#!/usr/bin/env python3
"""Run a test command with bounded, attributable process-group lifecycle.

This is deliberately a small wrapper around an existing test command.  It does
not change cargo/libtest arguments, retry failures, or turn a failed test into
success.  The wrapper only adds a suite deadline, a watchdog for the currently
reported libtest case, and fail-closed teardown evidence when a command does
not finish.

The child is started in a fresh session.  Its process group is therefore an
exact containment boundary: diagnostics and TERM/grace/KILL cleanup identify
the root PID, process-group ID, and every visible descendant in that group.
Diagnostics are bounded and include thread and file-descriptor counts without
recording descriptor targets or environment variables.
"""

from __future__ import annotations

import argparse
import codecs
import json
import os
import re
import selectors
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, TextIO


DEFAULT_SUITE_TIMEOUT_SECONDS = 5_400.0
DEFAULT_TEST_TIMEOUT_SECONDS = 900.0
DEFAULT_TERM_GRACE_SECONDS = 30.0
DEFAULT_KILL_GRACE_SECONDS = 10.0
DEFAULT_DIAGNOSTIC_PIDS = 128
MAX_DIAGNOSTIC_PIDS = 512
MAX_DIAGNOSTIC_FDS = 4_096
MAX_WCHAN_LENGTH = 160
ORPHAN_DRAIN_GRACE_SECONDS = 2.0

RUNNING_BINARY_RE = re.compile(r"^\s*(?:Running|Doc-tests)\s+(?P<binary>.+?)\s*$")
TEST_PROGRESS_RE = re.compile(
    r"^test\s+(?P<name>.+?)\s+\.\.\."
    r"(?:\s+(?P<result>ok|FAILED|ignored|bench))?\s*$"
)


@dataclass(frozen=True)
class LifecycleConfig:
    """Validated bounds for one command invocation."""

    suite_timeout_seconds: float = DEFAULT_SUITE_TIMEOUT_SECONDS
    test_timeout_seconds: float = DEFAULT_TEST_TIMEOUT_SECONDS
    term_grace_seconds: float = DEFAULT_TERM_GRACE_SECONDS
    kill_grace_seconds: float = DEFAULT_KILL_GRACE_SECONDS
    diagnostic_pids: int = DEFAULT_DIAGNOSTIC_PIDS

    def __post_init__(self) -> None:
        for name in (
            "suite_timeout_seconds",
            "test_timeout_seconds",
            "term_grace_seconds",
            "kill_grace_seconds",
        ):
            value = getattr(self, name)
            if value <= 0:
                raise ValueError(f"{name} must be positive")
        if not 1 <= self.diagnostic_pids <= MAX_DIAGNOSTIC_PIDS:
            raise ValueError(
                f"diagnostic_pids must be between 1 and {MAX_DIAGNOSTIC_PIDS}"
            )


@dataclass
class Progress:
    """The bounded amount of libtest progress needed by the watchdog."""

    binary: str | None = None
    active_test: str | None = None
    test_started_at: float | None = None
    tests_completed: int = 0

    def observe(self, line: str, now: float) -> None:
        binary_match = RUNNING_BINARY_RE.match(line)
        if binary_match:
            self.binary = binary_match.group("binary")
            self.active_test = None
            self.test_started_at = None

        test_match = TEST_PROGRESS_RE.match(line)
        if not test_match:
            return
        result = test_match.group("result")
        if result is None:
            self.active_test = test_match.group("name")
            self.test_started_at = now
            return
        self.tests_completed += 1
        self.active_test = None
        self.test_started_at = None


@dataclass(frozen=True)
class ProcessSnapshot:
    """Safe, bounded identity and resource counters for one child."""

    pid: int
    ppid: int | None
    pgid: int | None
    session: int | None
    state: str | None
    threads: int | None
    fd_count: int | None
    fd_count_capped: bool
    wchan: str | None
    command: str | None

    def as_dict(self) -> dict[str, object]:
        return {
            "pid": self.pid,
            "ppid": self.ppid,
            "pgid": self.pgid,
            "session": self.session,
            "state": self.state,
            "threads": self.threads,
            "fd_count": self.fd_count,
            "fd_count_capped": self.fd_count_capped,
            "wchan": self.wchan,
            "command": self.command,
        }


def _read_proc_stat(pid: int) -> tuple[int, int, int, int, str] | None:
    """Read the stable identity fields from Linux ``/proc/<pid>/stat``."""

    try:
        contents = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
    except (OSError, UnicodeError):
        return None
    closing_paren = contents.rfind(")")
    if closing_paren < 0:
        return None
    fields = contents[closing_paren + 2 :].split()
    # After comm: state(3), ppid(4), pgrp(5), session(6).
    if len(fields) < 4:
        return None
    try:
        return (
            int(fields[1]),
            int(fields[2]),
            int(fields[3]),
            int(pid),
            fields[0],
        )
    except ValueError:
        return None


def _read_thread_count(pid: int) -> int | None:
    try:
        for line in Path(f"/proc/{pid}/status").read_text(
            encoding="utf-8", errors="replace"
        ).splitlines():
            if line.startswith("Threads:"):
                return int(line.split(":", 1)[1].strip())
    except (OSError, ValueError):
        pass
    return None


def _read_fd_count(pid: int) -> tuple[int | None, bool]:
    try:
        count = 0
        with os.scandir(f"/proc/{pid}/fd") as entries:
            for _ in entries:
                count += 1
                if count >= MAX_DIAGNOSTIC_FDS:
                    return count, True
        return count, False
    except OSError:
        return None, False


def _read_wchan(pid: int) -> str | None:
    try:
        return Path(f"/proc/{pid}/wchan").read_text(
            encoding="utf-8", errors="replace"
        ).strip()[:MAX_WCHAN_LENGTH]
    except OSError:
        return None


def _read_command(pid: int) -> str | None:
    # Use argv[0]/comm only.  Full argv can contain credentials or test data;
    # process identity does not require recording those arguments.
    try:
        raw = Path(f"/proc/{pid}/cmdline").read_bytes().split(b"\0", 1)[0]
        if raw:
            return raw.decode("utf-8", errors="replace")[:MAX_WCHAN_LENGTH]
    except OSError:
        pass
    try:
        return Path(f"/proc/{pid}/comm").read_text(
            encoding="utf-8", errors="replace"
        ).strip()[:MAX_WCHAN_LENGTH]
    except OSError:
        return None


def _snapshot_pid(pid: int) -> ProcessSnapshot | None:
    parsed = _read_proc_stat(pid)
    if parsed is None:
        return None
    ppid, pgid, session, _, state = parsed
    fd_count, fd_count_capped = _read_fd_count(pid)
    return ProcessSnapshot(
        pid=pid,
        ppid=ppid,
        pgid=pgid,
        session=session,
        state=state,
        threads=_read_thread_count(pid),
        fd_count=fd_count,
        fd_count_capped=fd_count_capped,
        wchan=_read_wchan(pid),
        command=_read_command(pid),
    )


def _group_snapshots(
    pgid: int, *, max_pids: int
) -> tuple[list[ProcessSnapshot], int]:
    """Return sorted members and the number omitted by the diagnostics bound."""

    pids: list[int] = []
    matching_count = 0
    try:
        with os.scandir("/proc") as proc_entries:
            for entry in proc_entries:
                if not entry.name.isdigit():
                    continue
                pid = int(entry.name)
                parsed = _read_proc_stat(pid)
                if parsed is not None and parsed[1] == pgid:
                    matching_count += 1
                    if len(pids) < max_pids:
                        pids.append(pid)
    except OSError:
        return [], 0
    pids.sort()
    snapshots = [snapshot for pid in pids if (snapshot := _snapshot_pid(pid))]
    return snapshots, max(0, matching_count - len(pids))


def _group_has_members(pgid: int) -> bool:
    snapshots, _ = _group_snapshots(pgid, max_pids=1)
    return bool(snapshots)


def _emit_evidence(
    *,
    suite_name: str,
    phase: str,
    started_at: float,
    root_pid: int | None = None,
    pgid: int | None = None,
    reason: str | None = None,
    active_test: str | None = None,
    test_started_at: float | None = None,
    binary: str | None = None,
    returncode: int | None = None,
    snapshots: Iterable[ProcessSnapshot] = (),
    omitted_members: int = 0,
) -> None:
    payload: dict[str, object] = {
        "event": "bounded_test_suite",
        "suite": suite_name,
        "phase": phase,
        "suite_elapsed_s": round(max(0.0, time.monotonic() - started_at), 3),
        "test_elapsed_s": (
            round(max(0.0, time.monotonic() - test_started_at), 3)
            if test_started_at is not None
            else None
        ),
        "root_pid": root_pid,
        "pgid": pgid,
        "reason": reason,
        "active_test": active_test,
        "binary": binary,
        "returncode": returncode,
        "members": [item.as_dict() for item in sorted(snapshots, key=lambda x: x.pid)],
        "omitted_members": omitted_members,
    }
    print(
        "LIFECYCLE_EVIDENCE "
        + json.dumps(payload, sort_keys=True, separators=(",", ":")),
        file=sys.stderr,
        flush=True,
    )


class _OutputPump:
    """Non-blocking merged stdout/stderr reader preserving child output."""

    def __init__(self, stream: TextIO, progress: Progress) -> None:
        self.stream = stream
        self.progress = progress
        self.selector = selectors.DefaultSelector()
        self.selector.register(stream, selectors.EVENT_READ)
        self.decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
        self.pending = ""
        self.closed = False
        self.partial_progress_name: str | None = None

    def read(self, timeout: float) -> bool:
        if self.closed:
            return False
        events = self.selector.select(max(0.0, timeout))
        if not events:
            return False
        fd = events[0][0].fileobj.fileno()
        try:
            chunk = os.read(fd, 65_536)
        except OSError:
            chunk = b""
        if not chunk:
            self._finish()
            return False
        self.pending += self.decoder.decode(chunk, final=False)
        self._write_complete_lines()
        self._observe_partial_progress()
        return True

    def _write_complete_lines(self) -> None:
        while "\n" in self.pending:
            line, self.pending = self.pending.split("\n", 1)
            line += "\n"
            sys.stdout.write(line)
            sys.stdout.flush()
            self.progress.observe(line.rstrip("\r\n"), time.monotonic())
            self.partial_progress_name = None

    def _observe_partial_progress(self) -> None:
        # libtest prints `test NAME ... ` and flushes before entering a test;
        # the result/newline arrives later. Watch that partial marker too, so
        # the per-test deadline does not silently degrade into suite-only
        # protection when stdout is a pipe rather than a terminal.
        match = TEST_PROGRESS_RE.match(self.pending)
        if match is None or match.group("result") is not None:
            return
        name = match.group("name")
        if name == self.partial_progress_name:
            return
        self.partial_progress_name = name
        self.progress.observe(self.pending, time.monotonic())

    def _finish(self) -> None:
        if self.closed:
            return
        self.pending += self.decoder.decode(b"", final=True)
        if self.pending:
            sys.stdout.write(self.pending)
            sys.stdout.flush()
            self.progress.observe(self.pending.rstrip("\r\n"), time.monotonic())
            self.pending = ""
        self.selector.close()
        self.closed = True

    def close(self) -> None:
        self._finish()
        try:
            self.stream.close()
        except OSError:
            pass


def _signal_group(pgid: int, signum: signal.Signals) -> str | None:
    try:
        os.killpg(pgid, signum)
    except ProcessLookupError:
        return "already-exited"
    except OSError as exc:
        return f"{type(exc).__name__}: {exc}"
    return None


def _drain_until(
    process: subprocess.Popen[bytes],
    pump: _OutputPump,
    deadline: float,
) -> None:
    while time.monotonic() < deadline:
        pump.read(min(0.1, max(0.0, deadline - time.monotonic())))
        if process.poll() is not None and pump.closed:
            return


def _contain(
    process: subprocess.Popen[bytes],
    pump: _OutputPump,
    progress: Progress,
    *,
    suite_name: str,
    started_at: float,
    pgid: int,
    config: LifecycleConfig,
    reason: str,
) -> int:
    before_term, omitted = _group_snapshots(
        pgid, max_pids=config.diagnostic_pids
    )
    _emit_evidence(
        suite_name=suite_name,
        phase="timeout_detected",
        started_at=started_at,
        root_pid=process.pid,
        pgid=pgid,
        reason=reason,
        active_test=progress.active_test,
        test_started_at=progress.test_started_at,
        binary=progress.binary,
        snapshots=before_term,
        omitted_members=omitted,
    )

    term_error = _signal_group(pgid, signal.SIGTERM)
    _emit_evidence(
        suite_name=suite_name,
        phase="term_sent",
        started_at=started_at,
        root_pid=process.pid,
        pgid=pgid,
        reason=term_error or reason,
        active_test=progress.active_test,
        test_started_at=progress.test_started_at,
        binary=progress.binary,
    )
    _drain_until(
        process,
        pump,
        time.monotonic() + config.term_grace_seconds,
    )

    after_term, omitted = _group_snapshots(
        pgid, max_pids=config.diagnostic_pids
    )
    if process.poll() is None or after_term:
        kill_error = _signal_group(pgid, signal.SIGKILL)
        _emit_evidence(
            suite_name=suite_name,
            phase="kill_sent",
            started_at=started_at,
            root_pid=process.pid,
            pgid=pgid,
            reason=kill_error or reason,
            active_test=progress.active_test,
            test_started_at=progress.test_started_at,
            binary=progress.binary,
            snapshots=after_term,
            omitted_members=omitted,
        )
        _drain_until(
            process,
            pump,
            time.monotonic() + config.kill_grace_seconds,
        )

    try:
        process.wait(timeout=config.kill_grace_seconds)
    except subprocess.TimeoutExpired:
        # The group was already addressed above; retain a bounded final
        # diagnostic rather than waiting indefinitely for a broken reaper.
        pass
    survivors, omitted = _group_snapshots(
        pgid, max_pids=config.diagnostic_pids
    )
    _emit_evidence(
        suite_name=suite_name,
        phase="reaped" if not survivors else "containment_incomplete",
        started_at=started_at,
        root_pid=process.pid,
        pgid=pgid,
        reason=reason,
        active_test=progress.active_test,
        test_started_at=progress.test_started_at,
        binary=progress.binary,
        returncode=process.returncode,
        snapshots=survivors,
        omitted_members=omitted,
    )
    pump.close()
    # 124 is the conventional timeout status and is intentionally distinct
    # from a child's assertion/signal status. Never convert a test failure to
    # success or retry it here.
    if process.returncode not in (None, 0) and reason == "orphaned_child_output":
        return process.returncode
    return 124 if not survivors else 125


def run_bounded_command(
    command: list[str], *, suite_name: str, config: LifecycleConfig
) -> int:
    if not command:
        raise ValueError("a command is required after --")
    started_at = time.monotonic()
    progress = Progress()
    try:
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            bufsize=0,
        )
    except OSError as exc:
        _emit_evidence(
            suite_name=suite_name,
            phase="spawn_failed",
            started_at=started_at,
            reason=f"{type(exc).__name__}: {exc}",
        )
        return 127

    if process.stdout is None:
        raise RuntimeError("bounded runner could not capture child output")
    try:
        pgid = os.getpgid(process.pid)
    except OSError as exc:
        _emit_evidence(
            suite_name=suite_name,
            phase="attribution_failed",
            started_at=started_at,
            root_pid=process.pid,
            reason=f"{type(exc).__name__}: {exc}",
        )
        process.kill()
        process.wait()
        process.stdout.close()
        return 125

    if pgid != process.pid:
        _emit_evidence(
            suite_name=suite_name,
            phase="attribution_failed",
            started_at=started_at,
            root_pid=process.pid,
            pgid=pgid,
            reason="start_new_session did not establish a private process group",
        )
        return _contain(
            process,
            _OutputPump(process.stdout, progress),
            progress,
            suite_name=suite_name,
            started_at=started_at,
            pgid=pgid,
            config=config,
            reason="attribution_failed",
        )

    pump = _OutputPump(process.stdout, progress)
    _emit_evidence(
        suite_name=suite_name,
        phase="started",
        started_at=started_at,
        root_pid=process.pid,
        pgid=pgid,
    )
    orphan_deadline: float | None = None
    timeout_reason: str | None = None
    while True:
        now = time.monotonic()
        if process.poll() is None:
            suite_remaining = config.suite_timeout_seconds - (now - started_at)
            if suite_remaining <= 0:
                timeout_reason = "suite_timeout"
                break
            if progress.test_started_at is not None:
                test_remaining = config.test_timeout_seconds - (
                    now - progress.test_started_at
                )
                if test_remaining <= 0:
                    timeout_reason = "test_timeout"
                    break
            else:
                test_remaining = config.test_timeout_seconds
            pump.read(min(0.2, suite_remaining, test_remaining))
            continue

        # A cargo process can exit while a detached/child process retains the
        # merged pipe. Give that exact process group a short bounded drain, then
        # contain it as an orphan rather than waiting forever on EOF.
        if orphan_deadline is None:
            orphan_deadline = now + ORPHAN_DRAIN_GRACE_SECONDS
        if pump.closed:
            break
        if now >= orphan_deadline:
            timeout_reason = "orphaned_child_output"
            break
        pump.read(min(0.1, max(0.0, orphan_deadline - now)))

    if timeout_reason is not None:
        return _contain(
            process,
            pump,
            progress,
            suite_name=suite_name,
            started_at=started_at,
            pgid=pgid,
            config=config,
            reason=timeout_reason,
        )

    returncode = process.wait()
    survivors, omitted = _group_snapshots(
        pgid, max_pids=config.diagnostic_pids
    )
    if survivors:
        # A successful cargo exit must not hide a leaked child. Contain the
        # exact group and fail the gate; a non-zero test result remains intact.
        _emit_evidence(
            suite_name=suite_name,
            phase="orphan_detected",
            started_at=started_at,
            root_pid=process.pid,
            pgid=pgid,
            reason="child_process_group_survived_root_exit",
            returncode=returncode,
            snapshots=survivors,
            omitted_members=omitted,
        )
        containment_rc = _contain(
            process,
            pump,
            progress,
            suite_name=suite_name,
            started_at=started_at,
            pgid=pgid,
            config=config,
            reason="orphaned_child",
        )
        return containment_rc if returncode == 0 else returncode

    pump.close()
    _emit_evidence(
        suite_name=suite_name,
        phase="completed",
        started_at=started_at,
        root_pid=process.pid,
        pgid=pgid,
        returncode=returncode,
    )
    return returncode


def _positive_float(value: str) -> float:
    parsed = float(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return parsed


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return parsed


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run a test command with bounded process-group lifecycle."
    )
    parser.add_argument("--suite-name", required=True)
    parser.add_argument(
        "--suite-timeout",
        type=_positive_float,
        default=DEFAULT_SUITE_TIMEOUT_SECONDS,
        metavar="SECONDS",
    )
    parser.add_argument(
        "--test-timeout",
        type=_positive_float,
        default=DEFAULT_TEST_TIMEOUT_SECONDS,
        metavar="SECONDS",
    )
    parser.add_argument(
        "--term-grace",
        type=_positive_float,
        default=DEFAULT_TERM_GRACE_SECONDS,
        metavar="SECONDS",
    )
    parser.add_argument(
        "--kill-grace",
        type=_positive_float,
        default=DEFAULT_KILL_GRACE_SECONDS,
        metavar="SECONDS",
    )
    parser.add_argument(
        "--diagnostic-pids",
        type=_positive_int,
        default=DEFAULT_DIAGNOSTIC_PIDS,
        metavar="COUNT",
    )
    parser.add_argument("command", nargs=argparse.REMAINDER)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    command = list(args.command)
    if command and command[0] == "--":
        command.pop(0)
    if not command:
        print("bounded_test_runner: command is required after --", file=sys.stderr)
        return 2
    try:
        config = LifecycleConfig(
            suite_timeout_seconds=args.suite_timeout,
            test_timeout_seconds=args.test_timeout,
            term_grace_seconds=args.term_grace,
            kill_grace_seconds=args.kill_grace,
            diagnostic_pids=args.diagnostic_pids,
        )
    except ValueError as exc:
        print(f"bounded_test_runner: {exc}", file=sys.stderr)
        return 2
    return run_bounded_command(command, suite_name=args.suite_name, config=config)


if __name__ == "__main__":
    raise SystemExit(main())
