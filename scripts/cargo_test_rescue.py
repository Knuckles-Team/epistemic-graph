#!/usr/bin/env python3
"""Run `cargo test`; give each test of a signal-killed binary a verdict (EH-376).

    python3 scripts/cargo_test_rescue.py cargo test <cargo args> [-- <libtest args>]

A libtest binary runs all its tests in one process. When one test dies on a
signal -- a stack overflow is SIGABRT -- the whole binary dies with it: cargo
reports one failed target and every test that had not yet finished never gets
a verdict. Measured on 235fffb28: one overflow in
`bolt_atomic_commit_survives_backend_restart` hid 1,270 of 1,750 lib tests.

This wrapper runs the command unchanged, streaming its output. On success it
exits 0 and costs nothing else. When a target died on a signal, it re-runs ONLY
that target under `cargo nextest run` -- one process per test, same cargo
flags, same test filters -- and prints one verdict per test. The step still
fails: the gate is the signal death AND the per-test verdicts, reported together.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
from collections.abc import Iterable, Sequence
from dataclasses import dataclass

NEXTEST_VERSION = "0.9.143"
_ANSI = re.compile(r"\x1b\[[0-9;]*m")
_RERUN = re.compile(r"to rerun pass `([^`]+)`")
_SIGNAL = re.compile(r"process didn't exit successfully: `[^`]*` \(signal: (\d+)")
_VERDICT = re.compile(
    r"^\s+(PASS|FAIL|SIGABRT|SIGSEGV|SIGKILL|SIGBUS|TIMEOUT|ABORT)\s+\["
)
# Cargo options that select targets: the re-run replaces them with cargo's own
# `to rerun pass` selector. Values are consumed with the option.
_SELECTORS_WITH_VALUE = {
    "-p",
    "--package",
    "--test",
    "--bin",
    "--example",
    "--bench",
    "--exclude",
}
_SELECTORS = {
    "--no-fail-fast",  # not a selector: the re-run always passes nextest's own
    "--lib",
    "--bins",
    "--tests",
    "--examples",
    "--benches",
    "--all-targets",
    "--workspace",
    "--doc",
}
# Cargo options whose value must travel with them (so it is not taken for a filter).
_OPTIONS_WITH_VALUE = {
    "--features",
    "-F",
    "--target",
    "--target-dir",
    "-j",
    "--jobs",
    "--manifest-path",
    "--config",
}
_NEXTEST_FLAG = {
    "--nocapture": ["--no-capture"],
    "--ignored": ["--run-ignored", "only"],
    "--include-ignored": ["--run-ignored", "all"],
}


class RescueError(ValueError):
    """The command cannot be translated to an equivalent nextest selection."""


@dataclass(frozen=True)
class Selection:
    cargo_flags: tuple[str, ...]
    filters: tuple[str, ...]
    nextest_flags: tuple[str, ...]
    exact: bool


def signal_killed_targets(lines: Iterable[str]) -> list[str]:
    """Cargo's re-run selector for every target whose test process died on a signal."""
    killed: list[str] = []
    selector: str | None = None
    for raw in lines:
        line = _ANSI.sub("", raw)
        rerun = _RERUN.search(line)
        if rerun:
            selector = rerun.group(1)
        elif selector and _SIGNAL.search(line):
            killed.append(selector)
            selector = None
    return killed


def _split_cargo_args(args: Sequence[str]) -> tuple[list[str], list[str]]:
    flags: list[str] = []
    filters: list[str] = []
    it = iter(args)
    for arg in it:
        if arg in _SELECTORS_WITH_VALUE:
            next(it, None)
        elif arg in _SELECTORS or arg.split("=", 1)[0] in _SELECTORS_WITH_VALUE:
            continue
        elif arg in _OPTIONS_WITH_VALUE:
            flags += [arg, next(it, "")]
        elif arg.startswith("-"):
            flags.append(arg)
        else:
            filters.append(arg)
    return flags, filters


def _libtest_threads(arg: str, it) -> list[str]:
    value = arg.split("=", 1)[1] if "=" in arg else next(it, "")
    return ["--test-threads", value]


def _split_libtest_args(args: Sequence[str]) -> tuple[list[str], list[str], bool]:
    flags: list[str] = []
    filters: list[str] = []
    exact = False
    it = iter(args)
    for arg in it:
        if arg == "--exact":
            exact = True
        elif arg.startswith("--test-threads"):
            flags += _libtest_threads(arg, it)
        elif arg in _NEXTEST_FLAG:
            flags += _NEXTEST_FLAG[arg]
        elif arg.startswith("-"):
            raise RescueError(f"no nextest equivalent for libtest argument {arg!r}")
        else:
            filters.append(arg)
    return flags, filters, exact


def parse_selection(command: Sequence[str]) -> Selection:
    """Split `cargo test <cargo args> [-- <libtest args>]` into its nextest parts."""
    if list(command[:2]) != ["cargo", "test"]:
        raise RescueError("the wrapped command must start with `cargo test`")
    args = list(command[2:])
    cut = args.index("--") if "--" in args else len(args)
    cargo_flags, cargo_filters = _split_cargo_args(args[:cut])
    nextest_flags, libtest_filters, exact = _split_libtest_args(args[cut + 1 :])
    return Selection(
        tuple(cargo_flags),
        tuple(cargo_filters + libtest_filters),
        tuple(nextest_flags),
        exact,
    )


def nextest_command(selection: Selection, target: str) -> list[str]:
    """The nextest re-run of ONE killed target, with the original flags and filters."""
    command = ["cargo", "nextest", "run", *selection.cargo_flags, *target.split()]
    command += [*selection.nextest_flags, "--no-fail-fast"]
    if selection.exact and selection.filters:
        command += ["-E", " | ".join(f"test(={name})" for name in selection.filters)]
    else:
        command += list(selection.filters)
    return command


def _stream(command: Sequence[str]) -> tuple[int, list[str]]:
    lines: list[str] = []
    with subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        errors="replace",
    ) as proc:
        for line in proc.stdout or ():
            sys.stdout.write(line)
            lines.append(line)
    sys.stdout.flush()
    return proc.returncode, lines


def _ensure_nextest() -> bool:
    if shutil.which("cargo-nextest"):
        return True
    if os.environ.get("CI") != "true":
        print(
            "cargo-test-rescue: cargo-nextest is not installed on this host",
            file=sys.stderr,
        )
        return False
    installed = subprocess.run(
        ["cargo", "install", "--locked", f"cargo-nextest@{NEXTEST_VERSION}"]
    )
    return installed.returncode == 0


def verdicts(lines: Iterable[str]) -> list[tuple[str, str]]:
    """(status, `binary test`) for every per-test verdict nextest printed."""
    found: dict[str, str] = {}
    for raw in lines:
        line = _ANSI.sub("", raw)
        match = _VERDICT.match(line)
        if match:
            # nextest repeats each failure in its final summary; one verdict per test.
            found[line.split(")", 1)[-1].strip()] = match.group(1)
    return [(verdict, test) for test, verdict in found.items()]


def _report(target: str, results: list[tuple[str, str]], status: int) -> None:
    failed = [(verdict, test) for verdict, test in results if verdict != "PASS"]
    print(
        f"\n=== cargo-test-rescue: `{target}` died on a signal; "
        "per-test verdicts under nextest:"
    )
    for verdict, test in failed:
        print(f"RESCUE-VERDICT {verdict:8s} {test}")
    print(
        f"=== cargo-test-rescue: `{target}`: {len(results)} test(s) with a verdict, "
        f"{len(results) - len(failed)} passed, {len(failed)} failed "
        f"(nextest exit {status})"
    )


def rescue(command: Sequence[str], killed: Sequence[str]) -> None:
    """Re-run each killed target under nextest and report; never changes the verdict."""
    try:
        selection = parse_selection(command)
    except RescueError as exc:
        print(f"cargo-test-rescue: cannot re-run under nextest: {exc}", file=sys.stderr)
        return
    if not _ensure_nextest():
        return
    for target in killed:
        rerun = nextest_command(selection, target)
        print(
            f"\n=== cargo-test-rescue: re-running `{target}`: {' '.join(rerun)}",
            flush=True,
        )
        status, lines = _stream(rerun)
        _report(target, verdicts(lines), status)


def main(argv: Sequence[str]) -> int:
    if not argv:
        print(__doc__, file=sys.stderr)
        return 2
    status, lines = _stream(argv)
    if status == 0:
        return 0
    killed = signal_killed_targets(lines)
    if killed:
        rescue(argv, killed)
        print(
            f"=== cargo-test-rescue: gate FAILS: {len(killed)} target(s) "
            f"died on a signal: {killed}"
        )
    return status


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
