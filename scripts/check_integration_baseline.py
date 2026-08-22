#!/usr/bin/env python3
"""Run the integration suite and gate on REGRESSIONS, not on absolute green.

Why this exists
---------------
Until 2026-08-21 this suite could not run at all. ``conftest.py``'s
``start_epistemic_graph_server`` falls back to ``cargo build --features full``
INSIDE a fixture, and fixture setup runs inside pytest-timeout's per-test 60s
window, so a cold target produced ~500 ``Timeout (>60.0s)`` errors — every one
of them about the build, none about the code. The gate reported catastrophe and
carried no information.

Building the engine before pytest fixed that, and the first real run was 513
passed / 17 failed. Those 17 were then checked against ``origin/main`` file by
file: every failing test module, and every source it exercises, was byte-
identical there. They are pre-existing debt the timeouts had been masking, not
regressions — so blocking every push on them means blocking on a backlog that
predates the branch under test.

This is the same problem the merge queue already solved by gating differentially
against a base ref, and the same shape as ``protocol_unbound_baseline.txt``: a
committed list of known-bad entries, each carrying an owner and a review date,
that fails the moment anything NEW joins it or anything on it starts passing.
A regression still fails the gate. A fixed test still fails the gate, until its
line is removed. What no longer happens is a release blocked by debt nobody
introduced.
"""

from __future__ import annotations

import argparse
import datetime
import re
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
BASELINE_PATH = REPO_ROOT / "tests" / "integration_failure_baseline.txt"

#: `FAILED tests/x.py::test_y - AssertionError: ...` and the ERROR equivalent.
#: The reason after " - " is deliberately dropped: this gate tracks WHICH tests
#: fail, never WHY, so a baselined test whose failure mode changes is still
#: baselined. That is a real limitation, stated rather than hidden.
#:
#: Matched ONLY inside pytest's "short test summary info" block. Applying it to
#: the whole output was a real defect: pytest pads captured log records to a
#: level column, so a line like
#:     ERROR    asyncio:base_events.py:1785 Task was destroyed but it is pending!
#: begins with ERROR + whitespace and was read as a failing node id. The gate
#: then reported a REGRESSION for a test that does not exist, on a run whose
#: counts (17 failed / 513 passed) matched the baseline exactly — a gate
#: inventing a failure is precisely what this one exists to prevent.
#: A pytest id may contain spaces (parametrised ids embed the parameter text),
#: so the node is everything up to the reason separator rather than `\S+`.
#: Captures the ENTIRE remainder; `_node_id` decides where the id ends, because
#: that decision needs to know whether a `[...]` parameter section is open and
#: a regex alternation cannot express that without becoming unreadable.
_OUTCOME = re.compile(r"^(?:FAILED|ERROR)\s+(.+)$")

#: `  # owner=` is the separator, not a bare `#`, because a parametrised id can
#: legitimately contain one.
_ENTRY = re.compile(
    r"^(?P<node>.+?)\s\s+#\s*owner=(?P<owner>\S+)\s+review-by=(?P<date>\d{4}-\d{2}-\d{2})\s*(?:note=(?P<note>\S+))?\s*$"
)


def _base(node: str) -> str:
    """`path::test[param]` -> `path::test`."""
    return node.split("[", 1)[0]


def parse_baseline(text: str) -> tuple[set[str], list[tuple[str, datetime.date]]]:
    """Return (node ids, [(node id, review-by)]) — refusing a malformed line."""
    nodes: set[str] = set()
    dated: list[tuple[str, datetime.date]] = []
    for number, raw in enumerate(text.splitlines(), start=1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        match = _ENTRY.match(line)
        if match is None:
            raise SystemExit(
                f"{BASELINE_PATH.name}:{number}: malformed entry. Every line must be\n"
                f"  <test-node-id>  # owner=@handle review-by=YYYY-MM-DD [note=tag]\n"
                f"got: {line!r}"
            )
        nodes.add(match.group("node"))
        dated.append(
            (match.group("node"), datetime.date.fromisoformat(match.group("date")))
        )
    return nodes, dated


#: pytest's own delimiter for the block that lists failing node ids. Everything
#: before it is test progress and captured output; everything after belongs to
#: the run epilogue.
_SUMMARY_START = re.compile(r"^=+\s*short test summary info\s*=+$")
_SUMMARY_END = re.compile(r"^=+.*(?:passed|failed|error|no tests ran).*=+$")


def parse_outcomes(output: str) -> set[str]:
    """Failing/erroring node ids, read only from pytest's summary block.

    Scanning the whole output cannot work: captured log records share the
    `ERROR <text>` prefix and are indistinguishable from summary lines by shape
    alone. Bounding the scan to the block pytest itself delimits removes the
    ambiguity instead of trying to out-guess it with a cleverer pattern.
    """

    found: set[str] = set()
    inside = False
    for raw in output.splitlines():
        line = raw.strip()
        if not inside:
            if _SUMMARY_START.match(line):
                inside = True
            continue
        if _SUMMARY_END.match(line):
            break
        match = _OUTCOME.match(line)
        if match is not None:
            found.add(_node_id(match.group(1)))
    return found


def _node_id(captured: str) -> str:
    """Strip pytest's ` - <reason>` suffix without truncating a parametrised id.

    pytest writes `FAILED <nodeid> - <reason>`, but a parametrised id can itself
    contain " - " — this repo has several, e.g.
    `test_inventory_drift_fails_closed[... does not exactly cover ...]`. Cutting
    at the first " - " silently shortens those, and a shortened id then fails to
    match its own baseline entry. So: when the id carries a `[...]` parameter
    section, keep everything through its final `]`; only outside a parameter
    section does " - " introduce the reason.
    """

    if "[" in captured:
        close = captured.rfind("]")
        if close != -1:
            return captured[: close + 1].strip()
    head, separator, _ = captured.partition(" - ")
    return (head if separator else captured).strip()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--today",
        default=None,
        help="Override today's date (YYYY-MM-DD) for the review-by ratchet.",
    )
    parser.add_argument("pytest_args", nargs=argparse.REMAINDER)
    namespace = parser.parse_args()

    baseline_text = BASELINE_PATH.read_text(encoding="utf-8")
    baseline, dated = parse_baseline(baseline_text)

    arguments = namespace.pytest_args
    if arguments and arguments[0] == "--":
        arguments = arguments[1:]
    completed = subprocess.run(
        [sys.executable, "-m", "pytest", *arguments],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        stdin=subprocess.DEVNULL,
    )
    output = completed.stdout + completed.stderr
    print(output)

    failing = parse_outcomes(output)
    # An exit code that reports neither "all passed" (0) nor "tests failed" (1)
    # means pytest itself broke — a collection error, an internal error, an
    # interrupt. Its failure list is not trustworthy, so the gate must not
    # reason about it at all.
    if completed.returncode not in (0, 1):
        print(
            f"REFUSED: pytest exited {completed.returncode}, which is not a test "
            f"verdict (0=all passed, 1=tests failed). Nothing was compared to the "
            f"baseline.",
            file=sys.stderr,
        )
        return 1

    # A baseline entry written WITHOUT brackets covers every parametrisation of
    # that test. Three of the known failures are parametrised variants of one
    # `test_inventory_drift_fails_closed` case whose ids embed multi-line source
    # snippets; listing each verbatim would be brittle for no added precision,
    # since they share one root cause. An entry WITH brackets still matches only
    # that exact case.
    covered = {node for node in failing if node in baseline or _base(node) in baseline}
    regressions = sorted(failing - covered)
    satisfied = {
        entry
        for entry in baseline
        for node in failing
        if node == entry or _base(node) == entry
    }
    repaired = sorted(baseline - satisfied)
    stale = sorted(
        node
        for node, review in dated
        if review
        < (
            datetime.date.fromisoformat(namespace.today)
            if namespace.today
            else datetime.date.today()
        )
    )

    problems = False
    if regressions:
        problems = True
        print(
            "\nREGRESSION — these tests are NOT in the baseline and are failing now:",
            file=sys.stderr,
        )
        for node in regressions:
            print(f"  {node}", file=sys.stderr)
        print(
            "Fix them. Adding a line to the baseline is for debt this change did "
            "not introduce, never for a failure it did.",
            file=sys.stderr,
        )
    if repaired:
        problems = True
        print(
            "\nFIXED — these are baselined but now PASS. Remove their lines from "
            f"{BASELINE_PATH.name} so the ratchet keeps tightening:",
            file=sys.stderr,
        )
        for node in repaired:
            print(f"  {node}", file=sys.stderr)
    if stale:
        problems = True
        print(
            "\nOVERDUE — these baseline entries are past their review-by date. "
            "Fix the test or move the date deliberately (a real decision):",
            file=sys.stderr,
        )
        for node in stale:
            print(f"  {node}", file=sys.stderr)

    if problems:
        return 1
    if failing:
        print(
            f"\nintegration baseline: OK — {len(failing)} known failure(s), no "
            f"regressions. See {BASELINE_PATH.name}."
        )
    else:
        print("\nintegration baseline: OK — nothing failing at all.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
