#!/usr/bin/env python3
"""Measurement harness — thin twin (mirrors the Python side's full implementation).

CONCEPT:EG-KG.build.measurement-harness-lite

Mirrors, on epistemic-graph's own gates/CI, the fuller measurement harness
built in `agent-utilities` (`agent_utilities/measurement/`, capabilities
A-G, one module per catalogued false-alarm incident — see that package's
README for the full incident list). Same convention this repo already uses
for `scripts/check_cargo_advisories.sh` mirroring `audit_dependencies.py`.

Deliberately a SEPARATE, dependency-free file rather than an
`agent-utilities` import, for one concrete reason: `epistemic-graph`
publishes as a self-contained wheel (`pip install epistemic-graph` alone,
no extras needed for the full engine — see this repo's own
`pyproject.toml` "FULL-BY-DEFAULT" comment on `[project.dependencies]`) and
`agent-utilities` already depends on `epistemic-graph` (`agent_utilities.
numeric`'s `xp` shim binds `epistemic_graph.numeric`, CONCEPT:KG-2.315) —
so an `epistemic-graph` -> `agent-utilities` dependency, even a dev/test
one, would create a two-repo circular dependency at install/CI-checkout
time. A gate or CI job in THIS repo must be runnable from a bare clone of
just `epistemic-graph`, before any sibling checkout exists. Hence: not a
full reimplementation of all 7 capabilities, just the subset this repo's
own gates/build actually need, kept small on purpose (see each function's
own docstring for its source incident).

Covers:
* `check_load` / `gate_or_raise`        -- capability B (load gating)
* `run`                                  -- capability D (exit-code-correct exec)
* `merged_tree` / `files_deleted_by_merge` -- capability E (git merge-tree)
* `run_background` / `poll`              -- capability G (systemd-run, redirect inside)
* `env_fingerprint`                      -- the sccache/RUSTC_WRAPPER half of
  capability A -- deliberately included because that incident (a Cargo
  `rustc-wrapper` config passing every LOCAL gate and killing every CI job)
  is literally an epistemic-graph incident, not a generic one.

NOT mirrored here (see `agent_utilities/measurement/README.md`'s "What's
not mechanical" for the full reasoning): full provenance headers, copy
integrity, and safe process targeting. Those are heavier and less specific
to this repo's own Rust/Cargo-centric gate surface; a lane that needs them
for an epistemic-graph script should invoke `agent-utilities`'s CLI/script
surface as a SUBPROCESS (not an import) if it must, exactly like any other
cross-repo tool invocation in this ecosystem.
"""

from __future__ import annotations

import dataclasses
import os
import shlex
import shutil
import subprocess
import sys
import uuid
from pathlib import Path

# --- B: load gating -----------------------------------------------------

TOO_LOADED_TO_MEASURE = "TOO_LOADED_TO_MEASURE"

#: Watched build-environment variables -- the sccache/rustc-wrapper incident's
#: own shape: `rustc-wrapper = "sccache"` passed every local gate (this host
#: has sccache) and killed every CI job in 24s (runners do not).
WATCHED_ENV_VARS = ("RUSTC_WRAPPER", "CARGO_BUILD_RUSTC_WRAPPER", "CARGO_TARGET_DIR")


class TooLoadedToMeasureError(Exception):
    """Raised by gate_or_raise() when load exceeds threshold -- never a number, never a silent pass."""


def default_threshold(cpu_count: int | None = None) -> float:
    override = os.environ.get("MEASUREMENT_LOAD_THRESHOLD")
    if override is not None:
        return float(override)
    n = cpu_count if cpu_count is not None else (os.cpu_count() or 1)
    return 1.5 * n


@dataclasses.dataclass(frozen=True)
class LoadStatus:
    status: str  # "OK" | "TOO_LOADED_TO_MEASURE" | "UNKNOWN_LOAD"
    load1: float | None
    threshold: float

    @property
    def ok(self) -> bool:
        return self.status == "OK"


def check_load(threshold: float | None = None) -> LoadStatus:
    """Danger zone this defends against: load ~62, swap exhausted, 24-core box."""
    t = threshold if threshold is not None else default_threshold()
    try:
        load1 = os.getloadavg()[0]
    except (OSError, AttributeError):
        return LoadStatus(status="UNKNOWN_LOAD", load1=None, threshold=t)
    status = "OK" if load1 <= t else TOO_LOADED_TO_MEASURE
    return LoadStatus(status=status, load1=load1, threshold=t)


def gate_or_raise(threshold: float | None = None) -> LoadStatus:
    result = check_load(threshold)
    if result.status == TOO_LOADED_TO_MEASURE:
        raise TooLoadedToMeasureError(
            f"{TOO_LOADED_TO_MEASURE}: load {result.load1:.2f} > threshold {result.threshold:.2f}"
        )
    return result


def env_fingerprint(watch: tuple[str, ...] = WATCHED_ENV_VARS) -> dict[str, str | None]:
    """The sccache incident's own instrument: record the build env vars that
    have, on this repo specifically, been the entire difference between a
    passing local gate and a failing CI run."""
    return {name: os.environ.get(name) for name in watch}


# --- D: exit-code-correct execution --------------------------------------


@dataclasses.dataclass(frozen=True)
class RunResult:
    cmd: list[str]
    returncode: int
    stdout: str
    stderr: str
    killed_by_signal: int | None

    @property
    def ok(self) -> bool:
        return self.killed_by_signal is None and self.returncode == 0


def run(cmd: list[str], *, timeout: float | None = None, **kwargs) -> RunResult:
    """Capture the REAL exit status of `cmd` -- never a pipeline stage's.

    `cmd | tail -N` then reading `$?` measures `tail`'s exit code, not the
    piped command's. This never goes through a shell at all.
    """
    if isinstance(cmd, str):
        raise TypeError("run() requires an argv list, not a shell string")
    proc = subprocess.run(
        cmd, capture_output=True, text=True, timeout=timeout, **kwargs
    )
    killed = -proc.returncode if proc.returncode < 0 else None
    return RunResult(
        cmd=list(cmd),
        returncode=proc.returncode,
        stdout=proc.stdout,
        stderr=proc.stderr,
        killed_by_signal=killed,
    )


# --- E: merged-tree helper -------------------------------------------------


def _git(repo: Path, *args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["git", "-C", str(repo), *args], capture_output=True, text=True, check=False
    )


def merged_tree(repo: Path, base: str, branch: str) -> str:
    """Return the tree OID `git merge-tree --write-tree base branch` would produce.

    Use this, never a two-dot `git diff base..branch`, to answer "what
    would land in base" -- a two-dot diff shows base's own later commits as
    apparent deletions from the branch side.
    """
    proc = _git(repo, "merge-tree", "--write-tree", base, branch)
    lines = proc.stdout.splitlines()
    if not lines or len(lines[0].strip()) < 40:
        raise RuntimeError(
            f"git merge-tree failed (rc={proc.returncode}): {proc.stderr or proc.stdout}"
        )
    return lines[0].strip()


def files_deleted_by_merge(repo: Path, base: str, branch: str) -> set[str]:
    tree = merged_tree(repo, base, branch)
    base_files = {
        ln
        for ln in _git(repo, "ls-tree", "-r", "--name-only", base).stdout.splitlines()
        if ln
    }
    merged_files = {
        ln
        for ln in _git(repo, "ls-tree", "-r", "--name-only", tree).stdout.splitlines()
        if ln
    }
    return base_files - merged_files


# --- G: background-run wrapper ---------------------------------------------


class SystemdRunUnavailableError(Exception):
    pass


@dataclasses.dataclass(frozen=True)
class BackgroundRun:
    unit_name: str
    log_path: Path
    launch_returncode: int


def run_background(
    cmd: str, *, unit_name: str | None = None, log_dir: Path | None = None
) -> BackgroundRun:
    """Launch `cmd` in a transient user systemd unit with the redirect INSIDE the command.

    Reproduces (twice) as: launching via `systemd-run ... bash -c "cmd"`
    WITHOUT the redirect inside the quoted command sends output to the
    systemd JOURNAL, not the log file an operator tails -- the run looks
    dead when it is not. The `{ cmd; } > log 2>&1` grouping (not a bare
    `cmd > log`) also matters for multi-stage `&&`-joined commands: without
    the group, the redirect binds only to the LAST stage.
    """
    systemd_run = shutil.which("systemd-run")
    if systemd_run is None:
        raise SystemdRunUnavailableError("systemd-run not found on PATH")
    unit = unit_name or f"eg-measurement-{uuid.uuid4().hex[:12]}"
    log_dir = Path(log_dir) if log_dir is not None else Path("/var/tmp")
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"{unit}.log"
    inner_cmd = f"{{ {cmd} ; }} > {shlex.quote(str(log_path))} 2>&1"
    argv = [
        systemd_run,
        "--user",
        "--collect",
        f"--unit={unit}",
        "/bin/bash",
        "-c",
        inner_cmd,
    ]
    proc = subprocess.run(argv, capture_output=True, text=True, check=False)
    return BackgroundRun(
        unit_name=unit, log_path=log_path, launch_returncode=proc.returncode
    )


def poll(log_path: Path) -> str:
    log_path = Path(log_path)
    if not log_path.exists():
        return ""
    return log_path.read_text(errors="replace")


if __name__ == "__main__":
    # Smoke-test entry point: `python3 scripts/measurement_lite.py`
    status = check_load()
    print(f"load: {status}")
    print(f"env_fingerprint: {env_fingerprint()}")
    print(f"interpreter: {sys.executable}")
