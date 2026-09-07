#!/usr/bin/env python3
"""Local replica of every workflow under .github/workflows/.

WHY THIS PARSES THE WORKFLOW FILES INSTEAD OF HAND-COPYING THEIR STEPS: a
hand-copied step list silently drifts from the real workflow the moment
either changes — exactly the failure mode this script exists to close (eg's
CI was red for two releases on a test the local pre-push tier never ran). So
this script reads every workflow file itself, at run time, and classifies
every step it finds:

  * a step with a `run:` shell block in a job this repo has marked EXECUTABLE
    (see WORKFLOW_REGISTRY below) is executed VERBATIM (its literal shell
    text, `${{ ... }}` GitHub Actions expressions stripped to empty — see
    _strip_gha_expressions — except `${{ matrix.* }}` references inside a
    `strategy.matrix` job, which are substituted with the real per-leg
    value first, see _substitute_matrix), with $GITHUB_ENV/$GITHUB_OUTPUT/
    $GITHUB_PATH shimmed the same way GH Actions threads state between steps
    of one job. A new `run:` step added to a known job is run here with NO
    code change.
  * a step with `uses:` naming a known environment-setup action (checkout,
    setup-python, rust-toolchain, rust-cache, setup-uv) is a silent no-op
    locally — the dev machine already has these tools.
  * a step with `uses:` naming an artifact-transfer action (upload/download-
    artifact) is a silent no-op — it moves files between CI jobs, not logic.
  * any other `uses:` step (a marketplace build/publish action with no local
    equivalent, or a job that calls a reusable workflow with no `steps:` at
    all) is reported LOUDLY as "NOT VALIDATED LOCALLY", never silently
    skipped and never counted toward a pass.
  * an entire JOB can be marked out-of-scope (a WORKFLOW_REGISTRY entry's
    job_skip_reasons) when NONE of its steps can run locally at all (a
    5-platform native build matrix, a tag-gated Docker/PyPI publish, a
    windows-latest-only job on this Linux dev host) — every one of its
    steps still shows up in the summary as NOT VALIDATED LOCALLY, with the
    reason. A `strategy.matrix` job (executable or skip-reasoned) is
    expanded per matrix leg (see _matrix_combinations) so e.g. a 10-way
    feature-build matrix shows up as 10 distinct rows, not one.

EVERY workflow file under .github/workflows/ is covered. WORKFLOW_REGISTRY
carries a `blocking` flag per workflow (release.yml's jobs block the
pre-push gate on failure; repro-diagnose.yml — workflow_dispatch-only,
never on push/PR — never blocks) — "blocking" only changes whether a red
step FAILS the overall run, never whether it RUNS or gets REPORTED.

Historical note: this repo used to split checks across release.yml
(blocking) and a second file, advisory.yml, where every step carried its
own `continue-on-error: true`. That file was retired 2026-08-28
(wD9-CIGATE): a `push: branches: [main]` on release.yml already publishes
to PyPI, so anything living outside it wasn't merely non-blocking, it was
outside the publish chain entirely — a ratchet by this project's
definition. Its jobs are now release.yml jobs with no `continue-on-error`
(see WORKFLOW_REGISTRY['release.yml']'s comments for the couple of
exceptions still disclosed as open gaps rather than silently accepted).
Before an earlier redesign, this script only replayed release.yml's
`gates` job, so an entire workflow (the-then advisory.yml — a 10-job
feature-build matrix plus the perf/recall bench gate) was never validated
locally at all, invisible until a push broke it in GitHub-hosted CI; that
gap is what widened WORKFLOW_REGISTRY to cover every file in the first
place.

THE ANTI-DRIFT GUARANTEE (--consistency-check): every workflow file present
under .github/workflows/ must be registered in WORKFLOW_REGISTRY, and every
job each registered workflow declares must be classified as either
EXECUTABLE or explicitly skip-reasoned. A workflow file with no registry
entry, or a job that is neither classified (a brand new top-level job, or a
renamed one), makes the consistency check — and therefore the whole run —
FAIL LOUDLY rather than silently ignoring it. The same check also parses
.cargo/config.toml for an external-binary build dependency (rustc-wrapper,
a target linker/runner, a `-fuse-ld=<x>`/`-C linker=<x>` in rustflags) that
none of the workflow files ever install — the exact class of break commit
652f91c shipped (`rustc-wrapper = "sccache"` with no workflow step that
installs sccache; CI hard-errors at the first cargo invocation because cargo
does not soft-fall-back when a configured rustc-wrapper is missing). See the
module docstring tests in tests/test_ci_gate_replica_consistency_check.py for
proof both classes of drift actually fire.

LOCAL-HOST TOOLCHAIN DEGRADATION (GAP 4, see that section below): the mirror
image of the .cargo/config.toml check above. A RUN step whose OWN cargo
invocation names (directly, or transitively through the root Cargo.toml's
`[features]` graph — a `--features full-extras` implies `ros2-rmw`, for
example) a feature this repo documents as needing an external build-time
tool (e.g. `cmake` for `ros2-rmw`'s vendored CycloneDDS C build) that THIS
ONE machine doesn't have on PATH right now is reclassified from RUN to
NOT_VALIDATED_LOCALLY before it is ever attempted — reported loudly, in the
summary, by name, never silently and never counted as a pass. This is never
a CI defect (a GitHub-hosted runner, or a different dev host, may have the
tool) — it is the opposite failure mode from the runner-side check: "this
host can't validate this step" vs. "no host CI runs on ever could". The
detection reads the step's actual command text, not a job/workflow name, so
it applies uniformly wherever a heavy, toolchain-gated feature combination
is invoked — see check_toolchain_requirements.

Usage:
  scripts/ci_gate_replica.py                    # full run (heavy — pre-push/manual only)
  scripts/ci_gate_replica.py --dry-run           # print the plan, execute nothing
  scripts/ci_gate_replica.py --consistency-check # only the anti-drift check
  scripts/ci_gate_replica.py --skip-safe [REF]   # is it safe to skip this gate for the diff vs REF (default HEAD)?
  scripts/ci_gate_replica.py --workflows-dir DIR # override the workflows directory (testing)

LOCAL CARGO RESOURCE GUARD: every executed step receives a bounded
`CARGO_BUILD_JOBS` environment value. The default is `min(4, detected CPUs)`
with a floor of 1. Operators may provide only `CI_GATE_CARGO_BUILD_JOBS`; it
must be a strict positive decimal integer, and values above the hard local
maximum of 8 are clamped to 8. The ordinary `CARGO_BUILD_JOBS` environment
variable is deliberately ignored as an operator override. This changes only
the local build resource bound; each workflow step's shell text is preserved.

SAME-INVOCATION EXECUTION EVIDENCE: when running as a hook, this process is
the single producer for a private, HMAC-protected evidence ledger. Exact
selection duplicates in one derived plan execute once. Later hooks can reuse
only a successful, source/dirty-diff/lockfile/toolchain/effective-environment
identical record, or a versioned subset proof declared by
``scripts/push_gate_evidence.py``. Missing, partial, failed, stale, or
unverifiable evidence never changes the command plan and falls through to
normal execution.
"""

from __future__ import annotations

import argparse
import datetime
import os
import shutil
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
_SCRIPTS_DIR = REPO_ROOT / "scripts"
for _entry in (str(REPO_ROOT), str(_SCRIPTS_DIR)):
    if _entry not in sys.path:
        sys.path.insert(0, _entry)

# ─────────────────────────────────────────────────────────────────────────
# This file is the executable entry point and the module every consumer loads
# (`python3 scripts/ci_gate_replica.py`, the pre-commit hooks, release.yml, and
# the meta-tests, which import it by path). It owns step EXECUTION and the CLI;
# the cohesive parts it composes live in the `ci_replica` package, whose names
# are re-exported here so that surface is unchanged. See ci_replica/__init__.py
# for the dependency direction.
# ─────────────────────────────────────────────────────────────────────────
from ci_replica.build_toolchain import (  # noqa: E402
    TOOLCHAIN_FEATURE_REQUIREMENTS,
    _expand_features,
    _extract_requested_features,
    _feature_names,
    _load_cargo_features,
    _parse_cargo_config_sections,
    _target_toolchain_binaries,
    _wrapper_binary,
    check_build_tool_dependencies,
    check_toolchain_requirements,
    find_required_build_binaries,
)
from ci_replica.drift import (  # noqa: E402
    DriftReport,
    consistency_check,
)
from ci_replica.registry import (  # noqa: E402
    ARTIFACT_IO_ACTIONS,
    BUILD_AFFECTING_FILE_PATTERNS,
    CARGO_CONFIG_PATH,
    CARGO_TOML_PATH,
    CI_GATE_CARGO_BUILD_JOBS_ENV,
    ENV_SETUP_ACTIONS,
    LOCAL_ENV_OVERRIDES,
    MAX_LOCAL_CARGO_BUILD_JOBS,
    NON_BLOCKING_STATUSES,
    STEP_TIMEOUT_SECS,
    WORKFLOW_REGISTRY,
    WORKFLOWS_DIR,
    WorkflowSpec,
    build_affecting_files,
    diff_touches_build_affecting_files,
    is_build_affecting,
    resolve_cargo_build_jobs,
)
from ci_replica.workflow_plan import (  # noqa: E402
    _action_name,
    _apply_matrix,
    _combo_label,
    _job_steps,
    _matrix_combinations,
    _step_label,
    _strip_gha_expressions,
    build_plan_for_workflow,
    classify_step,
    discover_workflow_files,
    load_workflow,
)

from scripts import push_gate_evidence  # noqa: E402

#: Public surface re-exported from the `ci_replica` package for every consumer
#: that loads this file as one module (the hooks, release.yml, the meta-tests).
__all__ = (
    "ARTIFACT_IO_ACTIONS",
    "BUILD_AFFECTING_FILE_PATTERNS",
    "CARGO_CONFIG_PATH",
    "CARGO_TOML_PATH",
    "CI_GATE_CARGO_BUILD_JOBS_ENV",
    "DriftReport",
    "ENV_SETUP_ACTIONS",
    "LOCAL_ENV_OVERRIDES",
    "MAX_LOCAL_CARGO_BUILD_JOBS",
    "NON_BLOCKING_STATUSES",
    "STEP_TIMEOUT_SECS",
    "TOOLCHAIN_FEATURE_REQUIREMENTS",
    "WORKFLOWS_DIR",
    "WORKFLOW_REGISTRY",
    "WorkflowSpec",
    "_action_name",
    "_apply_matrix",
    "_combo_label",
    "_expand_features",
    "_extract_requested_features",
    "_feature_names",
    "_job_steps",
    "_load_cargo_features",
    "_matrix_combinations",
    "_parse_cargo_config_sections",
    "_step_label",
    "_strip_gha_expressions",
    "_target_toolchain_binaries",
    "_wrapper_binary",
    "build_affecting_files",
    "build_plan_for_workflow",
    "check_build_tool_dependencies",
    "check_toolchain_requirements",
    "classify_step",
    "consistency_check",
    "diff_touches_build_affecting_files",
    "discover_workflow_files",
    "find_required_build_binaries",
    "is_build_affecting",
    "load_workflow",
    "resolve_cargo_build_jobs",
)


def _run_step(
    run_text: str,
    job_env: dict,
    cargo_build_jobs: int | None = None,
) -> tuple[object, float]:
    cmd_text, stripped = _strip_gha_expressions(run_text)
    if stripped:
        print(f"    [gha-expr stripped to empty locally: {stripped}]")

    env_fd, env_path = tempfile.mkstemp(prefix="gh_env_")
    out_fd, out_path = tempfile.mkstemp(prefix="gh_out_")
    path_fd, path_path = tempfile.mkstemp(prefix="gh_path_")
    for fd in (env_fd, out_fd, path_fd):
        os.close(fd)

    env = dict(job_env)
    # Inject this on every individual child process, rather than only once in
    # the job base environment: a prior step may write CARGO_BUILD_JOBS via
    # GITHUB_ENV, and an inherited/workflow value must never bypass the local
    # resource ceiling. The shell text itself remains byte-for-byte unchanged.
    if cargo_build_jobs is None:
        cargo_build_jobs = resolve_cargo_build_jobs()
    elif (
        not isinstance(cargo_build_jobs, int)
        or isinstance(cargo_build_jobs, bool)
        or cargo_build_jobs < 1
    ):
        raise ValueError("cargo_build_jobs must be a positive integer")
    else:
        cargo_build_jobs = min(cargo_build_jobs, MAX_LOCAL_CARGO_BUILD_JOBS)
    env["CARGO_BUILD_JOBS"] = str(cargo_build_jobs)
    env["GITHUB_ENV"] = env_path
    env["GITHUB_OUTPUT"] = out_path
    env["GITHUB_PATH"] = path_path
    env.setdefault("RUNNER_TEMP", LOCAL_ENV_OVERRIDES["TMPDIR"])

    t0 = time.monotonic()
    status: object
    try:
        proc = subprocess.run(
            ["bash", "-c", cmd_text],
            cwd=REPO_ROOT,
            env=env,
            timeout=STEP_TIMEOUT_SECS,
            # A replicated CI step is NON-INTERACTIVE by definition: on a real
            # runner stdin is closed. Without this the child inherits OUR stdin,
            # which under systemd-run/pre-commit never delivers EOF, so any step
            # that reads input blocks for the FULL STEP_TIMEOUT_SECS (3600s)
            # instead of failing immediately. Measured: a single pre-push run
            # sat for 9.5 hours -- ~9 steps x 1h -- emitting nothing, because
            # pre-commit buffers hook output until the hook exits, so it looked
            # wedged rather than slow.
            stdin=subprocess.DEVNULL,
        )
        status = proc.returncode
    except subprocess.TimeoutExpired:
        status = "TIMEOUT"
    elapsed = time.monotonic() - t0

    # Thread $GITHUB_ENV / $GITHUB_PATH additions forward to later steps in
    # this job, the same way GitHub Actions does.
    try:
        for line in Path(env_path).read_text(encoding="utf-8").splitlines():
            if "=" in line and not line.strip().startswith("#"):
                k, _, v = line.partition("=")
                job_env[k.strip()] = v
    except OSError:
        pass
    try:
        for p in Path(path_path).read_text(encoding="utf-8").splitlines():
            if p.strip():
                job_env["PATH"] = p.strip() + os.pathsep + job_env.get("PATH", "")
    except OSError:
        pass
    for p in (env_path, out_path, path_path):
        try:
            os.unlink(p)
        except OSError:
            pass

    return status, elapsed


#: Cached result of :func:`_local_setup_python_bin` -- a real path once
#: provisioned, an empty ``Path`` once provisioning has been tried and failed
#: (so a broken host is reported once, not once per job).
_LOCAL_SETUP_PYTHON: Path | None = None


def _local_setup_python_bin() -> str | None:
    """Provide locally what ``actions/setup-python`` provides in CI.

    ``actions/setup-python`` is classified ENV_SETUP and skipped here, which is
    right -- but nothing replaced the one property it supplies that the local
    host does NOT have for free: a Python whose site-packages this run may
    write to. On a PEP 668 distro interpreter every replicated ``pip install``
    step dies with ``error: externally-managed-environment``, and it dies for a
    reason that is a fact about this host, not about the workflow. Worse, it
    does not stop there: a later step that USES what the install would have
    provided then runs against a stale environment and reports a confident,
    entirely fictional failure -- ``Install the wheel`` failing is what made
    ``numpy-free boundary contract (NE-249)`` "fail" against a previously
    installed copy of the package it was supposed to be testing.

    So: one venv per run, created with ``--system-site-packages`` so every step
    that already worked against the ambient interpreter keeps working, and
    prepended to PATH only for jobs that actually declare ``setup-python``.
    ``VIRTUAL_ENV`` is deliberately NOT exported -- ``uv run`` steps consult it
    and would switch environments underneath themselves; a venv's own
    ``bin/pip`` installs into its own prefix from PATH alone.

    This is a LOCAL_ENV_OVERRIDES-class adjustment: declared, printed, and
    never a substitute for a workflow-declared value.
    """

    global _LOCAL_SETUP_PYTHON
    if _LOCAL_SETUP_PYTHON is None:
        target = Path(LOCAL_ENV_OVERRIDES["TMPDIR"]) / "setup-python-venv"
        if not (target / "bin" / "pip").exists():
            target.parent.mkdir(parents=True, exist_ok=True)
            # `uv venv --seed` first, `python3 -m venv` second. On a Debian
            # interpreter -- which is exactly the PEP 668 case this exists for --
            # the stdlib route fails with "ensurepip is not available", so the
            # obvious ordering is the one that does not work on the only host
            # that needs it. uv is already a hard dependency of this repo's own
            # gates, so it is not a new requirement.
            attempts = []
            if shutil.which("uv") is not None:
                attempts.append(
                    [
                        "uv",
                        "venv",
                        "--seed",
                        "--system-site-packages",
                        "--python",
                        sys.executable,
                        str(target),
                    ]
                )
            attempts.append(
                [sys.executable, "-m", "venv", "--system-site-packages", str(target)]
            )
            failures = []
            for command in attempts:
                created = subprocess.run(
                    command,
                    capture_output=True,
                    text=True,
                    stdin=subprocess.DEVNULL,
                    timeout=300,
                )
                if created.returncode == 0 and (target / "bin" / "pip").exists():
                    break
                # `python3 -m venv` reports the ensurepip failure on STDOUT, so
                # a stderr-only report prints an empty reason and reads as a
                # mystery. Take whichever stream actually said something.
                failures.append(
                    f"{command[0]}: "
                    f"{(created.stderr.strip() or created.stdout.strip())[:300]}"
                )
            else:
                print(
                    "    [local setup-python replacement UNAVAILABLE -- pip steps "
                    f"will fail as they do today: {' | '.join(failures)}]"
                )
                _LOCAL_SETUP_PYTHON = Path("")
                return None
        print(f"    [local setup-python replacement: {target}]")
        _LOCAL_SETUP_PYTHON = target
    if not str(_LOCAL_SETUP_PYTHON):
        return None
    return str(_LOCAL_SETUP_PYTHON / "bin")


def _job_declares_setup_python(job: dict) -> bool:
    """Does this job ask CI for a provisioned Python interpreter?"""
    for step in _job_steps(job or {}):
        if _action_name(str((step or {}).get("uses", "") or "")) == (
            "actions/setup-python"
        ):
            return True
    return False


def _job_base_env(doc: dict, job_id: str) -> dict:
    env = dict(os.environ)
    env.update({k: str(v) for k, v in (doc.get("env") or {}).items()})
    env.update({k: str(v) for k, v in (doc["jobs"][job_id].get("env") or {}).items()})
    env.update(LOCAL_ENV_OVERRIDES)
    if _job_declares_setup_python(doc["jobs"][job_id]):
        bin_dir = _local_setup_python_bin()
        if bin_dir is not None:
            env["PATH"] = bin_dir + os.pathsep + env.get("PATH", "")
    return env


def _local_hygiene() -> None:
    """Remove stale glob-ambiguous artifacts from a PREVIOUS local run of this
    script. NOT a workflow step — CI starts every job from a fresh
    checkout, so this only exists to give a repeated local run the same
    "exactly one matching wheel" property the real workflow gets for free.
    Printed loudly so it is never mistaken for a workflow step."""
    import shutil

    removed = []
    for pattern in ("target/wheels",):
        p = REPO_ROOT / pattern
        if p.exists():
            shutil.rmtree(p, ignore_errors=True)
            removed.append(pattern)
    for pattern in (
        "numdist-primary",
        "numdist-reproduction",
        "dist-primary",
        "dist-reproduction",
        "numdist-gates",
        "dist-gates",
        ".gates-wheel-venv",
    ):
        p = REPO_ROOT / pattern
        if p.exists():
            shutil.rmtree(p, ignore_errors=True)
            removed.append(pattern)
    if removed:
        print(f"[local-only hygiene, NOT a workflow step] removed stale: {removed}")


def _parse_ci_gate_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument(
        "--consistency-check", action="store_true", help="only run the anti-drift check"
    )
    ap.add_argument(
        "--dry-run", action="store_true", help="print the execution plan; run nothing"
    )
    ap.add_argument(
        "--skip-safe",
        nargs="?",
        const="HEAD",
        metavar="BASE_REF",
        help="print whether skipping this gate is safe for the diff vs BASE_REF (default HEAD); exit 0 if safe, 1 if not",
    )
    ap.add_argument(
        "--workflows-dir",
        type=Path,
        default=WORKFLOWS_DIR,
        help="override the workflows directory (testing)",
    )
    return ap.parse_args()


def _handle_skip_safe(base_ref: str) -> int:
    safe, hits = diff_touches_build_affecting_files(base_ref)
    if safe:
        print(
            f"SKIP-SAFE: no build-affecting files changed vs {base_ref!r}; skipping ci-gate-replica is safe."
        )
        return 0
    print(
        f"NOT SKIP-SAFE: build-affecting file(s) changed vs {base_ref!r}; do NOT skip ci-gate-replica:"
    )
    for hit in hits:
        print(f"  - {hit}")
    return 1


def _build_execution_plan(workflows_dir: Path) -> tuple[list[dict], dict[str, dict]]:
    all_plan: list[dict] = []
    docs: dict[str, dict] = {}
    for fname, spec in WORKFLOW_REGISTRY.items():
        path = workflows_dir / fname
        if not path.is_file():
            continue
        doc = load_workflow(path)
        docs[fname] = doc
        plan, _, _ = build_plan_for_workflow(spec, doc)
        all_plan.extend(plan)
    return all_plan, docs


@dataclass
class _GateExecutionState:
    docs: dict[str, dict]
    dry_run: bool
    cargo_build_jobs: int
    evidence_store: Any = None
    prior_evidence: Any = None
    job_envs: dict[tuple[str, str], dict] = field(default_factory=dict)
    in_invocation: dict[str, tuple[object, float]] = field(default_factory=dict)


def _begin_evidence(dry_run: bool) -> tuple[Any, Any]:
    if dry_run:
        return None, None
    try:
        store = push_gate_evidence.EvidenceStore.begin_or_resume()
        return store, store.begin_execution()
    except (push_gate_evidence.EvidenceError, OSError) as exc:
        print(
            f"push-gate-evidence: unavailable ({type(exc).__name__}); executing normally"
        )
        return None, None


def _environment_for_item(item: dict, state: _GateExecutionState) -> dict:
    base_job_id = item["job"].split("#", 1)[0]
    key = (item["workflow"], item["job"])
    if key not in state.job_envs:
        state.job_envs[key] = _job_base_env(state.docs[item["workflow"]], base_job_id)
    return state.job_envs[key]


def _selection_for_item(item: dict, state: _GateExecutionState):
    return push_gate_evidence.selection_for_workflow_item(
        item,
        environment={
            **_environment_for_item(item, state),
            "CARGO_BUILD_JOBS": str(state.cargo_build_jobs),
        },
    )


def _reuse_invocation(item: dict, selection, state: _GateExecutionState) -> dict | None:
    selection_key = selection.selection_digest
    if selection_key not in state.in_invocation:
        return None
    status, elapsed = state.in_invocation[selection_key]
    print(f"push-gate-evidence: reused identical plan selection {selection.label}")
    return {**item, "status": status, "elapsed": elapsed, "cached": True}


def _reuse_prior_evidence(
    item: dict, selection, state: _GateExecutionState
) -> dict | None:
    if state.evidence_store is None or state.prior_evidence is None:
        return None
    try:
        reusable = state.evidence_store.consume_from(state.prior_evidence, selection)
    except (push_gate_evidence.EvidenceError, OSError):
        reusable = False
    if not reusable:
        return None
    print(f"push-gate-evidence: reused prior successful selection {selection.label}")
    state.in_invocation[selection.selection_digest] = (0, 0.0)
    return {**item, "status": 0, "elapsed": 0.0, "cached": True}


def _record_step_evidence(
    selection, status: object, elapsed: float, state: _GateExecutionState
) -> None:
    state.in_invocation[selection.selection_digest] = (status, elapsed)
    if state.evidence_store is None:
        return
    try:
        state.evidence_store.record(
            selection,
            exit_code=status if isinstance(status, int) else 1,
            elapsed=elapsed,
        )
    except (push_gate_evidence.EvidenceError, OSError) as exc:
        print(
            f"push-gate-evidence: write unavailable ({type(exc).__name__}); "
            "continuing without reuse"
        )
        state.evidence_store = None
        state.prior_evidence = None


def _run_fresh_item(item: dict, selection, state: _GateExecutionState) -> dict:
    print(
        f"\n############### STEP [{item['workflow']}:{item['job']}] {item['name']} ###############"
    )
    status, elapsed = _run_step(
        item["detail"],
        _environment_for_item(item, state),
        state.cargo_build_jobs,
    )
    print(
        f"### STEP_RESULT job={item['job']} name={item['name']!r} exit={status} secs={elapsed:.1f}"
    )
    _record_step_evidence(selection, status, elapsed, state)
    return {**item, "status": status, "elapsed": elapsed}


def _execute_run_item(item: dict, state: _GateExecutionState) -> dict:
    if state.dry_run:
        print(f"[DRY-RUN] would RUN [{item['workflow']}:{item['job']}] {item['name']}")
        return {**item, "status": "DRY_RUN", "elapsed": 0.0}

    selection = _selection_for_item(item, state)
    reused = _reuse_invocation(item, selection, state)
    if reused is not None:
        return reused
    reused = _reuse_prior_evidence(item, selection, state)
    if reused is not None:
        return reused
    return _run_fresh_item(item, selection, state)


def _non_run_item(item: dict) -> dict:
    mode = item["mode"]
    if mode in {"ENV_SETUP", "ARTIFACT_IO"}:
        status = mode
    else:
        tag = (
            "TOOLCHAIN ABSENT LOCALLY"
            if mode == "TOOLCHAIN_MISSING"
            else "NOT VALIDATED LOCALLY"
        )
        print(
            f"\n### {tag} [{item['workflow']}:{item['job']}] {item['name']}\n    reason: {item['detail']}"
        )
        status = "NOT_VALIDATED_LOCALLY"
    return {**item, "status": status, "elapsed": 0.0}


def _execute_item(item: dict, state: _GateExecutionState) -> dict:
    if item["mode"] == "RUN":
        return _execute_run_item(item, state)
    return _non_run_item(item)


def _execute_plan(plan: list[dict], state: _GateExecutionState) -> list[dict]:
    return [_execute_item(item, state) for item in plan]


def _finalize_evidence(state: _GateExecutionState) -> None:
    if state.evidence_store is None:
        return
    try:
        state.evidence_store.finalize("complete")
    except (push_gate_evidence.EvidenceError, OSError) as exc:
        print(
            f"push-gate-evidence: finalization unavailable ({type(exc).__name__}); "
            "results remain non-consumable"
        )


def _status_is_bad(status: object) -> bool:
    return (isinstance(status, str) and status not in NON_BLOCKING_STATUSES) or (
        isinstance(status, int) and status != 0
    )


def _print_summary_rows(results: list[dict]) -> tuple[bool, bool]:
    blocking_fail = False
    advisory_fail = False
    for result in results:
        status = result["status"]
        label = f"{result['workflow']}:{result['job']}"
        print(
            f"{label:36s} {result['name'][:56]:56s} status={str(status):22s} secs={result['elapsed']:8.1f}"
        )
        if _status_is_bad(status):
            if result["blocking"]:
                blocking_fail = True
            else:
                advisory_fail = True
    return blocking_fail, advisory_fail


def _print_not_validated(results: list[dict]) -> None:
    not_validated = [
        result for result in results if result["status"] == "NOT_VALIDATED_LOCALLY"
    ]
    if not not_validated:
        return
    print(
        f"\n### {len(not_validated)} STEP(S) NOT VALIDATED LOCALLY — these were NEVER RUN on "
        "this host and are NOT a pass, never counted as one:"
    )
    for result in not_validated:
        print(
            f"  - [{result['workflow']}:{result['job']}] {result['name']}\n      reason: {result['detail']}"
        )


def _summarize_results(results: list[dict]) -> int:
    print("\n################ SUMMARY ################")
    blocking_fail, advisory_fail = _print_summary_rows(results)
    _print_not_validated(results)
    print(f"BLOCKING_FAIL={'1' if blocking_fail else '0'}")
    print(
        f"ADVISORY_FAIL={'1' if advisory_fail else '0'} (never fails the pre-push gate — reported loudly only)"
    )
    print(f"OVERALL_FAIL={'1' if blocking_fail else '0'}")
    print(
        f"=== SENTINEL_COMPLETE {datetime.datetime.now(datetime.timezone.utc).isoformat()} ==="
    )
    return 1 if blocking_fail else 0


def _run_gate(
    args: argparse.Namespace,
    all_plan: list[dict],
    docs: dict[str, dict],
    cargo_build_jobs: int,
) -> int:
    if not args.dry_run:
        _local_hygiene()
    evidence_store, prior_evidence = _begin_evidence(args.dry_run)
    state = _GateExecutionState(
        docs=docs,
        dry_run=args.dry_run,
        cargo_build_jobs=cargo_build_jobs,
        evidence_store=evidence_store,
        prior_evidence=prior_evidence,
    )
    results = _execute_plan(all_plan, state)
    _finalize_evidence(state)
    return _summarize_results(results)


def main() -> int:
    args = _parse_ci_gate_args()
    if args.skip_safe is not None:
        return _handle_skip_safe(args.skip_safe)

    detected_cpus = os.cpu_count()
    try:
        cargo_build_jobs = resolve_cargo_build_jobs(detected_cpus=detected_cpus)
    except ValueError as exc:
        print(
            f"FATAL: local Cargo resource configuration is invalid: {exc}",
            file=sys.stderr,
        )
        return 2

    print(
        f"=== ci_gate_replica.py START {datetime.datetime.now(datetime.timezone.utc).isoformat()} "
        f"nproc={detected_cpus} cargo_build_jobs={cargo_build_jobs} "
        f"cargo_build_jobs_max={MAX_LOCAL_CARGO_BUILD_JOBS} ==="
    )

    ok = consistency_check(workflows_dir=args.workflows_dir)

    if args.consistency_check:
        return 0 if ok else 1

    if not ok:
        print(
            "\nRefusing to run the gate: the consistency check above failed. This IS the"
        )
        print(
            "anti-drift guard working as intended — fix the classification, don't bypass it."
        )
        return 1

    all_plan, docs = _build_execution_plan(args.workflows_dir)
    return _run_gate(args, all_plan, docs, cargo_build_jobs)


if __name__ == "__main__":
    sys.exit(main())
