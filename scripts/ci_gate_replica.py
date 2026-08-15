#!/usr/bin/env python3
"""Local replica of .github/workflows/release.yml's RELEASE-BLOCKING checks.

WHY THIS PARSES release.yml INSTEAD OF HAND-COPYING ITS STEPS: a hand-copied
step list silently drifts from the real workflow the moment either changes —
exactly the failure mode this script exists to close (eg's CI was red for two
releases on a test the local pre-push tier never ran). So this script reads
the workflow file itself, at run time, and classifies every step it finds:

  * a step with a `run:` shell block in an EXECUTABLE_JOBS job is executed
    VERBATIM (its literal shell text, `${{ ... }}` GitHub Actions expressions
    stripped to empty — see _strip_gha_expressions), with $GITHUB_ENV/
    $GITHUB_OUTPUT/$GITHUB_PATH shimmed the same way GH Actions threads state
    between steps of one job. A new `run:` step added to release.yml is
    executed by this script with NO code change here.
  * a step with `uses:` naming a known environment-setup action (checkout,
    setup-python, rust-toolchain, rust-cache) is a silent no-op locally — the
    dev machine already has these tools.
  * a step with `uses:` naming an artifact-transfer action (upload/download-
    artifact) is a silent no-op — it moves files between CI jobs, not logic.
  * any other `uses:` step (a marketplace build/publish action with no local
    equivalent — maturin-action, docker/*) is reported LOUDLY as
    "NOT VALIDATED LOCALLY", never silently skipped and never counted toward
    a pass.
  * an entire JOB can be marked out-of-scope (JOB_SKIP_REASONS below) when
    NONE of its steps can run locally at all (a 5-platform native build
    matrix, a tag-gated Docker/PyPI publish) — every one of its steps still
    shows up in the summary as NOT VALIDATED LOCALLY, with the reason.

THE ANTI-DRIFT GUARANTEE (--consistency-check): every job release.yml
declares must be classified as either EXECUTABLE or explicitly skip-reasoned.
A job that is neither (a brand new top-level job, or a renamed one) makes the
consistency check — and therefore the whole run — FAIL LOUDLY rather than
silently ignoring it. See the module docstring test in
tests/test_ci_gate_replica_consistency_check.py for a proof this actually
fires on an injected new job.

Usage:
  scripts/ci_gate_replica.py                    # full run (heavy — pre-push/manual only)
  scripts/ci_gate_replica.py --dry-run           # print the plan, execute nothing
  scripts/ci_gate_replica.py --consistency-check # only the anti-drift check
  scripts/ci_gate_replica.py --workflow PATH     # override the workflow file (testing)
"""
from __future__ import annotations

import argparse
import datetime
import os
import re
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_WORKFLOW_PATH = REPO_ROOT / ".github" / "workflows" / "release.yml"

# ─────────────────────────────────────────────────────────────────────────
# Per-repo configuration. This is the ONLY hand-maintained classification
# surface — everything else is derived from the parsed YAML. Keeping this
# list short and job-scoped (not step-scoped) is what makes the consistency
# check a real drift guard rather than just documentation: a new *step* in
# an already-known job is auto-classified (RUN if it has `run:`); only a new
# *job* requires a human to add an entry here, and failing to do so is
# exactly what --consistency-check catches.
# ─────────────────────────────────────────────────────────────────────────

# Jobs whose steps this script actually executes locally.
EXECUTABLE_JOBS = {"gates"}

# Jobs this script deliberately does NOT execute, with the reason surfaced
# in the summary as "NOT VALIDATED LOCALLY — <reason>" for every one of
# their steps.
JOB_SKIP_REASONS = {
    "build": (
        "5-platform native cross-compilation matrix (linux-x86_64/aarch64, "
        "windows-x86_64, macos-aarch64/x86_64) built via PyO3/maturin-action — "
        "cannot be reproduced by one local job. scripts/wheel_privacy_gate.sh "
        "gives a single-platform (linux, debug-profile) local proxy for the "
        "fold+normalize+audit+completeness sequence this job runs, but it is "
        "not this job and is not run by this script."
    ),
    "docker-image": (
        "tag-gated (`if: startsWith(github.ref, 'refs/tags/v')`) and builds a "
        "multi-arch Docker image from the release wheel artifacts — no local "
        "Docker registry/buildx multi-arch context is provisioned here."
    ),
    "publish-pypi": (
        "tag-gated PyPI publish requiring PYPI_API_TOKEN and the wheel "
        "artifacts from the (also-skipped) build job. Publishing must never "
        "happen from a local pre-push hook."
    ),
    "publish-image": (
        "tag-gated Docker registry push requiring DOCKER_* registry secrets "
        "via a GitHub Environment approval gate. Publishing must never "
        "happen from a local pre-push hook."
    ),
}

# `uses:` actions that are pure environment setup — the local dev machine
# already has the equivalent tool on PATH, so these are silent no-ops.
ENV_SETUP_ACTIONS = (
    "actions/checkout",
    "actions/setup-python",
    "actions/setup-node",
    "dtolnay/rust-toolchain",
    "Swatinem/rust-cache",
    "astral-sh/setup-uv",
)

# `uses:` actions that only move files between CI jobs — no logic to run.
ARTIFACT_IO_ACTIONS = ("actions/upload-artifact", "actions/download-artifact")

# Local-only execution-environment adjustments. NOT derived from release.yml
# (CI gets these properties for free from an ephemeral runner) — documented
# here, not hidden, and never silently substituted for a release.yml value.
LOCAL_ENV_OVERRIDES = {
    "CARGO_TARGET_DIR": os.environ.get("CI_GATE_CARGO_TARGET_DIR", "/var/tmp/eg-ci-gate-target"),
    "TMPDIR": os.environ.get("CI_GATE_TMPDIR", "/var/tmp/eg-ci-gate-tmp"),
}

STEP_TIMEOUT_SECS = int(os.environ.get("CI_GATE_STEP_TIMEOUT_SECS", "3600"))

GHA_EXPR_RE = re.compile(r"\$\{\{.*?\}\}")

# Statuses that are NOT a pass but also NOT a fail — visible, honest, and
# excluded from the pass/fail tally per design (never silently omitted,
# never counted as passing).
NON_BLOCKING_STATUSES = {"ENV_SETUP", "ARTIFACT_IO", "NOT_VALIDATED_LOCALLY", "DRY_RUN"}


def _strip_gha_expressions(text: str) -> tuple[str, list[str]]:
    """Replace every `${{ ... }}` GitHub Actions expression with the empty
    string. Outside an Actions runner these contexts (github.*, matrix.*,
    env.*, secrets.*) do not exist; stripping to empty is the closest honest
    local analogue (e.g. the secret-history step's base-SHA expression
    stripped to empty falls through to its own HEAD~1 fallback, same as the
    real workflow does on a repo's first push)."""
    found = GHA_EXPR_RE.findall(text)
    return GHA_EXPR_RE.sub("", text), found


def _action_name(uses: str) -> str:
    return uses.split("@", 1)[0]


def _step_label(step: dict) -> str:
    if step.get("name"):
        return step["name"]
    if step.get("id"):
        return step["id"]
    if step.get("uses"):
        return step["uses"]
    run = step.get("run", "")
    first_line = run.strip().splitlines()[0] if run.strip() else "<empty step>"
    return first_line[:60]


def load_workflow(path: Path) -> dict:
    if not path.is_file():
        print(f"FATAL: workflow file not found: {path}", file=sys.stderr)
        sys.exit(90)
    with open(path, encoding="utf-8") as f:
        doc = yaml.safe_load(f)
    if not isinstance(doc, dict) or "jobs" not in doc:
        print(f"FATAL: {path} did not parse into a workflow with a top-level 'jobs:' map", file=sys.stderr)
        sys.exit(90)
    return doc


def classify_step(step: dict) -> tuple[str, str]:
    """Return (mode, detail). mode is one of RUN/ENV_SETUP/ARTIFACT_IO/SKIP_LOUD."""
    if "run" in step and step["run"] is not None:
        return "RUN", step["run"]
    uses = step.get("uses", "") or ""
    name = _action_name(uses)
    if name in ENV_SETUP_ACTIONS:
        return "ENV_SETUP", uses
    if name in ARTIFACT_IO_ACTIONS:
        return "ARTIFACT_IO", uses
    return "SKIP_LOUD", f"marketplace action '{uses}' has no local equivalent — not executed here"


def build_plan(doc: dict) -> tuple[list[dict], list[str], list[str]]:
    """Returns (plan, unclassified_jobs, stale_config_job_ids).

    unclassified_jobs: jobs present in release.yml that are neither in
      EXECUTABLE_JOBS nor JOB_SKIP_REASONS — the drift this script exists to
      catch.
    stale_config_job_ids: job ids in EXECUTABLE_JOBS/JOB_SKIP_REASONS that no
      longer exist in release.yml — the opposite drift (config outlived the
      workflow), also a consistency failure.
    """
    jobs = doc.get("jobs", {}) or {}
    plan: list[dict] = []
    unclassified: list[str] = []

    for job_id, job in jobs.items():
        steps = (job or {}).get("steps", []) or []
        if job_id in EXECUTABLE_JOBS:
            for step in steps:
                mode, detail = classify_step(step)
                plan.append({"job": job_id, "name": _step_label(step), "mode": mode, "detail": detail})
        elif job_id in JOB_SKIP_REASONS:
            reason = JOB_SKIP_REASONS[job_id]
            for step in steps:
                plan.append({"job": job_id, "name": _step_label(step), "mode": "SKIP_LOUD", "detail": reason})
        else:
            unclassified.append(job_id)

    known = EXECUTABLE_JOBS | set(JOB_SKIP_REASONS)
    stale = sorted(j for j in known if j not in jobs)
    return plan, sorted(unclassified), stale


def consistency_check(doc: dict, *, verbose: bool = True) -> bool:
    plan, unclassified, stale = build_plan(doc)
    ok = True
    if unclassified:
        ok = False
        if verbose:
            print("CONSISTENCY CHECK FAILED — release.yml has job(s) this replica does not classify:")
            for j in unclassified:
                print(f"  - {j!r} is in neither EXECUTABLE_JOBS nor JOB_SKIP_REASONS")
            print("Update scripts/ci_gate_replica.py's EXECUTABLE_JOBS/JOB_SKIP_REASONS to cover it.")
    if stale:
        ok = False
        if verbose:
            print("CONSISTENCY CHECK FAILED — configured job(s) no longer exist in release.yml (stale config):")
            for j in stale:
                print(f"  - {j!r}")
            print("Remove the stale entry from EXECUTABLE_JOBS/JOB_SKIP_REASONS.")
    if ok and verbose:
        all_jobs = doc.get("jobs", {}) or {}
        print(
            f"CONSISTENCY CHECK PASSED — {len(all_jobs)} job(s) in release.yml, all classified "
            f"({len(EXECUTABLE_JOBS)} executable: {sorted(EXECUTABLE_JOBS)}; "
            f"{len(JOB_SKIP_REASONS)} explicitly skip-reasoned: {sorted(JOB_SKIP_REASONS)}); "
            f"{len(plan)} step(s) total."
        )
    return ok


def _run_step(run_text: str, job_env: dict) -> tuple[object, float]:
    cmd_text, stripped = _strip_gha_expressions(run_text)
    if stripped:
        print(f"    [gha-expr stripped to empty locally: {stripped}]")

    env_fd, env_path = tempfile.mkstemp(prefix="gh_env_")
    out_fd, out_path = tempfile.mkstemp(prefix="gh_out_")
    path_fd, path_path = tempfile.mkstemp(prefix="gh_path_")
    for fd in (env_fd, out_fd, path_fd):
        os.close(fd)

    env = dict(job_env)
    env["GITHUB_ENV"] = env_path
    env["GITHUB_OUTPUT"] = out_path
    env["GITHUB_PATH"] = path_path
    env.setdefault("RUNNER_TEMP", LOCAL_ENV_OVERRIDES["TMPDIR"])

    t0 = time.monotonic()
    status: object
    try:
        proc = subprocess.run(["bash", "-c", cmd_text], cwd=REPO_ROOT, env=env, timeout=STEP_TIMEOUT_SECS)
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


def _job_base_env(doc: dict, job_id: str) -> dict:
    env = dict(os.environ)
    env.update({k: str(v) for k, v in (doc.get("env") or {}).items()})
    env.update({k: str(v) for k, v in ((doc["jobs"][job_id].get("env") or {})).items()})
    env.update(LOCAL_ENV_OVERRIDES)
    return env


def _local_hygiene() -> None:
    """Remove stale glob-ambiguous artifacts from a PREVIOUS local run of this
    script. NOT a release.yml step — CI starts every job from a fresh
    checkout, so this only exists to give a repeated local run the same
    "exactly one matching wheel" property the real workflow gets for free.
    Printed loudly so it is never mistaken for a release.yml step."""
    import glob
    import shutil

    removed = []
    for pattern in ("target/wheels",):
        p = REPO_ROOT / pattern
        if p.exists():
            shutil.rmtree(p, ignore_errors=True)
            removed.append(pattern)
    for pattern in ("numdist-primary", "numdist-reproduction", "dist-primary", "dist-reproduction"):
        p = REPO_ROOT / pattern
        if p.exists():
            shutil.rmtree(p, ignore_errors=True)
            removed.append(pattern)
    if removed:
        print(f"[local-only hygiene, NOT a release.yml step] removed stale: {removed}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--consistency-check", action="store_true", help="only run the anti-drift check")
    ap.add_argument("--dry-run", action="store_true", help="print the execution plan; run nothing")
    ap.add_argument("--workflow", type=Path, default=DEFAULT_WORKFLOW_PATH, help="override the workflow path (testing)")
    args = ap.parse_args()

    doc = load_workflow(args.workflow)
    ok = consistency_check(doc)

    if args.consistency_check:
        return 0 if ok else 1

    if not ok:
        print("\nRefusing to run the gate: the consistency check above failed. This IS the")
        print("anti-drift guard working as intended — fix the classification, don't bypass it.")
        return 1

    plan, _, _ = build_plan(doc)

    if not args.dry_run:
        _local_hygiene()

    job_envs = {job_id: _job_base_env(doc, job_id) for job_id in EXECUTABLE_JOBS if job_id in doc.get("jobs", {})}

    print(f"=== ci_gate_replica.py START {datetime.datetime.now(datetime.timezone.utc).isoformat()} nproc={os.cpu_count()} ===")

    results: list[tuple[str, str, object, float]] = []
    for item in plan:
        job_id, name, mode, detail = item["job"], item["name"], item["mode"], item["detail"]
        if mode == "RUN":
            if args.dry_run:
                print(f"[DRY-RUN] would RUN [{job_id}] {name}")
                results.append((job_id, name, "DRY_RUN", 0.0))
                continue
            print(f"\n############### STEP [{job_id}] {name} ###############")
            status, elapsed = _run_step(detail, job_envs[job_id])
            print(f"### STEP_RESULT job={job_id} name={name!r} exit={status} secs={elapsed:.1f}")
            results.append((job_id, name, status, elapsed))
        elif mode == "ENV_SETUP":
            results.append((job_id, name, "ENV_SETUP", 0.0))
        elif mode == "ARTIFACT_IO":
            results.append((job_id, name, "ARTIFACT_IO", 0.0))
        else:
            print(f"\n### NOT VALIDATED LOCALLY [{job_id}] {name}\n    reason: {detail}")
            results.append((job_id, name, "NOT_VALIDATED_LOCALLY", 0.0))

    print("\n################ SUMMARY ################")
    fail = False
    for job_id, name, status, elapsed in results:
        print(f"{job_id:14s} {name[:64]:64s} status={str(status):22s} secs={elapsed:8.1f}")
        if isinstance(status, str) and status not in NON_BLOCKING_STATUSES:
            fail = True
        elif isinstance(status, int) and status != 0:
            fail = True
    print(f"OVERALL_FAIL={'1' if fail else '0'}")
    print(f"=== SENTINEL_COMPLETE {datetime.datetime.now(datetime.timezone.utc).isoformat()} ===")
    return 1 if fail else 0


if __name__ == "__main__":
    sys.exit(main())
