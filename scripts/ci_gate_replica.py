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

EVERY workflow file under .github/workflows/ is covered — not just the
release-blocking one. WORKFLOW_REGISTRY carries a `blocking` flag per
workflow (release.yml's jobs block the pre-push gate on failure; advisory.yml
never blocks, matching that file's own `continue-on-error: true` contract on
every step) — but "advisory" only changes whether a red step FAILS the
overall run, never whether it RUNS or gets REPORTED. Before this redesign,
this script only replayed release.yml's `gates` job, so an entire workflow
(advisory.yml — a 10-job feature-build matrix plus the perf/recall bench
gate) was never validated locally at all, invisible until a push broke it in
GitHub-hosted CI.

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
"""
from __future__ import annotations

import argparse
import datetime
import fnmatch
import os
import posixpath
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import time
import tomllib
from dataclasses import dataclass, field
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
WORKFLOWS_DIR = REPO_ROOT / ".github" / "workflows"
CARGO_CONFIG_PATH = REPO_ROOT / ".cargo" / "config.toml"
CARGO_TOML_PATH = REPO_ROOT / "Cargo.toml"

# ─────────────────────────────────────────────────────────────────────────
# Per-repo configuration. This is the ONLY hand-maintained classification
# surface — everything else is derived from the parsed YAML. Keeping this
# list short and job-scoped (not step-scoped) is what makes the consistency
# check a real drift guard rather than just documentation: a new *step* in
# an already-known job is auto-classified (RUN if it has `run:`); only a new
# *job*, or a new *workflow file*, requires a human to add an entry here,
# and failing to do so is exactly what --consistency-check catches.
# ─────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class WorkflowSpec:
    filename: str
    # True: a failing RUN step in this workflow fails the overall replica
    # run (release.yml — release-blocking). False: a failing RUN step is
    # still executed and reported loudly, but does not fail the run
    # (advisory.yml — every step there already carries its own
    # `continue-on-error: true` in the real workflow; this mirrors that).
    blocking: bool
    executable_jobs: frozenset[str]
    job_skip_reasons: dict[str, str] = field(default_factory=dict)


WORKFLOW_REGISTRY: dict[str, WorkflowSpec] = {
    "release.yml": WorkflowSpec(
        filename="release.yml",
        blocking=True,
        executable_jobs=frozenset({"gates"}),
        job_skip_reasons={
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
        },
    ),
    "advisory.yml": WorkflowSpec(
        filename="advisory.yml",
        blocking=False,
        # Previously NOT REPLICATED AT ALL — this is the whole of GAP 1: a
        # 10-way feature-build matrix and the perf/recall bench gate were
        # invisible locally. Both are ordinary local `cargo`/`python3`
        # commands once ${{ matrix.* }} is substituted; they just never ran.
        executable_jobs=frozenset({"lint-and-architecture", "feature-matrix", "benchmarks"}),
        job_skip_reasons={
            "pages": (
                "job-level `uses:` calling a reusable workflow "
                "(Knuckles-Team/pipelines pages_pipeline.yml) — no `steps:` at all, "
                "so nothing here is a local shell command; needs the GitHub Pages "
                "environment/actions with no local equivalent."
            ),
        },
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

# Local-only execution-environment adjustments. NOT derived from any
# workflow file (CI gets these properties for free from an ephemeral
# runner) — documented here, not hidden, and never silently substituted for
# a workflow-declared value.
LOCAL_ENV_OVERRIDES = {
    "CARGO_TARGET_DIR": os.environ.get("CI_GATE_CARGO_TARGET_DIR", "/var/tmp/eg-ci-gate-target"),
    "TMPDIR": os.environ.get("CI_GATE_TMPDIR", "/var/tmp/eg-ci-gate-tmp"),
}

STEP_TIMEOUT_SECS = int(os.environ.get("CI_GATE_STEP_TIMEOUT_SECS", "3600"))

GHA_EXPR_RE = re.compile(r"\$\{\{.*?\}\}")
MATRIX_EXPR_RE = re.compile(r"\$\{\{\s*matrix\.([\w.-]+)\s*\}\}")

# Statuses that are NOT a pass but also NOT a fail — visible, honest, and
# excluded from the pass/fail tally per design (never silently omitted,
# never counted as passing).
NON_BLOCKING_STATUSES = {"ENV_SETUP", "ARTIFACT_IO", "NOT_VALIDATED_LOCALLY", "DRY_RUN"}

# ─────────────────────────────────────────────────────────────────────────
# GAP 3 — the authoritative "does this diff affect the build" file set.
# Defined ONCE, here, so a human or another script has one place to ask "is
# skipping the (heavy, pre-push-only) ci-gate-replica hook safe for this
# diff" instead of an ad-hoc grep. The incident this closes: commit 652f91c
# changed ONLY .cargo/config.toml — no `.rs`/Cargo.toml/Cargo.lock file — and
# was judged safe to skip the replica on exactly that (too-narrow) pattern
# set. `.cargo/**` below would have caught it.
# ─────────────────────────────────────────────────────────────────────────
BUILD_AFFECTING_FILE_PATTERNS: tuple[str, ...] = (
    "**/*.rs",
    "Cargo.toml",
    "Cargo.lock",
    ".cargo/**",
    "rust-toolchain*",
    "build.rs",
    ".github/workflows/**",
    ".pre-commit-config.yaml",
)


def _strip_gha_expressions(text: str) -> tuple[str, list[str]]:
    """Replace every remaining `${{ ... }}` GitHub Actions expression with
    the empty string. Outside an Actions runner these contexts (github.*,
    env.*, secrets.*) do not exist; stripping to empty is the closest honest
    local analogue (e.g. the secret-history step's base-SHA expression
    stripped to empty falls through to its own HEAD~1 fallback, same as the
    real workflow does on a repo's first push). Call this AFTER
    _substitute_matrix so `${{ matrix.* }}` references are resolved to real
    values first, not blanked."""
    found = GHA_EXPR_RE.findall(text)
    return GHA_EXPR_RE.sub("", text), found


def _substitute_matrix(text: str, combo: dict) -> str:
    """Replace `${{ matrix.a.b }}` with the real value from this matrix
    leg's combo dict (dotted path lookup). Anything unresolved (typo'd path,
    or genuinely not present in this combo) is left alone for
    _strip_gha_expressions to blank afterward — never a KeyError."""
    if not combo:
        return text

    def repl(m: re.Match) -> str:
        value: object = combo
        for part in m.group(1).split("."):
            if isinstance(value, dict) and part in value:
                value = value[part]
            else:
                return m.group(0)
        return "" if value is None else str(value)

    return MATRIX_EXPR_RE.sub(repl, text)


def _matrix_combinations(matrix: dict | None) -> list[dict]:
    """Cartesian-expand a `strategy.matrix` block into concrete per-leg
    combo dicts: one entry per (axis-value-combination ∪ include-entry).
    Deliberately does not implement GitHub's `exclude:` or the richer
    axis-matching `include:` merge semantics — none of this repo's
    workflows use them, and an unsupported shape degrading to "run the leg
    anyway with best-effort substitution" is safer here than silently
    dropping a leg."""
    if not matrix:
        return [{}]
    axes = {k: v for k, v in matrix.items() if k not in ("include", "exclude") and isinstance(v, list)}
    combos: list[dict] = []
    if axes:
        combos = [{}]
        for key, values in axes.items():
            combos = [dict(c, **{key: v}) for c in combos for v in values]
    for extra in matrix.get("include", []) or []:
        if isinstance(extra, dict):
            combos.append(dict(extra))
    return combos or [{}]


def _combo_label(combo: dict) -> str:
    """Human label suffix for one matrix leg, e.g. '#linux-x86_64' or
    '#crate-eg-types'. Prefers a top-level or nested 'name' field (the
    convention every matrix in this repo's workflows already follows);
    falls back to joining the raw values."""
    if not combo:
        return ""
    if "name" in combo and not isinstance(combo["name"], dict):
        return f"#{combo['name']}"
    parts = []
    for v in combo.values():
        parts.append(str(v.get("name", v)) if isinstance(v, dict) else str(v))
    return "#" + "-".join(parts) if parts else ""


def _apply_matrix(step: dict, combo: dict) -> dict:
    """Return a copy of `step` with `${{ matrix.* }}` references in its
    string fields resolved against this leg's combo. A no-op (returns the
    same object) when there is no matrix, so non-matrix jobs pay no cost."""
    if not combo:
        return step
    new_step = dict(step)
    for f in ("run", "name", "uses"):
        val = new_step.get(f)
        if isinstance(val, str):
            new_step[f] = _substitute_matrix(val, combo)
    return new_step


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


def _job_steps(job: dict) -> list[dict]:
    """Steps for a job — or, for a job that IS a reusable-workflow call
    (`uses:` at job level with no `steps:` key at all, valid GH Actions job
    syntax), a single synthetic step so it is still classified and reported
    instead of silently contributing zero plan rows."""
    if "steps" in job:
        return job.get("steps") or []
    if job.get("uses"):
        return [{"uses": job["uses"], "name": job.get("name") or f"(reusable workflow: {job['uses']})"}]
    return []


def discover_workflow_files(workflows_dir: Path = WORKFLOWS_DIR) -> list[Path]:
    if not workflows_dir.is_dir():
        return []
    return sorted(workflows_dir.glob("*.yml")) + sorted(workflows_dir.glob("*.yaml"))


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
    if ".github/workflows/" in uses:
        return "SKIP_LOUD", f"reusable workflow '{uses}' has no local runner equivalent — not executed here"
    return "SKIP_LOUD", f"marketplace action '{uses}' has no local equivalent — not executed here"


def build_plan_for_workflow(
    spec: WorkflowSpec, doc: dict, feature_table: dict[str, list[str]] | None = None
) -> tuple[list[dict], list[str], list[str]]:
    """Returns (plan, unclassified_jobs, stale_config_job_ids) for one
    workflow. unclassified_jobs: jobs present in the workflow that are
    neither in spec.executable_jobs nor spec.job_skip_reasons — the drift
    this script exists to catch. stale_config_job_ids: job ids in the spec
    that no longer exist in the workflow (the opposite drift), also a
    consistency failure.

    `feature_table` (GAP 4): the root Cargo.toml's parsed `[features]` graph,
    used to reclassify a RUN step to NOT_VALIDATED_LOCALLY when its own
    cargo invocation needs an external build tool this host doesn't have —
    see check_toolchain_requirements. Lazily loaded from CARGO_TOML_PATH
    when not supplied, so existing 2-arg callers are unaffected."""
    if feature_table is None:
        feature_table = _load_cargo_features()
    jobs = doc.get("jobs", {}) or {}
    plan: list[dict] = []
    unclassified: list[str] = []

    for job_id, job in jobs.items():
        job = job or {}
        steps = _job_steps(job)
        matrix = ((job.get("strategy") or {}).get("matrix")) or None
        combos = _matrix_combinations(matrix)

        if job_id in spec.executable_jobs:
            mode_for_missing = None  # classified per-step below
        elif job_id in spec.job_skip_reasons:
            mode_for_missing = "SKIP_LOUD"
        else:
            unclassified.append(job_id)
            continue

        for combo in combos:
            job_label = job_id + _combo_label(combo)
            for step in steps:
                step2 = _apply_matrix(step, combo)
                if mode_for_missing == "SKIP_LOUD":
                    mode, detail = "SKIP_LOUD", spec.job_skip_reasons[job_id]
                else:
                    mode, detail = classify_step(step2)
                    if mode == "RUN":
                        toolchain_reason = check_toolchain_requirements(detail, feature_table)
                        if toolchain_reason is not None:
                            mode, detail = "TOOLCHAIN_MISSING", toolchain_reason
                plan.append(
                    {
                        "workflow": spec.filename,
                        "blocking": spec.blocking,
                        "job": job_label,
                        "name": _step_label(step2),
                        "mode": mode,
                        "detail": detail,
                    }
                )

    known = spec.executable_jobs | set(spec.job_skip_reasons)
    stale = sorted(j for j in known if j not in jobs)
    return plan, sorted(unclassified), stale


# ─────────────────────────────────────────────────────────────────────────
# GAP 2 — external build-tool dependency check.
#
# A replica that only checks COMMAND equivalence (does `cargo test ...` run
# the same text locally and in CI) can never catch "you depend on a binary
# the CI runner doesn't have" — the exact class of the sccache/mold break:
# this build host has both installed, so the failing command SUCCEEDS here.
# This check instead parses .cargo/config.toml for anything that makes a
# cargo invocation hard-depend on an external binary (rustc-wrapper, a
# target linker/runner, `-fuse-ld=<x>`/`-C linker=<x>` in rustflags) and
# verifies the binary is mentioned SOMEWHERE in the workflow YAML that would
# run cargo — in practice, an explicit install step. Deliberately not
# special-cased to "sccache": any wrapper/linker/runner binary is checked.
# ─────────────────────────────────────────────────────────────────────────

_TOML_SECTION_RE = re.compile(r"^\[(?P<name>[^\]]+)\]\s*$")
_TOML_KV_RE = re.compile(r"^(?P<key>[A-Za-z0-9_.'\"-]+)\s*=\s*(?P<value>.+?)\s*$")
_FUSE_LD_RE = re.compile(r"-fuse-ld=([A-Za-z0-9_.+-]+)")
_C_LINKER_RE = re.compile(r"-C\s*linker=([^\s\"']+)")


def _parse_cargo_config_sections(text: str) -> list[tuple[str, str, str]]:
    """Deliberately narrow, non-general TOML reader: yields
    (section, key, raw_value) for every `key = value` line found, tagged
    with the `[section]` header it falls under (empty string before the
    first header). NOT a full TOML parser — cargo config files are a
    narrow, well-known dialect (flat key=value under bracketed sections)
    and this only needs to find a handful of well-known keys, not
    round-trip arbitrary TOML. A `#`-comment is stripped outside of a
    multi-line array; a `rustflags = [\\n  "...",\\n]` array is reassembled
    onto one logical line via a bracket-depth counter before matching."""
    section = ""
    results: list[tuple[str, str, str]] = []
    pending_key: str | None = None
    pending_value = ""
    depth = 0
    for raw_line in text.splitlines():
        if depth == 0:
            line = raw_line.split("#", 1)[0]
            stripped = line.strip()
            m = _TOML_SECTION_RE.match(stripped)
            if m:
                section = m.group("name").strip("'\" ")
                continue
            if not stripped:
                continue
            kv = _TOML_KV_RE.match(stripped)
            if not kv:
                continue
            key, value = kv.group("key"), kv.group("value")
            depth += value.count("[") - value.count("]")
            if depth > 0:
                pending_key, pending_value = key, value
                continue
            results.append((section, key.strip("'\""), value))
        else:
            stripped = raw_line.split("#", 1)[0].strip()
            pending_value += " " + stripped
            depth += stripped.count("[") - stripped.count("]")
            if depth <= 0:
                results.append((section, (pending_key or "").strip("'\""), pending_value))
                pending_key, pending_value, depth = None, "", 0
    return results


def find_required_build_binaries(cargo_config_path: Path) -> list[tuple[str, str]]:
    """Scan .cargo/config.toml for external binaries the build hard-depends
    on. Returns (binary_name, human-readable source description) pairs.
    Returns [] if the file does not exist — most repos have no
    .cargo/config.toml at all, which is not a finding, just nothing to
    check."""
    if not cargo_config_path.is_file():
        return []
    text = cargo_config_path.read_text(encoding="utf-8")
    found: list[tuple[str, str]] = []
    for section, key, raw_value in _parse_cargo_config_sections(text):
        value = raw_value.strip()
        if key == "rustc-wrapper" and section in ("build", ""):
            name = value.strip("'\"")
            if name:
                found.append((name, f"[{section or 'build'}] rustc-wrapper"))
        if key.upper() == "RUSTC_WRAPPER" and section == "env":
            m = re.search(r"[\"']([^\"']+)[\"']", value)
            if m:
                found.append((m.group(1), "[env] RUSTC_WRAPPER"))
        if key == "linker" and section.startswith("target."):
            name = value.strip("'\"")
            if name:
                found.append((Path(name).name, f"[{section}] linker"))
        if key == "runner" and section.startswith("target."):
            tokens = value.strip("'\"").split()
            if tokens:
                found.append((Path(tokens[0]).name, f"[{section}] runner"))
        if key == "rustflags" and section.startswith("target."):
            for m in _FUSE_LD_RE.finditer(value):
                found.append((m.group(1), f"[{section}] rustflags -fuse-ld"))
            for m in _C_LINKER_RE.finditer(value):
                found.append((Path(m.group(1)).name, f"[{section}] rustflags -C linker"))
    return found


def _binary_referenced_in_workflow(binary: str, workflow_text: str) -> bool:
    """True if `binary` appears anywhere in a workflow file's raw YAML text
    — in practice this means an explicit install step (`apt-get install
    <bin>`, a `<bin>-action` marketplace action, `cargo install <bin>`,
    etc.). Deliberately a broad substring-on-word-boundary match rather
    than trying to parse every possible install-step shape: if a build tool
    never appears in a workflow's text AT ALL, nothing in that workflow
    ever installs it, full stop."""
    return re.search(rf"\b{re.escape(binary)}\b", workflow_text) is not None


def check_build_tool_dependencies(cargo_config_path: Path, workflow_texts: dict[str, str]) -> list[str]:
    """Returns a list of human-readable problems (empty = clean). Each
    problem names the missing binary, where the .cargo/config.toml
    dependency comes from, and which workflow file(s) run cargo but never
    mention the binary anywhere in their YAML."""
    binaries = find_required_build_binaries(cargo_config_path)
    if not binaries:
        return []
    problems = []
    for binary, source in binaries:
        missing_in = sorted(
            wf
            for wf, text in workflow_texts.items()
            if re.search(r"\bcargo\b", text) and not _binary_referenced_in_workflow(binary, text)
        )
        if missing_in:
            try:
                display_path = cargo_config_path.relative_to(REPO_ROOT)
            except ValueError:
                display_path = cargo_config_path
            problems.append(
                f"'{binary}' (required by {source} in {display_path}) is never "
                f"installed or otherwise referenced in: {', '.join(missing_in)}. cargo hard-errors "
                f"(does not soft-fall-back) if a configured rustc-wrapper/linker/runner binary is "
                f"not on PATH — add an explicit install step to those workflow(s), or remove the "
                f"{source} setting."
            )
    return problems


# ─────────────────────────────────────────────────────────────────────────
# GAP 4 — local-host external-toolchain detection for a `run:` step's own
# cargo invocation.
#
# This is the MIRROR IMAGE of GAP 2 above (check_build_tool_dependencies),
# not a duplicate of it — the two must never be confused or let one weaken
# the other:
#
#   * GAP 2 (runner-side): does this repo's OWN build config (.cargo/
#     config.toml: rustc-wrapper/linker/runner/rustflags) hard-depend on a
#     binary that NONE of the workflow files ever install on the CI RUNNER?
#     That is always a real defect — every runner is an ephemeral, shared,
#     reproducible box; if the workflow never installs the tool, the build
#     WILL break there. It fails `--consistency-check`, i.e. the whole gate,
#     loudly. (The incident this closes: commit 652f91c's `rustc-wrapper =
#     "sccache"` with no install step anywhere.)
#   * GAP 4 (local-side, here): does a `run:` step's cargo invocation name a
#     cargo FEATURE (`--features`/`-F`/`--all-features`) that, per this
#     repo's OWN root Cargo.toml, needs an external binary to COMPILE, that
#     THIS ONE DEV HOST happens not to have on PATH right now? That is never
#     a defect — CI-hosted runners, or a future host, may have it; only the
#     one machine running this replica right now doesn't. It is classified
#     NOT_VALIDATED_LOCALLY (already a NON_BLOCKING_STATUS) — reported
#     loudly, never counted as a pass, never fails the gate.
#
# The detection is deliberately NOT keyed on a job or workflow name (a
# `if job == "feature-matrix"` special-case was explicitly rejected for
# this — a rename, reorder, or a brand new job invoking the same feature
# would silently blind it). Instead it reads the ACTUAL shell text of the
# step being classified, extracts whatever `--features`/`-F`/
# `--all-features` flags cargo itself would see, resolves them through the
# root Cargo.toml's real `[features]` graph (parsed with the stdlib TOML
# parser — this table is standard, well-formed TOML, unlike the narrow
# `.cargo/config.toml` dialect GAP 2 hand-parses above), and checks each
# resolved feature against TOOLCHAIN_FEATURE_REQUIREMENTS.
#
# TOOLCHAIN_FEATURE_REQUIREMENTS is intentionally SHORT and evidence-backed,
# not a guess: every entry is verified against this repo's own documented
# design AND, where practical, empirically. Two verified findings that
# shaped it:
#   * `ros2-rmw` (Cargo.toml ~line 1009-1011) pulls `cyclonedds-rust-sys`,
#     whose build.rs vendors the CycloneDDS C sources and configures+builds
#     them with `cmake` at COMPILE time — a real, unconditional local
#     build-time dependency. Listed below.
#   * `gpu-cuda` is DELIBERATELY NOT listed, even though it is the feature
#     named in this task's own problem statement as needing `nvcc`. Cargo.
#     toml says outright (line ~157-158, ~1020-1021) that it "builds clean
#     everywhere via dynamic-loading" — verified by reading cudarc 0.17.8's
#     own build.rs (only its `cuda-version-from-build-system` feature path
#     shells out to `nvcc --version`; this repo pins the fixed
#     `cuda-12060` feature instead, so that path never runs) AND by
#     actually running `cargo check -p eg-ann --no-default-features
#     --features gpu-cuda` on a host with no nvcc anywhere on PATH, which
#     SUCCEEDED. `ros2-dds` (pure-Rust Dust DDS) and `ros2-bridge`
#     (pure-Rust tokio-tungstenite) build clean everywhere for the same
#     reason cudarc does: no C/GPU toolchain in their dependency graph at
#     all. Listing any of these three would be a FALSE POSITIVE — silently
#     downgrading a step that actually compiles fine here from a real,
#     counted RUN to an uncounted NOT_VALIDATED_LOCALLY is exactly the
#     coverage regression this whole script exists to prevent (see the
#     module docstring: a skip must always be true, never a guess).
# ─────────────────────────────────────────────────────────────────────────

TOOLCHAIN_FEATURE_REQUIREMENTS: dict[str, tuple[tuple[str, str], ...]] = {
    "ros2-rmw": (
        (
            "cmake",
            "ros2-rmw pulls the cyclonedds-rust-sys crate, whose build.rs "
            "vendors the CycloneDDS C sources and configures+builds them "
            "with cmake at compile time (root Cargo.toml, the ros2-rmw "
            "feature's doc comment, ~line 1009-1011) — this is unconditional "
            "at build time, independent of whether a live ROS2/rmw daemon "
            "is present to actually talk to.",
        ),
        (
            "cc",
            "the same vendored CycloneDDS C build cyclonedds-rust-sys "
            "performs for ros2-rmw also needs a C compiler on PATH.",
        ),
    ),
}


def _load_cargo_features(cargo_toml_path: Path = CARGO_TOML_PATH) -> dict[str, list[str]]:
    """Parse a Cargo.toml's `[features]` table with the stdlib TOML parser
    (this is ordinary, well-formed TOML — unlike .cargo/config.toml's
    narrower dialect, no hand-rolled parser is needed or appropriate here).
    Returns {} if the file is missing or carries no `[features]` table."""
    if not cargo_toml_path.is_file():
        return {}
    with open(cargo_toml_path, "rb") as f:
        doc = tomllib.load(f)
    return doc.get("features", {}) or {}


def _extract_requested_features(run_text: str) -> tuple[set[str], bool]:
    """Tokenize a step's shell text the way a shell would (falling back to a
    plain whitespace split if it contains something shlex can't tokenize,
    e.g. an unbalanced quote from a stripped GHA expression) and pull out
    every `--features`/`-F` value plus whether `--all-features` appears
    anywhere. Deliberately tolerant of multiple cargo invocations in one
    step (`&&`-chained) — every occurrence in the whole text is unioned.
    Comma- AND space-separated feature lists are both cargo-valid, so both
    are split on."""
    try:
        tokens = shlex.split(run_text, posix=True)
    except ValueError:
        tokens = run_text.split()

    features: set[str] = set()
    all_features = False
    i = 0
    while i < len(tokens):
        tok = tokens[i]
        if tok == "--all-features":
            all_features = True
        elif tok in ("--features", "-F"):
            if i + 1 < len(tokens):
                features.update(v.strip() for v in re.split(r"[,\s]+", tokens[i + 1]) if v.strip())
                i += 1
        elif tok.startswith("--features="):
            features.update(v.strip() for v in re.split(r"[,\s]+", tok[len("--features=") :]) if v.strip())
        i += 1
    return features, all_features


def _expand_features(requested: set[str], feature_table: dict[str, list[str]], all_features: bool) -> set[str]:
    """Transitively resolve a set of requested cargo feature names through
    the workspace `[features]` graph, e.g. `full-extras` -> {full-extras,
    full, gpu-cuda, ros2-bridge, ros2-dds, ros2-rmw, ...}. `--all-features`
    seeds the closure with every feature the table defines, matching
    cargo's own semantics for that flag. An implied entry of the form
    `dep:pkg` (optional-dependency activation) or `pkg/feature` /
    `pkg?/feature` (a DIFFERENT crate's feature) is not itself a name in
    THIS table, so it naturally stops the walk there rather than needing
    special-casing — only entries that are themselves keys in this
    workspace's own feature table are followed further."""
    if all_features:
        requested = requested | set(feature_table)
    seen: set[str] = set()
    stack = list(requested)
    while stack:
        f = stack.pop()
        if f in seen:
            continue
        seen.add(f)
        for implied in feature_table.get(f, []):
            name = implied.split("/", 1)[0].rstrip("?")
            if name.startswith("dep:"):
                continue
            if name in feature_table and name not in seen:
                stack.append(name)
    return seen


def check_toolchain_requirements(run_text: str, feature_table: dict[str, list[str]]) -> str | None:
    """Returns a human reason string naming the first missing required tool
    if `run_text` invokes cargo requesting, directly or transitively, a
    workspace feature TOOLCHAIN_FEATURE_REQUIREMENTS documents as needing an
    external build-time tool not currently on PATH — else None (nothing
    required, or everything required is present). Looks only at what the
    step's OWN command line actually names; no job/step name is consulted,
    so this applies uniformly to every RUN step in every workflow."""
    if "cargo" not in run_text:
        return None
    requested, all_features = _extract_requested_features(run_text)
    if not requested and not all_features:
        return None
    for feature in sorted(_expand_features(requested, feature_table, all_features)):
        for tool, why in TOOLCHAIN_FEATURE_REQUIREMENTS.get(feature, ()):
            if shutil.which(tool) is None:
                return (
                    f"{tool!r} not on PATH -- required to compile the {feature!r} feature here "
                    f"({why}). NOT a CI defect: this host lacks the toolchain, the build config "
                    f"does not lack an install step (see check_build_tool_dependencies for that "
                    f"check)."
                )
    return None


# ─────────────────────────────────────────────────────────────────────────
# GAP 3 — is skipping the replica safe for a given diff?
# ─────────────────────────────────────────────────────────────────────────


def _pattern_matches(relpath: str, pattern: str) -> bool:
    """Match one BUILD_AFFECTING_FILE_PATTERNS entry against a repo-relative
    path. A small, deliberately restricted glob dialect:
      - "DIR/**"  -> path is DIR itself or nested under DIR/
      - "**/X"    -> the "**/" prefix is stripped; X is then matched against
                     either the full relative path or just its basename, so
                     it applies at any depth
      - otherwise -> matched against the full relative path OR the
                     basename via fnmatch, so "Cargo.toml" matches both the
                     workspace root file and crates/foo/Cargo.toml, and
                     "rust-toolchain*" matches a root-level rust-toolchain
                     or rust-toolchain.toml
    fnmatch's `*` already matches across `/` (it has no path-separator
    awareness), so this needs no manual recursive-descent matching once
    "**/" is stripped."""
    relpath = relpath.replace(os.sep, "/")
    if pattern.endswith("/**"):
        root = pattern[:-3]
        return relpath == root or relpath.startswith(root + "/")
    if pattern.startswith("**/"):
        pattern = pattern[3:]
    basename = posixpath.basename(relpath)
    return fnmatch.fnmatch(relpath, pattern) or fnmatch.fnmatch(basename, pattern)


def is_build_affecting(path: str) -> bool:
    return any(_pattern_matches(path, p) for p in BUILD_AFFECTING_FILE_PATTERNS)


def build_affecting_files(paths: list[str]) -> list[str]:
    return [p for p in paths if is_build_affecting(p)]


def diff_touches_build_affecting_files(base_ref: str, repo_root: Path = REPO_ROOT) -> tuple[bool, list[str]]:
    """(is_it_safe_to_skip, matched_paths). Compares the working tree
    (including staged and unstaged changes) against base_ref. Safe to skip
    means the diff touches NONE of BUILD_AFFECTING_FILE_PATTERNS."""
    out = subprocess.run(
        ["git", "diff", "--name-only", base_ref],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    changed = [line.strip() for line in out.splitlines() if line.strip()]
    hits = build_affecting_files(changed)
    return (len(hits) == 0, hits)


def consistency_check(
    *,
    verbose: bool = True,
    workflows_dir: Path = WORKFLOWS_DIR,
    workflow_docs: dict[str, dict] | None = None,
    cargo_config_path: Path | None = None,
    workflow_texts: dict[str, str] | None = None,
) -> bool:
    if cargo_config_path is None:
        cargo_config_path = CARGO_CONFIG_PATH
    ok = True

    found_files = {p.name for p in discover_workflow_files(workflows_dir)}
    registered = set(WORKFLOW_REGISTRY)
    unregistered = sorted(found_files - registered)
    stale_registrations = sorted(registered - found_files)

    if unregistered:
        ok = False
        if verbose:
            print("CONSISTENCY CHECK FAILED — workflow file(s) present but not in WORKFLOW_REGISTRY:")
            for f in unregistered:
                print(f"  - {f!r}")
            print("Add a WorkflowSpec entry for it in scripts/ci_gate_replica.py's WORKFLOW_REGISTRY.")
    if stale_registrations:
        ok = False
        if verbose:
            print("CONSISTENCY CHECK FAILED — WORKFLOW_REGISTRY names workflow file(s) that no longer exist:")
            for f in stale_registrations:
                print(f"  - {f!r}")
            print("Remove the stale entry from WORKFLOW_REGISTRY.")

    total_jobs = 0
    total_steps = 0
    for fname, spec in WORKFLOW_REGISTRY.items():
        if workflow_docs is not None and fname in workflow_docs:
            doc = workflow_docs[fname]
        else:
            path = workflows_dir / fname
            if not path.is_file():
                continue  # already reported above as a stale registration
            doc = load_workflow(path)

        plan, unclassified, stale_jobs = build_plan_for_workflow(spec, doc)
        total_jobs += len(doc.get("jobs", {}) or {})
        total_steps += len(plan)

        if unclassified:
            ok = False
            if verbose:
                print(f"CONSISTENCY CHECK FAILED — {fname} has job(s) this replica does not classify:")
                for j in unclassified:
                    print(f"  - {j!r} is in neither executable_jobs nor job_skip_reasons for {fname}")
                print(f"Update WORKFLOW_REGISTRY[{fname!r}] to cover it.")
        if stale_jobs:
            ok = False
            if verbose:
                print(f"CONSISTENCY CHECK FAILED — WORKFLOW_REGISTRY[{fname!r}] names job(s) no longer in {fname}:")
                for j in stale_jobs:
                    print(f"  - {j!r}")
                print(f"Remove the stale entry from WORKFLOW_REGISTRY[{fname!r}].")

    # GAP 2, folded into the same fast, pure, every-commit check.
    if workflow_texts is None:
        workflow_texts = {
            fname: (workflows_dir / fname).read_text(encoding="utf-8")
            for fname in WORKFLOW_REGISTRY
            if (workflows_dir / fname).is_file()
        }
    build_tool_problems = check_build_tool_dependencies(cargo_config_path, workflow_texts)
    if build_tool_problems:
        ok = False
        if verbose:
            print(
                "CONSISTENCY CHECK FAILED — a build-config external-binary dependency "
                "is never installed by the workflow(s) that would need it:"
            )
            for p in build_tool_problems:
                print(f"  - {p}")

    if ok and verbose:
        print(
            f"CONSISTENCY CHECK PASSED — {len(WORKFLOW_REGISTRY)} workflow(s) registered "
            f"({sorted(WORKFLOW_REGISTRY)}), {total_jobs} job(s), all classified, "
            f"{total_steps} step(s) total across all matrix legs; no unresolved build-tool "
            f"dependency found."
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
    env.update({k: str(v) for k, v in (doc["jobs"][job_id].get("env") or {}).items()})
    env.update(LOCAL_ENV_OVERRIDES)
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
    for pattern in ("numdist-primary", "numdist-reproduction", "dist-primary", "dist-reproduction"):
        p = REPO_ROOT / pattern
        if p.exists():
            shutil.rmtree(p, ignore_errors=True)
            removed.append(pattern)
    if removed:
        print(f"[local-only hygiene, NOT a workflow step] removed stale: {removed}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--consistency-check", action="store_true", help="only run the anti-drift check")
    ap.add_argument("--dry-run", action="store_true", help="print the execution plan; run nothing")
    ap.add_argument(
        "--skip-safe",
        nargs="?",
        const="HEAD",
        metavar="BASE_REF",
        help="print whether skipping this gate is safe for the diff vs BASE_REF (default HEAD); exit 0 if safe, 1 if not",
    )
    ap.add_argument("--workflows-dir", type=Path, default=WORKFLOWS_DIR, help="override the workflows directory (testing)")
    args = ap.parse_args()

    if args.skip_safe is not None:
        safe, hits = diff_touches_build_affecting_files(args.skip_safe)
        if safe:
            print(f"SKIP-SAFE: no build-affecting files changed vs {args.skip_safe!r}; skipping ci-gate-replica is safe.")
            return 0
        print(f"NOT SKIP-SAFE: build-affecting file(s) changed vs {args.skip_safe!r}; do NOT skip ci-gate-replica:")
        for h in hits:
            print(f"  - {h}")
        return 1

    ok = consistency_check(workflows_dir=args.workflows_dir)

    if args.consistency_check:
        return 0 if ok else 1

    if not ok:
        print("\nRefusing to run the gate: the consistency check above failed. This IS the")
        print("anti-drift guard working as intended — fix the classification, don't bypass it.")
        return 1

    all_plan: list[dict] = []
    docs: dict[str, dict] = {}
    for fname, spec in WORKFLOW_REGISTRY.items():
        path = args.workflows_dir / fname
        if not path.is_file():
            continue
        doc = load_workflow(path)
        docs[fname] = doc
        plan, _, _ = build_plan_for_workflow(spec, doc)
        all_plan.extend(plan)

    if not args.dry_run:
        _local_hygiene()

    # job_envs keyed by (workflow, base job id — pre-matrix-suffix) so every
    # matrix leg of one job still threads $GITHUB_ENV/$GITHUB_PATH state
    # independently is not required here (GH Actions env is per-job-run,
    # i.e. per matrix leg, in reality) — key by the exact plan "job" label
    # instead, built lazily on first use.
    job_envs: dict[tuple[str, str], dict] = {}

    def env_for(item: dict) -> dict:
        base_job_id = item["job"].split("#", 1)[0]
        key = (item["workflow"], item["job"])
        if key not in job_envs:
            job_envs[key] = _job_base_env(docs[item["workflow"]], base_job_id)
        return job_envs[key]

    print(f"=== ci_gate_replica.py START {datetime.datetime.now(datetime.timezone.utc).isoformat()} nproc={os.cpu_count()} ===")

    results: list[dict] = []
    for item in all_plan:
        mode = item["mode"]
        if mode == "RUN":
            if args.dry_run:
                print(f"[DRY-RUN] would RUN [{item['workflow']}:{item['job']}] {item['name']}")
                results.append({**item, "status": "DRY_RUN", "elapsed": 0.0})
                continue
            print(f"\n############### STEP [{item['workflow']}:{item['job']}] {item['name']} ###############")
            status, elapsed = _run_step(item["detail"], env_for(item))
            print(f"### STEP_RESULT job={item['job']} name={item['name']!r} exit={status} secs={elapsed:.1f}")
            results.append({**item, "status": status, "elapsed": elapsed})
        elif mode == "ENV_SETUP":
            results.append({**item, "status": "ENV_SETUP", "elapsed": 0.0})
        elif mode == "ARTIFACT_IO":
            results.append({**item, "status": "ARTIFACT_IO", "elapsed": 0.0})
        else:
            tag = "TOOLCHAIN ABSENT LOCALLY" if mode == "TOOLCHAIN_MISSING" else "NOT VALIDATED LOCALLY"
            print(f"\n### {tag} [{item['workflow']}:{item['job']}] {item['name']}\n    reason: {item['detail']}")
            results.append({**item, "status": "NOT_VALIDATED_LOCALLY", "elapsed": 0.0})

    print("\n################ SUMMARY ################")
    blocking_fail = False
    advisory_fail = False
    for r in results:
        status = r["status"]
        label = f"{r['workflow']}:{r['job']}"
        print(f"{label:36s} {r['name'][:56]:56s} status={str(status):22s} secs={r['elapsed']:8.1f}")
        bad = (isinstance(status, str) and status not in NON_BLOCKING_STATUSES) or (
            isinstance(status, int) and status != 0
        )
        if bad:
            if r["blocking"]:
                blocking_fail = True
            else:
                advisory_fail = True

    not_validated = [r for r in results if r["status"] == "NOT_VALIDATED_LOCALLY"]
    if not_validated:
        print(
            f"\n### {len(not_validated)} STEP(S) NOT VALIDATED LOCALLY — these were NEVER RUN on "
            "this host and are NOT a pass, never counted as one:"
        )
        for r in not_validated:
            print(f"  - [{r['workflow']}:{r['job']}] {r['name']}\n      reason: {r['detail']}")

    print(f"BLOCKING_FAIL={'1' if blocking_fail else '0'}")
    print(f"ADVISORY_FAIL={'1' if advisory_fail else '0'} (never fails the pre-push gate — reported loudly only)")
    print(f"OVERALL_FAIL={'1' if blocking_fail else '0'}")
    print(f"=== SENTINEL_COMPLETE {datetime.datetime.now(datetime.timezone.utc).isoformat()} ===")
    return 1 if blocking_fail else 0


if __name__ == "__main__":
    sys.exit(main())
