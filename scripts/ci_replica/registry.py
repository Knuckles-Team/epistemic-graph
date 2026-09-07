"""Per-repo classification surface for the CI replica.

The ONLY hand-maintained input to the replica: which workflow files exist,
which of their jobs this host executes, why the rest are skipped, what a local
run must override, and which files make a diff build-affecting. Everything else
is derived from the parsed YAML.
"""

from __future__ import annotations

import fnmatch
import os
import posixpath
import re
import subprocess
from dataclasses import dataclass, field
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
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
    # (repro-diagnose.yml — workflow_dispatch-only, never wired into the
    # release chain).
    blocking: bool
    executable_jobs: frozenset[str]
    job_skip_reasons: dict[str, str] = field(default_factory=dict)


WORKFLOW_REGISTRY: dict[str, WorkflowSpec] = {
    "release.yml": WorkflowSpec(
        filename="release.yml",
        blocking=True,
        # `lint-and-architecture` folded in from the former advisory.yml
        # 2026-08-28 (wD9-CIGATE) — it is now a real blocking job (no
        # `continue-on-error`) and, like `gates`, is ordinary
        # python3/cargo-clippy commands a local host can run.
        #
        # `feature-matrix`/`benchmarks` also folded in (same date) and stay
        # in `executable_jobs`, unchanged from their old advisory.yml
        # treatment (GAP 1's original point: an entire workflow was
        # otherwise invisible locally until it broke in hosted CI) --
        # ordinary `cargo build`/`cargo bench` commands once `${{ matrix... }}`
        # is substituted. Each still carries its own literal
        # `continue-on-error: true` in release.yml (a disclosed,
        # not-yet-measured gap -- see the wD9-CIGATE report), which
        # `build_plan_for_workflow` now reads PER-JOB to report
        # `blocking: False` for their rows specifically, even though this
        # whole file is otherwise `blocking=True` -- a failure there is run
        # and reported loudly here, exactly like every other RUN step, but
        # does not fail this local hook, matching what the real workflow
        # does today. Re-scoped `if: startsWith(github.ref, 'refs/tags/v')
        # || workflow_dispatch` in the real workflow (no longer runs on an
        # ordinary push/PR at all), which this replica does not model --
        # running the ordinary command locally on every invocation remains
        # the safer, more-coverage default matching GAP 1's intent.
        executable_jobs=frozenset(
            {"gates", "lint-and-architecture", "feature-matrix", "benchmarks"}
        ),
        job_skip_reasons={
            "scanner-quality": (
                "CI-only scanner profile: provisions the exact CCCC/KISS/dupehound/"
                "jscpd/import-linter/dependency-cruiser/arch-lint versions into an "
                "ephemeral runner directory. The local pre-commit/pre-push profile "
                "runs the same fail-closed wrappers and native architecture checks "
                "against preinstalled tools; replaying this job locally would "
                "download and compile tools during a hook, which is forbidden. "
                "Every step is therefore reported NOT VALIDATED LOCALLY rather than "
                "silently omitted."
            ),
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
            "pages": (
                "folded in from advisory.yml 2026-08-28 (wD9-CIGATE) — job-level "
                "`uses:` calling a reusable workflow (Knuckles-Team/pipelines "
                "pages_pipeline.yml) — no `steps:` at all, so nothing here is a "
                "local shell command; needs the GitHub Pages environment/actions "
                "with no local equivalent."
            ),
        },
    ),
    "repro-diagnose.yml": WorkflowSpec(
        filename="repro-diagnose.yml",
        # Never release-blocking: workflow_dispatch-only, never triggered by
        # push/pull_request, and not `needs:`-wired into release.yml's chain.
        blocking=False,
        executable_jobs=frozenset(),
        job_skip_reasons={
            "diagnose": (
                "workflow_dispatch-only, windows-latest-only fast-loop diagnostic "
                "for the windows-x86_64 release wheel reproducibility bug (builds a "
                "slim maturin wheel twice and compares them) — needs the Windows "
                "MSVC toolchain a local dev host does not have, same class of "
                "native-toolchain limitation as release.yml's `build` job skip "
                "reason above. Never runs on push/PR and never blocks a release."
            ),
        },
    ),
    # advisory.yml was retired 2026-08-28 (wD9-CIGATE): its jobs are folded
    # into release.yml above (see that WorkflowSpec's comments), and the
    # two-workflow/continue-on-error model it embodied is gone. No entry
    # here anymore — advisory.yml no longer exists.
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
    "CARGO_TARGET_DIR": os.environ.get(
        "CI_GATE_CARGO_TARGET_DIR", "/var/tmp/eg-ci-gate-target"
    ),
    "TMPDIR": os.environ.get("CI_GATE_TMPDIR", "/var/tmp/eg-ci-gate-tmp"),
    # A CI runner is ephemeral and has no user-site directory. A developer host
    # does, and inheriting it is not a harmless difference -- it manufactures
    # code verdicts out of stale artifacts.
    #
    # Measured 2026-08-21: `numpy-free boundary contract (NE-249)` failed 16 of
    # 17 cases against a `numeric.abi3.so` in ~/.local dated 10 July. The step
    # before it, `pip install --no-index --find-links target/wheels eg-numeric`,
    # had reported success while installing nothing: eg-numeric's version is
    # permanently 0.1.0, so pip found that six-week-old build "already
    # satisfied" and skipped. The freshly built wheel passes 17/17 -- verified
    # directly in a clean venv. The engine was never the problem; the reported
    # failures described a July build of the very thing under test.
    #
    # PYTHONNOUSERSITE hides ~/.local without hiding the system site-packages,
    # so the install actually lands and the test exercises what it claims to.
    "PYTHONNOUSERSITE": "1",
}

# A cold local Cargo gate can fan out enough compiler jobs to exhaust the
# machine before systemd-oomd gets a chance to intervene. Keep the override
# explicit and bounded: CI_GATE_CARGO_BUILD_JOBS is the ONLY operator input,
# malformed/non-positive values are rejected, and values above this hard local
# maximum are clamped rather than allowed to create an unsafe escape hatch.
MAX_LOCAL_CARGO_BUILD_JOBS = 8
CI_GATE_CARGO_BUILD_JOBS_ENV = "CI_GATE_CARGO_BUILD_JOBS"
_STRICT_POSITIVE_INTEGER_RE = re.compile(r"[1-9][0-9]*\Z")


def resolve_cargo_build_jobs(
    *,
    override: str | None = None,
    detected_cpus: int | None = None,
) -> int:
    """Resolve the bounded Cargo parallelism used by local workflow steps.

    ``override`` is primarily a test seam; in normal execution it is read
    from ``CI_GATE_CARGO_BUILD_JOBS``. The value is intentionally parsed
    strictly (no whitespace, sign, decimal, or empty string), and values over
    :data:`MAX_LOCAL_CARGO_BUILD_JOBS` are clamped to that hard ceiling.
    Without an override, use ``min(4, detected CPUs)`` with a floor of one;
    ``os.cpu_count()`` returning ``None`` is treated as one CPU.
    """
    if override is None:
        override = os.environ.get(CI_GATE_CARGO_BUILD_JOBS_ENV)
    if override is not None:
        if not _STRICT_POSITIVE_INTEGER_RE.fullmatch(override):
            raise ValueError(
                f"{CI_GATE_CARGO_BUILD_JOBS_ENV} must be a strict positive integer "
                f"(got {override!r})"
            )
        return min(int(override), MAX_LOCAL_CARGO_BUILD_JOBS)

    if detected_cpus is None:
        detected_cpus = os.cpu_count()
    return max(1, min(4, detected_cpus or 1))


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
    ".python-version",
    "build.rs",
    ".github/workflows/**",
    ".pre-commit-config.yaml",
    # Scanner contracts and architecture policies are executable build/release
    # inputs.  A diff in one of these files must not permit callers to skip the
    # workflow-derived gate on the grounds that no Rust source changed.
    "pyproject.toml",
    ".kiss/**",
    ".kissconfig",
    ".importlinter",
    "arch-lint.toml",
    "**/.dependency-cruiser.cjs",
    "**/.dependency-cruiser.js",
    "clients/js/package.json",
    "clients/js/package-lock.json",
    "scripts/scanner_contract.py",
    "scripts/check_complexity_staged.py",
    "scripts/check_dupehound.py",
    "scripts/check_duplication.py",
    "scripts/check_kiss_staged.sh",
    "scripts/list_scanner_sources.py",
    "scripts/validate_cccc_census.py",
)


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


def diff_touches_build_affecting_files(
    base_ref: str, repo_root: Path = REPO_ROOT
) -> tuple[bool, list[str]]:
    """(is_it_safe_to_skip, matched_paths). Compares the working tree
    (including staged and unstaged changes) against base_ref. Safe to skip
    means the diff touches NONE of BUILD_AFFECTING_FILE_PATTERNS."""
    out = subprocess.run(
        ["git", "diff", "--name-only", base_ref],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=True,
        stdin=subprocess.DEVNULL,
        timeout=120,
    ).stdout
    changed = [line.strip() for line in out.splitlines() if line.strip()]
    hits = build_affecting_files(changed)
    return (len(hits) == 0, hits)


