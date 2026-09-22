"""Turn one parsed workflow file into concrete, per-matrix-leg plan rows."""

from __future__ import annotations

import re
import sys
from pathlib import Path

import yaml

from ci_replica.build_toolchain import (
    _load_cargo_features,
    check_toolchain_requirements,
)
from ci_replica.registry import (
    ARTIFACT_IO_ACTIONS,
    ENV_SETUP_ACTIONS,
    GHA_EXPR_RE,
    MATRIX_EXPR_RE,
    WORKFLOWS_DIR,
    WorkflowSpec,
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


def _cartesian_legs(matrix: dict) -> list[dict]:
    """One combo dict per combination of the matrix's list-valued axes.
    `include`/`exclude` are not axes: include entries are appended by the
    caller and exclude is deliberately unimplemented (see
    _matrix_combinations)."""
    axes = {
        k: v
        for k, v in matrix.items()
        if k not in ("include", "exclude") and isinstance(v, list)
    }
    combos: list[dict] = [{}] if axes else []
    for key, values in axes.items():
        combos = [dict(c, **{key: v}) for c in combos for v in values]
    return combos


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
    combos = _cartesian_legs(matrix)
    combos.extend(
        dict(extra)
        for extra in (matrix.get("include") or [])
        if isinstance(extra, dict)
    )
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
        return [
            {
                "uses": job["uses"],
                "name": job.get("name") or f"(reusable workflow: {job['uses']})",
            }
        ]
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
        print(
            f"FATAL: {path} did not parse into a workflow with a top-level 'jobs:' map",
            file=sys.stderr,
        )
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
        return (
            "SKIP_LOUD",
            f"reusable workflow '{uses}' has no local runner equivalent — not executed "
            f"here",
        )
    return (
        "SKIP_LOUD",
        f"marketplace action '{uses}' has no local equivalent — not executed here",
    )


def _job_blocking(spec_blocking: bool, job: dict) -> bool:
    """A job's own literal `continue-on-error: true` makes IT non-blocking
    regardless of the file-level `spec.blocking` (release.yml carries blocking
    product, runtime-contract, and security jobs alongside advisory
    documentation, lint, scanner, feature-matrix, and benchmark jobs). Only the
    literal boolean `True` is recognized; an expression
    form (e.g. `${{ matrix.optional || false }}`, `build`'s macOS leg) is
    treated conservatively as blocking since it cannot be evaluated
    generically here -- `build` itself is not in any WorkflowSpec.
    executable_jobs, so this never actually needs to resolve that
    expression today."""
    return spec_blocking and job.get("continue-on-error") is not True


def _step_disposition(
    step: dict, skip_reason: str | None, feature_table: dict[str, list[str]]
) -> tuple[str, str]:
    """How one already-matrix-substituted step runs in this replica.

    `skip_reason` is the owning job's `WorkflowSpec.job_skip_reasons` entry when
    the job is not executable here; a step of an executable job is classified on
    its own text, then demoted to TOOLCHAIN_MISSING when its cargo invocation
    needs a build tool this host does not have (GAP 4).
    """
    if skip_reason is not None:
        return "SKIP_LOUD", skip_reason
    mode, detail = classify_step(step)
    if mode != "RUN":
        return mode, detail
    toolchain_reason = check_toolchain_requirements(detail, feature_table)
    if toolchain_reason is not None:
        return "TOOLCHAIN_MISSING", toolchain_reason
    return mode, detail


def _job_plan_rows(
    spec: WorkflowSpec,
    job_id: str,
    job: dict,
    skip_reason: str | None,
    feature_table: dict[str, list[str]],
) -> list[dict]:
    """Every plan row one job contributes: one per (matrix leg x step)."""
    steps = _job_steps(job)
    blocking = _job_blocking(spec.blocking, job)
    matrix = ((job.get("strategy") or {}).get("matrix")) or None
    rows: list[dict] = []
    for combo in _matrix_combinations(matrix):
        job_label = job_id + _combo_label(combo)
        for step in steps:
            substituted = _apply_matrix(step, combo)
            mode, detail = _step_disposition(substituted, skip_reason, feature_table)
            rows.append(
                {
                    "workflow": spec.filename,
                    "blocking": blocking,
                    "job": job_label,
                    "name": _step_label(substituted),
                    "mode": mode,
                    "detail": detail,
                }
            )
    return rows


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
        if job_id in spec.executable_jobs:
            skip_reason = None
        elif job_id in spec.job_skip_reasons:
            skip_reason = spec.job_skip_reasons[job_id]
        else:
            unclassified.append(job_id)
            continue

        plan.extend(_job_plan_rows(spec, job_id, job or {}, skip_reason, feature_table))

    known = spec.executable_jobs | set(spec.job_skip_reasons)
    stale = sorted(j for j in known if j not in jobs)
    return plan, sorted(unclassified), stale
