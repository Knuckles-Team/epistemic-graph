"""The anti-drift consistency check: workflow files versus this replica.

Every class of drift is one `DriftReport`; `consistency_check` assembles them in
a fixed order and answers whether any of them has offenders.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from ci_replica.build_toolchain import check_build_tool_dependencies
from ci_replica.registry import (
    CARGO_CONFIG_PATH,
    WORKFLOW_REGISTRY,
    WORKFLOWS_DIR,
    WorkflowSpec,
)
from ci_replica.workflow_plan import (
    build_plan_for_workflow,
    discover_workflow_files,
    load_workflow,
)


@dataclass(frozen=True)
class DriftReport:
    """One class of drift between a workflow file and this replica's registry.

    `bullets` are the already-rendered offender lines; `remedy` is the single
    closing instruction printed after them (empty when the headline already
    says what to do). An empty `bullets` is not drift.
    """

    headline: str
    bullets: list[str]
    remedy: str = ""

    def report(self) -> None:
        print(f"CONSISTENCY CHECK FAILED — {self.headline}")
        for bullet in self.bullets:
            print(f"  - {bullet}")
        if self.remedy:
            print(self.remedy)


def _registry_drift(workflows_dir: Path) -> list[DriftReport]:
    """Workflow files with no registry entry, and registry entries with no file."""
    found_files = {p.name for p in discover_workflow_files(workflows_dir)}
    registered = set(WORKFLOW_REGISTRY)
    return [
        DriftReport(
            "workflow file(s) present but not in WORKFLOW_REGISTRY:",
            [repr(f) for f in sorted(found_files - registered)],
            "Add a WorkflowSpec entry for it in scripts/ci_gate_replica.py's "
            "WORKFLOW_REGISTRY.",
        ),
        DriftReport(
            "WORKFLOW_REGISTRY names workflow file(s) that no longer exist:",
            [repr(f) for f in sorted(registered - found_files)],
            "Remove the stale entry from WORKFLOW_REGISTRY.",
        ),
    ]


def _job_drift(fname: str, unclassified: list[str], stale_jobs: list[str]) -> list[DriftReport]:
    """Jobs one workflow has that the registry does not classify, and vice versa."""
    return [
        DriftReport(
            f"{fname} has job(s) this replica does not classify:",
            [
                f"{j!r} is in neither executable_jobs nor job_skip_reasons "
                f"for {fname}"
                for j in unclassified
            ],
            f"Update WORKFLOW_REGISTRY[{fname!r}] to cover it.",
        ),
        DriftReport(
            f"WORKFLOW_REGISTRY[{fname!r}] names job(s) no longer in {fname}:",
            [repr(j) for j in stale_jobs],
            f"Remove the stale entry from WORKFLOW_REGISTRY[{fname!r}].",
        ),
    ]


def _registered_workflows(
    workflows_dir: Path, workflow_docs: dict[str, dict] | None
) -> list[tuple[str, WorkflowSpec, dict]]:
    """Every registered workflow whose file exists, with its parsed document.
    A registration with no file is skipped here — `_registry_drift` reports it."""
    loaded = []
    for fname, spec in WORKFLOW_REGISTRY.items():
        if workflow_docs is not None and fname in workflow_docs:
            loaded.append((fname, spec, workflow_docs[fname]))
            continue
        path = workflows_dir / fname
        if path.is_file():
            loaded.append((fname, spec, load_workflow(path)))
    return loaded


def _workflow_texts(workflows_dir: Path) -> dict[str, str]:
    """Raw YAML of every registered workflow present on disk."""
    return {
        fname: (workflows_dir / fname).read_text(encoding="utf-8")
        for fname in WORKFLOW_REGISTRY
        if (workflows_dir / fname).is_file()
    }


def _workflow_job_census(
    loaded: list[tuple[str, WorkflowSpec, dict]],
) -> tuple[list[DriftReport], int, int]:
    """Per-workflow job drift plus the job/step totals the summary line reports."""
    drift: list[DriftReport] = []
    total_jobs = 0
    total_steps = 0
    for fname, spec, doc in loaded:
        plan, unclassified, stale_jobs = build_plan_for_workflow(spec, doc)
        total_jobs += len(doc.get("jobs", {}) or {})
        total_steps += len(plan)
        drift.extend(_job_drift(fname, unclassified, stale_jobs))
    return drift, total_jobs, total_steps


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

    drift = _registry_drift(workflows_dir)
    per_workflow, total_jobs, total_steps = _workflow_job_census(
        _registered_workflows(workflows_dir, workflow_docs)
    )
    drift.extend(per_workflow)

    # GAP 2, folded into the same fast, pure, every-commit check.
    if workflow_texts is None:
        workflow_texts = _workflow_texts(workflows_dir)
    drift.append(
        DriftReport(
            "a build-config external-binary dependency is never installed by "
            "the workflow(s) that would need it:",
            [
                str(problem)
                for problem in check_build_tool_dependencies(
                    cargo_config_path, workflow_texts
                )
            ],
        )
    )

    failures = [report for report in drift if report.bullets]
    if not verbose:
        return not failures
    for failure in failures:
        failure.report()
    if failures:
        return False
    print(
        f"CONSISTENCY CHECK PASSED — {len(WORKFLOW_REGISTRY)} workflow(s) registered "
        f"({sorted(WORKFLOW_REGISTRY)}), {total_jobs} job(s), all classified, "
        f"{total_steps} step(s) total across all matrix legs; no unresolved build-tool "
        f"dependency found."
    )
    return True
