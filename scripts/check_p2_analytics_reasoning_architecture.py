#!/usr/bin/env python3
"""Static architecture gate for P2 analytics and incremental reasoning."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(Path(__file__).resolve().parent))

from rust_module_tree import read_module_tree  # noqa: E402


def _source(path: str) -> str:
    """The compiler-declared production source of a Rust module, not one file.

    Each needle below names a behaviour a *module* must carry. Reading only the
    facade file made every assertion silently stale the moment that module was
    decomposed into `<name>.rs` + `<name>/**`: the four consensus needles this
    gate reported missing from `src/server/dispatch.rs` all still exist, they
    just moved into `src/server/dispatch/consensus.rs`. The test-inclusive
    view is deliberate -- it is the exact superset of the single file this gate
    used to read (one needle here, `replicated_submission_and_claim_converge_
    across_independent_projections`, names a required test), so repointing adds
    declared children without narrowing any existing assertion. Non-Rust inputs
    (`Cargo.toml`) are read verbatim.
    """

    if not path.endswith(".rs"):
        return (ROOT / path).read_text(encoding="utf-8")
    return read_module_tree(path, root_dir=ROOT, include_tests=True)


def require(path: str, needles: list[str], failures: list[str]) -> str:
    text = _source(path)
    for needle in needles:
        if needle not in text:
            failures.append(f"{path}: missing {needle!r}")
    return text


#: Every source that must carry a declared set of literal markers, in the
#: order this gate reports them.
_REQUIRED_MARKERS: dict[str, tuple[str, ...]] = {
    "crates/eg-jobs/src/model.rs": (
        "Publishing {",
        "WorkerLease",
        "content_digest",
        "not_before_ms",
    ),
    "crates/eg-jobs/src/store.rs": (
        "claim_next(",
        "renew_lease(",
        "checkpoint_fenced(",
        "stage_result_fenced(",
        "complete_publication_fenced(",
        "complete_publication_prepared(",
        "release_publication_lease_fenced(",
        "fail_attempt_fenced(",
        "require_lease_ownership(",
        "cancellation_reconcile",
        "job_id_for_batch(",
        "replicated_submission_and_claim_converge_across_independent_projections",
    ),
    "src/server/handlers/jobs.rs": (
        "mine_labeled_cancellable(",
        "TenantJobQuota",
        "stage_result_fenced(",
        "commit_internal_graph_methods(",
        "complete_publication_fenced(",
        "verified v2 RequestContext",
        "validate_remote_result_privacy(",
        "association_rule_id_from_row(",
        "refresh_job_metrics(",
        'std::env::var("EG_ANALYTICS_WORKERS")',
        "for slot in 0..workers",
        "analytics jobs require a configured durable persistence directory",
        '"kernel_cancelled"',
        '"invalid_payload"',
        "PreparedJobPublication",
        "RoutedJobPublication",
        "FinalizeJobPublication",
        "apply_consensus_job_publication_commit(",
        "apply_consensus_job_publication_finalize(",
    ),
    "src/server/dispatch.rs": (
        "execute_consensus_job_publication(",
        "submit_consensus_job_publication_command(",
        '"target-commit"',
        '"scheduler-finalize"',
    ),
    "src/raft/mod.rs": (
        "JobPublicationCommit {",
        "JobPublicationFinalize {",
    ),
    "crates/eg-types/src/jobs.rs": (
        "WorkerClaim {",
        "WorkerRenew {",
        "WorkerCheckpoint {",
        "WorkerStage {",
        "WorkerPublish {",
        "WorkerFail {",
        "WorkerCancel {",
        "pub struct JobResult",
        "pub memory_bytes: Option<u64>",
        "pub required_capabilities: Vec<String>",
    ),
    "src/server/auth.rs": (
        "allows_analytics_worker",
        'allows_action("analytics:worker")',
    ),
    "src/server/reasoning_projection.rs": (
        "claim_mutation_outbox(",
        "read_mutation_batch(",
        "ReasoningProjectionWakeup",
        "operations_sha256",
        "read_mutation_projection_cursor(",
        "persist_index(",
        "read_index(",
        "claim_recompute(",
        "authoritative graph version changed",
        "file.sync_all()",
        "ack_mutation_outbox(",
    ),
    "crates/eg-epistemic/src/incremental.rs": (
        "IncrementalReasoningIndex",
        "stale_dependents",
        "CONTRADICTS",
        "CAUSES",
        "opaque_identity",
        "refresh_materialization",
        "ProjectionInvalidationKind",
        "recompute_fences",
        "IncrementalReasoningEvent",
        "apply_wakeup(",
        "IncrementalReasoningEvent::Recompute",
        "recompute_from_ref(",
    ),
    "src/server/handlers/query.rs": (
        '"reasoning-recompute"',
        'status: "Queued".to_string()',
        "projection_pending: true",
    ),
    "Cargo.toml": (
        '"jobs"',
        '"epistemic-tms"',
        '"epistemic-causal"',
    ),
}


#: `Succeeded` is reachable only from `Publishing`, so no publication-completion
#: entry point may carry a `Running` source arm. The historical check named a
#: `pub fn succeed` that has not existed since eg-jobs was split: `str.find`
#: returned -1, the slice was empty, and the assertion could never fire. Every
#: offset-based contract below therefore treats an absent marker as FATAL.
_PUBLICATION_COMPLETION_FNS = (
    "pub fn complete_publication_fenced(",
    "pub fn complete_publication_prepared(",
)
_RUNNING_SOURCE_ARM = "JobState::Running {"
_JOB_STORE = "crates/eg-jobs/src/store.rs"


def _method_body(source: str, signature: str) -> str | None:
    """One `impl` method's text, or None when its signature is absent.

    The body ends at the first impl-level closing brace, so a following
    method's doc comment can never be read as part of it.
    """
    start = source.find(signature)
    if start < 0:
        return None
    end = source.find("\n    }\n", start)
    return source[start:] if end < 0 else source[start : end + len("\n    }\n")]


def _ordered_markers(
    source: str, path: str, first: str, second: str, violation: str
) -> list[str]:
    """`first` must appear before `second` in `source`; an absent marker fails."""
    for marker in (first, second):
        if marker not in source:
            return [
                f"{path}: cannot locate {marker!r} to check its ordering contract"
            ]
    return [violation] if source.find(first) > source.find(second) else []


def _publication_barrier_failures(store: str) -> list[str]:
    """No publication-completion method may accept a Running job."""
    failures = []
    for signature in _PUBLICATION_COMPLETION_FNS:
        body = _method_body(store, signature)
        if body is None:
            failures.append(
                f"{_JOB_STORE}: cannot locate {signature!r}; the publication "
                "barrier cannot be checked"
            )
        elif _RUNNING_SOURCE_ARM in body:
            failures.append(
                f"{_JOB_STORE}: {signature[len('pub fn '):-1]} still permits "
                "Running -> Succeeded"
            )
    return failures


def _analytics_ordering_failures(sources: dict[str, str]) -> list[str]:
    """Ordering and authority contracts no single marker can express."""
    handler = sources["src/server/handlers/jobs.rs"]
    reasoning = sources["src/server/reasoning_projection.rs"]
    failures = _publication_barrier_failures(sources[_JOB_STORE])
    failures.extend(
        _ordered_markers(
            handler,
            "src/server/handlers/jobs.rs",
            "stage_result_fenced(",
            "complete_publication_fenced(",
            "handler completes publication before staging the typed result",
        )
    )
    failures.extend(
        _ordered_markers(
            reasoning,
            "src/server/reasoning_projection.rs",
            "persist_index(",
            "ack_mutation_outbox(",
            "reasoning cursor can advance before projection persistence",
        )
    )
    if (
        "from_slice::<Vec<MutationOperation>>(&lease.record.intent.payload)"
        in reasoning
    ):
        failures.append(
            "reasoning worker treats digest-only outbox payload as mutation data"
        )
    if "global_index()" in reasoning:
        failures.append("reasoning worker references a process-global TMS authority")
    return failures


def _full_build_failures(sources: dict[str, str]) -> list[str]:
    """The `full` feature must build the analytics/reasoning stack it declares."""
    cargo = sources["Cargo.toml"]
    failures = []
    full_line = next(
        (line for line in cargo.splitlines() if line.startswith("full =")), ""
    )
    for feature in ("jobs", "epistemic-tms", "epistemic-causal"):
        if f'"{feature}"' not in full_line:
            failures.append(f"full build omits {feature}")
    if "Publishing" not in sources["crates/eg-jobs/src/model.rs"]:
        failures.append("analytics state machine has no publication barrier")
    return failures


def main() -> int:
    failures: list[str] = []
    sources = {
        path: require(path, list(markers), failures)
        for path, markers in _REQUIRED_MARKERS.items()
    }
    failures.extend(_analytics_ordering_failures(sources))
    failures.extend(_full_build_failures(sources))

    if failures:
        print("P2 analytics/reasoning architecture gate failed:", file=sys.stderr)
        for failure in failures:
            print(f"- {failure}", file=sys.stderr)
        return 1
    print("P2 analytics/reasoning architecture gate passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
