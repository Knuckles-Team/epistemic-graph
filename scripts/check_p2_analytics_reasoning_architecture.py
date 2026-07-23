#!/usr/bin/env python3
"""Static architecture gate for P2 analytics and incremental reasoning."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def require(path: str, needles: list[str], failures: list[str]) -> str:
    text = (ROOT / path).read_text(encoding="utf-8")
    for needle in needles:
        if needle not in text:
            failures.append(f"{path}: missing {needle!r}")
    return text


def main() -> int:
    failures: list[str] = []
    model = require(
        "crates/eg-jobs/src/model.rs",
        ["Publishing {", "WorkerLease", "content_digest", "not_before_ms"],
        failures,
    )
    store = require(
        "crates/eg-jobs/src/store.rs",
        [
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
        ],
        failures,
    )
    handler = require(
        "src/server/handlers/jobs.rs",
        [
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
        ],
        failures,
    )
    require(
        "src/server/dispatch.rs",
        [
            "execute_consensus_job_publication(",
            "submit_consensus_job_publication_command(",
            '"target-commit"',
            '"scheduler-finalize"',
        ],
        failures,
    )
    require(
        "src/raft/mod.rs",
        ["JobPublicationCommit {", "JobPublicationFinalize {"],
        failures,
    )
    require(
        "crates/eg-types/src/jobs.rs",
        [
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
        ],
        failures,
    )
    require(
        "src/server/auth.rs",
        ["allows_analytics_worker", 'allows_action("analytics:worker")'],
        failures,
    )
    reasoning = require(
        "src/server/reasoning_projection.rs",
        [
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
        ],
        failures,
    )
    require(
        "crates/eg-epistemic/src/incremental.rs",
        [
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
        ],
        failures,
    )
    require(
        "src/server/handlers/query.rs",
        [
            '"reasoning-recompute"',
            'status: "Queued".to_string()',
            "projection_pending: true",
        ],
        failures,
    )
    cargo = require("Cargo.toml", ['"jobs"', '"epistemic-tms"', '"epistemic-causal"'], failures)

    if "JobState::Running { checkpoint } =>" in store[store.find("pub fn succeed"):store.find("pub fn fail")]:
        failures.append("JobStore::succeed still permits Running -> Succeeded")
    if handler.find("stage_result_fenced(") > handler.find("complete_publication_fenced("):
        failures.append("handler completes publication before staging the typed result")
    if reasoning.find("persist_index(") > reasoning.find("ack_mutation_outbox("):
        failures.append("reasoning cursor can advance before projection persistence")
    if "from_slice::<Vec<MutationOperation>>(&lease.record.intent.payload)" in reasoning:
        failures.append("reasoning worker treats digest-only outbox payload as mutation data")
    if "global_index()" in reasoning:
        failures.append("reasoning worker references a process-global TMS authority")
    full_line = next((line for line in cargo.splitlines() if line.startswith("full =")), "")
    for feature in ("jobs", "epistemic-tms", "epistemic-causal"):
        if f'"{feature}"' not in full_line:
            failures.append(f"full build omits {feature}")
    if "Publishing" not in model:
        failures.append("analytics state machine has no publication barrier")

    if failures:
        print("P2 analytics/reasoning architecture gate failed:", file=sys.stderr)
        for failure in failures:
            print(f"- {failure}", file=sys.stderr)
        return 1
    print("P2 analytics/reasoning architecture gate passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
