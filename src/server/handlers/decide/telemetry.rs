//! OTel spans for the statistical decision surface (EH-074).
//!
//! Each served call runs inside one `tracing` span (`eg.decide`,
//! `eg.decision_fit`, `eg.decision_eval`), exported by the engine's OTLP layer
//! when it is configured, and ends with one structured event carrying the
//! resolution kind, evidence class, outcome, abstention reason, candidate
//! count, exploration/audit flags, effective sample sizes and latency. The
//! dashboards and alert rules over them belong to W6 (LGTM spanmetrics).

use std::time::Instant;

use eg_types::decision::jobs::{DecisionEvalReceipt, DecisionThresholdAssessment};
use eg_types::decision::replay::EvaluationRun;
use eg_types::decision::statistical::{StatisticalDecisionRecord, StatisticalOutcome};

/// The span one served statistical call runs in.
pub(super) fn span(method: &'static str, tenant_id: &str) -> tracing::Span {
    tracing::info_span!("eg.decide.method", method, tenant = tenant_id)
}

fn outcome_label(outcome: &StatisticalOutcome) -> (&'static str, String) {
    match outcome {
        StatisticalOutcome::Acted { .. } => ("acted", String::new()),
        StatisticalOutcome::Explored { .. } => ("explored", String::new()),
        StatisticalOutcome::Advisory { .. } => ("advisory", String::new()),
        StatisticalOutcome::Abstained { reasons } => (
            "abstained",
            reasons
                .iter()
                .map(|reason| {
                    format!("{reason:?}")
                        .split([' ', '{'])
                        .next()
                        .unwrap_or("")
                        .to_string()
                })
                .collect::<Vec<_>>()
                .join(","),
        ),
    }
}

/// One `Decide` answer.
pub(super) fn decided(record: &StatisticalDecisionRecord, candidates: usize, started: Instant) {
    let (outcome, reasons) = outcome_label(&record.outcome);
    let evidence = format!("{:?}", record.evidence_class);
    crate::metrics::decision_answered(outcome, &evidence, &reasons);
    crate::metrics::decision_latency("Decide", started.elapsed().as_secs_f64());
    if record.inputs.exploration.is_some() {
        crate::metrics::decision_explored(matches!(
            record.outcome,
            StatisticalOutcome::Explored { .. }
        ));
    }
    tracing::info!(
        target: "eg.decide",
        resolution_kind = ?record.resolution_kind,
        evidence_class = ?record.evidence_class,
        outcome,
        abstain_reasons = %reasons,
        candidates,
        explored = record.inputs.exploration.is_some(),
        audit_sampled = record.audit.is_some_and(|a| a.sampled),
        synthetic = record.synthetic_evidence,
        latency_ms = started.elapsed().as_millis() as u64,
        "decision served"
    );
}

/// One finished replay evaluation (EH-528).
pub(super) fn replayed(run: &EvaluationRun, started: Instant) {
    crate::metrics::decision_latency("DecisionEval", started.elapsed().as_secs_f64());
    tracing::info!(
        target: "eg.decide",
        run_digest = %run.run_digest,
        folds = run.folds.len(),
        steps = run.path.len(),
        n_trials = run.validation.n_trials,
        deflated_sharpe_q32 = run.validation.deflated_sharpe.value,
        pbo_q32 = run.validation.probability_backtest_overfit.value,
        latency_ms = started.elapsed().as_millis() as u64,
        "decision head replayed"
    );
}

/// One finished evaluation.
pub(super) fn evaluated(receipt: &DecisionEvalReceipt, started: Instant) {
    crate::metrics::decision_evaluated(receipt.passed);
    crate::metrics::decision_latency("DecisionEval", started.elapsed().as_secs_f64());
    let min_ess = receipt
        .estimates
        .iter()
        .map(|e| e.effective_sample_size.value)
        .min()
        .unwrap_or(0);
    tracing::info!(
        target: "eg.decide",
        passed = receipt.passed,
        n_records = receipt.n_records,
        estimators = receipt.estimates.len(),
        min_ess_q32 = min_ess,
        failed_gates = %receipt.failed_gates.as_slice().join(","),
        latency_ms = started.elapsed().as_millis() as u64,
        "decision head evaluated"
    );
}

/// Emit only after the full-label receipt and its pinned-policy assessment
/// have committed. Idempotent job replays must not call this function.
pub(super) fn threshold_assessed(assessment: Option<&DecisionThresholdAssessment>) {
    let Some(assessment) = assessment else {
        return;
    };
    fn result(value: Option<bool>) -> &'static str {
        match value {
            Some(true) => "breach",
            Some(false) => "ok",
            None => "unavailable",
        }
    }

    let support = if assessment.insufficient_support {
        "breach"
    } else {
        "ok"
    };
    crate::metrics::decision_threshold_assessed("support", support);
    crate::metrics::decision_threshold_assessed(
        "coverage",
        result(assessment.coverage_below_policy),
    );
    crate::metrics::decision_threshold_assessed(
        "act_risk",
        result(assessment.act_risk_above_policy),
    );
}

/// One finished fit.
pub(super) fn fitted(n_training: u64, calibrated: bool, started: Instant) {
    crate::metrics::decision_latency("DecisionFit", started.elapsed().as_secs_f64());
    tracing::info!(
        target: "eg.decide",
        n_training,
        calibrated,
        latency_ms = started.elapsed().as_millis() as u64,
        "decision head fitted"
    );
}

/// One refused call.
pub(super) fn refused(method: &'static str, error: &str) {
    let code = refusal_code(error);
    crate::metrics::decision_refused(method, code);
    tracing::info!(target: "eg.decide", method, code, "decision call refused");
}

/// The closed code token of a refusal text, or `OTHER` for free text, so the
/// metric label set stays bounded.
fn refusal_code(error: &str) -> &str {
    let code = error.split(':').next().unwrap_or("");
    let closed = !code.is_empty() && code.bytes().all(|b| b.is_ascii_uppercase() || b == b'_');
    if closed {
        code
    } else {
        "OTHER"
    }
}

/// One served decision-log call.
pub(super) fn logged(ok: bool, started: Instant) {
    crate::metrics::decision_latency("DecisionLog", started.elapsed().as_secs_f64());
    tracing::info!(
        target: "eg.decide",
        ok,
        latency_ms = started.elapsed().as_millis() as u64,
        "decision log call served"
    );
}

/// Bounded diagnostics for the native scorer's stage failures. Never emit the
/// refusal itself: it can contain caller values, paths, or credentials.
pub(super) fn scorer_failure(stage: &'static str, error: String) -> String {
    let reason = scorer_failure_reason(&error);
    tracing::info!(parent: None, stage, reason, "resident scorer stage refused");
    error // Preserve the exact existing error and wire classification.
}

fn scorer_failure_reason(error: &str) -> &'static str {
    match error {
        "Agent Library requires a configured persist directory" => "persist_directory_missing",
        "cached owner manifest differs from persisted authority" => "owner_manifest_changed",
        "mutation read manifest authority changed" => "read_manifest_changed",
        "mutation scope is not bound to this store" => "scope_unbound",
        "mutation scope binding identity mismatch" => "scope_identity_mismatch",
        "mutation store handle does not match persisted root" => "store_root_mismatch",
        "authenticated serving grant does not match this store" => "serving_grant_mismatch",
        "owner read capability does not match this store" => "read_capability_mismatch",
        "scoped read may not open a table outside its layout" => "table_outside_layout",
        "a scope-prefixed owner table is reachable only through its scoped accessor" => {
            "scoped_accessor_required"
        }
        "only the file's control scope may open a file-wide owner table" => {
            "control_scope_required"
        }
        "agent component row is invalid or exceeds resource limits" => "component_row_decode",
        "agent component history exceeds its retained revision bound" => "component_revision_bound",
        "agent component history exceeds its retained byte bound" => "component_byte_bound",
        _ if error.starts_with("Decide task failed: ") => "worker_join_failed",
        _ if error.starts_with("CORRUPT_MUTATION_LEDGER: ") => "mutation_ledger_corrupt",
        _ if error.starts_with("COMPONENT_PIN_MISMATCH: ") => "component_pin_mismatch",
        _ if error.starts_with("HEAD_INVALID: ") => "head_invalid",
        _ if error.starts_with("FEATURE_SCHEMA_INVALID: ") => "feature_schema_invalid",
        _ => "unclassified",
    }
}

#[cfg(test)]
mod scorer_diagnostic_tests {
    use super::{scorer_failure, scorer_failure_reason};

    #[test]
    fn diagnostics_preserve_the_original_error() {
        let original = "private request=/private/path credential=do-not-export".to_string();
        assert_eq!(scorer_failure("execute", original.clone()), original);
    }

    #[test]
    fn diagnostics_are_fixed_categories_not_refusal_text() {
        assert_eq!(
            scorer_failure_reason("mutation read manifest authority changed"),
            "read_manifest_changed"
        );
        assert_eq!(
            scorer_failure_reason("agent component row is invalid or exceeds resource limits"),
            "component_row_decode"
        );
        assert_eq!(
            scorer_failure_reason("Decide task failed: secret=/private/path"),
            "worker_join_failed"
        );
        assert_eq!(
            scorer_failure_reason("CORRUPT_MUTATION_LEDGER: secret=/private/path"),
            "mutation_ledger_corrupt"
        );
        assert_eq!(
            scorer_failure_reason("secret=/private/path\nHEAD_INVALID: sensitive"),
            "unclassified"
        );
        assert_eq!(
            scorer_failure_reason("mutation read manifest authority changed secret"),
            "unclassified"
        );
    }
}
