//! OTel spans for the statistical decision surface (EH-074).
//!
//! Each served call runs inside one `tracing` span (`eg.decide`,
//! `eg.decision_fit`, `eg.decision_eval`), exported by the engine's OTLP layer
//! when it is configured, and ends with one structured event carrying the
//! resolution kind, evidence class, outcome, abstention reason, candidate
//! count, exploration/audit flags, effective sample sizes and latency. The
//! dashboards and alert rules over them belong to W6 (LGTM spanmetrics).

use std::time::Instant;

use eg_types::decision::jobs::DecisionEvalReceipt;
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

/// One finished evaluation.
pub(super) fn evaluated(receipt: &DecisionEvalReceipt, started: Instant) {
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

/// One finished fit.
pub(super) fn fitted(n_training: u64, calibrated: bool, started: Instant) {
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
    let code = error.split(':').next().unwrap_or("");
    tracing::info!(target: "eg.decide", method, code, "decision call refused");
}
