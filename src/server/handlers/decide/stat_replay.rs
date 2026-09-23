//! Verify-replay of a statistical record before it is logged (§4.4, EH-060).
//!
//! A record arriving on the wire is never trusted. Its digest is re-checked,
//! its pinned feature schema, head and policy are re-read by pin, and the
//! decision function re-runs on the STORED feature matrix, parameters and
//! clock -- never on live features, which move with every write. The outcome,
//! calibration statement, audit draw, explanation and logging distribution
//! must come out identical, and the recomputed keyed seed must match the
//! record's commitment; only then is the seed revealed in the logged copy.

use eg_numeric::decision::features::FeatureMatrix;
use eg_types::decision::digest::statistical_record_digest;
use eg_types::decision::statistical::keyed::{seed_commitment, seed_text};
use eg_types::decision::statistical::{
    FeatureMatrixRef, StatisticalDecisionRecord, StatisticalErrorCode, StatisticalOutcome,
};
use eg_types::decision::DecisionErrorCode;

use super::stat_executor::{
    pinned_inputs, recorded_explanation, run_on_matrix, Executed, ExecutionContext, MatrixInputs,
};
use super::stat_support::{refusal, resolve_policy};

fn mismatch(detail: &str) -> String {
    format!(
        "{}: {detail}",
        DecisionErrorCode::DecisionReplayMismatch.as_str()
    )
}

fn stored_matrix(record: &StatisticalDecisionRecord) -> Result<FeatureMatrix, String> {
    let FeatureMatrixRef::Inline {
        candidate_ids,
        feature_names,
        values,
        ..
    } = &record.inputs.feature_matrix
    else {
        return Err(refusal(
            StatisticalErrorCode::ParameterInvalid,
            "only an inline feature matrix can be replayed",
        ));
    };
    if values.is_empty() {
        return Err(mismatch("the record stores no feature values to replay"));
    }
    Ok(FeatureMatrix {
        candidate_ids: candidate_ids.iter().cloned().collect(),
        feature_names: feature_names.iter().cloned().collect(),
        values: values.iter().copied().collect(),
    })
}

/// Only an executed decision is logged: an advisory score or an abstention
/// has no executed option an outcome could evaluate.
fn executed(outcome: &StatisticalOutcome) -> bool {
    matches!(
        outcome,
        StatisticalOutcome::Acted { .. } | StatisticalOutcome::Explored { .. }
    )
}

fn same_answer(record: &StatisticalDecisionRecord, replayed: &Executed, explanation: bool) -> bool {
    record.outcome == replayed.ladder.outcome
        && record.calibration == replayed.ladder.calibration
        && record.audit == replayed.ladder.audit
        && record.logging_propensities.as_slice() == replayed.ladder.logging.as_slice()
        && explanation
}

/// Re-derive `record` and return the copy to log, with its seed revealed.
pub(super) fn replay(
    ctx: &ExecutionContext,
    record: &StatisticalDecisionRecord,
) -> Result<StatisticalDecisionRecord, String> {
    if statistical_record_digest(record) != record.record_digest {
        return Err(mismatch("the record digest does not match its content"));
    }
    if !executed(&record.outcome) {
        return Err(refusal(
            StatisticalErrorCode::ParameterInvalid,
            "only an acted or explored decision is logged",
        ));
    }
    let inputs = &record.inputs;
    let pinned = pinned_inputs(ctx, &inputs.feature_schema, inputs.head.as_ref())?;
    let policy = resolve_policy(ctx.store, ctx.tenant_id, &inputs.policy)?;
    if policy.digest != inputs.policy_digest {
        return Err(mismatch(
            "the pinned policy no longer has the recorded digest",
        ));
    }
    let matrix_inputs = MatrixInputs {
        question: &record.question,
        params: inputs.params.as_slice(),
        now_ms: inputs.shortlist.now_ms,
        server_secret: ctx.server_secret,
    };
    let replayed = run_on_matrix(&matrix_inputs, &pinned, &policy, stored_matrix(record)?)?;
    let explanation = recorded_explanation(&pinned, &replayed)? == record.explanation;
    if !same_answer(record, &replayed, explanation) {
        return Err(mismatch(
            "re-running the decision on its stored inputs gives another answer",
        ));
    }
    let mut logged = record.clone();
    if let Some(exploration) = logged.inputs.exploration.as_mut() {
        if exploration.seed_commitment != seed_commitment(&replayed.seed) {
            return Err(mismatch(
                "the exploration seed does not match its commitment",
            ));
        }
        exploration.revealed_seed = Some(seed_text(&replayed.seed));
    }
    logged.record_digest = statistical_record_digest(&logged);
    Ok(logged)
}
