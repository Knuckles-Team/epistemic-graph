//! Verify-replay of a statistical record before it is logged (§4.4, EH-060).
//!
//! A record arriving on the wire is never trusted. Its digest is re-checked,
//! its pinned feature schema, head and policy are re-read by pin, and the
//! decision function re-runs on the STORED feature matrix, parameters and
//! clock -- never on live features, which move with every write. The outcome,
//! calibration statement, audit draw, explanation, logging distribution and
//! belief slices (EH-297, from their stored matrices) must come out
//! identical, and the recomputed keyed seed must match the record's
//! commitment; only then is the seed revealed in the logged copy.

use eg_numeric::decision::features::FeatureMatrix;
use eg_types::decision::digest::statistical_record_digest;
use eg_types::decision::statistical::keyed::{seed_commitment, seed_text};
use eg_types::decision::statistical::{
    FeatureMatrixRef, StatisticalDecisionRecord, StatisticalErrorCode, StatisticalOutcome,
};
use eg_types::decision::DecisionErrorCode;

use super::stat_belief::points;
use super::stat_classes::current_rules;
use super::stat_executor::{
    pinned_inputs, recorded_explanation, run_on_matrix, Executed, ExecutionContext, MatrixInputs,
};
use super::stat_support::{materialize_matrix, refusal, resolve_policy};

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
    Ok(materialize_matrix(
        candidate_ids.as_slice(),
        feature_names.as_slice(),
        values.as_slice(),
    ))
}

/// An executed decision is logged so an outcome can evaluate it; an
/// abstention is logged so its escalation can be resolved against it
/// (EH-037). An advisory score executed nothing and resolves nothing.
fn loggable(outcome: &StatisticalOutcome) -> bool {
    matches!(
        outcome,
        StatisticalOutcome::Acted { .. }
            | StatisticalOutcome::Explored { .. }
            | StatisticalOutcome::Abstained { .. }
    )
}

/// `derived` says the explanation and the belief slices re-derived equal.
fn same_answer(record: &StatisticalDecisionRecord, replayed: &Executed, derived: bool) -> bool {
    record.outcome == replayed.ladder.outcome
        && record.calibration == replayed.ladder.calibration
        && record.audit == replayed.ladder.audit
        && record.logging_propensities.as_slice() == replayed.ladder.logging.as_slice()
        && derived
}

/// Re-derive `record` and return the copy to log, with its seed revealed.
pub(super) fn replay(
    ctx: &ExecutionContext,
    record: &StatisticalDecisionRecord,
) -> Result<StatisticalDecisionRecord, String> {
    if statistical_record_digest(record) != record.record_digest {
        return Err(mismatch("the record digest does not match its content"));
    }
    if !loggable(&record.outcome) {
        return Err(refusal(
            StatisticalErrorCode::ParameterInvalid,
            "only an acted, explored or abstained decision is logged",
        ));
    }
    let inputs = &record.inputs;
    if inputs.classification_rules.is_some() && inputs.classification_rules != current_rules() {
        return Err(mismatch(
            "the record's classes were derived under another rule set",
        ));
    }
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
    let matrix = stored_matrix(record)?;
    let options = matrix.candidate_ids.len();
    let replayed = run_on_matrix(&matrix_inputs, &pinned, &policy, matrix)?;
    let explanation = recorded_explanation(&pinned, &replayed)? == record.explanation;
    let belief =
        points(&pinned, inputs.belief_slices.as_slice(), options)? == record.belief.as_slice();
    if !same_answer(record, &replayed, explanation && belief) {
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
