//! `DecisionLog.resolve`: an escalated abstention re-enters as evidence of
//! its own class (EH-037, DECIDE-LAYER-DESIGN §6.4 "abstentions as labels").
//!
//! The decision abstained; AU escalated it to a model or a human; the answer
//! comes back here. It is stored beside the logged record, never over it: the
//! record stays an abstention, and the resolution is a separate row whose
//! class is fixed by its resolver -- a human answer is an observation (a
//! gold-label candidate), a model's answer is a claim. The resolved option
//! must be one the record already held, so an escalation can never add an
//! option the decision's legal set did not have.

use eg_types::decision::statistical::log::{
    AbstentionResolution, AbstentionResolver, StoredResolution,
};
use eg_types::decision::statistical::{
    FeatureMatrixRef, StatisticalDecisionRecord, StatisticalErrorCode, StatisticalOutcome,
};
use eg_types::decision::EvidenceClass;

use super::stat_executor::ExecutionContext;
use super::stat_log::{store_once, visible_entry, LogReader};
use super::stat_support::refusal;

/// Key of one resolution of a logged abstention.
fn resolution_key(record_id: &str, resolution_id: &str) -> String {
    format!("resolution:{record_id}:{resolution_id}")
}

fn class_of(resolver: &AbstentionResolver) -> EvidenceClass {
    match resolver {
        AbstentionResolver::Human => EvidenceClass::Observation,
        AbstentionResolver::Model { .. } => EvidenceClass::Claim,
    }
}

fn held_option(record: &StatisticalDecisionRecord, option_id: &str) -> bool {
    match &record.inputs.feature_matrix {
        FeatureMatrixRef::Inline { candidate_ids, .. } => {
            candidate_ids.iter().any(|id| id == option_id)
        }
        FeatureMatrixRef::Blob { .. } => false,
    }
}

fn invalid(detail: &str) -> String {
    refusal(StatisticalErrorCode::ParameterInvalid, detail)
}

/// Store one resolution; idempotent on identical content.
pub(super) fn resolve(
    ctx: &ExecutionContext,
    reader: &LogReader,
    resolution: AbstentionResolution,
) -> Result<StoredResolution, String> {
    let entry = visible_entry(ctx.store, reader, &resolution.record_id)?
        .ok_or_else(|| invalid("no committed record with that id is visible"))?;
    if !matches!(entry.record.outcome, StatisticalOutcome::Abstained { .. }) {
        return Err(invalid("only a logged abstention is resolved"));
    }
    if !held_option(&entry.record, &resolution.option_id) {
        return Err(invalid(
            "the resolved option is not one the abstained decision held",
        ));
    }
    let key = resolution_key(&resolution.record_id, &resolution.resolution_id);
    let stored = StoredResolution {
        class: class_of(&resolution.resolver),
        resolution,
        producer: reader.principal.clone(),
        recorded_at_ms: ctx.now_ms,
    };
    store_once(ctx, key, "resolution", stored, |existing, fresh| {
        existing.resolution == fresh.resolution && existing.producer == fresh.producer
    })
}
