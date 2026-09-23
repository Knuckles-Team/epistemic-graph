//! EH-012: assembly-level slate crediting (DECIDE-LAYER-DESIGN §6.4).
//!
//! An assembly is combinatorial, so an outcome is credited to the WHOLE
//! assembled graph -- the slate -- never split across its components (a
//! per-component utility would need a declared additive-reward assumption and
//! would be advisory at most). A committed v1 assembly record lives in the
//! Agent Library's `decision_records` table, not in the statistical log; an
//! independent evaluation of it joins by record id exactly like one of a
//! statistical record, and the outcome aggregate reports it under the
//! `assembly` question as option `slate:<graph digest>`.
//!
//! Visibility: a v1 record is library-sourced, so it is tenant-wide (§4.3),
//! which is the visibility of the inputs it was decided on.

use std::collections::BTreeMap;

use eg_types::decision::statistical::log::StoredEvaluation;
use eg_types::decision::{DecisionOutcome, DecisionRecord};

use crate::server::persistence::agent_library::AgentLibraryStore;
use crate::server::persistence::decision_jobs::{decode_artifact, record_key};

/// The question id slate rows are aggregated under.
pub(super) const SLATE_QUESTION: &str = "assembly";

/// One committed, solved assembly: the slate an outcome is credited to.
pub(super) struct Slate {
    pub(super) option_id: String,
    pub(super) policy_digest: String,
    pub(super) decider: String,
    pub(super) created_at_ms: u64,
}

/// The slate of committed v1 record `record_id`, if it is a solved assembly.
pub(super) fn slate_of(
    store: &AgentLibraryStore,
    tenant_id: &str,
    record_id: &str,
) -> Result<Option<Slate>, String> {
    let Some(body) = store.decision_record_body(tenant_id, record_id)? else {
        return Ok(None);
    };
    let record: DecisionRecord = serde_json::from_slice(&body)
        .map_err(|error| format!("COMPONENT_BODY_UNAVAILABLE: {error}"))?;
    let DecisionOutcome::Solved { graph_digest, .. } = &record.outcome else {
        return Ok(None);
    };
    Ok(Some(Slate {
        option_id: format!("slate:{graph_digest}"),
        policy_digest: record.inputs.policy_digest.clone(),
        decider: record.caller_principal.clone(),
        created_at_ms: record.created_at_ms,
    }))
}

/// Evaluations grouped by the record they evaluate, for records that are NOT
/// in the statistical log (those are joined by the log itself).
fn unlogged_evaluations(
    store: &AgentLibraryStore,
    tenant_id: &str,
    max_rows: usize,
) -> Result<BTreeMap<String, Vec<StoredEvaluation>>, String> {
    let mut by_record: BTreeMap<String, Vec<StoredEvaluation>> = BTreeMap::new();
    for (_, bytes) in store.decision_artifacts_with_prefix(tenant_id, "evaluation:", max_rows)? {
        let stored: StoredEvaluation = decode_artifact(&bytes, "decision outcome evaluation")?;
        by_record
            .entry(stored.evaluation.record_id.clone())
            .or_default()
            .push(stored);
    }
    let mut out = BTreeMap::new();
    for (record_id, evaluations) in by_record {
        if store
            .decision_artifact(tenant_id, &record_key(&record_id))?
            .is_none()
        {
            out.insert(record_id, evaluations);
        }
    }
    Ok(out)
}

/// Every evaluated slate inside `window`, when the aggregate covers the
/// `assembly` question (or every question).
pub(super) fn evaluated_slates(
    store: &AgentLibraryStore,
    tenant_id: &str,
    question_id: Option<&str>,
    window: (u64, u64),
    max_rows: usize,
) -> Result<Vec<(Slate, Vec<StoredEvaluation>)>, String> {
    if question_id.is_some_and(|q| q != SLATE_QUESTION) {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    for (record_id, evaluations) in unlogged_evaluations(store, tenant_id, max_rows)? {
        let Some(slate) = slate_of(store, tenant_id, &record_id)? else {
            continue;
        };
        if (window.0..=window.1).contains(&slate.created_at_ms) {
            out.push((slate, evaluations));
        }
    }
    Ok(out)
}
