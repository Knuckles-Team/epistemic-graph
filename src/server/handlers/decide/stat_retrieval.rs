//! Retrieval outcomes on the decision log (EH-394, EH-395): the outcome join
//! (the one write a retrieval run makes) and the judged reading every
//! learned artefact is derived from. The read-only relations that serve what
//! was learned are [`super::stat_retrieval_views`].
//!
//! An outcome is stored under its record (`retrieval:<record>`) and read only
//! THROUGH that record: a reader that cannot see the record sees neither the
//! outcome nor anything learned from it.

use eg_numeric::decision::retrieval::{verdict, Verdict};
use eg_types::decision::statistical::log::DecisionLogEntry;
use eg_types::decision::statistical::retrieval::{RetrievalOutcome, StoredRetrievalOutcome};
use eg_types::decision::statistical::{QuestionKind, StatisticalErrorCode};
use eg_types::decision::RecordWindow;

use super::stat_executor::ExecutionContext;
use super::stat_log::{
    evaluations_of, executed_option, store_once, visible_entry, LogReader, MAX_LOG_ROWS,
};
use super::stat_support::{default_statistical_policy, refusal};
use crate::server::persistence::agent_library::AgentLibraryStore;
use crate::server::persistence::decision_jobs::decode_artifact;

/// Key family of retrieval outcomes.
const OUTCOME_PREFIX: &str = "retrieval:";

fn invalid(detail: impl std::fmt::Display) -> String {
    refusal(StatisticalErrorCode::ParameterInvalid, detail)
}

/// A visible outcome, its record and what the record's evaluations say.
pub(super) struct JudgedOutcome {
    pub(super) entry: DecisionLogEntry,
    pub(super) stored: StoredRetrievalOutcome,
    pub(super) verdict: Verdict,
}

/// Which records a read covers.
#[derive(Clone, Copy)]
pub(super) struct Scope<'a> {
    pub(super) question_id: Option<&'a str>,
    pub(super) window: RecordWindow,
}

impl Scope<'_> {
    /// Every question, all time.
    pub(super) const ALL: Scope<'static> = Scope {
        question_id: None,
        window: RecordWindow {
            from_ms: 0,
            to_ms: u64::MAX,
        },
    };

    fn covers(&self, entry: &DecisionLogEntry) -> bool {
        let record = &entry.record;
        let in_question = self
            .question_id
            .is_none_or(|q| q == record.question.question_id);
        in_question && (self.window.from_ms..=self.window.to_ms).contains(&record.created_at_ms)
    }
}

fn check_joinable(entry: Option<DecisionLogEntry>, reader: &LogReader) -> Result<(), String> {
    let entry = entry.ok_or_else(|| invalid("no committed record with that id is visible"))?;
    if entry.record.question.kind != QuestionKind::RetrievalPlan {
        return Err(invalid("an outcome joins a retrieval-plan record only"));
    }
    if executed_option(&entry.record.outcome).is_none() {
        return Err(invalid("the record executed no option"));
    }
    if entry.committed_by != reader.principal {
        return Err(
            "ACCESS_DENIED: a retrieval outcome is attested by the principal that committed \
             its record"
                .to_string(),
        );
    }
    Ok(())
}

/// Join one outcome to its committed, executed retrieval-plan record.
pub(super) fn record_outcome(
    ctx: &ExecutionContext,
    reader: &LogReader,
    outcome: RetrievalOutcome,
) -> Result<StoredRetrievalOutcome, String> {
    outcome.check().map_err(invalid)?;
    check_joinable(
        visible_entry(ctx.store, reader, &outcome.record_id)?,
        reader,
    )?;
    let key = format!("{OUTCOME_PREFIX}{}", outcome.record_id);
    let stored = StoredRetrievalOutcome {
        outcome,
        producer: reader.principal.clone(),
        recorded_at_ms: ctx.now_ms,
    };
    store_once(ctx, key, "retrieval outcome", stored, |existing, fresh| {
        existing.outcome == fresh.outcome && existing.producer == fresh.producer
    })
}

/// Every visible outcome in `scope`, each with its record's verdict.
pub(super) fn judged_outcomes(
    store: &AgentLibraryStore,
    reader: &LogReader,
    scope: Scope<'_>,
) -> Result<Vec<JudgedOutcome>, String> {
    let floor = default_statistical_policy().min_outcome_fidelity;
    let mut out = Vec::new();
    for (_, bytes) in
        store.decision_artifacts_with_prefix(&reader.tenant_id, OUTCOME_PREFIX, MAX_LOG_ROWS)?
    {
        let stored: StoredRetrievalOutcome = decode_artifact(&bytes, "retrieval outcome")?;
        let Some(entry) = visible_entry(store, reader, &stored.outcome.record_id)? else {
            continue;
        };
        if !scope.covers(&entry) {
            continue;
        }
        let evaluations = evaluations_of(store, &reader.tenant_id, &entry.record.record_id)?;
        let verdict = verdict(&evaluations, &stored.producer, floor);
        out.push(JudgedOutcome {
            entry,
            stored,
            verdict,
        });
    }
    Ok(out)
}
