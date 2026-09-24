//! `DecisionLog.retrieval`, the log-reading half (EH-394, EH-395, EH-398):
//! retrieval outcomes joined to committed retrieval-plan records, and the
//! hard negatives, per-class usage and proven paths read from them.
//!
//! An outcome is stored under its record's key family (`retrieval:<record>`)
//! and is only ever read THROUGH its record: a reader that cannot see the
//! record sees neither the outcome nor anything learned from it.

use eg_numeric::decision::retrieval::{
    class_usage, hard_negatives, proven_paths, verdict, Verdict,
};
use eg_types::contract::BoundedVec;
use eg_types::decision::digest::digest_text;
use eg_types::decision::statistical::log::DecisionLogEntry;
use eg_types::decision::statistical::retrieval::{
    HardNegativeRequest, HardNegativeSet, PathRequest, ProvenPaths, RetrievalOutcome,
    RetrievalUsage, StoredRetrievalOutcome, MAX_HARD_NEGATIVES, MAX_PROVEN_PATHS,
    RETRIEVAL_LEARNING_SCHEMA_VERSION,
};
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
/// Digest domain of a path template.
const PATH_DOMAIN: &str = "eg/retrieval-path/v1";

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

/// Durable hard negatives of the independently judged successful runs.
pub(super) fn read_hard_negatives(
    store: &AgentLibraryStore,
    reader: &LogReader,
    request: &HardNegativeRequest,
) -> Result<HardNegativeSet, String> {
    let scope = Scope {
        question_id: request.question_id.as_deref(),
        window: request.window,
    };
    let outcomes = judged_outcomes(store, reader, scope)?;
    let judged = outcomes
        .iter()
        .filter(|o| o.verdict != Verdict::Unjudged)
        .count();
    let mut rows: Vec<_> = outcomes
        .iter()
        .filter(|o| o.verdict == Verdict::Success)
        .flat_map(|o| hard_negatives(&o.stored.outcome))
        .collect();
    let limit = (request.limit as usize).min(MAX_HARD_NEGATIVES);
    let truncated = rows.len() > limit;
    rows.truncate(limit);
    Ok(HardNegativeSet {
        schema_version: RETRIEVAL_LEARNING_SCHEMA_VERSION,
        judged: judged as u64,
        unjudged: (outcomes.len() - judged) as u64,
        truncated,
        rows: BoundedVec::new(rows).map_err(invalid)?,
    })
}

/// Per content-class usage of every visible outcome in the window.
pub(super) fn read_usage(
    store: &AgentLibraryStore,
    reader: &LogReader,
    window: RecordWindow,
) -> Result<RetrievalUsage, String> {
    let scope = Scope {
        question_id: None,
        window,
    };
    let outcomes = judged_outcomes(store, reader, scope)?;
    let min_support = default_statistical_policy().min_support;
    let rows = class_usage(outcomes.iter().map(|o| &o.stored.outcome), min_support);
    Ok(RetrievalUsage {
        schema_version: RETRIEVAL_LEARNING_SCHEMA_VERSION,
        min_support,
        outcomes: outcomes.len() as u64,
        rows: BoundedVec::new(rows).map_err(invalid)?,
    })
}

/// The content digest of a path template.
pub(super) fn template_digest(
    template: &eg_types::decision::statistical::retrieval::RetrievalPathTemplate,
) -> String {
    digest_text(PATH_DOMAIN, template)
}

/// The proven paths of one task class under one schema identity. A template
/// proven under another `composed_digest` is simply not returned: the schema
/// identity changing is what invalidates a path.
pub(super) fn read_paths(
    store: &AgentLibraryStore,
    reader: &LogReader,
    request: &PathRequest,
) -> Result<ProvenPaths, String> {
    let scope = Scope {
        question_id: None,
        window: request.window,
    };
    let outcomes = judged_outcomes(store, reader, scope)?;
    let judged = outcomes.iter().filter_map(|o| {
        let template = o.stored.outcome.path.as_ref()?;
        let same_policy = request
            .policy_version
            .as_deref()
            .is_none_or(|v| v == template.policy_version);
        let matches = template.task_class == request.task_class
            && template.composed_digest == request.composed_digest
            && same_policy
            && o.verdict != Verdict::Unjudged;
        matches.then(|| (template_digest(template), template, o.verdict))
    });
    Ok(ProvenPaths {
        schema_version: RETRIEVAL_LEARNING_SCHEMA_VERSION,
        rows: BoundedVec::new(proven_paths(judged, MAX_PROVEN_PATHS)).map_err(invalid)?,
    })
}
