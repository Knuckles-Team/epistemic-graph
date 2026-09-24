//! The retrieval-learning relations of the decision views (EH-394..EH-398),
//! served through the one authorized SQL projection like every decision view
//! (EH-066), never through an op:
//!
//! * `decision_retrieval_outcomes` -- one row per unit a visible run returned
//!   (rank, content class, whether the answer cited it, the run's verdict);
//! * `decision_hard_negatives` -- returned, uncited units that outranked a
//!   cited one in an independently judged successful run;
//! * `decision_class_usage` -- per content class, returned and cited counts,
//!   k-anonymised by the policy's `min_support`;
//! * `decision_proven_paths` -- judged plan templates by digest with their
//!   successes and failures (a template names schema terms, never rows);
//! * `decision_pointers` -- every governed pointer move (adapter, generation)
//!   of the caller's tenant.

use eg_numeric::decision::retrieval::{class_usage, hard_negatives, proven_paths, Verdict};
use eg_query::ColumnType;
use eg_types::decision::digest::digest_text;
use eg_types::decision::statistical::retrieval::{
    ClassUsage, HardNegative, ProvenPath, RetrievalPathTemplate, ReturnedEvidence,
};
use eg_types::decision::statistical::retrieval_pointer::PointerEvent;
use serde_json::Value;

use super::stat_log::LogReader;
use super::stat_pointer::pointer_events;
use super::stat_retrieval::{judged_outcomes, JudgedOutcome, Scope};
use super::stat_support::default_statistical_policy;
use super::stat_view::{millis, relation, text, wire_name, Col};
use crate::server::persistence::agent_library::AgentLibraryStore;
use crate::server::sql_catalog_acl::relations::Relation;

/// Digest domain of a path template.
const PATH_DOMAIN: &str = "eg/retrieval-path/v1";

/// The content digest of a path template.
fn template_digest(template: &RetrievalPathTemplate) -> String {
    digest_text(PATH_DOMAIN, template)
}

fn verdict_name(verdict: Verdict) -> &'static str {
    match verdict {
        Verdict::Success => "success",
        Verdict::Failure => "failure",
        Verdict::Unjudged => "unjudged",
    }
}

/// One returned unit of one visible run.
struct UnitRow {
    record_id: String,
    question_id: String,
    unit: ReturnedEvidence,
    rank: u64,
    cited: bool,
    verdict: Verdict,
    producer: String,
    recorded_at_ms: u64,
}

const OUTCOME_COLUMNS: &[Col<UnitRow>] = &[
    ("record_id", ColumnType::Text, |r| {
        Value::from(r.record_id.clone())
    }),
    ("question_id", ColumnType::Text, |r| {
        Value::from(r.question_id.clone())
    }),
    ("evidence_id", ColumnType::Text, |r| {
        Value::from(r.unit.evidence_id.clone())
    }),
    ("rank", ColumnType::BigInt, |r| millis(r.rank)),
    ("content_class", ColumnType::Text, |r| {
        text(r.unit.content_class.clone())
    }),
    ("cited", ColumnType::Bool, |r| Value::Bool(r.cited)),
    ("verdict", ColumnType::Text, |r| {
        Value::from(verdict_name(r.verdict))
    }),
    ("producer", ColumnType::Text, |r| {
        Value::from(r.producer.clone())
    }),
    ("recorded_at_ms", ColumnType::BigInt, |r| {
        millis(r.recorded_at_ms)
    }),
];

const NEGATIVE_COLUMNS: &[Col<HardNegative>] = &[
    ("record_id", ColumnType::Text, |n| {
        Value::from(n.record_id.clone())
    }),
    ("evidence_id", ColumnType::Text, |n| {
        Value::from(n.evidence_id.clone())
    }),
    ("rank", ColumnType::BigInt, |n| Value::from(n.rank)),
    ("outranked_cited", ColumnType::BigInt, |n| {
        Value::from(n.outranked_cited)
    }),
];

const USAGE_COLUMNS: &[Col<ClassUsage>] = &[
    ("content_class", ColumnType::Text, |u| {
        Value::from(u.content_class.clone())
    }),
    ("returned", ColumnType::BigInt, |u| millis(u.returned)),
    ("cited", ColumnType::BigInt, |u| millis(u.cited)),
];

const PATH_COLUMNS: &[Col<ProvenPath>] = &[
    ("template_digest", ColumnType::Text, |p| {
        Value::from(p.template_digest.clone())
    }),
    ("task_class", ColumnType::Text, |p| {
        Value::from(p.template.task_class.clone())
    }),
    ("composed_digest", ColumnType::Text, |p| {
        Value::from(p.template.composed_digest.clone())
    }),
    ("policy_version", ColumnType::Text, |p| {
        Value::from(p.template.policy_version.clone())
    }),
    ("anchor_class", ColumnType::Text, |p| {
        Value::from(p.template.anchor_class.clone())
    }),
    ("rank", ColumnType::Text, |p| {
        text(wire_name(&p.template.rank, ""))
    }),
    ("skill_ref", ColumnType::Text, |p| {
        text(p.template.skill_ref.clone())
    }),
    ("template_json", ColumnType::Text, |p| {
        text(serde_json::to_string(&p.template).ok())
    }),
    ("successes", ColumnType::BigInt, |p| millis(p.successes)),
    ("failures", ColumnType::BigInt, |p| millis(p.failures)),
];

const POINTER_COLUMNS: &[Col<(String, usize, bool, PointerEvent)>] = &[
    ("pointer_key", ColumnType::Text, |(k, _, _, _)| {
        Value::from(k.clone())
    }),
    ("seq", ColumnType::BigInt, |(_, i, _, _)| millis(*i as u64)),
    ("active", ColumnType::Bool, |(_, _, a, _)| Value::Bool(*a)),
    ("transition", ColumnType::Text, |(_, _, _, e)| {
        text(wire_name(&e.transition, ""))
    }),
    ("target", ColumnType::Text, |(_, _, _, e)| {
        text(e.target.clone())
    }),
    ("receipt_digest", ColumnType::Text, |(_, _, _, e)| {
        text(e.receipt_digest.clone())
    }),
    ("principal", ColumnType::Text, |(_, _, _, e)| {
        Value::from(e.principal.clone())
    }),
    ("at_ms", ColumnType::BigInt, |(_, _, _, e)| millis(e.at_ms)),
];

fn judged_paths(runs: &[JudgedOutcome]) -> Vec<ProvenPath> {
    let judged = runs.iter().filter_map(|run| {
        let template = run.stored.outcome.path.as_ref()?;
        (run.verdict != Verdict::Unjudged)
            .then(|| (template_digest(template), template, run.verdict))
    });
    proven_paths(judged, usize::MAX)
}

fn run_rows(run: &JudgedOutcome) -> impl Iterator<Item = UnitRow> + '_ {
    let outcome = &run.stored.outcome;
    outcome
        .returned
        .iter()
        .enumerate()
        .map(move |(index, unit)| UnitRow {
            record_id: outcome.record_id.clone(),
            question_id: run.entry.record.question.question_id.clone(),
            unit: unit.clone(),
            rank: index as u64 + 1,
            cited: outcome.cited.iter().any(|c| *c == unit.evidence_id),
            verdict: run.verdict,
            producer: run.stored.producer.clone(),
            recorded_at_ms: run.stored.recorded_at_ms,
        })
}

fn unit_rows(runs: &[JudgedOutcome]) -> Vec<UnitRow> {
    runs.iter().flat_map(run_rows).collect()
}

/// The retrieval-learning relations of one reader: its visible outcomes and
/// what was learned from them, and the pointers of its served tenant.
pub(super) fn learning_relations(
    store: &AgentLibraryStore,
    reader: &LogReader,
    served_tenant: &str,
) -> Result<Vec<Relation>, String> {
    let runs = judged_outcomes(store, reader, Scope::ALL)?;
    let negatives: Vec<HardNegative> = runs
        .iter()
        .filter(|run| run.verdict == Verdict::Success)
        .flat_map(|run| hard_negatives(&run.stored.outcome))
        .collect();
    let min_support = default_statistical_policy().min_support;
    let usage = class_usage(runs.iter().map(|run| &run.stored.outcome), min_support);
    Ok(vec![
        relation(
            "decision_retrieval_outcomes",
            OUTCOME_COLUMNS,
            &unit_rows(&runs),
        ),
        relation("decision_hard_negatives", NEGATIVE_COLUMNS, &negatives),
        relation("decision_class_usage", USAGE_COLUMNS, &usage),
        relation("decision_proven_paths", PATH_COLUMNS, &judged_paths(&runs)),
        relation(
            "decision_pointers",
            POINTER_COLUMNS,
            &pointer_events(store, served_tenant)?,
        ),
    ])
}
