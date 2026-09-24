//! EH-525 (ANALYTICS-HARVEST AH-05): the learned reputation view — a read-only
//! `reputation` relation over the caller's VISIBLE decision log, and the learned
//! reliability the UQL `SOURCE RELIABILITY` stage reads from it.
//!
//! Rows, one per subject:
//! * `option` — an executed option (a candidate, a source, an assembly slate), and
//!   `agent` — the agent an executed option ran as: the success probability learned from
//!   the independent evaluations of its runs, pooled empirical-Bayes across its kind's
//!   population and discounted by age (half-life [`HALF_LIFE_MS`]);
//! * `forecaster` — a fitted decision head: its proper scores (Brier with the Murphy
//!   decomposition, log loss, ECE) over the executions it forecast, each forecast
//!   re-read from the record's own inputs.
//!
//! What counts is the outcome aggregate's ONE label rule
//! ([`eg_numeric::decision::aggregate::label_use`]): an observation (not a claim), at
//! or above the fidelity floor, by an evaluator that is neither the selected agent, its
//! lease holder nor the decider — so a self-report never moves a reputation.
//! Visibility is the log's (EH-066, EH-395 ruling): only records the caller may
//! `DecisionLog.get` contribute, so a named evaluator's grant — evaluation only, never
//! read — widens nothing here, and an outcome the caller cannot see never appears in a
//! count. Below the policy's `min_support` a row reports no counts and no estimate
//! (k-anonymity, as the aggregate does): its status is `insufficient_history`.

use std::collections::BTreeMap;

use eg_numeric::calibration::ProperScores;
use eg_numeric::decision::aggregate::{label_use, LabelUse};
use eg_numeric::decision::features::FeatureMatrix;
use eg_numeric::decision::track_record::{
    executed_forecast, track_records, ResolvedForecast, TrackRecord,
};
use eg_numeric::detkernel::Level;
use eg_numeric::risk::{
    anchored_reliability, learn_reputation, ConcentrationBounds, Estimate, Observation, Reputation,
    ReputationRules, SampleGate, SubjectReputation,
};
use eg_plan::exec::LearnedReliability;
use eg_query::ColumnType;
use eg_types::agent_component::AgentComponentKind;
use eg_types::decision::policy::TraceFidelityLevel;
use eg_types::decision::statistical::head::DecisionHeadBody;
use eg_types::decision::statistical::log::{DecisionLogEntry, StoredEvaluation};
use eg_types::decision::statistical::{FeatureMatrixRef, StatisticalErrorCode};
use eg_types::decision::QuantScaleTag;
use serde_json::Value;

use super::stat_log::{executed_option, joined, LogReader, MAX_LOG_ROWS};
use super::stat_slate::evaluated_slates;
use super::stat_support::{default_statistical_policy, pinned_body};
use super::stat_view::{relation, Col};
use crate::server::persistence::agent_library::AgentLibraryStore;
use crate::server::sql_catalog_acl::relations::Relation;

/// Outcomes lose half their weight every 30 days.
pub(super) const HALF_LIFE_MS: u64 = 30 * 24 * 60 * 60 * 1000;

/// One independent outcome, owned.
struct Outcome {
    subject: String,
    at_ms: u64,
    success: bool,
}

/// The labelled evidence of the visible log.
#[derive(Default)]
struct Evidence {
    options: Vec<Outcome>,
    agents: Vec<Outcome>,
    forecasts: Vec<ResolvedForecast>,
}

/// One `reputation` row.
struct ReputationRow {
    kind: &'static str,
    subject: String,
    /// `None` below the gate (k-anonymised).
    counts: Option<(u64, u64)>,
    estimate: Option<Estimate>,
    scores: Option<ProperScores>,
    n_min: u64,
}

fn number(value: Option<f64>) -> Value {
    value.map_or(Value::Null, Value::from)
}

fn count(value: Option<u64>) -> Value {
    value.map_or(Value::Null, |n| {
        Value::from(i64::try_from(n).unwrap_or(i64::MAX))
    })
}

const COLUMNS: &[Col<ReputationRow>] = &[
    ("subject_kind", ColumnType::Text, |r| Value::from(r.kind)),
    ("subject", ColumnType::Text, |r| {
        Value::from(r.subject.clone())
    }),
    ("status", ColumnType::Text, |r| {
        let known = r.estimate.is_some() || r.scores.is_some();
        Value::from(if known {
            "estimated"
        } else {
            "insufficient_history"
        })
    }),
    ("trials", ColumnType::BigInt, |r| {
        count(r.counts.map(|c| c.0))
    }),
    ("successes", ColumnType::BigInt, |r| {
        count(r.counts.map(|c| c.1))
    }),
    ("mean", ColumnType::Double, |r| {
        number(r.estimate.map(|e| e.mean))
    }),
    ("lower", ColumnType::Double, |r| {
        number(r.estimate.map(|e| e.lower))
    }),
    ("upper", ColumnType::Double, |r| {
        number(r.estimate.map(|e| e.upper))
    }),
    ("prior_mean", ColumnType::Double, |r| {
        number(r.estimate.map(|e| e.prior_mean))
    }),
    ("prior_strength", ColumnType::Double, |r| {
        number(r.estimate.map(|e| e.prior_strength))
    }),
    ("brier", ColumnType::Double, |r| {
        number(r.scores.map(|s| s.brier))
    }),
    ("log_loss", ColumnType::Double, |r| {
        number(r.scores.map(|s| s.log_loss))
    }),
    ("reliability", ColumnType::Double, |r| {
        number(r.scores.map(|s| s.reliability))
    }),
    ("resolution", ColumnType::Double, |r| {
        number(r.scores.map(|s| s.resolution))
    }),
    ("uncertainty", ColumnType::Double, |r| {
        number(r.scores.map(|s| s.uncertainty))
    }),
    ("ece", ColumnType::Double, |r| {
        number(r.scores.map(|s| s.expected_calibration_error))
    }),
    ("n_min", ColumnType::BigInt, |r| count(Some(r.n_min))),
];

/// The first independent label of `evaluations` (a record decided by `decider`).
fn first_label(
    evaluations: &[StoredEvaluation],
    decider: &str,
    floor: TraceFidelityLevel,
) -> Option<bool> {
    evaluations
        .iter()
        .find_map(|stored| match label_use(stored, decider, floor) {
            LabelUse::Label(success) => Some(success),
            LabelUse::Censored | LabelUse::Refused => None,
        })
}

/// Fitted heads by pin, read once; `None` for a head that no longer resolves.
type Heads = BTreeMap<(String, String), Option<(DecisionHeadBody, String)>>;

fn head_of<'h>(
    store: &AgentLibraryStore,
    reader: &LogReader,
    heads: &'h mut Heads,
    entry: &DecisionLogEntry,
) -> Option<&'h (DecisionHeadBody, String)> {
    let pin = entry.record.inputs.head.as_ref()?;
    let key = (pin.component_id.clone(), pin.definition_digest.clone());
    heads
        .entry(key)
        .or_insert_with(|| {
            pinned_body::<DecisionHeadBody>(
                store,
                &reader.tenant_id,
                pin,
                AgentComponentKind::DecisionHead,
                StatisticalErrorCode::HeadInvalid,
            )
            .ok()
        })
        .as_ref()
}

/// The inline feature matrix a record was decided on (compacted records have none).
fn matrix_of(entry: &DecisionLogEntry) -> Option<FeatureMatrix> {
    let FeatureMatrixRef::Inline {
        candidate_ids,
        feature_names,
        scale: QuantScaleTag::Q32,
        values,
    } = &entry.record.inputs.feature_matrix
    else {
        return None;
    };
    Some(FeatureMatrix {
        candidate_ids: candidate_ids.iter().cloned().collect(),
        feature_names: feature_names.iter().cloned().collect(),
        values: values.iter().copied().collect(),
    })
}

/// The record's forecast of its own executed option, when its head gives one.
fn forecast_of(
    store: &AgentLibraryStore,
    reader: &LogReader,
    heads: &mut Heads,
    entry: &DecisionLogEntry,
    success: bool,
) -> Option<ResolvedForecast> {
    let executed = executed_option(&entry.record.outcome)?;
    let matrix = matrix_of(entry)?;
    let (head, digest) = head_of(store, reader, heads, entry)?;
    let probability = executed_forecast(head, &matrix, executed).ok()??;
    Some(ResolvedForecast {
        forecaster: digest.clone(),
        probability,
        success,
    })
}

fn gather(store: &AgentLibraryStore, reader: &LogReader) -> Result<Evidence, String> {
    let floor = default_statistical_policy().min_outcome_fidelity;
    let mut evidence = Evidence::default();
    let mut heads = Heads::new();
    for (entry, evaluations) in joined(store, reader, None, None)? {
        let decider = entry.record.caller_principal.as_str();
        let Some(option) = executed_option(&entry.record.outcome) else {
            continue;
        };
        for stored in &evaluations {
            if let LabelUse::Label(success) = label_use(stored, decider, floor) {
                let at_ms = stored.recorded_at_ms;
                evidence.options.push(Outcome {
                    subject: option.to_string(),
                    at_ms,
                    success,
                });
                let agent = stored.evaluation.selected_agent.clone();
                evidence.agents.push(Outcome {
                    subject: agent,
                    at_ms,
                    success,
                });
            }
        }
        if let Some(success) = first_label(&evaluations, decider, floor) {
            evidence
                .forecasts
                .extend(forecast_of(store, reader, &mut heads, &entry, success));
        }
    }
    gather_slates(store, reader, floor, &mut evidence)?;
    Ok(evidence)
}

/// Assembly slates (EH-012) are options too: `slate:<graph digest>`.
fn gather_slates(
    store: &AgentLibraryStore,
    reader: &LogReader,
    floor: TraceFidelityLevel,
    evidence: &mut Evidence,
) -> Result<(), String> {
    for (slate, evaluations) in
        evaluated_slates(store, &reader.tenant_id, None, (0, u64::MAX), MAX_LOG_ROWS)?
    {
        for stored in &evaluations {
            if let LabelUse::Label(success) = label_use(stored, &slate.decider, floor) {
                let at_ms = stored.recorded_at_ms;
                evidence.options.push(Outcome {
                    subject: slate.option_id.clone(),
                    at_ms,
                    success,
                });
            }
        }
    }
    Ok(())
}

fn rules(now_ms: u64) -> Result<ReputationRules, String> {
    let policy = default_statistical_policy();
    let render = |e: eg_numeric::detkernel::StatError| e.to_string();
    Ok(ReputationRules {
        half_life_ms: Some(HALF_LIFE_MS),
        now_ms,
        gate: SampleGate::new(policy.min_support.max(1)).map_err(render)?,
        delta: Level::new(1, 20).map_err(render)?,
        bounds: ConcentrationBounds::new(1.0, 1_000.0).map_err(render)?,
    })
}

fn learned(
    outcomes: &[Outcome],
    rules: &ReputationRules,
) -> Result<Vec<SubjectReputation>, String> {
    let observations: Vec<Observation> = outcomes
        .iter()
        .map(|o| Observation {
            subject: &o.subject,
            at_ms: o.at_ms,
            success: o.success,
        })
        .collect();
    learn_reputation(&observations, rules).map_err(|e| e.to_string())
}

fn subject_row(kind: &'static str, row: SubjectReputation) -> ReputationRow {
    let (counts, estimate, n_min) = match row.reputation {
        Reputation::Estimated(estimate) => (Some((row.trials, row.successes)), Some(estimate), 0),
        Reputation::InsufficientHistory { n_min } => (None, None, n_min),
    };
    ReputationRow {
        kind,
        subject: row.subject,
        counts,
        estimate,
        scores: None,
        n_min,
    }
}

fn forecaster_row(record: TrackRecord, n_min: u64) -> ReputationRow {
    let gated = record.scores.is_none();
    ReputationRow {
        kind: "forecaster",
        subject: record.forecaster,
        counts: record.scores.map(|_| (record.n, record.successes)),
        estimate: None,
        scores: record.scores,
        n_min: if gated { n_min } else { 0 },
    }
}

/// Every `reputation` row the reader may see, options then agents then forecasters.
fn rows(
    store: &AgentLibraryStore,
    reader: &LogReader,
    now_ms: u64,
) -> Result<Vec<ReputationRow>, String> {
    let evidence = gather(store, reader)?;
    let rules = rules(now_ms)?;
    let mut out: Vec<ReputationRow> = Vec::new();
    for (kind, outcomes) in [("option", &evidence.options), ("agent", &evidence.agents)] {
        out.extend(
            learned(outcomes, &rules)?
                .into_iter()
                .map(|row| subject_row(kind, row)),
        );
    }
    let records = track_records(&evidence.forecasts, rules.gate).map_err(|e| e.to_string())?;
    let n_min = rules.gate.n_min();
    out.extend(
        records
            .into_iter()
            .map(|record| forecaster_row(record, n_min)),
    );
    Ok(out)
}

/// The `reputation` relation of the reader's visible log.
pub(super) fn reputation_relation(
    store: &AgentLibraryStore,
    reader: &LogReader,
) -> Result<Relation, String> {
    let now_ms = crate::server::dispatch::authoritative_now_ms();
    Ok(relation(
        "reputation",
        COLUMNS,
        &rows(store, reader, now_ms)?,
    ))
}

/// `subject`'s learned reliability (an option first, then an agent), anchored at the
/// caller's prior; `None` when the visible log has no gated estimate of it.
pub(super) fn learned_reliability(
    store: &AgentLibraryStore,
    reader: &LogReader,
    subject: &str,
    prior: (f64, f64),
) -> Result<Option<LearnedReliability>, String> {
    let evidence = gather(store, reader)?;
    let rules = rules(crate::server::dispatch::authoritative_now_ms())?;
    for outcomes in [&evidence.options, &evidence.agents] {
        let found = learned(outcomes, &rules)?.into_iter().find(|row| {
            row.subject == subject && matches!(row.reputation, Reputation::Estimated(_))
        });
        if let Some(row) = found {
            let estimate = anchored_reliability(prior.0, prior.1, &row, rules.delta)
                .map_err(|e| e.to_string())?;
            return Ok(Some(LearnedReliability {
                mean: estimate.mean,
                lower: estimate.lower,
                upper: estimate.upper,
                trials: row.trials,
            }));
        }
    }
    Ok(None)
}
