//! Feature matrices over visible candidates (EH-021).
//!
//! One column per schema feature, one row per candidate, every cell on `Q32`.
//! A cell that cannot be computed is `None` until the feature's declared
//! missing-value rule resolves it: an imputation that the schema published, or
//! an `UnknownFact` abstention naming the candidate and field. Unknown is never
//! read as zero.

use eg_types::agent_component::FactQuality;
use eg_types::agent_ontology::satisfies;
use eg_types::decision::statistical::features::{FeatureKind, FeatureSchemaBody, MissingValue};
use eg_types::decision::statistical::StatisticalErrorCode;
use eg_types::decision::statistical::{TypedParam, TypedValue};

use super::bm25;
use super::candidate::CandidateView;
use super::quant::{q32, q32_integer};
use super::refusal::{Refusal, RefusalResult};

/// A computed, complete, fixed-point feature matrix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeatureMatrix {
    pub candidate_ids: Vec<String>,
    pub feature_names: Vec<String>,
    /// Row-major `Q32`, `candidates x features`.
    pub values: Vec<i64>,
}

impl FeatureMatrix {
    /// One candidate's row.
    pub fn row(&self, index: usize) -> &[i64] {
        let width = self.feature_names.len();
        &self.values[index * width..(index + 1) * width]
    }
}

/// A matrix, or the first fact whose absence the schema says to abstain on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MatrixOutcome {
    Complete(FeatureMatrix),
    UnknownFact { component_id: String, field: String },
}

/// What features read besides the candidates.
#[derive(Debug, Clone, Copy)]
pub struct FeatureInputs<'a> {
    pub params: &'a [TypedParam],
    /// The recorded clock the age features read.
    pub now_ms: u64,
}

fn param<'a>(inputs: &FeatureInputs<'a>, name: &str) -> RefusalResult<&'a TypedValue> {
    inputs
        .params
        .iter()
        .find(|p| p.name == name)
        .map(|p| &p.value)
        .ok_or_else(|| {
            Refusal::new(
                StatisticalErrorCode::ParameterInvalid,
                format!("parameter {name} is required"),
            )
        })
}

fn wrong_type(name: &str, expected: &str) -> Refusal {
    Refusal::new(
        StatisticalErrorCode::ParameterInvalid,
        format!("parameter {name} must be {expected}"),
    )
}

fn coverage_column(
    candidates: &[CandidateView],
    inputs: &FeatureInputs,
    name: &str,
) -> RefusalResult<Vec<Option<i64>>> {
    let TypedValue::IriList(required) = param(inputs, name)? else {
        return Err(wrong_type(name, "an iri_list"));
    };
    let total = required.len() as i64;
    candidates
        .iter()
        .map(|candidate| {
            if total == 0 {
                return Ok(Some(0));
            }
            let covered = required
                .iter()
                .filter(|need| {
                    candidate
                        .classification
                        .iter()
                        .any(|have| satisfies(have, need))
                })
                .count() as i64;
            Ok(Some(q32_integer(covered)? / total))
        })
        .collect()
}

fn text_column(
    candidates: &[CandidateView],
    inputs: &FeatureInputs,
    key: &str,
    name: &str,
) -> RefusalResult<Vec<Option<i64>>> {
    let TypedValue::Text(query) = param(inputs, name)? else {
        return Err(wrong_type(name, "text"));
    };
    let present: Vec<Option<&str>> = candidates
        .iter()
        .map(|c| c.texts.get(key).map(String::as_str))
        .collect();
    let documents: Vec<&str> = present.iter().flatten().copied().collect();
    let scores = bm25::scores(&documents, query);
    let mut next = scores.into_iter();
    present
        .iter()
        .map(|text| match text {
            Some(_) => next
                .next()
                .map(|score| q32(score).map(|v| v.value))
                .transpose(),
            None => Ok(None),
        })
        .collect()
}

fn integer_column(values: impl Iterator<Item = Option<i64>>) -> RefusalResult<Vec<Option<i64>>> {
    values
        .map(|value| value.map(q32_integer).transpose())
        .collect()
}

fn quality_rank(quality: FactQuality) -> i64 {
    match quality {
        FactQuality::Measured => 3,
        FactQuality::Estimated => 2,
        FactQuality::Declared => 1,
        FactQuality::Unavailable => 0,
    }
}

fn age_seconds(candidate: &CandidateView, now_ms: u64) -> Option<i64> {
    candidate
        .updated_at_ms
        .map(|updated| (now_ms.saturating_sub(updated) / 1_000) as i64)
}

fn column(
    kind: &FeatureKind,
    candidates: &[CandidateView],
    inputs: &FeatureInputs,
) -> RefusalResult<Vec<Option<i64>>> {
    match kind {
        FeatureKind::CoverageFraction { param } => coverage_column(candidates, inputs, param),
        FeatureKind::DeclaredCostMicros => integer_column(
            candidates
                .iter()
                .map(|c| c.cost_micros.and_then(|v| i64::try_from(v).ok())),
        ),
        FeatureKind::DeclaredP95LatencyMs => {
            integer_column(candidates.iter().map(|c| c.p95_ms.map(i64::from)))
        }
        FeatureKind::CostQuality => {
            integer_column(candidates.iter().map(|c| c.cost_quality.map(quality_rank)))
        }
        FeatureKind::AgeSeconds => {
            integer_column(candidates.iter().map(|c| age_seconds(c, inputs.now_ms)))
        }
        FeatureKind::Number { key } => Ok(candidates
            .iter()
            .map(|c| c.numbers.get(key).copied())
            .collect()),
        FeatureKind::TextBm25 { key, param } => text_column(candidates, inputs, key, param),
    }
}

fn resolve(cell: Option<i64>, missing: MissingValue) -> Option<i64> {
    match (cell, missing) {
        (Some(value), _) => Some(value),
        (None, MissingValue::Impute { value }) => {
            q32(super::quant::value_of(value)).ok().map(|v| v.value)
        }
        (None, MissingValue::Abstain) => None,
    }
}

/// Compute the feature matrix of `candidates` under `schema`.
pub fn feature_matrix(
    schema: &FeatureSchemaBody,
    candidates: &[CandidateView],
    inputs: &FeatureInputs,
) -> RefusalResult<MatrixOutcome> {
    let width = schema.features.len();
    let mut values = vec![0i64; candidates.len() * width];
    for (column_index, spec) in schema.features.iter().enumerate() {
        let cells = column(&spec.kind, candidates, inputs)?;
        for (row, cell) in cells.into_iter().enumerate() {
            let Some(value) = resolve(cell, spec.missing) else {
                return Ok(MatrixOutcome::UnknownFact {
                    component_id: candidates[row].id.clone(),
                    field: spec.name.clone(),
                });
            };
            values[row * width + column_index] = value;
        }
    }
    Ok(MatrixOutcome::Complete(FeatureMatrix {
        candidate_ids: candidates.iter().map(|c| c.id.clone()).collect(),
        feature_names: schema.names(),
        values,
    }))
}
