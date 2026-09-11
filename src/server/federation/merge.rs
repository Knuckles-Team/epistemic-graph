//! Shared normalization and merge logic for federated result partials.

use std::collections::{HashMap, HashSet};

use serde_json::Value;

use super::{FedRow, FederatedMetadata, FederatedResponse, PeerOutcome};

/// RRF rank constant `k` (the canonical 60) used when fusing ranked partials.
const RRF_K: f64 = 60.0;

/// The healthy rows and provenance collected from one set of local/peer outcomes.
/// Both ranked and typed fusion use this summary so errors, source order, and
/// contribution counts have one implementation.
struct OutcomeSummary {
    healthy_lists: Vec<Vec<FedRow>>,
    metadata: FederatedMetadata,
}

impl OutcomeSummary {
    fn finish(self, rows: Vec<FedRow>) -> FederatedResponse {
        FederatedResponse {
            rows,
            metadata: self.metadata,
        }
    }
}

fn summarize_outcomes(outcomes: Vec<PeerOutcome>) -> OutcomeSummary {
    let mut peers_queried = Vec::new();
    let mut failed_peers = Vec::new();
    let mut healthy_lists = Vec::new();
    let mut contributing_sources = 0;

    for outcome in outcomes {
        let is_local = outcome.source == "<local>";
        if !is_local {
            peers_queried.push(outcome.source.clone());
        }
        match outcome.rows {
            Ok(rows) => {
                contributing_sources += 1;
                healthy_lists.push(rows);
            }
            Err(_) if !is_local => failed_peers.push(outcome.source),
            Err(_) => {}
        }
    }

    let metadata = FederatedMetadata {
        partial: !failed_peers.is_empty(),
        peers_queried,
        failed_peers,
        contributing_sources,
    };
    OutcomeSummary {
        healthy_lists,
        metadata,
    }
}

/// Merge the local + peer partial result sets into ONE answer.
///
/// Rows are unioned and de-duplicated by [`FedRow::key`] (first occurrence
/// wins for the payload). When any source contributes a score, rows are
/// re-ranked with Reciprocal Rank Fusion over each source's own ranking. With
/// no scores, first-seen source/row order is preserved. A degraded peer is
/// omitted from the rows and recorded in the response metadata.
pub fn merge_partials(outcomes: Vec<PeerOutcome>) -> FederatedResponse {
    let summary = summarize_outcomes(outcomes);
    let rows = merge_ranked_rows(&summary.healthy_lists);
    summary.finish(rows)
}

fn merge_ranked_rows(lists: &[Vec<FedRow>]) -> Vec<FedRow> {
    let mut order = Vec::new();
    let mut merged = HashMap::new();
    for list in lists {
        for row in list {
            match merged.get_mut(&row.key) {
                Some(existing) => update_best_score(existing, row.score),
                None => {
                    order.push(row.key.clone());
                    merged.insert(row.key.clone(), row.clone());
                }
            }
        }
    }

    let any_score = lists.iter().flatten().any(|row| row.score.is_some());
    if !any_score {
        return take_rows(order, merged);
    }

    let rrf = rrf_scores(lists);
    let mut rows = take_rows(order, merged);
    rows.sort_by(|left, right| {
        let left_score = rrf.get(&left.key).copied().unwrap_or(0.0);
        let right_score = rrf.get(&right.key).copied().unwrap_or(0.0);
        right_score
            .partial_cmp(&left_score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left.key.cmp(&right.key))
    });
    for row in &mut rows {
        row.score = rrf.get(&row.key).copied();
    }
    rows
}

fn update_best_score(existing: &mut FedRow, score: Option<f64>) {
    if let Some(score) = score {
        existing.score = Some(existing.score.map_or(score, |current| current.max(score)));
    }
}

fn take_rows(order: Vec<String>, mut merged: HashMap<String, FedRow>) -> Vec<FedRow> {
    order
        .into_iter()
        .filter_map(|key| merged.remove(&key))
        .collect()
}

/// Reciprocal Rank Fusion score per key across source ranked lists.
fn rrf_scores(lists: &[Vec<FedRow>]) -> HashMap<String, f64> {
    let mut scores = HashMap::new();
    for list in lists {
        for (rank, row) in list.iter().enumerate() {
            let contribution = 1.0 / (RRF_K + (rank as f64) + 1.0);
            *scores.entry(row.key.clone()).or_insert(0.0) += contribution;
        }
    }
    scores
}

/// The schema field carried by a decoded partial for a language.
fn typed_schema_field(lang: &str) -> &'static str {
    match lang {
        "sparql" => "vars",
        _ => "columns",
    }
}

/// Whether a language uses schema-aware tabular fusion.
pub fn is_typed_lang(lang: &str) -> bool {
    matches!(lang, "sql" | "sparql")
}

fn extract_schema_cells(data: &Value, field: &str) -> (Vec<String>, Vec<Value>) {
    let names = data
        .get(field)
        .and_then(Value::as_array)
        .map(|array| {
            array
                .iter()
                .map(|value| match value {
                    Value::String(name) => name.clone(),
                    other => other.to_string(),
                })
                .collect()
        })
        .unwrap_or_default();
    let cells = data
        .get("cells")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    (names, cells)
}

/// Reconcile one cell to a canonical typed token for cross-source deduplication.
pub(crate) fn canonical_token(value: &Value) -> String {
    match value {
        Value::Null => "\u{0}null".to_string(),
        Value::Bool(boolean) => format!("b{boolean}"),
        Value::Number(number) => canonical_number_token(number),
        Value::String(string) => canonical_scalar_string_token(string),
        other => format!("j{other}"),
    }
}

fn canonical_number_token(number: &serde_json::Number) -> String {
    if let Some(integer) = number.as_i64() {
        return format!("n{integer}");
    }
    if let Some(unsigned) = number.as_u64() {
        return format!("n{unsigned}");
    }
    if let Some(float) = number.as_f64() {
        if float.is_finite()
            && float.fract() == 0.0
            && float >= i64::MIN as f64
            && float <= i64::MAX as f64
        {
            return format!("n{}", float as i64);
        }
        return format!("f{float}");
    }
    format!("f{number}")
}

fn canonical_scalar_string_token(string: &str) -> String {
    if let Ok(integer) = string.parse::<i64>() {
        if integer.to_string() == string {
            return format!("n{integer}");
        }
    }
    if let Ok(float) = string.parse::<f64>() {
        if float.is_finite() {
            if float.fract() == 0.0 && float >= i64::MIN as f64 && float <= i64::MAX as f64 {
                let integer = float as i64;
                if integer.to_string() == string {
                    return format!("n{integer}");
                }
            } else if float.to_string() == string {
                return format!("f{float}");
            }
        }
    }
    format!("s{string}")
}

fn typed_dedup_key(row: &HashMap<String, Value>, sorted_schema: &[String]) -> String {
    sorted_schema
        .iter()
        .map(|name| {
            let token = row
                .get(name)
                .map(canonical_token)
                .unwrap_or_else(|| canonical_token(&Value::Null));
            format!("{name}={token}")
        })
        .collect::<Vec<_>>()
        .join("\u{1f}")
}

type TypedRow = HashMap<String, Value>;

struct TypedRows {
    maps: Vec<TypedRow>,
    union_schema: Vec<String>,
}

fn normalize_typed_rows(lists: Vec<Vec<FedRow>>, field: &str) -> TypedRows {
    let mut maps = Vec::new();
    let mut union_schema = Vec::new();
    let mut schema_names = HashSet::new();
    for list in lists {
        for row in list {
            maps.push(normalize_typed_row(
                row,
                field,
                &mut union_schema,
                &mut schema_names,
            ));
        }
    }
    TypedRows { maps, union_schema }
}

fn normalize_typed_row(
    row: FedRow,
    field: &str,
    union_schema: &mut Vec<String>,
    schema_names: &mut HashSet<String>,
) -> TypedRow {
    let (names, cells) = extract_schema_cells(&row.data, field);
    let mut normalized = HashMap::new();
    for (index, name) in names.into_iter().enumerate() {
        if schema_names.insert(name.clone()) {
            union_schema.push(name.clone());
        }
        let value = cells.get(index).cloned().unwrap_or(Value::Null);
        normalized.insert(name, value);
    }
    normalized
}

fn dedup_typed_rows(typed: TypedRows, field: &str) -> Vec<FedRow> {
    let TypedRows { maps, union_schema } = typed;
    let mut sorted_schema = union_schema.clone();
    sorted_schema.sort();
    let mut seen = HashSet::new();
    let mut rows = Vec::new();
    for map in maps {
        let key = typed_dedup_key(&map, &sorted_schema);
        if !seen.insert(key.clone()) {
            continue;
        }
        let cells: Vec<_> = union_schema
            .iter()
            .map(|name| map.get(name).cloned().unwrap_or(Value::Null))
            .collect();
        rows.push(FedRow {
            key,
            score: None,
            data: serde_json::json!({ field: union_schema.clone(), "cells": cells }),
        });
    }
    rows
}

/// Schema-aware typed fusion of SQL / SPARQL federated partials. Columns or
/// variables are aligned by name, cells use canonical typed equality, and rows
/// retain first-seen order after deterministic deduplication.
pub fn merge_partials_typed(outcomes: Vec<PeerOutcome>, lang: &str) -> FederatedResponse {
    let OutcomeSummary {
        healthy_lists,
        metadata,
    } = summarize_outcomes(outcomes);
    let field = typed_schema_field(lang);
    let typed = normalize_typed_rows(healthy_lists, field);
    FederatedResponse {
        rows: dedup_typed_rows(typed, field),
        metadata,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn eg309_typed_dedup_reconciles_numeric_string_and_number() {
        assert_eq!(
            canonical_token(&serde_json::json!(30)),
            canonical_token(&serde_json::json!("30"))
        );
        assert_eq!(
            canonical_token(&serde_json::json!(30.0)),
            canonical_token(&serde_json::json!(30))
        );
        assert_ne!(
            canonical_token(&serde_json::json!("007")),
            canonical_token(&serde_json::json!(7))
        );
        assert_ne!(
            canonical_token(&serde_json::json!("alice")),
            canonical_token(&serde_json::json!("bob"))
        );
    }
}
