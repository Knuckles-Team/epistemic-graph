//! `DecisionLog.query`: one read-only SQL statement over the caller's visible
//! decision log (EH-066).
//!
//! The three relations are materialised per request from exactly what the
//! caller may read -- visibility is applied by [`visible_entries`] BEFORE a row
//! exists, and evaluations and resolutions are read only for visible records --
//! so no SQL text can reach an invisible row, count it or join against it. The
//! statement must classify as a read (`SELECT`/`WITH`/`SHOW`/`EXPLAIN`); DDL
//! (`CREATE EXTERNAL TABLE` would read server files), DML and `COPY` are
//! refused before planning. The SQL runs on the graph-free DataFusion path
//! (`eg_query::exec_sql_over_tables`) the observability log search uses.

use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use serde::Serialize;

use eg_types::contract::BoundedVec;
use eg_types::decision::statistical::log::{DecisionLogEntry, StoredEvaluation, StoredResolution};
use eg_types::decision::statistical::log_view::{
    DecisionLogRows, ViewCell, MAX_VIEW_ROWS, MAX_VIEW_SQL_BYTES,
};
use eg_types::decision::statistical::StatisticalErrorCode;

use super::stat_executor::ExecutionContext;
use super::stat_log::{evaluations_of, visible_entries, LogReader, MAX_LOG_ROWS};
use super::stat_support::refusal;
use crate::server::persistence::decision_jobs::decode_artifact;

/// Format identity of a view answer.
const VIEW_SCHEMA_VERSION: u16 = 1;

/// One column of a materialised relation.
enum Column {
    Text(Vec<Option<String>>),
    Int(Vec<Option<i64>>),
    Bool(Vec<Option<bool>>),
}

type Table = (String, SchemaRef, Vec<RecordBatch>);

fn invalid(detail: impl std::fmt::Display) -> String {
    refusal(StatisticalErrorCode::ParameterInvalid, detail)
}

fn table(name: &str, columns: Vec<(&str, Column)>) -> Result<Table, String> {
    let mut fields = Vec::with_capacity(columns.len());
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(columns.len());
    for (column, values) in columns {
        let (kind, array): (DataType, ArrayRef) = match values {
            Column::Text(v) => (DataType::Utf8, Arc::new(StringArray::from(v))),
            Column::Int(v) => (DataType::Int64, Arc::new(Int64Array::from(v))),
            Column::Bool(v) => (DataType::Boolean, Arc::new(BooleanArray::from(v))),
        };
        fields.push(Field::new(column, kind, true));
        arrays.push(array);
    }
    let schema: SchemaRef = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema.clone(), arrays)
        .map_err(|error| format!("decision view `{name}`: {error}"))?;
    Ok((name.to_string(), schema, vec![batch]))
}

/// The wire name of a serde value: a unit enum's string, or `tag` of a
/// tagged one.
fn wire_name(value: &impl Serialize, tag: &str) -> Option<String> {
    match serde_json::to_value(value).ok()? {
        serde_json::Value::String(name) => Some(name),
        serde_json::Value::Object(map) => map.get(tag)?.as_str().map(str::to_string),
        _ => None,
    }
}

fn ms(value: u64) -> Option<i64> {
    Some(i64::try_from(value).unwrap_or(i64::MAX))
}

fn chosen(entry: &DecisionLogEntry) -> Option<String> {
    let outcome = serde_json::to_value(&entry.record.outcome).ok()?;
    outcome.get("option_id")?.as_str().map(str::to_string)
}

fn decisions(entries: &[DecisionLogEntry]) -> Result<Table, String> {
    let text = |f: &dyn Fn(&DecisionLogEntry) -> Option<String>| {
        Column::Text(entries.iter().map(f).collect())
    };
    let int =
        |f: &dyn Fn(&DecisionLogEntry) -> Option<i64>| Column::Int(entries.iter().map(f).collect());
    table(
        "decisions",
        vec![
            ("record_id", text(&|e| Some(e.record.record_id.clone()))),
            (
                "question_id",
                text(&|e| Some(e.record.question.question_id.clone())),
            ),
            (
                "question_kind",
                text(&|e| wire_name(&e.record.question.kind, "")),
            ),
            (
                "safety",
                text(&|e| wire_name(&e.record.question.safety, "")),
            ),
            (
                "source",
                text(&|e| wire_name(&e.record.candidate_source, "source")),
            ),
            (
                "outcome",
                text(&|e| wire_name(&e.record.outcome, "outcome")),
            ),
            ("option_id", text(&chosen)),
            (
                "resolution_kind",
                text(&|e| wire_name(&e.record.resolution_kind, "")),
            ),
            (
                "evidence_class",
                text(&|e| wire_name(&e.record.evidence_class, "")),
            ),
            (
                "policy_digest",
                text(&|e| Some(e.record.inputs.policy_digest.clone())),
            ),
            ("committed_by", text(&|e| Some(e.committed_by.clone()))),
            ("created_at_ms", int(&|e| ms(e.record.created_at_ms))),
            ("committed_at_ms", int(&|e| ms(e.committed_at_ms))),
        ],
    )
}

fn evaluations(rows: &[StoredEvaluation]) -> Result<Table, String> {
    let text = |f: &dyn Fn(&StoredEvaluation) -> Option<String>| {
        Column::Text(rows.iter().map(f).collect())
    };
    table(
        "evaluations",
        vec![
            ("record_id", text(&|r| Some(r.evaluation.record_id.clone()))),
            (
                "evaluation_id",
                text(&|r| Some(r.evaluation.evaluation_id.clone())),
            ),
            ("class", text(&|r| wire_name(&r.evaluation.class, ""))),
            ("fidelity", text(&|r| wire_name(&r.evaluation.fidelity, ""))),
            ("producer", text(&|r| Some(r.producer.clone()))),
            (
                "success",
                Column::Bool(rows.iter().map(|r| r.evaluation.success).collect()),
            ),
            (
                "recorded_at_ms",
                Column::Int(rows.iter().map(|r| ms(r.recorded_at_ms)).collect()),
            ),
        ],
    )
}

fn resolutions(rows: &[StoredResolution]) -> Result<Table, String> {
    let text = |f: &dyn Fn(&StoredResolution) -> Option<String>| {
        Column::Text(rows.iter().map(f).collect())
    };
    table(
        "resolutions",
        vec![
            ("record_id", text(&|r| Some(r.resolution.record_id.clone()))),
            (
                "resolution_id",
                text(&|r| Some(r.resolution.resolution_id.clone())),
            ),
            ("option_id", text(&|r| Some(r.resolution.option_id.clone()))),
            (
                "resolver",
                text(&|r| wire_name(&r.resolution.resolver, "resolver")),
            ),
            ("class", text(&|r| wire_name(&r.class, ""))),
            ("producer", text(&|r| Some(r.producer.clone()))),
            (
                "recorded_at_ms",
                Column::Int(rows.iter().map(|r| ms(r.recorded_at_ms)).collect()),
            ),
        ],
    )
}

fn resolutions_of(
    ctx: &ExecutionContext,
    record_id: &str,
) -> Result<Vec<StoredResolution>, String> {
    ctx.store
        .decision_artifacts_with_prefix(
            ctx.tenant_id,
            &format!("resolution:{record_id}:"),
            MAX_LOG_ROWS,
        )?
        .into_iter()
        .map(|(_, bytes)| decode_artifact(&bytes, "abstention resolution"))
        .collect()
}

/// The three relations, built from the caller's visible entries only.
fn relations(ctx: &ExecutionContext, reader: &LogReader) -> Result<Vec<Table>, String> {
    let entries = visible_entries(ctx.store, reader)?;
    let mut stored_evaluations = Vec::new();
    let mut stored_resolutions = Vec::new();
    for entry in &entries {
        let id = &entry.record.record_id;
        stored_evaluations.extend(evaluations_of(ctx.store, ctx.tenant_id, id)?);
        stored_resolutions.extend(resolutions_of(ctx, id)?);
    }
    Ok(vec![
        decisions(&entries)?,
        evaluations(&stored_evaluations)?,
        resolutions(&stored_resolutions)?,
    ])
}

fn cell(value: serde_json::Value) -> ViewCell {
    match value {
        serde_json::Value::Null => ViewCell::Null,
        serde_json::Value::Bool(b) => ViewCell::Bool(b),
        serde_json::Value::Number(n) => n
            .as_i64()
            .map_or_else(|| ViewCell::Text(n.to_string()), ViewCell::Int),
        serde_json::Value::String(s) => ViewCell::Text(s),
        other => ViewCell::Text(other.to_string()),
    }
}

fn check_read_only(sql: &str) -> Result<(), String> {
    if sql.len() > MAX_VIEW_SQL_BYTES {
        return Err(invalid(format!(
            "a view query is at most {MAX_VIEW_SQL_BYTES} bytes"
        )));
    }
    match eg_query::classify(sql) {
        Ok(eg_query::StatementKind::Read) => Ok(()),
        Ok(_) => Err(invalid("a decision view answers one read-only statement")),
        Err(error) => Err(invalid(error)),
    }
}

/// Run `sql` over the caller's visible decision log.
pub(super) fn query_view(
    ctx: &ExecutionContext,
    reader: &LogReader,
    sql: &str,
) -> Result<DecisionLogRows, String> {
    check_read_only(sql)?;
    let result = eg_query::exec_sql_over_tables(relations(ctx, reader)?, sql).map_err(invalid)?;
    if result.rows.len() > MAX_VIEW_ROWS {
        return Err(refusal(
            StatisticalErrorCode::CandidateSetTooLarge,
            format!("the view answer exceeds {MAX_VIEW_ROWS} rows; narrow it"),
        ));
    }
    let bounded = |detail: String| invalid(detail);
    let rows = result
        .rows
        .into_iter()
        .map(|row| BoundedVec::new(row.into_iter().map(cell).collect()).map_err(bounded))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(DecisionLogRows {
        schema_version: VIEW_SCHEMA_VERSION,
        columns: BoundedVec::new(result.columns.into_iter().map(|c| c.name).collect())
            .map_err(bounded)?,
        rows: BoundedVec::new(rows).map_err(bounded)?,
    })
}
