//! Decision record views (EH-066): read-only relations over the caller's VISIBLE
//! decision log, served through the SQL catalog every SQL surface reads (`Method::Sql`,
//! the KnowledgeStream SQL family, the Postgres wire) and through the UQL `DECISIONS`
//! source. No wire method of their own.
//!
//! * `decisions` — one row per visible record (question, candidate source, outcome and
//!   the chosen option, resolution kind, evidence class, policy digest, who committed it
//!   and when);
//! * `decision_evaluations` — the evaluations of those records;
//! * `decision_resolutions` — the abstention resolutions of those records;
//! * `reputation` — learned reputations over those records (EH-525, `stat_reputation`).
//!
//! Visibility is applied BEFORE a row exists — tenant-visible records plus the caller's
//! own principal-visible ones ([`visible_entries`]) — and evaluations and resolutions
//! are read only for visible records, so no statement can reach, count or join a row
//! the caller could not `DecisionLog.get`. The names are reserved in the SQL catalog
//! (no user table, view or `COPY` can take them), and the relations live only in the
//! caller's per-statement projection, so they are read-only by construction.

use std::sync::Arc;

use eg_query::{Column, ColumnType, TableSchema};
use eg_types::decision::statistical::log::{DecisionLogEntry, StoredEvaluation, StoredResolution};
use serde::Serialize;
use serde_json::{Map, Value};

use super::stat_log::{evaluations_of, visible_entries, LogReader, MAX_LOG_ROWS};
use super::stat_retention::Retention;
use super::SharedState;
use crate::server::access::CarrierAuthority;
use crate::server::persistence::agent_library::AgentLibraryStore;
use crate::server::persistence::decision_jobs::decode_artifact;
use crate::server::sql_catalog_acl::relations::{ReadOnlyRelations, Relation};

/// One column: name, SQL type, and its cell for an item.
pub(super) type Col<T> = (&'static str, ColumnType, fn(&T) -> Value);

pub(super) fn text(value: Option<String>) -> Value {
    value.map_or(Value::Null, Value::String)
}

pub(super) fn millis(value: u64) -> Value {
    Value::from(i64::try_from(value).unwrap_or(i64::MAX))
}

/// The wire name of a serde value: a unit enum's string, or `tag` of a tagged one.
pub(super) fn wire_name(value: &impl Serialize, tag: &str) -> Option<String> {
    match serde_json::to_value(value).ok()? {
        Value::String(name) => Some(name),
        Value::Object(map) => map.get(tag)?.as_str().map(str::to_string),
        _ => None,
    }
}

fn chosen(entry: &DecisionLogEntry) -> Option<String> {
    let outcome = serde_json::to_value(&entry.record.outcome).ok()?;
    outcome.get("option_id")?.as_str().map(str::to_string)
}

const DECISION_COLUMNS: &[Col<DecisionLogEntry>] = &[
    ("record_id", ColumnType::Text, |e| {
        Value::from(e.record.record_id.clone())
    }),
    ("question_id", ColumnType::Text, |e| {
        Value::from(e.record.question.question_id.clone())
    }),
    ("question_kind", ColumnType::Text, |e| {
        text(wire_name(&e.record.question.kind, ""))
    }),
    ("safety", ColumnType::Text, |e| {
        text(wire_name(&e.record.question.safety, ""))
    }),
    ("source", ColumnType::Text, |e| {
        text(wire_name(&e.record.candidate_source, "source"))
    }),
    ("outcome", ColumnType::Text, |e| {
        text(wire_name(&e.record.outcome, "outcome"))
    }),
    ("option_id", ColumnType::Text, |e| text(chosen(e))),
    ("resolution_kind", ColumnType::Text, |e| {
        text(wire_name(&e.record.resolution_kind, ""))
    }),
    ("evidence_class", ColumnType::Text, |e| {
        text(wire_name(&e.record.evidence_class, ""))
    }),
    ("policy_digest", ColumnType::Text, |e| {
        Value::from(e.record.inputs.policy_digest.clone())
    }),
    ("committed_by", ColumnType::Text, |e| {
        Value::from(e.committed_by.clone())
    }),
    ("created_at_ms", ColumnType::BigInt, |e| {
        millis(e.record.created_at_ms)
    }),
    ("committed_at_ms", ColumnType::BigInt, |e| {
        millis(e.committed_at_ms)
    }),
];

const EVALUATION_COLUMNS: &[Col<StoredEvaluation>] = &[
    ("record_id", ColumnType::Text, |r| {
        Value::from(r.evaluation.record_id.clone())
    }),
    ("evaluation_id", ColumnType::Text, |r| {
        Value::from(r.evaluation.evaluation_id.clone())
    }),
    ("class", ColumnType::Text, |r| {
        text(wire_name(&r.evaluation.class, ""))
    }),
    ("fidelity", ColumnType::Text, |r| {
        text(wire_name(&r.evaluation.fidelity, ""))
    }),
    ("producer", ColumnType::Text, |r| {
        Value::from(r.producer.clone())
    }),
    ("success", ColumnType::Bool, |r| {
        r.evaluation.success.map_or(Value::Null, Value::Bool)
    }),
    ("recorded_at_ms", ColumnType::BigInt, |r| {
        millis(r.recorded_at_ms)
    }),
];

const RESOLUTION_COLUMNS: &[Col<StoredResolution>] = &[
    ("record_id", ColumnType::Text, |r| {
        Value::from(r.resolution.record_id.clone())
    }),
    ("resolution_id", ColumnType::Text, |r| {
        Value::from(r.resolution.resolution_id.clone())
    }),
    ("option_id", ColumnType::Text, |r| {
        Value::from(r.resolution.option_id.clone())
    }),
    ("resolver", ColumnType::Text, |r| {
        text(wire_name(&r.resolution.resolver, "resolver"))
    }),
    ("class", ColumnType::Text, |r| text(wire_name(&r.class, ""))),
    ("producer", ColumnType::Text, |r| {
        Value::from(r.producer.clone())
    }),
    ("recorded_at_ms", ColumnType::BigInt, |r| {
        millis(r.recorded_at_ms)
    }),
];

fn schema_of<T>(name: &str, columns: &[Col<T>]) -> TableSchema {
    let columns = columns
        .iter()
        .map(|(column, ty, _)| Column::new(*column, *ty, true, false))
        .collect();
    TableSchema::new(name, columns)
}

pub(super) fn relation<T>(name: &str, columns: &[Col<T>], items: &[T]) -> Relation {
    let rows = items
        .iter()
        .map(|item| columns.iter().map(|(_, _, cell)| cell(item)).collect())
        .collect();
    (schema_of(name, columns), rows)
}

/// The caller's view of one tenant's decision log.
pub(crate) struct DecisionViews {
    store: Arc<AgentLibraryStore>,
    reader: LogReader,
    /// The verified carrier's tenant scope: where governed pointers live.
    served_tenant: String,
}

impl DecisionViews {
    /// The views of `authority`'s tenant log as its principal sees it.
    pub(crate) fn of(store: Arc<AgentLibraryStore>, authority: &CarrierAuthority) -> Self {
        let (tenant_id, principal) = authority.log_owner();
        Self {
            store,
            reader: LogReader {
                tenant_id: tenant_id.to_string(),
                principal: principal.to_string(),
                roles: Vec::new(),
                retention: Retention::none(),
            },
            served_tenant: authority.tenant_scope().to_string(),
        }
    }

    /// [`Self::of`] over the server's decision log; `None` when the log cannot be
    /// opened (no persistence directory) — the relations are then simply absent.
    pub(crate) async fn served(
        state: &SharedState,
        authority: &CarrierAuthority,
    ) -> Option<Arc<Self>> {
        let open = state.read().await.agent_library.clone();
        let store = match open {
            Some(store) => store,
            None => match state.write().await.ensure_agent_library() {
                Ok(store) => store,
                Err(error) => {
                    tracing::warn!(%error, "decision record views unavailable");
                    return None;
                }
            },
        };
        Some(Arc::new(Self::of(store, authority)))
    }

    fn entries(&self) -> Result<Vec<DecisionLogEntry>, String> {
        visible_entries(&self.store, &self.reader)
    }

    fn resolutions_of(&self, record_id: &str) -> Result<Vec<StoredResolution>, String> {
        self.store
            .decision_artifacts_with_prefix(
                &self.reader.tenant_id,
                &format!("resolution:{record_id}:"),
                MAX_LOG_ROWS,
            )?
            .into_iter()
            .map(|(_, bytes)| decode_artifact(&bytes, "abstention resolution"))
            .collect()
    }
}

impl ReadOnlyRelations for DecisionViews {
    fn materialize(&self) -> Result<Vec<Relation>, String> {
        let entries = self.entries()?;
        let mut evaluations = Vec::new();
        let mut resolutions = Vec::new();
        for entry in &entries {
            let id = &entry.record.record_id;
            evaluations.extend(evaluations_of(&self.store, &self.reader.tenant_id, id)?);
            resolutions.extend(self.resolutions_of(id)?);
        }
        let mut relations = vec![
            relation("decisions", DECISION_COLUMNS, &entries),
            relation("decision_evaluations", EVALUATION_COLUMNS, &evaluations),
            relation("decision_resolutions", RESOLUTION_COLUMNS, &resolutions),
        ];
        // EH-394..EH-397: the retrieval-learning relations, over the same reader.
        relations.extend(super::stat_retrieval_views::learning_relations(
            &self.store,
            &self.reader,
            &self.served_tenant,
        )?);
        // EH-525: learned reputations over the same visible log.
        relations.push(super::stat_reputation::reputation_relation(
            &self.store,
            &self.reader,
        )?);
        Ok(relations)
    }
}

impl eg_plan::exec::DecisionSource for DecisionViews {
    /// EH-525: `SOURCE RELIABILITY` reads the subject's learned reputation.
    fn learned_reliability(
        &self,
        subject: &str,
        prior_mean: f64,
        prior_strength: f64,
    ) -> Result<Option<eg_plan::exec::LearnedReliability>, String> {
        super::stat_reputation::learned_reliability(
            &self.store,
            &self.reader,
            subject,
            (prior_mean, prior_strength),
        )
    }

    fn decision_rows(&self) -> Result<Vec<Map<String, Value>>, String> {
        let (_, rows) = relation("decisions", DECISION_COLUMNS, &self.entries()?);
        Ok(rows
            .into_iter()
            .map(|cells| {
                DECISION_COLUMNS
                    .iter()
                    .map(|(name, _, _)| name.to_string())
                    .zip(cells)
                    .collect()
            })
            .collect())
    }
}
