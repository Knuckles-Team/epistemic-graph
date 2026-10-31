//! Run audit rows, tool-call rows and the conversation-history source over
//! stored steps (MEDULLA-UNIFIED-DATA-PLANE-R750.750 to R750.752).
//!
//! Audit append is idempotent per run and per tool call. Identities are
//! stored redacted: a record that carries a raw identity is refused. Search
//! filters by tenant and caller scope before ranking, and a search outside
//! the caller's scope is refused.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// A redacted identity: `redacted:` plus a SHA-256 prefix of the raw value.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RedactedIdentity(String);

impl RedactedIdentity {
    pub const PREFIX: &'static str = "redacted:";

    /// Redact a raw identity.
    pub fn redact(raw: &str) -> Self {
        let digest = hex::encode(Sha256::digest(raw.as_bytes()));
        Self(format!("{}{}", Self::PREFIX, &digest[..16]))
    }

    pub fn is_redacted(&self) -> bool {
        self.0
            .strip_prefix(Self::PREFIX)
            .is_some_and(|rest| rest.len() == 16 && rest.chars().all(|c| c.is_ascii_hexdigit()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// One run's audit row, linked to its trace node.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunAuditRecord {
    pub tenant: String,
    pub scope: String,
    pub run_id: String,
    pub trace_node: String,
    pub actor: RedactedIdentity,
    pub outcome: String,
    pub at_ms: i64,
}

/// One tool call within a run.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolCallRecord {
    pub tenant: String,
    pub run_id: String,
    pub call_id: String,
    pub tool: String,
    pub trace_node: String,
    pub args_digest: String,
    pub at_ms: i64,
}

/// One stored conversation step.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredStep {
    pub tenant: String,
    pub scope: String,
    pub run_id: String,
    pub index: u32,
    pub speaker: RedactedIdentity,
    pub text: String,
}

/// A refused history call, with its stable code.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HistoryRefusal {
    /// `IDENTITY_NOT_REDACTED`
    IdentityNotRedacted,
    /// `HISTORY_SCOPE_FORBIDDEN`
    ScopeForbidden(String),
}

impl HistoryRefusal {
    pub fn code(&self) -> &'static str {
        match self {
            Self::IdentityNotRedacted => "IDENTITY_NOT_REDACTED",
            Self::ScopeForbidden(_) => "HISTORY_SCOPE_FORBIDDEN",
        }
    }
}

/// Whether an append wrote a new row or found the same one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Appended {
    New,
    AlreadyPresent,
}

/// The caller of a history search.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HistoryCaller {
    pub tenant: String,
    pub scopes: Vec<String>,
}

/// One ranked search hit.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct HistoryHit {
    pub run_id: String,
    pub index: u32,
    pub score: f64,
}

/// The step store and audit rows behind the conversation-history source.
#[derive(Clone, Debug, Default)]
pub struct StepStore {
    runs: BTreeMap<(String, String), RunAuditRecord>,
    calls: BTreeMap<(String, String, String), ToolCallRecord>,
    steps: Vec<StoredStep>,
}

impl StepStore {
    /// Append one run row; idempotent per tenant and run.
    pub fn append_run(&mut self, record: RunAuditRecord) -> Result<Appended, HistoryRefusal> {
        if !record.actor.is_redacted() {
            return Err(HistoryRefusal::IdentityNotRedacted);
        }
        let key = (record.tenant.clone(), record.run_id.clone());
        if self.runs.contains_key(&key) {
            return Ok(Appended::AlreadyPresent);
        }
        self.runs.insert(key, record);
        Ok(Appended::New)
    }

    /// Append one tool call; idempotent per tenant, run and call id.
    pub fn append_call(&mut self, record: ToolCallRecord) -> Appended {
        let key = (
            record.tenant.clone(),
            record.run_id.clone(),
            record.call_id.clone(),
        );
        if self.calls.contains_key(&key) {
            return Appended::AlreadyPresent;
        }
        self.calls.insert(key, record);
        Appended::New
    }

    /// Store one step; its speaker must be redacted.
    pub fn store_step(&mut self, step: StoredStep) -> Result<(), HistoryRefusal> {
        if !step.speaker.is_redacted() {
            return Err(HistoryRefusal::IdentityNotRedacted);
        }
        self.steps.push(step);
        Ok(())
    }

    pub fn run_count(&self) -> usize {
        self.runs.len()
    }

    pub fn call_count(&self) -> usize {
        self.calls.len()
    }

    /// Search steps in `scope`. The tenant and scope filter runs before
    /// ranking; a scope the caller does not hold is refused.
    pub fn search(
        &self,
        caller: &HistoryCaller,
        scope: &str,
        query: &str,
        k: usize,
    ) -> Result<Vec<HistoryHit>, HistoryRefusal> {
        if !caller.scopes.iter().any(|s| s == scope) {
            return Err(HistoryRefusal::ScopeForbidden(scope.to_string()));
        }
        let terms: Vec<String> = query.split_whitespace().map(str::to_lowercase).collect();
        let mut hits: Vec<HistoryHit> = self
            .steps
            .iter()
            .filter(|step| step.tenant == caller.tenant && step.scope == scope)
            .filter_map(|step| {
                let text = step.text.to_lowercase();
                let matched = terms.iter().filter(|t| text.contains(t.as_str())).count();
                (matched > 0).then(|| HistoryHit {
                    run_id: step.run_id.clone(),
                    index: step.index,
                    score: matched as f64 / terms.len() as f64,
                })
            })
            .collect();
        hits.sort_by(|a, b| {
            b.score
                .total_cmp(&a.score)
                .then_with(|| a.run_id.cmp(&b.run_id))
                .then_with(|| a.index.cmp(&b.index))
        });
        hits.truncate(k);
        Ok(hits)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(tenant: &str, run_id: &str, actor: RedactedIdentity) -> RunAuditRecord {
        RunAuditRecord {
            tenant: tenant.to_string(),
            scope: "project:alpha".to_string(),
            run_id: run_id.to_string(),
            trace_node: format!("trace:{run_id}"),
            actor,
            outcome: "completed".to_string(),
            at_ms: 1_761_000_000_000,
        }
    }

    fn step(tenant: &str, scope: &str, run_id: &str, index: u32, text: &str) -> StoredStep {
        StoredStep {
            tenant: tenant.to_string(),
            scope: scope.to_string(),
            run_id: run_id.to_string(),
            index,
            speaker: RedactedIdentity::redact("alice@example.org"),
            text: text.to_string(),
        }
    }

    // spec: MEDULLA-UNIFIED-DATA-PLANE-R750.750
    #[test]
    fn history_records_match_golden_vectors() {
        let record = run("acme", "r1", RedactedIdentity::redact("alice@example.org"));
        let golden = r#"{"tenant":"acme","scope":"project:alpha","run_id":"r1","trace_node":"trace:r1","actor":"redacted:7a64adf28737ea90","outcome":"completed","at_ms":1761000000000}"#;
        assert_eq!(serde_json::to_string(&record).unwrap(), golden);
        assert_eq!(
            serde_json::from_str::<RunAuditRecord>(golden).unwrap(),
            record
        );
        let call = ToolCallRecord {
            tenant: "acme".to_string(),
            run_id: "r1".to_string(),
            call_id: "c1".to_string(),
            tool: "graph_query".to_string(),
            trace_node: "trace:r1/c1".to_string(),
            args_digest: "sha256:00".to_string(),
            at_ms: 1,
        };
        let json = serde_json::to_string(&call).unwrap();
        assert_eq!(serde_json::from_str::<ToolCallRecord>(&json).unwrap(), call);
    }

    // spec: MEDULLA-UNIFIED-DATA-PLANE-R750.751
    #[test]
    fn append_is_idempotent_and_search_filters_before_ranking() {
        let mut store = StepStore::default();
        let actor = RedactedIdentity::redact("alice@example.org");
        assert_eq!(
            store.append_run(run("acme", "r1", actor.clone())),
            Ok(Appended::New)
        );
        assert_eq!(
            store.append_run(run("acme", "r1", actor)),
            Ok(Appended::AlreadyPresent)
        );
        let call = ToolCallRecord {
            tenant: "acme".to_string(),
            run_id: "r1".to_string(),
            call_id: "c1".to_string(),
            tool: "t".to_string(),
            trace_node: "n".to_string(),
            args_digest: "d".to_string(),
            at_ms: 0,
        };
        assert_eq!(store.append_call(call.clone()), Appended::New);
        assert_eq!(store.append_call(call), Appended::AlreadyPresent);
        assert_eq!((store.run_count(), store.call_count()), (1, 1));

        store
            .store_step(step("acme", "p:a", "r1", 0, "deploy the cache"))
            .unwrap();
        store
            .store_step(step("acme", "p:a", "r1", 1, "deploy cache now please"))
            .unwrap();
        store
            .store_step(step("acme", "p:b", "r2", 0, "deploy cache secret"))
            .unwrap();
        store
            .store_step(step("globex", "p:a", "r3", 0, "deploy cache"))
            .unwrap();
        let caller = HistoryCaller {
            tenant: "acme".to_string(),
            scopes: vec!["p:a".to_string()],
        };
        let hits = store.search(&caller, "p:a", "deploy cache", 10).unwrap();
        assert_eq!(hits.len(), 2);
        assert!(hits.iter().all(|h| h.run_id == "r1"));
    }

    // spec: MEDULLA-UNIFIED-DATA-PLANE-R750.752
    #[test]
    fn unredacted_identity_and_foreign_scope_are_refused() {
        let mut store = StepStore::default();
        let raw = RedactedIdentity("alice@example.org".to_string());
        let error = store
            .append_run(run("acme", "r1", raw.clone()))
            .unwrap_err();
        assert_eq!(error.code(), "IDENTITY_NOT_REDACTED");
        let mut bad = step("acme", "p:a", "r1", 0, "x");
        bad.speaker = raw;
        assert_eq!(
            store.store_step(bad).unwrap_err(),
            HistoryRefusal::IdentityNotRedacted
        );
        assert_eq!(store.run_count(), 0);
        let caller = HistoryCaller {
            tenant: "acme".to_string(),
            scopes: vec!["p:a".to_string()],
        };
        let error = store.search(&caller, "p:b", "x", 5).unwrap_err();
        assert_eq!(error.code(), "HISTORY_SCOPE_FORBIDDEN");
    }
}
