//! Server side of the host-driven bounded foreign-read lifecycle.
//!
//! The host owns the connector call.  This module owns only the authenticated
//! execution handle, its approval identity, bounded pages, and the request
//! scoped `ForeignSource` that the existing UQL/plan executor consumes.
//!
//! The current `RowSet` currency carries only `(id, score)`. This leaf validates
//! and bounds the source-provided provenance references, but deliberately does
//! not claim the R722.5 answer-provenance projection; that remains a separate
//! result/projection leaf until the existing column/evidence path is wired.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use medulla_types::connector_pack::ConnectorPackSourceApprovalResult;
use medulla_types::contract::Digest256;
use medulla_types::foreign_read::{
    ForeignReadAbortRequest, ForeignReadAbortResult, ForeignReadChunkRequest,
    ForeignReadChunkResult, ForeignReadCompleteRequest, ForeignReadCompleteResult,
    ForeignReadExecutionRef, ForeignReadLimits, ForeignReadRow, ForeignReadSourceIdentity,
};

use super::access::CarrierAuthority;
use super::foreign_catalog::{ForeignSourceCatalog, OwnedForeignRegistry};
use super::persistence::agent_library::AgentLibraryStore;
use crate::isolation::IsolationLayer;

/// Owner-controlled dependencies used to revalidate and bind completed reads.
pub(crate) struct ForeignBindingContext<'a> {
    pub(crate) catalog: &'a ForeignSourceCatalog,
    pub(crate) store: &'a AgentLibraryStore,
    pub(crate) isolation: &'a IsolationLayer,
}

pub(crate) use crate::server::handlers::query::canonical_plan_digest;

const MAX_SESSIONS: usize = 1_024;
// A process-wide cap protects the page cache when many otherwise-valid
// executions each stay below their per-execution limit. The budget is held
// until a handle is removed or expires; it is never inferred from caller
// supplied limits.
const MAX_TOTAL_ROWS: u64 = 1_000_000;
const MAX_TOTAL_BYTES: u64 = 256 * 1024 * 1024;
const MAX_ID_BYTES: usize = 64 * 1024;
const MAX_COLUMNS: usize = 128;
const MAX_COLUMN_NAME_BYTES: usize = 256;
const MAX_COLUMN_VALUE_BYTES: usize = 64 * 1024;
const MAX_PROVENANCE_ITEMS: usize = 32;
const MAX_PROVENANCE_BYTES: usize = 4 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SessionState {
    Open,
    Complete,
    Aborted,
}

struct SessionRecord {
    owner_scope: String,
    effective_actor_scope: String,
    delegation_digest: Digest256,
    authority_digest: Digest256,
    source_name: String,
    source_identity: ForeignReadSourceIdentity,
    approval: ConnectorPackSourceApprovalResult,
    plan_digest: Digest256,
    query_digest: Digest256,
    limits: ForeignReadLimits,
    expires_at: Instant,
    expires_at_ms: u64,
    next_sequence: u64,
    total_rows: u64,
    total_bytes: u64,
    rows: Vec<BufferedForeignRow>,
    state: SessionState,
    consumed: bool,
}

#[derive(Clone)]
struct BufferedForeignRow {
    id: String,
    score: Option<f32>,
    columns: std::collections::BTreeMap<String, serde_json::Value>,
    provenance: Vec<String>,
}

/// Process-local bounded execution handles. Durable approval and catalog state
/// remain authoritative; this map is only the in-flight page buffer.
pub(crate) struct ForeignReadSessions {
    next_id: AtomicU64,
    sessions: DashMap<u64, Arc<Mutex<SessionRecord>>>,
    admission: Mutex<()>,
    budget: Mutex<GlobalBudget>,
}

#[derive(Default)]
struct GlobalBudget {
    rows: u64,
    bytes: u64,
}

impl Default for ForeignReadSessions {
    fn default() -> Self {
        Self {
            next_id: AtomicU64::new(1),
            sessions: DashMap::new(),
            admission: Mutex::new(()),
            budget: Mutex::new(GlobalBudget::default()),
        }
    }
}

impl ForeignReadSessions {
    pub(crate) fn open(
        &self,
        caller: &CarrierAuthority,
        source_name: String,
        approval: ConnectorPackSourceApprovalResult,
        plan_digest: Digest256,
        query_digest: Digest256,
        limits: ForeignReadLimits,
    ) -> Result<(ForeignReadExecutionRef, u64, ForeignReadSourceIdentity), String> {
        limits.validate()?;
        if source_name.is_empty() || source_name.len() > MAX_ID_BYTES {
            return Err("INVALID_ARGUMENT: foreign read source_name is empty or too large".into());
        }
        let source_identity = ForeignReadSourceIdentity::from_approval(&approval)?;
        let _admission = self
            .admission
            .lock()
            .map_err(|_| "INTERNAL: foreign read admission lock poisoned".to_string())?;
        self.reap_expired();
        if self.sessions.len() >= MAX_SESSIONS {
            return Err("ENGINE_RESOURCE_EXHAUSTED: too many foreign read executions".into());
        }
        let execution_id = self.next_id.fetch_add(1, Ordering::Relaxed);
        if execution_id == 0 {
            return Err("INTERNAL: foreign read execution id exhausted".into());
        }
        let expires_at = Instant::now() + Duration::from_millis(limits.deadline_ms);
        let expires_at_ms = now_ms().saturating_add(limits.deadline_ms);
        let record = Arc::new(Mutex::new(SessionRecord {
            owner_scope: caller.owner_scope().to_string(),
            effective_actor_scope: caller.effective_actor_scope().to_string(),
            delegation_digest: caller.delegation_digest(),
            authority_digest: caller.authority_digest(),
            source_name,
            source_identity: source_identity.clone(),
            approval,
            plan_digest,
            query_digest,
            limits,
            expires_at,
            expires_at_ms,
            next_sequence: 0,
            total_rows: 0,
            total_bytes: 0,
            rows: Vec::new(),
            state: SessionState::Open,
            consumed: false,
        }));
        self.sessions.insert(execution_id, Arc::clone(&record));
        Ok((
            ForeignReadExecutionRef {
                execution_id,
                query_digest,
            },
            expires_at_ms,
            source_identity,
        ))
    }

    pub(crate) fn remove(&self, execution_id: u64) {
        let Some((_, record)) = self.sessions.remove(&execution_id) else {
            return;
        };
        let Ok(record) = record.lock() else {
            return;
        };
        let Ok(mut budget) = self.budget.lock() else {
            return;
        };
        budget.rows = budget.rows.saturating_sub(record.total_rows);
        budget.bytes = budget.bytes.saturating_sub(record.total_bytes);
    }

    pub(crate) fn page_source(
        &self,
        execution_id: u64,
    ) -> Result<medulla_plan::federation::SharedForeignSource, String> {
        let record = self
            .sessions
            .get(&execution_id)
            .map(|entry| Arc::clone(entry.value()))
            .ok_or_else(|| "FOREIGN_READ_NOT_FOUND: execution handle is unknown".to_string())?;
        Ok(Arc::new(PageBackedSource {
            record: Arc::downgrade(&record),
        }))
    }

    pub(crate) fn append(
        &self,
        caller: &CarrierAuthority,
        store: &AgentLibraryStore,
        catalog: &ForeignSourceCatalog,
        request: ForeignReadChunkRequest,
    ) -> Result<ForeignReadChunkResult, String> {
        let record = self.record_for(caller, request.execution)?;
        self.revalidate_phase(caller, store, catalog, &record)?;
        self.append_inner(request, record)
    }

    #[cfg(test)]
    fn append_for_test(
        &self,
        caller: &CarrierAuthority,
        request: ForeignReadChunkRequest,
    ) -> Result<ForeignReadChunkResult, String> {
        let record = self.record_for(caller, request.execution)?;
        self.append_inner(request, record)
    }

    #[cfg(test)]
    fn complete_for_test(
        &self,
        caller: &CarrierAuthority,
        request: ForeignReadCompleteRequest,
    ) -> Result<ForeignReadCompleteResult, String> {
        let record = self.record_for(caller, request.execution)?;
        let mut record = lock(&record)?;
        self.check_open(&record, request.execution, request.sequence)?;
        self.check_expiry(&record)?;
        record.state = SessionState::Complete;
        Ok(ForeignReadCompleteResult {
            execution: request.execution,
            sequence: request.sequence,
            total_rows: record.total_rows,
            total_bytes: record.total_bytes,
        })
    }

    #[cfg(test)]
    fn abort_for_test(
        &self,
        caller: &CarrierAuthority,
        request: ForeignReadAbortRequest,
    ) -> Result<ForeignReadAbortResult, String> {
        let record = self.record_for(caller, request.execution)?;
        let mut record = lock(&record)?;
        self.check_open(&record, request.execution, request.sequence)?;
        self.check_expiry(&record)?;
        if request.reason.is_empty() || request.reason.len() > MAX_ID_BYTES {
            return Err("INVALID_ARGUMENT: foreign read abort reason is empty or too large".into());
        }
        record.state = SessionState::Aborted;
        Ok(ForeignReadAbortResult {
            execution: request.execution,
            sequence: request.sequence,
            reason: request.reason,
        })
    }

    fn append_inner(
        &self,
        request: ForeignReadChunkRequest,
        record: Arc<Mutex<SessionRecord>>,
    ) -> Result<ForeignReadChunkResult, String> {
        let mut record = lock(&record)?;
        self.check_open(&record, request.execution, request.sequence)?;
        self.check_expiry(&record)?;
        if request.rows.is_empty() {
            return Err(
                "INVALID_ARGUMENT: foreign read chunk must contain at least one row".into(),
            );
        }
        if request.source_identity != record.source_identity {
            return Err(
                "FOREIGN_READ_SOURCE_IDENTITY_MISMATCH: chunk source identity differs".into(),
            );
        }
        if request.rows.len() > record.limits.chunk_rows as usize {
            return Err("ENGINE_RESOURCE_EXHAUSTED: foreign read chunk exceeds row limit".into());
        }
        let chunk_bytes = encoded_rows_bytes(&request.rows)?;
        if chunk_bytes > u64::from(record.limits.chunk_bytes) {
            return Err("ENGINE_RESOURCE_EXHAUSTED: foreign read chunk exceeds byte limit".into());
        }
        if record.total_rows.saturating_add(request.rows.len() as u64)
            > u64::from(record.limits.max_rows)
        {
            return Err("ENGINE_RESOURCE_EXHAUSTED: foreign read exceeds max_rows".into());
        }
        if record.total_bytes.saturating_add(chunk_bytes) > record.limits.max_bytes {
            return Err("ENGINE_RESOURCE_EXHAUSTED: foreign read exceeds max_bytes".into());
        }
        for row in &request.rows {
            validate_row(row)?;
        }
        let row_count = request.rows.len() as u64;
        self.reserve_budget(row_count, chunk_bytes)?;
        for row in &request.rows {
            record.rows.push(BufferedForeignRow {
                id: row.id.clone(),
                score: row.score,
                columns: row.columns.clone(),
                provenance: row.provenance.clone(),
            });
        }
        record.total_rows += row_count;
        record.total_bytes += chunk_bytes;
        record.next_sequence += 1;
        Ok(ForeignReadChunkResult {
            execution: request.execution,
            sequence: request.sequence,
            accepted_rows: request.rows.len() as u32,
            total_rows: record.total_rows,
            total_bytes: record.total_bytes,
            next_sequence: record.next_sequence,
        })
    }

    pub(crate) fn complete(
        &self,
        caller: &CarrierAuthority,
        store: &AgentLibraryStore,
        catalog: &ForeignSourceCatalog,
        request: ForeignReadCompleteRequest,
    ) -> Result<ForeignReadCompleteResult, String> {
        let record = self.record_for(caller, request.execution)?;
        self.revalidate_phase(caller, store, catalog, &record)?;
        let mut record = lock(&record)?;
        self.check_open(&record, request.execution, request.sequence)?;
        self.check_expiry(&record)?;
        record.state = SessionState::Complete;
        Ok(ForeignReadCompleteResult {
            execution: request.execution,
            sequence: request.sequence,
            total_rows: record.total_rows,
            total_bytes: record.total_bytes,
        })
    }

    pub(crate) fn abort(
        &self,
        caller: &CarrierAuthority,
        store: &AgentLibraryStore,
        catalog: &ForeignSourceCatalog,
        request: ForeignReadAbortRequest,
    ) -> Result<ForeignReadAbortResult, String> {
        let record = self.record_for(caller, request.execution)?;
        self.revalidate_phase(caller, store, catalog, &record)?;
        let mut record = lock(&record)?;
        self.check_open(&record, request.execution, request.sequence)?;
        self.check_expiry(&record)?;
        if request.reason.is_empty() || request.reason.len() > MAX_ID_BYTES {
            return Err("INVALID_ARGUMENT: foreign read abort reason is empty or too large".into());
        }
        record.state = SessionState::Aborted;
        Ok(ForeignReadAbortResult {
            execution: request.execution,
            sequence: request.sequence,
            reason: request.reason,
        })
    }

    /// Revalidate the one authoritative approval/catalog and return a registry
    /// whose named source is the completed page buffer.
    pub(crate) fn bind_for_query(
        &self,
        caller: &CarrierAuthority,
        execution: ForeignReadExecutionRef,
        plan_digest: Digest256,
        catalog: &ForeignSourceCatalog,
        store: &AgentLibraryStore,
        isolation: &IsolationLayer,
    ) -> Result<OwnedForeignRegistry, String> {
        let record_handle = self.record_for(caller, execution)?;
        let mut record = lock(&record_handle)?;
        if record.plan_digest != plan_digest {
            return Err("FOREIGN_READ_PLAN_MISMATCH: query plan digest differs".into());
        }
        if record.state != SessionState::Complete {
            return Err("FOREIGN_READ_NOT_COMPLETE: source pages are not complete".into());
        }
        if record.consumed {
            return Err("FOREIGN_READ_REPLAY: execution was already consumed".into());
        }
        record.consumed = true;
        let source_name = record.source_name.clone();
        let approval = record.approval.clone();
        let page_source = Arc::new(PageBackedSource {
            record: Arc::downgrade(&record_handle),
        });
        drop(record);
        catalog.bind_registered_for(
            caller,
            store,
            isolation,
            &approval,
            &source_name,
            page_source,
        )
    }

    /// Atomically bind bounded, distinct completed executions to one plan.
    /// Every session and durable approval is revalidated before any is consumed.
    pub(crate) fn bind_many_for_query(
        &self,
        caller: &CarrierAuthority,
        executions: &[ForeignReadExecutionRef],
        plan: &medulla_plan::Plan,
        plan_digest: Digest256,
        binding: ForeignBindingContext<'_>,
    ) -> Result<OwnedForeignRegistry, String> {
        if executions.is_empty() || executions.len() > 16 {
            return Err("INVALID_ARGUMENT: foreign execution count is out of bounds".into());
        }
        let mut ordered = executions.to_vec();
        ordered.sort_by_key(|execution| execution.execution_id);
        if ordered
            .windows(2)
            .any(|pair| pair[0].execution_id == pair[1].execution_id)
        {
            return Err("INVALID_ARGUMENT: repeated foreign execution".into());
        }
        let handles = ordered
            .iter()
            .map(|execution| self.record_for(caller, *execution))
            .collect::<Result<Vec<_>, _>>()?;
        // A stable lock order prevents two concurrent group binds from deadlocking.
        let mut records = handles
            .iter()
            .map(|handle| lock(handle))
            .collect::<Result<Vec<_>, _>>()?;
        let mut sources = Vec::with_capacity(records.len());
        let plan_names: std::collections::HashSet<&str> = plan
            .ops
            .iter()
            .filter_map(|op| match op {
                medulla_plan::Op::Foreign { name } => Some(name.as_str()),
                medulla_plan::Op::ForeignScan { source, .. } => match source.as_ref() {
                    medulla_types::wire::ForeignSourceSpec::Named { name } => Some(name.as_str()),
                    _ => None,
                },
                _ => None,
            })
            .collect();
        for (record, handle) in records.iter().zip(&handles) {
            self.check_expiry(record)?;
            if record.plan_digest != plan_digest {
                return Err("FOREIGN_READ_PLAN_MISMATCH: query plan digest differs".into());
            }
            if record.state != SessionState::Complete {
                return Err("FOREIGN_READ_NOT_COMPLETE: source pages are not complete".into());
            }
            if record.consumed {
                return Err("FOREIGN_READ_REPLAY: execution was already consumed".into());
            }
            if !plan_names.contains(record.source_name.as_str()) {
                return Err("FOREIGN_READ_UNUSED: execution source is absent from plan".into());
            }
            let driver: medulla_plan::federation::SharedForeignSource = Arc::new(PageBackedSource {
                record: Arc::downgrade(handle),
            });
            sources.push((record.approval.clone(), record.source_name.clone(), driver));
        }
        let registry = binding.catalog.bind_registered_many(
            caller,
            binding.store,
            binding.isolation,
            &sources,
        )?;
        for record in &mut records {
            record.consumed = true;
        }
        Ok(registry)
    }

    fn record_for(
        &self,
        caller: &CarrierAuthority,
        execution: ForeignReadExecutionRef,
    ) -> Result<Arc<Mutex<SessionRecord>>, String> {
        let record = self
            .sessions
            .get(&execution.execution_id)
            .map(|entry| Arc::clone(entry.value()))
            .ok_or_else(|| "FOREIGN_READ_NOT_FOUND: execution handle is unknown".to_string())?;
        let guard = lock(&record)?;
        if guard.owner_scope != caller.owner_scope() {
            return Err("ACCESS_DENIED: foreign read belongs to another owner".into());
        }
        if guard.effective_actor_scope != caller.effective_actor_scope() {
            return Err("ACCESS_DENIED: foreign read belongs to another effective actor".into());
        }
        if guard.delegation_digest != caller.delegation_digest() {
            return Err("ACCESS_DENIED: foreign read delegation differs".into());
        }
        if guard.authority_digest != caller.authority_digest() {
            return Err("ACCESS_DENIED: foreign read authority differs".into());
        }
        if guard.query_digest != execution.query_digest {
            return Err("FOREIGN_READ_QUERY_MISMATCH: query digest differs".into());
        }
        self.check_expiry(&guard)?;
        drop(guard);
        Ok(record)
    }

    fn revalidate_phase(
        &self,
        caller: &CarrierAuthority,
        store: &AgentLibraryStore,
        catalog: &ForeignSourceCatalog,
        record: &Arc<Mutex<SessionRecord>>,
    ) -> Result<(), String> {
        let (approval, source_name) = {
            let guard = lock(record)?;
            let (tenant, _) = caller.log_owner();
            if tenant != guard.approval.tenant_id {
                return Err("ACCESS_DENIED: source approval belongs to another tenant".into());
            }
            (guard.approval.clone(), guard.source_name.clone())
        };
        catalog.verify_registered_for(caller, store, &approval, &source_name)?;
        let current_identity = ForeignReadSourceIdentity::from_approval(&approval)?;
        let guard = lock(record)?;
        if current_identity != guard.source_identity {
            return Err("FOREIGN_READ_SOURCE_IDENTITY_STALE: source identity changed".into());
        }
        Ok(())
    }

    fn check_expiry(&self, record: &SessionRecord) -> Result<(), String> {
        if Instant::now() >= record.expires_at {
            return Err(format!(
                "{}: execution deadline elapsed",
                medulla_types::contract::EngineErrorCode::EngineDeadlineExceeded.as_str()
            ));
        }
        Ok(())
    }

    fn check_open(
        &self,
        record: &SessionRecord,
        execution: ForeignReadExecutionRef,
        sequence: u64,
    ) -> Result<(), String> {
        if record.state != SessionState::Open {
            return Err("FOREIGN_READ_TERMINAL: execution is already terminal".into());
        }
        if record.next_sequence != sequence {
            return Err("FOREIGN_READ_SEQUENCE: unexpected page sequence".into());
        }
        if record.query_digest != execution.query_digest {
            return Err("FOREIGN_READ_QUERY_MISMATCH: query digest differs".into());
        }
        Ok(())
    }

    fn reserve_budget(&self, rows: u64, bytes: u64) -> Result<(), String> {
        let mut budget = self
            .budget
            .lock()
            .map_err(|_| "INTERNAL: foreign read budget lock poisoned".to_string())?;
        let next_rows = budget.rows.checked_add(rows).ok_or_else(|| {
            "ENGINE_RESOURCE_EXHAUSTED: foreign read row budget overflow".to_string()
        })?;
        let next_bytes = budget.bytes.checked_add(bytes).ok_or_else(|| {
            "ENGINE_RESOURCE_EXHAUSTED: foreign read byte budget overflow".to_string()
        })?;
        if next_rows > MAX_TOTAL_ROWS {
            return Err(
                "ENGINE_RESOURCE_EXHAUSTED: foreign read process row budget exceeded".into(),
            );
        }
        if next_bytes > MAX_TOTAL_BYTES {
            return Err(
                "ENGINE_RESOURCE_EXHAUSTED: foreign read process byte budget exceeded".into(),
            );
        }
        budget.rows = next_rows;
        budget.bytes = next_bytes;
        Ok(())
    }

    fn reap_expired(&self) {
        let expired = self
            .sessions
            .iter()
            .filter_map(|entry| {
                let record = entry.value().lock().ok()?;
                (Instant::now() >= record.expires_at).then_some(*entry.key())
            })
            .collect::<Vec<_>>();
        for execution_id in expired {
            self.remove(execution_id);
        }
    }
}

/// The source callback is installed only after `ForeignSourceCatalog` has
/// revalidated the durable approval and current owner registration.
/// `fetch_columns` exposes only bounded connector/contract/mapping/fetch-time
/// metadata through the existing `ForeignRows` projection; the legacy RowSet
/// path remains id/score-only until the served R722.5 result contract is wired.
struct PageBackedSource {
    record: Weak<Mutex<SessionRecord>>,
}

impl medulla_plan::federation::ForeignSource for PageBackedSource {
    fn fetch(&self) -> Result<medulla_plan::RowSet, String> {
        let record = self
            .record
            .upgrade()
            .ok_or_else(|| "FOREIGN_READ_NOT_FOUND: execution handle is unknown".to_string())?;
        let record = lock(&record)?;
        if Instant::now() >= record.expires_at {
            return Err(format!(
                "{}: execution deadline elapsed",
                medulla_types::contract::EngineErrorCode::EngineDeadlineExceeded.as_str()
            ));
        }
        if record.state != SessionState::Complete {
            return Err("FOREIGN_READ_NOT_COMPLETE: source pages are not complete".into());
        }
        Ok(medulla_plan::RowSet::from_rows(
            record.rows.iter().map(|row| (row.id.clone(), row.score)),
        ))
    }

    fn fetch_columns(&self) -> Result<medulla_plan::federation_opt::ForeignRows, String> {
        let record = self
            .record
            .upgrade()
            .ok_or_else(|| "FOREIGN_READ_NOT_FOUND: execution handle is unknown".to_string())?;
        let record = lock(&record)?;
        if Instant::now() >= record.expires_at {
            return Err(format!(
                "{}: execution deadline elapsed",
                medulla_types::contract::EngineErrorCode::EngineDeadlineExceeded.as_str()
            ));
        }
        if record.state != SessionState::Complete {
            return Err("FOREIGN_READ_NOT_COMPLETE: source pages are not complete".into());
        }
        let fetch_time_ms = now_ms();
        let connector = record.approval.connector.as_str().to_string();
        let contract_iri = record.approval.registration.contract_iri.clone();
        // The SDK access contract's IRI is its virtual-mapping identity. The
        // registration's resource URI identifies the served MCP/API resource
        // and must not be relabeled as the mapping.
        let mapping_iri = contract_iri.clone();
        let rows = record
            .rows
            .iter()
            .map(|row| {
                let mut columns = row.columns.clone();
                for (name, value) in [
                    (
                        "connector",
                        serde_json::Value::String(connector.clone()),
                    ),
                    (
                        "contract_iri",
                        serde_json::Value::String(contract_iri.clone()),
                    ),
                    (
                        "mapping_iri",
                        serde_json::Value::String(mapping_iri.clone()),
                    ),
                    (
                        "fetch_time_ms",
                        serde_json::Value::Number(fetch_time_ms.into()),
                    ),
                    (
                        "provenance",
                        serde_json::Value::Array(
                            row.provenance
                                .iter()
                                .cloned()
                                .map(serde_json::Value::String)
                                .collect(),
                        ),
                    ),
                ] {
                    if columns.insert(name.to_string(), value).is_some() {
                        return Err(format!(
                            "FOREIGN_READ_COLUMN_COLLISION: mapped cell '{name}' is reserved for answer provenance"
                        ));
                    }
                }
                Ok(medulla_plan::federation_opt::ForeignRow {
                    id: row.id.clone(),
                    score: row.score,
                    columns,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        Ok(medulla_plan::federation_opt::ForeignRows::from_rows(rows))
    }

    fn fetch_projected(
        &self,
        plan: &medulla_plan::federation_opt::ColumnPlan,
    ) -> Result<medulla_plan::federation_opt::ForeignRows, String> {
        // Reuse the same mapped-cell currency and local residual semantics as
        // SQL/HTTP foreign sources. The host has already supplied cells that
        // the approved connector mapping permits; this step only filters and
        // projects them according to the existing plan.
        Ok(plan.finish(self.fetch_columns()?))
    }
}

fn lock<T>(value: &Mutex<T>) -> Result<std::sync::MutexGuard<'_, T>, String> {
    value
        .lock()
        .map_err(|_| "INTERNAL: foreign read session lock poisoned".to_string())
}

fn encoded_rows_bytes(rows: &[ForeignReadRow]) -> Result<u64, String> {
    rmp_serde::to_vec_named(rows)
        .map(|bytes| bytes.len() as u64)
        .map_err(|error| format!("INVALID_ARGUMENT: foreign read rows are not encodable: {error}"))
}

fn validate_row(row: &ForeignReadRow) -> Result<(), String> {
    if row.id.is_empty() || row.id.len() > MAX_ID_BYTES {
        return Err("INVALID_ARGUMENT: foreign read row id is empty or too large".into());
    }
    if row.provenance.len() > MAX_PROVENANCE_ITEMS
        || row
            .provenance
            .iter()
            .any(|item| item.is_empty() || item.len() > MAX_PROVENANCE_BYTES)
    {
        return Err("INVALID_ARGUMENT: foreign read row provenance is invalid".into());
    }
    if row.columns.len() > MAX_COLUMNS {
        return Err("INVALID_ARGUMENT: foreign read row has too many columns".into());
    }
    for (name, value) in &row.columns {
        if name.is_empty() || name.len() > MAX_COLUMN_NAME_BYTES {
            return Err("INVALID_ARGUMENT: foreign read column name is empty or too large".into());
        }
        let value_bytes = serde_json::to_vec(value)
            .map_err(|_| "INVALID_ARGUMENT: foreign read column value is not JSON")?;
        if value_bytes.len() > MAX_COLUMN_VALUE_BYTES {
            return Err("ENGINE_RESOURCE_EXHAUSTED: foreign read column value is too large".into());
        }
    }
    Ok(())
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis() as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::acl::RequestContextClaims;
    use crate::server::auth::VerifiedRequestContext;
    use medulla_types::connector_pack::{
        PackHeadRef, PackSourceApproval, PackSourceKind, PackSourceRegistration,
        CONNECTOR_PACK_SCHEMA_VERSION,
    };
    use medulla_types::contract::ResourceId;

    fn limits() -> ForeignReadLimits {
        ForeignReadLimits {
            max_rows: 8,
            max_bytes: 16 * 1024,
            chunk_rows: 4,
            chunk_bytes: 8 * 1024,
            deadline_ms: 1_000,
        }
    }

    #[cfg(feature = "query")]
    #[test]
    fn canonical_plan_digest_is_the_query_binding_pin() {
        let plan = medulla_plan::Plan::new(vec![medulla_plan::Op::Scan {
            label: "Issue".into(),
        }]);
        let expected =
            Digest256::sha256(&rmp_serde::to_vec_named(&plan).expect("serialize query plan"));
        assert_eq!(canonical_plan_digest(&plan).expect("digest plan"), expected);
    }

    fn approval() -> ConnectorPackSourceApprovalResult {
        let connector = ResourceId::new("connector").expect("connector id");
        ConnectorPackSourceApprovalResult {
            schema_version: CONNECTOR_PACK_SCHEMA_VERSION,
            tenant_id: "tenant-shared".into(),
            connector: connector.clone(),
            head: PackHeadRef {
                binding_revision: 1,
                pack_digest: Digest256::sha256(b"head"),
            },
            registration: PackSourceRegistration {
                connector,
                contract_iri: "ac:contract".into(),
                resource_uri: "mcp://connector/read".into(),
                source_kind: PackSourceKind::McpTool,
                approval: PackSourceApproval::Approved,
                class_iri: "ac:Issue".into(),
                source_id: "connector.issue.read".into(),
                operation: "read".into(),
                entity: "issue".into(),
                key_field: "id".into(),
            },
            contract_digest: Digest256::sha256(b"contract"),
            batch_id: "batch".into(),
            committed_version: 1,
        }
    }

    fn execution(
        sessions: &ForeignReadSessions,
        owner: &CarrierAuthority,
    ) -> ForeignReadExecutionRef {
        sessions
            .open(
                owner,
                "source".into(),
                approval(),
                Digest256::sha256(b"plan"),
                Digest256::sha256(b"query"),
                limits(),
            )
            .expect("open")
            .0
    }

    fn source_identity() -> ForeignReadSourceIdentity {
        ForeignReadSourceIdentity::from_approval(&approval()).expect("source identity")
    }

    #[test]
    fn source_identity_is_derived_from_approved_registration_and_head() {
        let approval = approval();
        let identity =
            ForeignReadSourceIdentity::from_approval(&approval).expect("source identity");
        assert_eq!(identity.source_ref, approval.registration.resource_uri);
        assert_eq!(identity.source_revision, approval.head.binding_revision);
        assert_eq!(identity.schema_digest, approval.contract_digest);
        assert_eq!(identity.snapshot_ref, approval.head.pack_digest);
    }

    fn carrier_with_delegation(agent_id: &str, delegation: &[&str]) -> CarrierAuthority {
        CarrierAuthority::from_verified(&VerifiedRequestContext::from_verified_claims(
            RequestContextClaims {
                principal: "principal:owner".into(),
                tenant: "tenant-shared".into(),
                agent_id: agent_id.into(),
                audience: "medulla".into(),
                policy_version: "policy-test".into(),
                scopes: vec!["kg:read".into()],
                delegation: delegation.iter().map(|value| (*value).into()).collect(),
                ..RequestContextClaims::default()
            },
            format!("test:{agent_id}"),
        ))
        .expect("verified test carrier")
    }

    #[test]
    fn owner_digest_and_sequence_are_checked_before_page_acceptance() {
        let sessions = ForeignReadSessions::default();
        let owner = CarrierAuthority::verified_for_test("owner");
        let other = CarrierAuthority::verified_for_test("other");
        let execution = execution(&sessions, &owner);
        let row = ForeignReadRow {
            id: "issue-1".into(),
            score: Some(1.0),
            columns: std::collections::BTreeMap::new(),
            provenance: vec!["connector:issue-1".into()],
        };

        let denied = sessions.append_for_test(
            &other,
            ForeignReadChunkRequest {
                execution,
                sequence: 0,
                source_identity: source_identity(),
                rows: vec![row.clone()],
            },
        );
        assert!(denied
            .expect_err("another owner must be refused")
            .starts_with("ACCESS_DENIED:"));

        let wrong_digest = ForeignReadExecutionRef {
            query_digest: Digest256::sha256(b"other-query"),
            ..execution
        };
        let mismatch = sessions.append_for_test(
            &owner,
            ForeignReadChunkRequest {
                execution: wrong_digest,
                sequence: 0,
                source_identity: source_identity(),
                rows: vec![row.clone()],
            },
        );
        assert!(mismatch
            .expect_err("caller cannot substitute a query digest")
            .starts_with("FOREIGN_READ_QUERY_MISMATCH:"));

        let out_of_sequence = sessions.append_for_test(
            &owner,
            ForeignReadChunkRequest {
                execution,
                sequence: 1,
                source_identity: source_identity(),
                rows: vec![row.clone()],
            },
        );
        assert!(out_of_sequence
            .expect_err("page sequence must start at zero")
            .starts_with("FOREIGN_READ_SEQUENCE:"));

        let mut wrong_identity = source_identity();
        wrong_identity.schema_digest = Digest256::sha256(b"different-contract");
        let identity_mismatch = sessions.append_for_test(
            &owner,
            ForeignReadChunkRequest {
                execution,
                sequence: 0,
                source_identity: wrong_identity,
                rows: vec![row.clone()],
            },
        );
        assert!(identity_mismatch
            .expect_err("a chunk cannot replace the approved source identity")
            .starts_with("FOREIGN_READ_SOURCE_IDENTITY_MISMATCH:"));

        let accepted = sessions
            .append_for_test(
                &owner,
                ForeignReadChunkRequest {
                    execution,
                    sequence: 0,
                    source_identity: source_identity(),
                    rows: vec![row],
                },
            )
            .expect("owner may append the first page");
        assert_eq!(accepted.next_sequence, 1);
        sessions
            .complete_for_test(
                &owner,
                ForeignReadCompleteRequest {
                    execution,
                    sequence: 1,
                },
            )
            .expect("owner may complete the page stream");
        let replay = sessions.complete_for_test(
            &owner,
            ForeignReadCompleteRequest {
                execution,
                sequence: 1,
            },
        );
        assert!(replay
            .expect_err("terminal executions cannot be replayed")
            .starts_with("FOREIGN_READ_TERMINAL:"));
    }

    #[test]
    fn execution_binds_effective_actor_and_delegation_not_just_owner_scope() {
        let sessions = ForeignReadSessions::default();
        let owner = carrier_with_delegation("worker-a", &["principal:owner", "agent:worker-a"]);
        let same_owner_other_actor =
            carrier_with_delegation("worker-b", &["principal:owner", "agent:worker-b"]);
        let same_actor_other_delegation =
            carrier_with_delegation("worker-a", &["principal:owner", "agent:other"]);
        let execution = execution(&sessions, &owner);
        let row = ForeignReadRow {
            id: "issue-1".into(),
            score: None,
            columns: std::collections::BTreeMap::new(),
            provenance: vec!["connector:issue-1".into()],
        };

        for (caller, expected) in [
            (&same_owner_other_actor, "effective actor"),
            (&same_actor_other_delegation, "delegation"),
        ] {
            let refusal = sessions
                .append_for_test(
                    caller,
                    ForeignReadChunkRequest {
                        execution,
                        sequence: 0,
                        source_identity: source_identity(),
                        rows: vec![row.clone()],
                    },
                )
                .expect_err("a phase must retain the verified authority binding");
            assert!(
                refusal.starts_with("ACCESS_DENIED:"),
                "{expected}: {refusal}"
            );
            assert!(refusal.contains(expected), "{expected}: {refusal}");
        }
    }

    #[test]
    fn abort_is_terminal_and_expired_execution_cannot_complete() {
        let sessions = ForeignReadSessions::default();
        let owner = CarrierAuthority::verified_for_test("owner");
        let execution = execution(&sessions, &owner);
        sessions
            .abort_for_test(
                &owner,
                ForeignReadAbortRequest {
                    execution,
                    sequence: 0,
                    reason: "source unavailable".into(),
                },
            )
            .expect("owner may abort an open execution");
        let replay = sessions.abort_for_test(
            &owner,
            ForeignReadAbortRequest {
                execution,
                sequence: 0,
                reason: "replay".into(),
            },
        );
        assert!(replay
            .expect_err("abort is terminal")
            .starts_with("FOREIGN_READ_TERMINAL:"));

        let mut expired_limits = limits();
        expired_limits.deadline_ms = 1;
        let expired = sessions
            .open(
                &owner,
                "expired".into(),
                approval(),
                Digest256::sha256(b"expired-plan"),
                Digest256::sha256(b"expired-query"),
                expired_limits,
            )
            .expect("open short-lived execution")
            .0;
        std::thread::sleep(Duration::from_millis(10));
        let refusal = sessions.complete_for_test(
            &owner,
            ForeignReadCompleteRequest {
                execution: expired,
                sequence: 0,
            },
        );
        assert!(refusal
            .expect_err("expired execution cannot complete")
            .starts_with("ENGINE_DEADLINE_EXCEEDED:"));
    }

    #[test]
    fn foreign_read_rows_match_sdk_msgpack_golden_fixture() {
        let rows = vec![ForeignReadRow {
            id: "東京".into(),
            score: None,
            columns: std::collections::BTreeMap::from([
                ("unicode".into(), serde_json::Value::String("café".into())),
                ("float".into(), serde_json::json!(0.0)),
                ("array".into(), serde_json::json!([1, "é", true])),
            ]),
            provenance: vec!["source:✓".into()],
        }];
        let bytes = rmp_serde::to_vec_named(&rows).expect("encode rows");
        let expected = concat!(
            "9184a26964a6e69db1e4baaca573636f7265c0",
            "a7636f6c756d6e7383a561727261799301a2c3a9c3a5",
            "666c6f6174cb0000000000000000a7756e69636f6465",
            "a5636166c3a9aa70726f76656e616e636591aa736f7572",
            "63653ae29c93",
        );
        assert_eq!(bytes.len(), 92);
        assert_eq!(hex::encode(&bytes), expected);
        assert_eq!(
            Digest256::sha256(&bytes).to_hex(),
            "28a764dca2b38da98b274805f746279d5899a01cd1111d169803a9981e12c3a0"
        );
        assert_eq!(
            encoded_rows_bytes(&rows).expect("count encoded rows"),
            bytes.len() as u64
        );
    }

    #[test]
    fn foreign_read_score_uses_optional_f32_wire_width() {
        let some = vec![ForeignReadRow {
            id: "row-1".into(),
            score: Some(0.5),
            columns: std::collections::BTreeMap::new(),
            provenance: Vec::new(),
        }];
        let none = vec![ForeignReadRow {
            score: None,
            ..some[0].clone()
        }];
        let some_bytes = rmp_serde::to_vec_named(&some).expect("encode f32 score");
        let none_bytes = rmp_serde::to_vec_named(&none).expect("encode nil score");
        assert_eq!(some_bytes.len(), 43);
        assert_eq!(none_bytes.len(), 39);
        assert_eq!(
            hex::encode(some_bytes),
            concat!(
                "9184a26964a5726f772d31a573636f7265ca3f000000",
                "a7636f6c756d6e7380aa70726f76656e616e636590",
            )
        );
        assert_eq!(encoded_rows_bytes(&some).expect("count f32 score"), 43);
    }

    #[test]
    fn mapped_cells_use_existing_foreign_rows_residual_and_projection() {
        use medulla_plan::federation_opt::{ColumnPlan, ColumnPredicate, Comparison, PushdownSupport};

        let sessions = ForeignReadSessions::default();
        let owner = CarrierAuthority::verified_for_test("owner");
        let execution = execution(&sessions, &owner);
        let row = |id: &str, title: &str, state: &str| ForeignReadRow {
            id: id.into(),
            score: None,
            columns: std::collections::BTreeMap::from([
                ("title".into(), serde_json::Value::String(title.into())),
                ("state".into(), serde_json::Value::String(state.into())),
            ]),
            provenance: vec![format!("connector:{id}")],
        };
        sessions
            .append_for_test(
                &owner,
                ForeignReadChunkRequest {
                    execution,
                    sequence: 0,
                    source_identity: source_identity(),
                    rows: vec![
                        row("issue-1", "keep", "open"),
                        row("issue-2", "drop", "closed"),
                    ],
                },
            )
            .expect("mapped page accepted");
        sessions
            .complete_for_test(
                &owner,
                ForeignReadCompleteRequest {
                    execution,
                    sequence: 1,
                },
            )
            .expect("mapped page completed");

        let source = sessions
            .page_source(execution.execution_id)
            .expect("page source exists");
        let mapped = source
            .fetch_columns()
            .expect("mapped rows are materialized");
        assert_eq!(
            mapped.rows()[0].columns.get("mapping_iri"),
            Some(&serde_json::Value::String("ac:contract".into()))
        );
        let plan = ColumnPlan::new(
            &std::collections::BTreeSet::from(["title".into(), "state".into()]),
            &["title".into()],
            &[ColumnPredicate {
                column: "state".into(),
                comparison: Comparison::Eq,
                value: serde_json::Value::String("open".into()),
            }],
            false,
            |_| PushdownSupport::Unsupported,
        )
        .expect("mapping-approved columns form a plan");
        let rows = source
            .fetch_projected(&plan)
            .expect("existing ForeignRows projection applies");
        assert_eq!(rows.rows().len(), 1);
        assert_eq!(rows.rows()[0].id, "issue-1");
        assert_eq!(
            rows.rows()[0].columns,
            std::collections::BTreeMap::from([(
                "title".into(),
                serde_json::Value::String("keep".into()),
            )])
        );
    }
}
