//! Virtual-graph sources from connector packs, federated at query time
//! (EG-FEDERATED-QUERY-R722).
//!
//! * [`PackSourceSet`] holds one [`PackSourceRegistration`] per mapping. A new or
//!   changed contract registers `held`; an unchanged digest is a no-op; only
//!   [`PackSourceSet::approve`] lets a mapping serve reads.
//! * [`SourceExecutor`] is the one seam a read crosses. The engine never dials a
//!   connector: arbora implements the executor over its governed fleet call, so
//!   admission, the caller's grant and the outbound guard stay at the one door.
//! * [`federate_class`] plans a read over an ontology class: it fans out to every
//!   approved source mapped to the class, in parallel, with bounded pages and rows
//!   and a per-source deadline. It maps records to class properties, merges them and
//!   stamps each row with its source's provenance. A failed source is a named
//!   partial result, never an error for the whole query and never a silent gap.
//! * [`ClassFederationSource`] exposes a class answer as a named [`ForeignSource`],
//!   so `FOREIGN SCAN 'vg:Issue'` and `QueryForeignColumns` read it through the same
//!   registry as every other foreign source.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::mpsc;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use eg_types::connector_pack::pack_source::MAX_CLASS_SOURCES;
use eg_types::connector_pack::{
    class_matches, iri_local_name, PackSourceApproval, PackSourceRefusal, PackSourceRegistration,
    SourceCaller, SourceOutcome, SourceOutcomeStatus, SourceReadError, SourceReadPage,
    SourceReadRequest, VirtualClassAnswer, VirtualClassQuery, VirtualClassRow,
    VirtualRowProvenance,
};
use serde_json::Value;

use crate::federation::ForeignSource;
use crate::federation_opt::{ForeignRow, ForeignRows};
use crate::rowset::RowSet;

/// The registry name prefix of a class federation source: `vg:<class local name>`.
pub const CLASS_SOURCE_PREFIX: &str = "vg:";

/// Provenance and outcome columns a class source exposes besides its properties.
pub const PROVENANCE_COLUMNS: &[&str] = &[
    "_class_iri",
    "_key",
    "_connector",
    "_contract_iri",
    "_mapping_iri",
    "_resource_uri",
    "_source_kind",
    "_contract_digest",
    "_fetched_at_ms",
];
/// Columns of the one outcome row per source in a column read.
pub const OUTCOME_COLUMNS: &[&str] = &["_outcome", "_outcome_reason", "_outcome_rows"];

/// The source executor arbora implements (see the module doc). `read` performs one
/// page read for `request.caller` and must return by `request.deadline_ms`.
pub trait SourceExecutor: Send + Sync {
    fn read(&self, request: &SourceReadRequest) -> Result<SourceReadPage, SourceReadError>;
}

/// What a registration changed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RegisterOutcome {
    Added,
    /// Same contract digest: approval and mapping are kept.
    Unchanged,
    /// Changed contract: the mapping is replaced and returns to `held`.
    Replaced,
}

/// Every registered pack source, keyed by mapping IRI.
#[derive(Clone, Debug, Default)]
pub struct PackSourceSet {
    by_mapping: BTreeMap<String, PackSourceRegistration>,
}

impl PackSourceSet {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register one data resource. The caller's `approval` is ignored: registration
    /// never approves (EG-FEDERATED-QUERY-R722.2).
    pub fn register(
        &mut self,
        mut registration: PackSourceRegistration,
    ) -> Result<RegisterOutcome, PackSourceRefusal> {
        registration.validate()?;
        registration.approval = PackSourceApproval::Held;
        match self.by_mapping.get(&registration.mapping_iri) {
            Some(existing) if existing.contract_digest == registration.contract_digest => {
                Ok(RegisterOutcome::Unchanged)
            }
            Some(_) => {
                self.by_mapping
                    .insert(registration.mapping_iri.clone(), registration);
                Ok(RegisterOutcome::Replaced)
            }
            None => {
                self.by_mapping
                    .insert(registration.mapping_iri.clone(), registration);
                Ok(RegisterOutcome::Added)
            }
        }
    }

    /// Approve a mapping for reads (the admission decision of `ARBORA-FLEET-R042`).
    /// `contract_digest` must name the registered contract, so an approval decided
    /// against an older contract cannot approve a newer one.
    pub fn approve(&mut self, mapping_iri: &str, contract_digest: &str) -> Result<(), String> {
        self.set_approval(mapping_iri, contract_digest, PackSourceApproval::Approved)
    }

    /// Return a mapping to `held`.
    pub fn hold(&mut self, mapping_iri: &str, contract_digest: &str) -> Result<(), String> {
        self.set_approval(mapping_iri, contract_digest, PackSourceApproval::Held)
    }

    fn set_approval(
        &mut self,
        mapping_iri: &str,
        contract_digest: &str,
        approval: PackSourceApproval,
    ) -> Result<(), String> {
        let registration = self
            .by_mapping
            .get_mut(mapping_iri)
            .ok_or_else(|| format!("NOT_FOUND: no pack source mapping {mapping_iri}"))?;
        if registration.contract_digest != contract_digest {
            return Err(format!(
                "CONFLICT: mapping {mapping_iri} is registered at contract {} not {contract_digest}",
                registration.contract_digest
            ));
        }
        registration.approval = approval;
        Ok(())
    }

    pub fn get(&self, mapping_iri: &str) -> Option<&PackSourceRegistration> {
        self.by_mapping.get(mapping_iri)
    }

    pub fn len(&self) -> usize {
        self.by_mapping.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_mapping.is_empty()
    }

    /// Every registration (held or approved) whose class answers `class`.
    pub fn for_class(&self, class: &str) -> Vec<PackSourceRegistration> {
        self.by_mapping
            .values()
            .filter(|r| class_matches(&r.class_iri, class))
            .cloned()
            .collect()
    }

    /// The local names of every registered class, the `vg:<name>` sources a caller sees.
    pub fn class_names(&self) -> BTreeSet<String> {
        self.by_mapping
            .values()
            .map(|r| iri_local_name(&r.class_iri).to_string())
            .collect()
    }

    /// Every registration in mapping order.
    pub fn iter(&self) -> impl Iterator<Item = &PackSourceRegistration> {
        self.by_mapping.values()
    }
}

/// Decides whether `caller` may read one source. The served engine passes its own
/// grant check; the executor still enforces the caller's grant at the door.
pub type SourceAuthorizer = dyn Fn(&PackSourceRegistration) -> Result<(), String> + Send + Sync;

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// What one source's read loop produced.
struct SourceRead {
    rows: Vec<VirtualClassRow>,
    outcome: SourceOutcome,
}

/// Plan and run a federated read over one ontology class (EG-FEDERATED-QUERY-R722).
///
/// `sources` are the registrations for the class (held ones included, so they are
/// named in the answer). Rows come back sorted by connector, then key.
pub fn federate_class(
    sources: &[PackSourceRegistration],
    query: &VirtualClassQuery,
    caller: &SourceCaller,
    executor: Option<Arc<dyn SourceExecutor>>,
    authorize: &SourceAuthorizer,
) -> VirtualClassAnswer {
    let query = query.bounded();
    let mut outcomes = Vec::new();
    let mut rows = Vec::new();
    let mut pending = Vec::new();
    let mut matched: Vec<&PackSourceRegistration> = sources
        .iter()
        .filter(|r| class_matches(&r.class_iri, &query.class_iri))
        .collect();
    matched.sort_by(|a, b| a.mapping_iri.cmp(&b.mapping_iri));
    for (index, registration) in matched.into_iter().enumerate() {
        let refuse = |status: SourceOutcomeStatus, reason: String| SourceOutcome {
            connector: registration.connector.clone(),
            mapping_iri: registration.mapping_iri.clone(),
            status,
            rows: 0,
            pages: 0,
            reason: Some(format!("{}: {reason}", status.code())),
        };
        if index >= MAX_CLASS_SOURCES {
            outcomes.push(refuse(
                SourceOutcomeStatus::Refused,
                format!("class fan-out is bounded at {MAX_CLASS_SOURCES} sources"),
            ));
            continue;
        }
        if registration.approval != PackSourceApproval::Approved {
            outcomes.push(refuse(
                SourceOutcomeStatus::Held,
                format!("mapping {} is not approved", registration.mapping_iri),
            ));
            continue;
        }
        if let Err(reason) = authorize(registration) {
            outcomes.push(refuse(SourceOutcomeStatus::NotGranted, reason));
            continue;
        }
        let Some(executor) = executor.clone() else {
            outcomes.push(refuse(
                SourceOutcomeStatus::Unavailable,
                "no source executor is bound to this engine".into(),
            ));
            continue;
        };
        pending.push((registration.clone(), executor));
    }

    // Fan out: one thread per source, all bounded by the same deadline.
    let deadline = Instant::now() + Duration::from_millis(query.source_timeout_ms);
    let (sender, receiver) = mpsc::channel::<(String, SourceRead)>();
    let mut waiting: BTreeMap<String, PackSourceRegistration> = BTreeMap::new();
    for (registration, executor) in pending {
        let sender = sender.clone();
        let query = query.clone();
        let caller = caller.clone();
        let kept = registration.clone();
        let spawned = std::thread::Builder::new()
            .name("eg-pack-source-read".into())
            .spawn(move || {
                let read = read_source(&registration, &query, &caller, &*executor, deadline);
                let _ = sender.send((registration.mapping_iri.clone(), read));
            });
        match spawned {
            Ok(_) => {
                waiting.insert(kept.mapping_iri.clone(), kept);
            }
            Err(error) => outcomes.push(SourceOutcome {
                connector: kept.connector,
                mapping_iri: kept.mapping_iri,
                status: SourceOutcomeStatus::Unavailable,
                rows: 0,
                pages: 0,
                reason: Some(format!("SOURCE_UNAVAILABLE: read worker failed: {error}")),
            }),
        }
    }
    drop(sender);
    while !waiting.is_empty() {
        let left = deadline.saturating_duration_since(Instant::now());
        match receiver.recv_timeout(left) {
            Ok((mapping, read)) => {
                if waiting.remove(&mapping).is_some() {
                    rows.extend(read.rows);
                    outcomes.push(read.outcome);
                }
            }
            Err(_) => break,
        }
    }
    for (_, registration) in waiting {
        outcomes.push(SourceOutcome {
            connector: registration.connector,
            mapping_iri: registration.mapping_iri,
            status: SourceOutcomeStatus::Timeout,
            rows: 0,
            pages: 0,
            reason: Some(format!(
                "SOURCE_TIMEOUT: no answer within {} ms",
                query.source_timeout_ms
            )),
        });
    }

    if let Some(key) = &query.key {
        // The exact local residual: always applied, pushed down or not.
        rows.retain(|row| &row.key == key);
    }
    rows.sort_by(|a, b| {
        (a.provenance.connector.as_str(), a.key.as_str())
            .cmp(&(b.provenance.connector.as_str(), b.key.as_str()))
    });
    outcomes.sort_by(|a, b| a.mapping_iri.cmp(&b.mapping_iri));
    let partial = outcomes.iter().any(|o| o.status != SourceOutcomeStatus::Ok);
    VirtualClassAnswer {
        class_iri: query.class_iri.clone(),
        rows,
        sources: outcomes,
        partial,
    }
}

/// The bounded page loop for one source.
fn read_source(
    registration: &PackSourceRegistration,
    query: &VirtualClassQuery,
    caller: &SourceCaller,
    executor: &dyn SourceExecutor,
    deadline: Instant,
) -> SourceRead {
    let mut rows = Vec::new();
    let mut seen = BTreeSet::new();
    let mut pages = 0u32;
    let mut page_token = None;
    let mut status = SourceOutcomeStatus::Ok;
    let mut reason = None;
    let mut dropped = 0u32;
    let key_filter = if registration.key_filter_pushdown {
        query.key.clone()
    } else {
        None
    };
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            status = SourceOutcomeStatus::Timeout;
            reason = Some(format!(
                "SOURCE_TIMEOUT: deadline reached after {pages} pages"
            ));
            break;
        }
        let request = SourceReadRequest {
            connector: registration.connector.clone(),
            contract_iri: registration.contract_iri.clone(),
            mapping_iri: registration.mapping_iri.clone(),
            source_kind: registration.source_kind,
            operation: registration.operation.clone(),
            key_field: registration.key_field.clone(),
            key_filter: key_filter.clone(),
            pagination: registration.pagination,
            page_token: page_token.clone(),
            page_size: query.page_size,
            deadline_ms: left.as_millis() as u64,
            caller: caller.clone(),
        };
        let page = match executor.read(&request) {
            Ok(page) => page,
            Err(error) => {
                // A failure after some pages keeps those rows and names the gap.
                status = error.code.into();
                reason = Some(format!("{}: {}", status.code(), error.reason));
                break;
            }
        };
        pages += 1;
        let fetched_at_ms = now_ms();
        for record in page.records {
            if rows.len() as u32 >= query.max_rows_per_source {
                status = SourceOutcomeStatus::Truncated;
                reason = Some(format!(
                    "SOURCE_TRUNCATED: row bound {} reached",
                    query.max_rows_per_source
                ));
                break;
            }
            match map_record(registration, &query.class_iri, record, fetched_at_ms) {
                Some(row) if seen.insert(row.key.clone()) => rows.push(row),
                Some(_) => {}
                None => dropped += 1,
            }
        }
        if status != SourceOutcomeStatus::Ok {
            break;
        }
        match page.next_page_token {
            Some(token)
                if registration.pagination != eg_types::connector_pack::PackPagination::None =>
            {
                if pages >= query.max_pages_per_source {
                    status = SourceOutcomeStatus::Truncated;
                    reason = Some(format!(
                        "SOURCE_TRUNCATED: page bound {} reached",
                        query.max_pages_per_source
                    ));
                    break;
                }
                if page_token.as_deref() == Some(token.as_str()) {
                    status = SourceOutcomeStatus::Refused;
                    reason = Some("SOURCE_REFUSED: source repeated its page token".into());
                    break;
                }
                page_token = Some(token);
            }
            _ => break,
        }
    }
    if dropped > 0 && reason.is_none() {
        reason = Some(format!(
            "{dropped} records had no key field and were dropped"
        ));
    }
    let outcome = SourceOutcome {
        connector: registration.connector.clone(),
        mapping_iri: registration.mapping_iri.clone(),
        status,
        rows: rows.len() as u32,
        pages,
        reason,
    };
    SourceRead { rows, outcome }
}

/// Map one source record to one class row; `None` when it has no usable key.
fn map_record(
    registration: &PackSourceRegistration,
    class: &str,
    record: BTreeMap<String, Value>,
    fetched_at_ms: u64,
) -> Option<VirtualClassRow> {
    let key = match record.get(&registration.key_field)? {
        Value::String(s) if !s.is_empty() => s.clone(),
        Value::Number(n) => n.to_string(),
        _ => return None,
    };
    let mut properties = BTreeMap::new();
    for binding in &registration.fields {
        if let Some(value) = record.get(&binding.field) {
            properties.insert(binding.property_iri.clone(), value.clone());
        }
    }
    let class_iri = if class_matches(&registration.class_iri, class) {
        registration.class_iri.clone()
    } else {
        class.to_string()
    };
    Some(VirtualClassRow {
        class_iri,
        id: format!("{}/{key}", registration.connector),
        key,
        properties,
        provenance: VirtualRowProvenance {
            connector: registration.connector.clone(),
            contract_iri: registration.contract_iri.clone(),
            mapping_iri: registration.mapping_iri.clone(),
            resource_uri: registration.resource_uri.clone(),
            source_kind: registration.source_kind,
            contract_digest: registration.contract_digest.clone(),
            fetched_at_ms,
        },
    })
}

/// A class answer as a named foreign source. The registry builds one per caller, so
/// the caller and the authorizer are fixed for the source's life.
pub struct ClassFederationSource {
    pub class: String,
    pub sources: Vec<PackSourceRegistration>,
    pub caller: SourceCaller,
    pub executor: Option<Arc<dyn SourceExecutor>>,
    pub authorize: Arc<SourceAuthorizer>,
}

impl ClassFederationSource {
    /// Run the federated read with the default bounds.
    pub fn answer(&self) -> VirtualClassAnswer {
        federate_class(
            &self.sources,
            &VirtualClassQuery::new(self.class.clone()),
            &self.caller,
            self.executor.clone(),
            &*self.authorize,
        )
    }
}

/// The column name of a property: its IRI local name.
pub fn property_column(property_iri: &str) -> String {
    iri_local_name(property_iri).to_string()
}

impl ForeignSource for ClassFederationSource {
    /// Instance ids only (`<connector>/<key>`), for the id/score algebra.
    fn fetch(&self) -> Result<RowSet, String> {
        let answer = self.answer();
        Ok(RowSet::from_rows(
            answer.rows.into_iter().map(|row| (row.id, None)),
        ))
    }

    /// Instance rows with property and provenance columns, then one outcome row
    /// per source (id `outcome:<mapping>`) carrying `_outcome` and its reason.
    fn fetch_columns(&self) -> Result<ForeignRows, String> {
        let answer = self.answer();
        let mut out = Vec::with_capacity(answer.rows.len() + answer.sources.len());
        for row in answer.rows {
            let mut columns: BTreeMap<String, Value> = row
                .properties
                .into_iter()
                .map(|(iri, value)| (property_column(&iri), value))
                .collect();
            let p = row.provenance;
            for (name, value) in [
                ("_class_iri", Value::from(row.class_iri)),
                ("_key", Value::from(row.key)),
                ("_connector", Value::from(p.connector)),
                ("_contract_iri", Value::from(p.contract_iri)),
                ("_mapping_iri", Value::from(p.mapping_iri)),
                ("_resource_uri", Value::from(p.resource_uri)),
                ("_source_kind", Value::from(p.source_kind.as_str())),
                ("_contract_digest", Value::from(p.contract_digest)),
                ("_fetched_at_ms", Value::from(p.fetched_at_ms)),
            ] {
                columns.insert(name.to_string(), value);
            }
            out.push(ForeignRow {
                id: row.id,
                score: None,
                columns,
            });
        }
        for outcome in answer.sources {
            let mut columns = BTreeMap::new();
            columns.insert("_connector".into(), Value::from(outcome.connector));
            columns.insert(
                "_mapping_iri".into(),
                Value::from(outcome.mapping_iri.clone()),
            );
            columns.insert("_outcome".into(), Value::from(outcome.status.code()));
            columns.insert(
                "_outcome_reason".into(),
                outcome.reason.map(Value::from).unwrap_or(Value::Null),
            );
            columns.insert("_outcome_rows".into(), Value::from(outcome.rows));
            out.push(ForeignRow {
                id: format!("outcome:{}", outcome.mapping_iri),
                score: None,
                columns,
            });
        }
        Ok(ForeignRows::from_rows(out))
    }

    fn exposed_columns(&self) -> Option<BTreeSet<String>> {
        let mut exposed: BTreeSet<String> = self
            .sources
            .iter()
            .flat_map(|r| r.fields.iter().map(|b| property_column(&b.property_iri)))
            .collect();
        exposed.extend(PROVENANCE_COLUMNS.iter().map(|c| c.to_string()));
        exposed.extend(OUTCOME_COLUMNS.iter().map(|c| c.to_string()));
        Some(exposed)
    }
}
