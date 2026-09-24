//! Typed, tenant-bound reads of native WorkItem rows: `GetWorkItem` and
//! `ListWorkItems` (EH-219).
//!
//! A WorkItem row carries two kinds of state. The CALLER's view -- what was
//! submitted, where it is in its lifecycle, which revision of the row this is
//! -- is what these reads answer. The WORKER's authority -- the lease owner,
//! lease epoch and fencing token a worker presents to prove it still holds a
//! lease -- is never part of the view: handing it to a reader would let any
//! reader impersonate the lease holder on the next fenced transition.
//!
//! Both reads are bound to the verified request tenant by the server; a row of
//! another tenant is simply not visible, never an error that names it.
//!
//! # Paging
//!
//! `ListWorkItems` pages with the three-bound rule `AgentComponent.Search`
//! established: a page stops at whichever comes first of the caller's limit,
//! the rows-examined bound and the bytes-examined bound, and any early stop
//! returns an opaque tenant-bound cursor. WorkItems share their graph's node
//! table with every other node, so the scan bound applies to rows EXAMINED and
//! a page may be empty yet still carry a cursor. Callers loop until
//! `next_cursor` is `None`, not until a page is empty.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::native_control::MAX_SUBMIT_REF_BYTES;
use crate::tenant_cursor::CursorFamily;

/// Most WorkItems one `ListWorkItems` page may return.
pub const MAX_WORK_ITEM_LIST_LIMIT: u32 = 100;
/// Most node rows one page may examine. Rows of other node types count: they
/// are what makes an empty-but-resumable page possible.
pub const MAX_WORK_ITEM_LIST_SCAN: usize = 1_024;
/// Most stored row bytes one page may examine (the response-size bound).
pub const MAX_WORK_ITEM_LIST_BYTES: usize = 4 * 1024 * 1024;
/// Longest opaque cursor a caller may hand back.
pub const MAX_WORK_ITEM_LIST_CURSOR_BYTES: usize = 16 * 1024;
/// The row property the native WorkItem writer bumps on every row write. The
/// read side projects it as [`WorkItemView::version`].
pub const WORK_ITEM_ROW_REVISION: &str = "row_revision";

/// The WorkItem-row properties a terminal commit with a `TerminalOutcomeExtension`
/// binds its provenance under (graph-os EG-3). Written only by the native
/// commit, refused to generic writers, and read by `GetWorkItemOutcome`.
pub const WORK_ITEM_OUTCOME_REF: &str = "outcome_ref";
pub const WORK_ITEM_OUTCOME_DIGEST: &str = "outcome_digest";
pub const WORK_ITEM_TRACE_REF: &str = "trace_ref";
pub const WORK_ITEM_TOOL_CALL_REFS: &str = "tool_call_refs";
/// Every WorkItem-row property only the native kernel may write.
pub const NATIVE_WORK_ITEM_ROW_KEYS: [&str; 5] = [
    WORK_ITEM_ROW_REVISION,
    WORK_ITEM_OUTCOME_REF,
    WORK_ITEM_OUTCOME_DIGEST,
    WORK_ITEM_TRACE_REF,
    WORK_ITEM_TOOL_CALL_REFS,
];

/// Most metadata keys one `ListWorkItems` `metadata_match` may name.
pub const MAX_WORK_ITEM_METADATA_MATCH_KEYS: usize = 8;

/// `SubmitWorkItem`'s own bound on a WorkItem id, reused for the tenant.
const MAX_WORK_ITEM_ID_BYTES: usize = 512;

/// The `ListWorkItems` cursor family.
pub const WORK_ITEM_LIST_CURSOR: CursorFamily = CursorFamily {
    domain: b"eg/work-item-list-cursor/v1",
    max_bytes: MAX_WORK_ITEM_LIST_CURSOR_BYTES,
    noun: "WorkItem list",
};

/// Where a WorkItem is in its lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum WorkItemStatus {
    /// Admitted, waiting on dependencies.
    Submitted,
    /// Claimable.
    Ready,
    /// Claimed under a live lease, not yet started.
    Leased,
    /// Executing under a live lease.
    Running,
    Succeeded,
    Failed,
    Cancelled,
    /// A retryable failure that exhausted its attempts.
    DeadLetter,
}

/// The stored `status` text of each lifecycle state. A table rather than a
/// match so the wire enum and the row vocabulary are read off one list.
const STORED_STATUS: [(&str, WorkItemStatus); 8] = [
    ("submitted", WorkItemStatus::Submitted),
    ("ready", WorkItemStatus::Ready),
    ("leased", WorkItemStatus::Leased),
    ("running", WorkItemStatus::Running),
    ("succeeded", WorkItemStatus::Succeeded),
    ("failed", WorkItemStatus::Failed),
    ("cancelled", WorkItemStatus::Cancelled),
    ("dead_letter", WorkItemStatus::DeadLetter),
];

impl WorkItemStatus {
    /// The lifecycle state a stored row's `status` names, if it names one.
    pub fn from_stored(stored: &str) -> Option<Self> {
        stored_value(&STORED_STATUS, stored)
    }
}

/// The value a stored-text table maps `stored` to: the one lookup every
/// `(text, variant)` table of a stored row's closed vocabulary shares.
pub(crate) fn stored_value<T: Copy>(table: &[(&str, T)], stored: &str) -> Option<T> {
    table
        .iter()
        .find(|(text, _)| *text == stored)
        .map(|(_, value)| *value)
}

/// The caller's view of one WorkItem row.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct WorkItemView {
    pub work_item_id: String,
    pub kind: String,
    pub status: WorkItemStatus,
    /// The `input_ref` the item was submitted with.
    pub input_ref: String,
    /// The caller-owned scheduling metadata, as last written.
    pub metadata: Map<String, Value>,
    /// Row revision: 1 at submission, bumped by every native write of the row.
    /// A reader compares two views of the same item by it.
    pub version: u64,
    pub updated_at_ms: u64,
}

impl WorkItemView {
    /// Project one stored node row to the caller's view -- `None` when the
    /// row is not a WorkItem of `tenant`, which is how another tenant's item
    /// stays invisible rather than refused by name.
    pub fn from_tenant_row(
        work_item_id: &str,
        row: &Map<String, Value>,
        tenant: &str,
    ) -> Result<Option<Self>, String> {
        if text(row, "node_type") != "WorkItem" || text(row, "tenant") != tenant {
            return Ok(None);
        }
        let stored = text(row, "status");
        let status = WorkItemStatus::from_stored(stored).ok_or_else(|| {
            format!("WorkItem '{work_item_id}' carries an unrecognized status '{stored}'")
        })?;
        Ok(Some(Self {
            work_item_id: work_item_id.to_string(),
            kind: text(row, "kind").to_string(),
            status,
            input_ref: text(row, "payload_ref").to_string(),
            metadata: row
                .get("metadata")
                .and_then(Value::as_object)
                .cloned()
                .unwrap_or_default(),
            // A row written before the revision counter existed is revision 1.
            version: row
                .get(WORK_ITEM_ROW_REVISION)
                .and_then(Value::as_u64)
                .unwrap_or(0)
                .max(1),
            updated_at_ms: seconds_to_ms(row.get("updated_at").and_then(Value::as_f64)),
        }))
    }
}

fn text<'a>(row: &'a Map<String, Value>, key: &str) -> &'a str {
    row.get(key).and_then(Value::as_str).unwrap_or("")
}

/// Stored timestamps are float seconds; the view answers integer milliseconds.
fn seconds_to_ms(seconds: Option<f64>) -> u64 {
    // `as` saturates: a missing, negative or NaN timestamp reads as 0.
    (seconds.unwrap_or(0.0) * 1000.0).round() as u64
}

/// One page of `ListWorkItems`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct WorkItemPage {
    pub items: Vec<WorkItemView>,
    /// `Some` when more of the graph remains to be scanned; hand it back
    /// unmodified to continue. A page may be empty and still carry one.
    pub next_cursor: Option<String>,
}

/// `GetWorkItemOutcome`: a terminal WorkItem and the provenance its commit
/// bound (graph-os EG-3). `outcome` is the OutcomeEvaluation receipt's stored
/// properties, verified against the digest the commit recorded -- `None` when
/// the bundle committed without one (a `degraded` outcome).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct WorkItemOutcomeView {
    pub work_item: WorkItemView,
    pub trace_ref: String,
    pub tool_call_refs: Vec<String>,
    pub outcome_ref: String,
    pub outcome: Option<Map<String, Value>>,
}

/// The provenance references a native terminal commit bound to a WorkItem row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedOutcomeRefs {
    pub trace_ref: String,
    pub tool_call_refs: Vec<String>,
    pub outcome_ref: String,
    /// SHA-256 of the OutcomeEvaluation receipt's stored properties, when the
    /// bundle carried that receipt.
    pub outcome_digest: Option<String>,
}

impl CommittedOutcomeRefs {
    /// The references on `row`, `None` when it carries no committed bundle.
    pub fn from_row(row: &Map<String, Value>) -> Option<Self> {
        let outcome_ref = row.get(WORK_ITEM_OUTCOME_REF)?.as_str()?.to_string();
        let tool_call_refs = row
            .get(WORK_ITEM_TOOL_CALL_REFS)
            .and_then(Value::as_array)
            .map(|refs| {
                refs.iter()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default();
        Some(Self {
            trace_ref: text(row, WORK_ITEM_TRACE_REF).to_string(),
            tool_call_refs,
            outcome_ref,
            outcome_digest: row
                .get(WORK_ITEM_OUTCOME_DIGEST)
                .and_then(Value::as_str)
                .map(str::to_string),
        })
    }
}

/// Validate a `GetWorkItem` request's own fields.
pub fn validate_work_item_get(tenant: &str, work_item_id: &str) -> Result<(), String> {
    bounded("tenant", tenant, MAX_WORK_ITEM_ID_BYTES)?;
    bounded("work_item_id", work_item_id, MAX_WORK_ITEM_ID_BYTES)
}

/// A `ListWorkItems` request, assembled from the method's wire fields.
#[derive(Debug, Clone, PartialEq)]
pub struct WorkItemListRequest {
    pub tenant: String,
    pub cursor: Option<String>,
    pub limit: u32,
    pub kind: Option<String>,
    pub metadata_match: Option<Map<String, Value>>,
}

impl WorkItemListRequest {
    pub fn validate(&self) -> Result<(), String> {
        bounded("tenant", &self.tenant, MAX_WORK_ITEM_ID_BYTES)?;
        if let Some(kind) = &self.kind {
            bounded("kind", kind, MAX_SUBMIT_REF_BYTES)?;
        }
        self.validate_metadata_match()?;
        if self.limit == 0 || self.limit > MAX_WORK_ITEM_LIST_LIMIT {
            return Err(format!(
                "ListWorkItems limit must be 1..={MAX_WORK_ITEM_LIST_LIMIT}"
            ));
        }
        Ok(())
    }

    fn validate_metadata_match(&self) -> Result<(), String> {
        let Some(wanted) = &self.metadata_match else {
            return Ok(());
        };
        if wanted.is_empty() || wanted.len() > MAX_WORK_ITEM_METADATA_MATCH_KEYS {
            return Err(format!(
                "ListWorkItems metadata_match must name 1..={MAX_WORK_ITEM_METADATA_MATCH_KEYS} keys"
            ));
        }
        wanted
            .keys()
            .try_for_each(|key| bounded("metadata_match key", key, MAX_WORK_ITEM_ID_BYTES))
    }

    /// Whether one visible item passes the kind and metadata filters.
    pub fn admits(&self, view: &WorkItemView) -> bool {
        let kind_ok = self.kind.as_deref().is_none_or(|kind| view.kind == kind);
        let metadata_ok = self.metadata_match.as_ref().is_none_or(|wanted| {
            wanted
                .iter()
                .all(|(key, value)| view.metadata.get(key) == Some(value))
        });
        kind_ok && metadata_ok
    }

    /// The row key a cursor resumes strictly after, refused by name when it
    /// was minted for another tenant or is malformed.
    pub fn resume_after(&self) -> Result<Option<String>, String> {
        self.cursor
            .as_deref()
            .map(|cursor| WORK_ITEM_LIST_CURSOR.decode(&self.tenant, cursor, is_row_key))
            .transpose()
    }
}

fn is_row_key(key: &str) -> bool {
    !key.is_empty()
}

fn bounded(field: &str, value: &str, max_bytes: usize) -> Result<(), String> {
    if value.trim().is_empty() || value.len() > max_bytes {
        return Err(format!("WorkItem read {field} is outside native bounds"));
    }
    Ok(())
}

/// One `ListWorkItems` page under construction, fed rows in key order.
///
/// Every bound is checked BEFORE a row is consumed, never after, so the
/// cursor always resumes strictly after a row the caller has already been
/// shown (or skipped as not theirs).
pub struct WorkItemPageScan<'r> {
    request: &'r WorkItemListRequest,
    items: Vec<WorkItemView>,
    scanned: usize,
    bytes: usize,
    last_consumed: Option<String>,
    truncated: bool,
}

impl<'r> WorkItemPageScan<'r> {
    pub fn new(request: &'r WorkItemListRequest) -> Self {
        Self {
            request,
            items: Vec::new(),
            scanned: 0,
            bytes: 0,
            last_consumed: None,
            truncated: false,
        }
    }

    /// Whether the page may examine one more row. `false` closes the page:
    /// the caller stops scanning and the page will carry a cursor.
    pub fn admits_another_row(&mut self) -> bool {
        let full = self.items.len() >= self.request.limit as usize
            || self.scanned >= MAX_WORK_ITEM_LIST_SCAN
            || (self.scanned > 0 && self.bytes >= MAX_WORK_ITEM_LIST_BYTES);
        self.truncated |= full;
        !full
    }

    /// Consume one examined row of `row_bytes` stored bytes.
    pub fn consume(
        &mut self,
        row_id: &str,
        row_bytes: usize,
        row: &Map<String, Value>,
    ) -> Result<(), String> {
        self.scanned += 1;
        self.bytes = self.bytes.saturating_add(row_bytes);
        let view = WorkItemView::from_tenant_row(row_id, row, &self.request.tenant)?;
        if let Some(view) = view.filter(|view| self.request.admits(view)) {
            self.items.push(view);
        }
        self.last_consumed = Some(row_id.to_string());
        Ok(())
    }

    /// Close the page, minting a cursor when a bound stopped it early.
    pub fn finish(self) -> WorkItemPage {
        let next_cursor = self
            .last_consumed
            .filter(|_| self.truncated)
            .map(|row_id| WORK_ITEM_LIST_CURSOR.encode(&self.request.tenant, &row_id));
        WorkItemPage {
            items: self.items,
            next_cursor,
        }
    }
}

#[cfg(test)]
mod tests;
