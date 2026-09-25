//! The graph-driven work market (EH-348): the ONE canonical `:Gap`, written
//! only through typed methods and always paired with its one native WorkItem,
//! plus the versioned `WorkOffer` derived from it.
//!
//! # What EG owns here
//!
//! * **One Gap per `(tenant, gap_id)`.** Every discovery track -- a failure
//!   cluster, an evaluation regression, a code audit, research relevance --
//!   folds its evidence into the same record through `GapUpsert`. The row is
//!   keyed by a digest of the tenant and the canonical id, so two tenants that
//!   derive the same `gap:<source>:<signature>` id never meet.
//! * **A Gap is never schedulable without its WorkItem.** `GapUpsert` creates
//!   the Gap and admits its native WorkItem in the same durable transaction,
//!   or commits neither. A resolved or deferred Gap reopens -- a new generation
//!   and a new WorkItem -- only on evidence it has not seen before; re-sending
//!   old evidence is `unchanged`, so a cooldown cannot be bypassed by
//!   repetition.
//! * **Outcome evidence is engine-derived.** `GapSettle` reads the Gap's
//!   current WorkItem row inside the transaction and records its terminal
//!   state as evidence; a caller cannot claim an outcome.
//! * **An offer is derived state, not a task.** `WorkOfferPut` records the
//!   pricing inputs on the Gap (compare-and-set on its version) and the engine
//!   computes the deterministic utility rate. Selecting among offers is the
//!   `Decide` layer's job ([`selection`]); claiming stays with the native
//!   `ClaimWorkItem` fence. Nothing here is a second queue or scheduler.
//!
//! No float and no clock: severities, probabilities and money are fixed-point
//! integers, and every timestamp is the committing batch's authoritative time.

pub mod gap;
pub mod lifecycle;
pub mod list;
pub mod offer;
pub mod selection;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

pub use gap::{GapEvidence, GapEvidenceInput, GapUpsertRequest, GapView, GapWorkSpec};
pub use lifecycle::{
    GapSettleOutcome, GapSettleRequest, GapSettled, GapTransitionOutcome, GapTransitionRequest,
    GapTransitionTarget, GapTransitioned, GapUpsertOutcome, GapUpserted, WorkOfferPutOutcome,
    WorkOfferPutRequest, WorkOfferRecorded,
};
pub use list::{GapListRequest, GapPage};
pub use offer::{work_offer_utility_rate, BlastRadius, WorkOffer, WorkOfferView};

/// `node_type` of a canonical Gap row.
pub const GAP_NODE_TYPE: &str = "Gap";
/// Row key prefix of a canonical Gap.
const GAP_ROW_KEY_PREFIX: &str = "gap-row:";
/// Row key prefix of a Gap's WorkItem.
const GAP_WORK_ITEM_PREFIX: &str = "work-item:gap:";
/// Bound on every identifier, kind and reference the market carries.
pub const MAX_MARKET_REF_BYTES: usize = 512;
/// Largest fixed-point probability or severity (1.0 in parts per million).
pub const PPM_SCALE: u32 = 1_000_000;

/// Where a Gap is in its lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum GapStatus {
    /// Discovered and unaddressed; its WorkItem is schedulable.
    Open,
    /// A specification is in flight.
    Specified,
    /// Closed on evidence.
    Resolved,
    /// Parked: its last attempt ended without closing it.
    Deferred,
}

impl GapStatus {
    /// Whether new work may be priced or run against the Gap.
    pub fn is_live(self) -> bool {
        matches!(self, Self::Open | Self::Specified)
    }
}

/// The shared 0 (critical) ..= 3 (background) priority bucket of a severity.
/// A table so the thresholds are read off one list.
const SEVERITY_BUCKETS: [(u32, u8); 3] = [(850_000, 0), (600_000, 1), (300_000, 2)];

/// The WorkItem priority bucket a Gap of this severity schedules under.
pub fn severity_bucket(severity_ppm: u32) -> u8 {
    SEVERITY_BUCKETS
        .iter()
        .find(|(floor, _)| severity_ppm >= *floor)
        .map_or(3, |(_, bucket)| *bucket)
}

/// Hex SHA-256 over `domain` and each length-prefixed part: the one digest
/// every market identity (row key, WorkItem id, evidence, command) is cut from.
pub fn scoped_digest(domain: &[u8], parts: &[&[u8]]) -> String {
    scoped_digest_parts(domain, parts.iter().copied())
}

/// The same digest over borrowed parts supplied by any iterator. This lets
/// callers with stack arrays keep their existing byte order without copying.
pub fn scoped_digest_parts<'a>(domain: &[u8], parts: impl IntoIterator<Item = &'a [u8]>) -> String {
    use sha2::{Digest, Sha256};
    let mut digest = Sha256::new();
    digest.update(domain);
    for part in parts {
        digest.update((part.len() as u64).to_be_bytes());
        digest.update(part);
    }
    hex::encode(digest.finalize())
}

/// The node key of `tenant`'s Gap `gap_id`.
pub fn gap_row_key(tenant: &str, gap_id: &str) -> String {
    let digest = scoped_digest(b"eg/gap-row/v1", &[tenant.as_bytes(), gap_id.as_bytes()]);
    format!("{GAP_ROW_KEY_PREFIX}{digest}")
}

/// The id of generation `generation`'s WorkItem of `tenant`'s Gap `gap_id`.
pub fn gap_work_item_id(tenant: &str, gap_id: &str, generation: u32) -> String {
    let digest = scoped_digest(
        b"eg/gap-work-item/v1",
        &[
            tenant.as_bytes(),
            gap_id.as_bytes(),
            &generation.to_be_bytes(),
        ],
    );
    format!("{GAP_WORK_ITEM_PREFIX}{digest}")
}

/// `sha256:<hex>` over the given parts, the market's evidence digest shape.
pub fn evidence_digest(parts: &[&str]) -> String {
    let bytes: Vec<&[u8]> = parts.iter().map(|part| part.as_bytes()).collect();
    format!("sha256:{}", scoped_digest(b"eg/gap-evidence/v1", &bytes))
}

/// Refuse an empty or oversized identifier by field name.
pub(crate) fn bounded(field: &str, value: &str) -> Result<(), String> {
    if value.trim().is_empty() || value.len() > MAX_MARKET_REF_BYTES {
        return Err(format!("work market {field} is outside native bounds"));
    }
    Ok(())
}

/// Refuse a list longer than `max` or holding an out-of-bounds entry.
pub(crate) fn bounded_list(field: &str, values: &[String], max: usize) -> Result<(), String> {
    if values.len() > max {
        return Err(format!("work market {field} holds more than {max} entries"));
    }
    values.iter().try_for_each(|value| bounded(field, value))
}

/// Refuse a fixed-point fraction above 1.0.
pub(crate) fn bounded_ppm(field: &str, value: u32) -> Result<(), String> {
    if value > PPM_SCALE {
        return Err(format!("work market {field} exceeds {PPM_SCALE} ppm"));
    }
    Ok(())
}

/// Whether a stored node row is a Gap of any tenant. The generic row guard
/// consults the label; this is the typed readers' test.
pub fn is_gap_row(row: &Map<String, Value>) -> bool {
    row.get("node_type").and_then(Value::as_str) == Some(GAP_NODE_TYPE)
}

/// Whether a stored node row is a Gap of `tenant`.
pub fn is_tenant_gap(row: &Map<String, Value>, tenant: &str) -> bool {
    is_gap_row(row) && row.get("tenant").and_then(Value::as_str) == Some(tenant)
}

/// The tenant and idempotency key a work-market write names in its body, or
/// `None` for any other method. One owner for the kernel's tenant binding,
/// batch identity and audit.
pub fn market_write_scope(method: &crate::protocol::Method) -> Option<(&str, &str)> {
    use crate::protocol::Method;
    match method {
        Method::GapUpsert { request } => {
            Some((request.tenant.as_str(), request.idempotency_key.as_str()))
        }
        Method::GapTransition { request } => {
            Some((request.tenant.as_str(), request.idempotency_key.as_str()))
        }
        Method::GapSettle { request } => {
            Some((request.tenant.as_str(), request.idempotency_key.as_str()))
        }
        Method::WorkOfferPut { request } => {
            Some((request.tenant.as_str(), request.idempotency_key.as_str()))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests;
