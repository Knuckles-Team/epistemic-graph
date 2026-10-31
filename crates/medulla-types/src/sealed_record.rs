//! Sealed record classes (EH-558): content-addressed records stored as ordinary
//! graph nodes whose content must never change after they are written.
//!
//! A sealed record carries its own seal (a digest over its content, usually also
//! its node id). Re-sealing on read makes such a record tamper-EVIDENT; this module
//! makes it tamper-PROOF:
//!
//! * **Generic writes are create-only.** Every generic node write — single, batched,
//!   compare-and-set, staged — that would change or remove a stored sealed row is
//!   refused, in every engine mode. Creating a record is allowed, and so is
//!   re-writing identical content, which keeps retries and create-if-absent
//!   idempotent.
//! * **Retirement is the owning op.** [`RetireSealedRecordRequest`] is the only way to
//!   remove a sealed record's content. It needs the record's digest and replaces the
//!   row, in one durable, audited, idempotent native transaction, with a
//!   [`SEALED_TOMBSTONE_TYPE`] row naming the record's class, digest, who retired it,
//!   when and why. The tombstone is itself sealed and cannot be created by a generic
//!   write, so the id can never again hold a forged record.
//! * **Expiry is the same op.** A retention policy retires a record whose
//!   `sealed_at_property` is older than the class's configured retention, through the
//!   same request, attributed to the engine.
//!
//! A whole-graph clear or delete still removes sealed rows with the graph.
//!
//! A row's class is its `type` property, the label convention every scan uses.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// `type` of the row a retirement leaves in place of a sealed record.
pub const SEALED_TOMBSTONE_TYPE: &str = "SealedRecordTombstone";
/// Bound on the tenant, node id, digest, idempotency key and actor reference.
const MAX_SEALED_REF_BYTES: usize = 512;
/// Bound on a retirement reason.
const MAX_SEALED_REASON_BYTES: usize = 2_048;

/// One sealed record class.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SealedClass {
    /// The row's `type`.
    pub name: &'static str,
    /// The property holding the record's seal; retirement must present it.
    pub digest_property: &'static str,
    /// The epoch-milliseconds property a retention policy ages the record by. A row
    /// without it is never expired.
    pub sealed_at_property: &'static str,
    /// Whether a generic write may create a row of this class. Tombstones are
    /// written only by retirement.
    pub generic_create: bool,
}

/// The sealed classes. To seal a class, add it here.
///
/// * `AnalysisSnapshot` — a sealed markets analysis record (finance-v1); its node id
///   is derived from its digest and share links verify it by re-sealing.
/// * [`SEALED_TOMBSTONE_TYPE`] — what retirement leaves behind.
pub const SEALED_RECORD_CLASSES: &[SealedClass] = &[
    SealedClass {
        name: "AnalysisSnapshot",
        digest_property: "analysisDigest",
        sealed_at_property: "sealedAtMs",
        generic_create: true,
    },
    SealedClass {
        name: SEALED_TOMBSTONE_TYPE,
        digest_property: "digest",
        sealed_at_property: "retired_at_ms",
        generic_create: false,
    },
];

/// The sealed class named `class`, if any.
pub fn sealed_class(class: &str) -> Option<&'static SealedClass> {
    SEALED_RECORD_CLASSES
        .iter()
        .find(|sealed| sealed.name == class)
}

/// Whether `class` (a row's `type` value) is a sealed record class.
pub fn is_sealed_record_class(class: &str) -> bool {
    sealed_class(class).is_some()
}

/// The sealed class of a stored row, or `None` for an ordinary row.
pub fn sealed_record_class(row: &Map<String, Value>) -> Option<&'static SealedClass> {
    row.get("type")
        .and_then(Value::as_str)
        .and_then(sealed_class)
}

/// Whether a stored row belongs to a sealed record class.
pub fn is_sealed_record_row(row: &Map<String, Value>) -> bool {
    sealed_record_class(row).is_some()
}

/// `RetireSealedRecord`: replace one sealed record with its audited tombstone.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RetireSealedRecordRequest {
    /// Must equal the verified request tenant.
    pub tenant: String,
    pub node_id: String,
    /// The record's seal, as its class's digest property stores it.
    pub digest: String,
    /// Why the record is retired (for example `share revoked` or `expired`).
    pub reason: String,
    /// When the retirement takes effect, epoch milliseconds. Part of the retry
    /// identity, so a retry carries the same value.
    pub retired_at_ms: u64,
    /// Caller-stable retry identity for this retirement.
    pub idempotency_key: String,
    /// Who retired it. The engine stamps this from the verified caller (a
    /// principal fingerprint) before the commit; a client-supplied value is replaced.
    #[serde(default)]
    pub retired_by: String,
}

/// How a retirement resolved.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum SealedRecordRetireOutcome {
    /// The record was replaced by its tombstone.
    Retired,
    /// The id already holds the tombstone of a record with this digest.
    AlreadyRetired,
    /// No row with this id exists.
    NotFound,
    /// The row is not a sealed record; nothing was written.
    NotSealed,
    /// The presented digest is not the record's seal; nothing was written.
    DigestMismatch,
}

/// The audited tombstone a retirement leaves in place of a sealed record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SealedRecordTombstone {
    pub node_id: String,
    pub record_class: String,
    pub digest: String,
    pub retired_by: String,
    pub retired_at_ms: u64,
    pub reason: String,
}

/// Result of `RetireSealedRecord`. `changed_work_item_ids` names the row the commit
/// wrote, so the serving projection refreshes from authority exactly as it does for
/// a WorkItem transition.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SealedRecordRetirement {
    pub outcome: SealedRecordRetireOutcome,
    pub tombstone: Option<SealedRecordTombstone>,
    pub changed_work_item_ids: Vec<String>,
}

fn bounded(field: &str, value: &str, max: usize) -> Result<(), String> {
    if value.trim().is_empty() || value.len() > max {
        return Err(format!("sealed record {field} is outside native bounds"));
    }
    Ok(())
}

impl RetireSealedRecordRequest {
    /// Field bounds; `retired_by` must already be stamped.
    pub fn validate(&self) -> Result<(), String> {
        for (field, value) in [
            ("tenant", &self.tenant),
            ("node_id", &self.node_id),
            ("digest", &self.digest),
            ("idempotency_key", &self.idempotency_key),
            ("retired_by", &self.retired_by),
        ] {
            bounded(field, value, MAX_SEALED_REF_BYTES)?;
        }
        bounded("reason", &self.reason, MAX_SEALED_REASON_BYTES)?;
        if self.retired_at_ms == 0 {
            return Err("sealed record retired_at_ms must be positive".to_string());
        }
        Ok(())
    }

    /// Resolve this retirement against the stored row: the outcome, and the tombstone
    /// row to write when the outcome is `Retired`.
    pub fn resolve(&self, stored: Option<&Map<String, Value>>) -> RetireResolution {
        let Some(row) = stored else {
            return RetireResolution::unchanged(SealedRecordRetireOutcome::NotFound, None);
        };
        let Some(class) = sealed_record_class(row) else {
            return RetireResolution::unchanged(SealedRecordRetireOutcome::NotSealed, None);
        };
        let sealed_digest = row.get(class.digest_property).and_then(Value::as_str);
        if sealed_digest != Some(self.digest.as_str()) {
            return RetireResolution::unchanged(SealedRecordRetireOutcome::DigestMismatch, None);
        }
        if class.name == SEALED_TOMBSTONE_TYPE {
            let existing = SealedRecordTombstone::from_row(&self.node_id, row);
            return RetireResolution::unchanged(
                SealedRecordRetireOutcome::AlreadyRetired,
                existing,
            );
        }
        let tombstone = SealedRecordTombstone {
            node_id: self.node_id.clone(),
            record_class: class.name.to_string(),
            digest: self.digest.clone(),
            retired_by: self.retired_by.clone(),
            retired_at_ms: self.retired_at_ms,
            reason: self.reason.clone(),
        };
        RetireResolution {
            outcome: SealedRecordRetireOutcome::Retired,
            write: Some(tombstone.row()),
            tombstone: Some(tombstone),
        }
    }
}

/// What a retirement does to the stored row.
#[derive(Debug, Clone, PartialEq)]
pub struct RetireResolution {
    pub outcome: SealedRecordRetireOutcome,
    /// The tombstone row to write in place of the record; `None` writes nothing.
    pub write: Option<Map<String, Value>>,
    pub tombstone: Option<SealedRecordTombstone>,
}

impl RetireResolution {
    fn unchanged(
        outcome: SealedRecordRetireOutcome,
        tombstone: Option<SealedRecordTombstone>,
    ) -> Self {
        Self {
            outcome,
            write: None,
            tombstone,
        }
    }

    /// The result reported to the caller.
    pub fn result(&self) -> SealedRecordRetirement {
        let changed = match (&self.write, &self.tombstone) {
            (Some(_), Some(tombstone)) => vec![tombstone.node_id.clone()],
            _ => Vec::new(),
        };
        SealedRecordRetirement {
            outcome: self.outcome,
            tombstone: self.tombstone.clone(),
            changed_work_item_ids: changed,
        }
    }
}

impl SealedRecordTombstone {
    /// The stored tombstone row.
    pub fn row(&self) -> Map<String, Value> {
        let mut row = Map::new();
        row.insert("type".into(), SEALED_TOMBSTONE_TYPE.into());
        row.insert("record_class".into(), self.record_class.clone().into());
        row.insert("digest".into(), self.digest.clone().into());
        row.insert("retired_by".into(), self.retired_by.clone().into());
        row.insert("retired_at_ms".into(), self.retired_at_ms.into());
        row.insert("reason".into(), self.reason.clone().into());
        row
    }

    fn from_row(node_id: &str, row: &Map<String, Value>) -> Option<Self> {
        let text = |key: &str| row.get(key).and_then(Value::as_str).map(str::to_string);
        Some(Self {
            node_id: node_id.to_string(),
            record_class: text("record_class")?,
            digest: text("digest")?,
            retired_by: text("retired_by")?,
            retired_at_ms: row.get("retired_at_ms").and_then(Value::as_u64)?,
            reason: text("reason")?,
        })
    }
}

/// The instant a sealed row expires under `retention_ms`, or `None` when the row is
/// not a retirable sealed record or carries no sealed-at time.
pub fn expires_at_ms(row: &Map<String, Value>, retention_ms: u64) -> Option<u64> {
    let class = sealed_record_class(row).filter(|class| class.name != SEALED_TOMBSTONE_TYPE)?;
    let sealed_at = row.get(class.sealed_at_property).and_then(Value::as_u64)?;
    Some(sealed_at.saturating_add(retention_ms))
}

#[cfg(test)]
mod tests;
