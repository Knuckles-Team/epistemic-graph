//! The canonical Gap record, the upsert request that feeds it, and the pure
//! merge rule every upsert applies. The store only loads, writes and admits
//! the WorkItem; what an upsert DOES to a Gap is decided here.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use super::offer::WorkOfferView;
use super::{bounded, bounded_list, bounded_ppm, severity_bucket, GapStatus, GAP_NODE_TYPE};
use crate::work_item_read::WORK_ITEM_ROW_REVISION;

/// Most evidence entries one upsert may carry.
pub const MAX_UPSERT_EVIDENCE: usize = 16;
/// Most evidence entries a Gap retains (oldest dropped first).
pub const MAX_GAP_EVIDENCE: usize = 128;
/// Most concept ids a Gap cites.
pub const MAX_GAP_CONCEPTS: usize = 32;
/// Most specification references a Gap retains.
pub const MAX_GAP_SPEC_REFS: usize = 16;
/// Largest statement a Gap carries.
pub const MAX_GAP_STATEMENT_BYTES: usize = 4 * 1024;
/// Most attempts a Gap's WorkItem may make (`SubmitWorkItem`'s own bound).
const MAX_GAP_WORK_ATTEMPTS: u64 = 4096;

/// One evidence signal a caller submits.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct GapEvidenceInput {
    /// `sha256:<64 hex>` of the evidence; the dedupe identity.
    pub digest: String,
    /// The signal family, e.g. `failure_cluster`, `code_audit`, `research`.
    pub kind: String,
    /// Where the evidence lives (a node id, a run id, a report ref).
    pub reference: String,
}

/// One evidence entry as the Gap records it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct GapEvidence {
    pub digest: String,
    pub kind: String,
    pub reference: String,
    /// The generation the evidence arrived in.
    pub generation: u32,
    pub recorded_at_ms: u64,
}

/// The WorkItem a Gap schedules as.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct GapWorkSpec {
    /// The WorkItem kind (and queue), e.g. `gap_remediation`.
    pub kind: String,
    pub max_attempts: u64,
}

/// `GapUpsert`: fold evidence into one canonical Gap and ensure its WorkItem.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct GapUpsertRequest {
    /// Must equal the verified request tenant.
    pub tenant: String,
    /// The canonical `gap:<source>:<signature>` id.
    pub gap_id: String,
    pub source: String,
    pub signature: String,
    pub statement: String,
    #[serde(default)]
    pub domain: String,
    /// Severity in parts per million (0 ..= 1_000_000).
    pub severity_ppm: u32,
    #[serde(default)]
    pub concept_ids: Vec<String>,
    /// 1 ..= 16 signals.
    pub evidence: Vec<GapEvidenceInput>,
    pub work: GapWorkSpec,
    /// Caller-stable retry identity for this upsert.
    pub idempotency_key: String,
}

/// The caller's view of one Gap.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct GapView {
    pub gap_id: String,
    pub source: String,
    pub signature: String,
    pub statement: String,
    pub domain: String,
    pub severity_ppm: u32,
    /// The WorkItem priority bucket, 0 (critical) ..= 3 (background).
    pub priority_bucket: u8,
    pub status: GapStatus,
    /// 1 at creation; +1 each time new evidence reopens a closed Gap.
    pub generation: u32,
    /// The current generation's native WorkItem.
    pub work_item_id: String,
    pub work: GapWorkSpec,
    /// The most recent evidence, oldest first.
    pub evidence: Vec<GapEvidence>,
    /// Every evidence entry ever recorded, including those no longer retained.
    pub evidence_count: u64,
    pub concept_ids: Vec<String>,
    pub spec_refs: Vec<String>,
    /// The current generation's derived offer, if one was priced.
    pub offer: Option<WorkOfferView>,
    /// The last offer version ever recorded on this Gap (0 = never priced).
    /// Monotonic across generations, so an offer compare-and-set can never
    /// confuse two generations' offers.
    pub offer_version: u64,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    /// Row revision: 1 at creation, bumped by every native write.
    pub revision: u64,
}

/// What an upsert does to an existing Gap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GapMerge {
    /// No evidence the Gap had not already seen: nothing is written.
    Unchanged,
    /// New evidence folded into a live Gap.
    Merged,
    /// New evidence reopened a resolved or deferred Gap as a new generation;
    /// the caller must admit the generation's WorkItem.
    Reopened,
}

fn valid_digest(digest: &str) -> bool {
    digest
        .strip_prefix("sha256:")
        .is_some_and(|hex| hex.len() == 64 && hex.bytes().all(|b| b.is_ascii_hexdigit()))
}

impl GapEvidenceInput {
    fn validate(&self) -> Result<(), String> {
        if !valid_digest(&self.digest) {
            return Err("gap evidence digest must be sha256:<64 hex>".to_string());
        }
        bounded("evidence kind", &self.kind)?;
        bounded("evidence reference", &self.reference)
    }

    fn recorded(&self, generation: u32, now_ms: u64) -> GapEvidence {
        GapEvidence {
            digest: self.digest.clone(),
            kind: self.kind.clone(),
            reference: self.reference.clone(),
            generation,
            recorded_at_ms: now_ms,
        }
    }
}

impl GapWorkSpec {
    fn validate(&self) -> Result<(), String> {
        bounded("work kind", &self.kind)?;
        if self.max_attempts == 0 || self.max_attempts > MAX_GAP_WORK_ATTEMPTS {
            return Err(format!(
                "work market max_attempts must be 1..={MAX_GAP_WORK_ATTEMPTS}"
            ));
        }
        Ok(())
    }
}

impl GapUpsertRequest {
    pub fn validate(&self) -> Result<(), String> {
        for (field, value) in [
            ("tenant", &self.tenant),
            ("gap_id", &self.gap_id),
            ("source", &self.source),
            ("signature", &self.signature),
            ("idempotency_key", &self.idempotency_key),
        ] {
            bounded(field, value)?;
        }
        if self.statement.trim().is_empty() || self.statement.len() > MAX_GAP_STATEMENT_BYTES {
            return Err("work market statement is outside native bounds".to_string());
        }
        if self.domain.len() > super::MAX_MARKET_REF_BYTES {
            return Err("work market domain is outside native bounds".to_string());
        }
        bounded_ppm("severity_ppm", self.severity_ppm)?;
        bounded_list("concept_ids", &self.concept_ids, MAX_GAP_CONCEPTS)?;
        if self.evidence.is_empty() || self.evidence.len() > MAX_UPSERT_EVIDENCE {
            return Err(format!(
                "GapUpsert carries 1..={MAX_UPSERT_EVIDENCE} evidence entries"
            ));
        }
        self.evidence
            .iter()
            .try_for_each(GapEvidenceInput::validate)?;
        self.work.validate()
    }

    /// The first generation of a Gap no row holds yet.
    pub fn fresh_gap(&self, work_item_id: String, now_ms: u64) -> GapView {
        let mut gap = GapView {
            gap_id: self.gap_id.clone(),
            source: self.source.clone(),
            signature: self.signature.clone(),
            statement: self.statement.clone(),
            domain: self.domain.clone(),
            severity_ppm: self.severity_ppm,
            priority_bucket: severity_bucket(self.severity_ppm),
            status: GapStatus::Open,
            generation: 1,
            work_item_id,
            work: self.work.clone(),
            evidence: Vec::new(),
            evidence_count: 0,
            concept_ids: Vec::new(),
            spec_refs: Vec::new(),
            offer: None,
            offer_version: 0,
            created_at_ms: now_ms,
            updated_at_ms: now_ms,
            revision: 0,
        };
        gap.absorb(self, now_ms);
        gap
    }
}

impl GapView {
    /// Whether the Gap already recorded evidence with this digest.
    pub fn has_evidence(&self, digest: &str) -> bool {
        self.evidence.iter().any(|entry| entry.digest == digest)
    }

    /// Record one evidence entry, dropping the oldest past the retention bound.
    pub fn record_evidence(&mut self, entry: GapEvidence) {
        self.evidence.push(entry);
        self.evidence_count = self.evidence_count.saturating_add(1);
        if self.evidence.len() > MAX_GAP_EVIDENCE {
            self.evidence.remove(0);
        }
    }

    /// Fold an upsert's NEW evidence and concepts in; returns how many entries
    /// were new.
    fn absorb(&mut self, request: &GapUpsertRequest, now_ms: u64) -> usize {
        let fresh: Vec<GapEvidence> = request
            .evidence
            .iter()
            .filter(|input| !self.has_evidence(&input.digest))
            .map(|input| input.recorded(self.generation, now_ms))
            .collect();
        let added = fresh.len();
        fresh
            .into_iter()
            .for_each(|entry| self.record_evidence(entry));
        for concept in &request.concept_ids {
            if !self.concept_ids.contains(concept) && self.concept_ids.len() < MAX_GAP_CONCEPTS {
                self.concept_ids.push(concept.clone());
            }
        }
        self.severity_ppm = self.severity_ppm.max(request.severity_ppm);
        self.priority_bucket = severity_bucket(self.severity_ppm);
        added
    }

    /// Apply an upsert to an existing Gap. Only evidence the Gap has not seen
    /// changes anything; on a closed Gap it opens the next generation, whose
    /// WorkItem id the caller supplies.
    pub fn merge(
        &mut self,
        request: &GapUpsertRequest,
        next_work_item_id: impl FnOnce(u32) -> String,
        now_ms: u64,
    ) -> GapMerge {
        let novel = request
            .evidence
            .iter()
            .any(|input| !self.has_evidence(&input.digest));
        if !novel {
            return GapMerge::Unchanged;
        }
        let reopened = !self.status.is_live();
        if reopened {
            self.generation = self.generation.saturating_add(1);
            self.work_item_id = next_work_item_id(self.generation);
            self.work = request.work.clone();
            self.status = GapStatus::Open;
            self.offer = None;
        }
        self.absorb(request, now_ms);
        self.updated_at_ms = now_ms;
        if reopened {
            GapMerge::Reopened
        } else {
            GapMerge::Merged
        }
    }

    /// The stored row of this Gap for `tenant` (before the row writer stamps
    /// its revision).
    pub fn to_row(&self, tenant: &str, row: &mut Map<String, Value>) -> Result<(), String> {
        row.insert("node_type".into(), GAP_NODE_TYPE.into());
        row.insert("tenant".into(), tenant.into());
        row.insert("gap_id".into(), self.gap_id.clone().into());
        row.insert(
            "gap".into(),
            serde_json::to_value(self).map_err(|error| error.to_string())?,
        );
        Ok(())
    }

    /// Project a stored Gap row. The caller has already checked the row is a
    /// Gap of the requesting tenant.
    pub fn from_row(row: &Map<String, Value>) -> Result<Self, String> {
        let body = row
            .get("gap")
            .cloned()
            .ok_or_else(|| "stored Gap row carries no body".to_string())?;
        let mut gap: Self = serde_json::from_value(body)
            .map_err(|error| format!("stored Gap row is not a Gap: {error}"))?;
        gap.revision = row
            .get(WORK_ITEM_ROW_REVISION)
            .and_then(Value::as_u64)
            .unwrap_or(0)
            .max(1);
        Ok(gap)
    }
}
