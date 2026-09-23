//! `GapList`: one bounded, tenant-bound page of Gaps, by the same three-bound
//! keyset rule as `ListWorkItems`. Pages come back in row-key order: this is a
//! listing, never a ranking -- ranking offers is the `Decide` layer's.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use super::gap::GapView;
use super::{bounded, is_tenant_gap, GapStatus};
use crate::keyset_page::KeysetListing;
use crate::tenant_cursor::CursorFamily;

/// Most Gaps one page may return.
pub const MAX_GAP_LIST_LIMIT: u32 = 100;
/// The `GapList` cursor family.
pub const GAP_LIST_CURSOR: CursorFamily = CursorFamily {
    domain: b"eg/gap-list-cursor/v1",
    max_bytes: 16 * 1024,
    noun: "gap list",
};

/// `GapList`: `tenant`'s Gaps, optionally of one status and source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct GapListRequest {
    /// Must equal the verified request tenant.
    pub tenant: String,
    #[serde(default)]
    pub status: Option<GapStatus>,
    #[serde(default)]
    pub source: Option<String>,
    #[serde(default)]
    pub cursor: Option<String>,
    pub limit: u32,
}

/// One page of `GapList`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct GapPage {
    pub gaps: Vec<GapView>,
    /// `Some` when more remains to be scanned; a page may be empty and still
    /// carry one.
    pub next_cursor: Option<String>,
}

impl GapListRequest {
    pub fn validate(&self) -> Result<(), String> {
        bounded("tenant", &self.tenant)?;
        if let Some(source) = &self.source {
            bounded("source", source)?;
        }
        if self.limit == 0 || self.limit > MAX_GAP_LIST_LIMIT {
            return Err(format!("GapList limit must be 1..={MAX_GAP_LIST_LIMIT}"));
        }
        Ok(())
    }

    /// The row key the cursor resumes strictly after.
    pub fn resume_after(&self) -> Result<Option<String>, String> {
        self.cursor
            .as_deref()
            .map(|cursor| GAP_LIST_CURSOR.decode(&self.tenant, cursor, |key| !key.is_empty()))
            .transpose()
    }

    fn admits(&self, gap: &GapView) -> bool {
        self.status.is_none_or(|status| gap.status == status)
            && self
                .source
                .as_deref()
                .is_none_or(|source| gap.source == source)
    }
}

impl KeysetListing for GapListRequest {
    type Item = GapView;
    const CURSOR: CursorFamily = GAP_LIST_CURSOR;

    fn tenant(&self) -> &str {
        &self.tenant
    }

    fn limit(&self) -> usize {
        self.limit as usize
    }

    fn select(&self, _row_id: &str, row: &Map<String, Value>) -> Result<Option<GapView>, String> {
        if !is_tenant_gap(row, &self.tenant) {
            return Ok(None);
        }
        let gap = GapView::from_row(row)?;
        Ok(Some(gap).filter(|gap| self.admits(gap)))
    }
}
