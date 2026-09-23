//! The three-bound keyset page every tenant-bound native listing shares
//! (`ListWorkItems`, `ListControlLeases`).
//!
//! A page stops at whichever comes first of the caller's limit, the
//! rows-examined bound and the bytes-examined bound, and any early stop mints
//! an opaque tenant-bound cursor (see [`crate::tenant_cursor`]). Every bound is
//! checked BEFORE a row is consumed, never after, so the cursor always resumes
//! strictly after a row the caller has already been shown (or skipped as not
//! theirs). A listing only says how one stored row projects to its item.

use serde_json::{Map, Value};

use crate::tenant_cursor::CursorFamily;

/// Most node rows one page may examine. Rows of other node types count: they
/// are what makes an empty-but-resumable page possible.
pub const MAX_KEYSET_PAGE_SCAN: usize = 1_024;
/// Most stored row bytes one page may examine (the response-size bound).
pub const MAX_KEYSET_PAGE_BYTES: usize = 4 * 1024 * 1024;

/// One tenant-bound native listing.
pub trait KeysetListing {
    type Item;
    /// The listing's cursor family.
    const CURSOR: CursorFamily;
    /// The tenant the listing is bound to (and its cursor minted for).
    fn tenant(&self) -> &str;
    /// Most items one page may return.
    fn limit(&self) -> usize;
    /// The item one stored row contributes, `None` when it is not the
    /// tenant's or does not pass the listing's filters.
    fn select(&self, row_id: &str, row: &Map<String, Value>) -> Result<Option<Self::Item>, String>;
}

/// One page: at most `limit` items and the cursor that resumes the scan.
#[derive(Debug, Clone, PartialEq)]
pub struct KeysetPage<T> {
    pub items: Vec<T>,
    pub next_cursor: Option<String>,
}

/// One page under construction, fed rows in key order.
pub struct KeysetScan<'r, L: KeysetListing> {
    listing: &'r L,
    items: Vec<L::Item>,
    scanned: usize,
    bytes: usize,
    last_consumed: Option<String>,
    truncated: bool,
}

impl<'r, L: KeysetListing> KeysetScan<'r, L> {
    pub fn new(listing: &'r L) -> Self {
        Self {
            listing,
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
        let full = self.items.len() >= self.listing.limit()
            || self.scanned >= MAX_KEYSET_PAGE_SCAN
            || (self.scanned > 0 && self.bytes >= MAX_KEYSET_PAGE_BYTES);
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
        if let Some(item) = self.listing.select(row_id, row)? {
            self.items.push(item);
        }
        self.last_consumed = Some(row_id.to_string());
        Ok(())
    }

    /// Close the page, minting a cursor when a bound stopped it early.
    pub fn finish(self) -> KeysetPage<L::Item> {
        let next_cursor = self
            .last_consumed
            .filter(|_| self.truncated)
            .map(|row_id| L::CURSOR.encode(self.listing.tenant(), &row_id));
        KeysetPage {
            items: self.items,
            next_cursor,
        }
    }
}

/// Most keys an exact-pair filter (`metadata_match`, `grant_match`) may name.
pub const MAX_PAIR_FILTER_KEYS: usize = 8;
/// Longest key an exact-pair filter may name.
const MAX_PAIR_FILTER_KEY_BYTES: usize = 512;

/// Validate an optional exact top-level key/value filter.
pub fn validate_pair_filter(
    field: &str,
    wanted: Option<&Map<String, Value>>,
) -> Result<(), String> {
    let Some(wanted) = wanted else {
        return Ok(());
    };
    let keys_ok = wanted
        .keys()
        .all(|key| !key.trim().is_empty() && key.len() <= MAX_PAIR_FILTER_KEY_BYTES);
    if wanted.is_empty() || wanted.len() > MAX_PAIR_FILTER_KEYS || !keys_ok {
        return Err(format!(
            "{field} must name 1..={MAX_PAIR_FILTER_KEYS} bounded keys"
        ));
    }
    Ok(())
}

/// Whether `have` holds every one of `wanted`'s top-level pairs exactly.
pub fn matches_pairs(wanted: Option<&Map<String, Value>>, have: &Map<String, Value>) -> bool {
    wanted.is_none_or(|wanted| {
        wanted
            .iter()
            .all(|(key, value)| have.get(key) == Some(value))
    })
}
