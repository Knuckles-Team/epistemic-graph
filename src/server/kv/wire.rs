//! The served, versioned half of the KV surface (EG-DURABLE-KERNEL-R102).
//!
//! The wire methods own their namespaces outright: [`wire_namespace`] maps a
//! caller's local name to an opaque `kv-namespace:<sha256>` key derived from the
//! verified tenant, principal and request graph, so no Redis/S3/KV-cache adapter
//! namespace can ever alias one. That ownership is what lets a served row carry a
//! version: its stored bytes are `[version: u64 BE][value]`, where `version` is the
//! namespace's mutation-kernel commit version at the write. The kernel's version
//! never decreases, so a version is unique per namespace across deletes and
//! re-creates, and `KvCas` compares it rather than the value (no A-B-A).

use super::*;
use eg_types::storage_wire::{KvCasOutcome, KvEntry, KvScanPage, KvScanRow};

const VERSION_BYTES: usize = 8;
/// Page bounds of a served `KvScan`.
const DEFAULT_WIRE_SCAN_LIMIT: usize = 100;
const MAX_WIRE_SCAN_LIMIT: usize = 1_000;
const MAX_WIRE_SCAN_PAGE_BYTES: usize = 4 * 1024 * 1024;

/// The store-level namespace one served call acts on.
pub(super) fn wire_namespace(authority: &CarrierAuthority, graph: &str, local: &str) -> String {
    let mut scoped = String::with_capacity(graph.len() + local.len() + 1);
    scoped.push_str(graph);
    scoped.push('\u{1f}');
    scoped.push_str(local);
    authority.namespace("kv-namespace", &scoped)
}

fn encode_row(version: u64, value: &[u8]) -> Vec<u8> {
    let mut row = Vec::with_capacity(VERSION_BYTES + value.len());
    row.extend_from_slice(&version.to_be_bytes());
    row.extend_from_slice(value);
    row
}

fn decode_row(row: &[u8]) -> Result<(u64, &[u8]), String> {
    if row.len() < VERSION_BYTES {
        return Err("served KV row is missing its version header".to_string());
    }
    let (version, value) = row.split_at(VERSION_BYTES);
    let mut be = [0u8; VERSION_BYTES];
    be.copy_from_slice(version);
    Ok((u64::from_be_bytes(be), value))
}

fn current_version(row: Option<&[u8]>) -> Result<u64, String> {
    row.map(|row| decode_row(row).map(|(version, _)| version))
        .transpose()
        .map(|version| version.unwrap_or(0))
}

/// The version a write stamps: the kernel commit version, and in any case past the
/// row's own current version (the ephemeral backend keeps no kernel versions).
fn next_version(stamp: u64, current: u64) -> u64 {
    stamp.max(current.saturating_add(1))
}

/// Row access inside ONE write: a redb owner table or the locked in-memory map.
trait RowAccess {
    fn row(&mut self, namespace: &str, key: &str) -> Result<Option<Vec<u8>>, String>;
    fn store(&mut self, namespace: &str, key: &str, row: Option<&[u8]>) -> Result<(), String>;
}

impl RowAccess for redb::Table<'_, (&'static str, &'static str), &'static [u8]> {
    fn row(&mut self, namespace: &str, key: &str) -> Result<Option<Vec<u8>>, String> {
        Ok(self
            .get((namespace, key))
            .map_err(|e| e.to_string())?
            .map(|value| value.value().to_vec()))
    }

    fn store(&mut self, namespace: &str, key: &str, row: Option<&[u8]>) -> Result<(), String> {
        match row {
            Some(row) => self.insert((namespace, key), row).map(|_| ()),
            None => self.remove((namespace, key)).map(|_| ()),
        }
        .map_err(|e| e.to_string())
    }
}

impl RowAccess for BTreeMap<(String, String), Vec<u8>> {
    fn row(&mut self, namespace: &str, key: &str) -> Result<Option<Vec<u8>>, String> {
        Ok(self.get(&(namespace.to_string(), key.to_string())).cloned())
    }

    fn store(&mut self, namespace: &str, key: &str, row: Option<&[u8]>) -> Result<(), String> {
        let slot = (namespace.to_string(), key.to_string());
        match row {
            Some(row) => self.insert(slot, row.to_vec()),
            None => self.remove(&slot),
        };
        Ok(())
    }
}

/// One versioned write against `rows`: `expected` is `None` for an unconditional
/// put, `Some(version)` for a compare-and-swap.
fn versioned_write(
    rows: &mut dyn RowAccess,
    (namespace, key): (&str, &str),
    expected: Option<u64>,
    new: Option<&[u8]>,
    stamp: u64,
) -> Result<KvCasOutcome, String> {
    let current = current_version(rows.row(namespace, key)?.as_deref())?;
    if expected.is_some_and(|expected| expected != current) {
        return Ok(KvCasOutcome {
            swapped: false,
            version: current,
        });
    }
    let version = match new {
        Some(value) => {
            let version = next_version(stamp, current);
            rows.store(namespace, key, Some(&encode_row(version, value)))?;
            version
        }
        None => {
            rows.store(namespace, key, None)?;
            0
        }
    };
    Ok(KvCasOutcome {
        swapped: true,
        version,
    })
}

/// A compiled served write: its batch, admission time and kernel version stamp.
pub(super) struct WireWrite<'a> {
    pub(super) batch: &'a MutationBatch,
    pub(super) at_ms: u64,
    pub(super) stamp: u64,
}

impl KvStore {
    /// `KvGet`: the value and its version, or `None` when absent.
    pub(crate) fn get_versioned(
        &self,
        namespace: &str,
        key: &str,
    ) -> Result<Option<KvEntry>, String> {
        self.get(namespace, key)?
            .map(|row| {
                decode_row(&row).map(|(version, value)| KvEntry {
                    value: value.to_vec(),
                    version,
                })
            })
            .transpose()
    }

    /// `KvPut` (`expected == None`) and `KvCas` (`Some(version)`), as one admitted
    /// MutationBatch whose exact verdict is the replayable result.
    pub(super) fn write_versioned(
        &self,
        (namespace, key): (&str, &str),
        expected: Option<u64>,
        new: Option<Vec<u8>>,
        write: &WireWrite<'_>,
    ) -> Result<KvCasOutcome, String> {
        validate_key(namespace, key)?;
        if let Some(value) = new.as_deref() {
            validate_value(value)?;
        }
        let new = new.as_deref();
        match &self.backend {
            Backend::Redb(store) => store.admit_batch(write.batch, write.at_ms, |owner_write| {
                let mut table = owner_write.open_table(KV)?;
                versioned_write(&mut table, (namespace, key), expected, new, write.stamp)
            }),
            Backend::Memory(m) => {
                versioned_write(&mut *m.lock(), (namespace, key), expected, new, write.stamp)
            }
        }
    }

    /// `KvScan`: one bounded page after `cursor` (exclusive) of keys with `prefix`.
    pub(crate) fn scan_versioned(
        &self,
        namespace: &str,
        prefix: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<KvScanPage, String> {
        validate_key(namespace, prefix)?;
        let start = scan_start(prefix, cursor)?;
        let mut page = PageBuilder::new(prefix, cursor, page_limit(limit));
        match &self.backend {
            Backend::Redb(store) => scan_redb_page(store, namespace, start, &mut page)?,
            Backend::Memory(m) => scan_memory_page(&m.lock(), namespace, start, &mut page)?,
        }
        Ok(page.finish())
    }
}

/// Where a page's range starts: the cursor (which must lie inside the prefix), else
/// the prefix itself.
fn scan_start<'a>(prefix: &'a str, cursor: Option<&'a str>) -> Result<&'a str, String> {
    match cursor {
        None => Ok(prefix),
        Some(cursor) if cursor.starts_with(prefix) && cursor.len() <= MAX_KV_KEY_BYTES => {
            Ok(cursor)
        }
        Some(_) => Err("KV scan cursor does not belong to this prefix".to_string()),
    }
}

fn page_limit(limit: usize) -> usize {
    if limit == 0 {
        DEFAULT_WIRE_SCAN_LIMIT
    } else {
        limit.min(MAX_WIRE_SCAN_LIMIT)
    }
}

fn scan_redb_page(
    store: &RedbBackend,
    namespace: &str,
    start: &str,
    page: &mut PageBuilder<'_>,
) -> Result<(), String> {
    let read = store.read()?;
    let table = read.open_owner_table(KV)?;
    for entry in table
        .range((namespace, start)..)
        .map_err(|e| e.to_string())?
    {
        let (k, v) = entry.map_err(|e| e.to_string())?;
        let (ns, key) = k.value();
        if ns != namespace || !page.push(key, v.value())? {
            break;
        }
    }
    Ok(())
}

fn scan_memory_page(
    rows: &BTreeMap<(String, String), Vec<u8>>,
    namespace: &str,
    start: &str,
    page: &mut PageBuilder<'_>,
) -> Result<(), String> {
    for ((ns, key), v) in rows.range((namespace.to_string(), start.to_string())..) {
        if ns != namespace || !page.push(key, v)? {
            break;
        }
    }
    Ok(())
}

/// Accumulates one scan page under its row and byte bounds.
struct PageBuilder<'a> {
    prefix: &'a str,
    cursor: Option<&'a str>,
    limit: usize,
    bytes: usize,
    entries: Vec<KvScanRow>,
    more: bool,
}

impl<'a> PageBuilder<'a> {
    fn new(prefix: &'a str, cursor: Option<&'a str>, limit: usize) -> Self {
        Self {
            prefix,
            cursor,
            limit,
            bytes: 0,
            entries: Vec::new(),
            more: false,
        }
    }

    /// Take one ranged row; `false` once the prefix ends or the page is full.
    fn push(&mut self, key: &str, row: &[u8]) -> Result<bool, String> {
        if self.cursor == Some(key) {
            return Ok(true);
        }
        if !key.starts_with(self.prefix) {
            return Ok(false);
        }
        let size = key.len().saturating_add(row.len());
        let full = self.entries.len() >= self.limit
            || (!self.entries.is_empty()
                && self.bytes.saturating_add(size) > MAX_WIRE_SCAN_PAGE_BYTES);
        if full {
            self.more = true;
            return Ok(false);
        }
        let (version, value) = decode_row(row)?;
        self.bytes = self.bytes.saturating_add(size);
        self.entries.push(KvScanRow {
            key: key.to_string(),
            value: value.to_vec(),
            version,
        });
        Ok(true)
    }

    fn finish(self) -> KvScanPage {
        let next_cursor = self
            .more
            .then(|| self.entries.last().map(|row| row.key.clone()))
            .flatten();
        KvScanPage {
            entries: self.entries,
            next_cursor,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn row_codec_round_trips_and_refuses_a_headerless_row() {
        let row = encode_row(7, b"value");
        assert_eq!(decode_row(&row).unwrap(), (7, &b"value"[..]));
        assert!(decode_row(b"short").is_err());
    }

    // spec: EG-DURABLE-KERNEL-R102.3
    #[test]
    fn versions_never_repeat_across_delete_and_recreate() {
        let mut rows = BTreeMap::new();
        let first = versioned_write(&mut rows, ("ns", "k"), None, Some(b"a"), 5).unwrap();
        let deleted =
            versioned_write(&mut rows, ("ns", "k"), Some(first.version), None, 6).unwrap();
        let again = versioned_write(&mut rows, ("ns", "k"), Some(0), Some(b"b"), 7).unwrap();
        assert_eq!((first.version, deleted.version, again.version), (5, 0, 7));
        let stale =
            versioned_write(&mut rows, ("ns", "k"), Some(first.version), Some(b"c"), 8).unwrap();
        assert_eq!(
            stale,
            KvCasOutcome {
                swapped: false,
                version: 7
            }
        );
    }
}
