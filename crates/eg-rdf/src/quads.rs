//! Opt-in lossless `quads` table (CONCEPT:KG-2.217, feature `rdf-redb`).
//!
//! The property-graph mapping is faithful for the COMMON shape but lossy for ONE
//! edge: a node property map is key-unique, so a subject with two DIFFERENT literals
//! for the SAME predicate (`ex:x ex:tag "a", "b"`) collapses to one. This table is
//! the authoritative, lossless store for exactly those extras — it sits beside the
//! engine's `NODES`/`EDGES` redb tables (same composite-key discipline) and is used
//! ONLY when a predicate is multi-valued. The property-graph stays the query-fast
//! default projection; this is the lossless backstop.
//!
//! Schema (one table):
//! ```text
//! RDF_QUADS: TableDefinition<(&str, &str, &str, u32), &[u8]>
//!            // (graph, subject_id, predicate_iri, ordinal) -> msgpack literal cell
//! ```
//! The ordinal makes a second/third literal for the same `(graph, s, p)` a distinct
//! key, so N values for one predicate are N rows — the lossless property the blob
//! cannot give. The value is the SAME `{value, datatype, lang}` typed cell the
//! property blob stores, so datatype/lang survive identically.

use redb::{Database, ReadableDatabase, TableDefinition};

/// `(graph, subject, predicate, ordinal)` → msgpack literal cell. CANONICAL schema.
pub const RDF_QUADS: TableDefinition<'static, (&str, &str, &str, u32), &[u8]> =
    TableDefinition::new("rdf_quads");

/// A durable, lossless store for the multi-valued-literal RDF cases. Opens its own
/// redb `Database` (`rdf_quads.redb`) beside `graph.redb`, matching how the
/// time-series store opens `series.redb`.
pub struct QuadStore {
    db: Database,
}

fn err<E: std::fmt::Display>(e: E) -> String {
    e.to_string()
}

impl QuadStore {
    /// Open (or create) the quad store at `path` (e.g. `<persist_dir>/rdf_quads.redb`).
    pub fn open(path: impl AsRef<std::path::Path>) -> Result<Self, String> {
        let db = Database::create(path).map_err(err)?;
        // Materialize the table so a first read on an empty store doesn't error.
        let wtx = db.begin_write().map_err(err)?;
        {
            let _ = wtx.open_table(RDF_QUADS).map_err(err)?;
        }
        wtx.commit().map_err(err)?;
        Ok(Self { db })
    }

    /// Append one extra literal for `(graph, subject, predicate)` losslessly. The
    /// ordinal is the current count for that `(graph, s, p)` prefix, so values never
    /// collide.
    pub fn put_literal(
        &self,
        graph: &str,
        subject: &str,
        predicate: &str,
        cell: &serde_json::Value,
    ) -> Result<(), String> {
        let blob = rmp_serde::to_vec_named(cell).map_err(err)?;
        let next = self.count(graph, subject, predicate)? as u32;
        let wtx = self.db.begin_write().map_err(err)?;
        {
            let mut t = wtx.open_table(RDF_QUADS).map_err(err)?;
            t.insert((graph, subject, predicate, next), blob.as_slice())
                .map_err(err)?;
        }
        wtx.commit().map_err(err)?;
        Ok(())
    }

    /// How many extra literals are stored for `(graph, subject, predicate)`.
    fn count(&self, graph: &str, subject: &str, predicate: &str) -> Result<usize, String> {
        let rtx = self.db.begin_read().map_err(err)?;
        let t = rtx.open_table(RDF_QUADS).map_err(err)?;
        let lo = (graph, subject, predicate, 0u32);
        let hi = (graph, subject, predicate, u32::MAX);
        let mut n = 0usize;
        for r in t.range(lo..=hi).map_err(err)? {
            r.map_err(err)?;
            n += 1;
        }
        Ok(n)
    }

    /// Scan every stored extra literal for `graph` → `(subject, predicate, cell)`.
    pub fn scan_literals(
        &self,
        graph: &str,
    ) -> Result<Vec<(String, String, serde_json::Value)>, String> {
        let rtx = self.db.begin_read().map_err(err)?;
        let t = rtx.open_table(RDF_QUADS).map_err(err)?;
        // Bound the scan to this graph's key prefix.
        let lo = (graph, "", "", 0u32);
        let hi = (graph, "\u{10FFFF}", "\u{10FFFF}", u32::MAX);
        let mut out = Vec::new();
        for r in t.range(lo..=hi).map_err(err)? {
            let (k, v) = r.map_err(err)?;
            let (g, s, p, _ord) = k.value();
            if g != graph {
                continue;
            }
            let cell: serde_json::Value = rmp_serde::from_slice(v.value()).map_err(err)?;
            out.push((s.to_string(), p.to_string(), cell));
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn multivalue_literals_round_trip_losslessly() {
        let dir = tempfile::tempdir().unwrap();
        let store = QuadStore::open(dir.path().join("rdf_quads.redb")).unwrap();
        let cell_b = serde_json::json!({
            "value": "b",
            "datatype": "http://www.w3.org/2001/XMLSchema#string",
        });
        let cell_c = serde_json::json!({
            "value": "c",
            "datatype": "http://www.w3.org/2001/XMLSchema#string",
        });
        store
            .put_literal("g", "<http://ex/x>", "<http://ex/tag>", &cell_b)
            .unwrap();
        store
            .put_literal("g", "<http://ex/x>", "<http://ex/tag>", &cell_c)
            .unwrap();

        let mut got = store.scan_literals("g").unwrap();
        got.sort_by(|a, b| a.2["value"].as_str().cmp(&b.2["value"].as_str()));
        assert_eq!(got.len(), 2, "both extras stored distinctly (ordinal keys)");
        assert_eq!(got[0].2["value"], "b");
        assert_eq!(got[1].2["value"], "c");
        // A different graph is isolated.
        assert!(store.scan_literals("other").unwrap().is_empty());
    }
}
