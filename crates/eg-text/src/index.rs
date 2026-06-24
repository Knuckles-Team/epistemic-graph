//! The Tantivy-backed BM25 inverted index (feature `tantivy`).
//!
//! Schema = two fields:
//!   * `node_id`  — `STRING | STORED`: the un-tokenized exact node id. STRING (not
//!     TEXT) so it is a single non-analyzed term — that makes `delete_term(node_id)`
//!     an exact tombstone of one document (the incremental-delete primitive).
//!   * `text`     — `TEXT`: the analyzed body, tokenized + Porter-stemmed by
//!     Tantivy's default `en_stem` pipeline, with positions+freqs indexed so BM25 has
//!     term frequencies. NOT stored — we only need the id back, not the body.
//!
//! Persistence: the index lives in a directory (`MmapDirectory`). Reopening is
//! [`TextIndex::open`] → `Index::open_in_dir` — it maps the existing segments, it does
//! NOT re-tokenize or rebuild postings from raw text. That is the persist-no-rebuild
//! contract (proven by `tests::persists_without_rebuild`). An in-memory variant
//! ([`TextIndex::in_memory`]) backs unit tests and ephemeral use.
//!
//! Incrementality: [`TextIndex::upsert`] deletes any existing doc with that id, then
//! adds the new one — so re-indexing one changed node is O(1 doc). [`TextIndex::delete`]
//! tombstones by id. Both take effect on [`TextIndex::commit`].

use tantivy::collector::TopDocs;
use tantivy::directory::MmapDirectory;
use tantivy::query::QueryParser;
use tantivy::schema::{
    Field, IndexRecordOption, Schema, TextFieldIndexing, TextOptions, Value, STORED, STRING,
};
use tantivy::tokenizer::{LowerCaser, SimpleTokenizer, Stemmer, TextAnalyzer};
use tantivy::{doc, Index, IndexWriter, TantivyDocument, Term};

use crate::TextHit;

/// 50 MB writer heap — Tantivy's documented sane floor for a small embedded index.
const WRITER_HEAP_BYTES: usize = 50_000_000;

/// An embedded BM25 full-text index over `(node_id, text)`.
///
/// One long-lived `IndexWriter` (Tantivy allows a single writer per index) is held so
/// add/delete/commit are cheap; reads go through a fresh `Searcher` per query off a
/// `Reader` that reloads on commit.
pub struct TextIndex {
    index: Index,
    writer: IndexWriter,
    reader: tantivy::IndexReader,
    f_node_id: Field,
    f_text: Field,
}

impl TextIndex {
    /// Name of the analyzer registered on the index for the `text` field: a
    /// lower-casing + Porter-stemming pipeline (so "databases" and "database" share a
    /// term). Registered in [`Self::from_index`].
    const ANALYZER: &'static str = "en_stem";

    fn build_schema() -> (Schema, Field, Field) {
        let mut sb = Schema::builder();
        // STRING => one exact, un-analyzed term; STORED so a hit returns the id.
        let f_node_id = sb.add_text_field("node_id", STRING | STORED);
        // The `text` field uses our `en_stem` analyzer (lower-case + Porter stem) with
        // positions+freqs indexed for BM25. Not STORED — we never read the body back.
        let text_indexing = TextFieldIndexing::default()
            .set_tokenizer(Self::ANALYZER)
            .set_index_option(IndexRecordOption::WithFreqsAndPositions);
        let text_opts = TextOptions::default().set_indexing_options(text_indexing);
        let f_text = sb.add_text_field("text", text_opts);
        (sb.build(), f_node_id, f_text)
    }

    fn from_index(index: Index) -> tantivy::Result<Self> {
        // Register the lower-case + Porter-stem analyzer under our name. Idempotent —
        // re-registering on a reopened index is fine.
        let analyzer = TextAnalyzer::builder(SimpleTokenizer::default())
            .filter(LowerCaser)
            .filter(Stemmer::new(tantivy::tokenizer::Language::English))
            .build();
        index.tokenizers().register(Self::ANALYZER, analyzer);

        let (_schema, f_node_id, f_text) = Self::build_schema();
        let writer: IndexWriter = index.writer(WRITER_HEAP_BYTES)?;
        let reader = index
            .reader_builder()
            .reload_policy(tantivy::ReloadPolicy::Manual)
            .try_into()?;
        Ok(Self {
            index,
            writer,
            reader,
            f_node_id,
            f_text,
        })
    }

    /// Open (or create) a PERSISTENT index in `dir`. On a pre-existing index this maps
    /// the on-disk segments WITHOUT rebuilding postings from raw text — the
    /// no-rebuild-on-load contract.
    pub fn open(dir: impl AsRef<std::path::Path>) -> tantivy::Result<Self> {
        let (schema, _, _) = Self::build_schema();
        let mmap = MmapDirectory::open(dir.as_ref())?;
        let index = Index::open_or_create(mmap, schema)?;
        Self::from_index(index)
    }

    /// An in-RAM index (no persistence) — for tests / ephemeral indexing.
    pub fn in_memory() -> tantivy::Result<Self> {
        let (schema, _, _) = Self::build_schema();
        let index = Index::create_in_ram(schema);
        Self::from_index(index)
    }

    /// Insert-or-replace the document for `node_id` with `text`. Deletes any prior doc
    /// with that exact id first (STRING term tombstone), then adds the new one, so a
    /// re-index of one changed node is O(1 doc). Effective after [`Self::commit`].
    pub fn upsert(&mut self, node_id: &str, text: &str) {
        let term = Term::from_field_text(self.f_node_id, node_id);
        self.writer.delete_term(term);
        self.writer
            .add_document(doc!(
                self.f_node_id => node_id,
                self.f_text => text,
            ))
            .expect("add_document into in-RAM/mmap index");
    }

    /// Tombstone the document for `node_id`. Effective after [`Self::commit`].
    pub fn delete(&mut self, node_id: &str) {
        let term = Term::from_field_text(self.f_node_id, node_id);
        self.writer.delete_term(term);
    }

    /// Commit pending adds/deletes durably (a new segment + tombstones) and reload the
    /// reader so subsequent [`Self::search`] sees them. One round-trip-amortized commit
    /// after a batch of upserts is the intended usage.
    pub fn commit(&mut self) -> tantivy::Result<()> {
        self.writer.commit()?;
        self.reader.reload()?;
        Ok(())
    }

    /// BM25 top-`k` for the natural-language `query` over the `text` field. Returns
    /// `(id, bm25_score)` descending — the same shape as the vector kNN, so it fuses
    /// into the RowSet algebra. An empty/unparsable query yields no hits (never errs
    /// the plan).
    pub fn search(&self, query: &str, k: usize) -> Vec<TextHit> {
        if query.trim().is_empty() || k == 0 {
            return Vec::new();
        }
        let searcher = self.reader.searcher();
        let parser = QueryParser::for_index(&self.index, vec![self.f_text]);
        let Ok(parsed) = parser.parse_query(query) else {
            return Vec::new();
        };
        // `.order_by_score()` selects the BM25-score-ordered top-k collector
        // (tantivy 0.26 split the bare `TopDocs` from the score-ordered Collector).
        let Ok(top) = searcher.search(&parsed, &TopDocs::with_limit(k).order_by_score()) else {
            return Vec::new();
        };
        let mut out = Vec::with_capacity(top.len());
        for (score, addr) in top {
            let Ok(doc) = searcher.doc::<TantivyDocument>(addr) else {
                continue;
            };
            if let Some(id) = doc
                .get_first(self.f_node_id)
                .and_then(|v| v.as_str())
                .map(str::to_owned)
            {
                out.push(TextHit { id, score });
            }
        }
        out
    }

    /// Number of (live) documents — for tests / introspection.
    pub fn num_docs(&self) -> u64 {
        self.reader.searcher().num_docs()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seed(ix: &mut TextIndex) {
        ix.upsert("d1", "the quick brown fox jumps over the lazy dog");
        ix.upsert("d2", "graph databases store nodes and edges efficiently");
        ix.upsert(
            "d3",
            "vector search ranks documents by embedding similarity",
        );
        ix.upsert("d4", "the lazy dog sleeps all day in the warm sun");
        ix.commit().unwrap();
    }

    /// BM25 recall: a query term retrieves the docs containing it, most-relevant
    /// first, and excludes docs that lack the term.
    #[test]
    fn bm25_recall_and_ranking() {
        let mut ix = TextIndex::in_memory().unwrap();
        seed(&mut ix);
        let hits = ix.search("lazy dog", 10);
        let ids: Vec<&str> = hits.iter().map(|h| h.id.as_str()).collect();
        // d1 and d4 both contain "lazy dog"; d2/d3 do not.
        assert!(ids.contains(&"d1"), "d1 has the term: {ids:?}");
        assert!(ids.contains(&"d4"), "d4 has the term: {ids:?}");
        assert!(!ids.contains(&"d2"), "d2 must not match: {ids:?}");
        assert!(!ids.contains(&"d3"), "d3 must not match: {ids:?}");
        // Scores descending.
        assert!(
            hits.windows(2).all(|w| w[0].score >= w[1].score),
            "BM25 descending: {hits:?}"
        );
    }

    /// Stemming: the default en_stem analyzer matches "database" against the indexed
    /// "databases" — proving Tantivy's tokenization/stemming is live.
    #[test]
    fn stemming_matches_morphological_variant() {
        let mut ix = TextIndex::in_memory().unwrap();
        seed(&mut ix);
        let hits = ix.search("database", 10);
        let ids: Vec<&str> = hits.iter().map(|h| h.id.as_str()).collect();
        assert!(
            ids.contains(&"d2"),
            "stemmed 'database'→'databas' hits d2: {ids:?}"
        );
    }

    /// Incremental add then delete: a newly upserted doc is found; after delete+commit
    /// it is gone, and the doc count tracks both.
    #[test]
    fn incremental_add_and_delete() {
        let mut ix = TextIndex::in_memory().unwrap();
        seed(&mut ix);
        assert_eq!(ix.num_docs(), 4);

        ix.upsert("d5", "incremental indexing of a single new document");
        ix.commit().unwrap();
        assert_eq!(ix.num_docs(), 5);
        assert!(
            ix.search("incremental", 10).iter().any(|h| h.id == "d5"),
            "new doc d5 is searchable"
        );

        ix.delete("d5");
        ix.commit().unwrap();
        assert_eq!(ix.num_docs(), 4, "delete tombstoned d5");
        assert!(
            ix.search("incremental", 10).is_empty(),
            "deleted doc no longer matches"
        );
    }

    /// Upsert REPLACES (does not duplicate) an existing id, and the new text is what
    /// matches — the incremental-update primitive.
    #[test]
    fn upsert_replaces_in_place() {
        let mut ix = TextIndex::in_memory().unwrap();
        seed(&mut ix);
        ix.upsert(
            "d1",
            "completely different content about astronomy and stars",
        );
        ix.commit().unwrap();
        assert_eq!(ix.num_docs(), 4, "upsert replaced d1, not duplicated it");
        // Old terms gone, new terms present, all still on id d1.
        assert!(ix.search("fox", 10).is_empty(), "old d1 text retired");
        let stars = ix.search("astronomy", 10);
        assert_eq!(stars.len(), 1);
        assert_eq!(stars[0].id, "d1");
    }

    /// PERSIST WITHOUT REBUILD: write an index to disk, DROP it, reopen from the same
    /// dir, and search succeeds WITHOUT any re-indexing call — the postings were read
    /// off the persisted segments, not rebuilt from raw text.
    #[test]
    fn persists_without_rebuild() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut ix = TextIndex::open(dir.path()).unwrap();
            seed(&mut ix);
            assert_eq!(ix.num_docs(), 4);
        } // writer/index dropped — nothing kept in RAM.

        // Reopen: NO upsert/seed call. If search works, the index loaded from disk.
        let ix = TextIndex::open(dir.path()).unwrap();
        assert_eq!(ix.num_docs(), 4, "reopened persisted index without rebuild");
        let hits = ix.search("graph databases", 10);
        assert!(
            hits.iter().any(|h| h.id == "d2"),
            "persisted postings served a query with no rebuild: {hits:?}"
        );
    }

    /// Empty/whitespace query is a no-op, not an error (never breaks a plan).
    #[test]
    fn empty_query_is_empty() {
        let mut ix = TextIndex::in_memory().unwrap();
        seed(&mut ix);
        assert!(ix.search("", 10).is_empty());
        assert!(ix.search("   ", 10).is_empty());
        assert!(ix.search("lazy", 0).is_empty(), "k=0 yields nothing");
    }
}
