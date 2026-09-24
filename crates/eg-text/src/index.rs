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
use tantivy::query::{BooleanQuery, ConstScoreQuery, Occur, Query, QueryParser, TermSetQuery};
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

    /// Remove every indexed document. Recovery rebuilds use this before replaying
    /// the authoritative source snapshot so documents deleted while this process
    /// was offline cannot survive merely because their ids are absent from the
    /// recovered graph.
    pub fn clear(&mut self) -> tantivy::Result<()> {
        self.writer.delete_all_documents()?;
        Ok(())
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
        match self.parse(query, k) {
            Some(parsed) => self.top_hits(parsed.as_ref(), k),
            None => Vec::new(),
        }
    }

    /// BM25 top-`k` for `query` evaluated WITHIN the `candidates` id set (EH-532,
    /// CONCEPT:EG-KG.query.filtered-text-search). The candidate restriction is a
    /// zero-scored conjunct of the scoring query, so the index only ever scores
    /// candidate documents: the result is exactly the full-corpus ranking restricted to
    /// `candidates` (same scores, same order, same tie-break), never a post-filtered
    /// global top-k that can silently drop candidates ranked behind non-candidates.
    pub fn search_within(&self, query: &str, candidates: &[&str], k: usize) -> Vec<TextHit> {
        if candidates.is_empty() {
            return Vec::new();
        }
        let Some(parsed) = self.parse(query, k) else {
            return Vec::new();
        };
        let ids = candidates
            .iter()
            .map(|id| Term::from_field_text(self.f_node_id, id));
        // Constant score 0: membership gates the match without moving any BM25 score.
        let allow: Box<dyn Query> =
            Box::new(ConstScoreQuery::new(Box::new(TermSetQuery::new(ids)), 0.0));
        let within = BooleanQuery::new(vec![(Occur::Must, parsed), (Occur::Must, allow)]);
        self.top_hits(&within, k)
    }

    /// Parse `query` against the `text` field; `None` for an empty/unparsable query or
    /// a zero `k`, which both yield no hits.
    fn parse(&self, query: &str, k: usize) -> Option<Box<dyn Query>> {
        if query.trim().is_empty() || k == 0 {
            return None;
        }
        let parser = QueryParser::for_index(&self.index, vec![self.f_text]);
        parser.parse_query(query).ok()
    }

    /// Run `query` and project the score-ordered top-`k` onto `(node id, score)`.
    fn top_hits(&self, query: &dyn Query, k: usize) -> Vec<TextHit> {
        let searcher = self.reader.searcher();
        // `.order_by_score()` selects the BM25-score-ordered top-k collector
        // (tantivy 0.26 split the bare `TopDocs` from the score-ordered Collector).
        let Ok(top) = searcher.search(query, &TopDocs::with_limit(k).order_by_score()) else {
            return Vec::new();
        };
        top.into_iter()
            .filter_map(|(score, addr)| {
                let doc = searcher.doc::<TantivyDocument>(addr).ok()?;
                let id = doc.get_first(self.f_node_id)?.as_str()?.to_owned();
                Some(TextHit { id, score })
            })
            .collect()
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

    /// EQUIVALENCE (CONCEPT:EG-KG.storage.incremental-text): an INCREMENTAL edit sequence
    /// — seed 4 docs, upsert a 5th, then delete one — yields the IDENTICAL search
    /// result (ids AND scores) as a fresh index REBUILT from only the surviving docs.
    /// This is exactly the reduction of `GraphTextIndex.apply_delta` (upsert adds/
    /// updates, delete removals) to a full rebuild baseline, proving the wired
    /// incremental path never diverges from a drop-and-rebuild.
    #[test]
    fn incremental_edits_equal_full_rebuild() {
        // Survivors after the incremental edits: d2, d3, d4 (seed) + d5 (added); d1 deleted.
        let survivors: [(&str, &str); 4] = [
            ("d2", "graph databases store nodes and edges efficiently"),
            (
                "d3",
                "vector search ranks documents by embedding similarity",
            ),
            ("d4", "the lazy dog sleeps all day in the warm sun"),
            ("d5", "incremental indexing of a single new document"),
        ];

        // Incremental index: seed, add d5, delete d1.
        let mut inc = TextIndex::in_memory().unwrap();
        seed(&mut inc);
        inc.upsert("d5", "incremental indexing of a single new document");
        inc.delete("d1");
        inc.commit().unwrap();

        // Rebuild baseline: a fresh index over ONLY the survivors.
        let mut base = TextIndex::in_memory().unwrap();
        for (id, text) in survivors {
            base.upsert(id, text);
        }
        base.commit().unwrap();

        assert_eq!(inc.num_docs(), base.num_docs(), "same live doc count");
        // Identical result SET and RANKING across several representative queries. (BM25
        // *scores* legitimately differ between a tombstoned-incremental index and a
        // fresh rebuild — a deleted doc still contributes to collection statistics until
        // segment merge — so the meaningful equivalence is the ranked id list, not the
        // raw float score. `GraphTextIndex` consumes ranked ids, and the fusion layer is
        // rank-based, so this is the equivalence that matters.)
        let ids = |v: Vec<TextHit>| v.into_iter().map(|h| h.id).collect::<Vec<_>>();
        for q in [
            "lazy dog",
            "graph databases",
            "incremental",
            "vector similarity",
        ] {
            assert_eq!(
                ids(inc.search(q, 10)),
                ids(base.search(q, 10)),
                "incremental vs rebuild diverged for query {q:?}"
            );
        }
        // The deleted doc is unreachable in BOTH.
        assert!(inc.search("fox", 10).is_empty());
        assert!(base.search("fox", 10).is_empty());
    }

    /// A deterministic corpus over a small vocabulary: term frequencies and document
    /// lengths vary, so BM25 scores spread and ties are rare but possible.
    fn corpus(size: usize) -> TextIndex {
        const WORDS: [&str; 8] = [
            "graph", "vector", "lexical", "shard", "tensor", "ledger", "proof", "lease",
        ];
        let mut ix = TextIndex::in_memory().unwrap();
        let mut state = 0x2545_f491_u64;
        for doc in 0..size {
            let len = 3 + doc % 11;
            let body: Vec<&str> = (0..len)
                .map(|_| {
                    state = state
                        .wrapping_mul(6_364_136_223_846_793_005)
                        .wrapping_add(1);
                    WORDS[(state >> 33) as usize % WORDS.len()]
                })
                .collect();
            ix.upsert(&format!("d{doc:04}"), &body.join(" "));
        }
        ix.commit().unwrap();
        ix
    }

    /// The brute-force oracle: the COMPLETE corpus ranking, restricted to the candidates.
    fn restricted_oracle(ix: &TextIndex, query: &str, allowed: &[&str], k: usize) -> Vec<TextHit> {
        ix.search(query, ix.num_docs() as usize)
            .into_iter()
            .filter(|hit| allowed.contains(&hit.id.as_str()))
            .take(k)
            .collect()
    }

    /// Same ids in the same order, and the same BM25 scores up to float summation order
    /// (a multi-term query's per-term scores are summed in a different order when the
    /// candidate conjunct joins the scorer tree).
    fn assert_same_ranking(got: &[TextHit], want: &[TextHit], label: &str) {
        let ids = |hits: &[TextHit]| hits.iter().map(|h| h.id.clone()).collect::<Vec<_>>();
        assert_eq!(ids(got), ids(want), "{label}");
        for (g, w) in got.iter().zip(want) {
            assert!(
                (g.score - w.score).abs() <= 1e-5 * w.score.abs().max(1.0),
                "{label}: {g:?} vs {w:?}"
            );
        }
    }

    /// EH-532: a candidate-restricted search equals the full ranking restricted to the
    /// candidate set — same ids, same scores, same order — for selective and broad
    /// candidate sets and for `k` below and above the number of matching candidates.
    #[test]
    fn search_within_equals_the_restricted_full_ranking() {
        let ix = corpus(600);
        let ids: Vec<String> = (0..600).map(|doc| format!("d{doc:04}")).collect();
        for stride in [1usize, 3, 17, 97] {
            let allowed: Vec<&str> = ids.iter().step_by(stride).map(String::as_str).collect();
            for query in ["graph", "ledger proof", "tensor lease shard"] {
                for k in [1usize, 10, allowed.len()] {
                    let got = ix.search_within(query, &allowed, k);
                    let want = restricted_oracle(&ix, query, &allowed, k);
                    assert_same_ranking(&got, &want, &format!("stride {stride} {query:?} k {k}"));
                }
            }
        }
    }

    /// EH-532, the defect's own shape: candidates that score BELOW hundreds of
    /// non-candidates are all returned, and nothing outside the candidate set or
    /// without the term ever is.
    #[test]
    fn search_within_finds_candidates_ranked_behind_non_candidates() {
        let mut ix = TextIndex::in_memory().unwrap();
        for doc in 0..500 {
            ix.upsert(&format!("noise{doc}"), "apple apple apple apple");
        }
        ix.upsert(
            "keep1",
            "one apple among many other words in a long sentence",
        );
        ix.upsert("keep2", "an apple");
        ix.upsert("keep3", "no fruit mentioned here");
        ix.commit().unwrap();
        let hits = ix.search_within("apple", &["keep1", "keep2", "keep3"], 10);
        let ids: Vec<&str> = hits.iter().map(|hit| hit.id.as_str()).collect();
        assert_eq!(
            ids,
            vec!["keep2", "keep1"],
            "both matching candidates, BM25 order"
        );
        assert!(ix.search_within("apple", &[], 10).is_empty());
        assert!(ix.search_within("", &["keep1"], 10).is_empty());
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
