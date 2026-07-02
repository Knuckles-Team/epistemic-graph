//! Classic Robertson/Spärck-Jones BM25 relevance scoring + highlighted snippets
//! (CONCEPT:EG-311) — the REAL ranking behind ParadeDB's `paradedb.score()` /
//! `paradedb.snippet()`, replacing the EG-119 placeholders (score ≡ 1.0, snippet ≡
//! echo).
//!
//! ## Why this lives here, dep-free
//!
//! [`TextIndex`](crate::TextIndex) already computes corpus-true BM25 through Tantivy,
//! but that path is HARD-gated behind the heavy `tantivy` feature and its API is
//! index-shaped (`search(query, k)` over a persisted corpus). Two callers need a
//! *value*-shaped scorer instead — `score(query, doc)` / `snippet(query, doc, maxlen)`
//! over a single `(query, doc)` pair with NO index in hand:
//!
//!   * the eg-query `bm25_score` / `bm25_snippet` scalar UDFs (CONCEPT:EG-119), which
//!     run per-row inside DataFusion and only ever see one document's text at a time;
//!   * any server-side lowering that already has the matched row's text.
//!
//! So this module is **pure Rust, behind NO feature gate** — it compiles in every
//! build (including Pi) and pulls nothing. The classic BM25 term-saturation formula
//! (`k1`/`b`) plus per-term document-frequency IDF is small and self-contained; the
//! heavy machinery Tantivy buys (mmap segments, stemming, incremental delete) is only
//! needed for the *indexed* corpus path, not for scoring a pair you already hold.
//!
//! ## The single-document corpus
//!
//! Corpus BM25 needs three corpus statistics: `N` (document count), `avgdl` (average
//! document length), and `df(term)` (how many documents contain the term). The
//! value-shaped [`bm25_score`] has only ONE document, so it scores against a
//! **1-document corpus** (`N = 1`, `avgdl = |doc|`, `df = 1` for every present term).
//! IDF is then a positive constant across documents, so ranking reduces to BM25's
//! term-frequency saturation — which is exactly what makes a document that mentions the
//! query terms *more often* out-rank one that mentions them once (proven by
//! [`tests::eg311_score_ranks_more_relevant_doc_higher`]). For a genuinely multi-doc
//! IDF, build a [`Corpus`] and call [`Bm25::score_in_corpus`].

use std::collections::HashMap;

/// BM25 free parameters (CONCEPT:EG-311). `k1` controls term-frequency saturation
/// (higher ⇒ raw counts matter more before flattening); `b` controls length
/// normalization (`0` ⇒ ignore document length, `1` ⇒ fully normalize by it). The
/// defaults are the classic, widely-used `k1 = 1.2`, `b = 0.75`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Bm25 {
    pub k1: f32,
    pub b: f32,
}

impl Default for Bm25 {
    fn default() -> Self {
        Self { k1: 1.2, b: 0.75 }
    }
}

/// Corpus-level statistics for true IDF (CONCEPT:EG-311): document count, average
/// document length (in tokens) and the per-term document frequency. Build one with
/// [`Corpus::from_docs`] to score many documents against a query with a *shared* IDF.
#[derive(Clone, Debug, Default)]
pub struct Corpus {
    n_docs: usize,
    avgdl: f32,
    doc_freq: HashMap<String, usize>,
}

impl Corpus {
    /// Compute corpus statistics from an iterator of document bodies (CONCEPT:EG-311).
    /// Each document is tokenized once; `df(term)` counts *documents* (not occurrences)
    /// containing the term, and `avgdl` is the mean token count.
    pub fn from_docs<'a, I>(docs: I) -> Self
    where
        I: IntoIterator<Item = &'a str>,
    {
        let mut n_docs = 0usize;
        let mut total_len = 0usize;
        let mut doc_freq: HashMap<String, usize> = HashMap::new();
        for doc in docs {
            n_docs += 1;
            let toks = tokenize(doc);
            total_len += toks.len();
            let mut seen: std::collections::HashSet<&str> = std::collections::HashSet::new();
            for t in &toks {
                if seen.insert(t.as_str()) {
                    *doc_freq.entry(t.clone()).or_insert(0) += 1;
                }
            }
        }
        let avgdl = if n_docs == 0 {
            0.0
        } else {
            total_len as f32 / n_docs as f32
        };
        Self {
            n_docs,
            avgdl,
            doc_freq,
        }
    }
}

impl Bm25 {
    /// Construct with explicit parameters (CONCEPT:EG-311).
    pub fn new(k1: f32, b: f32) -> Self {
        Self { k1, b }
    }

    /// The BM25 IDF of a term given corpus `n_docs` and its document frequency `df`
    /// (CONCEPT:EG-311): `ln(1 + (N - df + 0.5) / (df + 0.5))`. The `1 +` (Lucene's
    /// variant) keeps IDF strictly non-negative even for very common terms, so no term
    /// ever *subtracts* from relevance.
    fn idf(n_docs: usize, df: usize) -> f32 {
        let n = n_docs.max(1) as f32;
        let df = df.max(1) as f32; // a term we are scoring appears in ≥ 1 doc
        (1.0 + (n - df + 0.5) / (df + 0.5)).ln()
    }

    /// BM25 score of `query` against `doc` using explicit corpus statistics
    /// (CONCEPT:EG-311) — the multi-document path where IDF is shared across every
    /// scored document. Sum, over distinct query terms present in `doc`, of
    /// `idf(term) * tf * (k1 + 1) / (tf + k1 * (1 - b + b * |doc| / avgdl))`.
    pub fn score_in_corpus(&self, query: &str, doc: &str, corpus: &Corpus) -> f32 {
        let doc_toks = tokenize(doc);
        if doc_toks.is_empty() {
            return 0.0;
        }
        let dl = doc_toks.len() as f32;
        let avgdl = if corpus.avgdl > 0.0 { corpus.avgdl } else { dl };
        let mut tf: HashMap<&str, f32> = HashMap::new();
        for t in &doc_toks {
            *tf.entry(t.as_str()).or_insert(0.0) += 1.0;
        }
        let mut score = 0.0f32;
        let mut counted: std::collections::HashSet<String> = std::collections::HashSet::new();
        for qt in tokenize(query) {
            // Score each distinct query term once (repeats in the query don't inflate).
            let Some(&f) = tf.get(qt.as_str()) else {
                continue;
            };
            if !counted.insert(qt.clone()) {
                continue;
            }
            let df = corpus.doc_freq.get(&qt).copied().unwrap_or(1);
            let idf = Self::idf(corpus.n_docs, df);
            let denom = f + self.k1 * (1.0 - self.b + self.b * dl / avgdl);
            score += idf * (f * (self.k1 + 1.0)) / denom;
        }
        score
    }
}

/// BM25 score of `query` against a single `doc` (CONCEPT:EG-311), self-contained: the
/// document is treated as a 1-document corpus (`N = 1`, `avgdl = |doc|`). Ranking many
/// docs against one query with this reflects term-frequency saturation — a doc that
/// mentions the query terms more often scores higher — which is the property the
/// per-row `bm25_score` UDF needs. Uses the default `k1`/`b`.
pub fn bm25_score(query: &str, doc: &str) -> f32 {
    let corpus = Corpus::from_docs(std::iter::once(doc));
    Bm25::default().score_in_corpus(query, doc, &corpus)
}

/// A highlighted relevance snippet of `doc` for `query` (CONCEPT:EG-311) — the REAL
/// `paradedb.snippet()`, replacing the EG-119 echo placeholder. Finds the first window
/// of `doc` (snapped to token boundaries, ≈ `maxlen` chars) that contains a query term,
/// wraps every whole-word, case-insensitive query-term occurrence in that window with
/// `<b>…</b>` (original case preserved), and prefixes/suffixes `…` when the window is a
/// truncation of `doc`. A query with no match yields the head of `doc` (no highlight).
pub fn bm25_snippet(query: &str, doc: &str, maxlen: usize) -> String {
    let maxlen = maxlen.max(1);
    let q_terms: std::collections::HashSet<String> = tokenize(query).into_iter().collect();
    let spans = token_spans(doc);
    if spans.is_empty() {
        return String::new();
    }

    // The first token that is a query term anchors the window.
    let anchor = spans
        .iter()
        .position(|s| q_terms.contains(&doc[s.0..s.1].to_ascii_lowercase()));

    let (lo_byte, hi_byte, truncated_left, truncated_right) = match anchor {
        Some(a) => {
            // Grow a token window [lo, hi] outward from the anchor while it fits maxlen:
            // extend forward first (trailing context), then backward.
            let mut lo = a;
            let mut hi = a;
            let fits = |lo: usize, hi: usize| spans[hi].1 - spans[lo].0 <= maxlen;
            while hi + 1 < spans.len() && fits(lo, hi + 1) {
                hi += 1;
            }
            while lo > 0 && fits(lo - 1, hi) {
                lo -= 1;
            }
            (spans[lo].0, spans[hi].1, lo > 0, hi + 1 < spans.len())
        }
        None => {
            // No match: return the head of the doc up to maxlen, token-boundary snapped.
            let mut hi = 0usize;
            while hi + 1 < spans.len() && spans[hi + 1].1 <= maxlen {
                hi += 1;
            }
            (0usize, spans[hi].1, false, hi + 1 < spans.len())
        }
    };

    // Rebuild the window, wrapping each query-term token span in <b>…</b>.
    let mut out = String::new();
    if truncated_left {
        out.push('…');
    }
    let mut cursor = lo_byte;
    for &(s, e) in spans.iter().filter(|&&(s, e)| s >= lo_byte && e <= hi_byte) {
        if q_terms.contains(&doc[s..e].to_ascii_lowercase()) {
            out.push_str(&doc[cursor..s]);
            out.push_str("<b>");
            out.push_str(&doc[s..e]);
            out.push_str("</b>");
            cursor = e;
        }
    }
    out.push_str(&doc[cursor..hi_byte]);
    if truncated_right {
        out.push('…');
    }
    out
}

/// Lowercased alphanumeric tokens of `text` (CONCEPT:EG-311). Deliberately simple —
/// split on any non-alphanumeric run, lowercase — matching the value-shaped scorer's
/// "no external tokenizer" contract (the Tantivy path in [`crate::TextIndex`] does the
/// stemming; here we keep it dep-free).
fn tokenize(text: &str) -> Vec<String> {
    text.split(|c: char| !c.is_alphanumeric())
        .filter(|t| !t.is_empty())
        .map(|t| t.to_ascii_lowercase())
        .collect()
}

/// Byte spans `(start, end)` of each alphanumeric token run in `text`, in order
/// (CONCEPT:EG-311) — used by [`bm25_snippet`] to highlight in place without losing the
/// original casing / separators. Boundaries fall between an alphanumeric and a
/// non-alphanumeric char, so every span start/end is a valid UTF-8 char boundary.
fn token_spans(text: &str) -> Vec<(usize, usize)> {
    let mut spans = Vec::new();
    let mut start: Option<usize> = None;
    for (i, c) in text.char_indices() {
        if c.is_alphanumeric() {
            start.get_or_insert(i);
        } else if let Some(s) = start.take() {
            spans.push((s, i));
        }
    }
    if let Some(s) = start {
        spans.push((s, text.len()));
    }
    spans
}

#[cfg(test)]
mod tests {
    use super::*;

    /// CONCEPT:EG-311 — real BM25 ranking: a document that mentions the query terms
    /// more often out-ranks one that mentions them once, and a document with none of
    /// the query terms scores zero. This is the placeholder-killer: EG-119 returned a
    /// constant 1.0 for every row, which could not order anything.
    #[test]
    fn eg311_score_ranks_more_relevant_doc_higher() {
        let query = "lazy dog";
        let very = "the lazy dog naps while another lazy dog guards the dog house";
        let some = "the quick brown fox jumps over the lazy dog just once";
        let none = "vector search ranks documents by embedding similarity";

        let s_very = bm25_score(query, very);
        let s_some = bm25_score(query, some);
        let s_none = bm25_score(query, none);

        assert!(
            s_very > s_some,
            "more query-term occurrences ⇒ higher BM25: very={s_very} some={s_some}"
        );
        assert!(
            s_some > s_none,
            "any match beats no match: some={s_some} none={s_none}"
        );
        assert_eq!(s_none, 0.0, "no query term present ⇒ zero relevance");
    }

    /// CONCEPT:EG-311 — corpus IDF: with a shared multi-document corpus, a RARE query
    /// term contributes more relevance than a term present in every document (which has
    /// IDF ≈ 0), so the doc matching the rare term ranks higher.
    #[test]
    fn eg311_corpus_idf_favors_rarer_term() {
        let docs = [
            "the graph engine is fast",
            "the graph engine is durable",
            "the graph engine reasons over ontologies",
            "the tantivy lexical index is fast",
        ];
        let corpus = Corpus::from_docs(docs.iter().copied());
        let bm25 = Bm25::default();
        // "the" appears in every doc (IDF≈0); "tantivy" in exactly one (high IDF).
        let rare = bm25.score_in_corpus("tantivy engine", docs[3], &corpus);
        let common = bm25.score_in_corpus("the engine", docs[0], &corpus);
        assert!(
            rare > common,
            "the rare-term match should out-score the common-term match: rare={rare} common={common}"
        );
    }

    /// CONCEPT:EG-311 — snippet highlights every matched term with <b>…</b>, preserving
    /// the original casing, and does NOT bold non-matching words.
    #[test]
    fn eg311_snippet_highlights_matched_terms() {
        let doc = "Graph Databases store Nodes and Edges efficiently for traversal";
        let snip = bm25_snippet("nodes edges", doc, 200);
        assert!(snip.contains("<b>Nodes</b>"), "matched term bolded (cased): {snip}");
        assert!(snip.contains("<b>Edges</b>"), "second matched term bolded: {snip}");
        assert!(
            !snip.contains("<b>store</b>"),
            "non-matching word must not be bolded: {snip}"
        );
    }

    /// CONCEPT:EG-311 — snippet windows around the FIRST match and marks truncation
    /// with an ellipsis when the match is deep in a long document.
    #[test]
    fn eg311_snippet_windows_around_match_with_ellipsis() {
        let doc = "aaa bbb ccc ddd eee fff ggg hhh iii jjj needle kkk lll mmm nnn ooo ppp";
        let snip = bm25_snippet("needle", doc, 24);
        assert!(snip.contains("<b>needle</b>"), "match highlighted: {snip}");
        assert!(snip.starts_with('…'), "left-truncated window marked: {snip}");
        assert!(
            snip.chars().count() <= 24 + 2 /* ellipses */ + "<b></b>".len(),
            "window bounded near maxlen: {snip:?}"
        );
        assert!(
            !snip.contains("aaa"),
            "far-away head excluded from the window: {snip}"
        );
    }

    /// CONCEPT:EG-311 — a query with no matching term yields the (truncated) head of the
    /// document, with no highlight markup (never panics, never errors a plan).
    #[test]
    fn eg311_snippet_no_match_returns_plain_head() {
        let doc = "the quick brown fox jumps over the lazy dog";
        let snip = bm25_snippet("astronomy", doc, 100);
        assert!(!snip.contains("<b>"), "no highlight when nothing matches: {snip}");
        assert!(snip.starts_with("the quick"), "returns head of doc: {snip}");
    }

    /// CONCEPT:EG-311 — empty / whitespace inputs are total (no panic): empty doc ⇒
    /// empty snippet and zero score; empty query ⇒ zero score.
    #[test]
    fn eg311_empty_inputs_are_total() {
        assert_eq!(bm25_score("q", ""), 0.0);
        assert_eq!(bm25_score("", "some document text"), 0.0);
        assert_eq!(bm25_snippet("q", "", 50), "");
    }
}
