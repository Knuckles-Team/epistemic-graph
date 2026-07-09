// CONCEPT:EG-KG.mining.tfidf — Text mining: TF-IDF + topic modeling.
//
// Pure-Rust, dependency-light, batch (one round-trip): given a tokenized text
// corpus (each document a `Vec<TermId>`), compute either TF-IDF term weights
// per document, or a `k`-topic model via LDA (collapsed Gibbs sampling) or NMF
// (multiplicative updates on the TF-IDF matrix). This module is graph/index
// agnostic: the handler (`src/server/handlers/mining.rs`) tokenizes node text
// properties into a corpus (compute-near-data — no Tantivy/eg-text dependency,
// mirroring how `association`/`sequence` avoid a separate store for their
// graph-derived sources) and does the KG write-back (`:Topic{terms}`).
//
// * **TF-IDF**       (CONCEPT:EG-KG.mining.tfidf) — term-frequency × inverse-
//   document-frequency, the classic per-document term-weighting baseline.
// * **LDA**          (CONCEPT:EG-KG.mining.lda-topic-model) — Latent Dirichlet
//   Allocation fit by collapsed Gibbs sampling (deterministic per `seed`).
// * **NMF**          (CONCEPT:EG-KG.mining.nmf-topic-model) — Non-negative
//   Matrix Factorization of the TF-IDF matrix by multiplicative updates
//   (Lee & Seung; deterministic per `seed`, which only seeds the initial `W`/
//   `H` factors — the update rule itself has no randomness).

use std::collections::HashMap;

/// An interned term id (small, dense — assigned by [`intern`]).
pub type TermId = u32;

/// Which text-mining engine to run, with its parameters.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Algorithm {
    Tfidf,
    Lda {
        k: usize,
        alpha: f64,
        beta: f64,
        iterations: usize,
        seed: u64,
    },
    Nmf {
        k: usize,
        iterations: usize,
        seed: u64,
    },
}

/// The mining outcome over interned [`TermId`]s: `doc_terms` (TF-IDF only — per
/// document, terms sorted by descending weight) XOR `topics` + `doc_topics`
/// (LDA/NMF only — per-topic term weights sorted descending, and each
/// document's topic-membership distribution).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TextResult {
    pub doc_terms: Vec<Vec<(TermId, f64)>>,
    pub topics: Vec<Vec<(TermId, f64)>>,
    pub doc_topics: Vec<Vec<f64>>,
}

/// Lowercase, alnum-run tokenization (CONCEPT:EG-KG.mining.tfidf): splits on any
/// non-alphanumeric byte, drops empty runs. Pure-Rust, no stemming/stopwords —
/// callers wanting either can pre/post-filter the token list.
pub fn tokenize(text: &str) -> Vec<String> {
    text.to_lowercase()
        .split(|c: char| !c.is_alphanumeric())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect()
}

// ─────────────────────────── TF-IDF ───────────────────────────

/// TF-IDF (CONCEPT:EG-KG.mining.tfidf): `weight = (term_count / doc_len) *
/// (ln(N / (1 + df)) + 1)` — smoothed inverse document frequency (never
/// diverges for a term present in every document). Returns, per document, its
/// terms sorted by descending weight.
pub fn tfidf(docs: &[Vec<TermId>], vocab_size: usize) -> Vec<Vec<(TermId, f64)>> {
    let n = docs.len();
    let mut df = vec![0usize; vocab_size];
    for doc in docs {
        let mut seen = doc.clone();
        seen.sort_unstable();
        seen.dedup();
        for t in seen {
            df[t as usize] += 1;
        }
    }
    let idf: Vec<f64> = df
        .iter()
        .map(|&d| ((n as f64) / (1.0 + d as f64)).ln() + 1.0)
        .collect();

    docs.iter()
        .map(|doc| {
            let mut tf: HashMap<TermId, usize> = HashMap::new();
            for &t in doc {
                *tf.entry(t).or_insert(0) += 1;
            }
            let doc_len = (doc.len().max(1)) as f64;
            let mut terms: Vec<(TermId, f64)> = tf
                .into_iter()
                .map(|(t, c)| (t, (c as f64 / doc_len) * idf[t as usize]))
                .collect();
            terms.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal).then(a.0.cmp(&b.0)));
            terms
        })
        .collect()
}

// ─────────────────────────── LDA (collapsed Gibbs sampling) ───────────────────────────

/// LDA topic model (CONCEPT:EG-KG.mining.lda-topic-model): `k` topics fit by
/// collapsed Gibbs sampling over the standard LDA generative model (symmetric
/// Dirichlet priors `alpha` over doc-topic, `beta` over topic-term).
/// Deterministic per `seed`. Returns each topic's term distribution (all
/// `vocab_size` terms, sorted by descending weight) and each document's
/// topic-membership distribution.
pub fn lda(
    docs: &[Vec<TermId>],
    vocab_size: usize,
    k: usize,
    alpha: f64,
    beta: f64,
    iterations: usize,
    seed: u64,
) -> (Vec<Vec<(TermId, f64)>>, Vec<Vec<f64>>) {
    let n_docs = docs.len();
    if k == 0 || vocab_size == 0 || n_docs == 0 {
        return (Vec::new(), Vec::new());
    }
    let mut rng = SplitMix64::new(seed);
    let mut assignments: Vec<Vec<usize>> = docs.iter().map(|d| vec![0usize; d.len()]).collect();
    let mut doc_topic_counts = vec![vec![0u32; k]; n_docs];
    let mut topic_term_counts = vec![vec![0u32; vocab_size]; k];
    let mut topic_totals = vec![0u32; k];

    for (d, doc) in docs.iter().enumerate() {
        for (i, &term) in doc.iter().enumerate() {
            let topic = (rng.next_u64() as usize) % k;
            assignments[d][i] = topic;
            doc_topic_counts[d][topic] += 1;
            topic_term_counts[topic][term as usize] += 1;
            topic_totals[topic] += 1;
        }
    }

    for _ in 0..iterations {
        for d in 0..n_docs {
            for i in 0..docs[d].len() {
                let term = docs[d][i] as usize;
                let old_topic = assignments[d][i];
                doc_topic_counts[d][old_topic] -= 1;
                topic_term_counts[old_topic][term] -= 1;
                topic_totals[old_topic] -= 1;

                let mut cum = vec![0.0; k];
                let mut running = 0.0;
                for t in 0..k {
                    let p = (doc_topic_counts[d][t] as f64 + alpha)
                        * (topic_term_counts[t][term] as f64 + beta)
                        / (topic_totals[t] as f64 + vocab_size as f64 * beta);
                    running += p;
                    cum[t] = running;
                }
                let r = rng.next_f64() * running;
                let mut new_topic = k - 1;
                for (t, &c) in cum.iter().enumerate() {
                    if r <= c {
                        new_topic = t;
                        break;
                    }
                }

                assignments[d][i] = new_topic;
                doc_topic_counts[d][new_topic] += 1;
                topic_term_counts[new_topic][term] += 1;
                topic_totals[new_topic] += 1;
            }
        }
    }

    let mut topics: Vec<Vec<(TermId, f64)>> = Vec::with_capacity(k);
    for t in 0..k {
        let denom = topic_totals[t] as f64 + vocab_size as f64 * beta;
        let mut terms: Vec<(TermId, f64)> = (0..vocab_size)
            .map(|w| (w as TermId, (topic_term_counts[t][w] as f64 + beta) / denom))
            .collect();
        terms.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal).then(a.0.cmp(&b.0)));
        topics.push(terms);
    }
    let doc_topics: Vec<Vec<f64>> = (0..n_docs)
        .map(|d| {
            let denom = doc_topic_counts[d].iter().sum::<u32>() as f64 + k as f64 * alpha;
            (0..k)
                .map(|t| (doc_topic_counts[d][t] as f64 + alpha) / denom)
                .collect()
        })
        .collect();
    (topics, doc_topics)
}

// ─────────────────────────── NMF (multiplicative updates) ───────────────────────────

/// NMF topic model (CONCEPT:EG-KG.mining.nmf-topic-model): factorize the TF-IDF
/// matrix `V` (docs × vocab) into `W` (docs × k) and `H` (k × vocab) by
/// Lee & Seung's multiplicative-update rule, minimizing `||V - W*H||²`.
/// `seed` only determines the initial `W`/`H` factors (the update rule itself
/// is deterministic given them). Returns each topic's (row of `H`) term
/// weights sorted descending, and each document's (row-normalized `W`)
/// topic-membership distribution.
pub fn nmf(
    docs: &[Vec<TermId>],
    vocab_size: usize,
    k: usize,
    iterations: usize,
    seed: u64,
) -> (Vec<Vec<(TermId, f64)>>, Vec<Vec<f64>>) {
    let n_docs = docs.len();
    if k == 0 || vocab_size == 0 || n_docs == 0 {
        return (Vec::new(), Vec::new());
    }
    let tfidf_rows = tfidf(docs, vocab_size);
    let mut v = vec![vec![0.0; vocab_size]; n_docs];
    for (d, row) in tfidf_rows.iter().enumerate() {
        for &(t, w) in row {
            v[d][t as usize] = w;
        }
    }

    let mut rng = SplitMix64::new(seed);
    let mut w_mat = vec![vec![0.0; k]; n_docs];
    let mut h_mat = vec![vec![0.0; vocab_size]; k];
    for row in w_mat.iter_mut() {
        for x in row.iter_mut() {
            *x = 0.1 + rng.next_f64();
        }
    }
    for row in h_mat.iter_mut() {
        for x in row.iter_mut() {
            *x = 0.1 + rng.next_f64();
        }
    }

    const EPS: f64 = 1e-10;
    for _ in 0..iterations {
        // H *= (WᵀV) / (WᵀW·H)
        let mut wtv = vec![vec![0.0; vocab_size]; k];
        for d in 0..n_docs {
            for t in 0..k {
                let wdt = w_mat[d][t];
                if wdt == 0.0 {
                    continue;
                }
                for j in 0..vocab_size {
                    wtv[t][j] += wdt * v[d][j];
                }
            }
        }
        let mut wtw = vec![vec![0.0; k]; k];
        for d in 0..n_docs {
            for a in 0..k {
                for b in 0..k {
                    wtw[a][b] += w_mat[d][a] * w_mat[d][b];
                }
            }
        }
        let mut wtwh = vec![vec![0.0; vocab_size]; k];
        for a in 0..k {
            for b in 0..k {
                let wab = wtw[a][b];
                if wab == 0.0 {
                    continue;
                }
                for j in 0..vocab_size {
                    wtwh[a][j] += wab * h_mat[b][j];
                }
            }
        }
        for a in 0..k {
            for j in 0..vocab_size {
                h_mat[a][j] *= wtv[a][j] / (wtwh[a][j] + EPS);
            }
        }

        // W *= (V·Hᵀ) / (W·H·Hᵀ)
        let mut vht = vec![vec![0.0; k]; n_docs];
        for d in 0..n_docs {
            for a in 0..k {
                let mut s = 0.0;
                for j in 0..vocab_size {
                    s += v[d][j] * h_mat[a][j];
                }
                vht[d][a] = s;
            }
        }
        let mut hht = vec![vec![0.0; k]; k];
        for a in 0..k {
            for b in 0..k {
                let mut s = 0.0;
                for j in 0..vocab_size {
                    s += h_mat[a][j] * h_mat[b][j];
                }
                hht[a][b] = s;
            }
        }
        let mut whht = vec![vec![0.0; k]; n_docs];
        for d in 0..n_docs {
            for a in 0..k {
                let mut s = 0.0;
                for b in 0..k {
                    s += w_mat[d][b] * hht[b][a];
                }
                whht[d][a] = s;
            }
        }
        for d in 0..n_docs {
            for a in 0..k {
                w_mat[d][a] *= vht[d][a] / (whht[d][a] + EPS);
            }
        }
    }

    let mut topics: Vec<Vec<(TermId, f64)>> = Vec::with_capacity(k);
    for a in 0..k {
        let mut terms: Vec<(TermId, f64)> = (0..vocab_size).map(|j| (j as TermId, h_mat[a][j])).collect();
        terms.sort_by(|x, y| y.1.partial_cmp(&x.1).unwrap_or(std::cmp::Ordering::Equal).then(x.0.cmp(&y.0)));
        topics.push(terms);
    }
    let doc_topics: Vec<Vec<f64>> = w_mat
        .iter()
        .map(|row| {
            let s: f64 = row.iter().sum();
            if s > 0.0 {
                row.iter().map(|&x| x / s).collect()
            } else {
                vec![0.0; k]
            }
        })
        .collect();
    (topics, doc_topics)
}

/// Run the chosen text-mining engine.
pub fn mine(docs: &[Vec<TermId>], vocab_size: usize, algorithm: Algorithm) -> TextResult {
    match algorithm {
        Algorithm::Tfidf => TextResult {
            doc_terms: tfidf(docs, vocab_size),
            topics: Vec::new(),
            doc_topics: Vec::new(),
        },
        Algorithm::Lda { k, alpha, beta, iterations, seed } => {
            let (topics, doc_topics) = lda(docs, vocab_size, k, alpha, beta, iterations, seed);
            TextResult { doc_terms: Vec::new(), topics, doc_topics }
        }
        Algorithm::Nmf { k, iterations, seed } => {
            let (topics, doc_topics) = nmf(docs, vocab_size, k, iterations, seed);
            TextResult { doc_terms: Vec::new(), topics, doc_topics }
        }
    }
}

// ─────────────────────────── String-labeled convenience ───────────────────────────

/// Intern string documents (already tokenized) into dense [`TermId`]s,
/// preserving a stable id↔label mapping (first-seen order) — mirrors
/// `association::intern`/`sequence::intern`.
pub fn intern(docs: &[Vec<String>]) -> (Vec<Vec<TermId>>, Vec<String>) {
    let mut labels: Vec<String> = Vec::new();
    let mut index: HashMap<String, TermId> = HashMap::new();
    let mut out: Vec<Vec<TermId>> = Vec::with_capacity(docs.len());
    for doc in docs {
        let mut row: Vec<TermId> = Vec::with_capacity(doc.len());
        for term in doc {
            let id = *index.entry(term.clone()).or_insert_with(|| {
                let id = labels.len() as TermId;
                labels.push(term.clone());
                id
            });
            row.push(id);
        }
        out.push(row);
    }
    (out, labels)
}

/// String-labeled mining result (the wire/row shape the handler and client
/// see). `top_n` caps how many terms are kept per document/topic row (the full
/// vocabulary weight vector is rarely useful to a caller).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct LabeledTextResult {
    pub doc_terms: Vec<Vec<(String, f64)>>,
    pub topics: Vec<Vec<(String, f64)>>,
    pub doc_topics: Vec<Vec<f64>>,
}

/// Mine string-labeled documents: intern → mine → relabel (top `top_n` terms
/// per row).
pub fn mine_labeled(docs: &[Vec<String>], algorithm: Algorithm, top_n: usize) -> LabeledTextResult {
    let (interned, labels) = intern(docs);
    let result = mine(&interned, labels.len(), algorithm);
    let cap = top_n.max(1);
    let relabel = |v: Vec<(TermId, f64)>| -> Vec<(String, f64)> {
        v.into_iter().take(cap).map(|(t, w)| (labels[t as usize].clone(), w)).collect()
    };
    LabeledTextResult {
        doc_terms: result.doc_terms.into_iter().map(relabel).collect(),
        topics: result.topics.into_iter().map(relabel).collect(),
        doc_topics: result.doc_topics,
    }
}

/// A tiny deterministic splitmix64 PRNG — keeps LDA/NMF init dependency-free
/// (mirrors `cluster.rs`/`reduce.rs`'s hand-rolled generator).
struct SplitMix64 {
    state: u64,
}
impl SplitMix64 {
    fn new(seed: u64) -> Self {
        SplitMix64 { state: seed.wrapping_add(0x9E37_79B9_7F4A_7C15) }
    }
    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn words(s: &str) -> Vec<String> {
        s.split_whitespace().map(|w| w.to_string()).collect()
    }

    #[test]
    fn tokenize_lowercases_and_splits_on_punctuation() {
        assert_eq!(
            tokenize("Hello, World! It's 2026."),
            vec!["hello", "world", "it", "s", "2026"]
        );
    }

    #[test]
    fn tfidf_downweights_a_term_present_in_every_doc() {
        // "the" appears in every doc (idf ~ln(1)+1=1); "rocket" is rare (higher idf).
        let docs = vec![
            words("the cat sat on the mat"),
            words("the dog ran in the park"),
            words("the rocket launched into orbit"),
        ];
        let out = mine_labeled(&docs, Algorithm::Tfidf, 10);
        // In doc 2 ("rocket" doc), "rocket" must outrank "the" (idf(rocket) > idf(the)).
        let doc2 = &out.doc_terms[2];
        let rank = |term: &str| doc2.iter().position(|(t, _)| t == term);
        assert!(rank("rocket") < rank("the"), "rocket should outrank the common term 'the'");
    }

    /// Two disjoint-vocabulary document groups (pets vs. finance) — LDA at k=2
    /// must recover each group as its own topic (fixed seed).
    #[test]
    fn lda_recovers_planted_topics() {
        let pet_words = ["cat", "dog", "pet", "leash", "vet"];
        let fin_words = ["stock", "market", "bond", "yield", "trader"];
        let mut docs: Vec<Vec<String>> = Vec::new();
        for i in 0..15 {
            // Deterministic, varying-length documents from each vocabulary.
            let n = 6 + (i % 4);
            docs.push((0..n).map(|j| pet_words[(i + j) % pet_words.len()].to_string()).collect());
            docs.push((0..n).map(|j| fin_words[(i + j) % fin_words.len()].to_string()).collect());
        }
        let out = mine_labeled(
            &docs,
            Algorithm::Lda { k: 2, alpha: 0.1, beta: 0.01, iterations: 200, seed: 42 },
            5,
        );
        assert_eq!(out.topics.len(), 2);
        for topic in &out.topics {
            let top_terms: Vec<&str> = topic.iter().map(|(t, _)| t.as_str()).collect();
            let pet_hits = top_terms.iter().filter(|t| pet_words.contains(t)).count();
            let fin_hits = top_terms.iter().filter(|t| fin_words.contains(t)).count();
            // Each recovered topic should be dominated by ONE vocabulary, not a mix.
            assert!(
                pet_hits == 0 || fin_hits == 0,
                "topic mixed vocabularies: {top_terms:?}"
            );
            assert!(pet_hits > 0 || fin_hits > 0);
        }
    }

    #[test]
    fn nmf_recovers_planted_topics() {
        let pet_words = ["cat", "dog", "pet", "leash", "vet"];
        let fin_words = ["stock", "market", "bond", "yield", "trader"];
        let mut docs: Vec<Vec<String>> = Vec::new();
        for i in 0..15 {
            let n = 6 + (i % 4);
            docs.push((0..n).map(|j| pet_words[(i + j) % pet_words.len()].to_string()).collect());
            docs.push((0..n).map(|j| fin_words[(i + j) % fin_words.len()].to_string()).collect());
        }
        let out = mine_labeled(&docs, Algorithm::Nmf { k: 2, iterations: 200, seed: 7 }, 5);
        assert_eq!(out.topics.len(), 2);
        for topic in &out.topics {
            let top_terms: Vec<&str> = topic.iter().map(|(t, _)| t.as_str()).collect();
            let pet_hits = top_terms.iter().filter(|t| pet_words.contains(t)).count();
            let fin_hits = top_terms.iter().filter(|t| fin_words.contains(t)).count();
            assert!(
                pet_hits == 0 || fin_hits == 0,
                "topic mixed vocabularies: {top_terms:?}"
            );
            assert!(pet_hits > 0 || fin_hits > 0);
        }
    }

    #[test]
    fn doc_topics_distribution_sums_to_one() {
        let docs = vec![
            words("cat dog pet leash vet cat dog"),
            words("stock market bond yield trader stock market"),
        ];
        let out = mine_labeled(&docs, Algorithm::Lda { k: 2, alpha: 0.1, beta: 0.01, iterations: 100, seed: 3 }, 5);
        for dist in &out.doc_topics {
            let sum: f64 = dist.iter().sum();
            assert!((sum - 1.0).abs() < 1e-9, "doc-topic distribution should sum to 1, got {sum}");
        }
    }

    #[test]
    fn labeled_roundtrip_produces_string_terms() {
        let docs = vec![words("alpha beta gamma"), words("beta gamma delta")];
        let out = mine_labeled(&docs, Algorithm::Tfidf, 10);
        assert_eq!(out.doc_terms.len(), 2);
        for row in &out.doc_terms {
            for (term, _) in row {
                assert!(["alpha", "beta", "gamma", "delta"].contains(&term.as_str()));
            }
        }
    }
}
