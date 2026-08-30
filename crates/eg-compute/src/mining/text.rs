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

// The topic-model matrix math (LDA count matrices, NMF multiplicative updates on
// the term-document matrix) reads more clearly with explicit `for d in 0..n_docs
// { m[d][t] }` indexing than enumerate/zip rewrites. Scope the lint to this
// compute module rather than contorting the math.
#![allow(clippy::needless_range_loop)]

use std::collections::HashMap;

/// An interned term id (small, dense — assigned by [`intern`]).
pub type TermId = u32;

/// A fitted topic model: `(topic-term weights, per-document topic distribution)`
/// — `topics[k]` is topic `k`'s sparse term weights, `doc_topics[d]` is document
/// `d`'s dense topic mixture.
type TopicModel = (Vec<Vec<(TermId, f64)>>, Vec<Vec<f64>>);

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
            terms.sort_by(|a, b| {
                b.1.partial_cmp(&a.1)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then(a.0.cmp(&b.0))
            });
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
) -> TopicModel {
    let n_docs = docs.len();
    if k == 0 || vocab_size == 0 || n_docs == 0 {
        return (Vec::new(), Vec::new());
    }
    let params = LdaParams {
        k,
        alpha,
        beta,
        vocab_size,
    };
    let (mut state, mut rng) = initialize_lda_state(docs, &params, seed);
    run_lda_iterations(docs, &params, iterations, &mut rng, &mut state);
    let topics = lda_topic_terms(&state, &params);
    let doc_topics = lda_doc_topics(&state, &params, n_docs);
    (topics, doc_topics)
}

#[derive(Clone, Copy)]
struct LdaParams {
    k: usize,
    alpha: f64,
    beta: f64,
    vocab_size: usize,
}

struct LdaState {
    assignments: Vec<Vec<usize>>,
    doc_topic_counts: Vec<Vec<u32>>,
    topic_term_counts: Vec<Vec<u32>>,
    topic_totals: Vec<u32>,
}

/// Seed assignments and count matrices from the same stream used by sampling.
fn initialize_lda_state(
    docs: &[Vec<TermId>],
    params: &LdaParams,
    seed: u64,
) -> (LdaState, SplitMix64) {
    let n_docs = docs.len();
    let mut rng = SplitMix64::new(seed);
    let mut state = LdaState {
        assignments: docs.iter().map(|doc| vec![0usize; doc.len()]).collect(),
        doc_topic_counts: vec![vec![0u32; params.k]; n_docs],
        topic_term_counts: vec![vec![0u32; params.vocab_size]; params.k],
        topic_totals: vec![0u32; params.k],
    };
    for (doc_index, doc) in docs.iter().enumerate() {
        for (token_index, &term) in doc.iter().enumerate() {
            let topic = (rng.next_u64() as usize) % params.k;
            state.assignments[doc_index][token_index] = topic;
            state.doc_topic_counts[doc_index][topic] += 1;
            state.topic_term_counts[topic][term as usize] += 1;
            state.topic_totals[topic] += 1;
        }
    }
    (state, rng)
}

/// Perform the requested number of deterministic collapsed-Gibbs sweeps.
fn run_lda_iterations(
    docs: &[Vec<TermId>],
    params: &LdaParams,
    iterations: usize,
    rng: &mut SplitMix64,
    state: &mut LdaState,
) {
    for _ in 0..iterations {
        for (doc_index, doc) in docs.iter().enumerate() {
            for token_index in 0..doc.len() {
                resample_lda_token(doc, doc_index, token_index, params, rng, state);
            }
        }
    }
}

/// Remove one token's old assignment, sample its new topic, and restore counts.
fn resample_lda_token(
    doc: &[TermId],
    doc_index: usize,
    token_index: usize,
    params: &LdaParams,
    rng: &mut SplitMix64,
    state: &mut LdaState,
) {
    let term = doc[token_index] as usize;
    let old_topic = state.assignments[doc_index][token_index];
    state.doc_topic_counts[doc_index][old_topic] -= 1;
    state.topic_term_counts[old_topic][term] -= 1;
    state.topic_totals[old_topic] -= 1;

    let new_topic = sample_lda_topic(
        &state.doc_topic_counts[doc_index],
        &state.topic_term_counts,
        &state.topic_totals,
        term,
        params,
        rng,
    );
    state.assignments[doc_index][token_index] = new_topic;
    state.doc_topic_counts[doc_index][new_topic] += 1;
    state.topic_term_counts[new_topic][term] += 1;
    state.topic_totals[new_topic] += 1;
}

/// Sample one topic from the collapsed-Gibbs cumulative probability row.
fn sample_lda_topic(
    doc_counts: &[u32],
    term_counts: &[Vec<u32>],
    topic_totals: &[u32],
    term: usize,
    params: &LdaParams,
    rng: &mut SplitMix64,
) -> usize {
    let mut cumulative = vec![0.0; params.k];
    let mut running = 0.0;
    for topic in 0..params.k {
        let probability = (doc_counts[topic] as f64 + params.alpha)
            * (term_counts[topic][term] as f64 + params.beta)
            / (topic_totals[topic] as f64 + params.vocab_size as f64 * params.beta);
        running += probability;
        cumulative[topic] = running;
    }
    let draw = rng.next_f64() * running;
    cumulative
        .iter()
        .position(|&value| draw <= value)
        .unwrap_or(params.k - 1)
}

/// Convert topic-term count rows into stable, descending term distributions.
fn lda_topic_terms(state: &LdaState, params: &LdaParams) -> Vec<Vec<(TermId, f64)>> {
    (0..params.k)
        .map(|topic| {
            let denominator =
                state.topic_totals[topic] as f64 + params.vocab_size as f64 * params.beta;
            let weights: Vec<f64> = (0..params.vocab_size)
                .map(|term| {
                    (state.topic_term_counts[topic][term] as f64 + params.beta) / denominator
                })
                .collect();
            sorted_term_weights(&weights)
        })
        .collect()
}

/// Convert document-topic counts into each document's topic mixture.
fn lda_doc_topics(state: &LdaState, params: &LdaParams, n_docs: usize) -> Vec<Vec<f64>> {
    (0..n_docs)
        .map(|doc| {
            let denominator = state.doc_topic_counts[doc].iter().sum::<u32>() as f64
                + params.k as f64 * params.alpha;
            (0..params.k)
                .map(|topic| {
                    (state.doc_topic_counts[doc][topic] as f64 + params.alpha) / denominator
                })
                .collect()
        })
        .collect()
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
) -> TopicModel {
    let n_docs = docs.len();
    if k == 0 || vocab_size == 0 || n_docs == 0 {
        return (Vec::new(), Vec::new());
    }
    let v = tfidf_matrix(docs, vocab_size);
    let (mut w_mat, mut h_mat) = initialize_nmf_factors(n_docs, vocab_size, k, seed);
    run_nmf_updates(&v, &mut w_mat, &mut h_mat, iterations);
    let topics = topic_terms(&h_mat);
    let doc_topics = normalized_topics(&w_mat, k);
    (topics, doc_topics)
}

/// Materialize the TF-IDF rows as the dense, non-negative matrix NMF consumes.
fn tfidf_matrix(docs: &[Vec<TermId>], vocab_size: usize) -> Vec<Vec<f64>> {
    let tfidf_rows = tfidf(docs, vocab_size);
    let mut matrix = vec![vec![0.0; vocab_size]; docs.len()];
    for (d, row) in tfidf_rows.iter().enumerate() {
        for &(term, weight) in row {
            matrix[d][term as usize] = weight;
        }
    }
    matrix
}

/// Initialize NMF's positive factors from one deterministic random stream.
fn initialize_nmf_factors(
    n_docs: usize,
    vocab_size: usize,
    k: usize,
    seed: u64,
) -> (Vec<Vec<f64>>, Vec<Vec<f64>>) {
    let mut rng = SplitMix64::new(seed);
    let mut w_mat = vec![vec![0.0; k]; n_docs];
    let mut h_mat = vec![vec![0.0; vocab_size]; k];
    initialize_factor(&mut w_mat, &mut rng);
    initialize_factor(&mut h_mat, &mut rng);
    (w_mat, h_mat)
}

/// Fill one factor with the strictly positive values used by multiplicative updates.
fn initialize_factor(matrix: &mut [Vec<f64>], rng: &mut SplitMix64) {
    for row in matrix.iter_mut() {
        for value in row.iter_mut() {
            *value = 0.1 + rng.next_f64();
        }
    }
}

/// Run the fixed number of deterministic Lee–Seung updates.
fn run_nmf_updates(
    v: &[Vec<f64>],
    w_mat: &mut [Vec<f64>],
    h_mat: &mut [Vec<f64>],
    iterations: usize,
) {
    for _ in 0..iterations {
        update_h_factor(v, w_mat, h_mat);
        update_w_factor(v, w_mat, h_mat);
    }
}

/// Apply the H update: `H *= (WᵀV) / (WᵀW·H)`.
fn update_h_factor(v: &[Vec<f64>], w_mat: &[Vec<f64>], h_mat: &mut [Vec<f64>]) {
    let w_transposed = transpose_matrix(w_mat);
    let wtv = matrix_product(&w_transposed, v, true);
    let wtw = matrix_product(&w_transposed, w_mat, false);
    let wtwh = matrix_product(&wtw, h_mat, true);
    apply_multiplicative_update(h_mat, &wtv, &wtwh);
}

/// Apply the W update: `W *= (V·Hᵀ) / (W·H·Hᵀ)`.
fn update_w_factor(v: &[Vec<f64>], w_mat: &mut [Vec<f64>], h_mat: &[Vec<f64>]) {
    let h_transposed = transpose_matrix(h_mat);
    let vht = matrix_product(v, &h_transposed, false);
    let hht = matrix_product(h_mat, &h_transposed, false);
    let whht = matrix_product(w_mat, &hht, false);
    apply_multiplicative_update(w_mat, &vht, &whht);
}

/// Multiply two dense matrices, optionally skipping zero left-hand values.
/// The flag retains NMF's original sparse-product behavior without duplicating
/// the matrix traversal for the two multiplicative-update numerators.
fn matrix_product(left: &[Vec<f64>], right: &[Vec<f64>], skip_zero: bool) -> Vec<Vec<f64>> {
    let rows = left.len();
    let columns = right[0].len();
    let mut product = vec![vec![0.0; columns]; rows];
    for (row_index, product_row) in product.iter_mut().enumerate() {
        for (inner_index, right_row) in right.iter().enumerate() {
            let left_value = left[row_index][inner_index];
            if skip_zero && left_value == 0.0 {
                continue;
            }
            for column in 0..columns {
                product_row[column] += left_value * right_row[column];
            }
        }
    }
    product
}

/// Transpose a rectangular dense matrix while preserving row/column order.
fn transpose_matrix(matrix: &[Vec<f64>]) -> Vec<Vec<f64>> {
    let rows = matrix.len();
    let columns = matrix[0].len();
    let mut transposed = vec![vec![0.0; rows]; columns];
    for row in 0..rows {
        for column in 0..columns {
            transposed[column][row] = matrix[row][column];
        }
    }
    transposed
}

/// Apply one Lee–Seung ratio update with the shared numerical safeguard.
fn apply_multiplicative_update(
    factor: &mut [Vec<f64>],
    numerator: &[Vec<f64>],
    denominator: &[Vec<f64>],
) {
    const EPS: f64 = 1e-10;
    for row in 0..factor.len() {
        for column in 0..factor[row].len() {
            factor[row][column] *= numerator[row][column] / (denominator[row][column] + EPS);
        }
    }
}

/// Sort each H row by descending weight, breaking ties by stable term id.
fn topic_terms(h_mat: &[Vec<f64>]) -> Vec<Vec<(TermId, f64)>> {
    h_mat.iter().map(|row| sorted_term_weights(row)).collect()
}

/// Sort one term-weight row by descending weight and stable term id.
fn sorted_term_weights(weights: &[f64]) -> Vec<(TermId, f64)> {
    let mut terms: Vec<(TermId, f64)> = weights
        .iter()
        .enumerate()
        .map(|(term, &weight)| (term as TermId, weight))
        .collect();
    terms.sort_by(|left, right| {
        right
            .1
            .partial_cmp(&left.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(left.0.cmp(&right.0))
    });
    terms
}

/// Normalize each W row to a document-topic distribution.
fn normalized_topics(w_mat: &[Vec<f64>], k: usize) -> Vec<Vec<f64>> {
    w_mat
        .iter()
        .map(|row| {
            let sum: f64 = row.iter().sum();
            if sum > 0.0 {
                row.iter().map(|&value| value / sum).collect()
            } else {
                vec![0.0; k]
            }
        })
        .collect()
}

/// Run the chosen text-mining engine.
pub fn mine(docs: &[Vec<TermId>], vocab_size: usize, algorithm: Algorithm) -> TextResult {
    match algorithm {
        Algorithm::Tfidf => TextResult {
            doc_terms: tfidf(docs, vocab_size),
            topics: Vec::new(),
            doc_topics: Vec::new(),
        },
        Algorithm::Lda {
            k,
            alpha,
            beta,
            iterations,
            seed,
        } => {
            let (topics, doc_topics) = lda(docs, vocab_size, k, alpha, beta, iterations, seed);
            TextResult {
                doc_terms: Vec::new(),
                topics,
                doc_topics,
            }
        }
        Algorithm::Nmf {
            k,
            iterations,
            seed,
        } => {
            let (topics, doc_topics) = nmf(docs, vocab_size, k, iterations, seed);
            TextResult {
                doc_terms: Vec::new(),
                topics,
                doc_topics,
            }
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
        v.into_iter()
            .take(cap)
            .map(|(t, w)| (labels[t as usize].clone(), w))
            .collect()
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
        SplitMix64 {
            state: seed.wrapping_add(0x9E37_79B9_7F4A_7C15),
        }
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
        assert!(
            rank("rocket") < rank("the"),
            "rocket should outrank the common term 'the'"
        );
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
            docs.push(
                (0..n)
                    .map(|j| pet_words[(i + j) % pet_words.len()].to_string())
                    .collect(),
            );
            docs.push(
                (0..n)
                    .map(|j| fin_words[(i + j) % fin_words.len()].to_string())
                    .collect(),
            );
        }
        let out = mine_labeled(
            &docs,
            Algorithm::Lda {
                k: 2,
                alpha: 0.1,
                beta: 0.01,
                iterations: 200,
                seed: 42,
            },
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
            docs.push(
                (0..n)
                    .map(|j| pet_words[(i + j) % pet_words.len()].to_string())
                    .collect(),
            );
            docs.push(
                (0..n)
                    .map(|j| fin_words[(i + j) % fin_words.len()].to_string())
                    .collect(),
            );
        }
        let out = mine_labeled(
            &docs,
            Algorithm::Nmf {
                k: 2,
                iterations: 200,
                seed: 7,
            },
            5,
        );
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
        let out = mine_labeled(
            &docs,
            Algorithm::Lda {
                k: 2,
                alpha: 0.1,
                beta: 0.01,
                iterations: 100,
                seed: 3,
            },
            5,
        );
        for dist in &out.doc_topics {
            let sum: f64 = dist.iter().sum();
            assert!(
                (sum - 1.0).abs() < 1e-9,
                "doc-topic distribution should sum to 1, got {sum}"
            );
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
