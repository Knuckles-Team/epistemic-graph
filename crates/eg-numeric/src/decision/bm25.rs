//! Candidate-local BM25.
//!
//! Document frequency and average length come from the CANDIDATE SET ONLY,
//! never from a global index: with global statistics an invisible document
//! moves every visible candidate's score, which is both a leak and a replay
//! hazard. Here the score of a visible candidate is a pure function of the
//! visible candidates and the query.

use std::collections::BTreeMap;

use crate::detkernel::math;

/// Term-frequency saturation.
pub const K1: f64 = 1.2;
/// Length normalisation.
pub const B: f64 = 0.75;

/// Lower-cased alphanumeric tokens, in order.
pub fn tokens(text: &str) -> Vec<String> {
    text.split(|c: char| !c.is_alphanumeric())
        .filter(|token| !token.is_empty())
        .map(str::to_lowercase)
        .collect()
}

fn term_counts(tokens: &[String]) -> BTreeMap<&str, u64> {
    let mut counts = BTreeMap::new();
    for token in tokens {
        *counts.entry(token.as_str()).or_insert(0) += 1;
    }
    counts
}

fn idf(documents: u64, containing: u64) -> f64 {
    let (n, df) = (documents as f64, containing as f64);
    math::ln_1p((n - df + 0.5) / (df + 0.5))
}

/// BM25 of every document against `query`, statistics taken over `documents`.
pub fn scores(documents: &[&str], query: &str) -> Vec<f64> {
    let tokenised: Vec<Vec<String>> = documents.iter().map(|d| tokens(d)).collect();
    let counts: Vec<BTreeMap<&str, u64>> = tokenised.iter().map(|t| term_counts(t)).collect();
    let total_len: u64 = tokenised.iter().map(|t| t.len() as u64).sum();
    let n = documents.len() as u64;
    let avgdl = if n == 0 {
        0.0
    } else {
        total_len as f64 / n as f64
    };
    let mut query_terms = tokens(query);
    query_terms.sort_unstable();
    query_terms.dedup();
    let weights: Vec<(String, f64)> = query_terms
        .into_iter()
        .map(|term| {
            let df = counts
                .iter()
                .filter(|c| c.contains_key(term.as_str()))
                .count() as u64;
            let weight = idf(n, df);
            (term, weight)
        })
        .collect();
    tokenised
        .iter()
        .zip(&counts)
        .map(|(doc, tf)| document_score(&weights, tf, doc.len() as f64, avgdl))
        .collect()
}

fn document_score(
    weights: &[(String, f64)],
    tf: &BTreeMap<&str, u64>,
    len: f64,
    avgdl: f64,
) -> f64 {
    let norm = if avgdl > 0.0 { len / avgdl } else { 0.0 };
    let mut total = 0.0;
    for (term, weight) in weights {
        let frequency = tf.get(term.as_str()).copied().unwrap_or(0) as f64;
        if frequency > 0.0 {
            total += weight * frequency * (K1 + 1.0) / (frequency + K1 * (1.0 - B + B * norm));
        }
    }
    total
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_matching_document_outscores_a_non_matching_one() {
        let docs = ["search the web for papers", "write a file to disk"];
        let s = scores(&docs, "web search");
        assert!(s[0] > s[1]);
        assert_eq!(s[1], 0.0);
    }

    #[test]
    fn scores_depend_only_on_the_candidate_set() {
        let visible = ["search the web", "read a document"];
        let first = scores(&visible, "search");
        let again = scores(&visible, "search");
        assert_eq!(first, again);
    }
}
