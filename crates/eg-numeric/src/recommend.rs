//! Semantic-id item recommendation (EG-DECISION-ENGINE-R175.2).
//!
//! [`codebook`] fits a residual quantizer with seeded k-means per level (at
//! most [`MAX_LEVELS`] levels of at most [`MAX_CODES`] codes) and encodes a
//! vector into its code tuple in O(L·C·d). [`rank`] indexes a catalog by code
//! prefix, refines a query vector over `p` pause steps toward the caller's
//! history and its nearest bucket items (O(p·(h + m)·d) plus the bucket
//! size), then ranks the candidate bucket by cosine. Everything is
//! deterministic for a given seed: ties break by catalog position.

pub mod codebook;
pub mod rank;

pub use codebook::{ResidualQuantizer, MAX_CODES, MAX_LEVELS};
pub use rank::{Ranked, RecommendIndex};

/// Why a fit, encode or ranking was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecommendError {
    /// The catalog holds no item.
    EmptyCatalog,
    /// A vector's dimension differs from the catalog's.
    DimensionMismatch { expected: usize, found: usize },
    /// Levels or codes outside 1..=4 and 1..=256.
    BadShape,
}

impl std::fmt::Display for RecommendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyCatalog => write!(f, "empty_catalog"),
            Self::DimensionMismatch { expected, found } => {
                write!(f, "dimension mismatch: expected {expected}, found {found}")
            }
            Self::BadShape => write!(f, "levels must be 1 to 4 and codes 1 to 256"),
        }
    }
}

impl std::error::Error for RecommendError {}

pub(crate) fn dot(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b).map(|(x, y)| x * y).sum()
}

pub(crate) fn sq_dist(a: &[f32], b: &[f32]) -> f32 {
    a.iter().zip(b).map(|(x, y)| (x - y) * (x - y)).sum()
}

pub(crate) fn cosine(a: &[f32], b: &[f32]) -> f32 {
    let norm = (dot(a, a) * dot(b, b)).sqrt();
    if norm > 0.0 {
        dot(a, b) / norm
    } else {
        0.0
    }
}

pub(crate) fn normalized(mut v: Vec<f32>) -> Vec<f32> {
    let norm = dot(&v, &v).sqrt();
    if norm > 0.0 {
        v.iter_mut().for_each(|x| *x /= norm);
    }
    v
}

#[cfg(test)]
mod tests;
