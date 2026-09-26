//! Bounded detached prototype matching for semantic subsumption adapters.
//! The caller retains ontology labels and lineage; this kernel owns only the
//! vector comparison and first-wins argmax.

use crate::{linalg, Result};
use ndarray::ArrayView1;

/// Return the first prototype with the largest strictly positive cosine score.
/// Empty and zero-norm vectors score zero. A nonzero, mismatched row has the
/// same shape error as `linalg::dot`, reached only after the norm guards.
pub fn best_cosine_prototype(
    query: &[f64],
    prototypes: &[Vec<f64>],
) -> Result<Option<(usize, f64)>> {
    if query.is_empty() {
        return Ok(None);
    }
    let query_norm = linalg::norm(ArrayView1::from(query));
    let mut best: Option<(usize, f64)> = None;
    for (index, prototype) in prototypes.iter().enumerate() {
        if prototype.is_empty() {
            continue;
        }
        let prototype_norm = linalg::norm(ArrayView1::from(prototype));
        if query_norm == 0.0 || prototype_norm == 0.0 {
            continue;
        }
        let score = linalg::dot(ArrayView1::from(query), ArrayView1::from(prototype))?
            / (query_norm * prototype_norm);
        if score > best.map_or(0.0, |(_, prior)| prior) {
            best = Some((index, score));
        }
    }
    Ok(best)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_positive_maximum_and_zero_rows() {
        let rows = vec![vec![0.0, 0.0], vec![1.0, 0.0], vec![1.0, 0.0]];
        assert_eq!(
            best_cosine_prototype(&[1.0, 0.0], &rows).unwrap(),
            Some((1, 1.0))
        );
        assert_eq!(best_cosine_prototype(&[0.0, 0.0], &rows).unwrap(), None);
        assert_eq!(
            best_cosine_prototype(&[1.0, 0.0], &[vec![-1.0, 0.0]]).unwrap(),
            None
        );
    }

    #[test]
    fn ragged_zero_row_is_harmless_but_nonzero_mismatch_errors() {
        assert_eq!(
            best_cosine_prototype(&[1.0, 0.0], &[vec![0.0]]).unwrap(),
            None
        );
        assert!(best_cosine_prototype(&[1.0, 0.0], &[vec![1.0]]).is_err());
    }
}
