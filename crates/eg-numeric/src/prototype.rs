//! Bounded detached prototype matching for semantic subsumption adapters.
//! The caller retains ontology labels and lineage; this kernel owns only the
//! vector comparison and first-wins argmax.

use crate::{linalg, Result};
use ndarray::ArrayView1;

/// Return the first prototype with the largest strictly positive cosine score.
/// Empty and zero-norm vectors score zero. A nonzero, mismatched row has the
/// same shape error as `linalg::dot`. Only actual zero rows are exempt from
/// shape validation: an underflowed norm must not hide a nonzero ragged row.
/// Norms and dot products retain the existing kernel's IEEE arithmetic:
/// NaN scores never win; overflowing or underflowing norms are not rescaled.
pub fn best_cosine_prototype(
    query: &[f64],
    prototypes: &[Vec<f64>],
) -> Result<Option<(usize, f64)>> {
    if query.iter().all(|value| *value == 0.0) {
        return Ok(None);
    }
    let query_norm = linalg::norm(ArrayView1::from(query));
    let mut best: Option<(usize, f64)> = None;
    for (index, prototype) in prototypes.iter().enumerate() {
        if prototype.iter().all(|value| *value == 0.0) {
            continue;
        }
        // Validate shape before a nonzero vector's norm can underflow to zero.
        let dot = linalg::dot(ArrayView1::from(query), ArrayView1::from(prototype))?;
        let prototype_norm = linalg::norm(ArrayView1::from(prototype));
        if query_norm == 0.0 || prototype_norm == 0.0 {
            continue;
        }
        let score = dot / (query_norm * prototype_norm);
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

    #[test]
    fn empty_and_nonpositive_inputs_have_no_match() {
        for query in [vec![], vec![0.0, 0.0], vec![1.0, 0.0]] {
            assert_eq!(best_cosine_prototype(&query, &[]).unwrap(), None);
            assert_eq!(best_cosine_prototype(&query, &[vec![]]).unwrap(), None);
        }
        assert_eq!(best_cosine_prototype(&[], &[vec![1.0]]).unwrap(), None);
        assert_eq!(
            best_cosine_prototype(&[0.0], &[vec![1.0, 2.0]]).unwrap(),
            None
        );
        assert_eq!(
            best_cosine_prototype(&[1.0, 0.0], &[vec![0.0, 1.0]]).unwrap(),
            None
        );
    }

    #[test]
    fn later_maximum_wins_but_later_shape_errors_are_not_hidden() {
        let rows = vec![vec![3.0, 4.0], vec![1.0, 0.0], vec![2.0, 0.0]];
        assert_eq!(
            best_cosine_prototype(&[1.0, 0.0], &rows).unwrap(),
            Some((1, 1.0))
        );
        assert!(matches!(
            best_cosine_prototype(&[1.0, 0.0], &[vec![1.0, 0.0], vec![1.0]]),
            Err(crate::NumericError::Shape(_))
        ));
    }

    #[test]
    fn ieee_scores_follow_existing_norm_and_dot_policy() {
        for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, f64::MAX, 1e-300] {
            assert_eq!(
                best_cosine_prototype(&[value], &[vec![value]]).unwrap(),
                None
            );
        }
        let rows = vec![vec![f64::NAN, 0.0], vec![1.0, 0.0]];
        assert_eq!(
            best_cosine_prototype(&[1.0, 0.0], &rows).unwrap(),
            Some((1, 1.0))
        );
        assert!(best_cosine_prototype(&[f64::NAN, 0.0], &[vec![1.0]]).is_err());
    }

    #[test]
    fn norm_underflow_does_not_hide_nonzero_shape_errors() {
        for (query, row) in [
            (vec![1.0, 0.0], vec![1e-300]),
            (vec![1e-300], vec![1.0, 0.0]),
        ] {
            assert!(matches!(
                best_cosine_prototype(&query, &[row]),
                Err(crate::NumericError::Shape(_))
            ));
        }
    }
}
