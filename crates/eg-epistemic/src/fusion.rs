//! Domain-neutral, bounded Bayesian fusion of directional evidence.
//!
//! Callers supply reliability learned from independent outcomes (or a prior
//! when no estimate is available). This kernel never reads an authority store.

/// One directional observation and its source's calibrated reliability.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Evidence {
    pub direction: i8,
    pub reliability: f64,
    pub weight: f64,
}

/// A malformed fusion request; no partial posterior is returned.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FusionError {
    InvalidPrior,
    InvalidReliability,
    InvalidWeight,
    InvalidDirection,
    TooManyObservations,
}

fn probability(value: f64) -> bool {
    value.is_finite() && (0.0..=1.0).contains(&value)
}

/// Fuse at most 1,024 observations in caller-declared order.
pub fn fuse(prior: f64, evidence: &[Evidence]) -> Result<f64, FusionError> {
    if !probability(prior) {
        return Err(FusionError::InvalidPrior);
    }
    if evidence.len() > 1024 {
        return Err(FusionError::TooManyObservations);
    }
    let mut posterior = prior;
    for item in evidence {
        if !probability(item.reliability) {
            return Err(FusionError::InvalidReliability);
        }
        if !probability(item.weight) {
            return Err(FusionError::InvalidWeight);
        }
        if !(-1..=1).contains(&item.direction) {
            return Err(FusionError::InvalidDirection);
        }
        if item.direction == 0 {
            continue;
        }
        let likely_up = if item.direction > 0 {
            item.reliability
        } else {
            1.0 - item.reliability
        };
        let likely_down = 1.0 - likely_up;
        let normalizer = likely_up * posterior + likely_down * (1.0 - posterior);
        if normalizer > 0.0 {
            let updated = likely_up * posterior / normalizer;
            posterior += item.weight * (updated - posterior);
        }
    }
    Ok(posterior)
}

#[cfg(test)]
mod tests {
    use super::{fuse, Evidence, FusionError};

    #[test]
    fn evidence_fusion_is_bounded_and_ordered() {
        let evidence = [
            Evidence {
                direction: 1,
                reliability: 0.7,
                weight: 0.56,
            },
            Evidence {
                direction: -1,
                reliability: 0.55,
                weight: 0.5,
            },
        ];
        let out = fuse(0.5, &evidence).unwrap();
        assert!((out - 0.587_710_310_965_630_1).abs() < 1e-12);
        assert!((0.0..=1.0).contains(&out));
        assert_eq!(
            fuse(
                0.5,
                &[Evidence {
                    direction: 2,
                    ..evidence[0]
                }]
            ),
            Err(FusionError::InvalidDirection)
        );
        assert_eq!(
            fuse(0.5, &vec![evidence[0]; 1025]),
            Err(FusionError::TooManyObservations)
        );
    }
}
