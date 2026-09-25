//! MASS query-by-example (EH-529): the z-normalised distance from a query shape to every
//! length-`m` subsequence of a series in O(n log n) — "find past windows shaped like
//! this one". The FFT ([`super::fft`]) ranks the placements; [`Shape::distance_at`]
//! re-derives any distance a caller reports from a direct dot product, so reported
//! values are identical on every node.

use super::distance::{centred, dot, znorm_distance, SubsequenceStats, MIN_LENGTH};
use super::fft::Correlator;
use crate::error::{NumericError, Result};

/// Refuse a series or query the distance is undefined for.
pub fn check_series(xs: &[f64], m: usize) -> Result<()> {
    if m < MIN_LENGTH {
        return Err(NumericError::bounds(format!(
            "a subsequence length of {m} is below {MIN_LENGTH}"
        )));
    }
    if xs.len() < m {
        return Err(NumericError::shape(format!(
            "a series of {} points has no subsequence of length {m}",
            xs.len()
        )));
    }
    if xs.iter().all(|x| x.is_finite()) {
        return Ok(());
    }
    Err(NumericError::bounds("the series holds a non-finite value"))
}

/// A series prepared for query-by-example.
pub struct Shape {
    m: usize,
    series: Vec<f64>,
    stats: SubsequenceStats,
    query: Vec<f64>,
    query_stats: (f64, f64),
}

impl Shape {
    /// Prepare `series` for `query` (refused when either is out of domain).
    pub fn new(query: &[f64], series: &[f64]) -> Result<Self> {
        let m = query.len();
        check_series(query, m)?;
        check_series(series, m)?;
        let series = centred(series);
        let query = centred(query);
        let query_stats = SubsequenceStats::of(&query, m).at(0);
        Ok(Self {
            m,
            stats: SubsequenceStats::of(&series, m),
            series,
            query,
            query_stats,
        })
    }

    /// Placements of the query: `n − m + 1`.
    pub fn placements(&self) -> usize {
        self.stats.mean.len()
    }

    /// The distance profile: the query's distance to every placement (FFT, O(n log n)).
    pub fn profile(&self) -> Vec<f64> {
        let qt = Correlator::new(&self.series, self.m).dots(&self.query);
        qt.iter()
            .enumerate()
            .map(|(i, &q)| znorm_distance(q, self.m, self.query_stats, self.stats.at(i)))
            .collect()
    }

    /// The query's distance to placement `i`, from a direct dot product (O(m)).
    pub fn distance_at(&self, i: usize) -> f64 {
        let window = &self.series[i..i + self.m];
        znorm_distance(
            dot(&self.query, window),
            self.m,
            self.query_stats,
            self.stats.at(i),
        )
    }
}

/// The distance profile of `query` over `series` (MASS).
pub fn mass(query: &[f64], series: &[f64]) -> Result<Vec<f64>> {
    Ok(Shape::new(query, series)?.profile())
}
