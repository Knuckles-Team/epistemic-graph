//! Random generation — the numpy `default_rng`/`Generator`/`normal`/`uniform`/
//! `randint`/`RandomState` surface. Backed by a seedable ChaCha20 stream.
//!
//! NOTE ON PARITY: bit-for-bit value parity with numpy's PCG64/MT19937 is NOT a
//! goal (different algorithms). The parity corpus asserts *distributional* parity
//! (shape, and mean/std within statistical tolerance), and *determinism* (same
//! seed → same stream). This mirrors how numpy code actually depends on the RNG.

use rand::{distributions::WeightedIndex, seq::index, Rng, SeedableRng};
use rand_chacha::ChaCha20Rng;
use rand_distr::{Distribution, Normal, Uniform};

/// The shared output budget for every allocation at the native boundary.
pub const MAX_RANDOM_ELEMENTS: usize = 1_000_000;

/// A seedable generator, the kernel twin of `numpy.random.Generator`.
pub struct Generator {
    rng: ChaCha20Rng,
}

impl Generator {
    /// `numpy.random.default_rng(seed)`.
    pub fn new(seed: u64) -> Self {
        Generator {
            rng: ChaCha20Rng::seed_from_u64(seed),
        }
    }

    /// Entropy-backed construction is intentionally not exposed by the Python
    /// contract.  The pure Rust kernel keeps this helper for existing engine
    /// call sites, while all serving paths use [`Generator::new`] so a run is
    /// reproducible and scoped to one instance.
    pub fn from_entropy() -> Self {
        Generator {
            rng: ChaCha20Rng::from_entropy(),
        }
    }

    /// `gen.normal(loc, scale, size)`.
    ///
    /// This established Rust-kernel API retains its `Vec` return type for the
    /// in-process callers that use it for one-element draws. Python-facing
    /// code should use [`Self::try_normal`] so invalid parameters and output
    /// budgets become Python exceptions instead of panics.
    pub fn normal(&mut self, loc: f64, scale: f64, size: usize) -> Vec<f64> {
        let dist = Normal::new(loc, scale).expect("scale must be finite & >= 0");
        (0..size).map(|_| dist.sample(&mut self.rng)).collect()
    }

    /// `gen.uniform(low, high, size)` / `random(size)` when low=0, high=1.
    ///
    /// See [`Self::try_uniform`] for the checked Python-boundary variant.
    pub fn uniform(&mut self, low: f64, high: f64, size: usize) -> Vec<f64> {
        let dist = Uniform::new(low, high);
        (0..size).map(|_| dist.sample(&mut self.rng)).collect()
    }

    /// Checked normal sampling for a native boundary.
    pub fn try_normal(&mut self, loc: f64, scale: f64, size: usize) -> crate::Result<Vec<f64>> {
        check_size(size)?;
        if !loc.is_finite() || !scale.is_finite() || scale < 0.0 {
            return Err(crate::NumericError::random(
                "normal requires finite loc and non-negative finite scale",
            ));
        }
        let dist = Normal::new(loc, scale)
            .map_err(|_| crate::NumericError::random("invalid normal distribution"))?;
        Ok((0..size).map(|_| dist.sample(&mut self.rng)).collect())
    }

    /// Checked uniform sampling for a native boundary.
    pub fn try_uniform(&mut self, low: f64, high: f64, size: usize) -> crate::Result<Vec<f64>> {
        check_size(size)?;
        if !low.is_finite() || !high.is_finite() || low >= high {
            return Err(crate::NumericError::random(
                "uniform requires finite low < high",
            ));
        }
        let dist = Uniform::new(low, high);
        Ok((0..size).map(|_| dist.sample(&mut self.rng)).collect())
    }

    /// `gen.integers(low, high, size)` (half-open [low, high)).
    ///
    /// See [`Self::try_integers`] for the checked Python-boundary variant.
    pub fn integers(&mut self, low: i64, high: i64, size: usize) -> Vec<i64> {
        (0..size).map(|_| self.rng.gen_range(low..high)).collect()
    }

    /// Checked integer sampling for a native boundary.
    pub fn try_integers(&mut self, low: i64, high: i64, size: usize) -> crate::Result<Vec<i64>> {
        check_size(size)?;
        if low >= high {
            return Err(crate::NumericError::random("integers requires low < high"));
        }
        Ok((0..size).map(|_| self.rng.gen_range(low..high)).collect())
    }

    /// `gen.standard_normal(size)`.
    pub fn standard_normal(&mut self, size: usize) -> Vec<f64> {
        self.normal(0.0, 1.0, size)
    }

    /// Checked standard-normal sampling for a native boundary.
    pub fn try_standard_normal(&mut self, size: usize) -> crate::Result<Vec<f64>> {
        self.try_normal(0.0, 1.0, size)
    }

    /// Draw indices from `0..population_size` in one bounded batch.
    ///
    /// With replacement, each draw is independent.  Without replacement,
    /// unweighted draws use the `rand` crate's unbiased index sampler and
    /// weighted draws use Efraimidis-Spirakis weighted sampling.  The latter
    /// is probability-proportional-to-size without replacement, so repeated
    /// draws do not fall back to a biased modulo/shrinking-range loop at the
    /// caller boundary.
    pub fn try_choice_indices(
        &mut self,
        population_size: usize,
        sample_size: usize,
        replace: bool,
        weights: Option<&[f64]>,
    ) -> crate::Result<Vec<usize>> {
        check_population_size(population_size)?;
        check_size(sample_size)?;
        if population_size == 0 {
            if sample_size == 0 && weights.is_none() {
                return Ok(Vec::new());
            }
            return Err(crate::NumericError::random(
                "choice cannot sample an empty population",
            ));
        }
        if !replace && sample_size > population_size {
            return Err(crate::NumericError::random(
                "cannot take a larger sample than population without replacement",
            ));
        }

        let weights = match weights {
            Some(weights) => {
                validate_weights(weights, population_size)?;
                Some(weights)
            }
            None => None,
        };

        match (replace, weights) {
            (true, None) => Ok((0..sample_size)
                .map(|_| self.rng.gen_range(0..population_size))
                .collect()),
            (true, Some(weights)) => {
                self.weighted_choice_with_replacement(population_size, sample_size, weights)
            }
            (false, None) => {
                Ok(index::sample(&mut self.rng, population_size, sample_size).into_vec())
            }
            (false, Some(weights)) => {
                self.weighted_choice_without_replacement(sample_size, weights)
            }
        }
    }

    /// Return a uniformly random permutation of `0..population_size`.
    pub fn try_permutation_indices(&mut self, population_size: usize) -> crate::Result<Vec<usize>> {
        check_population_size(population_size)?;
        Ok(index::sample(&mut self.rng, population_size, population_size).into_vec())
    }

    fn weighted_choice_with_replacement(
        &mut self,
        population_size: usize,
        sample_size: usize,
        weights: &[f64],
    ) -> crate::Result<Vec<usize>> {
        debug_assert_eq!(weights.len(), population_size);
        // Scale before constructing WeightedIndex so a valid collection of
        // large finite weights cannot overflow while accumulating its total.
        let max_weight = weights.iter().copied().fold(0.0_f64, f64::max);
        let distribution = WeightedIndex::new(weights.iter().map(|weight| *weight / max_weight))
            .map_err(|_| crate::NumericError::random("invalid choice weights"))?;
        Ok((0..sample_size)
            .map(|_| distribution.sample(&mut self.rng))
            .collect())
    }

    fn weighted_choice_without_replacement(
        &mut self,
        sample_size: usize,
        weights: &[f64],
    ) -> crate::Result<Vec<usize>> {
        let max_weight = weights.iter().copied().fold(0.0_f64, f64::max);
        let positive_indices: Vec<usize> = weights
            .iter()
            .enumerate()
            .filter_map(|(index, weight)| (*weight > 0.0).then_some(index))
            .collect();
        if sample_size > positive_indices.len() {
            return Err(crate::NumericError::random(
                "cannot take a sample larger than the number of positive weights without replacement",
            ));
        }
        let sampled = index::sample_weighted(
            &mut self.rng,
            positive_indices.len(),
            |index| weights[positive_indices[index]] / max_weight,
            sample_size,
        )
        .map_err(|_| crate::NumericError::random("invalid choice weights"))?;
        Ok(sampled
            .into_iter()
            .map(|index| positive_indices[index])
            .collect())
    }
}

fn check_size(size: usize) -> crate::Result<()> {
    if size > MAX_RANDOM_ELEMENTS {
        return Err(crate::NumericError::resource(format!(
            "output size exceeds the {MAX_RANDOM_ELEMENTS}-element limit"
        )));
    }
    Ok(())
}

fn check_population_size(population_size: usize) -> crate::Result<()> {
    if population_size > MAX_RANDOM_ELEMENTS {
        return Err(crate::NumericError::resource(format!(
            "population size exceeds the {MAX_RANDOM_ELEMENTS}-element limit"
        )));
    }
    Ok(())
}

fn validate_weights(weights: &[f64], population_size: usize) -> crate::Result<()> {
    if weights.len() > MAX_RANDOM_ELEMENTS {
        return Err(crate::NumericError::resource(format!(
            "weights exceed the {MAX_RANDOM_ELEMENTS}-element limit"
        )));
    }
    if weights.len() != population_size {
        return Err(crate::NumericError::random(
            "choice weights must match the population size",
        ));
    }
    if weights
        .iter()
        .any(|weight| !weight.is_finite() || *weight < 0.0)
    {
        return Err(crate::NumericError::random(
            "choice weights must be finite and non-negative",
        ));
    }
    if weights.iter().all(|weight| *weight == 0.0) {
        return Err(crate::NumericError::random(
            "choice weights must contain a positive value",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{Generator, MAX_RANDOM_ELEMENTS};
    use crate::NumericError;

    #[test]
    fn checked_sampling_rejects_invalid_parameters() {
        let mut generator = Generator::new(7);
        assert!(matches!(
            generator.try_normal(0.0, -1.0, 1),
            Err(NumericError::Random(_))
        ));
        assert!(matches!(
            generator.try_uniform(1.0, 1.0, 1),
            Err(NumericError::Random(_))
        ));
        assert!(matches!(
            generator.try_integers(2, 2, 1),
            Err(NumericError::Random(_))
        ));
    }

    #[test]
    fn checked_sampling_rejects_oversized_output_before_allocation() {
        let mut generator = Generator::new(7);
        assert!(matches!(
            generator.try_standard_normal(MAX_RANDOM_ELEMENTS + 1),
            Err(NumericError::Resource(_))
        ));
    }

    #[test]
    fn established_sampling_api_stays_vec_valued() {
        let mut generator = Generator::new(7);
        assert_eq!(generator.normal(0.0, 1.0, 2).len(), 2);
        assert_eq!(generator.uniform(0.0, 1.0, 2).len(), 2);
        assert_eq!(generator.integers(0, 2, 2).len(), 2);
        assert_eq!(generator.standard_normal(2).len(), 2);
    }

    #[test]
    fn choice_indices_are_bounded_and_unbiased_without_replacement() {
        let mut generator = Generator::new(7);
        let values = generator.try_choice_indices(32, 16, false, None).unwrap();
        assert_eq!(values.len(), 16);
        assert!(values.iter().all(|value| *value < 32));
        let mut sorted = values.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), values.len());

        let permutation = generator.try_permutation_indices(32).unwrap();
        assert_eq!(permutation.len(), 32);
        let mut sorted = permutation;
        sorted.sort_unstable();
        assert_eq!(sorted, (0..32).collect::<Vec<_>>());
    }

    #[test]
    fn weighted_choice_indices_honors_zero_weights_and_replacement() {
        let mut generator = Generator::new(7);
        let weights = [0.0, 1.0, 3.0, 0.0];
        let with_replacement = generator
            .try_choice_indices(4, 128, true, Some(&weights))
            .unwrap();
        assert!(with_replacement
            .iter()
            .all(|value| *value == 1 || *value == 2));

        let without_replacement = generator
            .try_choice_indices(4, 2, false, Some(&weights))
            .unwrap();
        assert_eq!(without_replacement.len(), 2);
        assert!(without_replacement.contains(&1));
        assert!(without_replacement.contains(&2));
    }

    #[test]
    fn choice_indices_reject_invalid_weights_and_bounds() {
        let mut generator = Generator::new(7);
        assert!(generator
            .try_choice_indices(0, 0, true, None)
            .unwrap()
            .is_empty());
        for weights in [
            vec![0.0, 0.0],
            vec![-1.0, 1.0],
            vec![f64::NAN, 1.0],
            vec![f64::INFINITY, 1.0],
            vec![1.0],
        ] {
            assert!(matches!(
                generator.try_choice_indices(2, 1, true, Some(&weights)),
                Err(NumericError::Random(_))
            ));
        }
        assert!(matches!(
            generator.try_choice_indices(2, 3, false, None),
            Err(NumericError::Random(_))
        ));
        assert!(matches!(
            generator.try_choice_indices(MAX_RANDOM_ELEMENTS + 1, 1, true, None),
            Err(NumericError::Resource(_))
        ));
        assert!(matches!(
            generator.try_permutation_indices(MAX_RANDOM_ELEMENTS + 1),
            Err(NumericError::Resource(_))
        ));
    }
}
