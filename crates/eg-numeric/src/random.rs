//! Random generation — the numpy `default_rng`/`Generator`/`normal`/`uniform`/
//! `randint`/`RandomState` surface. Backed by a seedable ChaCha20 stream.
//!
//! NOTE ON PARITY: bit-for-bit value parity with numpy's PCG64/MT19937 is NOT a
//! goal (different algorithms). The parity corpus asserts *distributional* parity
//! (shape, and mean/std within statistical tolerance), and *determinism* (same
//! seed → same stream). This mirrors how numpy code actually depends on the RNG.

use rand::{Rng, SeedableRng};
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
}

fn check_size(size: usize) -> crate::Result<()> {
    if size > MAX_RANDOM_ELEMENTS {
        return Err(crate::NumericError::resource(format!(
            "output size exceeds the {MAX_RANDOM_ELEMENTS}-element limit"
        )));
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
}
