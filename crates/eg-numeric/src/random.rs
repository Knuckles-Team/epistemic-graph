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

    pub fn from_entropy() -> Self {
        Generator {
            rng: ChaCha20Rng::from_entropy(),
        }
    }

    /// `gen.normal(loc, scale, size)`.
    pub fn normal(&mut self, loc: f64, scale: f64, size: usize) -> Vec<f64> {
        let dist = Normal::new(loc, scale).expect("scale must be finite & >= 0");
        (0..size).map(|_| dist.sample(&mut self.rng)).collect()
    }

    /// `gen.uniform(low, high, size)` / `random(size)` when low=0, high=1.
    pub fn uniform(&mut self, low: f64, high: f64, size: usize) -> Vec<f64> {
        let dist = Uniform::new(low, high);
        (0..size).map(|_| dist.sample(&mut self.rng)).collect()
    }

    /// `gen.integers(low, high, size)` (half-open [low, high)).
    pub fn integers(&mut self, low: i64, high: i64, size: usize) -> Vec<i64> {
        (0..size).map(|_| self.rng.gen_range(low..high)).collect()
    }

    /// `gen.standard_normal(size)`.
    pub fn standard_normal(&mut self, size: usize) -> Vec<f64> {
        self.normal(0.0, 1.0, size)
    }
}
