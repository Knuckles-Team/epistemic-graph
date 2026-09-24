//! The scalar Kalman filter (EH-530, re-homed from the finance state-space module): one
//! recurrence behind the local-level and dynamic-beta series kernels and the finance
//! `kalman_filter_1d` / `kalman_beta` Methods.

use serde::{Deserialize, Serialize};

use super::KalmanNoise;

/// Below this the innovation variance is treated as zero (no update).
const S_FLOOR: f64 = 1e-18;

/// `x_t = F·x_{t−1} + w (Q)`, `z_t = H_t·x_t + v (R)`: the filtered state and variance.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Kalman {
    f: f64,
    q: f64,
    r: f64,
    /// `None` until seeded: a local-level filter seeds with its first observation.
    x: Option<f64>,
    p: f64,
}

impl Kalman {
    /// A filter with explicit transition `f` and initial state `x0` at variance `p0`.
    pub fn new(f: f64, noise: KalmanNoise, x0: f64, p0: f64) -> Self {
        Self {
            f,
            q: noise.q,
            r: noise.r,
            x: Some(x0),
            p: p0,
        }
    }

    /// A random-walk level seeded with its first observation at variance `r`.
    pub fn level(noise: KalmanNoise) -> Self {
        Self {
            f: 1.0,
            q: noise.q,
            r: noise.r,
            x: None,
            p: noise.r,
        }
    }

    /// A random-walk regression coefficient starting at `β = 0`, variance 1.
    pub fn beta(noise: KalmanNoise) -> Self {
        Self::new(1.0, noise, 0.0, 1.0)
    }

    /// Predict, then update on observation `z` with measurement coefficient `h`; the
    /// filtered `(state, variance)`.
    pub fn observe(&mut self, z: f64, h: f64) -> (f64, f64) {
        let Some(mut x) = self.x else {
            self.x = Some(z);
            return (z, self.p);
        };
        x *= self.f;
        let mut p = self.f * self.p * self.f + self.q;
        let s = h * p * h + self.r;
        let k = if s.abs() > S_FLOOR { p * h / s } else { 0.0 };
        x += k * (z - h * x);
        p *= 1.0 - k * h;
        self.x = Some(x);
        self.p = p;
        (x, p)
    }
}
