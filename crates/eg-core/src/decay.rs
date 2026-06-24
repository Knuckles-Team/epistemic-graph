//! The ONE temporal-decay curve (CONCEPT:KG-2.211).
//!
//! The engine's memory/confidence model and the time-series store are the **same
//! temporal model**: a fact's salience and a tick's recency-weight both fall off
//! exponentially with age on an Ebbinghaus forgetting curve. Historically that
//! curve was inlined in `src/server/compute.rs` (semantic-search confidence decay,
//! CONCEPT:KG-2.16). It now lives here, at the bottom of the DAG, so BOTH the
//! semantic-memory path AND the time-series `decay_weighted_mean` (eg-tsdb) call
//! ONE function — proving "memory + series are one temporal model".
//!
//! The curve is `exp(-lambda * age)` with `lambda = ln(2) / half_life`, i.e. a
//! half-life parameterisation: at `age == half_life` the weight is exactly `0.5`.
//! It is unit-agnostic — `age` and `half_life` must share a unit (the semantic
//! path uses days; the series path uses seconds). A non-positive age yields `1.0`
//! (no decay for a not-yet-aged / future observation).

/// Ebbinghaus recency weight in `[0, 1]` for an observation of the given `age`,
/// using a `half_life` parameterisation. `age` and `half_life` MUST share a unit.
///
/// * `age <= 0.0`        ⇒ `1.0` (future / brand-new observation, no decay)
/// * `age == half_life`  ⇒ `0.5`
/// * `half_life <= 0.0`  ⇒ `1.0` (degenerate: decay disabled)
#[inline]
pub fn ebbinghaus_weight(age: f64, half_life: f64) -> f64 {
    if age <= 0.0 || half_life <= 0.0 {
        return 1.0;
    }
    let lambda = std::f64::consts::LN_2 / half_life;
    (-lambda * age).exp()
}

#[cfg(test)]
mod tests {
    use super::ebbinghaus_weight;

    #[test]
    fn half_life_is_one_half() {
        assert!((ebbinghaus_weight(30.0, 30.0) - 0.5).abs() < 1e-12);
    }

    #[test]
    fn future_and_degenerate_are_unity() {
        assert_eq!(ebbinghaus_weight(0.0, 30.0), 1.0);
        assert_eq!(ebbinghaus_weight(-5.0, 30.0), 1.0);
        assert_eq!(ebbinghaus_weight(10.0, 0.0), 1.0);
    }

    #[test]
    fn matches_the_inlined_30day_curve() {
        // The exact expression the semantic path used: (-ln2/30 * age_days).exp().
        for age_days in [1.0_f64, 7.0, 15.0, 60.0, 365.0] {
            let want = (-std::f64::consts::LN_2 / 30.0 * age_days).exp();
            assert!((ebbinghaus_weight(age_days, 30.0) - want).abs() < 1e-15);
        }
    }
}
