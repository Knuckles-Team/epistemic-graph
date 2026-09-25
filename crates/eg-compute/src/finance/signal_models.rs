//! Signal fusion and the strategic-insider model (EH-423 / AUD-30).
//!
//! Moved from agent-utilities' Python `domains/finance/{signal_fusion,
//! insider_equilibrium}` so the math has one owner. Two deliberate changes
//! from the Python: calls are applied in source-name order (a map's insertion
//! order is not a property of the evidence), and a seeded source's weight is
//! clamped to `[0, 1]` (the Python let `accuracy * sharpe > 1` push the
//! posterior outside `[0, 1]`). Pure compute; informational only.

pub use eg_types::compute_result::signal_models::*;

use super::market::{MarketError, MarketResult, INVALID_REQUEST};

fn refuse(detail: impl Into<String>) -> MarketError {
    MarketError::new(INVALID_REQUEST, detail)
}

fn require_finite(name: &str, value: f64) -> MarketResult<()> {
    if value.is_finite() {
        Ok(())
    } else {
        Err(refuse(format!("{name} must be finite")))
    }
}

// ── The strategic insider under dynamic legal risk ──────────────────────────

/// The clamped primitives every formula reads.
struct Game {
    enforcement: f64,
    sigma: f64,
    lambda: f64,
    kappa: f64,
    criminal: f64,
    civil: f64,
}

impl Game {
    fn of(inputs: &InsiderInputs, enforcement: f64) -> Self {
        let gap = inputs.gap_var.unwrap_or(inputs.sigma_v * inputs.sigma_v);
        Self {
            enforcement: enforcement.clamp(0.0, 1.0),
            sigma: gap.max(1e-12),
            lambda: 0.5 * inputs.sigma_v / inputs.sigma_u.max(1e-9),
            kappa: inputs.surveillance_kappa.max(0.0),
            criminal: inputs.criminal_penalty.max(0.0),
            civil: inputs.civil_penalty_rate.max(0.0),
        }
    }

    /// Enforcement-scaled detection hazard per unit of intensity.
    fn hazard(&self) -> f64 {
        self.enforcement * self.kappa
    }

    fn denominator(&self) -> f64 {
        2.0 * self.sigma * (self.lambda + self.hazard() * self.civil)
    }
}

fn validate_insider(inputs: &InsiderInputs) -> MarketResult<()> {
    let fields = [
        ("sigma_v", inputs.sigma_v),
        ("sigma_u", inputs.sigma_u),
        ("enforcement", inputs.enforcement),
        ("surveillance_kappa", inputs.surveillance_kappa),
        ("criminal_penalty", inputs.criminal_penalty),
        ("civil_penalty_rate", inputs.civil_penalty_rate),
        ("horizon", inputs.horizon),
        ("gap_var", inputs.gap_var.unwrap_or(0.0)),
    ];
    fields
        .iter()
        .try_for_each(|(name, value)| require_finite(name, *value))
}

fn binding_lever(game: &Game, suppressed: bool, gross: f64) -> BindingLever {
    if suppressed {
        return BindingLever::Criminal;
    }
    if game.enforcement <= 1e-9 || game.kappa <= 1e-9 {
        return BindingLever::None;
    }
    if game.hazard() * game.criminal > 0.0 && game.criminal >= game.civil * gross {
        return BindingLever::Criminal;
    }
    if game.civil > 0.0 {
        BindingLever::Civil
    } else {
        BindingLever::Enforcement
    }
}

/// `beta* = (S - e k C) / (2 S (lambda + e k r))`, floored at zero.
fn solve(inputs: &InsiderInputs, enforcement: f64) -> InsiderEquilibrium {
    let game = Game::of(inputs, enforcement);
    let denominator = game.denominator();
    let raw = if denominator > 0.0 {
        (game.sigma - game.hazard() * game.criminal) / denominator
    } else {
        0.0
    };
    let suppressed = raw <= 0.0;
    let intensity = raw.max(0.0);
    let detection = (game.hazard() * intensity).clamp(0.0, 1.0);
    let gross = intensity * game.sigma;
    let expected_profit = (intensity - game.lambda * intensity * intensity) * game.sigma;
    let expected_penalty = detection * (game.criminal + game.civil * gross);
    InsiderEquilibrium {
        intensity,
        baseline_intensity: if game.lambda > 0.0 {
            1.0 / (2.0 * game.lambda)
        } else {
            0.0
        },
        kyle_lambda: game.lambda,
        detection_prob: detection,
        expected_profit,
        expected_penalty,
        net_value: expected_profit - expected_penalty,
        suppressed,
        binding_lever: binding_lever(&game, suppressed, gross),
    }
}

/// Enforcement decays linearly with the remaining window, so the insider
/// accelerates toward the end.
fn schedule(inputs: &InsiderInputs, steps: u32) -> Vec<InsiderScheduleSample> {
    let steps = steps.max(1);
    (0..=steps)
        .map(|step| {
            let t = inputs.horizon * f64::from(step) / f64::from(steps);
            let remaining = if inputs.horizon > 0.0 {
                (inputs.horizon - t) / inputs.horizon
            } else {
                0.0
            };
            let enforcement = inputs.enforcement * remaining;
            let at = solve(inputs, enforcement);
            InsiderScheduleSample {
                t,
                remaining,
                enforcement,
                intensity: at.intensity,
                detection_prob: at.detection_prob,
            }
        })
        .collect()
}

fn verdict(game: &Game, gated: bool, floor: Option<f64>) -> PenaltyVerdict {
    if gated {
        return PenaltyVerdict::EnforcementGated;
    }
    match floor {
        Some(floor) if game.criminal > 0.0 && game.criminal >= floor => {
            PenaltyVerdict::CriminalSuppresses
        }
        _ => PenaltyVerdict::CriminalIsTheLever,
    }
}

/// `d beta*/dC` and `d beta*/dr` from the closed form, and the verdict.
fn policy(inputs: &InsiderInputs) -> PenaltyPolicy {
    let game = Game::of(inputs, inputs.enforcement);
    let hazard = game.hazard();
    let denominator = game.denominator();
    let slope = game.lambda + hazard * game.civil;
    let d_criminal = if denominator > 0.0 {
        -hazard / denominator
    } else {
        0.0
    };
    let d_civil = if slope > 0.0 {
        -hazard * (game.sigma - hazard * game.criminal) / (2.0 * game.sigma * slope * slope)
    } else {
        0.0
    };
    let floor = (hazard > 1e-12).then(|| game.sigma / hazard);
    let gated = game.enforcement < 0.25;
    PenaltyPolicy {
        d_intensity_d_criminal: d_criminal,
        d_intensity_d_civil: d_civil,
        criminal_intensity_floor: floor,
        civil_only_min_intensity: 0.0,
        enforcement_gated: gated,
        verdict: verdict(&game, gated, floor),
    }
}

/// The equilibrium at the stated enforcement, its schedule and the policy.
pub fn insider_equilibrium(request: &InsiderEquilibriumRequest) -> MarketResult<InsiderAnalysis> {
    validate_insider(&request.inputs)?;
    Ok(InsiderAnalysis {
        equilibrium: solve(&request.inputs, request.inputs.enforcement),
        schedule: schedule(&request.inputs, request.steps),
        policy: policy(&request.inputs),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn close(actual: f64, expected: f64) {
        assert!((actual - expected).abs() < 1e-12, "{actual} != {expected}");
    }

    fn inputs(enforcement: f64, criminal: f64, civil: f64) -> InsiderInputs {
        InsiderInputs {
            sigma_v: 0.3,
            sigma_u: 1.0,
            gap_var: None,
            enforcement,
            surveillance_kappa: 1.0,
            criminal_penalty: criminal,
            civil_penalty_rate: civil,
            horizon: 1.0,
        }
    }

    fn analysis(inputs: InsiderInputs, steps: u32) -> InsiderAnalysis {
        insider_equilibrium(&InsiderEquilibriumRequest { inputs, steps }).unwrap()
    }

    // Reference values: agent-utilities `insider_equilibrium.py` at d8b54a549.
    #[test]
    fn equilibrium_matches_the_python_reference() {
        let out = analysis(inputs(0.7, 0.05, 1.0), 4);
        let eq = &out.equilibrium;
        close(eq.intensity, 0.359_477_124_183_006_54);
        close(eq.baseline_intensity, 3.333_333_333_333_333_5);
        close(eq.kyle_lambda, 0.15);
        close(eq.detection_prob, 0.251_633_986_928_104_57);
        close(eq.expected_profit, 0.030_608_419_838_523_644);
        close(eq.expected_penalty, 0.020_722_798_923_490_965);
        close(eq.net_value, 0.009_885_620_915_032_68);
        assert!(!eq.suppressed);
        assert_eq!(eq.binding_lever, BindingLever::Criminal);
        close(out.policy.d_intensity_d_criminal, -4.575_163_398_692_81);
        close(out.policy.d_intensity_d_civil, -0.296_039_984_621_299_55);
        close(
            out.policy.criminal_intensity_floor.unwrap(),
            0.128_571_428_571_428_6,
        );
        assert_eq!(out.policy.verdict, PenaltyVerdict::CriminalIsTheLever);
    }

    #[test]
    fn the_schedule_accelerates_toward_the_end_of_the_window() {
        let out = analysis(inputs(0.7, 0.05, 1.0), 4);
        let intensities: Vec<f64> = out.schedule.iter().map(|s| s.intensity).collect();
        assert_eq!(intensities.len(), 5);
        close(intensities[1], 0.524_691_358_024_691_5);
        close(intensities[4], 3.333_333_333_333_333);
        assert!(intensities.windows(2).all(|pair| pair[0] <= pair[1]));
        close(out.schedule[4].detection_prob, 0.0);
    }

    #[test]
    fn a_criminal_cost_past_the_floor_suppresses_the_insider() {
        let out = analysis(inputs(0.9, 1.0, 0.0), 1);
        assert!(out.equilibrium.suppressed);
        close(out.equilibrium.intensity, 0.0);
        assert_eq!(out.policy.verdict, PenaltyVerdict::CriminalSuppresses);
        close(out.policy.criminal_intensity_floor.unwrap(), 0.1);
    }

    #[test]
    fn weak_enforcement_gates_the_civil_lever_and_no_enforcement_has_no_floor() {
        let weak = analysis(inputs(0.1, 0.0, 0.0), 1);
        assert_eq!(weak.policy.verdict, PenaltyVerdict::EnforcementGated);
        assert_eq!(weak.equilibrium.binding_lever, BindingLever::Enforcement);
        let none = analysis(inputs(0.0, 0.0, 0.0), 1);
        assert_eq!(none.policy.criminal_intensity_floor, None);
        assert_eq!(none.equilibrium.binding_lever, BindingLever::None);
    }

    #[test]
    fn non_finite_inputs_are_refused() {
        let mut bad = inputs(0.5, 0.0, 0.0);
        bad.sigma_v = f64::NAN;
        let refused = insider_equilibrium(&InsiderEquilibriumRequest {
            inputs: bad,
            steps: 1,
        })
        .unwrap_err();
        assert_eq!(refused.code, INVALID_REQUEST);
    }
}
