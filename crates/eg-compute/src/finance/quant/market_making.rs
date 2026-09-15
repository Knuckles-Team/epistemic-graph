use super::{logit, sigmoid};

// ════════════════════════════════════════════════════════════════════════
//  Market making: Avellaneda-Stoikov, GLT, logit-space
// ════════════════════════════════════════════════════════════════════════

pub use eg_types::compute_result::finance::Quote;

/// Avellaneda-Stoikov (2008) optimal quotes around a freely-drifting mid.
///   r = S − q·γ·σ²·(T−t);  δ* = γ·σ²·(T−t) + (2/γ)·ln(1+γ/κ)
pub fn avellaneda_stoikov(
    mid: f64,
    inventory: f64,
    sigma: f64,
    gamma: f64,
    kappa: f64,
    tau: f64,
) -> Quote {
    let reservation = mid - inventory * gamma * sigma * sigma * tau;
    let half_spread = gamma * sigma * sigma * tau + (2.0 / gamma) * (1.0 + gamma / kappa).ln();
    Quote {
        bid: reservation - half_spread,
        ask: reservation + half_spread,
        reservation,
        half_spread,
        withdraw: false,
    }
}

/// Guéant-Lehalle-Fernandez-Tapia (2013) closed form with asymmetric
/// inventory-dependent skew. `a` is the fill-intensity scale A.
pub fn glt_quotes(mid: f64, inventory: f64, sigma: f64, gamma: f64, kappa: f64, a: f64) -> Quote {
    let base = (1.0 / gamma) * (1.0 + gamma / kappa).ln();
    let inv_term = ((sigma * sigma * gamma) / (2.0 * kappa * a)
        * (1.0 + gamma / kappa).powf(1.0 + kappa / gamma))
    .sqrt();
    let delta_ask = base + ((2.0 * inventory + 1.0) / 2.0) * inv_term;
    let delta_bid = base + ((-2.0 * inventory + 1.0) / 2.0) * inv_term;
    Quote {
        bid: mid - delta_bid,
        ask: mid + delta_ask,
        reservation: mid,
        half_spread: 0.5 * (delta_ask + delta_bid),
        withdraw: false,
    }
}

/// Logit-space AS for bounded (0,1) prediction-market prices, with a
/// boundary-aware inventory cap |q| ≤ M·√(p(1−p)). Quotes are returned in
/// PRICE (probability) units. `withdraw=true` ⇒ inventory exceeds the cap.
pub fn logit_space_quotes(
    p_mid: f64,
    inventory: f64,
    sigma: f64,
    gamma: f64,
    kappa: f64,
    tau: f64,
    boundary_m: f64,
) -> Quote {
    let p = p_mid.clamp(1e-6, 1.0 - 1e-6);
    let cap = boundary_m * (p * (1.0 - p)).sqrt();
    let withdraw = boundary_m > 0.0 && inventory.abs() > cap;
    let x_mid = logit(p);
    let x_res = x_mid - inventory * gamma * sigma * sigma * tau;
    let half_spread = gamma * sigma * sigma * tau + (2.0 / gamma) * (1.0 + gamma / kappa).ln();
    Quote {
        bid: sigmoid(x_res - half_spread),
        ask: sigmoid(x_res + half_spread),
        reservation: sigmoid(x_res),
        half_spread,
        withdraw,
    }
}

/// Glosten-Milgrom adverse-selection spread for a binary payoff: 2·α·p·(1−p).
pub fn glosten_milgrom_spread(alpha: f64, p: f64) -> f64 {
    2.0 * alpha * p * (1.0 - p)
}

/// Expected maker PnL per unit time at half-spread δ. Positive ⇒ profitable,
/// negative ⇒ adversely selected. α is the informed-flow fraction (VPIN proxy).
pub fn expected_pnl_rate(
    delta: f64,
    a: f64,
    kappa: f64,
    alpha: f64,
    p: f64,
    v_h: f64,
    v_l: f64,
) -> f64 {
    let fill_rate = 2.0 * a * (-kappa * delta).exp();
    let spread_capture = (1.0 - alpha) * delta;
    let adv_selection = alpha * (v_h - v_l).abs() * p * (1.0 - p);
    fill_rate * (spread_capture - adv_selection)
}

/// Maximum informed fraction α before quoting half-spread δ goes unprofitable.
pub fn breakeven_alpha(delta: f64, p: f64, v_h: f64, v_l: f64) -> f64 {
    let payoff = (v_h - v_l).abs();
    delta / (payoff * p * (1.0 - p) + delta)
}
