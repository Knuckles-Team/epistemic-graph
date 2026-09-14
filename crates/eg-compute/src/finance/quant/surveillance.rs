use super::microstructure::vpin_pm;

// ════════════════════════════════════════════════════════════════════════
//  Kyle insider/stealth-trading + dynamic legal risk (CONCEPT:EG-KG.domains.concept-2)
//
//  Distils Qiao & Xia (2026), "Insider and stealth trading with dynamic
//  legal risk" (arXiv:2605.27684) — a continuous-time Kyle (1985)
//  microstructure model where surveillance intensity rises with abnormal
//  order flow and an accumulating hazard triggers prosecution. DEFENSIVE
//  use only: informed-flow / stealth-trading SURVEILLANCE and maker
//  adverse-selection protection — never trade concealment.
// ════════════════════════════════════════════════════════════════════════

/// Empirical Kyle's λ — price impact (depth) per unit signed net order flow.
/// OLS slope of Δprice on signed flow (Kyle 1985: ΔP = λ·Q). Returns 0 when
/// the flow has no variance or inputs are empty.
pub fn kyle_lambda(price_changes: &[f64], signed_order_flow: &[f64]) -> f64 {
    let n = price_changes.len().min(signed_order_flow.len());
    if n == 0 {
        return 0.0;
    }
    let mean_x = signed_order_flow[..n].iter().sum::<f64>() / n as f64;
    let mean_y = price_changes[..n].iter().sum::<f64>() / n as f64;
    let mut cov = 0.0;
    let mut var = 0.0;
    for i in 0..n {
        let dx = signed_order_flow[i] - mean_x;
        cov += dx * (price_changes[i] - mean_y);
        var += dx * dx;
    }
    if var <= 0.0 {
        0.0
    } else {
        cov / var
    }
}

pub use eg_types::compute_result::finance::SurveillanceRisk;

/// Continuous-time-Kyle surveillance estimator over a trailing book/flow
/// window. Reuses the existing microstructure primitives and combines:
/// - `kyle_lambda`: price impact (depth) from Δprice vs signed flow,
/// - `informed_share` (α): `vpin_pm` toxicity over buy/sell buckets,
/// - `detection_hazard`: mean |signed_flow| z-score vs a noise baseline σ
///   (surveillance intensity rises with abnormal flow),
/// - `cumulative_suspicion`: Σ max(0, z−1) — the paper's accumulating hazard,
/// - `stealth_ratio`: noise/informed volume (camouflage effectiveness),
/// - `legal_risk_score`: logistic squash of hazard·(1+α) ∈ [0,1] — the single
///   scalar a maker's adverse-selection gate reads.
///
/// `baseline_sigma` is the expected (noise-trader) signed-flow scale; pass ≤0 to
/// fall back to the sample std of `signed_flow`.
pub fn surveillance_risk(
    buy_vol: &[f64],
    sell_vol: &[f64],
    p_mean: &[f64],
    signed_flow: &[f64],
    price_changes: &[f64],
    baseline_sigma: f64,
) -> SurveillanceRisk {
    let kyle_lambda = kyle_lambda(price_changes, signed_flow);
    let informed_share = vpin_pm(buy_vol, sell_vol, p_mean);

    let n = signed_flow.len();
    let sigma = if baseline_sigma > 0.0 {
        baseline_sigma
    } else if n > 0 {
        let m = signed_flow.iter().sum::<f64>() / n as f64;
        (signed_flow.iter().map(|x| (x - m).powi(2)).sum::<f64>() / n as f64).sqrt()
    } else {
        0.0
    };

    let mut cumulative_suspicion = 0.0;
    let mut hazard_sum = 0.0;
    if sigma > 0.0 {
        for &q in signed_flow {
            let z = q.abs() / sigma;
            hazard_sum += z;
            cumulative_suspicion += (z - 1.0).max(0.0); // excess-over-noise
        }
    }
    let detection_hazard = if n > 0 { hazard_sum / n as f64 } else { 0.0 };

    let total_vol: f64 = buy_vol.iter().chain(sell_vol.iter()).sum();
    let informed_vol = total_vol * informed_share;
    let noise_vol = (total_vol - informed_vol).max(0.0);
    let stealth_ratio = if informed_vol > 0.0 {
        noise_vol / informed_vol
    } else {
        0.0
    };

    // Logistic squash: rises with detection hazard and informed share. The −2
    // offset places benign noise-only flow (hazard≈0.8, α≈0) well below 0.5 and
    // sustained toxic flow well above it.
    let x = detection_hazard * (1.0 + informed_share) - 2.0;
    let legal_risk_score = 1.0 / (1.0 + (-x).exp());

    SurveillanceRisk {
        kyle_lambda,
        informed_share,
        detection_hazard,
        cumulative_suspicion,
        stealth_ratio,
        legal_risk_score,
    }
}
