// CONCEPT:KG-2.20g — Forensic Accounting Scores
//
// Beneish M-Score, Altman Z-Score, Piotroski F-Score, Sloan accruals ratio.
// Coefficients are the published academic values (not a blog reimplementation —
// "half the M-Score code on GitHub is subtly wrong"). Inputs are two consecutive
// fiscal years of standardized financial-statement line items.

use serde::{Deserialize, Serialize};

/// One fiscal year of standardized financial-statement inputs.
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct YearData {
    pub sales: f64,
    pub cogs: f64,
    pub sga: f64,
    pub net_income: f64,
    pub cfo: f64, // operating cash flow
    pub receivables: f64,
    pub current_assets: f64,
    pub current_liabilities: f64,
    pub ppe_net: f64,
    pub depreciation: f64,
    pub total_assets: f64,
    pub total_liabilities: f64,
    pub long_term_debt: f64,
    pub retained_earnings: f64,
    pub ebit: f64,
    pub market_cap: f64,
    pub shares: f64,
}

#[inline]
fn d(a: f64, b: f64) -> f64 {
    if b.abs() > 1e-12 {
        a / b
    } else {
        0.0
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ForensicReport {
    pub m_score: f64,
    pub z_score: f64,
    pub f_score: i32,
    pub accruals_ratio: f64,
    /// Human-readable flags that crossed a threshold.
    pub flags: Vec<String>,
    pub verdict: String, // "INVESTIGATE" | "CLEAN"
}

/// Beneish M-Score (8 components). Above ≈ −1.78 (classic cutoff −2.22) flags
/// possible earnings manipulation. `t` = this year, `p` = prior year.
pub fn beneish_m_score(t: &YearData, p: &YearData) -> f64 {
    let dsri = d(d(t.receivables, t.sales), d(p.receivables, p.sales));
    let gmi = d(
        if p.sales != 0.0 {
            (p.sales - p.cogs) / p.sales
        } else {
            0.0
        },
        if t.sales != 0.0 {
            (t.sales - t.cogs) / t.sales
        } else {
            0.0
        },
    );
    let aqi = d(
        1.0 - d(t.current_assets + t.ppe_net, t.total_assets),
        1.0 - d(p.current_assets + p.ppe_net, p.total_assets),
    );
    let sgi = d(t.sales, p.sales);
    let depi = d(
        d(p.depreciation, p.depreciation + p.ppe_net),
        d(t.depreciation, t.depreciation + t.ppe_net),
    );
    let sgai = d(d(t.sga, t.sales), d(p.sga, p.sales));
    let tata = d(t.net_income - t.cfo, t.total_assets);
    let lvgi = d(
        d(t.total_liabilities, t.total_assets),
        d(p.total_liabilities, p.total_assets),
    );
    -4.84 + 0.92 * dsri + 0.528 * gmi + 0.404 * aqi + 0.892 * sgi + 0.115 * depi
        - 0.172 * sgai
        + 4.679 * tata
        - 0.327 * lvgi
}

/// Altman Z-Score (public manufacturers). < 1.81 distress, > 2.99 safe.
pub fn altman_z_score(t: &YearData) -> f64 {
    let wc = t.current_assets - t.current_liabilities;
    1.2 * d(wc, t.total_assets)
        + 1.4 * d(t.retained_earnings, t.total_assets)
        + 3.3 * d(t.ebit, t.total_assets)
        + 0.6 * d(t.market_cap, t.total_liabilities)
        + 1.0 * d(t.sales, t.total_assets)
}

/// Piotroski F-Score (9 binary points). 6+ = financially strengthening.
pub fn piotroski_f_score(t: &YearData, p: &YearData) -> i32 {
    let mut s = 0;
    s += (t.net_income > 0.0) as i32;
    s += (t.cfo > 0.0) as i32;
    s += (d(t.net_income, t.total_assets) > d(p.net_income, p.total_assets)) as i32;
    s += (t.cfo > t.net_income) as i32; // cash beats accruals
    s += (t.long_term_debt < p.long_term_debt) as i32;
    s += (d(t.current_assets, t.current_liabilities)
        > d(p.current_assets, p.current_liabilities)) as i32;
    s += (t.shares <= p.shares) as i32; // no dilution
    s += (d(t.sales - t.cogs, t.sales) > d(p.sales - p.cogs, p.sales)) as i32;
    s += (d(t.sales, t.total_assets) > d(p.sales, p.total_assets)) as i32;
    s
}

/// Sloan accruals ratio: (net income − operating cash flow) / total assets.
/// |x| > 0.25 ⇒ earnings-quality red flag.
pub fn sloan_accruals(t: &YearData) -> f64 {
    d(t.net_income - t.cfo, t.total_assets)
}

/// Run all four scores and produce a flagged verdict.
pub fn forensic_report(t: &YearData, p: &YearData) -> ForensicReport {
    const M_FLAG: f64 = -1.78;
    const Z_DISTRESS: f64 = 1.81;
    const ACCRUAL_BAD: f64 = 0.25;
    const F_STRONG: i32 = 6;

    let m = beneish_m_score(t, p);
    let z = altman_z_score(t);
    let f = piotroski_f_score(t, p);
    let a = sloan_accruals(t);

    let mut flags = vec![];
    if m > M_FLAG {
        flags.push(format!("M-Score {:+.2} — earnings-manipulation risk", m));
    }
    if z < Z_DISTRESS {
        flags.push(format!("Z-Score {:.2} — financial distress zone", z));
    }
    if a.abs() > ACCRUAL_BAD {
        flags.push(format!("Accruals {:+.1}% — earnings-quality red flag", a * 100.0));
    }
    if f < F_STRONG {
        flags.push(format!("F-Score {}/9 — not strengthening", f));
    }
    let verdict = if flags.is_empty() {
        "CLEAN".to_string()
    } else {
        "INVESTIGATE".to_string()
    };
    ForensicReport {
        m_score: m,
        z_score: z,
        f_score: f,
        accruals_ratio: a,
        flags,
        verdict,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A healthy company: cash-rich earnings, low leverage, growing.
    fn healthy() -> (YearData, YearData) {
        let p = YearData {
            sales: 1000.0,
            cogs: 600.0,
            sga: 150.0,
            net_income: 180.0,
            cfo: 200.0,
            receivables: 100.0,
            current_assets: 500.0,
            current_liabilities: 200.0,
            ppe_net: 400.0,
            depreciation: 40.0,
            total_assets: 1200.0,
            total_liabilities: 400.0,
            long_term_debt: 200.0,
            retained_earnings: 600.0,
            ebit: 250.0,
            market_cap: 3000.0,
            shares: 100.0,
        };
        let t = YearData {
            sales: 1100.0,
            cogs: 650.0,
            net_income: 210.0,
            cfo: 240.0,
            receivables: 105.0,
            current_assets: 560.0,
            total_assets: 1320.0,
            retained_earnings: 750.0,
            ebit: 290.0,
            long_term_debt: 180.0,
            shares: 100.0,
            ..p.clone()
        };
        (t, p)
    }

    #[test]
    fn test_healthy_company_is_clean() {
        let (t, p) = healthy();
        let r = forensic_report(&t, &p);
        // M well below the flag, Z in safe territory, strong F, low accruals
        assert!(r.m_score < -1.78, "M={}", r.m_score);
        assert!(r.z_score > 2.99, "Z={}", r.z_score);
        assert!(r.f_score >= 6, "F={}", r.f_score);
        assert!(r.accruals_ratio.abs() < 0.25);
        assert_eq!(r.verdict, "CLEAN");
    }

    #[test]
    fn test_accruals_blowout_flags() {
        let (mut t, p) = healthy();
        // Book huge profit with no cash behind it ⇒ accruals explode
        t.net_income = 600.0;
        t.cfo = 50.0;
        let r = forensic_report(&t, &p);
        assert!(r.accruals_ratio > 0.25);
        assert_eq!(r.verdict, "INVESTIGATE");
    }

    #[test]
    fn test_distress_flags_low_z() {
        let (mut t, p) = healthy();
        t.current_liabilities = 1500.0;
        t.total_liabilities = 2000.0;
        t.retained_earnings = -500.0;
        t.market_cap = 100.0;
        t.ebit = -50.0;
        let r = forensic_report(&t, &p);
        assert!(r.z_score < 1.81);
        assert_eq!(r.verdict, "INVESTIGATE");
    }
}
