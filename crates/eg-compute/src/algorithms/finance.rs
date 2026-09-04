//! Lightweight rolling statistics and order-book simulation.

use std::collections::HashMap;

// ── Quant epistemic-graph Algorithms ─────────────────────────────────────────────────

/// Compute rolling mean over a sliding window.
pub fn compute_rolling_mean(values: &[f64], window: usize) -> Vec<f64> {
    if window == 0 || values.is_empty() {
        return vec![0.0; values.len()];
    }
    let mut result = vec![0.0; values.len()];
    for i in 0..values.len() {
        let start = if i >= window - 1 { i + 1 - window } else { 0 };
        let slice = &values[start..=i];
        result[i] = slice.iter().sum::<f64>() / slice.len() as f64;
    }
    result
}

/// Compute rolling standard deviation over a sliding window.
pub fn compute_rolling_std(values: &[f64], window: usize) -> Vec<f64> {
    if window == 0 || values.is_empty() {
        return vec![0.0; values.len()];
    }
    let mut result = vec![0.0; values.len()];
    for (i, res_val) in result.iter_mut().enumerate() {
        let (_, variance) = window_stats(values, i, window);
        *res_val = variance.sqrt();
    }
    result
}

/// Compute rolling z-score over a sliding window.
pub fn compute_rolling_zscore(values: &[f64], window: usize) -> Vec<f64> {
    if window == 0 || values.is_empty() {
        return vec![0.0; values.len()];
    }
    let mut result = vec![0.0; values.len()];
    for i in 0..values.len() {
        let (mean, variance) = window_stats(values, i, window);
        let std = variance.sqrt();
        result[i] = if std > 0.0 {
            (values[i] - mean) / std
        } else {
            0.0
        };
    }
    result
}

/// Exponential decay (EMA) over a series.
pub fn compute_exponential_decay(values: &[f64], alpha: f64) -> Vec<f64> {
    if values.is_empty() {
        return vec![];
    }
    let mut result = vec![0.0; values.len()];
    result[0] = values[0];
    for i in 1..values.len() {
        result[i] = alpha * values[i] + (1.0 - alpha) * result[i - 1];
    }
    result
}

/// Order book matching simulation.
/// Match a buy order against the resting ask book. Split out of
/// `simulate_order_matching` (extract-method, cx/wD8) — same terms, same
/// fill-volume arithmetic order (`remaining_vol.min(ask.1)` then subtract
/// from both sides) as before.
fn match_order_against_asks(
    ask_book: &mut [(f64, f64)],
    order_id: &str,
    price: f64,
    mut remaining_vol: f64,
) -> Vec<HashMap<String, String>> {
    let mut matches = Vec::new();
    for ask in ask_book {
        let ask_price = ask.0;
        if ask_price <= price && remaining_vol > 0.0 && ask.1 > 0.0 {
            let fill_vol = remaining_vol.min(ask.1);
            remaining_vol -= fill_vol;
            ask.1 -= fill_vol;

            let mut m = HashMap::new();
            m.insert("order_id".to_string(), order_id.to_string());
            m.insert("match_price".to_string(), ask_price.to_string());
            m.insert("match_volume".to_string(), fill_vol.to_string());
            matches.push(m);
        }
    }
    matches
}

/// Match a sell order against the resting bid book. Split out of
/// `simulate_order_matching` (extract-method, cx/wD8) — same terms, same
/// fill-volume arithmetic order as before.
fn match_order_against_bids(
    bid_book: &mut [(f64, f64)],
    order_id: &str,
    price: f64,
    mut remaining_vol: f64,
) -> Vec<HashMap<String, String>> {
    let mut matches = Vec::new();
    for bid in bid_book {
        let bid_price = bid.0;
        if bid_price >= price && remaining_vol > 0.0 && bid.1 > 0.0 {
            let fill_vol = remaining_vol.min(bid.1);
            remaining_vol -= fill_vol;
            bid.1 -= fill_vol;

            let mut m = HashMap::new();
            m.insert("order_id".to_string(), order_id.to_string());
            m.insert("match_price".to_string(), bid_price.to_string());
            m.insert("match_volume".to_string(), fill_vol.to_string());
            matches.push(m);
        }
    }
    matches
}

pub fn simulate_order_matching(
    bids: Vec<(f64, f64)>,
    asks: Vec<(f64, f64)>,
    orders: Vec<(String, String, f64, f64)>,
) -> Vec<HashMap<String, String>> {
    let mut matches = Vec::new();
    let mut bid_book = bids;
    let mut ask_book = asks;

    bid_book.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
    ask_book.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));

    for (order_id, side, price, volume) in orders {
        if side.to_lowercase() == "buy" {
            matches.extend(match_order_against_asks(
                &mut ask_book,
                &order_id,
                price,
                volume,
            ));
        } else {
            matches.extend(match_order_against_bids(
                &mut bid_book,
                &order_id,
                price,
                volume,
            ));
        }
    }

    matches
}

/// Window statistics helper: returns (mean, variance) for the window ending at index `i`.
fn window_stats(values: &[f64], i: usize, window: usize) -> (f64, f64) {
    let start = if i >= window - 1 { i + 1 - window } else { 0 };
    let slice = &values[start..=i];
    let n = slice.len() as f64;
    let mean = slice.iter().sum::<f64>() / n;
    let variance = if n > 1.0 {
        slice.iter().map(|&x| (x - mean).powi(2)).sum::<f64>() / (n - 1.0)
    } else {
        0.0
    };
    (mean, variance)
}
