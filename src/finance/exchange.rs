// CONCEPT:KG-2.20e — Exchange & Execution Engine
//
// TWAP/VWAP execution algorithms, order book simulation, market impact,
// and pairs trading signal computation. Complements the existing
// simulate_order_matching in algorithms.rs.

use serde::{Deserialize, Serialize};

/// A single order in the book.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Order {
    pub id: String,
    pub side: String,   // "buy" or "sell"
    pub price: f64,
    pub quantity: f64,
    pub timestamp: u64,
}

/// Fill result from order matching.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Fill {
    pub order_id: String,
    pub fill_price: f64,
    pub fill_quantity: f64,
    pub side: String,
}

/// TWAP execution schedule — split a large order into N equal time slices.
pub fn twap_schedule(
    total_quantity: f64,
    n_slices: usize,
    start_time: u64,
    interval_secs: u64,
) -> Vec<(u64, f64)> {
    if n_slices == 0 {
        return vec![];
    }
    let slice_qty = total_quantity / n_slices as f64;
    (0..n_slices)
        .map(|i| (start_time + i as u64 * interval_secs, slice_qty))
        .collect()
}

/// VWAP execution schedule — weight slices by historical volume profile.
pub fn vwap_schedule(
    total_quantity: f64,
    volume_profile: &[f64],
    start_time: u64,
    interval_secs: u64,
) -> Vec<(u64, f64)> {
    if volume_profile.is_empty() {
        return vec![];
    }
    let total_vol: f64 = volume_profile.iter().sum();
    if total_vol <= 0.0 {
        return twap_schedule(total_quantity, volume_profile.len(), start_time, interval_secs);
    }
    volume_profile
        .iter()
        .enumerate()
        .map(|(i, &vol)| {
            let weight = vol / total_vol;
            (start_time + i as u64 * interval_secs, total_quantity * weight)
        })
        .collect()
}

/// Estimate market impact using the square-root model.
///
/// Impact = σ * k * sqrt(Q / ADV)
///
/// Where σ = daily volatility, k = impact coefficient (~0.1-0.5),
/// Q = order quantity, ADV = average daily volume.
pub fn estimate_market_impact(
    daily_volatility: f64,
    order_quantity: f64,
    average_daily_volume: f64,
    impact_coefficient: f64,
) -> f64 {
    if average_daily_volume <= 0.0 {
        return 0.0;
    }
    daily_volatility * impact_coefficient * (order_quantity / average_daily_volume).sqrt()
}

/// Pairs trading spread computation.
///
/// Computes the Z-score of the spread between two price series
/// using the hedge ratio from OLS regression.
pub fn pairs_trading_signal(
    prices_a: &[f64],
    prices_b: &[f64],
    lookback: usize,
) -> Vec<f64> {
    let n = prices_a.len().min(prices_b.len());
    if n < lookback || lookback < 2 {
        return vec![f64::NAN; n];
    }

    let mut signals = vec![f64::NAN; n];

    for i in (lookback - 1)..n {
        let window_a = &prices_a[(i + 1 - lookback)..=i];
        let window_b = &prices_b[(i + 1 - lookback)..=i];

        // OLS: a = β * b + α
        let mean_a: f64 = window_a.iter().sum::<f64>() / lookback as f64;
        let mean_b: f64 = window_b.iter().sum::<f64>() / lookback as f64;

        let mut cov = 0.0;
        let mut var_b = 0.0;
        for j in 0..lookback {
            cov += (window_a[j] - mean_a) * (window_b[j] - mean_b);
            var_b += (window_b[j] - mean_b).powi(2);
        }

        let beta = if var_b > 1e-12 { cov / var_b } else { 1.0 };
        let alpha = mean_a - beta * mean_b;

        // Compute spread and its z-score
        let spread: Vec<f64> = window_a.iter()
            .zip(window_b.iter())
            .map(|(&a, &b)| a - beta * b - alpha)
            .collect();

        let spread_mean: f64 = spread.iter().sum::<f64>() / lookback as f64;
        let spread_var: f64 = spread.iter().map(|s| (s - spread_mean).powi(2)).sum::<f64>() / lookback as f64;
        let spread_std = spread_var.sqrt();

        if spread_std > 1e-12 {
            let current_spread = prices_a[i] - beta * prices_b[i] - alpha;
            signals[i] = (current_spread - spread_mean) / spread_std;
        } else {
            signals[i] = 0.0;
        }
    }

    signals
}

/// Simple limit order book matching engine.
pub fn match_orders(orders: &[Order]) -> Vec<Fill> {
    let mut bids: Vec<Order> = Vec::new(); // sorted descending by price
    let mut asks: Vec<Order> = Vec::new(); // sorted ascending by price
    let mut fills: Vec<Fill> = Vec::new();

    for order in orders {
        match order.side.as_str() {
            "buy" => {
                // Try to match against asks
                let mut remaining = order.quantity;
                let mut matched_asks = Vec::new();

                for (idx, ask) in asks.iter().enumerate() {
                    if remaining <= 0.0 || order.price < ask.price {
                        break;
                    }
                    let fill_qty = remaining.min(ask.quantity);
                    fills.push(Fill {
                        order_id: order.id.clone(),
                        fill_price: ask.price,
                        fill_quantity: fill_qty,
                        side: "buy".to_string(),
                    });
                    remaining -= fill_qty;
                    if (ask.quantity - fill_qty).abs() < 1e-12 {
                        matched_asks.push(idx);
                    }
                }

                // Remove fully matched asks (in reverse to preserve indices)
                for idx in matched_asks.into_iter().rev() {
                    asks.remove(idx);
                }

                if remaining > 1e-12 {
                    let mut new_order = order.clone();
                    new_order.quantity = remaining;
                    bids.push(new_order);
                    bids.sort_by(|a, b| b.price.partial_cmp(&a.price).unwrap_or(std::cmp::Ordering::Equal));
                }
            }
            "sell" => {
                let mut remaining = order.quantity;
                let mut matched_bids = Vec::new();

                for (idx, bid) in bids.iter().enumerate() {
                    if remaining <= 0.0 || order.price > bid.price {
                        break;
                    }
                    let fill_qty = remaining.min(bid.quantity);
                    fills.push(Fill {
                        order_id: order.id.clone(),
                        fill_price: bid.price,
                        fill_quantity: fill_qty,
                        side: "sell".to_string(),
                    });
                    remaining -= fill_qty;
                    if (bid.quantity - fill_qty).abs() < 1e-12 {
                        matched_bids.push(idx);
                    }
                }

                for idx in matched_bids.into_iter().rev() {
                    bids.remove(idx);
                }

                if remaining > 1e-12 {
                    let mut new_order = order.clone();
                    new_order.quantity = remaining;
                    asks.push(new_order);
                    asks.sort_by(|a, b| a.price.partial_cmp(&b.price).unwrap_or(std::cmp::Ordering::Equal));
                }
            }
            _ => {}
        }
    }

    fills
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_twap_schedule() {
        let schedule = twap_schedule(1000.0, 5, 0, 60);
        assert_eq!(schedule.len(), 5);
        let total: f64 = schedule.iter().map(|(_, q)| q).sum();
        assert!((total - 1000.0).abs() < 1e-10);
    }

    #[test]
    fn test_vwap_schedule() {
        let profile = vec![100.0, 200.0, 300.0, 200.0, 100.0];
        let schedule = vwap_schedule(1000.0, &profile, 0, 60);
        assert_eq!(schedule.len(), 5);
        let total: f64 = schedule.iter().map(|(_, q)| q).sum();
        assert!((total - 1000.0).abs() < 1e-10);
        // Middle slice should have the most
        assert!(schedule[2].1 > schedule[0].1);
    }

    #[test]
    fn test_market_impact() {
        let impact = estimate_market_impact(0.02, 10000.0, 1_000_000.0, 0.3);
        assert!(impact > 0.0);
        assert!(impact < 1.0); // Should be reasonable
    }

    #[test]
    fn test_pairs_signal() {
        let a = vec![100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0, 107.0, 108.0, 109.0];
        let b = vec![50.0, 50.5, 51.0, 51.5, 52.0, 52.5, 53.0, 53.5, 54.0, 54.5];
        let signal = pairs_trading_signal(&a, &b, 5);
        assert_eq!(signal.len(), 10);
        assert!(signal[4].is_finite());
    }

    #[test]
    fn test_match_orders() {
        let orders = vec![
            Order { id: "b1".into(), side: "buy".into(), price: 100.0, quantity: 10.0, timestamp: 1 },
            Order { id: "s1".into(), side: "sell".into(), price: 99.0, quantity: 5.0, timestamp: 2 },
        ];
        let fills = match_orders(&orders);
        assert!(!fills.is_empty());
    }
}
