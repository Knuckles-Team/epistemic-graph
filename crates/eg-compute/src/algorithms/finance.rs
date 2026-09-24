//! Order-book simulation. (The rolling mean/std/z-score/EMA helpers that lived here were
//! uncalled duplicates of the series kernels and were removed — EH-530; use
//! `eg_numeric::series`.)

use std::collections::HashMap;

// ── Quant epistemic-graph Algorithms ─────────────────────────────────────────────────

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
