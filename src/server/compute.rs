//! Off-lock compute helpers: run CPU-heavy read-only work on the blocking pool,
//! and confidence-weight semantic-search hits — both never under the graph lock.

use crate::protocol::Response;

/// Run a CPU-heavy, read-only computation on the blocking thread pool
/// (CONCEPT:EG-KG.txn.per-graph-write-isolation). Callers snapshot whatever graph state the computation
/// needs under a short read lock, drop the lock, then hand the owned snapshot
/// here — so the tokio runtime threads and the per-graph RwLock are never
/// held across O(V·E)-class work.
pub(crate) async fn compute_off_lock<T, F>(req_id: u64, f: F) -> Result<T, Response>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| Response::err(req_id, format!("Blocking compute task failed: {}", e)))
}

/// Confidence-weight raw semantic-search hits (CONCEPT:EG-KG.txn.per-graph-write-isolation): drop
/// strictly-stale facts (validity window closed), apply Ebbinghaus temporal
/// decay (30-day half-life) to each hit's confidence, re-rank by the
/// decay-weighted similarity and truncate to `n_results`. Pure function —
/// runs on the blocking pool, never under the graph lock.
pub(crate) fn weight_semantic_results(
    candidates: Vec<(String, f32, Option<Vec<u8>>)>,
    now: u64,
    n_results: usize,
) -> Vec<(String, f32)> {
    if n_results == 0 {
        return Vec::new();
    }
    // The original ordinal is kept as the stable tiebreak for re-ranking.
    let mut weighted_results: Vec<(usize, String, f32)> = candidates
        .into_iter()
        .enumerate()
        .filter_map(|(ordinal, (node_id, similarity, props))| {
            let salience = salience_at(props.as_deref(), now)?;
            // Adjust similarity by current confidence (salience).
            Some((ordinal, node_id, similarity * salience))
        })
        .collect();

    // Re-rank by the new confidence-weighted similarity. The original ordinal is
    // the exact stable-sort tiebreak used by the historical full sort. NaN scores
    // are explicitly last so partial selection receives a total ordering.
    if weighted_results.len() > n_results {
        weighted_results.select_nth_unstable_by(n_results, rank_order);
        weighted_results.truncate(n_results);
    }
    weighted_results.sort_by(rank_order);
    weighted_results
        .into_iter()
        .map(|(_, node_id, similarity)| (node_id, similarity))
        .collect()
}

/// The multiplier a hit's properties apply to its similarity, or `None` when the
/// fact is strictly stale (its validity window has closed) and must be dropped.
/// A hit without decodable properties is unweighted (`1.0`).
fn salience_at(props: Option<&[u8]>, now: u64) -> Option<f32> {
    let Some(properties) =
        props.and_then(|bytes| eg_types::msgpack::decode_property_object(bytes).ok())
    else {
        return Some(1.0);
    };
    let property_u64 = |key: &str| properties.get(key).and_then(|value| value.as_u64());
    // Filter out strictly stale facts where the validity window has closed.
    if property_u64("valid_until").is_some_and(|valid_until| now > valid_until) {
        return None;
    }
    // Apply temporal decay to confidence using the ONE shared Ebbinghaus curve
    // (CONCEPT:EG-KG.compute.handled-outside-single-anchor, `eg_core::decay`): the same
    // half-life model the time-series `decay_weighted_mean` uses. Here the unit is
    // DAYS with a 30-day half-life — identical numerics to the previously-inlined
    // `(-ln2/30 * age_days).exp()`.
    let mut current_confidence = properties
        .get("confidence")
        .and_then(|value| value.as_f64())
        .unwrap_or(1.0);
    if let Some(valid_from) = property_u64("valid_from").filter(|valid_from| now > *valid_from) {
        let age_days = (now - valid_from) as f64 / 86400.0;
        current_confidence *= crate::decay::ebbinghaus_weight(age_days, 30.0);
    }
    Some(current_confidence as f32)
}

/// Descending weighted similarity, NaN last, original ordinal as the tiebreak.
fn rank_order(left: &(usize, String, f32), right: &(usize, String, f32)) -> std::cmp::Ordering {
    match (left.2.is_nan(), right.2.is_nan()) {
        (true, false) => std::cmp::Ordering::Greater,
        (false, true) => std::cmp::Ordering::Less,
        _ => right
            .2
            .partial_cmp(&left.2)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left.0.cmp(&right.0)),
    }
}

#[cfg(test)]
mod tests {
    use super::weight_semantic_results;

    #[test]
    fn semantic_weighting_selects_prefix_and_preserves_tie_order() {
        let candidates = vec![
            ("low".to_string(), 0.1, None),
            ("first-tie".to_string(), 0.9, None),
            ("second-tie".to_string(), 0.9, None),
            ("middle".to_string(), 0.5, None),
        ];
        assert_eq!(
            weight_semantic_results(candidates, 0, 2),
            vec![
                ("first-tie".to_string(), 0.9),
                ("second-tie".to_string(), 0.9),
            ]
        );
    }

    #[test]
    fn semantic_weighting_places_nan_last_and_short_circuits_zero_limit() {
        let candidates = vec![
            ("nan".to_string(), f32::NAN, None),
            ("finite".to_string(), 0.5, None),
        ];
        assert_eq!(
            weight_semantic_results(candidates.clone(), 0, 1),
            vec![("finite".to_string(), 0.5)]
        );
        assert!(weight_semantic_results(candidates, 0, 0).is_empty());
    }

    /// Pins the property-driven weighting: explicit confidence, the inclusive end of
    /// the validity window, a future `valid_from` (no decay), and undecodable bytes.
    #[test]
    fn semantic_weighting_pins_confidence_window_edges_and_bad_props() {
        let props = |value: serde_json::Value| Some(rmp_serde::to_vec_named(&value).unwrap());
        let now = 1_000_000u64;
        let candidates = vec![
            (
                "confident".to_string(),
                0.8,
                props(serde_json::json!({"confidence": 0.5})),
            ),
            (
                "window-ends-now".to_string(),
                0.3,
                props(serde_json::json!({"valid_until": now})),
            ),
            (
                "future".to_string(),
                0.6,
                props(serde_json::json!({"valid_from": now + 1, "confidence": 0.5})),
            ),
            ("undecodable".to_string(), 0.2, Some(vec![0xC1])),
        ];
        assert_eq!(
            weight_semantic_results(candidates, now, 10),
            vec![
                ("confident".to_string(), 0.4),
                ("window-ends-now".to_string(), 0.3),
                ("future".to_string(), 0.3),
                ("undecodable".to_string(), 0.2),
            ]
        );
    }
}
