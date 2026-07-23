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
    let mut weighted_results = Vec::new();
    for (ordinal, (node_id, mut similarity, props)) in candidates.into_iter().enumerate() {
        if let Some(props_bytes) = props {
            if let Ok(properties) = eg_types::msgpack::decode_property_object(&props_bytes) {
                // Filter out strictly stale facts where the validity window has closed
                if let Some(vu) = properties
                    .get("valid_until")
                    .and_then(|value| value.as_u64())
                {
                    if now > vu {
                        continue;
                    }
                }

                // Apply temporal decay to confidence using the ONE shared
                // Ebbinghaus curve (CONCEPT:EG-KG.compute.handled-outside-single-anchor, `eg_core::decay`): the same
                // half-life model the time-series `decay_weighted_mean` uses. Here
                // the unit is DAYS with a 30-day half-life — identical numerics to
                // the previously-inlined `(-ln2/30 * age_days).exp()`.
                let mut current_confidence = properties
                    .get("confidence")
                    .and_then(|value| value.as_f64())
                    .unwrap_or(1.0);
                if let Some(vf) = properties
                    .get("valid_from")
                    .and_then(|value| value.as_u64())
                {
                    if now > vf {
                        let age_days = (now - vf) as f64 / 86400.0;
                        current_confidence *= crate::decay::ebbinghaus_weight(age_days, 30.0);
                    }
                }

                // Adjust similarity by current confidence (salience)
                similarity *= current_confidence as f32;
            }
        }
        weighted_results.push((ordinal, node_id, similarity));
    }

    // Re-rank by the new confidence-weighted similarity. The original ordinal is
    // the exact stable-sort tiebreak used by the historical full sort. NaN scores
    // are explicitly last so partial selection receives a total ordering.
    let compare = |left: &(usize, String, f32), right: &(usize, String, f32)| match (
        left.2.is_nan(),
        right.2.is_nan(),
    ) {
        (true, false) => std::cmp::Ordering::Greater,
        (false, true) => std::cmp::Ordering::Less,
        _ => right
            .2
            .partial_cmp(&left.2)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left.0.cmp(&right.0)),
    };
    if weighted_results.len() > n_results {
        weighted_results.select_nth_unstable_by(n_results, compare);
        weighted_results.truncate(n_results);
    }
    weighted_results.sort_by(compare);
    weighted_results
        .into_iter()
        .map(|(_, node_id, similarity)| (node_id, similarity))
        .collect()
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
}
