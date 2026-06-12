//! Off-lock compute helpers: run CPU-heavy read-only work on the blocking pool,
//! and confidence-weight semantic-search hits — both never under the graph lock.

use crate::protocol::Response;

/// Run a CPU-heavy, read-only computation on the blocking thread pool
/// (CONCEPT:KG-2.51). Callers snapshot whatever graph state the computation
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

/// Confidence-weight raw semantic-search hits (CONCEPT:KG-2.51): drop
/// strictly-stale facts (validity window closed), apply Ebbinghaus temporal
/// decay (30-day half-life) to each hit's confidence, re-rank by the
/// decay-weighted similarity and truncate to `n_results`. Pure function —
/// runs on the blocking pool, never under the graph lock.
pub(crate) fn weight_semantic_results(
    candidates: Vec<(String, f32, Option<Vec<u8>>)>,
    now: u64,
    n_results: usize,
) -> Vec<(String, f32)> {
    let mut weighted_results = Vec::new();
    for (node_id, mut similarity, props) in candidates {
        if let Some(props_bytes) = props {
            if let Ok(json_str) = String::from_utf8(props_bytes) {
                let node_data = crate::types::NodeData::from_json_props(node_id.clone(), &json_str);

                // Filter out strictly stale facts where the validity window has closed
                if let Some(vu) = node_data.valid_until {
                    if now > vu {
                        continue;
                    }
                }

                // Apply temporal decay to confidence (Ebbinghaus Forgetting Curve)
                let mut current_confidence = node_data.confidence;
                if let Some(vf) = node_data.valid_from {
                    if now > vf {
                        let age_secs = now - vf;
                        let age_days = age_secs as f64 / 86400.0;
                        // Half-life of 30 days (decay rate lambda = ln(2) / 30)
                        let decay_rate = std::f64::consts::LN_2 / 30.0;
                        current_confidence *= (-decay_rate * age_days).exp();
                    }
                }

                // Adjust similarity by current confidence (salience)
                similarity *= current_confidence as f32;
            }
        }
        weighted_results.push((node_id, similarity));
    }

    // Re-sort descending based on the new confidence-weighted similarity
    weighted_results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    weighted_results.truncate(n_results);
    weighted_results
}
