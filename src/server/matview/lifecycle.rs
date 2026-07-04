//! Plan-backed materialized-view LIFECYCLE (CONCEPT:EG-KG.storage.plan-backed-matview):
//! result MATERIALIZATION (run the stored plan through the eg-plan runtime), the RESULT
//! cache-KEY derivation (so define/get/refresh agree on one key), and durable (de)serialize
//! of a definition for the disjoint `plan_matviews` redb table.

use eg_core::compute::semantic::SemanticStore;
use eg_core::graph::GraphView;
use eg_core::result_cache::ResultCache;

use super::manager::PlanMatView;

/// Derive the RESULT-cache key component for a plan-backed matview
/// (CONCEPT:EG-KG.storage.plan-backed-matview). Hashes the serialized plan + the reorder
/// hint under a dedicated `matview` kind, so define / get / refresh compute the IDENTICAL
/// `query_hash` and share one cached result; the graph `version` (the cache's 2nd key
/// dimension) makes a write retire it, and the `actor_scope_hash` (3rd dimension) keeps it
/// out of a different RLS actor's lookups.
pub fn plan_hash(def: &PlanMatView) -> u128 {
    let mut payload = rmp_serde::to_vec_named(&def.plan).unwrap_or_default();
    payload.extend(
        def.reorder_filter_selectivity
            .unwrap_or(f64::NAN)
            .to_le_bytes(),
    );
    ResultCache::hash_query("matview", &payload)
}

/// MATERIALIZE a plan-backed matview: run its stored `wire::Plan` through the eg-plan
/// runtime over `view`/`semantic` and return the `[id, score|nil]` rows. Uses the SAME
/// `execute` seam (Lane 0's `Driver`) a `UnifiedQuery` runs through — a matview is just a
/// named, cached plan. A `TsScan`/`ForeignScan` leg without its ctx binding degrades to no
/// rows (never errors), exactly as a bare `UnifiedQuery` would with a default ctx.
pub fn materialize(
    def: &PlanMatView,
    view: &GraphView,
    semantic: &SemanticStore,
) -> Result<Vec<(String, Option<f32>)>, String> {
    let ctx = eg_plan::PlanCtx::new(view, semantic);
    let out = eg_plan::execute(&def.plan, &ctx)?;
    Ok(out.rows().iter().map(|r| (r.id.clone(), r.score)).collect())
}

/// Serialize a definition for the durable `plan_matviews` redb row.
pub fn encode_def(def: &PlanMatView) -> Result<Vec<u8>, String> {
    rmp_serde::to_vec_named(def).map_err(|e| format!("serialize plan matview: {e}"))
}

/// Decode a durable `plan_matviews` row back into a definition.
pub fn decode_def(blob: &[u8]) -> Result<PlanMatView, String> {
    rmp_serde::from_slice(blob).map_err(|e| format!("deserialize plan matview: {e}"))
}
