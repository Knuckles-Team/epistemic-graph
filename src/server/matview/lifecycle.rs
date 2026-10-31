//! Plan-backed materialized-view LIFECYCLE (CONCEPT:MEDULLA-KG.storage.plan-backed-matview):
//! result MATERIALIZATION (run the stored plan through the medulla-plan runtime), the RESULT
//! cache-KEY derivation (so define/get/refresh agree on one key), and durable (de)serialize
//! of a definition for the disjoint `plan_matviews` redb table.

use medulla_core::compute::semantic::SemanticStore;
use medulla_core::graph::GraphView;
use medulla_core::result_cache::ResultCache;

use super::manager::PlanMatView;

const MAX_MATVIEW_DEFINITION_BYTES: usize = 16 * 1024 * 1024;
const MAX_MATVIEW_DEFINITION_ITEMS: usize = 100_000;

fn limits() -> medulla_types::msgpack::MsgpackLimits {
    medulla_types::msgpack::MsgpackLimits::new(
        MAX_MATVIEW_DEFINITION_BYTES,
        MAX_MATVIEW_DEFINITION_ITEMS,
        64,
    )
}

/// Derive the RESULT-cache key component for a plan-backed matview
/// (CONCEPT:MEDULLA-KG.storage.plan-backed-matview). Hashes the serialized plan under a
/// dedicated `matview` kind, so define / get / refresh compute the identical
/// `query_hash` and share one cached result; the graph `version` (the cache's 2nd key
/// dimension) makes a write retire it, and the `actor_scope_hash` (3rd dimension) keeps it
/// out of a different RLS actor's lookups.
pub fn plan_hash(def: &PlanMatView) -> u128 {
    let payload = rmp_serde::to_vec_named(&def.plan).unwrap_or_default();
    ResultCache::hash_query("matview", &payload)
}

/// MATERIALIZE a plan-backed matview: run its stored `wire::Plan` through the medulla-plan
/// runtime over `view`/`semantic` and return the `[id, score|nil]` rows. Uses the SAME
/// `execute` seam (Lane 0's `Driver`) a `UnifiedQuery` runs through — a matview is just a
/// named, cached plan. A `TsScan`/`ForeignScan` leg without its ctx binding degrades to no
/// rows (never errors), exactly as a bare `UnifiedQuery` would with a default ctx.
pub fn materialize(
    def: &PlanMatView,
    view: &GraphView,
    semantic: &SemanticStore,
) -> Result<Vec<(String, Option<f32>)>, String> {
    let ctx = medulla_plan::PlanCtx::new(view, semantic);
    let out = medulla_plan::execute(&def.plan, &ctx)?;
    Ok(out.rows().iter().map(|r| (r.id.clone(), r.score)).collect())
}

/// Serialize a definition for the durable `plan_matviews` redb row.
pub fn encode_def(def: &PlanMatView) -> Result<Vec<u8>, String> {
    let bytes =
        rmp_serde::to_vec_named(def).map_err(|_| "serialize plan matview failed".to_string())?;
    medulla_types::msgpack::validate_single_value(&bytes, limits())
        .map_err(|_| "plan matview definition exceeds limits".to_string())?;
    Ok(bytes)
}

/// Decode a durable `plan_matviews` row back into a definition.
pub fn decode_def(blob: &[u8]) -> Result<PlanMatView, String> {
    medulla_types::msgpack::decode_bounded(blob, limits())
        .map_err(|_| "stored plan matview definition is invalid".to_string())
}
