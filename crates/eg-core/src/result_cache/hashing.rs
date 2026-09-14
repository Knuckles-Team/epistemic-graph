use std::hash::{Hash, Hasher};

use super::ResultCache;

impl ResultCache {
    /// Hash a query's identity into the 128-bit key component. `kind` distinguishes
    /// the surfaces (so an identical text under SQL vs Cypher never collides) and
    /// `payload` is the query text or the canonical-bytes of a plan. Two `u64`
    /// `DefaultHasher` passes (salted) give a 128-bit space — collision-resistant
    /// enough for a per-graph in-memory cache (a collision would only serve a wrong
    /// cached result; the version still gates staleness across writes).
    pub fn hash_query(kind: &str, payload: &[u8]) -> u128 {
        fn one(salt: u64, kind: &str, payload: &[u8]) -> u64 {
            let mut h = std::collections::hash_map::DefaultHasher::new();
            salt.hash(&mut h);
            kind.hash(&mut h);
            payload.hash(&mut h);
            h.finish()
        }
        let lo = one(0, kind, payload);
        let hi = one(0x9E37_79B9_7F4A_7C15, kind, payload);
        ((hi as u128) << 64) | (lo as u128)
    }

    /// Hash an RLS ACTOR-SCOPE identity (its row-visibility key, e.g. the caller's
    /// `agent_id`) into the `actor_scope_hash` key component
    /// (CONCEPT:EG-KG.query.rls-scoped-result-cache). An EMPTY scope — the single-tenant /
    /// security-off case — hashes to `0`, so the key collapses to `(query_hash, version)`
    /// and existing callers that never pass a scope are byte-for-byte unchanged. Two
    /// callers with the same visibility key hash the same (they may share a cached result);
    /// two with different keys hash differently (they NEVER do).
    pub fn hash_actor_scope(scope: &str) -> u64 {
        if scope.is_empty() {
            return 0;
        }
        let mut h = std::collections::hash_map::DefaultHasher::new();
        "actor-scope".hash(&mut h);
        scope.hash(&mut h);
        // Keep 0 reserved for "no scope": a real scope that happens to hash to 0 is
        // nudged to 1, so a non-empty scope can never collide with the unscoped key.
        let v = h.finish();
        if v == 0 {
            1
        } else {
            v
        }
    }
}
