//! The SQL plan cache's key and lookup outcome (EG-DURABLE-KERNEL-R041): a
//! repeated parameterized SQL execution plan is cached keyed by its
//! statement digest AND the schema version it was compiled against, so a
//! schema change invalidates every plan compiled under the old version
//! without needing a separate cache-clearing mechanism. This is the
//! typed-model slice (`.1`): the key, a pure lookup decision over a
//! candidate cache entry, and the refusal for a lookup that is handed a
//! stale entry for the wrong statement. Wiring the cache into the
//! dispatcher and keying invalidation to the engine's existing dependency
//! clock is a later child.

use crate::contract::Digest256;

/// Identifies one cached SQL execution plan: the exact statement text's
/// digest, plus the schema version the plan was compiled against.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct PlanCacheKey {
    pub statement_digest: Digest256,
    pub schema_version: u64,
}

impl PlanCacheKey {
    pub fn new(statement_digest: Digest256, schema_version: u64) -> Self {
        Self {
            statement_digest,
            schema_version,
        }
    }
}

/// The outcome of looking a key up against a candidate cached entry's key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PlanCacheLookup {
    /// The candidate's digest and schema version both match: reuse its
    /// cached plan.
    Hit,
    /// No candidate was cached for this statement digest yet.
    Miss,
    /// The candidate was compiled for the SAME statement under a DIFFERENT
    /// schema version: its plan must never be reused as-is. The caller
    /// recompiles rather than silently serving a plan validated against
    /// column/type shapes that may no longer hold.
    StaleSchema { cached_schema_version: u64 },
}

/// Decide a cache lookup's outcome. `candidate` is `None` when nothing is
/// cached for this statement digest. Pure: makes no decision about WHICH
/// digest to look up under, and does not itself recompile a plan.
pub fn lookup(requested: &PlanCacheKey, candidate: Option<&PlanCacheKey>) -> PlanCacheLookup {
    let Some(candidate) = candidate else {
        return PlanCacheLookup::Miss;
    };
    if candidate.statement_digest != requested.statement_digest {
        // A lookup must always be addressed by its own statement digest; a
        // candidate for a different statement is a caller defect, treated
        // the same as no cached entry rather than ever being reused.
        return PlanCacheLookup::Miss;
    }
    if candidate.schema_version != requested.schema_version {
        return PlanCacheLookup::StaleSchema {
            cached_schema_version: candidate.schema_version,
        };
    }
    PlanCacheLookup::Hit
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(statement: &[u8], schema_version: u64) -> PlanCacheKey {
        PlanCacheKey::new(Digest256::sha256(statement), schema_version)
    }

    #[test]
    fn repeated_statement_same_schema_is_a_hit() {
        let requested = key(b"SELECT 1", 7);
        let cached = key(b"SELECT 1", 7);
        assert_eq!(lookup(&requested, Some(&cached)), PlanCacheLookup::Hit);
    }

    #[test]
    fn nothing_cached_is_a_miss() {
        let requested = key(b"SELECT 1", 7);
        assert_eq!(lookup(&requested, None), PlanCacheLookup::Miss);
    }

    #[test]
    fn schema_version_advance_invalidates_the_cached_plan() {
        let requested = key(b"SELECT 1", 8);
        let cached = key(b"SELECT 1", 7);
        assert_eq!(
            lookup(&requested, Some(&cached)),
            PlanCacheLookup::StaleSchema {
                cached_schema_version: 7
            }
        );
    }

    #[test]
    fn a_candidate_for_a_different_statement_is_never_reused() {
        let requested = key(b"SELECT 1", 7);
        let cached = key(b"SELECT 2", 7);
        assert_eq!(lookup(&requested, Some(&cached)), PlanCacheLookup::Miss);
    }

    #[test]
    fn key_is_deterministic_across_separate_digest_computations() {
        assert_eq!(key(b"SELECT 1", 7), key(b"SELECT 1", 7));
        assert_ne!(key(b"SELECT 1", 7), key(b"SELECT 1", 8));
    }
}
