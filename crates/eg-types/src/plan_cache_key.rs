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

/// The SQL prepare site's one entry point (`EG-DURABLE-KERNEL-R041.2`): a
/// statement handler calls [`PlanCache::prepare`] instead of unconditionally
/// recompiling. Generic over the compiled plan type `P` so this same cache
/// serves the pgwire/MySQL/MSSQL prepare paths without each inventing its
/// own cache.
#[derive(Debug)]
pub struct PlanCache<P> {
    entries: std::collections::HashMap<Digest256, (u64, P)>,
}

// Hand-written rather than `#[derive(Default)]`: the derive would add a
// spurious `P: Default` bound (an empty cache needs none).
impl<P> Default for PlanCache<P> {
    fn default() -> Self {
        Self {
            entries: std::collections::HashMap::new(),
        }
    }
}

impl<P: Clone> PlanCache<P> {
    pub fn new() -> Self {
        Self::default()
    }

    /// How many distinct statement digests currently hold a cached plan.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Prepare a statement: reuse the cached plan on a `Hit`, otherwise call
    /// `compile` and cache its result under `schema_version` (replacing any
    /// stale entry). Returns the lookup outcome alongside the plan so a
    /// caller can distinguish a cache hit from a (re)compile without a
    /// second lookup.
    pub fn prepare(
        &mut self,
        statement_digest: Digest256,
        schema_version: u64,
        compile: impl FnOnce() -> P,
    ) -> (PlanCacheLookup, P) {
        let requested = PlanCacheKey::new(statement_digest, schema_version);
        let candidate = self
            .entries
            .get(&statement_digest)
            .map(|(cached_schema_version, _)| {
                PlanCacheKey::new(statement_digest, *cached_schema_version)
            });
        let decision = lookup(&requested, candidate.as_ref());
        if let PlanCacheLookup::Hit = decision {
            let (_, plan) = self
                .entries
                .get(&statement_digest)
                .expect("Hit implies an entry exists for this digest");
            return (decision, plan.clone());
        }
        let plan = compile();
        self.entries
            .insert(statement_digest, (schema_version, plan.clone()));
        (decision, plan)
    }
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

    #[test]
    fn prepare_compiles_once_then_reuses_the_cached_plan() {
        let mut cache: PlanCache<u32> = PlanCache::new();
        let digest = Digest256::sha256(b"SELECT 1");
        let mut compiles = 0;
        let (first, plan) = cache.prepare(digest, 7, || {
            compiles += 1;
            42
        });
        assert_eq!(first, PlanCacheLookup::Miss);
        assert_eq!(plan, 42);
        let (second, plan) = cache.prepare(digest, 7, || {
            compiles += 1;
            42
        });
        assert_eq!(second, PlanCacheLookup::Hit);
        assert_eq!(plan, 42);
        assert_eq!(compiles, 1, "the second prepare must not recompile");
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn prepare_recompiles_after_the_schema_version_advances() {
        let mut cache: PlanCache<u32> = PlanCache::new();
        let digest = Digest256::sha256(b"SELECT 1");
        cache.prepare(digest, 7, || 42);
        let (decision, plan) = cache.prepare(digest, 8, || 43);
        assert_eq!(
            decision,
            PlanCacheLookup::StaleSchema {
                cached_schema_version: 7
            }
        );
        assert_eq!(plan, 43);
        // The stale entry is replaced, not kept alongside the new one.
        let (hit, plan) = cache.prepare(digest, 8, || panic!("must not recompile"));
        assert_eq!(hit, PlanCacheLookup::Hit);
        assert_eq!(plan, 43);
    }
}
