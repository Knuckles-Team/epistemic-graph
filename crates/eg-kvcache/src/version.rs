//! Data-version invalidation for the KV cache (CONCEPT:EG-KG.storage.content-addressed-put).
//!
//! ## The gap this closes
//!
//! [`crate::SharedKvIndex`] (EG-186) and [`crate::TieredCache`] (EG-185) are
//! **content-addressed by token-hash** — the address IS the content, so a *pure KV
//! page* (a deterministic function of `(token ids, model weights)`) is correctly served
//! from cache forever: identical tokens ⇒ identical KV bytes. But an LLM/agent
//! **context** that was *derived from graph data* (an assembled prompt, a retrieved
//! subgraph, a synthesized answer cached under a key that does NOT capture the underlying
//! data content) goes **stale** the moment that graph data changes — yet the same
//! token-hash keeps resolving to the old, now-wrong bytes. The audit calls this out:
//! "a cached KV/context entry is served even after the underlying graph data it was
//! derived from changes (stale LLM context)".
//!
//! ## The fix — mirror `eg-core`'s version-keyed result cache
//!
//! `eg-core::ResultCache` (CONCEPT:EG-KG.coordination.distributed-cache-coherence) already solved the analogous problem for
//! query RESULTS: it keys every entry by the `GraphCore::version()` (CONCEPT:EG-KG.txn.multi-op-occ-acid) —
//! a monotonic OCC write-generation bumped on EVERY committed write — so a single atomic
//! version bump retires every cached result at once (a stale entry can simply never be
//! looked up again). We mirror that **epoch** here: each cached KV/context entry captures
//! the [`DataVersion`] it was derived at, and an entry is a **MISS** once the store's
//! current version has moved past it.
//!
//! ## Epoch vs dependency-set — and why epoch
//!
//! Two designs were possible:
//!
//! * **Version epoch (chosen):** one monotonic version for the whole graph; any write
//!   bumps it and retires *every* version-tagged entry. Correct by construction (never a
//!   stale serve), O(1) to trigger, and it is EXACTLY the proven `ResultCache` pattern.
//!   Its only cost is *over-invalidation* — a write to an unrelated node also retires KV
//!   entries that did not depend on it — but that is a latency cost (recompute), never a
//!   correctness one.
//! * **Per-dependency set:** stamp each entry with the exact set of node-ids (+ their
//!   versions) it read, and invalidate only when one of THOSE changed. Precise (no
//!   over-invalidation) but it requires the context-derivation path to capture a precise
//!   read-set — information this leaf crate does not have (the KV/context bytes arrive
//!   already assembled) — and a per-entry dependency index. That precision is not needed
//!   for CORRECTNESS, so we take the simple epoch first, exactly as the task directs.
//!
//! A `Agnostic` sentinel is the escape hatch that keeps the epoch from harming the
//! genuine content-addressed dedup: a *pure KV page* is stamped [`DataVersion::Agnostic`]
//! and is **never** invalidated by a version bump (its address fully determines its
//! correctness). Only *derived-context* entries are stamped with a real
//! [`DataVersion::At`] and thus participate in epoch invalidation. This is what lets the
//! feature be **zero-cost / opt-in**: a cache used purely for content-addressed KV pages
//! (put `Agnostic`, never call `set_data_version`) behaves EXACTLY as before.

/// The data-version an entry was derived at — the invalidation epoch (CONCEPT:EG-KG.storage.content-addressed-put).
///
/// Deliberately an enum, NOT a bare `u64`, because a real `GraphCore::version()` starts
/// at `0` and increments; so `0` is a *valid* graph version and cannot double as the
/// "version-independent" sentinel. [`DataVersion::Agnostic`] is a distinct variant that
/// never collides with any real [`DataVersion::At`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum DataVersion {
    /// The entry does NOT depend on mutable graph data — a pure, content-addressed KV
    /// page (a deterministic function of its token-hash). It is **never** invalidated by
    /// a version bump: its address fully determines its correctness. This is the default
    /// so the content-addressed dedup / tiering path is untouched and zero-cost.
    Agnostic,
    /// The entry was DERIVED from graph data at this `GraphCore::version()`
    /// (CONCEPT:EG-KG.txn.multi-op-occ-acid). It stays fresh only while the store's current version equals
    /// this; a newer version retires it (stale LLM/agent context).
    At(u64),
}

impl Default for DataVersion {
    /// The default is [`DataVersion::Agnostic`] — a value put WITHOUT a data version is a
    /// pure content-addressed page that never goes stale (keeps existing dedup intact).
    fn default() -> Self {
        DataVersion::Agnostic
    }
}

impl DataVersion {
    /// A graph-derived version epoch (convenience for `DataVersion::At(v)`).
    pub const fn at(version: u64) -> Self {
        DataVersion::At(version)
    }

    /// Whether this is the version-independent (pure content-addressed) sentinel.
    pub const fn is_agnostic(self) -> bool {
        matches!(self, DataVersion::Agnostic)
    }

    /// Is an entry stamped `self` still **fresh** when the store's current version is
    /// `current`? The single correctness rule the whole feature rests on:
    ///
    /// * an [`Agnostic`](DataVersion::Agnostic) entry is fresh forever (pure KV page);
    /// * when the store's `current` version is `Agnostic`, version-tracking is inactive,
    ///   so nothing is considered stale (the zero-cost/disabled path);
    /// * otherwise an entry is fresh **iff** it was derived at exactly the current
    ///   version — a monotonic `GraphCore::version()` bump makes `At(old) != At(new)`, so
    ///   the entry misses (mirroring `ResultCache`'s `(hash, version)` key, KG-2.233).
    pub const fn is_fresh(self, current: DataVersion) -> bool {
        match (self, current) {
            (DataVersion::Agnostic, _) => true,
            (_, DataVersion::Agnostic) => true,
            (DataVersion::At(a), DataVersion::At(c)) => a == c,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn agnostic_is_never_stale() {
        // A pure KV page stays fresh no matter how far the graph version advances.
        assert!(DataVersion::Agnostic.is_fresh(DataVersion::Agnostic));
        assert!(DataVersion::Agnostic.is_fresh(DataVersion::At(0)));
        assert!(DataVersion::Agnostic.is_fresh(DataVersion::At(9_999)));
    }

    #[test]
    fn versioned_entry_fresh_only_at_matching_version() {
        // Version 0 is a REAL graph version (GraphCore starts at 0) — not agnostic.
        assert!(DataVersion::At(0).is_fresh(DataVersion::At(0)));
        assert!(!DataVersion::At(0).is_fresh(DataVersion::At(1)));
        assert!(DataVersion::At(7).is_fresh(DataVersion::At(7)));
        assert!(!DataVersion::At(7).is_fresh(DataVersion::At(8)));
    }

    #[test]
    fn inactive_tracking_never_invalidates() {
        // If the store's current version is Agnostic (set_data_version never called),
        // even a versioned entry is considered fresh — the disabled/zero-cost path.
        assert!(DataVersion::At(3).is_fresh(DataVersion::Agnostic));
    }

    #[test]
    fn default_is_agnostic() {
        assert_eq!(DataVersion::default(), DataVersion::Agnostic);
        assert!(DataVersion::default().is_agnostic());
    }
}
