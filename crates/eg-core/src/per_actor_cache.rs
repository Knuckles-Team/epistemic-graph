// The bounded per-actor, version-and-generation-keyed cache the RLS read paths share
// (U-142/U-143/U-145, BUG-130).
//
// `rls_projection_cache::ProjectionCache` (an `Arc<GraphCore>` per actor) and
// `rls_view_cache::FilteredViewCache` (an `Arc<GraphView>` per actor) were two copies of
// this one mechanism — the same LRU order, the same capacity, the same
// `(actor, version)` hit rule, the same whole-image `generation` race check and the same
// `Debug` summary — differing only in what they hold. Each of those modules now keeps its
// own rationale (WHY that particular result is worth amortizing, and what a caller may do
// with a shared entry) and names this type; the mechanism lives here once, so the two
// caches cannot drift on the invalidation contract they both depend on.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use parking_lot::Mutex;

/// Bounded so a deployment with many distinct concurrent actors cannot grow a cache
/// unboundedly. Realistic concurrent-actor counts (service accounts, interactive users)
/// are small; a capacity miss costs exactly what every call cost before the cache
/// existed — never worse.
pub(crate) const CAPACITY: usize = 64;

struct Inner<T> {
    entries: HashMap<String, (u64, Arc<T>)>,
    // Recency order, oldest at the front. `HashMap` iteration order is not defined, so
    // eviction order is tracked explicitly here rather than relied on from the map.
    order: VecDeque<String>,
    // Monotonic WHOLE-IMAGE generation, distinct from the per-write `version` key above.
    // `version` only ever advances on a normal committed mutation; it is untouched by a
    // same-version resident-image replacement (`GraphCore::prepare_snapshot_publish`/
    // `install_committed_snapshot` reconciling to the SAME already-committed version) or
    // by a whole-image wipe that intentionally does not bump `version` at all
    // (`GraphCore::clear`, and `hibernate`, which reuses it). Either path can leave a
    // per-actor entry cached at a `version` that is still numerically "current" while the
    // actual graph content underneath it changed completely — exactly the disagreement
    // live U-142 reproduced (governed node reads see the current image, Cypher over the
    // same graph sees the just-replaced/emptied one, or vice versa) and that U-143
    // observed as a cache serving stale nonempty rows after a fresh native execution had
    // already gone empty. [`PerActorCache::invalidate_all`] bumps this on every such
    // whole-image transition; [`PerActorCache::put`] only publishes a build if the
    // generation it captured before starting is still current, which also closes the race
    // the sibling `result_cache`'s `invalidate_all()`-then-clear idiom does not: an
    // expensive build in flight when a whole-image replacement happens must not publish
    // its now-stale result afterward.
    generation: u64,
}

impl<T> Default for Inner<T> {
    fn default() -> Self {
        Self {
            entries: HashMap::new(),
            order: VecDeque::new(),
            generation: 0,
        }
    }
}

impl<T> Inner<T> {
    fn touch(&mut self, actor: &str) {
        if let Some(position) = self.order.iter().position(|cached| cached == actor) {
            self.order.remove(position);
        }
        self.order.push_back(actor.to_string());
    }
}

/// One bounded per-actor cache of an expensive, RLS-filtered read result.
pub(crate) struct PerActorCache<T>(Mutex<Inner<T>>);

impl<T> Default for PerActorCache<T> {
    fn default() -> Self {
        Self(Mutex::new(Inner::default()))
    }
}

impl<T> std::fmt::Debug for PerActorCache<T> {
    /// `GraphCore` derives `Debug`; report summary counts, not cached content — entries
    /// hold `Arc<GraphCore>`/`Arc<GraphView>`, which are not `Debug` themselves, and
    /// per-actor cache contents are not diagnostic output anyone should print anyway.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self.0.lock();
        f.debug_struct("PerActorCache")
            .field("len", &inner.entries.len())
            .field("generation", &inner.generation)
            .finish()
    }
}

impl<T> PerActorCache<T> {
    /// Fresh hit only: `None` on a cold miss OR a stale (version-mismatched) entry.
    ///
    /// A stale entry is left in place (not evicted here) — it is cheap to hold until the
    /// next `put` for that actor overwrites it, and evicting eagerly would need the same
    /// lock a concurrent rebuild's `put` also wants. Entries are unreachable past an
    /// [`Self::invalidate_all`] regardless (it clears them outright), so `get` never needs
    /// to consult `generation` itself.
    pub(crate) fn get(&self, actor: &str, current_version: u64) -> Option<Arc<T>> {
        let mut inner = self.0.lock();
        let hit = match inner.entries.get(actor) {
            Some((version, cached)) if *version == current_version => Some(cached.clone()),
            _ => None,
        };
        if hit.is_some() {
            inner.touch(actor);
        }
        hit
    }

    /// The current whole-image generation, to be captured by a caller BEFORE it starts an
    /// (unlocked, potentially slow) rebuild and handed back to [`Self::put`] once that
    /// rebuild finishes — see the `generation` field doc for why.
    pub(crate) fn generation(&self) -> u64 {
        self.0.lock().generation
    }

    /// Insert/overwrite this actor's entry with a freshly built result, but ONLY if
    /// `generation` (captured via [`Self::generation`] right before the build started) is
    /// still the current one.
    ///
    /// A mismatch means an [`Self::invalidate_all`] landed while this build was in flight
    /// — the just-built result reflects an image that no longer exists, so it is discarded
    /// rather than published (never a correctness hazard to skip a store: the next reader
    /// simply re-triggers a fresh, now-current build). Checked and written under the SAME
    /// lock `invalidate_all` takes, so there is no window between the check and the store
    /// where a concurrent invalidation could sneak in.
    pub(crate) fn put(&self, actor: String, version: u64, generation: u64, value: Arc<T>) {
        let mut inner = self.0.lock();
        if generation != inner.generation {
            return;
        }
        if !inner.entries.contains_key(&actor) && inner.entries.len() >= CAPACITY {
            if let Some(evicted) = inner.order.pop_front() {
                inner.entries.remove(&evicted);
            }
        }
        inner.touch(&actor);
        inner.entries.insert(actor, (version, value));
    }

    /// Advance the generation and drop every cached entry (U-142/U-143/U-145, BUG-130).
    ///
    /// Call this from every whole-image transition that a plain `version` bump does not
    /// already cover on its own — `GraphCore::replace_snapshot` (same-version
    /// resident-image replacement) and `GraphCore::clear`/`hibernate` (wipe without a
    /// version bump). Bumping generation and clearing entries under ONE lock acquisition
    /// (rather than the sibling `result_cache`'s separate bump-then-clear) is what lets
    /// [`Self::put`]'s single re-check under the same lock be airtight: an in-flight
    /// build's eventual `put` either lands strictly before this call (and is then wiped by
    /// the `entries.clear()` below) or strictly after (and is then rejected by the
    /// generation check) — never both bypassed.
    pub(crate) fn invalidate_all(&self) {
        let mut inner = self.0.lock();
        inner.generation += 1;
        inner.entries.clear();
        inner.order.clear();
    }
}
