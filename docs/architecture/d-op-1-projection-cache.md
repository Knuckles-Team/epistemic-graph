# D-OP-1: cache `project_core()`'s RLS projection per (actor, graph version)

Status: **IMPLEMENTED** (D-OB-20, eg-perf lane, 2026-08-01) — option (a) below,
exactly as designed: `crates/eg-core/src/rls_projection_cache.rs` (new, bounded
per-actor cache on `GraphCore`) + `GraphReadAuthority::project_core` in
`src/server/access.rs` split into a cache-checking wrapper and the original
(expensive) materialization, now named `build_projection`. Option (b)
(narrowing `is_active()` itself) was investigated and NOT implemented — see
"is_active() disposition" below for why.
Originally filed as design-only by the orchestrator lane (2026-07-31) sweeping
`lane-orchestrator-perf`, with a failing regression test
(`project_core_of_a_single_has_node_call_must_not_rebuild_the_whole_graph`,
`src/server/access.rs`, `universal_row_read_tests` module) pinning the
pre-fix behavior; that test now passes (cache-hit path, microsecond cost), and
two more were added alongside it: a write-then-reread invalidation proof
(`project_core_reflects_a_mutation_after_the_previous_projection_was_cached`)
and a read-mix burst benchmark
(`project_core_read_burst_mirrors_grounding_read_mix`) shaped like what a
production grounding delegation actually issues.

**Measured, same benchmark, before vs. after** (25-read burst, same actor, no
writes mid-burst, 20,000 nodes / 1024-dim embeddings — a controlled local
reproduction, run both ways from the identical `main` base commit `8920e97`):
- **Before** (unmodified `main`): total 37.640s / 25 reads, avg **1.506s**/call,
  min 1.382s, max 1.698s.
- **After** (this fix): total 189.27µs / 25 reads, avg **7.57µs**/call, min
  5.44µs, max 39.11µs.
- That is a **~199,000x** per-call speedup on the warm-cache path, and directly
  explains the reported "grounding alone was 90.14s against a 10.0s production
  budget" (D-OB-20/ServiceNow probe): a burst of N reads by the same actor at a
  stable graph version now costs one full rebuild (still ~1.3-1.5s, unchanged)
  plus N-1 cache hits (microseconds each) instead of N full rebuilds.
- The FIRST read after a write (a genuine cold miss) is UNCHANGED by this fix —
  still pays the full `O(V log V + E log E + V*d)` materialization. That cost
  is real and is not addressed here; see "What this does NOT fix" below.

**is_active() disposition**: investigated per D-OB-20's item 4. It is
`#[cfg(feature = "security")] { true } #[cfg(not(feature = "security"))] {
false }` — a BUILD-TIME flag ("is RLS compiled into this binary"), not a
per-request/per-actor decision, and the default/shipped build (`full`, which
`default` includes) always compiles `security` in — so in practice every
served request already pays `is_active() == true`. All 9 call sites (grep
across `dist_compute.rs`, `streaming.rs`, `txn.rs`, `access.rs` itself) use it
consistently as that same "is RLS compiled in" gate, mostly to conservatively
REJECT unscoped shared caching (materialized views, streaming cursors) whenever
RLS COULD apply — a correct, fail-closed posture. Narrowing it to a per-actor
"does this specific actor have zero restrictions" check (design option (b))
was considered and NOT implemented: there is no existing cheap check for
"this actor has zero possible restrictions" (the closest, `can_see_row`'s
`AgentRole::System` short-circuit, is checked PER ROW, not per actor, and
still costs an O(V) scan through `filter_view` to discover that every row
happened to be visible), and getting the narrowing wrong in either direction
is either a no-op (still slow) or a genuine RLS bypass (a faster read that
returns different rows — exactly the security bug D-OB-20's own instructions
warn against). The caching fix (a) already removes the cost for the common
case option (b) targeted (an actor who sees everything still gets a cache HIT
on repeat calls, just like a genuinely restricted actor does), so (b)'s
marginal benefit is now confined to a SINGLE actor's SINGLE cold-miss call —
not worth the risk given (a) already captures the win.

**What this does NOT fix** (reported precisely, per D-OB-20's own
instructions, rather than silently narrowing scope): the FIRST call for a
given (actor, graph-version) pair — after a fresh write, or for a brand-new
actor — still pays the full `O(V log V + E log E + V*d)` materialization
(~1.3-1.5s at 20,000 nodes / 1024 dims locally; ~1.3s reported live at 25,075
nodes against `__commons__`). Replacing THAT cost with the Cypher/RDF paths'
snapshot-level filter (option (c) below) remains unimplemented — every
primitive/algorithm handler downstream of `try_handle`'s terminal match
(`has_node`, `get_neighbors`, `shortest_path`, community mining, semantic
search, …) is written against `&GraphCore`/`Arc<GraphCore>`, not `&GraphView`,
and rewriting that surface is a materially larger, higher-risk change than
this one — see option (c)'s own writeup below, unchanged from the original
design. A workload that writes on every request from a rotating cast of
actors (no cache reuse) would see no improvement from this fix; the measured
production workload (grounding: a read-heavy burst per delegation, largely
between writes) is exactly the shape this fix targets and fixes.

## The problem, precisely

`src/server/handlers/graph_ops.rs::try_handle` (the terminal handler every
non-gateway-routed graph method reaches) runs, unconditionally, before its
`match`:

```rust
let core = read_authority.project_core(&core);
```

`GraphReadAuthority::project_core` (`src/server/access.rs`) is a no-op when
`!self.is_active()`, but `is_active()` (same file) is:

```rust
pub(crate) fn is_active(&self) -> bool {
    #[cfg(feature = "security")]
    { true }
    #[cfg(not(feature = "security"))]
    { false }
}
```

— hard-coded to `true` whenever the `security` feature is compiled in, for
**every** caller, regardless of whether that caller's `IsolationLayer`
actually restricts anything for them. When active, `project_core`:

1. `core.analysis_snapshot()` + `filter_view()` — one row-visibility pass.
2. `GraphCore::new()` — a fresh, empty graph.
3. Collect **all** node ids, **sort** them, clone each visible node's
   properties, `add_node` — for every node the actor can see.
4. Collect **all** edge keys, **sort** them, clone each visible edge's
   properties, `add_edge`.
5. Rebuild the **semantic store** — copy every visible node's embedding.
6. Clear the ledger (correct: the original mutation ledger cannot be safely
   row-filtered from its unstructured string form).

That is `O(V log V + E log E + V·d)` (`d` = embedding dimension) **on every
single call**, independent of what was actually asked for — a `HasNode`
(one hash lookup) pays the identical cost as a full graph export.

## Why it is invisible on the fast paths

`CypherQuery`/RDF handlers "retain their snapshot-level filter" (comment at
`graph_ops.rs:2338`) instead of calling `project_core` — they filter lazily,
at read time, over the existing structure. Gateway-routed mutations
(`ClaimWorkItem`, `BatchUpdate`, …) are intercepted by `try_handle_gateway`
**before** reaching `try_handle`, so they never pay it either. Only the
terminal, non-gateway READ handlers pay the cost — which is exactly the
`HasNode`/`GetNodes`/`GetNodeProperties`/`GetNodePropertiesBatch` family the
orchestrator lane measured at 2.7-3.0s live, with **zero** samples under 1s
across dozens of calls, against `CypherQuery`'s <0.0001s on 31 calls in the
same window. The two paths look identical to a caller until you cross-check
against `__commons__`'s actual size (25,075 nodes / 2,656 edges, live gauges)
and the resulting ~103 MB semantic-store memcpy per call (25,075 × 1024 dims
× 4 bytes).

## Constraint: do NOT weaken RLS

The projection is a **correctness control** — it is the only thing standing
between a caller and rows/edges/embeddings/counts they must not see. The bug
is that it is eager and uncached on every call, not that it exists. Any fix
must produce **byte-identical visible output** to today's `project_core` for
the same (actor, graph state) — the existing test
`alice_and_bob_same_tenant_shared_graph_cannot_observe_each_others_rows` (same
file) is the correctness oracle a cached implementation must keep passing
unmodified.

## Fix, ranked (cheapest / highest-leverage first)

### (a) — chosen: cache the projected core per (actor, graph-version), invalidate on mutation

`GraphCore` already carries exactly the invalidation key this needs, and
already uses this exact pattern for two other lazily-built, mutation-
invalidated caches on the same struct:

```rust
pub version: std::sync::atomic::AtomicU64,   // bumped once per committed write
ontology_index: RwLock<Option<OntologyTermIndex>>,   // lazy, "reused while node count is unchanged"
// (a sibling label index right below it, same shape)
```

Add a bounded cache alongside them:

```rust
/// Cached RLS projection per actor, invalidated by GraphCore::version()
/// advancing (CONCEPT:EG-KG.sharding.row-level-security, D-OP-1). A small
/// bounded map, not a single slot: unlike ontology_index (shared across all
/// actors, no row filtering), a projection is PER-ACTOR, so concurrent
/// distinct callers must not evict each other's cache. Capped (LRU) to bound
/// memory under many distinct actors; entries whose stored version no longer
/// matches self.version.load() are simply stale hits and get rebuilt in
/// place, same as a cold miss.
projection_cache: RwLock<LruCache<String /* actor */, (u64 /* version at cache time */, Arc<GraphCore>)>>,
```

`GraphReadAuthority::project_core` becomes:

```rust
pub(crate) fn project_core(&self, core: &Arc<GraphCore>) -> Arc<GraphCore> {
    if !self.is_active() {
        return core.clone();
    }
    let actor = self.actor.clone();   // already computed by from_verified
    let current_version = core.version.load(Ordering::Acquire);
    if let Some((cached_version, cached_core)) = core.projection_cache.read().peek(&actor) {
        if *cached_version == current_version {
            return cached_core.clone();
        }
    }
    let projected = self.build_projection(core);   // today's body, renamed
    core.projection_cache.write().put(actor, (current_version, projected.clone()));
    projected
}
```

Properties:
- **Correctness is unchanged**: a cache hit only fires when the graph's
  `version` (bumped on every committed write — already exists, already
  monotonic) has not moved since the entry was built, for that exact actor.
  A concurrent write during a read is not a new hazard: the existing code
  already serves a point-in-time snapshot per call; caching just amortizes
  the SAME snapshot across calls between writes.
- **No RLS weakening**: `is_active()` is untouched by this option (see (b)
  below for that, kept separate on purpose — combining them means a broken
  cache-key change and a broken activity-detection change would be
  indistinguishable if either regressed).
- **Bounded memory**: capacity tuned to the realistic concurrent-actor count
  (an LRU, not an unbounded map) — a single-tenant deployment with one
  service actor costs one cache slot; a multi-tenant deployment with N
  concurrently-active distinct actors costs at most N slots at any moment,
  aged out under memory pressure like `ontology_index`'s neighbor already is
  conceptually (though that one is unconditionally kept — this is the first
  *bounded* cache on this struct, worth flagging in review).
- **Correctness harness**: extend
  `alice_and_bob_same_tenant_shared_graph_cannot_observe_each_others_rows`
  with a write-then-reread case: after Alice's first `project_core` call is
  cached, mutate the graph (add a node Alice can see), call `project_core`
  again, and assert the SECOND projection reflects the mutation (proves the
  version check actually invalidates, not just that it compiles).

### (b) — complementary, smaller win: make `is_active()` reflect real restriction

Today `is_active()` is `true` for literally every caller whenever `security`
is compiled in, even an actor whose `IsolationLayer` grants them unrestricted
visibility (e.g. a `System`-role service account with no per-row grants to
filter). For that caller, `project_core` still pays the full cost to produce
an **identical** copy of the input. Gate `is_active()` (or add a fast
pre-check inside `project_core`) on
`IsolationLayer::actor_has_any_restriction(actor)` (name illustrative — the
real check should reuse whatever `can_see_row`'s underlying policy lookup
already computes, not re-derive it) so an unrestricted caller takes the
`core.clone()` fast path unconditionally, same as `!self.is_active()` today.
This is strictly additive on top of (a): even with caching, an un-restricted
caller pays a cache-lookup plus a clone on every call instead of a bare
clone; (b) removes that residual cost for the (likely common — service
accounts, admin tooling) unrestricted-caller case. Lower priority than (a)
because (a) alone already fixes the measured 15,600× regression for every
restricted caller, which is the reported symptom.

### (c) — considered, not recommended as the primary fix: filter lazily like Cypher/RDF

The `CypherQuery`/RDF path proves a snapshot-level, filter-at-read-time
design is sufficient and is the strongest available precedent. It was not
chosen as the PRIMARY fix here because it is a materially larger, more
invasive change: every primitive/algorithm currently written against a plain
`&GraphCore` (has_node, get_neighbors, shortest-path, …) would need to
either accept an `IsolationLayer` + actor and filter internally, or be
re-expressed over the `GraphView` abstraction the Cypher path already uses.
(a) gets the same effective outcome (no more per-call O(V) rebuild) as a
strictly smaller, lower-risk diff confined to `access.rs` + one new field on
`GraphCore`. (c) remains the better answer if this code is ever restructured
more broadly; it is not blocked by adopting (a) now.

## Verification plan for whoever implements this

1. Land (a). Re-run
   `project_core_of_a_single_has_node_call_must_not_rebuild_the_whole_graph`
   — must now pass (cache hit path is µs-order).
2. Add the write-then-reread invalidation case described above — must pass
   (proves the cache is not just fast, but correct after a mutation).
3. Re-run `alice_and_bob_same_tenant_shared_graph_cannot_observe_each_others_rows`
   unmodified — must still pass (proves no RLS weakening).
4. Live re-measurement (same method the orchestrator lane used): hit
   `/metrics` before/after on a real deployment, compare
   `epistemic_graph_request_duration_seconds` for `HasNode`/`GetNodes` mean
   and p100 against the 2.7-3.0s / zero-under-1s baseline recorded
   2026-07-31. Target: cached calls should land near `CypherQuery`'s
   existing <0.0001s fast-path order of magnitude for a cache hit, with only
   the first call after a mutation paying the full rebuild.
5. `EPISTEMIC_GRAPH_SLOW_QUERY_MS` is unset live today (D-OE-2, separately
   filed) — the slow-query log never fires. Set it as part of rolling this
   out so a REGRESSION in the cache (e.g. a key bug that never hits) is
   caught by the existing slow-query logging, not only by the next perf
   sweep.

## Addendum (BUG-130 / U-142, U-143, U-145) — whole-image generation

(a)'s invalidate-on-`version()`-change key handles every ORDINARY committed
mutation (each one bumps `version`), but it does **not** cover a whole-image
transition that can leave `version()` numerically unchanged: a same-version
resident-image reconciliation (`GraphCore::prepare_snapshot_publish` /
`install_committed_snapshot`, which route through `replace_snapshot`) or an
intentional non-version-bumping wipe (`GraphCore::clear`, and `hibernate`,
which reuses it). Live, this reproduced as U-142 (native Cypher observing an
empty snapshot while a governed node read on the same graph still saw data)
and its cache-specific angle U-143 (a cache serving stale nonempty rows after
a fresh native execution had already gone empty).

The sibling `result_cache` already called `invalidate_all()` at both
`replace_snapshot` and `clear`, but that alone is insufficient: `project_core`
builds its (expensive, `O(V log V + E log E + V*d)`) projection **off-lock**,
so a whole-image transition landing while a build is in flight could still
publish a now-stale result microseconds after a bare "clear the map"
invalidation ran.

Fix (`crates/eg-core/src/rls_projection_cache.rs`): a monotonic `generation`
counter lives inside the SAME mutex as the cache entries. A caller captures
`generation()` immediately before starting its unlocked rebuild and passes it
back to `put`, which only stores if that captured value is still current —
checked under the identical lock `invalidate_all` uses to bump the generation
and clear entries, so there is no window between the check and the store
where a concurrent invalidation can land unnoticed. `replace_snapshot` and
`clear` (and therefore `hibernate`) each call `invalidate_projection_cache()`
alongside their existing `result_cache.invalidate_all()`. See
`rls_projection_cache.rs`'s own module doc and its
`a_build_racing_invalidation_never_publishes_its_stale_result` /
`invalidate_all_evicts_a_same_version_entry_that_a_plain_version_check_would_keep_serving`
tests for the exact race this closes.

U-148's memory-pressure eviction (`src/cost.rs::enforce_memory_budgets`,
`src/server/persistence/cold_offload.rs::offload_cold_tenants`) turned out to
be a second, independent instance of the U-142 SYMPTOM produced by a
different mechanism (the registry never transitioning a fully-reclaimed
graph to catalog-only, so dispatch's lazy-open never re-fires) — see
`plans/graph-os-completion-program/designs/BUG-REMEDIATION-DESIGNS.md#bug-130--unified-projection-cache-and-eviction-registry-staleness-u-142u-143u-145u-148`
for the unified writeup covering both.
