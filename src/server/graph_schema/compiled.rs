//! The compiled GraphSchema cache (X9 §1.3 "Compiled cache", EH-137/EH-382).
//!
//! Composing a graph's schema (Turtle parse, K1–K7, EL⁺/RL terminology
//! classification) is a pure function of the composed schema's content identity,
//! [`crate::graph::GraphSchemaSources::composed_digest`]: the ordered scope, id, origin and the
//! shapes/ontology document digests of every core and dynamic source. Callers
//! validate the sources before asking, so every declared document digest is the
//! digest of the bytes it travels with, and the core catalog is this binary's.
//!
//! No graph owns an entry. Two graphs that compose the same schema share one; an
//! attach, detach, replace or core upgrade yields a different identity, so the old
//! entry is never looked up again and ages out of the bounded LRU. A stale
//! composition therefore cannot be served, and a missing entry only recompiles:
//! this is derived state, never authority. Failures are not cached.
//!
//! Concurrent misses on one identity are single-flight: they share one
//! [`OnceLock`], so exactly one caller compiles and the others wait for its result.

use std::sync::{Arc, OnceLock};

use eg_types::contract::Digest256;
use parking_lot::Mutex;

use super::compose::ComposedSchema;

/// Distinct composed schemas kept compiled at once. The shipped core corpus alone
/// composes to ~12.7k ontology triples, so an entry costs megabytes; sixteen bounds
/// the cache while covering every graph that shares a handful of attached schemas.
pub(crate) const COMPILED_SCHEMA_CAPACITY: usize = 16;

type Flight<T> = Arc<OnceLock<Result<Arc<T>, String>>>;

/// A bounded, single-flight, content-keyed memo of compiled values.
pub(crate) struct CompiledCache<T> {
    capacity: usize,
    /// Least recently used first.
    entries: Mutex<Vec<(Digest256, Flight<T>)>>,
    compiles: std::sync::atomic::AtomicU64,
}

impl<T> CompiledCache<T> {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            entries: Mutex::new(Vec::new()),
            compiles: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// The compiled value for `key`, compiling it once on a miss. Callers that miss
    /// concurrently on the same key wait for the one compilation in flight.
    pub(crate) fn get_or_compile(
        &self,
        key: Digest256,
        compile: impl FnOnce() -> Result<T, String>,
    ) -> Result<Arc<T>, String> {
        let flight = self.flight(key);
        let outcome = flight
            .get_or_init(|| {
                self.compiles
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                compile().map(Arc::new)
            })
            .clone();
        if outcome.is_err() {
            self.forget(key, &flight);
        }
        outcome
    }

    /// How many compilations this cache has run (hits run none).
    #[cfg(test)]
    pub(crate) fn compile_count(&self) -> u64 {
        self.compiles.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn flight(&self, key: Digest256) -> Flight<T> {
        let mut entries = self.entries.lock();
        if let Some(index) = entries.iter().position(|(entry_key, _)| *entry_key == key) {
            let entry = entries.remove(index);
            let flight = Arc::clone(&entry.1);
            entries.push(entry);
            return flight;
        }
        if entries.len() >= self.capacity {
            entries.remove(0);
        }
        let flight = Flight::default();
        entries.push((key, Arc::clone(&flight)));
        flight
    }

    /// Drop a failed flight so the next caller compiles again, unless a newer
    /// flight already replaced it under the same key.
    fn forget(&self, key: Digest256, flight: &Flight<T>) {
        self.entries
            .lock()
            .retain(|(entry_key, entry)| *entry_key != key || !Arc::ptr_eq(entry, flight));
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.entries.lock().len()
    }
}

/// The process-wide cache every composition with an attached source goes through:
/// the RDF write guard, restore/rehydrate, reads, and attach-time validation.
pub(crate) fn compiled_schemas() -> &'static CompiledCache<ComposedSchema> {
    static CACHE: OnceLock<CompiledCache<ComposedSchema>> = OnceLock::new();
    CACHE.get_or_init(|| CompiledCache::new(COMPILED_SCHEMA_CAPACITY))
}

impl ComposedSchema {
    /// The full-ABox tableau verdict over this composition's ontology (EH-355,
    /// option (c)), decided at most once per compiled entry. Only acceptance is
    /// remembered: a refusal or an exhausted budget is decided again next time.
    /// The lock makes concurrent attaches of one identity single-flight.
    pub(crate) fn require_abox_consistency(
        &self,
        budget: eg_rdf::owl::DerivationBudget,
    ) -> Result<(), String> {
        let mut accepted = self.abox_consistent.lock();
        if *accepted {
            return Ok(());
        }
        match eg_rdf::tableau::abox_consistency_within(&self.ontology, budget) {
            Ok(true) => {
                *accepted = true;
                Ok(())
            }
            Ok(false) => Err(
                "ONTOLOGY_INCONSISTENT: the composed schema's individual assertions have no model"
                    .to_string(),
            ),
            Err(exhausted) => Err(exhausted.to_string()),
        }
    }
}

#[cfg(test)]
#[path = "compiled_tests.rs"]
mod tests;
