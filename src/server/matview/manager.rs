//! Plan-backed materialized-view MANAGER (CONCEPT:EG-KG.storage.plan-backed-matview).
//!
//! Holds the in-RAM registry of named, plan-backed materialized views + their freshness
//! state. A subsystem-local process singleton ([`manager`]) so BOTH the CDC hub
//! (`CdcHub::emit` → [`note_change`]) and the dispatch handler reach it WITHOUT threading
//! a new field through the shared `ServerState` (owned by another lane).

use std::collections::HashMap;
use std::sync::OnceLock;

use parking_lot::Mutex;

/// The durable DEFINITION of a plan-backed materialized view (CONCEPT:EG-KG.storage.plan-backed-matview):
/// a named `wire::Plan` over ONE `graph`. Serialized (MessagePack) into the disjoint
/// `plan_matviews` redb table. Carries NO result rows — the materialized RESULT rides the
/// version-keyed, RLS-aware result cache on the graph's `GraphCore`, so a write that bumps
/// the graph version retires it for free.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct PlanMatView {
    pub name: String,
    pub graph: String,
    pub plan: eg_types::wire::Plan,
}

/// In-RAM per-view tracking: the definition + a CDC-driven freshness flag.
struct Tracked {
    def: PlanMatView,
    /// Set by [`PlanMatViewManager::note_change`] when a committed write to `def.graph`
    /// lands — so the next `Get` (or an explicit `Refresh`) recomputes even if a stale
    /// cache entry somehow lingered. A freshly-materialized view clears it.
    stale: bool,
}

/// The process-global plan-backed matview registry.
#[derive(Default)]
pub struct PlanMatViewManager {
    views: Mutex<HashMap<String, Tracked>>,
}

static MANAGER: OnceLock<PlanMatViewManager> = OnceLock::new();

/// The process-global plan-backed matview manager (CONCEPT:EG-KG.storage.plan-backed-matview).
pub fn manager() -> &'static PlanMatViewManager {
    MANAGER.get_or_init(PlanMatViewManager::default)
}

impl PlanMatViewManager {
    /// Insert (or replace) a view definition, marking it freshly materialized.
    pub fn define(&self, def: PlanMatView) {
        self.views
            .lock()
            .insert(def.name.clone(), Tracked { def, stale: false });
    }

    /// The stored definition for `name`, if any.
    pub fn get(&self, name: &str) -> Option<PlanMatView> {
        self.views.lock().get(name).map(|t| t.def.clone())
    }

    /// Whether `name` needs recompute: a CDC change since the last materialize (or an
    /// unknown view) is stale. Reuses the (hash, version) result-cache discipline — this
    /// is the belt-and-braces signal ON TOP of the version key.
    pub fn is_stale(&self, name: &str) -> bool {
        self.views.lock().get(name).map(|t| t.stale).unwrap_or(true)
    }

    /// Mark `name` freshly materialized (clears the CDC stale flag).
    pub fn mark_fresh(&self, name: &str) {
        if let Some(t) = self.views.lock().get_mut(name) {
            t.stale = false;
        }
    }

    /// Drop a view's tracking; returns whether it existed.
    pub fn drop_view(&self, name: &str) -> bool {
        self.views.lock().remove(name).is_some()
    }

    /// The sorted names of every tracked view (observability/tests).
    pub fn list(&self) -> Vec<String> {
        let mut v: Vec<String> = self.views.lock().keys().cloned().collect();
        v.sort();
        v
    }

    /// CDC INVALIDATION (CONCEPT:EG-KG.storage.matview-cdc-invalidation): mark EVERY view
    /// over `graph` stale. Called from `CdcHub::emit` per committed change. A committed
    /// write already bumped the graph's OCC version (retiring the cached result under the
    /// (query_hash, version) key); this additionally flags the view so a subsequent `Get`
    /// definitely recomputes. Returns how many views it retired (tests read this).
    pub fn note_change(&self, graph: &str) -> usize {
        let mut views = self.views.lock();
        let mut n = 0;
        for t in views.values_mut() {
            if t.def.graph == graph && !t.stale {
                t.stale = true;
                n += 1;
            }
        }
        n
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn def(name: &str, graph: &str) -> PlanMatView {
        PlanMatView {
            name: name.into(),
            graph: graph.into(),
            plan: eg_types::wire::Plan::new(vec![]),
        }
    }

    #[test]
    fn cdc_change_marks_only_views_over_that_graph_stale() {
        // A LOCAL manager (not the global singleton) so the test is isolated.
        let m = PlanMatViewManager::default();
        m.define(def("v_a", "g1"));
        m.define(def("v_b", "g1"));
        m.define(def("v_c", "g2"));

        // Freshly defined ⇒ not stale.
        assert!(!m.is_stale("v_a"));
        assert!(!m.is_stale("v_c"));

        // A committed change to g1 retires BOTH views over g1, none over g2.
        assert_eq!(m.note_change("g1"), 2);
        assert!(m.is_stale("v_a"));
        assert!(m.is_stale("v_b"));
        assert!(!m.is_stale("v_c"), "a g2 view is untouched by a g1 change");

        // A second change to g1 retires nothing new (already stale).
        assert_eq!(m.note_change("g1"), 0);

        // Re-materialize clears the flag; a later change retires it again.
        m.mark_fresh("v_a");
        assert!(!m.is_stale("v_a"));
        assert_eq!(m.note_change("g1"), 1);
        assert!(m.is_stale("v_a"));
    }

    #[test]
    fn define_get_drop_roundtrip() {
        let m = PlanMatViewManager::default();
        assert!(m.get("v").is_none());
        assert!(m.is_stale("v"), "an unknown view is stale (needs compute)");
        m.define(def("v", "g1"));
        assert_eq!(m.get("v").unwrap().graph, "g1");
        assert_eq!(m.list(), vec!["v".to_string()]);
        assert!(m.drop_view("v"));
        assert!(!m.drop_view("v"), "second drop is a no-op");
        assert!(m.get("v").is_none());
    }
}
