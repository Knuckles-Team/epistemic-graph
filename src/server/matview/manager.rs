//! Plan-backed materialized-view MANAGER (CONCEPT:EG-KG.storage.plan-backed-matview).
//!
//! Holds the in-RAM registry of named, plan-backed materialized views + their freshness
//! state. A subsystem-local process singleton ([`manager`]) so BOTH the CDC hub
//! (`CdcHub::emit` → [`note_change`]) and the dispatch handler reach it WITHOUT threading
//! a new field through the shared `ServerState` (owned by another lane).

use std::collections::HashMap;
use std::sync::OnceLock;

use eg_plan::incremental::Circuit;
use eg_plan::RowSet;
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

/// Which maintenance mode a tracked view runs in
/// (CONCEPT:EG-KG.storage.incremental-matview).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    /// Its plan compiled to a DBSP circuit: `apply_delta` maintains `current` on each CDC
    /// change; `Get` serves `current` directly (no recompute, no cache round-trip).
    Incremental,
    /// Its plan has an op with no incremental form (Traverse/Reason/…): today's
    /// recompute-on-`Get` path, unchanged. A CDC change flags it stale.
    Recompute,
}

/// In-RAM per-view tracking: the definition + freshness flag + (for `Incremental` views)
/// the compiled circuit, its live maintained result, and its CDC watermark.
struct Tracked {
    def: PlanMatView,
    /// Set by [`PlanMatViewManager::note_change`] when a committed write to `def.graph`
    /// lands — so the next `Get` (or an explicit `Refresh`) recomputes even if a stale
    /// cache entry somehow lingered. A freshly-materialized view clears it. Governs
    /// `Mode::Recompute` views only; an `Incremental` view is fresh by construction.
    stale: bool,
    /// The maintenance mode (CONCEPT:EG-KG.storage.incremental-matview).
    mode: Mode,
    /// `Some` ⇒ `Mode::Incremental`: the compiled DBSP circuit `apply_delta` folds
    /// deltas into. `None` ⇒ `Mode::Recompute`.
    circuit: Option<Circuit>,
    /// The live, incrementally-maintained result an `Incremental` `Get` serves directly.
    current: RowSet,
    /// CDC watermark — the next `seq` this view expects (mirrors
    /// `cdc::ContinuousQuery::through_seq`). Deltas with `seq < through_seq` are already
    /// folded and skipped.
    through_seq: u64,
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
    /// Insert (or replace) a view definition in `Mode::Recompute`, marking it freshly
    /// materialized. The unchanged path — used for a plan the circuit compiler can't
    /// incrementalize and for boot reload (which re-materializes lazily on first `Get`).
    pub fn define(&self, def: PlanMatView) {
        self.views.lock().insert(
            def.name.clone(),
            Tracked {
                def,
                stale: false,
                mode: Mode::Recompute,
                circuit: None,
                current: RowSet::new(),
                through_seq: 0,
            },
        );
    }

    /// Install (or replace) a view in `Mode::Incremental` with its compiled `circuit`, an
    /// initial maintained `current` (the just-materialized rows), and the CDC `through_seq`
    /// watermark captured at definition time (CONCEPT:EG-KG.storage.incremental-matview).
    pub fn install_incremental(
        &self,
        def: PlanMatView,
        circuit: Circuit,
        current: RowSet,
        through_seq: u64,
    ) {
        self.views.lock().insert(
            def.name.clone(),
            Tracked {
                def,
                stale: false,
                mode: Mode::Incremental,
                circuit: Some(circuit),
                current,
                through_seq,
            },
        );
    }

    /// The maintenance mode of `name`, if tracked.
    pub fn mode(&self, name: &str) -> Option<Mode> {
        self.views.lock().get(name).map(|t| t.mode)
    }

    /// The live maintained result rows `[id, score?]` of an `Incremental` view (`None` for
    /// an unknown or `Recompute`-mode view — the caller then takes the recompute path).
    pub fn incremental_rows(&self, name: &str) -> Option<Vec<(String, Option<f32>)>> {
        let views = self.views.lock();
        let t = views.get(name)?;
        if t.mode != Mode::Incremental {
            return None;
        }
        Some(
            t.current
                .rows()
                .iter()
                .map(|r| (r.id.clone(), r.score))
                .collect(),
        )
    }

    /// A snapshot of an `Incremental` view's compiled circuit (for durable operator-state
    /// persistence). `None` for unknown/`Recompute` views.
    pub fn circuit_snapshot(&self, name: &str) -> Option<Circuit> {
        self.views.lock().get(name).and_then(|t| t.circuit.clone())
    }

    /// INCREMENTAL CDC maintenance (CONCEPT:EG-KG.storage.incremental-matview): fold one
    /// change into every `Mode::Incremental` view over `graph`. For each such view, only a
    /// change at/after its watermark is applied (mirroring `cdc::maintain`); the circuit's
    /// output updates `current` and the watermark advances. Returns how many views it
    /// maintained. `Mode::Recompute` views are UNTOUCHED here (they ride `note_change`).
    pub fn apply_delta(&self, graph: &str, event: &eg_types::wire::CdcEvent) -> usize {
        let mut views = self.views.lock();
        let mut n = 0;
        for t in views.values_mut() {
            if t.mode != Mode::Incremental || t.def.graph != graph {
                continue;
            }
            if event.seq < t.through_seq {
                continue;
            }
            if let Some(circuit) = t.circuit.as_mut() {
                let delta = super::incremental::event_to_delta(event);
                if !delta.is_empty() {
                    circuit.apply(&delta);
                    t.current = circuit.current();
                }
                t.through_seq = event.seq + 1;
                n += 1;
            }
        }
        n
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
            // Incremental views are maintained by `apply_delta`, not invalidated — the
            // stale flag governs only the recompute path.
            if t.mode == Mode::Incremental {
                continue;
            }
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

    /// A node-add CdcEvent carrying a `type`/`year` msgpack property blob.
    fn add_node_event(
        seq: u64,
        graph: &str,
        id: &str,
        ty: &str,
        year: i64,
    ) -> eg_types::wire::CdcEvent {
        let after =
            rmp_serde::to_vec_named(&serde_json::json!({"type": ty, "year": year})).unwrap();
        eg_types::wire::CdcEvent {
            seq,
            graph: graph.into(),
            kind: eg_types::wire::CdcKind::AddNode,
            node_id: id.into(),
            target_id: String::new(),
            label: ty.into(),
            before: Vec::new(),
            after,
            had_before: false,
            had_after: true,
        }
    }

    fn remove_node_event(
        seq: u64,
        graph: &str,
        id: &str,
        ty: &str,
        year: i64,
    ) -> eg_types::wire::CdcEvent {
        let before =
            rmp_serde::to_vec_named(&serde_json::json!({"type": ty, "year": year})).unwrap();
        eg_types::wire::CdcEvent {
            seq,
            graph: graph.into(),
            kind: eg_types::wire::CdcKind::RemoveNode,
            node_id: id.into(),
            target_id: String::new(),
            label: ty.into(),
            before,
            after: Vec::new(),
            had_before: true,
            had_after: false,
        }
    }

    #[test]
    fn apply_delta_maintains_incremental_view_end_to_end() {
        use eg_types::wire::{Op, Plan, Pred};
        // Scan{Doc} |> Filter year > 2000 — an Incremental-mode view.
        let plan = Plan::new(vec![
            Op::Scan {
                label: "Doc".into(),
            },
            Op::Filter {
                preds: vec![Pred::GtNum {
                    prop: "year".into(),
                    n: 2000.0,
                }],
            },
        ]);
        let circuit = Circuit::compile(&plan).unwrap();
        let d = PlanMatView {
            name: "v".into(),
            graph: "g".into(),
            plan,
        };

        let m = PlanMatViewManager::default();
        m.install_incremental(d, circuit, RowSet::new(), 0);
        assert_eq!(m.mode("v"), Some(Mode::Incremental));

        // add a passing Doc (2005), a filtered-out Doc (1999), a non-Doc — via CDC events.
        assert_eq!(
            m.apply_delta("g", &add_node_event(0, "g", "a", "Doc", 2005)),
            1
        );
        assert_eq!(
            m.apply_delta("g", &add_node_event(1, "g", "b", "Doc", 1999)),
            1
        );
        assert_eq!(
            m.apply_delta("g", &add_node_event(2, "g", "x", "Other", 2010)),
            1
        );
        let rows = m.incremental_rows("v").unwrap();
        assert_eq!(
            rows.iter().map(|(id, _)| id.as_str()).collect::<Vec<_>>(),
            vec!["a"]
        );

        // a change to a DIFFERENT graph maintains nothing.
        assert_eq!(
            m.apply_delta("other", &add_node_event(3, "other", "c", "Doc", 2005)),
            0
        );

        // remove the passing node → view empties.
        assert_eq!(
            m.apply_delta("g", &remove_node_event(3, "g", "a", "Doc", 2005)),
            1
        );
        assert!(m.incremental_rows("v").unwrap().is_empty());

        // note_change does NOT flag an Incremental view stale (it's maintained, not retired).
        assert_eq!(m.note_change("g"), 0);

        // a delta below the watermark is skipped (idempotent replay guard): not maintained.
        assert_eq!(
            m.apply_delta("g", &add_node_event(0, "g", "z", "Doc", 2005)),
            0
        );
        assert!(
            m.incremental_rows("v").unwrap().is_empty(),
            "stale seq is not folded"
        );
    }

    #[test]
    fn recompute_mode_view_is_untouched_by_apply_delta() {
        let m = PlanMatViewManager::default();
        m.define(def("r", "g")); // Recompute mode (empty plan)
        assert_eq!(m.mode("r"), Some(Mode::Recompute));
        assert_eq!(
            m.apply_delta("g", &add_node_event(0, "g", "a", "Doc", 2005)),
            0
        );
        assert!(
            m.incremental_rows("r").is_none(),
            "recompute views expose no incremental rows"
        );
        assert_eq!(
            m.note_change("g"),
            1,
            "recompute views still flag stale on CDC"
        );
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
