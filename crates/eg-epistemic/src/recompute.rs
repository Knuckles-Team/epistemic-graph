//! CONCEPT:EG-KG.epistemic.truth-maintenance — X-6 / EPI-P3-2: a live,
//! **dependency-driven recompute (truth-maintenance) engine** wired directly onto
//! the paraconsistent TMS ([`crate::tms`]) and the [`crate::BeliefGraph`] /
//! [`crate::JustificationGraph`] machinery already shipped (X2). Same `epistemic-tms`
//! feature — this module is the missing *live* half of X2: X2 answers "is this claim
//! justified, given the attacks against it"; this module answers "now that something
//! it was derived from changed, what ELSE needs re-answering, and how do I make sure
//! I never leave a stale answer lying around looking valid".
//!
//! ## The model: materializations + their dependency set
//! Every inferred edge/cluster/summary/classification/agent-fact is a
//! [`Materialization`]: an id, the set of base-fact/input ids it was derived from
//! (`depends_on` — the invalidation-dependency edges), and optionally the
//! `generating_activity` (a model version, job id, or ontology version) that
//! produced it. This mirrors the `:DerivedFrom`/`:GeneratedBy` invalidation-dependency
//! edges EPI-P3-1 adds onto the stored graph; [`TruthMaintenance`] keeps its own
//! compact in-memory index of that same shape so this crate does not need to depend
//! on EPI-P3-1's storage layer to reason about it (a follow-up wiring populates
//! [`TruthMaintenance::register`] from those real edges once they land — see the
//! module docs' "Follow-up" note at the bottom).
//!
//! ## Change events drive truth maintenance, never silent staleness
//! A [`ChangeEvent`] — a base fact deleted or updated, a permission/policy scope
//! changed, a model retired, an ontology version bumped — is fed to
//! [`TruthMaintenance::on_change`]. It walks the reverse dependency index
//! ([`TruthMaintenance::dependents_of`]) to find every materialization transitively
//! depending on the changed id (directly, or through another materialization that
//! itself depends on it), and marks each `Stale`. A model/ontology retirement instead
//! matches on `generating_activity`, then stales everything transitively built on
//! TOP of the retired generator's own outputs too. Nothing derived from a changed
//! input is ever left `Fresh` by omission.
//!
//! ## Dependency-directed retraction, wired into the actual TMS
//! [`TruthMaintenance::retract_and_propagate`] is the one entry point that ties this
//! module to [`crate::tms::retract`]: it runs the existing paraconsistent
//! dependency-directed retraction over a [`BeliefGraph`] (the grounded-acceptance
//! cascade — an id is a retraction casualty iff it was grounded-accepted before and
//! is not anymore, once the withdrawn evidence is gone) and then feeds every
//! casualty through [`TruthMaintenance::on_change`], so a materialization tracked ONE
//! LEVEL ABOVE the raw argumentation graph (e.g. a summary built from a retracted
//! claim) goes `Stale` too — retracting a claim marks its dependents, all the way up.
//!
//! ## Paraconsistency preserved
//! This module never "resolves" a contradiction on the caller's behalf. Two
//! materializations built from directly contradicting claims (`p`/`not_p`, neither
//! grounded-accepted per [`crate::tms::paraconsistency_no_explosion`][pnx]) both
//! remain independently trackable — `Fresh`, `Stale`, or `Retracted` on their own
//! merits — and a retraction anywhere in the graph only propagates along REAL
//! dependency edges, never "everything connected to a contradiction". See
//! [`tests::contradicting_materializations_coexist_without_exploding`].
//!
//! [pnx]: crate::tms
//!
//! ## Recompute, never rest at Stale
//! [`TruthMaintenance::recompute`] is the other half: given a closure that
//! re-derives a fresh value (and its — possibly different — dependency set) for a
//! stale/retracted id, it either lands `Fresh` against the new dependencies, or, if
//! the closure concludes no valid value exists anymore, `Retracted`. `Stale` is
//! strictly a transient in-flight state between a change event and a caller acting
//! on it, never a resting state.
//!
//! ## Follow-up: wiring a live CDC hook
//! This module deliberately does not depend on `eg-types::wire::CdcEvent` /
//! `CdcKind` (streaming feature) — `eg-epistemic` has no `streaming` dependency and
//! this keeps the truth-maintenance core testable in complete isolation. A server-
//! side hook (once EPI-P3-1's storage-level dependency edges exist to seed
//! `register` calls from) maps the existing CDC vocabulary onto [`ChangeEvent`]
//! one-for-one: `CdcKind::RemoveNode` -> [`ChangeEvent::Deleted`], `CdcKind::UpdateNode`
//! -> [`ChangeEvent::Updated`]; permission/policy-change and model/ontology-version
//! events are not yet on the CDC feed at all and would need their own emission point.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use eg_core::graph::GraphView;
use serde::{Deserialize, Serialize};

use crate::adapter::BeliefGraph;
use crate::tms::{self, RetractionResult};

/// Lifecycle state of one tracked [`Materialization`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MaterializationStatus {
    /// Computed from its current `depends_on` set; nothing it depends on
    /// (transitively) has changed since.
    Fresh,
    /// At least one input it (transitively) depends on changed since this was
    /// computed — the current value is not to be trusted as-is, but has not been
    /// thrown away: a [`TruthMaintenance::recompute`] may restore it (possibly to
    /// the very same value), or dependency-directed retraction may still reinstate
    /// what it depended on. Strictly transient — see module docs.
    Stale,
    /// Withdrawn outright: either this id itself WAS the thing that changed/was
    /// deleted, or a recompute concluded no valid value exists anymore.
    Retracted,
}

/// What changed, upstream of zero or more tracked materializations. See the module
/// docs' "Follow-up" note for how a live CDC feed maps onto this.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ChangeEvent {
    /// A base fact / input node was deleted outright.
    Deleted(String),
    /// A base fact / input node's value changed (it still exists; its content
    /// differs) — dependents go `Stale`, never `Retracted` (a recompute may well
    /// reproduce the very same downstream answer).
    Updated(String),
    /// A permission / access-policy scope changed for `id` — everything derived
    /// under that scope is no longer trustworthy as-is, same severity as a delete.
    PolicyChanged(String),
    /// A model version `id` was retired — every materialization whose
    /// `generating_activity == Some(id)` is invalidated even though none of its
    /// GRAPH inputs changed; the generator itself is no longer trusted.
    ModelRetired(String),
    /// An ontology/schema version `id` was bumped — same effect as
    /// [`ChangeEvent::ModelRetired`], kept as a distinct variant so a caller's
    /// audit trail tells the two triggers apart.
    OntologyEvolved(String),
}

impl ChangeEvent {
    /// The id this event is keyed on: a plain input/base-fact id for
    /// `Deleted`/`Updated`/`PolicyChanged` (matched against a materialization's
    /// `depends_on`), or a generator id for `ModelRetired`/`OntologyEvolved`
    /// (matched against `generating_activity`).
    pub fn subject(&self) -> &str {
        match self {
            ChangeEvent::Deleted(id)
            | ChangeEvent::Updated(id)
            | ChangeEvent::PolicyChanged(id)
            | ChangeEvent::ModelRetired(id)
            | ChangeEvent::OntologyEvolved(id) => id,
        }
    }

    /// Whether this event retracts (not merely stales) a materialization tracked
    /// directly AT `subject()`. Every variant except `Updated` removes trust in the
    /// id itself, not just its downstream — an updated input still exists (only its
    /// content changed), so a materialization tracked at that same id goes `Stale`.
    fn retracts_subject(&self) -> bool {
        !matches!(self, ChangeEvent::Updated(_))
    }

    /// Whether `generating_activity` (a tracked materialization's own generator id)
    /// is the one this event retires.
    fn matches_generator(&self, generating_activity: Option<&str>) -> bool {
        match (self, generating_activity) {
            (ChangeEvent::ModelRetired(id), Some(g)) => id == g,
            (ChangeEvent::OntologyEvolved(id), Some(g)) => id == g,
            _ => false,
        }
    }
}

/// One derived materialization the engine tracks — the epistemic analogue of a
/// materialized view: an inferred edge, cluster, summary, classification, or
/// agent-fact, plus the base-fact/input ids it derives from (the
/// invalidation-dependency edges) and the generating activity (model/job/ontology
/// version) that produced it, if any.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Materialization {
    pub id: String,
    pub depends_on: BTreeSet<String>,
    pub generating_activity: Option<String>,
    pub status: MaterializationStatus,
}

/// The recompute / truth-maintenance engine: a registry of [`Materialization`]s plus
/// the reverse (`input id -> depending materialization ids`) index that makes
/// [`TruthMaintenance::dependents_of`] and [`TruthMaintenance::on_change`] cheap —
/// no full-registry scan per change event.
#[derive(Default, Debug, Clone)]
pub struct TruthMaintenance {
    materializations: BTreeMap<String, Materialization>,
    /// input id -> materialization ids naming it in `depends_on` (direct edges
    /// only; [`Self::dependents_of`] walks this transitively). Does NOT cover
    /// `generating_activity` matches — those are found by the linear scan in
    /// [`Self::on_change`], since model/ontology retirement is rare and not worth a
    /// second reverse index.
    dependents: BTreeMap<String, BTreeSet<String>>,
}

impl TruthMaintenance {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register (or re-register) a materialization: `id` derives from every id in
    /// `depends_on`, optionally via `generating_activity`. Starts (or resets to)
    /// `Fresh`. Re-registering `id` replaces its previous dependency set entirely
    /// (the reverse index is cleaned of stale entries first) — the shape
    /// [`Self::recompute`] relies on when a fresh computation discovers a different
    /// input set than the stale value had.
    pub fn register(
        &mut self,
        id: impl Into<String>,
        depends_on: impl IntoIterator<Item = impl Into<String>>,
        generating_activity: Option<String>,
    ) {
        let id = id.into();
        self.unregister(&id);
        let depends_on: BTreeSet<String> = depends_on.into_iter().map(Into::into).collect();
        for dep in &depends_on {
            self.dependents
                .entry(dep.clone())
                .or_default()
                .insert(id.clone());
        }
        self.materializations.insert(
            id.clone(),
            Materialization {
                id,
                depends_on,
                generating_activity,
                status: MaterializationStatus::Fresh,
            },
        );
    }

    /// Drop `id` and its reverse-index entries. Does not touch anything that
    /// depends ON `id` — a subsequent [`Self::on_change`] is what
    /// staleness-propagates to those.
    fn unregister(&mut self, id: &str) {
        if let Some(old) = self.materializations.remove(id) {
            for dep in &old.depends_on {
                if let Some(set) = self.dependents.get_mut(dep) {
                    set.remove(id);
                }
            }
        }
    }

    pub fn get(&self, id: &str) -> Option<&Materialization> {
        self.materializations.get(id)
    }

    pub fn status_of(&self, id: &str) -> Option<MaterializationStatus> {
        self.materializations.get(id).map(|m| m.status)
    }

    /// Every currently `Stale` materialization id — the "what's stale" query.
    pub fn stale(&self) -> BTreeSet<String> {
        self.materializations
            .values()
            .filter(|m| m.status == MaterializationStatus::Stale)
            .map(|m| m.id.clone())
            .collect()
    }

    /// The transitive closure of everything that depends on `id`, direct or
    /// through other materializations (a materialization can itself be another's
    /// input) — the "what depends on X" query. Cycle-guarded BFS over the reverse
    /// index; `id` itself is never included in its own result.
    pub fn dependents_of(&self, id: &str) -> BTreeSet<String> {
        let mut seen: BTreeSet<String> = BTreeSet::new();
        let mut queue: VecDeque<String> = VecDeque::new();
        queue.push_back(id.to_string());
        let mut out = BTreeSet::new();
        while let Some(cur) = queue.pop_front() {
            if let Some(direct) = self.dependents.get(&cur) {
                for d in direct {
                    if seen.insert(d.clone()) {
                        out.insert(d.clone());
                        queue.push_back(d.clone());
                    }
                }
            }
        }
        out
    }

    /// Dependency-driven truth maintenance: apply `event` and propagate. Every
    /// materialization transitively dependent on `event.subject()` (via
    /// `depends_on`) is marked `Stale`, as is every materialization whose
    /// `generating_activity` names a retired model/ontology version — plus
    /// everything transitively built on top of THOSE. If the event retracts its
    /// subject outright (everything but `Updated`), the materialization tracked AT
    /// `event.subject()` (if any) is marked `Retracted` rather than merely `Stale` —
    /// there is no possibility a recompute reproduces that same id, it is gone.
    /// Returns every materialization id whose status changed, for the caller to act
    /// on (log, notify, schedule recomputes, ...). A change event that touches
    /// nothing tracked returns an empty set — this is a query, not an error.
    pub fn on_change(&mut self, event: &ChangeEvent) -> BTreeSet<String> {
        let subject = event.subject().to_string();
        let mut changed = BTreeSet::new();

        // Direct + transitive dependents via depends_on.
        for id in self.dependents_of(&subject) {
            if self.stale_unless_retracted(&id) {
                changed.insert(id);
            }
        }

        // Generator-keyed events (model retirement / ontology evolution): every
        // materialization the retired generator produced goes stale, and so does
        // everything transitively built on top of THOSE.
        let generator_hit: Vec<String> = self
            .materializations
            .values()
            .filter(|m| event.matches_generator(m.generating_activity.as_deref()))
            .map(|m| m.id.clone())
            .collect();
        for id in generator_hit {
            if self.stale_unless_retracted(&id) {
                changed.insert(id.clone());
            }
            for dep in self.dependents_of(&id) {
                if self.stale_unless_retracted(&dep) {
                    changed.insert(dep);
                }
            }
        }

        // The subject itself, if tracked.
        if event.retracts_subject() {
            if let Some(m) = self.materializations.get_mut(&subject) {
                m.status = MaterializationStatus::Retracted;
                changed.insert(subject);
            }
        } else if self.stale_unless_retracted(&subject) {
            changed.insert(subject);
        }

        changed
    }

    /// Mark `id` `Stale` unless it is already `Retracted` (retraction is terminal —
    /// never demoted back to a lesser "in-flight" state by a later event); returns
    /// whether a status change actually happened, so callers can build `changed`
    /// sets without a second lookup.
    fn stale_unless_retracted(&mut self, id: &str) -> bool {
        match self.materializations.get_mut(id) {
            Some(m) if m.status != MaterializationStatus::Retracted => {
                let was_stale = m.status == MaterializationStatus::Stale;
                m.status = MaterializationStatus::Stale;
                !was_stale
            }
            _ => false,
        }
    }

    /// Recompute a `Stale`/`Retracted` materialization: run `f` to produce a fresh
    /// value plus its (possibly different) dependency set. On `Ok`, `id` is
    /// re-registered `Fresh` against the NEW dependency set (a recomputation can
    /// discover different inputs than the stale value had — mirrors
    /// [`Self::register`]'s replace-not-merge semantics). On `Err`, `id` is marked
    /// `Retracted` — recompute never leaves a materialization resting at `Stale`;
    /// see the module docs.
    pub fn recompute<F, T, E>(&mut self, id: &str, f: F) -> Result<T, E>
    where
        F: FnOnce() -> Result<(T, BTreeSet<String>), E>,
    {
        let generating_activity = self
            .materializations
            .get(id)
            .and_then(|m| m.generating_activity.clone());
        match f() {
            Ok((value, new_deps)) => {
                self.register(id, new_deps, generating_activity);
                Ok(value)
            }
            Err(err) => {
                if let Some(m) = self.materializations.get_mut(id) {
                    m.status = MaterializationStatus::Retracted;
                }
                Err(err)
            }
        }
    }

    /// Wire the paraconsistent TMS's dependency-directed retraction directly into
    /// this materialization index: retract `evidence_id` from `bg` via
    /// [`tms::retract`] (the grounded-acceptance cascade over the
    /// support/contradiction/attack topology), then feed EVERY argument-level
    /// casualty (`RetractionResult::retracted`, plus `evidence_id` itself) through
    /// [`Self::on_change`] as a [`ChangeEvent::Deleted`] — so anything built ON TOP
    /// of a retracted claim (a summary/cluster naming it in `depends_on`) goes
    /// `Stale` too, one tracking level above what the raw argumentation graph sees.
    /// Returns the raw TMS [`RetractionResult`] (the argumentation-level story: what
    /// was retracted, what was reinstated, and why — [`RetractionResult::justifications`]
    /// carries the pre-retraction proof tree for each casualty) alongside the
    /// materialization ids staled/retracted by propagating it.
    ///
    /// Reinstated arguments are deliberately NOT auto-un-staled here: a reinstated
    /// argument regaining grounded acceptance does not mean a materialization built
    /// from it is known-good again without actually recomputing it — that still
    /// requires an explicit [`Self::recompute`] (never silently resurrect a stored
    /// value on the strength of someone else's status flipping).
    pub fn retract_and_propagate(
        &mut self,
        bg: &BeliefGraph,
        evidence_id: &str,
    ) -> (RetractionResult, BTreeSet<String>) {
        let retraction = tms::retract(bg, evidence_id);

        let mut propagated = BTreeSet::new();
        propagated.extend(self.on_change(&ChangeEvent::Deleted(evidence_id.to_string())));
        for casualty in &retraction.retracted {
            propagated.extend(self.on_change(&ChangeEvent::Deleted(casualty.clone())));
        }

        (retraction, propagated)
    }
}

/// L45 (EPI-P3-1 wiring): register `derived_id`'s [`TruthMaintenance`] materialization
/// straight from its OWN already-stored provenance, instead of a caller hand-building a
/// `depends_on` set. Reads TWO real, already-written provenance channels off
/// `derived_id`'s node/edges in `view` (the SAME blobs [`BeliefGraph::from_graph_view`]
/// decodes elsewhere in this crate — no new persistence, mirrors the crate's own "read
/// the existing convention" posture):
///
///   * the node's own `invalidation_deps` property — EG-P3-1's universal
///     writeback-lineage tuple (see `eg-jobs::claim::commit_result_claim` /
///     `src/server/handlers/mining.rs::materialize_claim`, the two writers of this
///     convention): the ids whose change/removal invalidates this node, already
///     EXACTLY the `depends_on` set [`TruthMaintenance::register`] wants;
///   * any OUTGOING edge classified `:DerivedFrom` or `:GeneratedBy`
///     (`relationship_type` `DERIVED_FROM`/`GENERATED_BY`, case-insensitive, mirroring
///     [`crate::model::classify_relationship`]'s own convention) — each target is
///     folded into `depends_on` too (a `:DerivedFrom` target is itself an invalidation
///     dependency), and the FIRST `:GeneratedBy` target additionally becomes
///     `generating_activity` (the model/job/ontology version `ChangeEvent::ModelRetired`/
///     `ChangeEvent::OntologyEvolved` key on) — the real writers today emit
///     `claim --GENERATED_BY--> activity`, so this is the live edge shape, not a
///     speculative one; `DERIVED_FROM` is recognized for forward-compatibility with a
///     future writer that emits it directly as an edge rather than folding its targets
///     into `invalidation_deps`.
///
/// A node with NEITHER channel registers with an EMPTY `depends_on` (nothing tracked
/// invalidates it) — a legitimate answer, not an error. Both `:GeneratedBy` targets and
/// `invalidation_deps` entries are deliberately treated as opaque ids (this function
/// does not care whether they resolve to another tracked `Materialization`,
/// [`TruthMaintenance::dependents_of`] only needs the id to key the reverse index).
pub fn register_from_provenance(tm: &mut TruthMaintenance, view: &GraphView, derived_id: &str) {
    let mut depends_on: BTreeSet<String> = BTreeSet::new();
    let mut generating_activity: Option<String> = None;

    // Channel 1: the node's own `invalidation_deps` property (EG-P3-1).
    if let Some(blob) = view.node_properties.get(derived_id) {
        if let Ok(v) = rmp_serde::from_slice::<serde_json::Value>(blob) {
            if let Some(arr) = v.get("invalidation_deps").and_then(|x| x.as_array()) {
                for item in arr {
                    if let Some(s) = item.as_str() {
                        depends_on.insert(s.to_string());
                    }
                }
            }
        }
    }

    // Channel 2: outgoing `:DerivedFrom` / `:GeneratedBy` edges.
    for ((source, target), blobs) in &view.edge_properties {
        if source != derived_id {
            continue;
        }
        for blob in blobs {
            let Some(rel) = rmp_serde::from_slice::<serde_json::Value>(blob)
                .ok()
                .and_then(|v| {
                    v.get("relationship_type")
                        .and_then(|r| r.as_str())
                        .map(str::to_ascii_uppercase)
                })
            else {
                continue;
            };
            match rel.as_str() {
                "DERIVED_FROM" => {
                    depends_on.insert(target.clone());
                }
                "GENERATED_BY" => {
                    depends_on.insert(target.clone());
                    if generating_activity.is_none() {
                        generating_activity = Some(target.clone());
                    }
                }
                _ => {}
            }
        }
    }

    tm.register(derived_id, depends_on, generating_activity);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::EdgeKind;

    // --- dependency-directed staleness ------------------------------------------

    // claim1 derives from fact_a + fact_b. Deleting fact_a must stale claim1
    // (dependency-directed: found via the dependency edge, not a full rescan).
    #[test]
    fn deleting_a_base_fact_stales_its_dependent_claim() {
        let mut tm = TruthMaintenance::new();
        tm.register("claim1", ["fact_a", "fact_b"], None);
        assert_eq!(tm.status_of("claim1"), Some(MaterializationStatus::Fresh));

        let changed = tm.on_change(&ChangeEvent::Deleted("fact_a".to_string()));
        assert!(changed.contains("claim1"));
        assert_eq!(tm.status_of("claim1"), Some(MaterializationStatus::Stale));
        // fact_a was never itself tracked, so the event touches nothing else.
        assert_eq!(changed, ["claim1".to_string()].into_iter().collect());
    }

    // An `Updated` input stales its dependents (and itself, if tracked) — never
    // retracts either, since the input still exists.
    #[test]
    fn updated_input_stales_but_never_retracts() {
        let mut tm = TruthMaintenance::new();
        tm.register("fact_a", Vec::<String>::new(), None);
        tm.register("claim1", ["fact_a"], None);

        let changed = tm.on_change(&ChangeEvent::Updated("fact_a".to_string()));
        assert_eq!(
            changed,
            ["fact_a".to_string(), "claim1".to_string()]
                .into_iter()
                .collect()
        );
        assert_eq!(tm.status_of("fact_a"), Some(MaterializationStatus::Stale));
        assert_eq!(tm.status_of("claim1"), Some(MaterializationStatus::Stale));
    }

    // Deleting/policy-changing a TRACKED id retracts it outright, not merely stales.
    #[test]
    fn deleting_a_tracked_id_retracts_it_outright() {
        let mut tm = TruthMaintenance::new();
        tm.register("claim1", ["fact_a"], None);
        tm.on_change(&ChangeEvent::Deleted("claim1".to_string()));
        assert_eq!(
            tm.status_of("claim1"),
            Some(MaterializationStatus::Retracted)
        );

        let mut tm2 = TruthMaintenance::new();
        tm2.register("claim2", ["fact_x"], None);
        tm2.on_change(&ChangeEvent::PolicyChanged("claim2".to_string()));
        assert_eq!(
            tm2.status_of("claim2"),
            Some(MaterializationStatus::Retracted)
        );
    }

    // Retiring a model version stales every materialization it generated even
    // though none of THEIR graph inputs changed, plus everything built on top.
    #[test]
    fn model_retirement_stales_generated_materializations_and_downstream() {
        let mut tm = TruthMaintenance::new();
        tm.register("classification1", ["doc1"], Some("model-v1".to_string()));
        tm.register("summary1", ["classification1"], None);
        tm.register("unrelated", ["doc2"], Some("model-v2".to_string()));

        let changed = tm.on_change(&ChangeEvent::ModelRetired("model-v1".to_string()));
        assert!(changed.contains("classification1"));
        assert!(changed.contains("summary1"));
        assert!(!changed.contains("unrelated"));
        assert_eq!(
            tm.status_of("classification1"),
            Some(MaterializationStatus::Stale)
        );
        assert_eq!(tm.status_of("summary1"), Some(MaterializationStatus::Stale));
        assert_eq!(
            tm.status_of("unrelated"),
            Some(MaterializationStatus::Fresh)
        );
    }

    // Same shape for an ontology-version bump — a distinct trigger, same effect.
    #[test]
    fn ontology_evolution_stales_generated_materializations() {
        let mut tm = TruthMaintenance::new();
        tm.register(
            "edge1",
            ["node_a", "node_b"],
            Some("ontology-v3".to_string()),
        );
        let changed = tm.on_change(&ChangeEvent::OntologyEvolved("ontology-v3".to_string()));
        assert!(changed.contains("edge1"));
        assert_eq!(tm.status_of("edge1"), Some(MaterializationStatus::Stale));
    }

    // --- "what depends on X" -----------------------------------------------------

    #[test]
    fn dependents_of_is_transitive_and_deduplicated() {
        let mut tm = TruthMaintenance::new();
        tm.register("claim1", ["fact_a"], None);
        tm.register("summary1", ["claim1"], None);
        tm.register("summary2", ["claim1", "summary1"], None); // two paths to summary1
        tm.register("unrelated", ["fact_z"], None);

        let deps = tm.dependents_of("fact_a");
        let expected: BTreeSet<String> = ["claim1", "summary1", "summary2"]
            .into_iter()
            .map(String::from)
            .collect();
        assert_eq!(deps, expected);
    }

    // --- recompute ----------------------------------------------------------------

    // Stale -> recompute -> Fresh, against a (possibly different) dependency set;
    // a failing recompute retracts rather than resting at Stale forever.
    #[test]
    fn recompute_restores_fresh_state_or_retracts_on_failure() {
        let mut tm = TruthMaintenance::new();
        tm.register("claim1", ["fact_a"], None);
        tm.on_change(&ChangeEvent::Deleted("fact_a".to_string()));
        assert_eq!(tm.status_of("claim1"), Some(MaterializationStatus::Stale));

        let result: Result<i32, &str> = tm.recompute("claim1", || {
            Ok((42, ["fact_b".to_string()].into_iter().collect()))
        });
        assert_eq!(result, Ok(42));
        assert_eq!(tm.status_of("claim1"), Some(MaterializationStatus::Fresh));
        assert_eq!(
            tm.get("claim1").unwrap().depends_on,
            ["fact_b".to_string()].into_iter().collect::<BTreeSet<_>>()
        );

        tm.on_change(&ChangeEvent::Deleted("fact_b".to_string()));
        assert_eq!(tm.status_of("claim1"), Some(MaterializationStatus::Stale));
        let fail: Result<i32, &str> = tm.recompute("claim1", || Err("no valid recompute"));
        assert_eq!(fail, Err("no valid recompute"));
        assert_eq!(
            tm.status_of("claim1"),
            Some(MaterializationStatus::Retracted)
        );
    }

    // --- TMS retraction wiring + paraconsistency ----------------------------------

    // ev -> x -> c (both Attacks, mirrors tms::tests::retract_cascades_through_justification),
    // PLUS an unrelated symmetric contradiction p <-> not_p with no other evidence
    // either way (mirrors tms::tests::paraconsistency_no_explosion).
    fn mixed_bg() -> BeliefGraph {
        BeliefGraph::from_parts(
            [
                ("ev", 0.9),
                ("x", 0.5),
                ("c", 0.5),
                ("p", 0.6),
                ("not_p", 0.6),
            ],
            [
                ("ev", "x", EdgeKind::Attacks),
                ("x", "c", EdgeKind::Attacks),
                ("p", "not_p", EdgeKind::Contradicts),
                ("not_p", "p", EdgeKind::Contradicts),
            ],
        )
    }

    // Withdrawing `ev` cascades through the TMS to retract `c`; that retraction
    // propagates one level up to any materialization tracking `c`. The unrelated
    // contradiction (p / not_p) is untouched — no explosion, and both remain
    // independently trackable, proving retraction only follows real dependency
    // edges rather than "everything connected to a contradiction".
    #[test]
    fn contradicting_materializations_coexist_without_exploding() {
        let bg = mixed_bg();
        let mut tm = TruthMaintenance::new();
        tm.register("c", ["x"], None);
        tm.register("summary_of_c", ["c"], None);
        tm.register("summary_p", ["p"], None);
        tm.register("summary_not_p", ["not_p"], None);

        let (retraction, propagated) = tm.retract_and_propagate(&bg, "ev");

        assert_eq!(retraction.retracted, vec!["c".to_string()]);
        assert_eq!(retraction.reinstated, vec!["x".to_string()]);
        assert!(propagated.contains("c"));
        assert!(propagated.contains("summary_of_c"));
        assert_eq!(tm.status_of("c"), Some(MaterializationStatus::Retracted));
        assert_eq!(
            tm.status_of("summary_of_c"),
            Some(MaterializationStatus::Stale)
        );

        // The unrelated contradiction is completely untouched by this retraction.
        assert_eq!(
            tm.status_of("summary_p"),
            Some(MaterializationStatus::Fresh)
        );
        assert_eq!(
            tm.status_of("summary_not_p"),
            Some(MaterializationStatus::Fresh)
        );

        // Paraconsistency itself still holds over the raw BeliefGraph: neither p
        // nor not_p is grounded-accepted, and no admissible set contains both.
        let grounded = tms::grounded_extension(&bg);
        assert!(!grounded.contains("p") && !grounded.contains("not_p"));
        for ext in tms::preferred_extensions(&bg) {
            assert!(!(ext.contains("p") && ext.contains("not_p")));
        }
    }

    // --- register_from_provenance (L45: EPI-P3-1 wiring) --------------------------

    fn node_blob(props: serde_json::Value) -> Vec<u8> {
        rmp_serde::to_vec_named(&props).unwrap()
    }

    fn edge_blob(relationship_type: &str) -> Vec<u8> {
        rmp_serde::to_vec_named(&serde_json::json!({ "relationship_type": relationship_type }))
            .unwrap()
    }

    // A base node + a derived node linked `derived --DERIVED_FROM--> base`:
    // `register_from_provenance` must read that edge into `depends_on`, and a
    // subsequent base change must invalidate (stale) the derived materialization.
    #[test]
    fn register_from_provenance_reads_derived_from_edge_and_invalidates_on_base_change() {
        let core = eg_core::graph::GraphCore::new();
        core.add_node(
            "base".to_string(),
            node_blob(serde_json::json!({ "type": "Claim" })),
        );
        core.add_node(
            "derived".to_string(),
            node_blob(serde_json::json!({ "type": "Claim" })),
        );
        core.add_edge(
            "derived".to_string(),
            "base".to_string(),
            edge_blob("DERIVED_FROM"),
        )
        .unwrap();

        let view = core.analysis_snapshot();
        let mut tm = TruthMaintenance::new();
        register_from_provenance(&mut tm, &view, "derived");

        assert_eq!(tm.status_of("derived"), Some(MaterializationStatus::Fresh));
        assert_eq!(
            tm.get("derived").unwrap().depends_on,
            ["base".to_string()].into_iter().collect::<BTreeSet<_>>()
        );

        // Simulate the base fact changing: `derived` must go Stale — proving the
        // registered dependency actually drives truth maintenance, not just recorded
        // for show.
        let changed = tm.on_change(&ChangeEvent::Updated("base".to_string()));
        assert!(changed.contains("derived"));
        assert_eq!(tm.status_of("derived"), Some(MaterializationStatus::Stale));
    }

    // A claim carrying the REAL `invalidation_deps` property (EG-P3-1's universal
    // writeback-lineage tuple, as `eg-jobs::claim::commit_result_claim` and
    // `mining.rs::materialize_claim` actually write it) plus a real
    // `claim --GENERATED_BY--> activity` edge: both channels must be folded into
    // `depends_on`, and the activity id must become `generating_activity` so a
    // `ChangeEvent::ModelRetired` on it invalidates the claim too.
    #[test]
    fn register_from_provenance_reads_invalidation_deps_and_generated_by_edge() {
        let core = eg_core::graph::GraphCore::new();
        core.add_node(
            "jobclaim:x".to_string(),
            node_blob(serde_json::json!({
                "type": "Claim",
                "invalidation_deps": ["snapshot:g1@5", "jobevidence:x:job-1"],
            })),
        );
        core.add_node(
            "jobactivity:x".to_string(),
            node_blob(serde_json::json!({ "type": "Activity" })),
        );
        core.add_edge(
            "jobclaim:x".to_string(),
            "jobactivity:x".to_string(),
            edge_blob("GENERATED_BY"),
        )
        .unwrap();

        let view = core.analysis_snapshot();
        let mut tm = TruthMaintenance::new();
        register_from_provenance(&mut tm, &view, "jobclaim:x");

        let m = tm.get("jobclaim:x").unwrap();
        assert_eq!(
            m.depends_on,
            [
                "snapshot:g1@5".to_string(),
                "jobevidence:x:job-1".to_string(),
                "jobactivity:x".to_string(),
            ]
            .into_iter()
            .collect::<BTreeSet<_>>()
        );
        assert_eq!(m.generating_activity, Some("jobactivity:x".to_string()));

        // Retiring the generating activity invalidates the claim even though none of
        // its graph inputs changed.
        let changed = tm.on_change(&ChangeEvent::ModelRetired("jobactivity:x".to_string()));
        assert!(changed.contains("jobclaim:x"));
        assert_eq!(
            tm.status_of("jobclaim:x"),
            Some(MaterializationStatus::Stale)
        );
    }

    // A node with neither an `invalidation_deps` property nor a recognized outgoing
    // edge registers with an EMPTY `depends_on` — a legitimate answer, not an error.
    #[test]
    fn register_from_provenance_with_no_provenance_registers_empty_deps() {
        let core = eg_core::graph::GraphCore::new();
        core.add_node(
            "solo".to_string(),
            node_blob(serde_json::json!({ "type": "Claim" })),
        );
        let view = core.analysis_snapshot();
        let mut tm = TruthMaintenance::new();
        register_from_provenance(&mut tm, &view, "solo");

        assert_eq!(tm.status_of("solo"), Some(MaterializationStatus::Fresh));
        assert!(tm.get("solo").unwrap().depends_on.is_empty());
        assert_eq!(tm.get("solo").unwrap().generating_activity, None);
    }
}
