//! The OWL membership closure behind a `REASON` stage (CONCEPT:MEDULLA-KG.ontology.concept-12):
//! computed once per stage and read two ways — the confidence-scored members the stage
//! emits, and (UQL `WITH PROOF`, EH-448) the proof that one row is a member.

use std::collections::HashMap;

use medulla_core::graph::GraphView;
use medulla_rdf::owl::{
    asserted_types_with_confidence_from_view, explain_instance, instances_of_weighted,
    Classification, Reasoner,
};
use medulla_types::protocol::ProofNodeWire;

use crate::rowset::RowSet;

/// The classification, the asserted instance types (with their possibly decayed
/// confidences) and the canonical target class of one `REASON` stage.
pub(crate) struct ReasonMembership {
    cls: Classification,
    asserted: HashMap<String, Vec<(String, f64)>>,
    target: String,
}

impl ReasonMembership {
    /// Classify the axioms — the `ontology` Turtle, or when it is empty the axioms
    /// already present in the graph view's blobs (they round-trip as RDF) — with
    /// confidence propagation (CONCEPT:MEDULLA-KG.ontology.concept-13), and read the graph's
    /// asserted instance types.
    ///
    /// A node with a BARE string `type` (e.g. `{"type":"Sensor"}`) is resolved in the
    /// target class's namespace (CONCEPT:MEDULLA-KG.ontology.string-type-iri-class), so it
    /// becomes a member of `REASON <base/Device>` through `<base/Sensor> ⊑ <base/Device>`;
    /// the target must therefore carry an absolute class namespace. A bound `(now,
    /// half_life)` decay context Ebbinghaus-decays each asserted fact's confidence by
    /// the node's age (CONCEPT:MEDULLA-KG.query.reason-decay-in-plan); without one the stage
    /// is decay-neutral.
    pub(crate) fn of(
        view: &GraphView,
        decay: Option<(u64, f64)>,
        target_class: &str,
        ontology: &str,
    ) -> Result<Self, String> {
        let triples = if ontology.trim().is_empty() {
            medulla_rdf::owl::tbox_triples_from_view(view)
        } else {
            medulla_rdf::mapping::parse_turtle(ontology)?
        };
        let cls = Reasoner::from_triples(&triples).classify_weighted();
        let target = normalize_class(target_class);
        let class_base = medulla_rdf::owl::class_namespace(&target).ok_or_else(|| {
            "Reason requires an absolute target class with a current class namespace".to_string()
        })?;
        let (now, half_life) = decay.unwrap_or((0, 0.0));
        let asserted = asserted_types_with_confidence_from_view(view, now, half_life, &class_base)?;
        Ok(Self {
            cls,
            asserted,
            target,
        })
    }

    /// Every (possibly only-inferred) member, scored by its membership confidence.
    pub(crate) fn members(&self) -> RowSet {
        let scored = instances_of_weighted(&self.cls, &self.asserted, &self.target, 0.0)
            .into_iter()
            .map(|(id, conf)| (id, conf as f32));
        RowSet::from_scored(scored)
    }

    /// The proof that `id` is a member: an asserted type fact plus the TBox subsumption
    /// chain to the target class; `None` when it is not (provably) one.
    pub(crate) fn explain(&self, id: &str) -> Option<ProofNodeWire> {
        explain_instance(&self.cls, &self.asserted, id, &self.target)
            .map(medulla_rdf::owl_wire::proof_node_to_wire)
    }
}

/// Canonicalize a class id to the ontology's `<iri>` form (accept a bare IRI too).
fn normalize_class(c: &str) -> String {
    if c.starts_with('<') {
        c.to_string()
    } else if c.starts_with("http") {
        format!("<{c}>")
    } else {
        c.to_string()
    }
}
