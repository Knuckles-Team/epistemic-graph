//! Telemetry facts as graph nodes and edges (EH-408 / EH-409).
//!
//! Every derived fact becomes one node typed by its ontology class and linked to
//! the entity it is about and to the facts it was derived from, so "why is this
//! an incident?" is a graph walk back to the raw windows:
//!
//! | node | class | edges |
//! |---|---|---|
//! | observation | `infrastructure:BehaviourObservation` | `OBSERVES` → entity |
//! | anomaly | `infrastructure:HealthAnomaly` | `AFFECTS_ENTITY` → entity, `DERIVED_FROM` → each observation |
//! | incident | `infrastructure:Incident` | `AFFECTS_ENTITY` → each entity, `DERIVED_FROM` → each anomaly |
//! | violation | `infrastructure:ConformanceViolation` | `AFFECTS_ENTITY` → entity, `DERIVED_FROM` → the observation |
//!
//! Node properties are the fact's own fields plus `type` (the class label),
//! `class_iri` and `evidence_class` (`observation` for rollups, `derived` for
//! everything computed from them). Node ids are the facts' ids, which are
//! functions of their inputs, so re-deriving the same telemetry reproduces the
//! same nodes — the projection is idempotent under create-if-absent.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use super::binding::{EntityRef, INFRASTRUCTURE_NAMESPACE};
use super::pipeline::TelemetryFacts;

pub const OBSERVES: &str = "OBSERVES";
pub const AFFECTS_ENTITY: &str = "AFFECTS_ENTITY";
pub const DERIVED_FROM: &str = "DERIVED_FROM";

/// The ontology class of each fact kind (local names in the infrastructure module).
const OBSERVATION_CLASS: &str = "BehaviourObservation";
const ANOMALY_CLASS: &str = "HealthAnomaly";
const INCIDENT_CLASS: &str = "Incident";
const VIOLATION_CLASS: &str = "ConformanceViolation";

/// Whether a fact was measured or computed from measurements.
#[derive(Clone, Copy)]
enum EvidenceClass {
    Observation,
    Derived,
}

impl EvidenceClass {
    fn label(self) -> &'static str {
        match self {
            EvidenceClass::Observation => "observation",
            EvidenceClass::Derived => "derived",
        }
    }
}

/// One fact node.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FactNode {
    pub id: String,
    pub class_iri: String,
    pub properties: Map<String, Value>,
}

/// One fact edge.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FactEdge {
    pub source: String,
    pub target: String,
    pub relationship: String,
}

/// The node/edge form of a [`TelemetryFacts`].
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct FactGraph {
    pub nodes: Vec<FactNode>,
    pub edges: Vec<FactEdge>,
}

impl FactGraph {
    fn node<T: Serialize>(
        &mut self,
        id: &str,
        class: &str,
        evidence: EvidenceClass,
        record: &T,
    ) -> Result<(), serde_json::Error> {
        let mut properties = match serde_json::to_value(record)? {
            Value::Object(fields) => fields,
            other => Map::from_iter([("value".to_string(), other)]),
        };
        let class_iri = format!("{INFRASTRUCTURE_NAMESPACE}{class}");
        properties.insert("type".into(), class.into());
        properties.insert("class_iri".into(), class_iri.clone().into());
        properties.insert("evidence_class".into(), evidence.label().into());
        self.nodes.push(FactNode {
            id: id.to_string(),
            class_iri,
            properties,
        });
        Ok(())
    }

    fn edge(&mut self, source: &str, target: &str, relationship: &str) {
        self.edges.push(FactEdge {
            source: source.to_string(),
            target: target.to_string(),
            relationship: relationship.to_string(),
        });
    }

    fn edges_to<'a>(
        &mut self,
        source: &str,
        targets: impl IntoIterator<Item = &'a str>,
        relationship: &str,
    ) {
        for target in targets {
            self.edge(source, target, relationship);
        }
    }
}

fn entity_ids(entities: &[EntityRef]) -> impl Iterator<Item = &str> {
    entities.iter().map(|entity| entity.id.as_str())
}

fn ids(values: &[String]) -> impl Iterator<Item = &str> {
    values.iter().map(String::as_str)
}

/// Project `facts` onto graph nodes and edges (see the module table).
pub fn fact_graph(facts: &TelemetryFacts) -> Result<FactGraph, serde_json::Error> {
    let mut graph = FactGraph::default();
    for observation in &facts.observations {
        graph.node(
            &observation.id,
            OBSERVATION_CLASS,
            EvidenceClass::Observation,
            observation,
        )?;
        graph.edge(&observation.id, &observation.entity.id, OBSERVES);
    }
    for anomaly in &facts.anomalies {
        graph.node(&anomaly.id, ANOMALY_CLASS, EvidenceClass::Derived, anomaly)?;
        graph.edge(&anomaly.id, &anomaly.entity.id, AFFECTS_ENTITY);
        graph.edges_to(&anomaly.id, ids(&anomaly.evidence), DERIVED_FROM);
    }
    for incident in &facts.incidents {
        graph.node(
            &incident.id,
            INCIDENT_CLASS,
            EvidenceClass::Derived,
            incident,
        )?;
        graph.edges_to(&incident.id, entity_ids(&incident.entities), AFFECTS_ENTITY);
        graph.edges_to(&incident.id, ids(&incident.anomalies), DERIVED_FROM);
    }
    for violation in &facts.violations {
        graph.node(
            &violation.id,
            VIOLATION_CLASS,
            EvidenceClass::Derived,
            violation,
        )?;
        graph.edge(&violation.id, &violation.entity.id, AFFECTS_ENTITY);
        graph.edge(&violation.id, &violation.observation, DERIVED_FROM);
    }
    Ok(graph)
}
