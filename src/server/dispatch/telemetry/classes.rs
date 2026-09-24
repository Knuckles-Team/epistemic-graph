//! Which node types bind as which entity class, decided by the ontology
//! (EH-408 / EH-410).
//!
//! A node takes part in binding when its class is subsumed by one of the
//! target classes (`kg:Server`, `infrastructure:Service`, `infrastructure:Host`,
//! `infrastructure:Workload`, `kg:Agent`) under the request graph's own
//! composed schema -- the immutable core corpus plus any attached sources --
//! classified by the engine's EL/RL reasoner. So a fleet `Deployment` binds as
//! a Workload and a `K8sService` / `SwarmService` as a Service because the
//! ontology says `Deployment ⊑ Workload` and `K8sService ⊑ Service`, not because
//! a list names them; and a `Pod` binds as nothing, because a Pod is not a
//! Workload.
//!
//! A node's `type` may be a class IRI or a bare local name. A bare name is used
//! only when every class carrying that local name falls under the same targets,
//! so an ambiguous name binds nothing rather than being guessed.
//!
//! The core-only classification is computed once per process; a graph with
//! attached sources is classified per derivation.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, OnceLock};

use eg_core::graph::GraphSchemaSources;
use eg_rdf::owl::{Classification, Reasoner};
use eg_stream::telemetry::EntityClass;

use crate::server::graph_schema::compose::validate_and_compose;

/// The classes telemetry binds to.
const TARGET_CLASSES: [EntityClass; 5] = [
    EntityClass::Server,
    EntityClass::Service,
    EntityClass::Host,
    EntityClass::Deployment,
    EntityClass::Agent,
];

/// Node `type` value → the target classes a node of that type is subsumed by.
#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct BindableTypes {
    by_type: BTreeMap<String, BTreeSet<EntityClass>>,
}

impl BindableTypes {
    /// Every named class of `classification` subsumed by at least one target.
    pub(super) fn from_classification(classification: &Classification) -> Self {
        let mut by_type: BTreeMap<String, BTreeSet<EntityClass>> = BTreeMap::new();
        let mut by_local_name: BTreeMap<String, Vec<BTreeSet<EntityClass>>> = BTreeMap::new();
        for (class, subsumers) in &classification.subsumers {
            if classification.unsatisfiable.contains(class) {
                continue;
            }
            let Some(iri) = class.strip_prefix('<').and_then(|c| c.strip_suffix('>')) else {
                continue;
            };
            let targets = targets_of(subsumers);
            by_local_name
                .entry(local_name(iri).to_string())
                .or_default()
                .push(targets.clone());
            if !targets.is_empty() {
                by_type.insert(iri.to_string(), targets);
            }
        }
        for (name, candidates) in by_local_name {
            if let Some(targets) = unambiguous(&candidates) {
                by_type.entry(name).or_insert_with(|| targets.clone());
            }
        }
        Self { by_type }
    }

    /// The target classes a node typed `node_type` binds as, if any.
    pub(super) fn targets(&self, node_type: &str) -> Option<&BTreeSet<EntityClass>> {
        self.by_type.get(node_type)
    }

    /// Every bindable type value, in order.
    pub(super) fn types(&self) -> impl Iterator<Item = &str> {
        self.by_type.keys().map(String::as_str)
    }
}

fn targets_of(subsumers: &BTreeSet<String>) -> BTreeSet<EntityClass> {
    TARGET_CLASSES
        .iter()
        .copied()
        .filter(|target| subsumers.contains(&format!("<{}>", target.class_iri())))
        .collect()
}

/// The shared targets of every class carrying one local name, when there are
/// any and they agree.
fn unambiguous(candidates: &[BTreeSet<EntityClass>]) -> Option<&BTreeSet<EntityClass>> {
    let first = candidates.first()?;
    (!first.is_empty() && candidates.iter().all(|targets| targets == first)).then_some(first)
}

fn local_name(iri: &str) -> &str {
    iri.rsplit(['#', '/']).next().unwrap_or(iri)
}

/// The bindable node types of a graph whose schema authority is `sources`.
pub(super) fn bindable_types(sources: &GraphSchemaSources) -> Result<Arc<BindableTypes>, String> {
    if sources.dynamic.is_empty() {
        static CORE: OnceLock<Result<Arc<BindableTypes>, String>> = OnceLock::new();
        return CORE.get_or_init(|| classify(sources)).clone();
    }
    classify(sources)
}

fn classify(sources: &GraphSchemaSources) -> Result<Arc<BindableTypes>, String> {
    let composed = validate_and_compose(sources)?;
    let classification = Reasoner::from_triples(&composed.ontology).classify();
    if !classification.consistent {
        return Err(
            "SCHEMA_INCONSISTENT: the graph's schema does not classify; no node type can bind"
                .to_string(),
        );
    }
    Ok(Arc::new(BindableTypes::from_classification(
        &classification,
    )))
}
