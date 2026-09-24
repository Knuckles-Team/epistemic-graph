//! Which node types bind as which entity class, decided by the ontology
//! (EH-408 / EH-410).
//!
//! A node takes part in binding when its class is subsumed by one of the
//! target classes (`kg:Server`, `infrastructure:Service`, `infrastructure:Host`,
//! `infrastructure:Workload`, `infrastructure:Pod`, `kg:Agent`) under the
//! request graph's own
//! composed schema -- the immutable core corpus plus any attached sources --
//! classified by the engine's EL/RL reasoner. So a fleet `Deployment` binds as
//! a Workload and a `K8sService` / `SwarmService` as a Service because the
//! ontology says `Deployment ⊑ Workload` and `K8sService ⊑ Service`, not because
//! a list names them. A `Pod` binds as a Pod -- never as a Workload, which
//! schedules it; its behaviour reaches the Workload through the declared
//! `scheduledBy` relation (see [`super::declarations`]).
//!
//! A node's `type` may be a class IRI or a bare local name. A bare name is used
//! only when every class carrying that local name falls under the same targets,
//! so an ambiguous name binds nothing rather than being guessed.
//!
//! Classified hierarchies are cached, keyed by the graph's ontology digest, in
//! a bounded most-recently-used cache: a derivation reclassifies only when the
//! graph's ontology changed.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use eg_core::graph::GraphSchemaSources;
use eg_rdf::owl::{Classification, Reasoner};
use eg_stream::telemetry::EntityClass;
use eg_types::contract::Digest256;
use parking_lot::Mutex;

use crate::server::graph_schema::compose::validate_and_compose;

/// The classes telemetry binds to.
const TARGET_CLASSES: [EntityClass; 6] = [
    EntityClass::Server,
    EntityClass::Service,
    EntityClass::Host,
    EntityClass::Deployment,
    EntityClass::Agent,
    EntityClass::Pod,
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

/// Domain of [`ontology_digest`].
const ONTOLOGY_DIGEST_DOMAIN: &[u8] = b"eg/telemetry-bindable-ontology/v1";

/// Distinct ontology identities whose classification is kept.
const CACHE_CAPACITY: usize = 16;

/// The identity a classification depends on: each source's id and ontology
/// digest, in composition order. Shapes and provenance do not change the class
/// hierarchy, so they are not part of it. `validate_and_compose` checks every
/// ontology document against its digest before a classification is built.
pub(super) fn ontology_digest(sources: &GraphSchemaSources) -> Result<Digest256, String> {
    let ontologies: Vec<(&str, Digest256)> = sources
        .all()
        .filter_map(|(id, source)| Some((id, source.ontology_sha256?)))
        .collect();
    let fields: Vec<&[u8]> = ontologies
        .iter()
        .flat_map(|(id, digest)| [id.as_bytes(), digest.as_bytes().as_slice()])
        .collect();
    Digest256::framed(ONTOLOGY_DIGEST_DOMAIN, &fields)
}

/// A bounded, most-recently-used cache of classified hierarchies keyed by
/// [`ontology_digest`]: an unchanged schema is never classified twice while its
/// entry is resident. Failures are not cached.
#[derive(Default)]
pub(super) struct ClassificationCache {
    entries: Mutex<Vec<(Digest256, Arc<BindableTypes>)>>,
    classifications: AtomicU64,
}

impl ClassificationCache {
    /// The cached hierarchy for `key`, or the result of `classify` (cached on
    /// success). The oldest entry is evicted past [`CACHE_CAPACITY`].
    pub(super) fn get_or_classify(
        &self,
        key: Digest256,
        classify: impl FnOnce() -> Result<Arc<BindableTypes>, String>,
    ) -> Result<Arc<BindableTypes>, String> {
        if let Some(hit) = self.take(key) {
            return Ok(hit);
        }
        self.classifications.fetch_add(1, Ordering::Relaxed);
        let fresh = classify()?;
        self.put(key, fresh.clone());
        Ok(fresh)
    }

    /// How many classifications this cache has run (its misses).
    pub(super) fn classifications(&self) -> u64 {
        self.classifications.load(Ordering::Relaxed)
    }

    fn take(&self, key: Digest256) -> Option<Arc<BindableTypes>> {
        let mut entries = self.entries.lock();
        let position = entries
            .iter()
            .position(|(entry_key, _)| *entry_key == key)?;
        let entry = entries.remove(position);
        let types = entry.1.clone();
        entries.insert(0, entry);
        Some(types)
    }

    fn put(&self, key: Digest256, types: Arc<BindableTypes>) {
        let mut entries = self.entries.lock();
        entries.retain(|(entry_key, _)| *entry_key != key);
        entries.insert(0, (key, types));
        entries.truncate(CACHE_CAPACITY);
    }
}

/// The bindable node types of a graph whose schema authority is `sources`.
pub(super) fn bindable_types(sources: &GraphSchemaSources) -> Result<Arc<BindableTypes>, String> {
    static CACHE: OnceLock<ClassificationCache> = OnceLock::new();
    let key = ontology_digest(sources)?;
    CACHE
        .get_or_init(ClassificationCache::default)
        .get_or_classify(key, || classify(sources))
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
