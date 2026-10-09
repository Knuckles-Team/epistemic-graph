//! EH-382: the compiled GraphSchema cache is content-keyed, invalidated by every
//! identity change, shared across graphs, bounded and single-flight.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Duration;

use super::super::compose::{compose_validated_sources, ComposedSchema};
use super::CompiledCache;
use crate::graph::{GraphCore, GraphSchemaSource, GraphSchemaSources, SchemaSourceOrigin};
use crate::test_rendezvous::{join_bounded, meet};
use eg_types::contract::Digest256;

const ONTOLOGY_A: &str = "@prefix ex: <http://example/eh382/> . \
    @prefix owl: <http://www.w3.org/2002/07/owl#> . ex:A a owl:Class .";
const ONTOLOGY_B: &str = "@prefix ex: <http://example/eh382/> . \
    @prefix owl: <http://www.w3.org/2002/07/owl#> . ex:B a owl:Class .";
const ONTOLOGY_B_REPLACED: &str = "@prefix ex: <http://example/eh382/> . \
    @prefix owl: <http://www.w3.org/2002/07/owl#> . ex:BPrime a owl:Class .";

fn admin(name: &str, ontology: &str) -> GraphSchemaSource {
    GraphSchemaSource::new(
        SchemaSourceOrigin::Admin {
            name: name.to_string(),
        },
        None,
        Some(Arc::from(ontology)),
        0,
    )
    .unwrap()
}

fn with_sources(sources: &[(&str, &str)]) -> GraphSchemaSources {
    let mut composed = GraphSchemaSources::default();
    for (name, ontology) in sources {
        composed
            .attach_dynamic(format!("admin:{name}"), admin(name, ontology))
            .unwrap();
    }
    composed
}

/// `validate_and_compose`'s dynamic path, through a test-local cache so no other
/// test's compositions can evict an entry under measurement.
fn compose_in(
    cache: &CompiledCache<ComposedSchema>,
    sources: &GraphSchemaSources,
) -> Arc<ComposedSchema> {
    sources.validate().unwrap();
    cache
        .get_or_compile(sources.composed_digest(), || {
            compose_validated_sources(sources)
        })
        .unwrap()
}

fn declares(composed: &ComposedSchema, class: &str) -> bool {
    composed
        .ontology
        .iter()
        .any(|triple| triple.subject.to_string() == format!("<http://example/eh382/{class}>"))
}

fn key(byte: u8) -> Digest256 {
    Digest256::sha256(&[byte])
}

// spec: EG-UNIFIED-DATA-PLANE-R035
#[test]
fn a_hit_returns_the_identical_compiled_schema() {
    let cache = CompiledCache::new(4);
    let sources = with_sources(&[("a", ONTOLOGY_A)]);
    let first = compose_in(&cache, &sources);
    let second = compose_in(&cache, &sources.clone());
    assert!(Arc::ptr_eq(&first, &second));
    assert_eq!(cache.compile_count(), 1);
}

// spec: EG-UNIFIED-DATA-PLANE-R035
#[test]
fn two_graphs_composing_the_same_schema_share_one_entry() {
    let cache = CompiledCache::new(4);
    let left = GraphCore::new();
    let right = GraphCore::new();
    left.install_schema_sources(Arc::new(with_sources(&[("a", ONTOLOGY_A)])));
    right.install_schema_sources(Arc::new(with_sources(&[("a", ONTOLOGY_A)])));
    let from_left = compose_in(&cache, &left.schema_sources());
    let from_right = compose_in(&cache, &right.schema_sources());
    assert!(Arc::ptr_eq(&from_left, &from_right));
    assert_eq!((cache.len(), cache.compile_count()), (1, 1));
}

// spec: EG-UNIFIED-DATA-PLANE-R035
#[test]
fn attach_detach_and_replace_each_change_the_identity() {
    let cache = CompiledCache::new(4);
    let only_a = with_sources(&[("a", ONTOLOGY_A)]);
    let a_entry = compose_in(&cache, &only_a);

    let attached = with_sources(&[("a", ONTOLOGY_A), ("b", ONTOLOGY_B)]);
    let attached_entry = compose_in(&cache, &attached);
    assert!(!Arc::ptr_eq(&a_entry, &attached_entry));
    assert!(declares(&attached_entry, "B"));
    assert_eq!(cache.compile_count(), 2);

    let replaced = with_sources(&[("a", ONTOLOGY_A), ("b", ONTOLOGY_B_REPLACED)]);
    let replaced_entry = compose_in(&cache, &replaced);
    assert!(!Arc::ptr_eq(&attached_entry, &replaced_entry));
    assert!(declares(&replaced_entry, "BPrime") && !declares(&replaced_entry, "B"));
    assert_eq!(cache.compile_count(), 3);

    let mut detached = replaced.clone();
    assert!(detached.detach_dynamic("admin:b"));
    let detached_entry = compose_in(&cache, &detached);
    // Detaching returns to the A-only identity: its own compiled entry, never the
    // composition that still carried B.
    assert!(Arc::ptr_eq(&a_entry, &detached_entry));
    assert!(!declares(&detached_entry, "BPrime"));
    assert_eq!(cache.compile_count(), 3);
}

#[test]
fn a_changed_core_catalog_changes_the_identity_and_is_refused() {
    let sources = with_sources(&[("a", ONTOLOGY_A)]);
    let mut upgraded = sources.clone();
    let (_, core) = upgraded
        .core
        .iter_mut()
        .find(|(_, source)| source.ontology_ttl.is_some())
        .unwrap();
    let document = format!("{}\n", core.ontology_ttl.as_deref().unwrap());
    core.ontology_sha256 = Some(Digest256::sha256(document.as_bytes()));
    core.ontology_ttl = Some(Arc::from(document));
    assert_ne!(sources.composed_digest(), upgraded.composed_digest());
    // A catalog that is not this binary's is refused before the cache is consulted.
    assert!(super::super::compose::validate_and_compose(&upgraded).is_err());
}

#[test]
fn concurrent_misses_on_one_identity_compile_once() {
    const WRITERS: usize = 8;
    let cache = Arc::new(CompiledCache::new(4));
    let compiled = Arc::new(AtomicUsize::new(0));
    let start = Arc::new(Barrier::new(WRITERS));
    let handles: Vec<_> = (0..WRITERS)
        .map(|_| {
            let (cache, compiled, start) = (cache.clone(), compiled.clone(), start.clone());
            std::thread::spawn(move || {
                meet(&start, "concurrent schema-cache writers");
                cache
                    .get_or_compile(key(1), || {
                        compiled.fetch_add(1, Ordering::SeqCst);
                        std::thread::sleep(Duration::from_millis(100));
                        Ok(7_u64)
                    })
                    .unwrap()
            })
        })
        .collect();
    let results: Vec<Arc<u64>> = handles
        .into_iter()
        .map(|handle| join_bounded(handle, "a concurrent schema-cache writer"))
        .collect();
    assert_eq!(compiled.load(Ordering::SeqCst), 1);
    assert!(results.iter().all(|value| Arc::ptr_eq(value, &results[0])));
}

#[test]
fn a_failed_compile_is_not_cached() {
    let cache = CompiledCache::<u64>::new(4);
    assert!(cache
        .get_or_compile(key(1), || Err("no".to_string()))
        .is_err());
    assert_eq!(cache.len(), 0);
    assert_eq!(*cache.get_or_compile(key(1), || Ok(3)).unwrap(), 3);
    assert_eq!(cache.compile_count(), 2);
}

#[test]
fn the_cache_is_bounded_and_evicts_the_least_recently_used() {
    let cache = CompiledCache::<u8>::new(2);
    for byte in [1, 2, 1, 3] {
        cache.get_or_compile(key(byte), || Ok(byte)).unwrap();
    }
    assert_eq!((cache.len(), cache.compile_count()), (2, 3));
    cache.get_or_compile(key(1), || Ok(1)).unwrap();
    assert_eq!(cache.compile_count(), 3, "the recently used entry survived");
    cache.get_or_compile(key(2), || Ok(2)).unwrap();
    assert_eq!(
        cache.compile_count(),
        4,
        "the least recently used entry was evicted"
    );
}
