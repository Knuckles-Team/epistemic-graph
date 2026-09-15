//! Private graph-ref cache tests for the analytics job handler.

use super::*;

#[cfg(test)]
mod resolve_core_ref_tests {
    use super::*;
    use crate::protocol::GraphType;
    use crate::registry::GraphRegistry;

    /// A unique-enough name per call so parallel `cargo test` threads sharing
    /// the ONE process-wide `opaque_graph_ref_index()` singleton never collide
    /// on the same digest.
    fn unique_name(label: &str) -> String {
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        format!("w1b-resolve-core-ref-test-{label}-{n}-{nanos}")
    }

    #[test]
    fn resolves_every_resident_graph_by_its_opaque_ref_after_a_cold_scan() {
        let mut registry = GraphRegistry::new();
        let a = unique_name("a");
        let b = unique_name("b");
        registry.create_graph(&a, GraphType::Agent, None).unwrap();
        registry.create_graph(&b, GraphType::Team, None).unwrap();

        let ref_a = native_opaque_ref("graph", &a);
        let ref_b = native_opaque_ref("graph", &b);

        let (name, kind, _core) = resolve_opaque_graph_ref(&registry, &ref_a).unwrap();
        assert_eq!(name, a);
        assert_eq!(kind, GraphType::Agent);

        let (name, kind, _core) = resolve_opaque_graph_ref(&registry, &ref_b).unwrap();
        assert_eq!(name, b);
        assert_eq!(kind, GraphType::Team);

        assert!(resolve_opaque_graph_ref(&registry, "eg:graph:not-a-real-digest").is_none());
    }

    #[test]
    fn a_warm_cache_hit_still_returns_the_live_registry_entry() {
        let mut registry = GraphRegistry::new();
        let name = unique_name("warm");
        registry
            .create_graph(&name, GraphType::Agent, None)
            .unwrap();
        let graph_ref = native_opaque_ref("graph", &name);

        // Cold lookup: falls back to the full scan and populates the index.
        let first = resolve_opaque_graph_ref(&registry, &graph_ref).unwrap();
        assert_eq!(first.0, name);

        // Warm lookup: same digest, same registry -- must take the cache-hit
        // path and still resolve to the live entry.
        let second = resolve_opaque_graph_ref(&registry, &graph_ref).unwrap();
        assert_eq!(second.0, name);
        assert_eq!(second.1, GraphType::Agent);
    }

    #[test]
    fn a_poisoned_cache_entry_can_only_waste_a_rescan_never_resolve_the_wrong_graph() {
        let mut registry = GraphRegistry::new();
        let real_name = unique_name("real");
        registry
            .create_graph(&real_name, GraphType::Agent, None)
            .unwrap();
        let real_ref = native_opaque_ref("graph", &real_name);

        // Directly poison the shared process-wide index with a WRONG name for
        // this exact digest, simulating a stale or corrupted cache entry --
        // e.g. left over from a deleted graph whose name got reused for a
        // digest collision class this test forces by hand.
        opaque_graph_ref_index()
            .write_recovering("opaque graph-ref cache")
            .insert(real_ref.clone(), "not-the-real-graph-name".to_string());

        // The live registry has no graph by that poisoned name, so the digest
        // re-verification must reject the cache hit and fall back to a fresh
        // scan -- resolving to the REAL graph, never the poisoned name and
        // never `None`.
        let resolved = resolve_opaque_graph_ref(&registry, &real_ref);
        assert_eq!(resolved.map(|(name, _, _)| name), Some(real_name));
    }

    #[test]
    fn a_deleted_graphs_stale_cache_entry_resolves_to_none_not_a_wrong_graph() {
        let mut registry = GraphRegistry::new();
        let name = unique_name("deleted");
        registry
            .create_graph(&name, GraphType::Agent, None)
            .unwrap();
        let graph_ref = native_opaque_ref("graph", &name);

        // Populate the cache while the graph is still live.
        assert!(resolve_opaque_graph_ref(&registry, &graph_ref).is_some());

        registry.delete_graph(&name).unwrap();

        // The index may still hold `graph_ref -> name`, but the live registry
        // no longer backs it -- must resolve to `None`, never a stale core.
        assert!(resolve_opaque_graph_ref(&registry, &graph_ref).is_none());
    }
}
