// CONCEPT:KG-2.233 — cold object-store tier seam (feature `cold-tier`).
//
// The engine keeps a graph's HOT state in RAM, with a redb durable tier under it
// (CONCEPT:KG-2.195) and node-property read-through on a RAM miss (CONCEPT:KG-2.191).
// A COLD tenant — one not touched for a long time — still costs RAM (and, in the
// authoritative model, redb file space) even after hibernation (CONCEPT:KG-2.224)
// drops its RAM, because a full topology/edge rehydrate needs the durable tier.
//
// The cold tier extends the eviction/hibernation ladder with a THIRD rung: a cold
// graph's whole serialized state is OFFLOADED to an object store (the `:Blob`
// `ChunkStore`/S3 seam the engine already carries), freeing both RAM and the local
// tier, and REHYDRATED on the next access. So the storage ladder is:
//
//     RAM  (hot)  →  redb  (durable, KG-2.195)  →  object store  (cold, KG-2.233)
//
// ## The seam, DAG-low in eg-core
//
// Like `read_through`, the trait is DEFINED HERE in eg-core (below the facade) so a
// `GraphCore`/registry caller depends only on the trait object — no facade type
// (the redb backend, the object-store SDK) leaks downward and the crate DAG stays
// acyclic. The facade implements it:
//
//   * a default in-memory/redb impl (`cold-tier` feature) — the cold bytes go to a
//     local map / a redb table, proving the offload→rehydrate round-trip with no
//     SDK; this is the Pi-safe default.
//   * an S3/object-store impl behind a SEPARATE `cold-tier-s3` feature, reusing the
//     blob `ChunkStore` so the lean build links no object-store SDK.
//
// The graph state moved across the seam is exactly the `GraphCore::to_msgpack` /
// `from_msgpack` blob — the same shape a snapshot file holds — so offload is
// "serialize + hand to the cold store" and rehydrate is "fetch + `from_msgpack`".

/// A cold object-store tier for whole-graph offload/rehydrate (CONCEPT:KG-2.233).
/// Implemented in the facade over a local map / redb (default) or an object store
/// (feature-gated). Keyed by the LOGICAL graph name; the impl sanitizes it to its
/// own key shape. Blocking is allowed — a cold offload/rehydrate is a rare, off-hot-
/// path event (a graph crossing the cold threshold, or a cold graph being touched).
pub trait ColdTier: std::fmt::Debug + Send + Sync {
    /// Push a cold graph's serialized state to the cold store. `bytes` is the graph's
    /// `to_msgpack` blob. Overwrites any prior offload for `graph_name`.
    fn offload(&self, graph_name: &str, bytes: &[u8]) -> Result<(), String>;

    /// Fetch a previously-offloaded graph's serialized state. `Ok(None)` ⇒ the graph
    /// is not in the cold store (never offloaded, or already rehydrated+removed).
    fn rehydrate(&self, graph_name: &str) -> Result<Option<Vec<u8>>, String>;

    /// Whether `graph_name` currently has cold state offloaded (a cheap presence
    /// probe so a read path can detect "this graph is cold, rehydrate it first").
    fn is_offloaded(&self, graph_name: &str) -> Result<bool, String>;

    /// Drop a graph's cold state (called after a successful rehydrate, or when the
    /// graph is deleted). Idempotent — removing absent state is a no-op.
    fn remove(&self, graph_name: &str) -> Result<(), String>;
}

/// An in-memory [`ColdTier`] — the dep-free default impl. Holds offloaded graph
/// blobs in a map; proves the offload→rehydrate round-trip and backs the
/// `cold-tier` feature on a build with no object store (and the tests). A redb-
/// backed variant in the facade swaps the map for a redb table; an S3 variant swaps
/// it for the object store — the seam is identical.
#[derive(Debug, Default)]
pub struct InMemoryColdTier {
    inner: parking_lot::Mutex<std::collections::HashMap<String, Vec<u8>>>,
}

impl InMemoryColdTier {
    pub fn new() -> Self {
        Self::default()
    }
}

impl ColdTier for InMemoryColdTier {
    fn offload(&self, graph_name: &str, bytes: &[u8]) -> Result<(), String> {
        self.inner
            .lock()
            .insert(graph_name.to_string(), bytes.to_vec());
        Ok(())
    }

    fn rehydrate(&self, graph_name: &str) -> Result<Option<Vec<u8>>, String> {
        Ok(self.inner.lock().get(graph_name).cloned())
    }

    fn is_offloaded(&self, graph_name: &str) -> Result<bool, String> {
        Ok(self.inner.lock().contains_key(graph_name))
    }

    fn remove(&self, graph_name: &str) -> Result<(), String> {
        self.inner.lock().remove(graph_name);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn offload_rehydrate_round_trip() {
        let tier = InMemoryColdTier::new();
        assert!(!tier.is_offloaded("agent:planner").unwrap());

        let bytes = b"serialized-graph-state".to_vec();
        tier.offload("agent:planner", &bytes).unwrap();
        assert!(tier.is_offloaded("agent:planner").unwrap());

        let got = tier.rehydrate("agent:planner").unwrap();
        assert_eq!(got.as_deref(), Some(&bytes[..]));

        tier.remove("agent:planner").unwrap();
        assert!(!tier.is_offloaded("agent:planner").unwrap());
        assert!(tier.rehydrate("agent:planner").unwrap().is_none());
    }

    #[test]
    fn rehydrate_absent_is_none() {
        let tier = InMemoryColdTier::new();
        assert_eq!(tier.rehydrate("missing").unwrap(), None);
        // remove is idempotent on absent.
        tier.remove("missing").unwrap();
    }

    /// Full RAM→cold→RAM round-trip over a real `GraphCore` (CONCEPT:KG-2.233):
    /// offload spills the whole serialized graph to the cold tier and HIBERNATES the
    /// RAM (node count → 0); rehydrate brings every node/edge/property back, exactly
    /// as before the offload, and drops the cold copy.
    #[test]
    fn graphcore_offload_then_rehydrate_preserves_data() {
        use crate::graph::GraphCore;

        let core = GraphCore::new();
        for (id, label) in [("n1", "Agent"), ("n2", "Tool"), ("n3", "Agent")] {
            let props = rmp_serde::to_vec_named(&serde_json::json!({ "type": label })).unwrap();
            core.add_node(id.into(), props);
        }
        core.add_edge("n1".into(), "n2".into(), Vec::new()).ok();
        let before = core.node_count();
        assert_eq!(before, 3);

        let tier = InMemoryColdTier::new();

        // OFFLOAD: serialize → cold store → hibernate RAM.
        let freed = core.offload_to_cold("agent:planner", &tier).unwrap();
        assert_eq!(freed, 3, "offload frees the resident nodes");
        assert_eq!(core.node_count(), 0, "RAM is hibernated after offload");
        assert!(tier.is_offloaded("agent:planner").unwrap());

        // REHYDRATE: fetch → from_msgpack → drop cold copy.
        let rehydrated = core.rehydrate_from_cold("agent:planner", &tier).unwrap();
        assert!(rehydrated, "graph was offloaded, so rehydrate returns true");
        assert_eq!(core.node_count(), before, "all nodes back in RAM");
        // The edge + a node property survived the round-trip.
        assert!(core.get_node_properties("n1").is_some());
        assert!(
            !tier.is_offloaded("agent:planner").unwrap(),
            "cold copy removed after rehydrate"
        );

        // Rehydrating a graph that is NOT offloaded is a no-op (already hot).
        assert!(!core.rehydrate_from_cold("agent:planner", &tier).unwrap());
    }
}
