// CONCEPT:EG-KG.compute.typed-graph-model - Typed Graph Data Model
//
// Typed node/edge structs with lifecycle state, embeddings,
// and metadata. Replaces raw String JSON blobs with structured
// Rust types for memory safety and performance.

use serde::{Deserialize, Serialize};

// ── Node Types ───────────────────────────────────────────────────────────

/// Lifecycle state for graph nodes — drives pruning and compaction.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum LifecycleState {
    #[default]
    Active,
    Compacted,
    Archived,
    PendingDeletion,
}

impl std::fmt::Display for LifecycleState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LifecycleState::Active => write!(f, "active"),
            LifecycleState::Compacted => write!(f, "compacted"),
            LifecycleState::Archived => write!(f, "archived"),
            LifecycleState::PendingDeletion => write!(f, "pending_deletion"),
        }
    }
}

/// Typed node data with embeddings and lifecycle awareness.
///
/// This type has one explicit current MessagePack shape. Graph property objects
/// are decoded independently and are never interpreted as retired JSON wrappers.
pub const TYPED_GRAPH_DATA_VERSION: u16 = 2;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeData {
    pub schema_version: u16,
    /// Unique node identifier.
    pub id: String,
    /// Node type (e.g., "Agent", "Concept", "Episode").
    pub node_type: String,
    /// Optional embedding vector for similarity operations.
    pub embedding: Option<Vec<f32>>,
    /// Lifecycle state drives pruning/compaction decisions.
    pub lifecycle_state: LifecycleState,
    /// Creation timestamp (epoch seconds).
    pub created_at: u64,
    /// Last update timestamp (epoch seconds).
    pub updated_at: u64,
    /// Extensible typed metadata.
    pub metadata: std::collections::BTreeMap<String, serde_json::Value>,
    /// Belief confidence score (0.0 to 1.0) for temporal fact decay.
    pub confidence: f64,
    /// Start of temporal validity window (epoch seconds).
    pub valid_from: Option<u64>,
    /// End of temporal validity window (epoch seconds), if applicable.
    pub valid_until: Option<u64>,
    /// Start of the transaction-time window — when the engine BEGAN believing
    /// this fact (epoch seconds). Distinct from `valid_from` (when the fact became
    /// true in the world). Together they make the store bi-temporal (KG-2.249).
    pub tx_from: Option<u64>,
    /// End of the transaction-time window — when the engine STOPPED believing this
    /// fact (e.g. it was superseded). `None` while currently believed.
    pub tx_to: Option<u64>,
}

impl NodeData {
    /// Create a new node with minimal required fields.
    pub fn new(id: String, node_type: String) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        NodeData {
            schema_version: TYPED_GRAPH_DATA_VERSION,
            id,
            node_type,
            embedding: None,
            lifecycle_state: LifecycleState::Active,
            created_at: now,
            updated_at: now,
            metadata: std::collections::BTreeMap::new(),
            confidence: 1.0,
            valid_from: Some(now),
            valid_until: None,
            tx_from: Some(now),
            tx_to: None,
        }
    }
}

// ── Edge Types ───────────────────────────────────────────────────────────

/// Typed edge data with weight, provenance, and a canonical relationship.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EdgeData {
    pub schema_version: u16,
    /// Relationship (e.g., "DEPENDS_ON", "CONTAINS", "SIMILAR_TO").
    pub relationship: String,
    /// Edge weight/score for ranking and traversal.
    pub weight: f64,
    /// Provenance tracking (e.g., "inferred", "user", "system").
    pub provenance: String,
    /// Extensible typed metadata.
    pub metadata: std::collections::BTreeMap<String, serde_json::Value>,
    /// Belief confidence score (0.0 to 1.0) for temporal fact decay.
    pub confidence: f64,
    /// Start of temporal validity window (epoch seconds).
    pub valid_from: Option<u64>,
    /// End of temporal validity window (epoch seconds), if applicable.
    pub valid_until: Option<u64>,
    /// Start of the transaction-time window — when the engine began believing
    /// this edge (epoch seconds). See [`NodeData::tx_from`] (KG-2.249).
    pub tx_from: Option<u64>,
    /// End of the transaction-time window — when this edge was superseded/retracted.
    /// `None` while currently believed; set by the invalidation path (KG-2.251).
    pub tx_to: Option<u64>,
}

impl EdgeData {
    pub fn new(relationship: String) -> Self {
        EdgeData {
            schema_version: TYPED_GRAPH_DATA_VERSION,
            relationship,
            weight: 1.0,
            provenance: String::new(),
            metadata: std::collections::BTreeMap::new(),
            confidence: 1.0,
            valid_from: None,
            valid_until: None,
            tx_from: None,
            tx_to: None,
        }
    }
}

// ── Graph Metrics ────────────────────────────────────────────────────────

/// Runtime metrics for monitoring and observability.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct GraphMetrics {
    pub node_count: usize,
    pub edge_count: usize,
    pub total_mutations: u64,
    pub last_prune_removed: usize,
    pub active_nodes: usize,
    pub compacted_nodes: usize,
    pub archived_nodes: usize,
}

// ── Prune Stats ──────────────────────────────────────────────────────────

/// Result of a lifecycle-aware pruning operation.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct PruneStats {
    pub nodes_removed: usize,
    pub edges_removed: usize,
    pub nodes_archived: usize,
}

// ── Decay Stats ──────────────────────────────────────────────────────────

/// Result of an Ebbinghaus forgetting-curve decay sweep (CONCEPT:EG-KG.compute.typed-graph-model).
///
/// `*_decayed` count items whose belief `confidence` was reduced this sweep;
/// `*_pruned` count items removed because their decayed confidence fell below
/// the sweep `floor`.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct DecayStats {
    pub nodes_decayed: usize,
    pub edges_decayed: usize,
    pub nodes_pruned: usize,
    pub edges_pruned: usize,
}

// ── Context View ─────────────────────────────────────────────────────────

/// Optimized context view returned by get_context_view.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ContextView {
    pub agent_id: String,
    pub nodes: Vec<String>,
    pub edges: Vec<(String, String, Vec<u8>)>,
    pub budget_used: u32,
    pub budget_max: u32,
}

// ── Tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod bitemporal_tests {
    use super::*;

    #[test]
    fn node_new_sets_both_time_axes() {
        let n = NodeData::new("n1".into(), "Episode".into());
        assert!(n.valid_from.is_some());
        assert!(
            n.tx_from.is_some(),
            "transaction-time start must be set on new"
        );
        assert_eq!(n.valid_until, None);
        assert_eq!(n.tx_to, None);
    }

    #[test]
    fn node_msgpack_roundtrip_preserves_tx_fields() {
        let mut n = NodeData::new("n1".into(), "Episode".into());
        n.valid_from = Some(100);
        n.valid_until = Some(200);
        n.tx_from = Some(150);
        n.tx_to = Some(250);
        let bytes = rmp_serde::to_vec_named(&n).expect("serialize");
        let back: NodeData = rmp_serde::from_slice(&bytes).expect("deserialize");
        assert_eq!(back.valid_from, Some(100));
        assert_eq!(back.valid_until, Some(200));
        assert_eq!(back.tx_from, Some(150));
        assert_eq!(back.tx_to, Some(250));
    }

    #[test]
    fn edge_msgpack_roundtrip_preserves_current_shape() {
        let mut e = EdgeData::new("LIKES".into());
        e.valid_from = Some(100);
        e.tx_from = Some(100);
        e.valid_until = Some(200);
        e.tx_to = Some(200);
        let bytes = rmp_serde::to_vec_named(&e).expect("serialize");
        let shape: serde_json::Value = rmp_serde::from_slice(&bytes).expect("inspect shape");
        assert_eq!(shape.get("relationship"), Some(&serde_json::json!("LIKES")));
        assert!(shape.get("relationship_type").is_none());
        let back: EdgeData = rmp_serde::from_slice(&bytes).expect("deserialize");
        assert_eq!(back.schema_version, TYPED_GRAPH_DATA_VERSION);
        assert_eq!(back.relationship, "LIKES");
        assert_eq!(back.valid_until, Some(200));
        assert_eq!(back.tx_to, Some(200));
    }

    #[test]
    fn retired_edge_relationship_key_is_rejected() {
        let retired = serde_json::json!({
            "schema_version": 1,
            "relationship_type": "LIKES",
            "weight": 1.0,
            "provenance": "",
            "metadata": {},
            "confidence": 1.0,
            "valid_from": null,
            "valid_until": null,
            "tx_from": null,
            "tx_to": null
        });
        let bytes = rmp_serde::to_vec_named(&retired).expect("encode retired shape");
        assert!(rmp_serde::from_slice::<EdgeData>(&bytes).is_err());
    }
}
