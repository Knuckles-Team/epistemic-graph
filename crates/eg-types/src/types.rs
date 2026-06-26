// CONCEPT:KG-2.16 - Typed Graph Data Model
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
/// Stored inside `GraphCore` as the node weight. Legacy JSON blobs
/// are parsed into this struct on ingestion for backward compat.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeData {
    /// Unique node identifier.
    pub id: String,
    /// Node type (e.g., "Agent", "Concept", "Episode").
    pub node_type: String,
    /// Optional embedding vector for similarity operations.
    #[serde(default)]
    pub embedding: Option<Vec<f32>>,
    /// Lifecycle state drives pruning/compaction decisions.
    #[serde(default)]
    pub lifecycle_state: LifecycleState,
    /// Creation timestamp (epoch seconds).
    #[serde(default)]
    pub created_at: u64,
    /// Last update timestamp (epoch seconds).
    #[serde(default)]
    pub updated_at: u64,
    /// Extensible metadata as JSON string.
    #[serde(default)]
    pub metadata: String,
    /// Belief confidence score (0.0 to 1.0) for temporal fact decay.
    #[serde(default = "default_confidence")]
    pub confidence: f64,
    /// Start of temporal validity window (epoch seconds).
    #[serde(default)]
    pub valid_from: Option<u64>,
    /// End of temporal validity window (epoch seconds), if applicable.
    #[serde(default)]
    pub valid_until: Option<u64>,
    /// Start of the transaction-time window — when the engine BEGAN believing
    /// this fact (epoch seconds). Distinct from `valid_from` (when the fact became
    /// true in the world). Together they make the store bi-temporal (KG-2.249).
    #[serde(default)]
    pub tx_from: Option<u64>,
    /// End of the transaction-time window — when the engine STOPPED believing this
    /// fact (e.g. it was superseded). `None` while currently believed.
    #[serde(default)]
    pub tx_to: Option<u64>,
}

fn default_confidence() -> f64 {
    1.0
}

impl NodeData {
    /// Create a new node with minimal required fields.
    pub fn new(id: String, node_type: String) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        NodeData {
            id,
            node_type,
            embedding: None,
            lifecycle_state: LifecycleState::Active,
            created_at: now,
            updated_at: now,
            metadata: String::new(),
            confidence: 1.0,
            valid_from: Some(now),
            valid_until: None,
            tx_from: Some(now),
            tx_to: None,
        }
    }

    /// Parse a legacy JSON properties string into NodeData.
    ///
    /// Extracts `type` from the JSON and stores the rest as metadata.
    pub fn from_json_props(id: String, json_str: &str) -> Self {
        if let Ok(val) = serde_json::from_slice::<serde_json::Value>(json_str.as_bytes()) {
            let node_type = val
                .get("type")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
                .to_string();
            let embedding = val
                .get("embedding")
                .and_then(|v| serde_json::from_value::<Vec<f32>>(v.clone()).ok());
            let lifecycle_str = val
                .get("lifecycle_state")
                .and_then(|v| v.as_str())
                .unwrap_or("active");
            let lifecycle_state = match lifecycle_str {
                "compacted" => LifecycleState::Compacted,
                "archived" => LifecycleState::Archived,
                "pending_deletion" => LifecycleState::PendingDeletion,
                _ => LifecycleState::Active,
            };
            NodeData {
                id,
                node_type,
                embedding,
                lifecycle_state,
                created_at: val.get("created_at").and_then(|v| v.as_u64()).unwrap_or(0),
                updated_at: val.get("updated_at").and_then(|v| v.as_u64()).unwrap_or(0),
                metadata: json_str.to_string(),
                confidence: val
                    .get("confidence")
                    .and_then(|v| v.as_f64())
                    .unwrap_or(1.0),
                valid_from: val.get("valid_from").and_then(|v| v.as_u64()),
                valid_until: val.get("valid_until").and_then(|v| v.as_u64()),
                tx_from: val.get("tx_from").and_then(|v| v.as_u64()),
                tx_to: val.get("tx_to").and_then(|v| v.as_u64()),
            }
        } else {
            NodeData::new(id, "unknown".to_string())
        }
    }

    /// Serialize back to JSON string (backward compat with legacy API).
    pub fn to_json_props(&self) -> String {
        // Try to merge into existing metadata JSON
        if let Ok(mut val) = serde_json::from_slice::<serde_json::Value>(self.metadata.as_bytes()) {
            if let Some(obj) = val.as_object_mut() {
                obj.insert(
                    "type".to_string(),
                    serde_json::Value::String(self.node_type.clone()),
                );
                obj.insert(
                    "lifecycle_state".to_string(),
                    serde_json::Value::String(self.lifecycle_state.to_string()),
                );
                obj.insert("confidence".to_string(), serde_json::json!(self.confidence));
                if let Some(vf) = self.valid_from {
                    obj.insert("valid_from".to_string(), serde_json::json!(vf));
                }
                if let Some(vu) = self.valid_until {
                    obj.insert("valid_until".to_string(), serde_json::json!(vu));
                }
                if let Some(tf) = self.tx_from {
                    obj.insert("tx_from".to_string(), serde_json::json!(tf));
                }
                if let Some(tt) = self.tx_to {
                    obj.insert("tx_to".to_string(), serde_json::json!(tt));
                }
                if let Ok(s) = serde_json::to_string(&val) {
                    return s;
                }
            }
        }
        // Fallback: create minimal JSON
        serde_json::json!({
            "type": self.node_type,
            "lifecycle_state": self.lifecycle_state.to_string(),
            "confidence": self.confidence,
            "valid_from": self.valid_from,
            "valid_until": self.valid_until,
            "tx_from": self.tx_from,
            "tx_to": self.tx_to,
        })
        .to_string()
    }
}

// ── Edge Types ───────────────────────────────────────────────────────────

/// Typed edge data with weight, provenance, and relationship type.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EdgeData {
    /// Relationship type (e.g., "DEPENDS_ON", "CONTAINS", "SIMILAR_TO").
    pub relationship_type: String,
    /// Edge weight/score for ranking and traversal.
    #[serde(default = "default_weight")]
    pub weight: f64,
    /// Provenance tracking (e.g., "inferred", "user", "system").
    #[serde(default)]
    pub provenance: String,
    /// Extensible metadata as JSON string.
    #[serde(default)]
    pub metadata: String,
    /// Belief confidence score (0.0 to 1.0) for temporal fact decay.
    #[serde(default = "default_confidence")]
    pub confidence: f64,
    /// Start of temporal validity window (epoch seconds).
    #[serde(default)]
    pub valid_from: Option<u64>,
    /// End of temporal validity window (epoch seconds), if applicable.
    #[serde(default)]
    pub valid_until: Option<u64>,
    /// Start of the transaction-time window — when the engine began believing
    /// this edge (epoch seconds). See [`NodeData::tx_from`] (KG-2.249).
    #[serde(default)]
    pub tx_from: Option<u64>,
    /// End of the transaction-time window — when this edge was superseded/retracted.
    /// `None` while currently believed; set by the invalidation path (KG-2.251).
    #[serde(default)]
    pub tx_to: Option<u64>,
}

fn default_weight() -> f64 {
    1.0
}

impl EdgeData {
    pub fn new(relationship_type: String) -> Self {
        EdgeData {
            relationship_type,
            weight: 1.0,
            provenance: String::new(),
            metadata: String::new(),
            confidence: 1.0,
            valid_from: None,
            valid_until: None,
            tx_from: None,
            tx_to: None,
        }
    }

    /// Parse a legacy JSON properties string into EdgeData.
    pub fn from_json_props(json_str: &str) -> Self {
        if let Ok(val) = serde_json::from_slice::<serde_json::Value>(json_str.as_bytes()) {
            let relationship_type = val
                .get("relationship")
                .or_else(|| val.get("type"))
                .and_then(|v| v.as_str())
                .unwrap_or("RELATED_TO")
                .to_string();
            let weight = val.get("weight").and_then(|v| v.as_f64()).unwrap_or(1.0);
            let provenance = val
                .get("provenance")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            EdgeData {
                relationship_type,
                weight,
                provenance,
                metadata: json_str.to_string(),
                confidence: val
                    .get("confidence")
                    .and_then(|v| v.as_f64())
                    .unwrap_or(1.0),
                valid_from: val.get("valid_from").and_then(|v| v.as_u64()),
                valid_until: val.get("valid_until").and_then(|v| v.as_u64()),
                tx_from: val.get("tx_from").and_then(|v| v.as_u64()),
                tx_to: val.get("tx_to").and_then(|v| v.as_u64()),
            }
        } else {
            EdgeData::new("RELATED_TO".to_string())
        }
    }

    /// Serialize back to JSON string (backward compat).
    pub fn to_json_props(&self) -> String {
        if let Ok(mut val) = serde_json::from_slice::<serde_json::Value>(self.metadata.as_bytes()) {
            if let Some(obj) = val.as_object_mut() {
                obj.insert(
                    "relationship".to_string(),
                    serde_json::Value::String(self.relationship_type.clone()),
                );
                obj.insert("weight".to_string(), serde_json::json!(self.weight));
                obj.insert("confidence".to_string(), serde_json::json!(self.confidence));
                if let Some(vf) = self.valid_from {
                    obj.insert("valid_from".to_string(), serde_json::json!(vf));
                }
                if let Some(vu) = self.valid_until {
                    obj.insert("valid_until".to_string(), serde_json::json!(vu));
                }
                if let Some(tf) = self.tx_from {
                    obj.insert("tx_from".to_string(), serde_json::json!(tf));
                }
                if let Some(tt) = self.tx_to {
                    obj.insert("tx_to".to_string(), serde_json::json!(tt));
                }
                if let Ok(s) = serde_json::to_string(&val) {
                    return s;
                }
            }
        }
        serde_json::json!({
            "relationship": self.relationship_type,
            "weight": self.weight,
            "confidence": self.confidence,
            "valid_from": self.valid_from,
            "valid_until": self.valid_until,
            "tx_from": self.tx_from,
            "tx_to": self.tx_to,
        })
        .to_string()
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

/// Result of an Ebbinghaus forgetting-curve decay sweep (CONCEPT:KG-2.16).
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
    fn old_node_blob_without_tx_decodes_as_none() {
        // A pre-bitemporal MessagePack blob has no tx_from/tx_to keys.
        let json = r#"{"type":"Episode","valid_from":100,"valid_until":200}"#;
        let n = NodeData::from_json_props("n1".into(), json);
        assert_eq!(n.valid_from, Some(100));
        assert_eq!(n.tx_from, None, "missing tx_from must default to None");
        assert_eq!(n.tx_to, None);
    }

    #[test]
    fn node_json_props_roundtrip_preserves_tx_fields() {
        let mut n = NodeData::new("n1".into(), "Episode".into());
        n.tx_from = Some(11);
        n.tx_to = Some(22);
        let json = n.to_json_props();
        let back = NodeData::from_json_props("n1".into(), &json);
        assert_eq!(back.tx_from, Some(11));
        assert_eq!(back.tx_to, Some(22));
    }

    #[test]
    fn edge_msgpack_roundtrip_and_old_blob_default() {
        let mut e = EdgeData::new("LIKES".into());
        e.valid_from = Some(100);
        e.tx_from = Some(100);
        e.valid_until = Some(200);
        e.tx_to = Some(200);
        let bytes = rmp_serde::to_vec_named(&e).expect("serialize");
        let back: EdgeData = rmp_serde::from_slice(&bytes).expect("deserialize");
        assert_eq!(back.valid_until, Some(200));
        assert_eq!(back.tx_to, Some(200));

        let old = EdgeData::from_json_props(r#"{"relationship":"LIKES","valid_from":100}"#);
        assert_eq!(old.valid_from, Some(100));
        assert_eq!(old.tx_from, None);
        assert_eq!(old.tx_to, None);
    }
}
