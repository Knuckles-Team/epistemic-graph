// CONCEPT:KG-2.19 — Multi-Tenant Graph Registry
//
// Manages named graphs with lifecycle operations. The `__bus__` graph
// is always present as the shared message bus.

use std::collections::HashMap;
use std::sync::Arc;

use crate::graph::GraphCore;
use crate::protocol::GraphType;

/// Per-graph write-ahead log handle (Phase B2). Lazily opened on the first durable
/// mutation; lives with its graph so the dispatch can append without the global
/// registry lock. Server-only (the WAL module is gated behind `server`).
#[cfg(feature = "server")]
pub type WalHandle = Arc<parking_lot::Mutex<Option<crate::wal::WalWriter>>>;

/// Metadata for a registered graph.
#[derive(Debug, Clone)]
pub struct GraphEntry {
    pub name: String,
    pub graph_type: GraphType,
    pub core: Arc<GraphCore>,
    pub owner: Option<String>,
    #[cfg(feature = "server")]
    pub wal: WalHandle,
}

/// Multi-tenant graph registry.
pub struct GraphRegistry {
    graphs: HashMap<String, GraphEntry>,
}

impl GraphRegistry {
    /// Create a new registry with the `__bus__` graph pre-created.
    pub fn new() -> Self {
        let mut graphs = HashMap::new();
        graphs.insert(
            "__bus__".to_string(),
            GraphEntry {
                name: "__bus__".to_string(),
                graph_type: GraphType::Bus,
                core: Arc::new(GraphCore::new()),
                owner: None,
                #[cfg(feature = "server")]
                wal: Arc::new(parking_lot::Mutex::new(None)),
            },
        );
        GraphRegistry { graphs }
    }

    /// Create a new named graph.
    pub fn create_graph(
        &mut self,
        name: &str,
        graph_type: GraphType,
        owner: Option<String>,
    ) -> Result<(), String> {
        if self.graphs.contains_key(name) {
            return Err(format!("Graph '{}' already exists", name));
        }
        self.graphs.insert(
            name.to_string(),
            GraphEntry {
                name: name.to_string(),
                graph_type,
                core: Arc::new(GraphCore::new()),
                owner,
                #[cfg(feature = "server")]
                wal: Arc::new(parking_lot::Mutex::new(None)),
            },
        );
        Ok(())
    }

    /// Delete a named graph. Cannot delete `__bus__`.
    pub fn delete_graph(&mut self, name: &str) -> Result<(), String> {
        if name == "__bus__" {
            return Err("Cannot delete the __bus__ graph".to_string());
        }
        self.graphs
            .remove(name)
            .map(|_| ())
            .ok_or_else(|| format!("Graph '{}' not found", name))
    }

    /// Get a reference to a graph entry.
    pub fn get(&self, name: &str) -> Option<&GraphEntry> {
        self.graphs.get(name)
    }

    /// Get a mutable reference to a graph entry.
    pub fn get_mut(&mut self, name: &str) -> Option<&mut GraphEntry> {
        self.graphs.get_mut(name)
    }

    /// List all registered graph names and types.
    pub fn list(&self) -> Vec<(String, GraphType)> {
        self.graphs
            .iter()
            .map(|(name, entry)| (name.clone(), entry.graph_type))
            .collect()
    }

    /// Check if a graph exists.
    pub fn exists(&self, name: &str) -> bool {
        self.graphs.contains_key(name)
    }

    /// Get all graph entries for checkpoint/persistence.
    pub fn all_entries(&self) -> Vec<&GraphEntry> {
        self.graphs.values().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bus_exists_on_creation() {
        let reg = GraphRegistry::new();
        assert!(reg.exists("__bus__"));
        assert_eq!(reg.list().len(), 1);
    }

    #[test]
    fn test_create_and_delete_graph() {
        let mut reg = GraphRegistry::new();
        reg.create_graph("agent:planner", GraphType::Agent, Some("planner".into()))
            .unwrap();
        assert!(reg.exists("agent:planner"));
        assert_eq!(reg.list().len(), 2);

        reg.delete_graph("agent:planner").unwrap();
        assert!(!reg.exists("agent:planner"));
    }

    #[test]
    fn test_cannot_delete_bus() {
        let mut reg = GraphRegistry::new();
        assert!(reg.delete_graph("__bus__").is_err());
    }

    #[test]
    fn test_duplicate_create_fails() {
        let mut reg = GraphRegistry::new();
        reg.create_graph("test", GraphType::Team, None).unwrap();
        assert!(reg.create_graph("test", GraphType::Team, None).is_err());
    }

    #[test]
    fn test_delete_nonexistent_fails() {
        let mut reg = GraphRegistry::new();
        assert!(reg.delete_graph("nope").is_err());
    }
}
