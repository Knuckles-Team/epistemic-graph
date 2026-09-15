use std::sync::Arc;

use eg_core::graph::GraphCore;

/// How an UPDATE resolves a SPARQL graph name to a writable [`GraphCore`], so the
/// executor is decoupled from the engine's graph registry. The default graph is `None`;
/// a named graph is `Some(bare-iri)`.
pub trait GraphStore {
    /// The core backing a graph, creating it on demand (writes auto-vivify a graph,
    /// matching the engine's lazy-create behavior). `None` ⇒ the store could not
    /// provide/create it (the op is then skipped or errors per `silent`).
    fn core(&self, graph: Option<&str>) -> Option<Arc<GraphCore>>;

    /// Every named graph as `(bare-iri, core)` — the candidate set a `GRAPH ?g` WHERE
    /// clause ranges over. The default impl returns nothing (single-graph stores).
    fn named(&self) -> Vec<(String, Arc<GraphCore>)> {
        Vec::new()
    }

    /// CLEAR a graph (remove all its triples) — idempotent. Default = clear the core.
    fn clear(&self, graph: Option<&str>) -> Result<(), String> {
        if let Some(c) = self.core(graph) {
            c.clear();
        }
        Ok(())
    }

    /// DROP a graph entirely. Default delegates to [`GraphStore::clear`] (the store may
    /// override to also evict the registry entry).
    fn drop_graph(&self, graph: Option<&str>) -> Result<(), String> {
        self.clear(graph)
    }

    /// CREATE GRAPH — ensure it exists. Default = touch the core.
    fn create(&self, iri: &str) -> Result<(), String> {
        match self.core(Some(iri)) {
            Some(_) => Ok(()),
            None => Err(format!("CREATE GRAPH <{iri}>: store could not create it")),
        }
    }
}

// ── an in-memory store for tests + embedded use ─────────────────────────────────

/// A simple [`GraphStore`] backed by in-memory `GraphCore`s, keyed by graph name (the
/// default graph is the empty key). Auto-creates graphs on first write. Used by the
/// eg-rdf tests and available to embedded callers that have no engine registry.
#[derive(Default)]
pub struct MapStore {
    graphs: std::sync::Mutex<std::collections::HashMap<String, Arc<GraphCore>>>,
}

impl MapStore {
    pub fn new() -> Self {
        Self::default()
    }
    fn key(graph: Option<&str>) -> String {
        graph.unwrap_or("").to_string()
    }
    /// Borrow (creating) the core for a graph — handy for tests asserting state.
    pub fn core_of(&self, graph: Option<&str>) -> Arc<GraphCore> {
        self.core(graph).unwrap()
    }
}

impl GraphStore for MapStore {
    fn core(&self, graph: Option<&str>) -> Option<Arc<GraphCore>> {
        let mut g = self.graphs.lock().unwrap();
        Some(
            g.entry(Self::key(graph))
                .or_insert_with(|| Arc::new(GraphCore::new()))
                .clone(),
        )
    }
    fn named(&self) -> Vec<(String, Arc<GraphCore>)> {
        self.graphs
            .lock()
            .unwrap()
            .iter()
            .filter(|(k, _)| !k.is_empty())
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
    fn clear(&self, graph: Option<&str>) -> Result<(), String> {
        if let Some(c) = self.graphs.lock().unwrap().get(&Self::key(graph)) {
            c.clear();
        }
        Ok(())
    }
    fn drop_graph(&self, graph: Option<&str>) -> Result<(), String> {
        self.graphs.lock().unwrap().remove(&Self::key(graph));
        Ok(())
    }
}
