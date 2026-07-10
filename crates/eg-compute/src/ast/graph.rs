//! Call-graph + module dependency-graph + change-causality (CONCEPT:EG-KG.compute
//! depth: per-modality code-graph traversal) built OVER an already-resolved
//! [`crate::parser::resolve::IndexResult`] — no new parsing, no new deps, pure
//! adjacency + BFS over the `calls`/`depends_on` edges `resolve::resolve` already
//! produces.
//!
//! `resolve::resolve` gives a flat `Vec<ExtractedEdge>` (every edge type mixed
//! together); that's the right shape for bulk KG ingestion but the wrong shape for
//! graph QUESTIONS like "what does this symbol call, transitively?" or "if I change
//! this symbol, what else in the codebase does that change reach?". [`DirectedGraph`]
//! is the queryable adjacency built from ONE edge type, with both directions
//! indexed so forward ("what does X reach") and reverse ("what reaches X" — the
//! classic impact-analysis/blast-radius direction) traversals are both O(1) lookups
//! per hop.
//!
//! * [`call_graph`] — symbol → symbol, from resolved `calls` edges.
//! * [`dependency_graph`] — file → file, from resolved `depends_on` edges (`file:`
//!   id prefix, matching [`crate::parser::resolve`]'s own convention).
//! * [`change_causality`] — the change-impact helper: given a changed symbol id,
//!   the full set of symbols it TRANSITIVELY reaches by calling (forward BFS over
//!   the call-graph). Pair with [`DirectedGraph::reachable_reverse`] on the SAME
//!   graph to instead ask "who (transitively) calls this" — the complementary
//!   "what could break if I change this" direction.

use crate::parser::resolve::IndexResult;
use std::collections::{HashMap, HashSet, VecDeque};

/// A directed graph over node ids (symbol ids or `file:`-prefixed file ids),
/// indexed BOTH ways so forward and reverse traversal are each a single hop away —
/// no re-scan of the edge list per query.
#[derive(Clone, Debug, Default)]
pub struct DirectedGraph {
    forward: HashMap<String, Vec<String>>,
    reverse: HashMap<String, Vec<String>>,
}

impl DirectedGraph {
    /// Build from an edge iterator of `(source, target)` pairs.
    pub fn from_edges<'a>(edges: impl Iterator<Item = (&'a str, &'a str)>) -> Self {
        let mut forward: HashMap<String, Vec<String>> = HashMap::new();
        let mut reverse: HashMap<String, Vec<String>> = HashMap::new();
        for (src, tgt) in edges {
            forward
                .entry(src.to_string())
                .or_default()
                .push(tgt.to_string());
            reverse
                .entry(tgt.to_string())
                .or_default()
                .push(src.to_string());
        }
        Self { forward, reverse }
    }

    /// Direct (one-hop) outgoing neighbours of `id` — e.g. the symbols `id` calls.
    pub fn out_neighbors(&self, id: &str) -> &[String] {
        self.forward.get(id).map(Vec::as_slice).unwrap_or(&[])
    }

    /// Direct (one-hop) incoming neighbours of `id` — e.g. the symbols that call
    /// `id`.
    pub fn in_neighbors(&self, id: &str) -> &[String] {
        self.reverse.get(id).map(Vec::as_slice).unwrap_or(&[])
    }

    /// Every node `start` transitively reaches by following edges FORWARD
    /// (excludes `start` itself; cycle-safe via a visited set).
    pub fn reachable_forward(&self, start: &str) -> HashSet<String> {
        bfs(&self.forward, start)
    }

    /// Every node that transitively reaches `start` by following edges BACKWARD —
    /// the impact-analysis / blast-radius direction ("who is affected if `start`
    /// changes").
    pub fn reachable_reverse(&self, start: &str) -> HashSet<String> {
        bfs(&self.reverse, start)
    }

    pub fn node_count(&self) -> usize {
        let mut ids: HashSet<&str> = HashSet::new();
        ids.extend(self.forward.keys().map(String::as_str));
        ids.extend(self.reverse.keys().map(String::as_str));
        ids.len()
    }
}

fn bfs(adj: &HashMap<String, Vec<String>>, start: &str) -> HashSet<String> {
    let mut seen: HashSet<String> = HashSet::new();
    let mut out: HashSet<String> = HashSet::new();
    let mut queue: VecDeque<String> = VecDeque::new();
    seen.insert(start.to_string());
    queue.push_back(start.to_string());
    while let Some(node) = queue.pop_front() {
        if let Some(next) = adj.get(&node) {
            for t in next {
                if seen.insert(t.clone()) {
                    out.insert(t.clone());
                    queue.push_back(t.clone());
                }
            }
        }
    }
    out
}

/// The call-graph (symbol → symbol): every resolved `calls` edge from
/// `resolve::resolve`'s `IndexResult`, as a queryable [`DirectedGraph`].
pub fn call_graph(result: &IndexResult) -> DirectedGraph {
    DirectedGraph::from_edges(
        result
            .edges
            .iter()
            .filter(|e| e.edge_type == "calls")
            .map(|e| (e.source.as_str(), e.target.as_str())),
    )
}

/// The module dependency-graph (`file:`-prefixed id → `file:`-prefixed id): every
/// resolved `depends_on` edge from `resolve::resolve`'s `IndexResult`.
pub fn dependency_graph(result: &IndexResult) -> DirectedGraph {
    DirectedGraph::from_edges(
        result
            .edges
            .iter()
            .filter(|e| e.edge_type == "depends_on")
            .map(|e| (e.source.as_str(), e.target.as_str())),
    )
}

/// Change-causality: given a `changed_symbol_id`, every symbol it TRANSITIVELY
/// **reaches** by calling — forward BFS over the resolved call-graph. This is
/// "what downstream behavior can this change's effects propagate into", the
/// literal reachability question. For the complementary "who calls this symbol
/// (and so might be affected if its CONTRACT changes)" impact-analysis direction,
/// call `call_graph(result).reachable_reverse(changed_symbol_id)` on the same
/// graph — both directions are O(1) per hop on one `DirectedGraph`, no rebuild.
pub fn change_causality(result: &IndexResult, changed_symbol_id: &str) -> HashSet<String> {
    call_graph(result).reachable_forward(changed_symbol_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::resolve::index_repository;

    fn files(pairs: &[(&str, &str)]) -> Vec<(String, Vec<u8>)> {
        pairs
            .iter()
            .map(|(p, s)| (p.to_string(), s.as_bytes().to_vec()))
            .collect()
    }

    fn node_id(r: &IndexResult, name: &str, file: &str) -> String {
        r.nodes
            .iter()
            .find(|n| {
                n.properties.get("name").map(String::as_str) == Some(name)
                    && n.properties.get("file_path").map(String::as_str) == Some(file)
            })
            .unwrap_or_else(|| panic!("no {name} in {file}"))
            .node_id
            .clone()
    }

    #[test]
    fn call_graph_direct_edge_matches_resolved_calls() {
        let r = index_repository(&files(&[(
            "m.py",
            "def helper():\n    return 1\n\ndef caller():\n    return helper()\n",
        )]));
        let g = call_graph(&r);
        let caller = node_id(&r, "caller", "m.py");
        let helper = node_id(&r, "helper", "m.py");
        assert!(g.out_neighbors(&caller).contains(&helper));
        assert!(g.in_neighbors(&helper).contains(&caller));
    }

    #[test]
    fn change_causality_finds_transitive_downstream_symbols() {
        // a -> b -> c (a chain of calls); changing `a` reaches b AND c transitively.
        let r = index_repository(&files(&[(
            "chain.py",
            "def c():\n    return 1\n\ndef b():\n    return c()\n\ndef a():\n    return b()\n",
        )]));
        let a = node_id(&r, "a", "chain.py");
        let b = node_id(&r, "b", "chain.py");
        let c = node_id(&r, "c", "chain.py");
        let reached = change_causality(&r, &a);
        assert!(reached.contains(&b), "a reaches b directly");
        assert!(
            reached.contains(&c),
            "a must reach c TRANSITIVELY (through b), got {reached:?}"
        );
        // `c` calls nothing — it reaches nothing.
        assert!(call_graph(&r).reachable_forward(&c).is_empty());
    }

    #[test]
    fn reverse_reachability_gives_impact_analysis_direction() {
        // a -> b -> c ; "who is impacted if c changes" = {a, b} (both transitively call it).
        let r = index_repository(&files(&[(
            "chain.py",
            "def c():\n    return 1\n\ndef b():\n    return c()\n\ndef a():\n    return b()\n",
        )]));
        let a = node_id(&r, "a", "chain.py");
        let b = node_id(&r, "b", "chain.py");
        let c = node_id(&r, "c", "chain.py");
        let impacted = call_graph(&r).reachable_reverse(&c);
        assert!(impacted.contains(&a));
        assert!(impacted.contains(&b));
    }

    #[test]
    fn dependency_graph_tracks_file_level_imports() {
        let r = index_repository(&files(&[
            ("pkg/util.py", "def shared():\n    return 1\n"),
            (
                "pkg/app.py",
                "from pkg.util import shared\n\ndef run():\n    return shared()\n",
            ),
        ]));
        let g = dependency_graph(&r);
        assert!(g
            .out_neighbors("file:pkg/app.py")
            .contains(&"file:pkg/util.py".to_string()));
        let reached = g.reachable_forward("file:pkg/app.py");
        assert!(reached.contains("file:pkg/util.py"));
    }

    #[test]
    fn cyclic_calls_do_not_infinite_loop() {
        // Mutual recursion: ping <-> pong. BFS must terminate and both must reach
        // each other (through the cycle) without looping forever.
        let r = index_repository(&files(&[(
            "rec.py",
            "def ping():\n    return pong()\n\ndef pong():\n    return ping()\n",
        )]));
        let ping = node_id(&r, "ping", "rec.py");
        let pong = node_id(&r, "pong", "rec.py");
        let reached = change_causality(&r, &ping);
        assert!(reached.contains(&pong));
        assert!(
            reached.contains(&ping),
            "a cycle means ping transitively reaches itself back"
        );
    }

    #[test]
    fn unknown_symbol_reaches_nothing() {
        let r = index_repository(&files(&[("m.py", "def a():\n    return 1\n")]));
        assert!(change_causality(&r, "sym:doesnotexist").is_empty());
    }
}
