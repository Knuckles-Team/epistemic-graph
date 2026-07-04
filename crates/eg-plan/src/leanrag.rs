//! # leanrag — structured hierarchical retrieval (CONCEPT:EG-KG.retrieval.bounded-drill)
//!
//! Meet/exceed the **LeanRAG** method: instead of a flat vector top-k over every
//! leaf memory — which clusters all its budget around the single nearest topic and
//! returns near-duplicate neighbours — retrieve at the **summary / abstraction
//! level** first, then **drill down** the provenance edges to the supporting leaves.
//! The result is a concise, de-duplicated, MULTI-LEVEL context that covers more
//! distinct topics for the same budget than flat top-k can.
//!
//! ## Where the tiers come from (read-only over the existing graph)
//!
//! This module consumes the **EG-220 summary-node tier** already built in eg-core:
//! `create_summary_node` links a `:SummaryNode` (marked `type = "SummaryNode"`,
//! `summary_level`) to its children via `SUMMARIZES` provenance edges, and EG-221
//! `consolidate` adds `CONSOLIDATES` edges. We NEVER write the graph — we read it
//! through the [`GraphTopology`] accessor (labels + provenance children +
//! embeddings) and retrieve candidates through the [`AnnIndex`] accessor. Both are
//! small traits so this module is pure-Rust, dep-free (Pi-tier clean), and testable
//! against an in-memory fixture without linking DataFusion / the real
//! `SemanticStore`. A production caller implements them over `GraphView` +
//! `eg_ann::IvfPq` / `SemanticStore`.
//!
//! ## The algorithm (CONCEPT:EG-KG.retrieval.bounded-drill)
//!
//!  1. **(a) retrieve at the summary level** — [`AnnIndex::search`] restricted to
//!     summary-labelled nodes for the top-`k` summaries by embedding similarity. If
//!     the graph has no summary tier yet we fall back to an unrestricted search so
//!     the retriever degrades gracefully to flat behaviour.
//!  2. **(b) drill down** — from each retrieved summary, a bounded BFS over
//!     `SUMMARIZES`/`CONSOLIDATES` children (at most `drill_depth` levels, keeping
//!     the `drill_breadth` most query-relevant children per node) collects the
//!     supporting leaf memories.
//!  3. **(c) assemble** — de-duplicate leaves shared across summaries (keeping the
//!     best score), keep only the `leaf_budget` most relevant, and emit a
//!     relevance-ordered flattened context (summaries + chosen leaves).
//!
//! This is a **library API** (structs + traits + functions), NOT a wire `Op` — a
//! caller composes it above the planner, the same way LeanRAG sits above a vector DB.

use std::collections::HashSet;

/// A node id paired with a relevance score (higher = more relevant to the query).
#[derive(Clone, Debug, PartialEq)]
pub struct Scored {
    pub id: String,
    pub score: f32,
}

/// The vector-retrieval accessor (CONCEPT:EG-KG.retrieval.bounded-drill). Production wraps `eg_ann::IvfPq`
/// / the `SemanticStore` kNN; the fixture wraps an in-memory `id -> embedding` map.
///
/// `allow`, when `Some`, restricts the candidate set (e.g. "summary-labelled only")
/// — mirroring `eg_ann::IvfPq::search_filtered`'s allow-predicate (CONCEPT:EG-KG.retrieval.hybrid-metadata-prefilter),
/// so a real implementation forwards straight to it with zero extra work.
pub trait AnnIndex {
    /// Top-`k` `(id, score)` nearest the `query` embedding, score DESC (higher =
    /// nearer). When `allow` is `Some`, only ids satisfying it are returned.
    fn search(&self, query: &[f32], k: usize, allow: Option<&dyn Fn(&str) -> bool>) -> Vec<Scored>;
}

/// The read-only graph accessor (CONCEPT:EG-KG.retrieval.bounded-drill): labels, provenance children, and
/// per-node embeddings. Production reads a `GraphView` (node property blob `type`,
/// the `SUMMARIZES`/`CONSOLIDATES` out-edges, the stored embedding); the fixture
/// reads plain maps. We do NOT edit eg-core — everything here is a read.
pub trait GraphTopology {
    /// The node's structural label (e.g. `"SummaryNode"`), if any.
    fn label(&self, id: &str) -> Option<String>;

    /// The provenance children of `id` — the nodes reached via `SUMMARIZES` /
    /// `CONSOLIDATES` out-edges. A leaf memory returns an empty vec.
    fn children(&self, id: &str) -> Vec<String>;

    /// The node's embedding, used to score drilled children against the query. A
    /// node with no embedding is treated as minimally relevant.
    fn embedding(&self, id: &str) -> Option<Vec<f32>>;
}

/// The retrieval budget knobs (CONCEPT:EG-KG.retrieval.bounded-drill).
#[derive(Clone, Copy, Debug)]
pub struct RetrievalParams {
    /// Summaries to retrieve at the abstraction level (step a).
    pub k: usize,
    /// How many provenance levels to drill below each summary (step b).
    pub drill_depth: usize,
    /// The most query-relevant children expanded per node per level (step b).
    pub drill_breadth: usize,
    /// Cap on the supporting leaves kept in the final context (step c).
    pub leaf_budget: usize,
}

impl Default for RetrievalParams {
    fn default() -> Self {
        Self {
            k: 5,
            drill_depth: 2,
            drill_breadth: 3,
            leaf_budget: 8,
        }
    }
}

/// A structured hierarchical result (CONCEPT:EG-KG.retrieval.bounded-drill).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct HierResult {
    /// The chosen summary nodes, in retrieval-relevance order.
    pub summaries: Vec<Scored>,
    /// The de-duplicated supporting leaves, best-relevance first, capped to budget.
    pub leaves: Vec<Scored>,
    /// The relevance-ordered flattened context (summaries + chosen leaves), each id
    /// appearing at most once — what an LLM prompt-assembler consumes downstream.
    pub context: Vec<Scored>,
}

impl HierResult {
    /// The flattened context ids in order — the convenience most callers want.
    pub fn context_ids(&self) -> Vec<String> {
        self.context.iter().map(|s| s.id.clone()).collect()
    }
}

/// The structured hierarchical retriever (CONCEPT:EG-KG.retrieval.bounded-drill) — LeanRAG over the
/// EG-220 summary tier. Holds only borrowed accessors, so it is cheap to construct
/// per query.
pub struct HierarchicalRetriever<'a> {
    ann: &'a dyn AnnIndex,
    graph: &'a dyn GraphTopology,
    /// Labels treated as "summary / abstraction level" (default: `["SummaryNode"]`).
    summary_labels: Vec<String>,
}

impl<'a> HierarchicalRetriever<'a> {
    /// New retriever over an ANN index + a read-only graph accessor. Summary tier is
    /// the eg-core `:SummaryNode` label by default.
    pub fn new(ann: &'a dyn AnnIndex, graph: &'a dyn GraphTopology) -> Self {
        Self {
            ann,
            graph,
            summary_labels: vec!["SummaryNode".to_string()],
        }
    }

    /// Override which labels count as the abstraction level (e.g. a multi-tier
    /// hierarchy `["SummaryNode", "Community"]`).
    pub fn with_summary_labels<I: IntoIterator<Item = String>>(mut self, labels: I) -> Self {
        self.summary_labels = labels.into_iter().collect();
        self
    }

    fn is_summary(&self, id: &str) -> bool {
        match self.graph.label(id) {
            Some(l) => self.summary_labels.iter().any(|s| s == &l),
            None => false,
        }
    }

    /// The full hierarchical retrieve (CONCEPT:EG-KG.retrieval.bounded-drill): summary-level retrieval →
    /// bounded provenance drill-down → de-duplicated, relevance-ordered multi-level
    /// context. See the module docs for the three steps.
    pub fn retrieve(&self, query: &[f32], params: RetrievalParams) -> HierResult {
        // (a) Retrieve at the summary / abstraction level. If the tier is absent the
        // allow-filter returns nothing, so fall back to an unrestricted search — the
        // retriever degrades to flat rather than returning empty.
        let is_summary = |id: &str| self.is_summary(id);
        let mut summaries = self.ann.search(query, params.k, Some(&is_summary));
        if summaries.is_empty() {
            summaries = self.ann.search(query, params.k, None);
        }

        // (b) Drill down each summary's provenance subtree, then (c) merge the
        // supporting leaves across summaries keeping the best score per id (this is
        // the de-dup that flat top-k cannot do — a leaf shared by two summaries is
        // counted ONCE, not twice).
        let summary_ids: HashSet<String> = summaries.iter().map(|s| s.id.clone()).collect();
        let mut best: Vec<Scored> = Vec::new();
        let mut pos: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
        for s in &summaries {
            for cand in self.drill(query, &s.id, params.drill_depth, params.drill_breadth) {
                // A retrieved summary is context in its own right, never a "leaf".
                if summary_ids.contains(&cand.id) {
                    continue;
                }
                match pos.get(&cand.id) {
                    Some(&i) => {
                        if cand.score > best[i].score {
                            best[i].score = cand.score;
                        }
                    }
                    None => {
                        pos.insert(cand.id.clone(), best.len());
                        best.push(cand);
                    }
                }
            }
        }
        best.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        best.truncate(params.leaf_budget);
        let leaves = best;

        // (c) The flattened relevance-ordered context: summaries then their chosen
        // supporting leaves, de-duplicated (a summary that also surfaced as a child
        // stays a summary). Summaries lead — they are the abstraction a reader wants
        // first — and leaves follow in relevance order.
        let mut seen: HashSet<String> = HashSet::new();
        let mut context: Vec<Scored> = Vec::new();
        for s in summaries.iter().chain(leaves.iter()) {
            if seen.insert(s.id.clone()) {
                context.push(s.clone());
            }
        }

        HierResult {
            summaries,
            leaves,
            context,
        }
    }

    /// Bounded provenance drill-down from one summary (CONCEPT:EG-KG.retrieval.bounded-drill step b): a BFS
    /// over `SUMMARIZES`/`CONSOLIDATES` children, at most `depth` levels, keeping the
    /// `breadth` most query-relevant children per node. Returns every visited
    /// descendant with its query relevance (the caller de-dups + caps).
    fn drill(&self, query: &[f32], summary_id: &str, depth: usize, breadth: usize) -> Vec<Scored> {
        let mut out: Vec<Scored> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();
        seen.insert(summary_id.to_string());
        let mut frontier: Vec<String> = vec![summary_id.to_string()];

        for _ in 0..depth {
            let mut next: Vec<String> = Vec::new();
            for node in &frontier {
                // Score this node's children by query relevance and keep the top
                // `breadth` (the bounded fan-out that keeps the context concise).
                let mut kids: Vec<Scored> = self
                    .graph
                    .children(node)
                    .into_iter()
                    .filter(|c| !seen.contains(c))
                    .map(|c| {
                        let score = self.relevance(query, &c);
                        Scored { id: c, score }
                    })
                    .collect();
                kids.sort_by(|a, b| {
                    b.score
                        .partial_cmp(&a.score)
                        .unwrap_or(std::cmp::Ordering::Equal)
                });
                kids.truncate(breadth);
                for k in kids {
                    if seen.insert(k.id.clone()) {
                        next.push(k.id.clone());
                        out.push(k);
                    }
                }
            }
            frontier = next;
            if frontier.is_empty() {
                break;
            }
        }
        out
    }

    /// Cosine similarity of a node's embedding to the query (0.0 when the node has no
    /// embedding — treated as minimally relevant rather than excluded).
    fn relevance(&self, query: &[f32], id: &str) -> f32 {
        match self.graph.embedding(id) {
            Some(v) => cosine(query, &v),
            None => 0.0,
        }
    }
}

/// The NAIVE flat top-k baseline (CONCEPT:EG-KG.retrieval.bounded-drill): a single vector search over the
/// LEAF level, returning the `budget` nearest leaves. This is exactly the RAG
/// behaviour hierarchical retrieval improves on — it has no summary tier and no
/// drill-down, so it spends its whole budget on the neighbours of the single closest
/// topic. Provided so callers (and the tests) can measure the coverage/redundancy
/// win directly.
pub fn flat_top_k(
    ann: &dyn AnnIndex,
    graph: &dyn GraphTopology,
    summary_labels: &[String],
    query: &[f32],
    budget: usize,
) -> Vec<Scored> {
    let not_summary = |id: &str| match graph.label(id) {
        Some(l) => !summary_labels.iter().any(|s| s == &l),
        None => true,
    };
    ann.search(query, budget, Some(&not_summary))
}

/// Cosine similarity, guarding zero-norm vectors (returns 0.0). Pure + dep-free.
fn cosine(a: &[f32], b: &[f32]) -> f32 {
    let n = a.len().min(b.len());
    let mut dot = 0.0f32;
    let mut na = 0.0f32;
    let mut nb = 0.0f32;
    for i in 0..n {
        dot += a[i] * b[i];
        na += a[i] * a[i];
        nb += b[i] * b[i];
    }
    if na == 0.0 || nb == 0.0 {
        0.0
    } else {
        dot / (na.sqrt() * nb.sqrt())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// An in-memory fixture implementing BOTH accessors over plain maps — a 2-level
    /// summary→leaves graph with fake embeddings. No eg-core, no DataFusion.
    #[derive(Default)]
    struct Fixture {
        emb: HashMap<String, Vec<f32>>,
        label: HashMap<String, String>,
        kids: HashMap<String, Vec<String>>,
    }

    impl Fixture {
        fn node(&mut self, id: &str, label: Option<&str>, emb: Vec<f32>) {
            if let Some(l) = label {
                self.label.insert(id.to_string(), l.to_string());
            }
            self.emb.insert(id.to_string(), emb);
        }
        fn summarizes(&mut self, parent: &str, child: &str) {
            self.kids
                .entry(parent.to_string())
                .or_default()
                .push(child.to_string());
        }
    }

    impl GraphTopology for Fixture {
        fn label(&self, id: &str) -> Option<String> {
            self.label.get(id).cloned()
        }
        fn children(&self, id: &str) -> Vec<String> {
            self.kids.get(id).cloned().unwrap_or_default()
        }
        fn embedding(&self, id: &str) -> Option<Vec<f32>> {
            self.emb.get(id).cloned()
        }
    }

    /// Brute-force ANN over the fixture's embeddings — the small-scale stand-in for
    /// `eg_ann::IvfPq`. Ranks ALL ids by cosine to the query, applies the allow
    /// filter, returns the top-k. (Correctness-equivalent to the real index; the
    /// real recall bar lives in eg-ann's own tests.)
    impl AnnIndex for Fixture {
        fn search(
            &self,
            query: &[f32],
            k: usize,
            allow: Option<&dyn Fn(&str) -> bool>,
        ) -> Vec<Scored> {
            let mut scored: Vec<Scored> = self
                .emb
                .iter()
                .filter(|(id, _)| allow.map(|f| f(id)).unwrap_or(true))
                .map(|(id, v)| Scored {
                    id: id.clone(),
                    score: cosine(query, v),
                })
                .collect();
            scored.sort_by(|a, b| {
                b.score
                    .partial_cmp(&a.score)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| a.id.cmp(&b.id))
            });
            scored.truncate(k);
            scored
        }
    }

    /// A 3-topic fixture. Each topic Ti has a `:SummaryNode` Si at its axis-aligned
    /// centroid and 4 leaves clustered near it. Topic tag = the id's first char run
    /// so a test can count distinct topics covered. `L_shared` is a child of BOTH S0
    /// and S1 — to prove de-dup.
    fn topic_fixture() -> Fixture {
        let mut f = Fixture::default();
        // Centroids: T0=(1,0,0) T1=(0,1,0) T2=(0,0,1).
        let centroids = [[1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.0, 0.0, 1.0]];
        for (t, c) in centroids.iter().enumerate() {
            f.node(&format!("S{t}"), Some("SummaryNode"), c.to_vec());
            for l in 0..4 {
                // Leaves sit near their centroid with a small deterministic wobble.
                let mut e = *c;
                e[(l as usize) % 3] += 0.02 * (l as f32 + 1.0);
                let id = format!("L{t}_{l}");
                f.node(&id, None, e.to_vec());
                f.summarizes(&format!("S{t}"), &id);
            }
        }
        // A leaf that supports two summaries (shared provenance) — near T0/T1 seam.
        f.node("L_shared", None, vec![0.6, 0.6, 0.0]);
        f.summarizes("S0", "L_shared");
        f.summarizes("S1", "L_shared");
        f
    }

    fn topic_of(id: &str) -> &str {
        // "S0" -> "0", "L2_3" -> "2", "L_shared" -> "shared".
        if let Some(rest) = id.strip_prefix('S') {
            return &rest[..1];
        }
        if let Some(rest) = id.strip_prefix('L') {
            if rest.starts_with('_') {
                return "shared";
            }
            return &rest[..1];
        }
        id
    }

    fn distinct_topics(ids: &[String]) -> usize {
        ids.iter()
            .map(|id| topic_of(id).to_string())
            .collect::<HashSet<_>>()
            .len()
    }

    /// CONCEPT:EG-KG.retrieval.bounded-drill — the retriever pulls the summary tier first, then drills to
    /// the correct supporting leaves (not arbitrary nodes).
    #[test]
    fn eg195_retrieves_summaries_then_drills_to_correct_leaves() {
        let f = topic_fixture();
        let r = HierarchicalRetriever::new(&f, &f);
        // Query tilted toward all three topics but strongest on T0.
        let q = [1.0, 0.9, 0.8];
        let res = r.retrieve(
            &q,
            RetrievalParams {
                k: 3,
                drill_depth: 1,
                drill_breadth: 2,
                leaf_budget: 6,
            },
        );

        // (a) top-3 retrieved nodes are the three SUMMARIES, all summary-labelled.
        assert_eq!(res.summaries.len(), 3, "expected the 3 summary nodes");
        for s in &res.summaries {
            assert!(
                s.id.starts_with('S'),
                "summary tier must be :SummaryNode, got {}",
                s.id
            );
        }

        // (b) every drilled leaf is an actual child of some retrieved summary (a
        // real supporting memory), never an unrelated node.
        let valid_children: HashSet<String> = res
            .summaries
            .iter()
            .flat_map(|s| f.children(&s.id))
            .collect();
        assert!(!res.leaves.is_empty(), "must drill to some leaves");
        for leaf in &res.leaves {
            assert!(
                valid_children.contains(&leaf.id),
                "drilled leaf {} is not a child of any retrieved summary",
                leaf.id
            );
        }
    }

    /// CONCEPT:EG-KG.retrieval.bounded-drill — a leaf shared by two summaries appears exactly ONCE in the
    /// leaves and once in the flattened context (the de-dup flat top-k can't do).
    #[test]
    fn eg195_dedups_shared_leaf_across_summaries() {
        let f = topic_fixture();
        let r = HierarchicalRetriever::new(&f, &f);
        // A query on the T0/T1 seam so BOTH S0 and S1 rank high and both drill into
        // L_shared, and L_shared is relevant enough to survive the budget.
        let q = [0.7, 0.7, 0.0];
        let res = r.retrieve(
            &q,
            RetrievalParams {
                k: 3,
                drill_depth: 1,
                drill_breadth: 5,
                leaf_budget: 12,
            },
        );

        let shared_in_leaves = res.leaves.iter().filter(|s| s.id == "L_shared").count();
        assert_eq!(
            shared_in_leaves, 1,
            "shared leaf must be de-duplicated to a single occurrence, got {shared_in_leaves}"
        );
        let ctx = res.context_ids();
        assert_eq!(
            ctx.iter().filter(|id| *id == "L_shared").count(),
            1,
            "shared leaf must appear once in the flattened context"
        );
        // The whole context is de-duplicated.
        let uniq: HashSet<&String> = ctx.iter().collect();
        assert_eq!(uniq.len(), ctx.len(), "context must be duplicate-free");
    }

    /// CONCEPT:EG-KG.retrieval.bounded-drill — the headline claim: for the SAME budget, hierarchical
    /// retrieval covers MORE distinct topics than flat top-k, which spends its whole
    /// budget on the nearest topic's near-duplicate neighbours.
    #[test]
    fn eg195_hierarchical_beats_flat_topk_on_coverage() {
        let f = topic_fixture();
        let r = HierarchicalRetriever::new(&f, &f);
        // Query near T0 but with real components on T1/T2 — a question that touches
        // all three topics yet leans T0.
        let q = [1.0, 0.85, 0.7];
        let budget = 4;

        // Flat top-k over the leaves: because the T0 leaves all beat every T1/T2
        // leaf on cosine, the whole budget is spent inside T0.
        let flat = flat_top_k(&f, &f, &["SummaryNode".to_string()], &q, budget);
        let flat_ids: Vec<String> = flat.iter().map(|s| s.id.clone()).collect();
        let flat_topics = distinct_topics(&flat_ids);

        // Hierarchical with the SAME total budget: k summaries + drilled leaves,
        // capped so summaries + leaves ~= budget worth of context.
        let res = r.retrieve(
            &q,
            RetrievalParams {
                k: 3,
                drill_depth: 1,
                drill_breadth: 1,
                leaf_budget: budget.saturating_sub(3),
            },
        );
        let hier_topics = distinct_topics(&res.context_ids());

        assert!(
            flat_topics < 3,
            "flat top-k should MISS at least one topic (its budget collapses onto the \
             nearest topics' near-duplicates), yet covered {flat_topics}: {flat_ids:?}"
        );
        assert!(
            hier_topics >= 3,
            "hierarchical must cover all 3 topics for the same budget, covered {hier_topics}: {:?}",
            res.context_ids()
        );
        assert!(
            hier_topics > flat_topics,
            "hierarchical ({hier_topics}) must beat flat ({flat_topics}) on topic coverage"
        );
    }

    /// CONCEPT:EG-KG.retrieval.bounded-drill — bounded drill: `drill_depth`/`drill_breadth` cap the fan-out
    /// so the context stays concise instead of pulling every descendant.
    #[test]
    fn eg195_drill_breadth_and_depth_are_bounded() {
        let f = topic_fixture();
        let r = HierarchicalRetriever::new(&f, &f);
        let q = [1.0, 0.0, 0.0];
        // breadth=1, so at most one child per summary is drilled; k=1 summary → <=1
        // leaf even though S0 has 5 children.
        let res = r.retrieve(
            &q,
            RetrievalParams {
                k: 1,
                drill_depth: 1,
                drill_breadth: 1,
                leaf_budget: 10,
            },
        );
        assert_eq!(res.summaries.len(), 1);
        assert!(
            res.leaves.len() <= 1,
            "breadth=1 must cap to one drilled child, got {}",
            res.leaves.len()
        );
    }

    /// CONCEPT:EG-KG.retrieval.bounded-drill — graceful degradation: with NO summary tier the retriever
    /// falls back to an unrestricted search rather than returning empty.
    #[test]
    fn eg195_falls_back_when_no_summary_tier() {
        let mut f = Fixture::default();
        // Only leaves, no :SummaryNode labels anywhere.
        f.node("A", None, vec![1.0, 0.0]);
        f.node("B", None, vec![0.0, 1.0]);
        let r = HierarchicalRetriever::new(&f, &f);
        let res = r.retrieve(&[1.0, 0.1], RetrievalParams::default());
        assert!(
            !res.summaries.is_empty(),
            "must fall back to a flat unrestricted search when the summary tier is absent"
        );
        assert_eq!(res.summaries[0].id, "A", "nearest node should lead");
    }
}
