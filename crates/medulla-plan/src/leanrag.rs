//! # leanrag — structured hierarchical retrieval (CONCEPT:MEDULLA-KG.retrieval.bounded-drill)
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
//! This module consumes the **MEDULLA-220 summary-node tier** already built in medulla-core:
//! `create_summary_node` links a `:SummaryNode` (marked `type = "SummaryNode"`,
//! `summary_level`) to its children via `SUMMARIZES` provenance edges, and MEDULLA-221
//! `consolidate` adds `CONSOLIDATES` edges. We NEVER write the graph — we read it
//! through the [`GraphTopology`] accessor (labels + provenance children +
//! embeddings) and retrieve candidates through the [`AnnIndex`] accessor. Both are
//! small traits so this module is pure-Rust, dep-free (Pi-tier clean), and testable
//! against an in-memory fixture without linking DataFusion / the real
//! `SemanticStore`. A production caller implements them over `GraphView` +
//! `medulla_ann::IvfPq` / `SemanticStore`.
//!
//! ## The algorithm (CONCEPT:MEDULLA-KG.retrieval.bounded-drill)
//!
//!  1. **(a) retrieve at the summary level** — [`AnnIndex::search`] restricted to
//!     summary-labelled nodes for the top-`k` summaries by embedding similarity. If
//!     the graph has no summary tier the retriever returns
//!     [`VectorFreeError::SummaryTierAbsent`] (`RETRIEVAL_HIERARCHY_INVALID`,
//!     `MEDULLA-FEDERATED-QUERY-R081.9.4`); it never replaces a failed descent with an
//!     unrestricted global search. [`flat_top_k`] is the explicit flat baseline.
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

use std::cmp::Ordering;
use std::collections::{HashSet, VecDeque};

/// A node id paired with a relevance score (higher = more relevant to the query).
#[derive(Clone, Debug, PartialEq)]
pub struct Scored {
    pub id: String,
    pub score: f32,
}

/// The vector-retrieval accessor (CONCEPT:MEDULLA-KG.retrieval.bounded-drill). Production wraps `medulla_ann::IvfPq`
/// / the `SemanticStore` kNN; the fixture wraps an in-memory `id -> embedding` map.
///
/// `allow`, when `Some`, restricts the candidate set (e.g. "summary-labelled only")
/// — mirroring `medulla_ann::IvfPq::search_filtered`'s allow-predicate (CONCEPT:MEDULLA-KG.retrieval.hybrid-metadata-prefilter),
/// so a real implementation forwards straight to it with zero extra work.
pub trait AnnIndex {
    /// Top-`k` `(id, score)` nearest the `query` embedding, score DESC (higher =
    /// nearer). When `allow` is `Some`, only ids satisfying it are returned.
    fn search(&self, query: &[f32], k: usize, allow: Option<&dyn Fn(&str) -> bool>) -> Vec<Scored>;
}

/// The read-only graph accessor (CONCEPT:MEDULLA-KG.retrieval.bounded-drill): labels, provenance children, and
/// per-node embeddings. Production reads a `GraphView` (node property blob `type`,
/// the `SUMMARIZES`/`CONSOLIDATES` out-edges, the stored embedding); the fixture
/// reads plain maps. We do NOT edit medulla-core — everything here is a read.
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

/// The stored granularity requested by the vector-free hierarchy mode.
///
/// This mode deliberately has no embedding or model input.  The caller supplies
/// already-authorized section records and the traversal returns the stored
/// summary/span at the requested level.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SectionGranularity {
    Document,
    Section,
    SourceSpan,
}

/// A half-open source range carried by one immutable section record.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SectionSpan {
    pub start: u64,
    pub end: u64,
}

impl SectionSpan {
    fn contains(self, child: Self) -> bool {
        self.start <= child.start && child.start <= child.end && child.end <= self.end
    }
}

/// The structural, source-bound data needed by vector-free hierarchy traversal.
///
/// The record contains the already-authorized stored summary, immutable source
/// span and their revision-bound digests; generation remains outside this
/// read-only traversal. `children` order is preserved by the traversal and is
/// the deterministic tie order when no vector score is available.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SectionRecord {
    pub document_id: String,
    pub document_revision: String,
    pub section_id: String,
    pub parent_id: Option<String>,
    pub children: Vec<String>,
    pub granularity: SectionGranularity,
    pub source_span: SectionSpan,
    pub summary: String,
    pub source_digest: String,
    pub summary_digest: String,
}

/// Read-only accessor for records that have already passed the caller's
/// authorization boundary.  This trait has no embedding, model or write
/// operation, which makes vector-free traversal explicit at the type boundary.
pub trait SectionTopology {
    fn section(&self, section_id: &str) -> Option<SectionRecord>;
}

/// Finite traversal limits for the vector-free hierarchy path.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VectorFreeLimits {
    pub max_depth: usize,
    pub max_visited: usize,
}

impl Default for VectorFreeLimits {
    fn default() -> Self {
        Self {
            max_depth: 32,
            max_visited: 4096,
        }
    }
}

/// The finite traversal dimension that stopped a vector-free request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TraversalLimit {
    Depth,
    Visited,
}

/// Result of a vector-free walk. `paths[i]` is the ordered root-to-record path
/// for `selected[i]`. Finite limit exhaustion is returned as a typed refusal
/// before the next child is read.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VectorFreeResult {
    pub selected: Vec<SectionRecord>,
    pub paths: Vec<Vec<String>>,
}

/// Refusals from structural validation or an invalid vector-free request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum VectorFreeError {
    InvalidLimits,
    MissingRecord(String),
    RootHasParent(String),
    DocumentMismatch {
        section_id: String,
        expected: String,
        actual: String,
    },
    RevisionMismatch {
        section_id: String,
        expected: String,
        actual: String,
    },
    ParentMismatch {
        section_id: String,
        expected: String,
        actual: Option<String>,
    },
    InvalidRecord(String),
    ChildOutsideParent {
        parent_id: String,
        child_id: String,
    },
    DuplicateOrCycle(String),
    TraversalLimitExceeded(TraversalLimit),
    NoMatchingGranularity(SectionGranularity),
    /// The retriever found no summary-tier node, so the hierarchical descent has no
    /// start. Wire code `RETRIEVAL_HIERARCHY_INVALID` (`MEDULLA-FEDERATED-QUERY-R081.9.4`).
    SummaryTierAbsent,
}

fn validate_section_record(record: &SectionRecord) -> Result<(), VectorFreeError> {
    if record.document_id.is_empty()
        || record.document_revision.is_empty()
        || record.section_id.is_empty()
        || record.summary.is_empty()
        || record.source_digest.is_empty()
        || record.summary_digest.is_empty()
        || record.source_span.start >= record.source_span.end
    {
        return Err(VectorFreeError::InvalidRecord(record.section_id.clone()));
    }
    Ok(())
}

fn read_root_section(
    topology: &dyn SectionTopology,
    root_id: &str,
    document_revision: &str,
) -> Result<String, VectorFreeError> {
    let root = topology
        .section(root_id)
        .ok_or_else(|| VectorFreeError::MissingRecord(root_id.to_string()))?;
    validate_section_record(&root)?;
    if root.section_id != root_id {
        return Err(VectorFreeError::InvalidRecord(root_id.to_string()));
    }
    if root.parent_id.is_some() {
        return Err(VectorFreeError::RootHasParent(root.section_id.clone()));
    }
    if root.document_revision != document_revision {
        return Err(VectorFreeError::RevisionMismatch {
            section_id: root.section_id.clone(),
            expected: document_revision.to_string(),
            actual: root.document_revision.clone(),
        });
    }
    Ok(root.document_id.clone())
}

fn read_section(
    topology: &dyn SectionTopology,
    section_id: &str,
    document_id: &str,
    document_revision: &str,
    expected_parent: Option<&str>,
) -> Result<SectionRecord, VectorFreeError> {
    let record = topology
        .section(section_id)
        .ok_or_else(|| VectorFreeError::MissingRecord(section_id.to_string()))?;
    validate_section_record(&record)?;
    if record.section_id != section_id {
        return Err(VectorFreeError::InvalidRecord(section_id.to_string()));
    }
    if record.document_id != document_id {
        return Err(VectorFreeError::DocumentMismatch {
            section_id: section_id.to_string(),
            expected: document_id.to_string(),
            actual: record.document_id,
        });
    }
    if record.document_revision != document_revision {
        return Err(VectorFreeError::RevisionMismatch {
            section_id: section_id.to_string(),
            expected: document_revision.to_string(),
            actual: record.document_revision,
        });
    }
    if let Some(parent_id) = expected_parent {
        if record.parent_id.as_deref() != Some(parent_id) {
            return Err(VectorFreeError::ParentMismatch {
                section_id: section_id.to_string(),
                expected: parent_id.to_string(),
                actual: record.parent_id,
            });
        }
    }
    Ok(record)
}

struct VectorFreeContext<'a> {
    topology: &'a dyn SectionTopology,
    document_id: &'a str,
    document_revision: &'a str,
    limits: VectorFreeLimits,
}

fn validate_child(
    context: &VectorFreeContext<'_>,
    child_id: &str,
    parent: &SectionRecord,
    queued: &HashSet<String>,
) -> Result<(), VectorFreeError> {
    if queued.contains(child_id) {
        return Err(VectorFreeError::DuplicateOrCycle(child_id.to_string()));
    }
    if queued.len() >= context.limits.max_visited {
        return Err(VectorFreeError::TraversalLimitExceeded(
            TraversalLimit::Visited,
        ));
    }
    let child = context
        .topology
        .section(child_id)
        .ok_or_else(|| VectorFreeError::MissingRecord(child_id.to_string()))?;
    validate_section_record(&child)?;
    if child.section_id != child_id {
        return Err(VectorFreeError::InvalidRecord(child_id.to_string()));
    }
    if child.document_id != context.document_id {
        return Err(VectorFreeError::DocumentMismatch {
            section_id: child.section_id.clone(),
            expected: context.document_id.to_string(),
            actual: child.document_id.clone(),
        });
    }
    if child.document_revision != context.document_revision {
        return Err(VectorFreeError::RevisionMismatch {
            section_id: child.section_id.clone(),
            expected: context.document_revision.to_string(),
            actual: child.document_revision.clone(),
        });
    }
    if child.parent_id.as_deref() != Some(parent.section_id.as_str()) {
        return Err(VectorFreeError::ParentMismatch {
            section_id: child.section_id.clone(),
            expected: parent.section_id.clone(),
            actual: child.parent_id.clone(),
        });
    }
    if !parent.source_span.contains(child.source_span) {
        return Err(VectorFreeError::ChildOutsideParent {
            parent_id: parent.section_id.clone(),
            child_id: child.section_id.clone(),
        });
    }
    Ok(())
}

fn enqueue_children(
    context: &VectorFreeContext<'_>,
    record: &SectionRecord,
    path: &[String],
    depth: usize,
    queued: &mut HashSet<String>,
    queue: &mut VecDeque<(String, Option<String>, Vec<String>, usize)>,
) -> Result<(), VectorFreeError> {
    for child_id in &record.children {
        validate_child(context, child_id, record, queued)?;
        queued.insert(child_id.clone());
        let mut child_path = path.to_vec();
        child_path.push(child_id.clone());
        queue.push_back((
            child_id.clone(),
            Some(record.section_id.clone()),
            child_path,
            depth + 1,
        ));
    }
    Ok(())
}

/// Traverse an authorized document-section tree without vectors or external
/// inference calls.  The result is structural-only: callers decide how to
/// attach this seam to the authenticated `UnifiedQuery`/`PlanCtx` path.
pub fn retrieve_vector_free(
    topology: &dyn SectionTopology,
    root_id: &str,
    document_revision: &str,
    granularity: SectionGranularity,
    limits: VectorFreeLimits,
) -> Result<VectorFreeResult, VectorFreeError> {
    if root_id.is_empty()
        || document_revision.is_empty()
        || limits.max_depth == 0
        || limits.max_visited == 0
    {
        return Err(VectorFreeError::InvalidLimits);
    }

    let document_id = read_root_section(topology, root_id, document_revision)?;
    let context = VectorFreeContext {
        topology,
        document_id: &document_id,
        document_revision,
        limits,
    };
    let mut queue: VecDeque<(String, Option<String>, Vec<String>, usize)> = VecDeque::new();
    queue.push_back((root_id.to_string(), None, vec![root_id.to_string()], 0));
    let mut queued = HashSet::from([root_id.to_string()]);
    let mut selected = Vec::new();
    let mut paths = Vec::new();

    while let Some((section_id, expected_parent, path, depth)) = queue.pop_front() {
        let record = read_section(
            topology,
            &section_id,
            context.document_id,
            document_revision,
            expected_parent.as_deref(),
        )?;

        if record.granularity == granularity {
            selected.push(record.clone());
            paths.push(path.clone());
        }

        if depth >= limits.max_depth {
            if !record.children.is_empty() {
                return Err(VectorFreeError::TraversalLimitExceeded(
                    TraversalLimit::Depth,
                ));
            }
            continue;
        }
        enqueue_children(&context, &record, &path, depth, &mut queued, &mut queue)?;
    }

    if selected.is_empty() {
        return Err(VectorFreeError::NoMatchingGranularity(granularity));
    }
    Ok(VectorFreeResult { selected, paths })
}

/// The retrieval budget knobs (CONCEPT:MEDULLA-KG.retrieval.bounded-drill).
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

/// A structured hierarchical result (CONCEPT:MEDULLA-KG.retrieval.bounded-drill).
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

/// The structured hierarchical retriever (CONCEPT:MEDULLA-KG.retrieval.bounded-drill) — LeanRAG over the
/// MEDULLA-220 summary tier. Holds only borrowed accessors, so it is cheap to construct
/// per query.
pub struct HierarchicalRetriever<'a> {
    ann: &'a dyn AnnIndex,
    graph: &'a dyn GraphTopology,
    /// Labels treated as "summary / abstraction level" (default: `["SummaryNode"]`).
    summary_labels: Vec<String>,
}

impl<'a> HierarchicalRetriever<'a> {
    /// New retriever over an ANN index + a read-only graph accessor. Summary tier is
    /// the medulla-core `:SummaryNode` label by default.
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

    /// The full hierarchical retrieve (CONCEPT:MEDULLA-KG.retrieval.bounded-drill): summary-level retrieval →
    /// bounded provenance drill-down → de-duplicated, relevance-ordered multi-level
    /// context. See the module docs for the three steps.
    pub fn retrieve(
        &self,
        query: &[f32],
        params: RetrievalParams,
    ) -> Result<HierResult, VectorFreeError> {
        // (a) Summary-level retrieval.
        let summaries = self.retrieve_summaries(query, params.k)?;
        // (b) Bounded provenance drill-down + de-duplicated leaf merge.
        let summary_ids: HashSet<String> = summaries.iter().map(|s| s.id.clone()).collect();
        let leaves = self.merge_drilled_leaves(query, &summaries, &summary_ids, params);
        // (c) Flattened, de-duplicated, relevance-ordered context.
        let context = flatten_context(&summaries, &leaves);

        Ok(HierResult {
            summaries,
            leaves,
            context,
        })
    }

    /// Step (a): retrieve at the summary / abstraction level. If the tier is absent the
    /// allow-filter returns nothing and the walk refuses with
    /// [`VectorFreeError::SummaryTierAbsent`]; it never falls back to an unrestricted
    /// search (`MEDULLA-FEDERATED-QUERY-R081.9.4`).
    fn retrieve_summaries(&self, query: &[f32], k: usize) -> Result<Vec<Scored>, VectorFreeError> {
        let is_summary = |id: &str| self.is_summary(id);
        let summaries = self.ann.search(query, k, Some(&is_summary));
        if summaries.is_empty() {
            Err(VectorFreeError::SummaryTierAbsent)
        } else {
            Ok(summaries)
        }
    }

    /// Step (b): drill down each summary's provenance subtree, then merge the
    /// supporting leaves across summaries keeping the best score per id (this is the
    /// de-dup that flat top-k cannot do — a leaf shared by two summaries is counted
    /// ONCE, not twice).
    fn merge_drilled_leaves(
        &self,
        query: &[f32],
        summaries: &[Scored],
        summary_ids: &HashSet<String>,
        params: RetrievalParams,
    ) -> Vec<Scored> {
        let mut best: Vec<Scored> = Vec::new();
        let mut pos: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
        for s in summaries {
            for cand in self.drill(query, &s.id, params.drill_depth, params.drill_breadth) {
                // A retrieved summary is context in its own right, never a "leaf".
                if summary_ids.contains(&cand.id) {
                    continue;
                }
                merge_scored_candidate(&mut best, &mut pos, cand);
            }
        }
        truncate_highest_scored(&mut best, params.leaf_budget);
        best
    }

    /// Bounded provenance drill-down from one summary (CONCEPT:MEDULLA-KG.retrieval.bounded-drill step b): a BFS
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
                truncate_highest_scored(&mut kids, breadth);
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

/// Insert `cand` into `best` keyed by id, keeping the higher of the two scores when
/// the id is already present — the merge step of [`HierarchicalRetriever::retrieve`]'s
/// leaf de-dup.
fn merge_scored_candidate(
    best: &mut Vec<Scored>,
    pos: &mut std::collections::HashMap<String, usize>,
    cand: Scored,
) {
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

/// The flattened relevance-ordered context: summaries then their chosen supporting
/// leaves, de-duplicated (a summary that also surfaced as a child stays a summary).
/// Summaries lead — they are the abstraction a reader wants first — and leaves follow
/// in relevance order.
fn flatten_context(summaries: &[Scored], leaves: &[Scored]) -> Vec<Scored> {
    let mut seen: HashSet<String> = HashSet::new();
    let mut context: Vec<Scored> = Vec::new();
    for s in summaries.iter().chain(leaves.iter()) {
        if seen.insert(s.id.clone()) {
            context.push(s.clone());
        }
    }
    context
}

/// Keep the exact highest-scoring prefix under a deterministic total order:
/// score descending, id ascending, NaN last. Expected O(N) selection plus
/// O(k log k) to order only the retained context budget.
#[inline]
fn truncate_highest_scored(values: &mut Vec<Scored>, limit: usize) {
    if limit == 0 {
        values.clear();
        return;
    }
    if values.len() > limit {
        values.select_nth_unstable_by(limit, scored_cmp);
        values.truncate(limit);
    }
    values.sort_unstable_by(scored_cmp);
}

#[inline]
fn scored_cmp(left: &Scored, right: &Scored) -> Ordering {
    let score_order = match (left.score.is_nan(), right.score.is_nan()) {
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        (true, true) => left.score.to_bits().cmp(&right.score.to_bits()),
        (false, false) => right.score.total_cmp(&left.score),
    };
    score_order.then_with(|| left.id.cmp(&right.id))
}

/// The NAIVE flat top-k baseline (CONCEPT:MEDULLA-KG.retrieval.bounded-drill): a single vector search over the
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
    /// summary→leaves graph with fake embeddings. No medulla-core, no DataFusion.
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
    /// `medulla_ann::IvfPq`. Ranks ALL ids by cosine to the query, applies the allow
    /// filter, returns the top-k. (Correctness-equivalent to the real index; the
    /// real recall bar lives in medulla-ann's own tests.)
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
            truncate_highest_scored(&mut scored, k);
            scored
        }
    }

    /// Read-only section fixture for the vector-free mode.  It intentionally
    /// implements only `SectionTopology`: a successful walk has no embedding or
    /// model accessor available to call.
    #[derive(Default)]
    struct SectionFixture {
        records: HashMap<String, SectionRecord>,
    }

    impl SectionFixture {
        fn add(&mut self, record: SectionRecord) {
            self.records.insert(record.section_id.clone(), record);
        }
    }

    impl SectionTopology for SectionFixture {
        fn section(&self, section_id: &str) -> Option<SectionRecord> {
            self.records.get(section_id).cloned()
        }
    }

    fn section_record(
        id: &str,
        parent: Option<&str>,
        children: &[&str],
        granularity: SectionGranularity,
        start: u64,
        end: u64,
        revision: &str,
    ) -> SectionRecord {
        SectionRecord {
            document_id: "doc-1".into(),
            document_revision: revision.into(),
            section_id: id.into(),
            parent_id: parent.map(str::to_owned),
            children: children.iter().map(|child| (*child).into()).collect(),
            granularity,
            source_span: SectionSpan { start, end },
            summary: format!("summary:{id}"),
            source_digest: format!("source:{id}"),
            summary_digest: format!("summary:{id}"),
        }
    }

    fn nested_section_fixture() -> SectionFixture {
        let mut fixture = SectionFixture::default();
        fixture.add(section_record(
            "doc-1",
            None,
            &["intro", "appendix"],
            SectionGranularity::Document,
            0,
            100,
            "rev-1",
        ));
        fixture.add(section_record(
            "intro",
            Some("doc-1"),
            &["intro-a", "intro-b"],
            SectionGranularity::Section,
            0,
            60,
            "rev-1",
        ));
        fixture.add(section_record(
            "appendix",
            Some("doc-1"),
            &[],
            SectionGranularity::Section,
            60,
            100,
            "rev-1",
        ));
        fixture.add(section_record(
            "intro-a",
            Some("intro"),
            &[],
            SectionGranularity::SourceSpan,
            0,
            30,
            "rev-1",
        ));
        fixture.add(section_record(
            "intro-b",
            Some("intro"),
            &[],
            SectionGranularity::SourceSpan,
            30,
            60,
            "rev-1",
        ));
        fixture
    }

    #[test]
    fn vector_free_walk_returns_each_granularity_and_full_paths() {
        let fixture = nested_section_fixture();
        let limits = VectorFreeLimits {
            max_depth: 4,
            max_visited: 16,
        };

        let documents = retrieve_vector_free(
            &fixture,
            "doc-1",
            "rev-1",
            SectionGranularity::Document,
            limits,
        )
        .expect("document walk should succeed");
        assert_eq!(
            documents
                .selected
                .iter()
                .map(|record| record.section_id.as_str())
                .collect::<Vec<_>>(),
            vec!["doc-1"]
        );
        assert_eq!(documents.paths, vec![vec!["doc-1".to_string()]]);

        let sections = retrieve_vector_free(
            &fixture,
            "doc-1",
            "rev-1",
            SectionGranularity::Section,
            limits,
        )
        .expect("section walk should succeed");
        assert_eq!(
            sections
                .selected
                .iter()
                .map(|record| record.section_id.as_str())
                .collect::<Vec<_>>(),
            vec!["intro", "appendix"]
        );
        assert_eq!(
            sections.paths,
            vec![
                vec!["doc-1".to_string(), "intro".to_string()],
                vec!["doc-1".to_string(), "appendix".to_string()],
            ]
        );

        let spans = retrieve_vector_free(
            &fixture,
            "doc-1",
            "rev-1",
            SectionGranularity::SourceSpan,
            limits,
        )
        .expect("source-span walk should succeed");
        assert_eq!(
            spans
                .selected
                .iter()
                .map(|record| (record.section_id.as_str(), record.source_span))
                .collect::<Vec<_>>(),
            vec![
                ("intro-a", SectionSpan { start: 0, end: 30 }),
                ("intro-b", SectionSpan { start: 30, end: 60 }),
            ]
        );
        assert_eq!(spans.selected[0].summary, "summary:intro-a");
        assert_eq!(
            spans.paths,
            vec![
                vec![
                    "doc-1".to_string(),
                    "intro".to_string(),
                    "intro-a".to_string()
                ],
                vec![
                    "doc-1".to_string(),
                    "intro".to_string(),
                    "intro-b".to_string()
                ],
            ]
        );
    }

    #[test]
    fn vector_free_walk_refuses_stale_revision_and_outside_span() {
        let mut stale = nested_section_fixture();
        stale.records.get_mut("intro-b").unwrap().document_revision = "rev-2".into();
        assert_eq!(
            retrieve_vector_free(
                &stale,
                "doc-1",
                "rev-1",
                SectionGranularity::SourceSpan,
                VectorFreeLimits::default(),
            ),
            Err(VectorFreeError::RevisionMismatch {
                section_id: "intro-b".into(),
                expected: "rev-1".into(),
                actual: "rev-2".into(),
            })
        );

        let mut outside = nested_section_fixture();
        outside.records.get_mut("intro-b").unwrap().source_span =
            SectionSpan { start: 60, end: 70 };
        assert_eq!(
            retrieve_vector_free(
                &outside,
                "doc-1",
                "rev-1",
                SectionGranularity::SourceSpan,
                VectorFreeLimits::default(),
            ),
            Err(VectorFreeError::ChildOutsideParent {
                parent_id: "intro".into(),
                child_id: "intro-b".into(),
            })
        );
    }

    #[test]
    fn vector_free_walk_stops_before_depth_or_visit_overflow() {
        let fixture = nested_section_fixture();
        assert_eq!(
            retrieve_vector_free(
                &fixture,
                "doc-1",
                "rev-1",
                SectionGranularity::SourceSpan,
                VectorFreeLimits {
                    max_depth: 1,
                    max_visited: 16,
                },
            ),
            Err(VectorFreeError::TraversalLimitExceeded(
                TraversalLimit::Depth
            ))
        );
        assert_eq!(
            retrieve_vector_free(
                &fixture,
                "doc-1",
                "rev-1",
                SectionGranularity::Section,
                VectorFreeLimits {
                    max_depth: 4,
                    max_visited: 2,
                },
            ),
            Err(VectorFreeError::TraversalLimitExceeded(
                TraversalLimit::Visited
            ))
        );
    }

    #[test]
    fn vector_free_walk_refuses_cycles_and_invalid_limits() {
        let mut cycle = nested_section_fixture();
        cycle
            .records
            .get_mut("intro-b")
            .unwrap()
            .children
            .push("doc-1".into());
        assert_eq!(
            retrieve_vector_free(
                &cycle,
                "doc-1",
                "rev-1",
                SectionGranularity::SourceSpan,
                VectorFreeLimits::default(),
            ),
            Err(VectorFreeError::DuplicateOrCycle("doc-1".into()))
        );

        assert_eq!(
            retrieve_vector_free(
                &cycle,
                "doc-1",
                "rev-1",
                SectionGranularity::SourceSpan,
                VectorFreeLimits {
                    max_depth: 4,
                    max_visited: 0,
                },
            ),
            Err(VectorFreeError::InvalidLimits)
        );
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

    #[test]
    fn bounded_score_selection_matches_total_full_sort() {
        let input = vec![
            Scored {
                id: "z".into(),
                score: 0.5,
            },
            Scored {
                id: "b".into(),
                score: 1.0,
            },
            Scored {
                id: "a".into(),
                score: 1.0,
            },
            Scored {
                id: "nan".into(),
                score: f32::NAN,
            },
            Scored {
                id: "m".into(),
                score: 0.75,
            },
        ];
        let mut expected = input.clone();
        expected.sort_unstable_by(scored_cmp);
        expected.truncate(3);

        let mut selected = input;
        truncate_highest_scored(&mut selected, 3);

        assert_eq!(selected, expected);
        assert_eq!(
            selected
                .iter()
                .map(|row| row.id.as_str())
                .collect::<Vec<_>>(),
            ["a", "b", "m"]
        );
    }

    #[test]
    fn wide_drill_selects_the_exact_bounded_prefix() {
        let mut fixture = Fixture::default();
        fixture.node("summary", Some("SummaryNode"), vec![1.0, 0.0]);
        for index in 0..257 {
            let id = format!("leaf-{index:03}");
            fixture.node(&id, None, vec![index as f32 + 1.0, 1.0]);
            fixture.summarizes("summary", &id);
        }
        let retriever = HierarchicalRetriever::new(&fixture, &fixture);
        let query = [0.0, 1.0];

        let selected = retriever.drill(&query, "summary", 1, 7);
        let mut expected: Vec<Scored> = fixture
            .children("summary")
            .into_iter()
            .map(|id| Scored {
                score: retriever.relevance(&query, &id),
                id,
            })
            .collect();
        expected.sort_unstable_by(scored_cmp);
        expected.truncate(7);

        assert_eq!(selected, expected);
    }

    /// CONCEPT:MEDULLA-KG.retrieval.bounded-drill — the retriever pulls the summary tier first, then drills to
    /// the correct supporting leaves (not arbitrary nodes).
    #[test]
    fn eg195_retrieves_summaries_then_drills_to_correct_leaves() {
        let f = topic_fixture();
        let r = HierarchicalRetriever::new(&f, &f);
        // Query tilted toward all three topics but strongest on T0.
        let q = [1.0, 0.9, 0.8];
        let res = r
            .retrieve(
                &q,
                RetrievalParams {
                    k: 3,
                    drill_depth: 1,
                    drill_breadth: 2,
                    leaf_budget: 6,
                },
            )
            .unwrap();

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

    /// CONCEPT:MEDULLA-KG.retrieval.bounded-drill — a leaf shared by two summaries appears exactly ONCE in the
    /// leaves and once in the flattened context (the de-dup flat top-k can't do).
    #[test]
    fn eg195_dedups_shared_leaf_across_summaries() {
        let f = topic_fixture();
        let r = HierarchicalRetriever::new(&f, &f);
        // A query on the T0/T1 seam so BOTH S0 and S1 rank high and both drill into
        // L_shared, and L_shared is relevant enough to survive the budget.
        let q = [0.7, 0.7, 0.0];
        let res = r
            .retrieve(
                &q,
                RetrievalParams {
                    k: 3,
                    drill_depth: 1,
                    drill_breadth: 5,
                    leaf_budget: 12,
                },
            )
            .unwrap();

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

    /// CONCEPT:MEDULLA-KG.retrieval.bounded-drill — the headline claim: for the SAME budget, hierarchical
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
        let res = r
            .retrieve(
                &q,
                RetrievalParams {
                    k: 3,
                    drill_depth: 1,
                    drill_breadth: 1,
                    leaf_budget: budget.saturating_sub(3),
                },
            )
            .unwrap();
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

    /// CONCEPT:MEDULLA-KG.retrieval.bounded-drill — bounded drill: `drill_depth`/`drill_breadth` cap the fan-out
    /// so the context stays concise instead of pulling every descendant.
    #[test]
    fn eg195_drill_breadth_and_depth_are_bounded() {
        let f = topic_fixture();
        let r = HierarchicalRetriever::new(&f, &f);
        let q = [1.0, 0.0, 0.0];
        // breadth=1, so at most one child per summary is drilled; k=1 summary → <=1
        // leaf even though S0 has 5 children.
        let res = r
            .retrieve(
                &q,
                RetrievalParams {
                    k: 1,
                    drill_depth: 1,
                    drill_breadth: 1,
                    leaf_budget: 10,
                },
            )
            .unwrap();
        assert_eq!(res.summaries.len(), 1);
        assert!(
            res.leaves.len() <= 1,
            "breadth=1 must cap to one drilled child, got {}",
            res.leaves.len()
        );
    }

    // spec: MEDULLA-FEDERATED-QUERY-R081.9.4
    /// CONCEPT:MEDULLA-KG.retrieval.bounded-drill — with NO summary tier the retriever refuses with a
    /// typed error instead of silently replacing the descent with a flat global search.
    #[test]
    fn eg195_refuses_when_no_summary_tier() {
        let mut f = Fixture::default();
        // Only leaves, no :SummaryNode labels anywhere.
        f.node("A", None, vec![1.0, 0.0]);
        f.node("B", None, vec![0.0, 1.0]);
        let r = HierarchicalRetriever::new(&f, &f);
        let res = r.retrieve(&[1.0, 0.1], RetrievalParams::default());
        assert_eq!(res.unwrap_err(), VectorFreeError::SummaryTierAbsent);
    }
}
