//! Serving one edge search (EH-351): the live generation proposes candidates,
//! visibility runs inside the walk, every candidate is re-read from the
//! caller's view and re-scored on its current property, and every edge of a
//! pair changed since the build is scored exactly. Without a servable
//! generation the search scores the view's edges exactly up to a bound.

use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::sync::Arc;

use eg_core::graph::GraphView;
use eg_types::RowPredicate;
use serde::{Deserialize, Serialize};

use super::generation::{
    decode_properties, value_of, view_edges, EdgeGeneration, EdgeGraph, EdgeValue,
};
use super::{EdgeIndex, EdgeIndexKind, EdgeIndexLimits, EdgeKey, EdgeScope, POISONED};

/// A search asks the graph for `k × OVERFETCH` candidates before exact ranking.
const OVERFETCH: usize = 8;
/// Smallest candidate pool.
const POOL_FLOOR: usize = 64;

/// What a search looks for.
#[derive(Debug, Clone, Copy)]
pub enum EdgeQuery<'a> {
    /// Nearest edges to a vector.
    Vector(&'a [f32]),
    /// Best BM25 matches of a text query.
    Text(&'a str),
}

/// One edge search.
#[derive(Clone, Copy)]
pub struct EdgeSearchRequest<'a> {
    /// The caller's tenant and purpose; must equal the index's.
    pub scope: &'a EdgeScope,
    pub query: EdgeQuery<'a>,
    /// Edges wanted, best first.
    pub k: usize,
    /// A predicate over the edge's own properties (label, property filters, the
    /// edge's own owner), applied inside the walk. `None` admits every edge the
    /// view holds.
    pub prefilter: Option<&'a RowPredicate>,
    /// The caller's row-level security over the edge's own property blob (the
    /// graph's `can_see_row` on the served path), applied inside the walk.
    pub visible: Option<&'a dyn Fn(&[u8]) -> bool>,
}

/// One result edge.
#[derive(Debug, Clone, PartialEq)]
pub struct EdgeHit {
    pub edge: EdgeKey,
    /// The distance (vector: smaller is nearer) or BM25 score (text: larger is
    /// better).
    pub score: f32,
}

/// Why a search took the bounded exact path.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EdgeFallbackReason {
    /// No generation is live yet (requested, backfilling or blocked).
    NoGeneration,
    /// The view is at a graph version the index has not accounted for.
    Unaccounted,
    /// More pairs changed since the build than one search scores exactly.
    DeltaOverflow,
    /// The filtered walk read more candidates than its budget.
    ProbeBudget,
    /// The query vector's width differs from the generation's.
    WidthMismatch,
}

/// The answer to one search.
#[derive(Debug, Clone, PartialEq)]
pub struct EdgeSearchAnswer {
    pub hits: Vec<EdgeHit>,
    /// `Ok(generation)` for the maintained generation, `Err(reason)` for the
    /// bounded exact path.
    pub path: Result<u64, EdgeFallbackReason>,
}

impl EdgeIndex {
    /// Search the edges of `view` — the caller's view, taken at graph version
    /// `view_version`. Never builds.
    pub fn search(
        &self,
        view: &GraphView,
        view_version: u64,
        request: &EdgeSearchRequest<'_>,
    ) -> Result<EdgeSearchAnswer, String> {
        if request.scope != &self.spec.scope {
            return Err(format!(
                "EDGE_INDEX_SCOPE_MISMATCH: index `{}` serves another tenant or purpose",
                self.spec.name
            ));
        }
        let probe = Probe {
            index: self,
            view,
            request,
            limits: self.limits(),
            corpus: None,
        };
        probe.validate_query()?;
        match self.servable(view_version) {
            Ok(generation) => probe.maintained(&generation),
            Err(reason) => probe.bounded_exact(reason),
        }
    }

    /// The generation a search of a view at `view_version` may use: one that
    /// was built no later than the view, with every committed batch since
    /// accounted for by the index.
    fn servable(&self, view_version: u64) -> Result<Arc<EdgeGeneration>, EdgeFallbackReason> {
        let generation = self
            .live_generation()
            .ok_or(EdgeFallbackReason::NoGeneration)?;
        let manifest = *self.lock_manifest();
        let accounted = manifest.completeness.complete
            && manifest.source_snapshot_version >= view_version
            && generation.built_version <= view_version;
        if !accounted {
            return Err(EdgeFallbackReason::Unaccounted);
        }
        Ok(generation)
    }

    /// Endpoint pairs changed at or after graph version `since`, at most
    /// `bound`; `None` when more changed.
    fn touched_since(&self, since: u64, bound: usize) -> Option<Vec<(String, String)>> {
        let touched = self.touched.lock().expect(POISONED);
        let pairs: Vec<(String, String)> = touched
            .iter()
            .filter(|(_, stamp)| **stamp >= since)
            .map(|(pair, _)| pair.clone())
            .take(bound.saturating_add(1))
            .collect();
        (pairs.len() <= bound).then_some(pairs)
    }
}

/// One scored, admitted edge; `rank` orders best first (the distance, or the
/// negated BM25 score).
struct Scored {
    rank: f32,
    score: f32,
    edge: EdgeKey,
}

#[derive(Clone, Copy)]
struct Probe<'p> {
    index: &'p EdgeIndex,
    view: &'p GraphView,
    request: &'p EdgeSearchRequest<'p>,
    limits: EdgeIndexLimits,
    /// The BM25 corpus scores are taken against.
    corpus: Option<&'p eg_text::Corpus>,
}

impl<'p> Probe<'p> {
    fn validate_query(&self) -> Result<(), String> {
        match (self.index.spec.kind, self.request.query) {
            (EdgeIndexKind::Vector { .. }, EdgeQuery::Vector(_))
            | (EdgeIndexKind::Text, EdgeQuery::Text(_)) => Ok(()),
            _ => Err(format!(
                "edge index `{}` answers the other query family",
                self.index.spec.name
            )),
        }
    }

    fn maintained(mut self, generation: &'p EdgeGeneration) -> Result<EdgeSearchAnswer, String> {
        if let EdgeGraph::Text(postings) = &generation.graph {
            self.corpus = Some(&postings.corpus);
        }
        let reason = match self.serve_generation(generation) {
            Ok(hits) => {
                return Ok(EdgeSearchAnswer {
                    hits,
                    path: Ok(generation.generation),
                })
            }
            Err(reason) => reason,
        };
        self.corpus = None;
        self.bounded_exact(reason)
    }

    fn serve_generation(
        &self,
        generation: &EdgeGeneration,
    ) -> Result<Vec<EdgeHit>, EdgeFallbackReason> {
        if let (EdgeGraph::Vector { dim, .. }, EdgeQuery::Vector(query)) =
            (&generation.graph, self.request.query)
        {
            if *dim != query.len() {
                return Err(EdgeFallbackReason::WidthMismatch);
            }
        }
        let pairs = self
            .index
            .touched_since(generation.built_version, self.limits.delta_pairs)
            .ok_or(EdgeFallbackReason::DeltaOverflow)?;
        let mut rows: Vec<Scored> = pairs
            .iter()
            .flat_map(|pair| self.pair_edges(pair))
            .filter_map(|edge| self.admit(&edge).and_then(|value| self.score(edge, &value)))
            .collect();
        rows.extend(self.walk(generation)?);
        Ok(best(rows, self.request.k))
    }

    /// The generation's candidates, admitted inside the walk.
    fn walk(&self, generation: &EdgeGeneration) -> Result<Vec<Scored>, EdgeFallbackReason> {
        let memo: RefCell<BTreeMap<u64, Option<Scored>>> = RefCell::new(BTreeMap::new());
        let over_budget = Cell::new(false);
        let allow = |id: u64| {
            if let Some(known) = memo.borrow().get(&id) {
                return known.is_some();
            }
            if memo.borrow().len() >= self.limits.probe_edges {
                over_budget.set(true);
                return false;
            }
            let scored = generation
                .keys
                .get(id as usize)
                .cloned()
                .and_then(|edge| self.admit(&edge).and_then(|value| self.score(edge, &value)));
            let admitted = scored.is_some();
            memo.borrow_mut().insert(id, scored);
            admitted
        };
        self.candidates(generation, &allow);
        if over_budget.get() {
            return Err(EdgeFallbackReason::ProbeBudget);
        }
        Ok(memo.into_inner().into_values().flatten().collect())
    }

    /// Drive the generation's candidate walk; `allow` sees every candidate.
    fn candidates(&self, generation: &EdgeGeneration, allow: &dyn Fn(u64) -> bool) {
        let pool = self.request.k.saturating_mul(OVERFETCH).max(POOL_FLOOR);
        match (&generation.graph, self.request.query) {
            (EdgeGraph::Vector { index, .. }, EdgeQuery::Vector(query)) => {
                index.search_filtered(query, pool, pool, Some(allow));
            }
            (EdgeGraph::Text(postings), EdgeQuery::Text(query)) => {
                let mut tokens = eg_text::tokenize(query);
                tokens.sort();
                tokens.dedup();
                for id in tokens
                    .iter()
                    .filter_map(|token| postings.postings.get(token))
                    .flatten()
                {
                    allow(*id);
                }
            }
            _ => {}
        }
    }

    /// The exact answer over the view's edges, examining at most
    /// `exact_edges`; past the bound it refuses with the reason.
    fn bounded_exact(&self, reason: EdgeFallbackReason) -> Result<EdgeSearchAnswer, String> {
        let spec = &self.index.spec;
        let mut admitted: Vec<(EdgeKey, EdgeValue)> = Vec::new();
        for (examined, (edge, _)) in view_edges(self.view, &spec.property, spec.kind).enumerate() {
            if examined == self.limits.exact_edges {
                return Err(format!(
                    "edge index `{}` has no servable generation ({reason:?}) and the graph \
                     exceeds the bounded exact fallback of {} edges",
                    spec.name, self.limits.exact_edges
                ));
            }
            if let Some(value) = self.admit(&edge) {
                admitted.push((edge, value));
            }
        }
        let corpus = exact_corpus(&admitted);
        let exact = Probe {
            corpus: corpus.as_ref(),
            ..*self
        };
        let rows = admitted
            .into_iter()
            .filter_map(|(edge, value)| exact.score(edge, &value))
            .collect();
        Ok(EdgeSearchAnswer {
            hits: best(rows, self.request.k),
            path: Err(reason),
        })
    }

    /// Every edge the view holds for `pair`.
    fn pair_edges(&self, pair: &(String, String)) -> Vec<EdgeKey> {
        let count = self.view.edge_properties.get(pair).map_or(0, Vec::len);
        (0..count)
            .map(|ordinal| EdgeKey {
                source: pair.0.clone(),
                target: pair.1.clone(),
                ordinal: ordinal as u32,
            })
            .collect()
    }

    /// The edge's current indexed value when the caller may see it: the edge
    /// still exists in the view, both endpoints are visible there, and the
    /// prefilter admits its properties. A deleted edge and a hidden one are
    /// both `None`.
    fn admit(&self, edge: &EdgeKey) -> Option<EdgeValue> {
        let view = self.view;
        if !view.node_map.contains_key(&edge.source) || !view.node_map.contains_key(&edge.target) {
            return None;
        }
        let blob = view
            .edge_properties
            .get(&(edge.source.clone(), edge.target.clone()))?
            .get(edge.ordinal as usize)?;
        if !self.request.visible.is_none_or(|visible| visible(blob)) {
            return None;
        }
        let properties = decode_properties(blob)?;
        if !self
            .request
            .prefilter
            .is_none_or(|predicate| predicate.eval(&properties))
        {
            return None;
        }
        value_of(&properties, &self.index.spec.property, self.index.spec.kind)
    }

    /// The exact rank of `edge`'s current value; `None` when it does not match
    /// at all (a different width, or no query term).
    fn score(&self, edge: EdgeKey, value: &EdgeValue) -> Option<Scored> {
        match (value, self.request.query, self.index.spec.kind) {
            (
                EdgeValue::Vector(vector),
                EdgeQuery::Vector(query),
                EdgeIndexKind::Vector { metric },
            ) => (vector.len() == query.len()).then(|| {
                let distance = crate::sql::metric_to_ann(metric).distance(query, vector);
                Scored {
                    rank: distance,
                    score: distance,
                    edge,
                }
            }),
            (EdgeValue::Text(text), EdgeQuery::Text(query), EdgeIndexKind::Text) => {
                let corpus = self.corpus?;
                let score = eg_text::Bm25::default().score_in_corpus(query, text, corpus);
                (score > 0.0).then_some(Scored {
                    rank: -score,
                    score,
                    edge,
                })
            }
            _ => None,
        }
    }
}

/// The BM25 corpus of the admitted texts: the exact statistics of what the
/// bounded exact path ranks.
fn exact_corpus(admitted: &[(EdgeKey, EdgeValue)]) -> Option<eg_text::Corpus> {
    let texts: Vec<&str> = admitted
        .iter()
        .filter_map(|(_, value)| match value {
            EdgeValue::Text(text) => Some(text.as_str()),
            EdgeValue::Vector(_) => None,
        })
        .collect();
    (!texts.is_empty()).then(|| eg_text::Corpus::from_docs(texts))
}

/// Best first by `(rank, edge)` — a total, deterministic order — each edge
/// once, `k` of them.
fn best(mut rows: Vec<Scored>, k: usize) -> Vec<EdgeHit> {
    rows.sort_by(|a, b| a.rank.total_cmp(&b.rank).then_with(|| a.edge.cmp(&b.edge)));
    rows.dedup_by(|a, b| a.edge == b.edge);
    rows.truncate(k);
    rows.into_iter()
        .map(|row| EdgeHit {
            edge: row.edge,
            score: row.score,
        })
        .collect()
}
