//! CONCEPT:EG-163 — distributed span model + indexed span store + trace assembly +
//! service-dependency graph. The third leg of the observability trilogy (logs +
//! metrics + traces) that lets the engine surpass OpenObserve.
//!
//! ## Why this lives here
//!
//! A span is time-ordered data (a `start_time` + `duration`) — the SAME shape the
//! rest of `eg-tsdb` handles — so the span MODEL and its indexed store live beside
//! the series store. The HTTP glue (OTLP-JSON ingest + the Jaeger/OpenObserve-style
//! trace-search API) lives in the facade (`src/server/traces`), exactly as the log
//! ingest listener and PromQL query API do.
//!
//! ## The store
//!
//! Spans are kept in-memory keyed by `trace_id` (a trace = every span sharing a
//! trace id), with secondary indices by `service` and `operation` for fast search.
//! Retrieval assembles a trace's flat span list into a `parent → children` tree.
//! Durable persistence (redb/blob segments, mirroring the log tier) is a documented
//! follow-up; the model + search + assembly + dependency-graph surface land now.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Mutex;

/// One event attached to a span (OTLP `span.events[]`): a timestamped, named marker
/// with its own attributes (e.g. an exception or a log point within the span).
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SpanEvent {
    /// Event time, epoch nanoseconds.
    pub ts: i64,
    /// Event name (e.g. `exception`).
    pub name: String,
    /// Event attributes (schema-on-read).
    pub attrs: BTreeMap<String, String>,
}

/// One distributed-tracing span (CONCEPT:EG-163). The fixed fields are the ones
/// every tracing format carries; `attributes` is a dynamic schema-on-read map.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Span {
    /// Trace identity — every span sharing this id is one trace.
    pub trace_id: String,
    /// This span's identity within the trace.
    pub span_id: String,
    /// The parent span's id; empty for a root span.
    pub parent_span_id: String,
    /// The emitting service (OTLP resource `service.name`).
    pub service: String,
    /// The operation / span name.
    pub operation: String,
    /// Span start, epoch nanoseconds.
    pub start_time: i64,
    /// Span duration, nanoseconds.
    pub duration: i64,
    /// Status text (`OK` / `ERROR` / empty when unset).
    pub status: String,
    /// Dynamic attributes (schema-on-read), sorted for stable serialization.
    pub attributes: BTreeMap<String, String>,
    /// Timeline events attached to the span.
    pub events: Vec<SpanEvent>,
}

impl Span {
    /// The span end time (`start_time + duration`), epoch nanoseconds.
    pub fn end_time(&self) -> i64 {
        self.start_time.saturating_add(self.duration)
    }

    /// Whether every `(key,value)` tag equality holds against this span's attributes.
    fn matches_tags(&self, tags: &[(String, String)]) -> bool {
        tags.iter()
            .all(|(k, v)| self.attributes.get(k).map(|got| got == v).unwrap_or(false))
    }
}

/// An assembled span tree node: a span plus its child subtrees (parent → children).
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SpanNode {
    /// The span at this node.
    pub span: Span,
    /// Child spans (those whose `parent_span_id` is this span's `span_id`), sorted by
    /// start time.
    pub children: Vec<SpanNode>,
}

/// A whole trace assembled from its spans: the root subtrees plus rollups.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AssembledTrace {
    /// The trace id.
    pub trace_id: String,
    /// Root span subtrees (a well-formed trace has exactly one; orphans surface as
    /// additional roots rather than being dropped).
    pub roots: Vec<SpanNode>,
    /// Total span count in the trace.
    pub span_count: usize,
    /// Earliest span start, epoch nanoseconds.
    pub start_time: i64,
    /// Wall-clock trace duration = `max(end) - min(start)`, nanoseconds.
    pub duration: i64,
    /// The distinct services the trace touched, sorted.
    pub services: Vec<String>,
}

/// A trace-search predicate (Jaeger/OpenObserve-style). All set fields must hold; a
/// trace matches if it contains a span satisfying the span-level filters within the
/// time window, and its overall duration falls in `[min_duration, max_duration]`.
#[derive(Clone, Debug, Default)]
pub struct TraceQuery {
    /// Require a span from this service.
    pub service: Option<String>,
    /// Require a span with this operation.
    pub operation: Option<String>,
    /// Minimum trace duration, nanoseconds.
    pub min_duration: Option<i64>,
    /// Maximum trace duration, nanoseconds.
    pub max_duration: Option<i64>,
    /// Trace-start lower bound (epoch-ns); `i64::MIN` ⇒ unbounded.
    pub from: i64,
    /// Trace-start upper bound (epoch-ns); `i64::MAX` ⇒ unbounded.
    pub to: i64,
    /// Attribute-equality tag filters a matching span must ALL satisfy.
    pub tags: Vec<(String, String)>,
    /// Max traces to return (newest-start first).
    pub limit: usize,
}

impl TraceQuery {
    /// A wide-open query (no filters, unbounded window) with the given result cap.
    pub fn new(limit: usize) -> Self {
        Self {
            from: i64::MIN,
            to: i64::MAX,
            limit,
            ..Default::default()
        }
    }
}

/// One directed edge of the service-dependency graph: `parent` service called
/// `child` service `call_count` times (a parent span in `parent` with a child span
/// in `child`).
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ServiceEdge {
    /// The calling (parent-span) service.
    pub parent: String,
    /// The called (child-span) service.
    pub child: String,
    /// Number of parent→child span links observed across all traces.
    pub call_count: u64,
}

/// The default search result cap when a caller omits one.
pub const DEFAULT_TRACE_LIMIT: usize = 20;

/// In-memory indexed span store (CONCEPT:EG-163): spans keyed by `trace_id`, with
/// secondary `service`/`operation` → trace-id indices for search. Cheap, lock-guarded,
/// dependency-free (no redb/Arrow) so it links into `pi`-adjacent builds unchanged.
#[derive(Default)]
pub struct SpanStore {
    inner: Mutex<Inner>,
}

#[derive(Default)]
struct Inner {
    /// trace_id → its spans (insertion order preserved).
    by_trace: HashMap<String, Vec<Span>>,
    /// service → the trace ids that contain a span from it.
    by_service: HashMap<String, BTreeSet<String>>,
    /// operation → the trace ids that contain a span with it.
    by_operation: HashMap<String, BTreeSet<String>>,
}

impl SpanStore {
    /// A fresh empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Ingest one span, updating the trace bucket and the service/operation indices.
    pub fn add_span(&self, span: Span) {
        let mut g = self.inner.lock().expect("span store lock");
        g.by_service
            .entry(span.service.clone())
            .or_default()
            .insert(span.trace_id.clone());
        g.by_operation
            .entry(span.operation.clone())
            .or_default()
            .insert(span.trace_id.clone());
        g.by_trace
            .entry(span.trace_id.clone())
            .or_default()
            .push(span);
    }

    /// Ingest a batch of spans; returns the count accepted.
    pub fn add_spans(&self, spans: Vec<Span>) -> usize {
        let n = spans.len();
        for s in spans {
            self.add_span(s);
        }
        n
    }

    /// The number of distinct traces held.
    pub fn trace_count(&self) -> usize {
        self.inner.lock().expect("span store lock").by_trace.len()
    }

    /// A trace's raw spans (insertion order), or empty if unknown.
    pub fn spans_of(&self, trace_id: &str) -> Vec<Span> {
        self.inner
            .lock()
            .expect("span store lock")
            .by_trace
            .get(trace_id)
            .cloned()
            .unwrap_or_default()
    }

    /// Assemble a trace into `parent → children` subtrees. Spans whose parent is
    /// absent (root, or a dropped/not-yet-arrived parent) surface as roots so nothing
    /// is silently lost. Children are sorted by start time. `None` ⇒ unknown trace.
    pub fn assemble(&self, trace_id: &str) -> Option<AssembledTrace> {
        let spans = self.spans_of(trace_id);
        if spans.is_empty() {
            return None;
        }
        Some(assemble_spans(trace_id, spans))
    }

    /// Search for traces matching `q`, returning the newest-started `limit` traces
    /// assembled as span trees.
    pub fn search(&self, q: &TraceQuery) -> Vec<AssembledTrace> {
        // Candidate trace-id set: intersect the service/operation indices when given,
        // else consider every trace.
        let candidate_ids: Vec<String> = {
            let g = self.inner.lock().expect("span store lock");
            let from_service = q
                .service
                .as_ref()
                .map(|s| g.by_service.get(s).cloned().unwrap_or_default());
            let from_operation = q
                .operation
                .as_ref()
                .map(|o| g.by_operation.get(o).cloned().unwrap_or_default());
            match (from_service, from_operation) {
                (Some(a), Some(b)) => a.intersection(&b).cloned().collect(),
                (Some(a), None) | (None, Some(a)) => a.into_iter().collect(),
                (None, None) => g.by_trace.keys().cloned().collect(),
            }
        };

        let mut matched: Vec<AssembledTrace> = candidate_ids
            .into_iter()
            .filter_map(|id| self.assemble(&id))
            .filter(|t| trace_matches(t, q, self))
            .collect();

        // Newest-started first, then apply the limit.
        matched.sort_by(|a, b| b.start_time.cmp(&a.start_time));
        let limit = if q.limit == 0 { DEFAULT_TRACE_LIMIT } else { q.limit };
        matched.truncate(limit);
        matched
    }

    /// Derive the service-dependency graph across ALL traces: for every span with a
    /// parent in the SAME trace whose service differs, count a `parent.service →
    /// child.service` edge. Returns edges sorted by `(parent, child)`.
    pub fn service_dependencies(&self) -> Vec<ServiceEdge> {
        let g = self.inner.lock().expect("span store lock");
        let mut counts: BTreeMap<(String, String), u64> = BTreeMap::new();
        for spans in g.by_trace.values() {
            // span_id → service, for parent lookup within the trace.
            let svc_of: HashMap<&str, &str> = spans
                .iter()
                .map(|s| (s.span_id.as_str(), s.service.as_str()))
                .collect();
            for s in spans {
                if s.parent_span_id.is_empty() {
                    continue;
                }
                if let Some(parent_svc) = svc_of.get(s.parent_span_id.as_str()) {
                    if *parent_svc != s.service.as_str() {
                        *counts
                            .entry((parent_svc.to_string(), s.service.clone()))
                            .or_default() += 1;
                    }
                }
            }
        }
        counts
            .into_iter()
            .map(|((parent, child), call_count)| ServiceEdge {
                parent,
                child,
                call_count,
            })
            .collect()
    }
}

/// Assemble a flat span list into an [`AssembledTrace`] (pure — testable without a
/// store).
fn assemble_spans(trace_id: &str, spans: Vec<Span>) -> AssembledTrace {
    let span_count = spans.len();
    let start_time = spans.iter().map(|s| s.start_time).min().unwrap_or(0);
    let max_end = spans.iter().map(|s| s.end_time()).max().unwrap_or(start_time);
    let duration = max_end.saturating_sub(start_time);
    let services: Vec<String> = {
        let mut set: BTreeSet<String> = BTreeSet::new();
        for s in &spans {
            set.insert(s.service.clone());
        }
        set.into_iter().collect()
    };

    // The set of span ids present, to decide which parents are "missing" (⇒ roots).
    let present: BTreeSet<String> = spans.iter().map(|s| s.span_id.clone()).collect();
    // parent_span_id → child spans.
    let mut children_of: HashMap<String, Vec<Span>> = HashMap::new();
    let mut root_spans: Vec<Span> = Vec::new();
    for s in spans {
        if s.parent_span_id.is_empty() || !present.contains(s.parent_span_id.as_str()) {
            root_spans.push(s);
        } else {
            children_of
                .entry(s.parent_span_id.clone())
                .or_default()
                .push(s);
        }
    }
    root_spans.sort_by_key(|s| s.start_time);
    let roots = root_spans
        .into_iter()
        .map(|s| build_node(s, &mut children_of))
        .collect();

    AssembledTrace {
        trace_id: trace_id.to_string(),
        roots,
        span_count,
        start_time,
        duration,
        services,
    }
}

/// Recursively build a [`SpanNode`], draining children out of `children_of`.
fn build_node(span: Span, children_of: &mut HashMap<String, Vec<Span>>) -> SpanNode {
    let mut kids = children_of.remove(&span.span_id).unwrap_or_default();
    kids.sort_by_key(|s| s.start_time);
    let children = kids
        .into_iter()
        .map(|c| build_node(c, children_of))
        .collect();
    SpanNode { span, children }
}

/// Whether an assembled trace satisfies a query's window/duration/tag/op predicates.
fn trace_matches(t: &AssembledTrace, q: &TraceQuery, store: &SpanStore) -> bool {
    if t.start_time < q.from || t.start_time > q.to {
        return false;
    }
    if let Some(min) = q.min_duration {
        if t.duration < min {
            return false;
        }
    }
    if let Some(max) = q.max_duration {
        if t.duration > max {
            return false;
        }
    }
    // Operation + tag predicates are span-level: at least one span in the trace must
    // satisfy them (and match the service filter when both were given).
    if q.operation.is_some() || !q.tags.is_empty() {
        let spans = store.spans_of(&t.trace_id);
        let ok = spans.iter().any(|s| {
            q.operation.as_ref().map(|o| &s.operation == o).unwrap_or(true)
                && q.service.as_ref().map(|sv| &s.service == sv).unwrap_or(true)
                && s.matches_tags(&q.tags)
        });
        if !ok {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    fn span(trace: &str, id: &str, parent: &str, svc: &str, op: &str, start: i64, dur: i64) -> Span {
        Span {
            trace_id: trace.into(),
            span_id: id.into(),
            parent_span_id: parent.into(),
            service: svc.into(),
            operation: op.into(),
            start_time: start,
            duration: dur,
            status: "OK".into(),
            attributes: BTreeMap::new(),
            events: Vec::new(),
        }
    }

    /// CONCEPT:EG-163 — a flat span list assembles into a parent→children tree, with
    /// children ordered by start time and rollups (count/duration/services) computed.
    #[test]
    fn eg163_span_tree_assembly_parent_to_children() {
        let store = SpanStore::new();
        // root(gateway) → [b(api, later), a(api, earlier)] → a has child c(db).
        store.add_span(span("t1", "root", "", "gateway", "GET /", 100, 500));
        store.add_span(span("t1", "b", "root", "api", "handle", 300, 50));
        store.add_span(span("t1", "a", "root", "api", "auth", 150, 100));
        store.add_span(span("t1", "c", "a", "db", "SELECT", 160, 40));

        let t = store.assemble("t1").expect("trace present");
        assert_eq!(t.span_count, 4);
        assert_eq!(t.start_time, 100);
        assert_eq!(t.duration, 500, "max end 600 - min start 100");
        assert_eq!(t.services, vec!["api", "db", "gateway"]);

        assert_eq!(t.roots.len(), 1);
        let root = &t.roots[0];
        assert_eq!(root.span.span_id, "root");
        // Children sorted by start: a(150) before b(300).
        assert_eq!(root.children.len(), 2);
        assert_eq!(root.children[0].span.span_id, "a");
        assert_eq!(root.children[1].span.span_id, "b");
        // a's child c is nested one level deeper.
        assert_eq!(root.children[0].children.len(), 1);
        assert_eq!(root.children[0].children[0].span.span_id, "c");
    }

    /// CONCEPT:EG-163 — a span whose parent is absent from the trace surfaces as a
    /// root rather than being dropped.
    #[test]
    fn eg163_orphan_span_surfaces_as_root() {
        let store = SpanStore::new();
        store.add_span(span("t2", "x", "missing", "svc", "op", 10, 5));
        let t = store.assemble("t2").unwrap();
        assert_eq!(t.roots.len(), 1);
        assert_eq!(t.roots[0].span.span_id, "x");
    }

    /// CONCEPT:EG-163 — search filters by service, operation, and trace duration.
    #[test]
    fn eg163_search_by_service_operation_and_duration() {
        let store = SpanStore::new();
        // Trace A: gateway root 1000ns, contains a checkout span.
        store.add_span(span("A", "ar", "", "gateway", "GET /buy", 0, 1000));
        store.add_span(span("A", "ac", "ar", "checkout", "charge", 100, 300));
        // Trace B: gateway root 50ns, no checkout.
        store.add_span(span("B", "br", "", "gateway", "GET /home", 5000, 50));

        // By service=checkout → only A.
        let hits = store.search(&TraceQuery {
            service: Some("checkout".into()),
            ..TraceQuery::new(10)
        });
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].trace_id, "A");

        // By operation=GET /home → only B.
        let hits = store.search(&TraceQuery {
            operation: Some("GET /home".into()),
            ..TraceQuery::new(10)
        });
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].trace_id, "B");

        // min_duration 500 → only A (dur 1000); B (50) filtered out.
        let hits = store.search(&TraceQuery {
            min_duration: Some(500),
            ..TraceQuery::new(10)
        });
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].trace_id, "A");

        // Both traces match a bare (unfiltered) query; newest-start first ⇒ B then A.
        let hits = store.search(&TraceQuery::new(10));
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].trace_id, "B", "start 5000 sorts before start 0");
    }

    /// CONCEPT:EG-163 — search filters by an attribute (tag) equality.
    #[test]
    fn eg163_search_by_tag_filter() {
        let store = SpanStore::new();
        let mut s = span("T", "s1", "", "api", "op", 0, 10);
        s.attributes.insert("http.status_code".into(), "500".into());
        store.add_span(s);
        store.add_span(span("U", "s2", "", "api", "op", 1, 10));

        let hits = store.search(&TraceQuery {
            tags: vec![("http.status_code".into(), "500".into())],
            ..TraceQuery::new(10)
        });
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].trace_id, "T");
    }

    /// CONCEPT:EG-163 — the service-dependency graph derives parent→child service
    /// edges with call counts across traces (leveraging the graph engine's forte).
    #[test]
    fn eg163_service_dependency_graph_edges() {
        let store = SpanStore::new();
        // gateway → api → db, twice (two traces).
        for t in ["t1", "t2"] {
            store.add_span(span(t, "g", "", "gateway", "GET", 0, 100));
            store.add_span(span(t, "a", "g", "api", "handle", 10, 50));
            store.add_span(span(t, "d", "a", "db", "SELECT", 20, 10));
        }
        // A same-service child (api → api) must NOT create a self-edge.
        store.add_span(span("t1", "a2", "a", "api", "inner", 15, 5));

        let edges = store.service_dependencies();
        assert_eq!(edges.len(), 2, "gateway→api and api→db only");
        assert_eq!(
            edges[0],
            ServiceEdge { parent: "api".into(), child: "db".into(), call_count: 2 }
        );
        assert_eq!(
            edges[1],
            ServiceEdge { parent: "gateway".into(), child: "api".into(), call_count: 2 }
        );
    }
}
