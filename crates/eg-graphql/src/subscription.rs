//! GraphQL **subscription** execution (CONCEPT:EG-019).
//!
//! A subscription is, structurally, a query whose result a client wants to keep watching
//! as the graph changes. The streaming TRANSPORT (a WebSocket / SSE carrier pushing each
//! delta) belongs to the server layer, not this pure-Rust, Pi-excludable surface — and
//! eg-core exposes an OCC write-version (`GraphCore::version()`) that already turns "did
//! anything change?" into a cheap atomic compare. So the cleanest execution path that
//! compiles + tests here is a **poll**: resolve the subscription's selection over the
//! current snapshot and return the matches NOW, paired with the version the result
//! reflects. A caller drives the subscription by polling and re-rendering only when the
//! version advances.
//!
//! ## Poll contract
//!   * [`poll`] — resolve the subscription against a `GraphView` snapshot, returning the
//!     same `{"data": …}` shape a query returns. (Identical resolution to the query
//!     path — a subscription is a query that streams.)
//!   * [`poll_versioned`] — resolve against the live `GraphCore` and also return the OCC
//!     `version()` the snapshot reflects, so a watcher can skip re-rendering when the
//!     graph is unchanged (`version` is stable ⇒ result is stable).
//!
//! ## Live query — real push subscription (CONCEPT:EG-064)
//! [`LiveQuery`] is the streaming path: a subscription parsed ONCE, then re-resolved
//! against a FRESH snapshot every time the graph changes. eg-core (`GraphCore::changes()`)
//! now emits a `ChangeEvent` on each committed write; the SERVER layer drives the loop —
//! it subscribes a Tokio-channel sink to that change stream and, on each event, calls
//! [`LiveQuery::resolve`] and pushes the `{"data": …}` frame down a WS/SSE carrier. This
//! crate itself stays pure-Rust + runtime-free (NO tokio — the facade keeps it
//! Pi-excludable); the async transport is entirely in the server. `resolve` runs the SAME
//! resolution as the poll path, so a live tick and a poll return identical shapes.

use eg_core::graph::{GraphCore, GraphView};
use serde_json::Value;

use crate::parser::{parse_operation, Operation, Query};
use crate::resolver::execute_query;

/// Parse + execute a GraphQL subscription string against a `GraphView` snapshot,
/// returning the current matches as `{"data": …}` (the poll path). A parse error, a
/// non-subscription operation, or an unknown root type is an `Err`.
pub fn poll(view: &GraphView, src: &str) -> Result<Value, String> {
    let q = parse_subscription(src)?;
    execute_query(view, &q)
}

/// Like [`poll`] but against the live `GraphCore`: also returns the OCC `version()` the
/// returned snapshot reflects. A watcher polls, and only re-renders when the version it
/// last saw has advanced (the result cannot have changed while the version is stable).
pub fn poll_versioned(core: &GraphCore, src: &str) -> Result<(Value, u64), String> {
    let q = parse_subscription(src)?;
    let view = core.analysis_snapshot();
    let version = core.version();
    let data = execute_query(&view, &q)?;
    Ok((data, version))
}

/// A compiled **live query** (CONCEPT:EG-064): a GraphQL subscription parsed once, then
/// re-resolved against a fresh snapshot each time the graph changes. The STREAMING
/// transport lives in the server layer (a WS/SSE carrier fed by `GraphCore::changes()`);
/// this type is the pure-Rust, runtime-free execution the carrier drives per change —
/// so the crate stays Pi-excludable (NO tokio here).
///
/// Typical server loop:
/// ```ignore
/// let live = LiveQuery::parse(subscription_src)?;
/// // initial frame:
/// let (data, mut last) = live.resolve(&core)?;   push(data);
/// // on each ChangeEvent from core.changes():
/// let (data, v) = live.resolve(&core)?;
/// if v != last { last = v; push(data); }         // skip no-op ticks
/// ```
#[derive(Debug, Clone)]
pub struct LiveQuery {
    query: Query,
}

impl LiveQuery {
    /// Parse a subscription document into a live query. A parse error, a `query`/
    /// `mutation` operation, or an unknown root type is an `Err`.
    pub fn parse(src: &str) -> Result<Self, String> {
        Ok(LiveQuery {
            query: parse_subscription(src)?,
        })
    }

    /// Re-resolve the current matches over a FRESH, point-in-time-consistent snapshot
    /// of `core`, returning the `{"data": …}` frame paired with the OCC `version` it
    /// reflects. A watcher pushes the frame only when `version` advances past the last
    /// one it emitted (the result cannot change while the version is stable).
    pub fn resolve(&self, core: &GraphCore) -> Result<(Value, u64), String> {
        // Same snapshot+version pairing the poll path uses (the point-in-time
        // `analysis_snapshot_versioned` is `result-cache`-gated; this crate builds
        // feature-free over eg-core). A version bump only makes a watcher re-render,
        // never serves stale data, so the tiny read/version window is benign.
        let view = core.analysis_snapshot();
        let version = core.version();
        let data = execute_query(&view, &self.query)?;
        Ok((data, version))
    }

    /// Resolve over an ALREADY-taken snapshot (when the caller holds a view — e.g. an
    /// RLS-filtered snapshot it already produced under the read lock).
    pub fn resolve_view(&self, view: &GraphView) -> Result<Value, String> {
        execute_query(view, &self.query)
    }
}

/// Parse `src` and require it to be a subscription, returning its selection as a
/// [`Query`] (subscriptions resolve exactly like queries).
fn parse_subscription(src: &str) -> Result<Query, String> {
    match parse_operation(src).map_err(|e| e.to_string())? {
        Operation::Subscription(s) => Ok(Query { roots: s.roots }),
        Operation::Query(_) => {
            Err("GraphQL: expected a subscription, got a query (use the query path)".into())
        }
        Operation::Mutation(_) => Err("GraphQL: expected a subscription, got a mutation".into()),
    }
}
