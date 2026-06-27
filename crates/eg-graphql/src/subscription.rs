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
//! ## Deferred (documented)
//! A push transport — a `tokio::sync::broadcast` change-stream fed by `GraphCore`'s
//! write path (each `mark_dirty` publishing the bumped version, subscribers re-resolving
//! and diffing) — is the natural next step. It is intentionally NOT wired here: it would
//! pull `tokio` into a crate the facade keeps Pi-excludable, and it belongs in the server
//! layer alongside the existing query/mutation HTTP handlers. The poll path below is the
//! same resolution the push path would call per tick, so wiring the transport later is
//! additive (no change to resolution).

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

/// Parse `src` and require it to be a subscription, returning its selection as a
/// [`Query`] (subscriptions resolve exactly like queries).
fn parse_subscription(src: &str) -> Result<Query, String> {
    match parse_operation(src).map_err(|e| e.to_string())? {
        Operation::Subscription(s) => Ok(Query { roots: s.roots }),
        Operation::Query(_) => {
            Err("GraphQL: expected a subscription, got a query (use the query path)".into())
        }
        Operation::Mutation(_) => {
            Err("GraphQL: expected a subscription, got a mutation".into())
        }
    }
}
