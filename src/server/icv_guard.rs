//! X5-enforce (CONCEPT:EG-KG.ontology.rdf-update-guard) — wires the EXISTING eg-shacl ICV
//! commit guard (`eg_shacl::policy::IcvPolicyRegistry`, which already implements
//! `eg_rdf::guard::WriteGuard`) onto the engine's LIVE RDF write path
//! (`AddTriples`/`RemoveTriples`/`ApplyMutation`), so a commit that violates a registered
//! SHACL shape — e.g. one emitted by the connector-manifest compiler's policy block
//! (agent-utilities side, alongside its RLS/ABAC output) — is REJECTED (`IcvMode::Enforce`)
//! or logged-and-applied (`IcvMode::Warn`), configurable per graph exactly like the
//! library-level `eg_rdf::update::execute_guarded` already offers the SPARQL-UPDATE surface.
//!
//! **Reuses eg-shacl's validation/guard verbatim — no new validator.** This module is
//! pure wiring: `Method::IcvConfigure` (re)registers a graph's shapes + mode, and
//! [`check_before_write`] evaluates the SAME `eg_shacl::icv::check_write` decision the
//! library's `IcvPolicyRegistry::check_graph` already runs, over a `base` graph built from
//! the SAME `eg_rdf::mapping::export_triples` snapshot `ShaclValidate`/`ShexValidate`
//! already use as "the current data graph" (`handlers/rdf.rs`).
//!
//! **Why a static, not a `ServerState` field:** `IcvPolicyRegistry` already keys by graph
//! name internally (one `default` policy + a `named: HashMap<graph, IcvPolicy>`), so ONE
//! process-wide instance covers every graph exactly like the registry's own API implies —
//! no new mandatory `ServerState` field, which ~25 call sites across the crate construct as
//! a full struct literal (test harnesses + wire-protocol facades), each of which would need
//! a mechanical edit for a field this module doesn't otherwise need. `std::sync::OnceLock`
//! (already the project's idiom for a process-wide lazy static — see `src/slow_query.rs`,
//! `src/cost.rs`, `src/server/replica.rs`) avoids pulling in a new `once_cell` dependency.
//!
//! Default (unconfigured) state: the registry is empty ⇒ `WriteGuard::active() == false` ⇒
//! [`check_before_write`] is a single cheap `bool` check with NO export/validate work — the
//! write path is byte-identical to pre-X5 until a caller explicitly configures a policy.

use std::sync::{Arc, OnceLock};

use parking_lot::RwLock;

use eg_rdf::guard::{GuardRejection, WriteGuard};
use eg_rdf::oxrdf::{Graph, Triple};
#[cfg(feature = "rdf-redb")]
use eg_rdf::quads::QuadStore;
use eg_shacl::policy::{IcvMode, IcvPolicy, IcvPolicyRegistry};

use crate::graph::GraphCore;

static ICV_REGISTRY: OnceLock<RwLock<IcvPolicyRegistry>> = OnceLock::new();

fn registry() -> &'static RwLock<IcvPolicyRegistry> {
    ICV_REGISTRY.get_or_init(|| RwLock::new(IcvPolicyRegistry::new()))
}

/// Parse the wire `mode` string (case-insensitive `off`/`warn`/`enforce`) into an
/// [`IcvMode`]. An unknown string is a configuration error — NEVER silently `Off`.
pub(crate) fn parse_mode(mode: &str) -> Result<IcvMode, String> {
    match mode.to_ascii_lowercase().as_str() {
        "off" => Ok(IcvMode::Off),
        "warn" => Ok(IcvMode::Warn),
        "enforce" => Ok(IcvMode::Enforce),
        other => Err(format!(
            "IcvConfigure: unknown mode '{other}' (want off/warn/enforce)"
        )),
    }
}

/// `Method::IcvConfigure` — (re)register a graph's SHACL shapes as closed-world integrity
/// constraints at the given enforcement mode. `graph = None` sets the DEFAULT-graph policy
/// (mirrors `IcvPolicyRegistry::set`). An empty `shapes_ttl` is only accepted with
/// `mode="off"` (clearing/no-op — there is nothing to enforce without shapes); a non-off
/// mode with no shapes is a configuration error rather than a silently inert policy.
pub(crate) fn configure(graph: Option<&str>, mode: &str, shapes_ttl: &str) -> Result<(), String> {
    let parsed_mode = parse_mode(mode)?;
    if shapes_ttl.trim().is_empty() {
        if parsed_mode != IcvMode::Off {
            return Err(format!(
                "IcvConfigure: mode '{mode}' requires a non-empty `shapes_ttl`"
            ));
        }
        registry()
            .write()
            .set(graph, IcvPolicy::new(IcvMode::Off, Graph::new()));
        return Ok(());
    }
    let shapes = eg_shacl::graph_from_turtle(shapes_ttl)
        .map_err(|e| format!("IcvConfigure: bad shapes graph: {e}"))?;
    registry()
        .write()
        .set(graph, IcvPolicy::new(parsed_mode, shapes));
    Ok(())
}

/// Run `f` with the CURRENT registry as a `&dyn WriteGuard` (for callers — e.g.
/// `eg_rdf::update::execute_guarded_str` — that take the guard by trait object for the
/// duration of one call). Holds the read lock only for `f`'s extent.
pub(crate) fn with_write_guard<R>(f: impl FnOnce(&dyn WriteGuard) -> R) -> R {
    let guard = registry().read();
    f(&*guard)
}

/// Enforce (or warn, per the registered mode) the ICV guard for a proposed
/// additions/removals change to `graph_name`'s live RDF projection — the direct-write
/// counterpart of `eg_rdf::update::execute_guarded` for handlers (`AddTriples`/
/// `RemoveTriples`) that mutate the property graph directly rather than through the
/// `GraphStore`/SPARQL-UPDATE executor. `Err` means the caller MUST NOT apply the change.
///
/// Fails CLOSED on a base-graph export error WHEN a policy is active (never silently lets
/// a write through it couldn't actually check) — the one exception to "byte-identical
/// until configured", since an active policy is itself an explicit opt-in.
pub(crate) fn check_before_write(
    core: &Arc<GraphCore>,
    graph_name: &str,
    additions: &[Triple],
    removals: &[Triple],
    #[cfg(feature = "rdf-redb")] quads: Option<&QuadStore>,
) -> Result<(), GuardRejection> {
    let reg = registry().read();
    if !reg.active() {
        return Ok(()); // no policy registered anywhere — the cheap, default path.
    }
    let exported = eg_rdf::mapping::export_triples(
        core,
        graph_name,
        #[cfg(feature = "rdf-redb")]
        quads,
    )
    .map_err(|e| GuardRejection {
        graph: Some(graph_name.to_string()),
        message: format!(
            "EG-KG.ontology.rdf-update-guard: could not read the base graph to evaluate ICV: {e}"
        ),
        details: serde_json::Value::Null,
    })?;
    let mut base = Graph::new();
    for t in &exported {
        base.insert(t);
    }
    reg.check_graph(Some(graph_name), &base, additions, removals)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::GraphCore;

    const PREFIXES: &str = r#"
@prefix sh:   <http://www.w3.org/ns/shacl#> .
@prefix ex:   <http://example.org/> .
"#;
    // At most one manager (a resource-valued maxCount, mirrors eg-shacl's own policy test).
    const SHAPES: &str = r#"
ex:PersonShape a sh:NodeShape ;
    sh:targetClass ex:Person ;
    sh:property [ sh:path ex:manager ; sh:maxCount 1 ] .
"#;

    fn triple(s: &str, p: &str, o: &str) -> Triple {
        use eg_rdf::oxrdf::NamedNode;
        Triple::new(
            NamedNode::new_unchecked(s),
            NamedNode::new_unchecked(p),
            NamedNode::new_unchecked(o),
        )
    }

    // Each test uses its own graph name (the registry is process-wide/shared across
    // `#[test]` threads) so configuring one test's policy can never leak into another's.
    fn unique_graph(tag: &str) -> String {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        format!("icv-guard-test:{tag}:{}", N.fetch_add(1, Ordering::Relaxed))
    }

    /// Test-only wrapper supplying `check_before_write`'s `rdf-redb`-gated `quads`
    /// argument as `None` (no lossless quad store bound), so every test call site
    /// stays feature-agnostic.
    fn check(
        core: &Arc<GraphCore>,
        graph_name: &str,
        additions: &[Triple],
        removals: &[Triple],
    ) -> Result<(), GuardRejection> {
        check_before_write(
            core,
            graph_name,
            additions,
            removals,
            #[cfg(feature = "rdf-redb")]
            None,
        )
    }

    #[test]
    fn unconfigured_graph_is_a_cheap_noop() {
        let core = Arc::new(GraphCore::new());
        let g = unique_graph("noop");
        // No policy registered for `g` at all — must accept ANYTHING, including a
        // maxCount-violating shape it never saw, since nothing is configured.
        let add = vec![triple(
            "http://example.org/a",
            "http://example.org/manager",
            "http://example.org/m2",
        )];
        assert!(check(&core, &g, &add, &[]).is_ok());
    }

    #[test]
    fn enforce_rejects_a_manifest_derived_shape_violation() {
        let core = Arc::new(GraphCore::new());
        let g = unique_graph("enforce");
        configure(Some(&g), "enforce", &format!("{PREFIXES}{SHAPES}")).expect("configure ok");

        // `core` is a fresh empty graph, so the exported BASE is empty (no prior
        // violation). The proposed ADDITIONS alone assert a Person with TWO managers —
        // introducing a fresh maxCount-1 violation relative to that empty base.
        let add = vec![
            triple(
                "http://example.org/a",
                "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                "http://example.org/Person",
            ),
            triple(
                "http://example.org/a",
                "http://example.org/manager",
                "http://example.org/m1",
            ),
            triple(
                "http://example.org/a",
                "http://example.org/manager",
                "http://example.org/m2",
            ),
        ];
        let err = check(&core, &g, &add, &[])
            .expect_err("enforce must reject the maxCount-1-breaking additions");
        assert_eq!(err.graph.as_deref(), Some(g.as_str()));
        assert!(err.details.to_string().contains("witness"));
    }

    #[test]
    fn enforce_accepts_a_clean_change() {
        let core = Arc::new(GraphCore::new());
        let g = unique_graph("clean");
        configure(Some(&g), "enforce", &format!("{PREFIXES}{SHAPES}")).expect("configure ok");
        let add = vec![
            triple(
                "http://example.org/a",
                "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                "http://example.org/Person",
            ),
            triple(
                "http://example.org/a",
                "http://example.org/manager",
                "http://example.org/m1",
            ),
        ];
        assert!(check(&core, &g, &add, &[]).is_ok());
    }

    #[test]
    fn warn_never_rejects() {
        let core = Arc::new(GraphCore::new());
        let g = unique_graph("warn");
        configure(Some(&g), "warn", &format!("{PREFIXES}{SHAPES}")).expect("configure ok");
        let add = vec![
            triple(
                "http://example.org/a",
                "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                "http://example.org/Person",
            ),
            triple(
                "http://example.org/a",
                "http://example.org/manager",
                "http://example.org/m1",
            ),
            triple(
                "http://example.org/a",
                "http://example.org/manager",
                "http://example.org/m2",
            ),
        ];
        // Warn never aborts, even though the SAME change rejects under Enforce above.
        assert!(check(&core, &g, &add, &[]).is_ok());
    }

    #[test]
    fn configure_rejects_unknown_mode() {
        assert!(configure(None, "bogus", "").is_err());
    }

    #[test]
    fn configure_rejects_non_off_mode_with_no_shapes() {
        let g = unique_graph("no-shapes");
        assert!(configure(Some(&g), "enforce", "").is_err());
    }

    #[test]
    fn off_with_empty_shapes_clears_cleanly() {
        let g = unique_graph("clear");
        configure(Some(&g), "enforce", &format!("{PREFIXES}{SHAPES}")).expect("configure ok");
        configure(Some(&g), "off", "").expect("clearing to off is always accepted");
        let core = Arc::new(GraphCore::new());
        let add = vec![triple(
            "http://example.org/a",
            "http://example.org/manager",
            "http://example.org/m2",
        )];
        assert!(check(&core, &g, &add, &[]).is_ok());
    }
}
