//! CA-12 — SPARQL `SERVICE`-federation bootstrap/health-check + qualification harness for
//! Fuseki (feature `sparql-fuseki`, off by default).
//!
//! ## Scope correction (2026-08-26) — flagged to `OWNER-PROGRAM`, not resolved unilaterally
//!
//! This module's reserving doc comment (`src/server/mod.rs`, landed by CA-17's stub commit)
//! and `DEC-CA-06`'s pipeline-stage table both describe this lane as *publishing* eg graph
//! state (an applied OWL pack) into Fuseki as a named graph keyed by the pack IRI
//! (`DEC-CA-06`: `"Publish | applied pack | Fuseki named graph <pack-iri> | new — CA-12"`).
//!
//! That reading conflicts with **this lane's own governing lane file**
//! (`plans/company-architecture/lanes/CA-12-eg-sparql-service-fuseki.md`), whose Non-goals
//! section explicitly assigns "Writing to Fuseki (publish path)" to CA-23/CA-45, and whose
//! Design section scopes this module to "a Fuseki-specific bootstrap/health-check at
//! startup and the qualification test harness" for the *inbound* `SERVICE`-query direction
//! (eg dispatching a `SERVICE <fuseki> {…}` clause), reusing the ALREADY-COMPLETE
//! `sparql_http::ServiceClient` (feature `sparql-service`) rather than building a new
//! publish path.
//!
//! It is independently unbuildable against the live target regardless of which reading is
//! correct: the deployed Fuseki `ds` dataset (`services/apache-jena` -- see that repo's
//! k8s manifest for the live ingress hostname, deliberately not restated in this tracked,
//! publicly-published source tree) is measured, admin-API-confirmed read-only via
//! `GET $/datasets` -- its service list carries only `query` and `gsp-r` (Graph Store
//! Protocol, read); there is no `update` or `gsp-rw` service registered, so both
//! `POST .../update` and `POST .../data` return HTTP 405 `"Read-only"` regardless of auth
//! (measured 2026-08-26, using the pod's own randomly-generated admin credential, read from
//! `kubectl -n apps logs`; no write service exists to grant access to).
//!
//! **This lane proceeds per its own lane file**: SERVICE-query federation config +
//! qualification against Fuseki, not a new publish path. `src/server/mod.rs`'s reserving
//! comment is CA-17's text, not this lane's to correct (out of this lane's file scope); the
//! conflict is recorded here and in this lane's final report instead.
//!
//! ## What this module does
//!
//! `SERVICE <endpoint> { … }` federation is already fully implemented and was already
//! reachable BEFORE this lane: `crates/eg-rdf/src/sparql.rs::eval_service` dispatches to an
//! injected `ctx.service: Option<&dyn RemoteSparql>`, and `server::sparql_http::ServiceClient`
//! (feature `sparql-service`) is a complete, SSRF-guarded `ureq` implementation of that
//! trait, bound in `run_dataset_query` whenever the allowlist env var
//! (`sparql_http::SERVICE_ALLOW_ENV`, `EPISTEMIC_GRAPH_SPARQL_SERVICE_ALLOW`) is non-empty.
//! Pointing that client at Fuseki needs **zero code change** — only the allowlist env value
//! (a `services/graph-os` deploy-manifest change this lane documents, per its Design
//! section, but does not itself edit).
//!
//! What this module adds:
//!   * [`startup_health_check`] — a best-effort, LOGGED-NOT-ENFORCED probe run once at
//!     server boot (wired in `src/main.rs`) that confirms the configured Fuseki endpoint
//!     answers a real SPARQL query through the SAME guarded client the live `SERVICE`
//!     dispatch path uses. It never blocks startup and never disables `SERVICE` — only
//!     `EPISTEMIC_GRAPH_SPARQL_SERVICE_ALLOW` (unset ⇒ fail-closed) does that.
//!   * The qualification test (`tests/e2e_ca12_sparql_fuseki.rs`) — proves a live,
//!     end-to-end `SERVICE` query against the real Fuseki instance returns real rows for a
//!     `GRAPH <urn:source:…>`-scoped pattern (au's named-graph partition convention,
//!     `agent_utilities/knowledge_graph/backends/sparql/source_partition.py`:
//!     `urn:source:<system>[:<instance>][:<kind>]`), that a non-allowlisted host is refused
//!     by name (not a silent empty solution), and that an allowlisted-but-unreachable
//!     endpoint fails/empties the `SERVICE` leg only (SILENT semantics) without breaking the
//!     rest of the query — see that file for the concrete evidence.
//!
//! `crates/eg-rdf/src/sparql.rs::build_service_query` was verified (not modified) to
//! preserve an inner `GRAPH <urn:source:…> {…}` clause unchanged when wrapping it in the
//! projected `SELECT` sent to the remote endpoint — it clones `inner` verbatim and only adds
//! an outer `Project`, rendered through spargebra's own `Display`. No `sparql.rs` change was
//! required (W04's "expected: none" outcome, confirmed).

/// Env var naming the Fuseki SPARQL endpoint this lane's startup health-check probes,
/// e.g. `http://<the deployed Fuseki ingress host>/ds/sparql` -- see `services/apache-jena`'s
/// k8s manifest for the live value. Unset ⇒ the health-check is a no-op — this module does
/// not invent a default host (Fuseki's live hostname is deploy-environment knowledge,
/// resolved once at W01 and set by the deploy manifest, not hardcoded here).
///
/// This is deliberately a SEPARATE env var from `EPISTEMIC_GRAPH_SPARQL_SERVICE_ALLOW`
/// (the allowlist `sparql_http::ServiceClient` reads): the allowlist says which hosts
/// `SERVICE` MAY reach; this var says which ONE of those (if any) the startup probe should
/// proactively check, so operators can allowlist several federation targets without every
/// one of them being probed at boot.
pub const FUSEKI_HEALTH_CHECK_ENDPOINT_ENV: &str = "EPISTEMIC_GRAPH_FUSEKI_HEALTH_CHECK_ENDPOINT";

/// Outcome of [`startup_health_check`], for the caller to log. Never causes startup to
/// fail — this is observability, not a gate (matches the lane file's W03: "Startup log
/// shows reachability result", not "startup refuses to serve").
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HealthCheckOutcome {
    /// [`FUSEKI_HEALTH_CHECK_ENDPOINT_ENV`] is unset: nothing to check.
    NotConfigured,
    /// An endpoint is configured but `EPISTEMIC_GRAPH_SPARQL_SERVICE_ALLOW` is empty/unset,
    /// so `SERVICE` is fail-closed and the endpoint could not be reached through it anyway
    /// — reported so an operator sees WHY nothing else runs, not just silence.
    AllowlistEmpty { endpoint: String },
    /// The endpoint answered a real SPARQL `SELECT` through the SAME client `SERVICE`
    /// dispatch uses.
    Reachable { endpoint: String, row_count: usize },
    /// The endpoint is configured (and may or may not be allowlisted) but the probe query
    /// failed — a network error, a non-2xx response, or the SSRF guard itself rejecting a
    /// misconfigured allowlist entry.
    Unreachable { endpoint: String, error: String },
}

impl HealthCheckOutcome {
    /// One-line, human-readable summary for the startup log.
    pub fn summary(&self) -> String {
        match self {
            Self::NotConfigured => format!(
                "Fuseki health-check: not configured ({FUSEKI_HEALTH_CHECK_ENDPOINT_ENV} unset)"
            ),
            Self::AllowlistEmpty { endpoint } => format!(
                "Fuseki health-check: {endpoint} configured but SERVICE allowlist \
                 (EPISTEMIC_GRAPH_SPARQL_SERVICE_ALLOW) is empty -- SERVICE stays fail-closed, \
                 skipping probe"
            ),
            Self::Reachable {
                endpoint,
                row_count,
            } => format!(
                "Fuseki health-check: {endpoint} reachable via SERVICE ({row_count} probe row(s))"
            ),
            Self::Unreachable { endpoint, error } => {
                format!("Fuseki health-check: {endpoint} UNREACHABLE via SERVICE: {error}")
            }
        }
    }
}

/// The real probe, reusing `sparql_http::ServiceClient` (feature `sparql-service`). Split
/// into its own `#[cfg]`ed submodule so `sparql-fuseki` alone (without `sparql-service`)
/// still compiles -- it just always reports [`HealthCheckOutcome::Unreachable`] with a
/// clear reason, per the fallback below, rather than failing to build.
#[cfg(feature = "sparql-service")]
mod probe {
    use super::{HealthCheckOutcome, FUSEKI_HEALTH_CHECK_ENDPOINT_ENV};
    use crate::server::sparql_http::ServiceClient;
    use eg_rdf::sparql::RemoteSparql;

    /// A trivial, dataset-agnostic probe: valid SPARQL 1.1 against ANY endpoint regardless
    /// of what data (if any) it holds, so the health-check never depends on fixture content.
    const PROBE_QUERY: &str = "SELECT (COUNT(*) AS ?c) WHERE { ?s ?p ?o }";

    pub(super) fn run() -> HealthCheckOutcome {
        let Ok(endpoint) = std::env::var(FUSEKI_HEALTH_CHECK_ENDPOINT_ENV) else {
            return HealthCheckOutcome::NotConfigured;
        };
        let Some(client) = ServiceClient::from_env() else {
            return HealthCheckOutcome::AllowlistEmpty { endpoint };
        };
        match client.select(&endpoint, PROBE_QUERY) {
            Ok(res) => HealthCheckOutcome::Reachable {
                endpoint,
                row_count: res.solutions.len(),
            },
            Err(error) => HealthCheckOutcome::Unreachable { endpoint, error },
        }
    }
}

/// Run the Fuseki reachability probe (see module docs). Best-effort, non-blocking to call,
/// never panics.
#[cfg(feature = "sparql-service")]
pub fn startup_health_check() -> HealthCheckOutcome {
    probe::run()
}

/// Without `sparql-service` there is no `RemoteSparql` client to probe with at all, so
/// `SERVICE` (and therefore this health-check) can never succeed no matter how
/// `sparql-fuseki` is configured -- report that plainly instead of silently doing nothing.
#[cfg(not(feature = "sparql-service"))]
pub fn startup_health_check() -> HealthCheckOutcome {
    match std::env::var(FUSEKI_HEALTH_CHECK_ENDPOINT_ENV) {
        Ok(endpoint) => HealthCheckOutcome::Unreachable {
            endpoint,
            error: "eg built without feature `sparql-service`; no SERVICE client is compiled \
                    in regardless of `sparql-fuseki`"
                .to_string(),
        },
        Err(_) => HealthCheckOutcome::NotConfigured,
    }
}

#[cfg(feature = "sparql-service")]
#[cfg(test)]
mod qualification;

#[cfg(test)]
mod tests {
    use super::*;

    /// Both configuration outcomes in ONE test (rather than two separate `#[test]` fns)
    /// deliberately: `FUSEKI_HEALTH_CHECK_ENDPOINT_ENV` and `SERVICE_ALLOW_ENV` are
    /// process-global, and `cargo test` runs test functions concurrently on multiple
    /// threads by default, so two functions each mutating the SAME env vars would race.
    /// `server::sparql_service::qualification`'s tests deliberately avoid touching either
    /// var (they build `ServiceClient` via the test-only `with_allow` constructor instead),
    /// so this remains the ONLY test in the crate touching these two names.
    #[test]
    fn health_check_outcomes_by_configuration() {
        std::env::remove_var(FUSEKI_HEALTH_CHECK_ENDPOINT_ENV);
        assert_eq!(
            startup_health_check(),
            HealthCheckOutcome::NotConfigured,
            "unset endpoint env ⇒ NotConfigured"
        );

        #[cfg(feature = "sparql-service")]
        {
            std::env::set_var(
                FUSEKI_HEALTH_CHECK_ENDPOINT_ENV,
                "http://example.invalid/sparql",
            );
            std::env::remove_var(crate::server::sparql_http::SERVICE_ALLOW_ENV);
            assert_eq!(
                startup_health_check(),
                HealthCheckOutcome::AllowlistEmpty {
                    endpoint: "http://example.invalid/sparql".to_string()
                },
                "endpoint set but allowlist empty ⇒ AllowlistEmpty, not a network attempt"
            );
        }

        std::env::remove_var(FUSEKI_HEALTH_CHECK_ENDPOINT_ENV);
    }
}
