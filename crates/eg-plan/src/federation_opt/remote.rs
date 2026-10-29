//! The pushdown seam: one remote source as the optimizer sees it, and the resolution of a
//! wire [`ForeignSourceSpec`] (or a registered name) to it.

use eg_types::wire::ForeignSourceSpec;

use super::capability::{RemoteRequest, SourceCapabilities};
use super::stats::{fingerprint, short, Fingerprint};
use crate::federation::{ForeignSource, ForeignSourceRegistry, SharedForeignSource};
use crate::rowset::RowSet;

/// A foreign source that can answer a [`RemoteRequest`]. `fetch` must return a SUPERSET of
/// the rows the request selects: every row whose id is in `keys` (when keys are given), the
/// first `limit` rows of its own order (when a limit is given), or the given page.
pub(crate) trait RemoteFetch {
    fn capabilities(&self) -> SourceCapabilities;
    fn identity(&self) -> &Identity;
    /// `<kind>[:<name>]#<fingerprint>` — never a URL, DSN or credential.
    fn label(&self) -> String {
        self.identity().label.clone()
    }
    fn fingerprint(&self) -> Fingerprint {
        self.identity().fingerprint
    }
    fn fetch(&self, request: &RemoteRequest) -> Result<RowSet, String>;
    /// A source safe to call on several threads. Opaque registered closures remain
    /// sequential because the registry's trait does not promise thread safety.
    fn parallel_safe(&self) -> Option<&(dyn RemoteFetch + Sync)> {
        None
    }
    /// How many of `keys` (a prefix) fit in one request; at least 1 when `keys` is non-empty.
    fn fit_keys(&self, keys: &[String]) -> usize {
        keys.len()
    }
    /// Whether `key` can be expressed in this source's key predicate at all.
    fn key_expressible(&self, _key: &str) -> bool {
        true
    }
}

/// The spec-derived identity of a source: its kind and fingerprint.
pub(crate) struct Identity {
    pub(crate) label: String,
    pub(crate) fingerprint: Fingerprint,
    pub(crate) cache_name: Option<String>,
}

impl Identity {
    /// Identity of a self-describing spec, optionally registered under `name`.
    pub(crate) fn of_spec(spec: &ForeignSourceSpec, name: Option<&str>) -> Self {
        let bytes = rmp_serde::to_vec_named(spec).unwrap_or_default();
        let fp = fingerprint(&bytes);
        let kind = match spec {
            ForeignSourceSpec::RemoteEngine { .. } => "remote-engine",
            ForeignSourceSpec::HttpJson { .. } => "http-json",
            ForeignSourceSpec::Sql { .. } => "sql",
            ForeignSourceSpec::Trino { .. }
            | ForeignSourceSpec::Cypher { .. }
            | ForeignSourceSpec::SparkBatch { .. }
            | ForeignSourceSpec::BrainGuarded { .. } => super::oq2::kind(spec).unwrap(),
            ForeignSourceSpec::Named { .. } => "named",
            ForeignSourceSpec::Api { .. } => "api",
            ForeignSourceSpec::Mcp { .. } => "mcp",
            ForeignSourceSpec::A2a { .. } => "a2a",
            ForeignSourceSpec::GraphQl { .. } => "graphql",
        };
        Self::labelled(kind, name, fp)
    }

    /// Identity of a registry-only source (a table/closure registered by name).
    fn of_registered(name: &str) -> Self {
        let fp = fingerprint(format!("named:{name}").as_bytes());
        Self::labelled("registered", Some(name), fp)
    }

    fn labelled(kind: &str, name: Option<&str>, fingerprint: Fingerprint) -> Self {
        let label = match name {
            Some(name) => format!("{kind}:{name}#{}", short(&fingerprint)),
            None => format!("{kind}#{}", short(&fingerprint)),
        };
        Self {
            label,
            fingerprint,
            cache_name: name.map(str::to_owned),
        }
    }
}

/// A source that can only be fetched whole: every request is answered by the full result
/// (its capabilities never let the optimizer ask for less).
pub(crate) struct Opaque<'a> {
    source: OpaqueSource<'a>,
    identity: Identity,
}

enum OpaqueSource<'a> {
    Spec(Box<dyn ForeignSource + 'a>),
    Registered(&'a SharedForeignSource),
}

impl RemoteFetch for Opaque<'_> {
    fn capabilities(&self) -> SourceCapabilities {
        SourceCapabilities::fetch_only()
    }
    fn identity(&self) -> &Identity {
        &self.identity
    }
    fn fetch(&self, _request: &RemoteRequest) -> Result<RowSet, String> {
        match &self.source {
            OpaqueSource::Spec(source) => source.fetch(),
            OpaqueSource::Registered(source) => source.fetch(),
        }
    }
}

/// Resolve `spec` to the source the optimizer drives. `None` means the spec names a
/// source nothing can resolve — the caller reports it through the naive path, which owns
/// those error messages.
pub(crate) fn resolve<'a>(
    spec: &'a ForeignSourceSpec,
    registry: Option<&'a ForeignSourceRegistry>,
) -> Option<Box<dyn RemoteFetch + 'a>> {
    match spec {
        ForeignSourceSpec::Named { name } => resolve_named(name, registry?),
        other => Some(from_spec(other, None)),
    }
}

/// Resolve a registered name: its spec when it was registered as one (pushdown-capable),
/// else the registered source itself (fetch-only).
pub(crate) fn resolve_named<'a>(
    name: &'a str,
    registry: &'a ForeignSourceRegistry,
) -> Option<Box<dyn RemoteFetch + 'a>> {
    if let Some((spec, source)) = registry.bound_entry(name) {
        return Some(Box::new(Opaque {
            source: OpaqueSource::Registered(source),
            // The callback is the effective source, but the retained spec remains the
            // canonical identity/fingerprint used by traces and cache keys.
            identity: Identity::of_spec(spec, Some(name)),
        }));
    }
    if let Some(spec) = registry.spec(name) {
        return Some(from_spec(spec, Some(name)));
    }
    let source = registry.get(name)?;
    Some(Box::new(Opaque {
        source: OpaqueSource::Registered(source),
        identity: Identity::of_registered(name),
    }))
}

/// The pushdown-capable implementation for a self-describing spec.
fn from_spec<'a>(spec: &'a ForeignSourceSpec, name: Option<&'a str>) -> Box<dyn RemoteFetch + 'a> {
    let identity = Identity::of_spec(spec, name);
    match spec {
        ForeignSourceSpec::Sql {
            dsn,
            query,
            id_field,
            score_field,
            ..
        } => Box::new(super::sql::SqlRemote::new(
            super::sql::SqlSpec {
                dsn,
                query,
                id_field,
                score_field: score_field.as_deref(),
            },
            identity,
        )),
        ForeignSourceSpec::HttpJson {
            url,
            json_path,
            field_map,
        } => Box::new(super::http::HttpRemote::new(
            url, json_path, field_map, identity,
        )),
        ForeignSourceSpec::RemoteEngine { .. } => {
            Box::new(super::engine::EngineRemote::new(spec, identity))
        }
        other => Box::new(Opaque {
            source: OpaqueSource::Spec(crate::federation::source_for(other)),
            identity,
        }),
    }
}

#[cfg(test)]
mod bound_driver_tests {
    use super::{resolve_named, Identity, RemoteRequest};
    use crate::federation::{ForeignSource, ForeignSourceRegistry};
    use crate::rowset::RowSet;
    use eg_types::wire::{ForeignOperationCapabilities, ForeignSourceSpec};
    use std::sync::Arc;

    struct BoundRows;

    impl ForeignSource for BoundRows {
        fn fetch(&self) -> Result<RowSet, String> {
            Ok(RowSet::from_ids(["bound-row".to_string()]))
        }
    }

    fn spec() -> ForeignSourceSpec {
        ForeignSourceSpec::Api {
            operation: "list_objects".into(),
            capabilities: ForeignOperationCapabilities::default(),
        }
    }

    /// MEDULLA-FEDERATED-QUERY-R722.3 — optimized resolution prefers the bound callback while
    /// retaining the spec-derived identity rather than the generic registered identity.
    #[test]
    fn bound_driver_precedes_spec_and_preserves_identity() {
        let spec = spec();
        let expected = Identity::of_spec(&spec, Some("objects"));
        let mut registry = ForeignSourceRegistry::new();
        registry.register_bound("objects", spec, Arc::new(BoundRows));

        let naive = registry
            .resolve("objects")
            .expect("bound source resolves naively");
        let remote = resolve_named("objects", &registry).expect("bound source resolves");
        assert_eq!(remote.identity().fingerprint, expected.fingerprint);
        assert_eq!(
            remote.capabilities(),
            super::super::capability::SourceCapabilities::fetch_only(),
            "a bound callback must not advertise pushdown before it has effective capabilities"
        );
        assert_eq!(
            remote.fetch(&RemoteRequest::full()).unwrap().ids(),
            naive.ids(),
            "naive and optimized resolution must drive the same bound source"
        );
    }

    /// MEDULLA-FEDERATED-QUERY-R722.4 — optimizer resolution keeps unbound operation specs on the
    /// typed refusing source instead of manufacturing a network callback.
    #[test]
    fn unbound_operation_spec_stays_fail_closed_in_optimized_path() {
        let mut registry = ForeignSourceRegistry::new();
        registry.register_spec("objects", spec());

        let remote = resolve_named("objects", &registry).expect("spec resolves to refusal source");
        let error = remote
            .fetch(&RemoteRequest::full())
            .expect_err("unbound operation must refuse");
        assert!(
            error.contains("requires a verified registration and bound driver"),
            "unexpected unbound refusal: {error}"
        );
    }
}
