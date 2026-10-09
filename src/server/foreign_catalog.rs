//! Owner-scoped foreign-source catalog (CONCEPT:EG-KG.query.query-federation, EH-373).
//!
//! `Method::RegisterForeignSource` records a named [`ForeignSourceSpec`]. A
//! `RemoteEngine` spec carries a shared `secret` and a signed request `context`, so a
//! registration is a credential. Before EH-373 the server kept ONE process-global
//! `name → spec` map and every caller's plan resolved against it: any principal holding
//! only `query:unified` could name another principal's source (`FOREIGN "<name>"`, a
//! `Named` `Op::ForeignScan`, or an NL-planned query) and make the server spend that
//! principal's credential on its behalf — a confused deputy.
//!
//! This module is the ONE place that scoping happens:
//!
//! * entries are keyed by `(owner, name)`, where the owner is
//!   [`CarrierAuthority::owner_scope`] — the verified tenant AND principal (actor). It
//!   comes only from a [`CarrierAuthority`] derived from the verified request envelope,
//!   never from a request field. One engine is bound to ONE tenant
//!   (`EPISTEMIC_GRAPH_TENANT`; every verified carrier carries it), so the principal is
//!   the boundary that matters today; the tenant stays inside the owner scope so
//!   cross-tenant stays closed if engines ever serve more than one tenant. This is the
//!   same least-privilege ownership the SQL catalog uses.
//! * [`ForeignSourceCatalog::registry_for`] is the only way to turn catalog entries into
//!   the executor's [`ForeignSourceRegistry`], and it copies only the caller's own
//!   entries. Every served plan path (`UnifiedQuery`, `Uql`, the
//!   policy-lease text path, in-txn UQL, `NlQuery`, and the wire-protocol UQL path)
//!   builds its registry through [`ForeignSourceCatalog::resolve_for_plan`].
//!
//! Sharing is explicit (EH-378, `server::foreign_share`): an administrator assigns the
//! engine-provisioned `foreign-source-use:<owner agent>/<name>` role, and the grantee then
//! addresses the source as `<owner agent>/<name>`; without that exact grant a name another
//! owner registered resolves exactly like a name nobody registered: the eg-plan
//! registry's "no foreign source registered under name" error, listing only the
//! caller's own names. It is deliberately NOT `ACCESS_DENIED`, which
//! would tell a caller that some other principal uses that name.
//!
//! **Foreign rows are not RLS-filtered.** A foreign source's rows come from outside
//! the local snapshot, so the row-level visibility filter that governs local graph
//! reads never sees them. Their only access control is the remote side's own
//! authorization of the registered credential, plus this owner scoping of who may
//! use that credential.
//!
//! Registrations live in memory only. `RegisterForeignSource` is a `ControlRedb`
//! session-control saga whose durable record is an opaque receipt; the endpoint
//! configuration is never persisted, so there is no stored key shape to migrate.
//!
//! **Outbound verification at registration time (`EG-UNIFIED-DATA-PLANE-R002.1`).**
//! A `Sql` spec's DSN is checked against the existing SSRF-sensitive destination gate
//! ([`eg_plan::federation_ssrf::check_sql_dsn`], already enforced at query time by
//! `crates/eg-plan/src/federation.rs`) BEFORE the entry is inserted, so a disallowed
//! destination is refused at `RegisterForeignSource` time rather than only failing
//! later when a plan tries to use it. Other [`ForeignSourceSpec`] variants are
//! unaffected; their own outbound gates (`HttpJson`'s `validate_http_json_target`,
//! `RemoteEngine`'s dial-time checks) run where they already ran before this change —
//! unifying every variant's registration-time gate is tracked separately as
//! `EG-UNIFIED-DATA-PLANE-R002.2`/`.3`.

use std::sync::Arc;

use dashmap::DashMap;
use eg_plan::federation::ForeignSourceRegistry;
use eg_types::wire::ForeignSourceSpec;
use tokio::sync::RwLock;

use super::access::{CarrierAuthority, GraphReadAuthority};
use super::foreign_share::{may_use_shared, share_resource, shared_name};
use super::state::ServerState;
use crate::isolation::IsolationLayer;

/// `(verified owner scope, source name)` — the catalog key.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct CatalogKey {
    owner_scope: String,
    name: String,
}

/// A registered spec plus the registering principal's agent id, which names the
/// source's share resource (EH-378).
struct OwnedSpec {
    owner_agent: String,
    spec: ForeignSourceSpec,
}

/// Every owner's registered foreign sources, partitioned by verified owner
/// (tenant+principal) scope.
#[derive(Default)]
pub struct ForeignSourceCatalog {
    entries: DashMap<CatalogKey, OwnedSpec>,
}

/// A caller's registry, built by [`ForeignSourceCatalog::registry_for`]: its own sources
/// by plain name plus any explicitly shared ones by qualified name. It carries a digest
/// of exactly what it resolved so result caches never serve rows across owners, grants,
/// revocations or re-registrations.
pub(crate) struct OwnedForeignRegistry {
    owner_scope: String,
    cache_salt: String,
    registry: ForeignSourceRegistry,
}

impl OwnedForeignRegistry {
    /// Install the EH-400 freshness proof for this query's named sources.
    #[cfg(feature = "result-cache")]
    pub(crate) fn install_fragment_cache(
        &mut self,
        sources: std::collections::HashMap<String, eg_plan::federation_opt::SourceWatermark>,
    ) {
        self.registry.set_cache_scope(std::sync::Arc::new(
            eg_plan::federation_opt::FragmentCacheScope::new(self.cache_salt.clone(), sources),
        ));
    }
    /// The verified owner (tenant+principal) scope this registry was built for.
    pub(crate) fn owner_scope(&self) -> &str {
        &self.owner_scope
    }

    /// Digest of `(owner scope, every resolved (name, spec))`, for result-cache keys.
    pub(crate) fn cache_salt(&self) -> &str {
        &self.cache_salt
    }

    /// The executor registry holding only this owner's sources.
    pub(crate) fn registry(&self) -> &ForeignSourceRegistry {
        &self.registry
    }
}

impl ForeignSourceCatalog {
    /// Record (or replace) `owner`'s source `name`. Another owner's entry under the
    /// same name is a different key, so it can be neither read nor overwritten here.
    ///
    /// `EG-UNIFIED-DATA-PLANE-R002.1`: a `Sql` spec's DSN is checked against the
    /// outbound destination gate FIRST; a disallowed destination is refused (`Err`)
    /// and nothing is inserted, so an unverified endpoint never reaches the catalog.
    pub(crate) fn register(
        &self,
        owner: &CarrierAuthority,
        name: String,
        spec: ForeignSourceSpec,
    ) -> Result<(), String> {
        verify_outbound_before_register(&spec)?;
        let key = CatalogKey {
            owner_scope: owner.owner_scope().to_string(),
            name,
        };
        let owner_agent = owner.agent_id().to_string();
        self.entries.insert(key, OwnedSpec { owner_agent, spec });
        Ok(())
    }

    /// THE scoping chokepoint: the executor registry holding `caller`'s own
    /// (tenant+principal) sources by plain name, plus each other owner's source the
    /// caller holds an exact share grant for (EH-378) by `<owner agent>/<name>`.
    /// Shared entries are registered first so a caller's own name always wins.
    pub(crate) fn registry_for(
        &self,
        caller: &CarrierAuthority,
        isolation: &IsolationLayer,
    ) -> OwnedForeignRegistry {
        let owner_scope = caller.owner_scope();
        let (mut shared, mut own) = (Vec::new(), Vec::new());
        for entry in self.entries.iter() {
            let (key, value) = (entry.key(), entry.value());
            if key.owner_scope == owner_scope {
                own.push((key.name.clone(), value.spec.clone()));
            } else if may_use_shared(
                isolation,
                caller.agent_id(),
                &share_resource(&value.owner_agent, &key.name),
            ) {
                shared.push((
                    shared_name(&value.owner_agent, &key.name),
                    value.spec.clone(),
                ));
            }
        }
        shared.sort_by(|a, b| a.0.cmp(&b.0));
        own.sort_by(|a, b| a.0.cmp(&b.0));
        let cache_salt = resolved_digest(owner_scope, shared.iter().chain(&own));
        let mut registry = ForeignSourceRegistry::new();
        for (name, spec) in shared.into_iter().chain(own) {
            registry.register_spec(name, spec);
        }
        OwnedForeignRegistry {
            owner_scope: owner_scope.to_string(),
            cache_salt,
            registry,
        }
    }

    /// Resolve the registry a served plan needs: `None` when the plan names no
    /// registered source, the caller's own registry otherwise. A name-resolving plan
    /// without a verified carrier is refused rather than run with no scope.
    pub(crate) fn resolve_for_plan(
        &self,
        ops: &[eg_plan::Op],
        caller: Option<&CarrierAuthority>,
        isolation: &IsolationLayer,
    ) -> Result<Option<OwnedForeignRegistry>, String> {
        if !crate::server::handlers::query::plan_needs_foreign(ops) {
            return Ok(None);
        }
        let Some(caller) = caller else {
            crate::metrics::access_denied();
            return Err(
                "ACCESS_DENIED: a named foreign source requires a verified tenant carrier"
                    .to_string(),
            );
        };
        Ok(Some(self.registry_for(caller, isolation)))
    }

    /// The spec `owner` registered under `name`, if any.
    #[cfg(test)]
    pub(crate) fn spec_for(
        &self,
        owner: &CarrierAuthority,
        name: &str,
    ) -> Option<ForeignSourceSpec> {
        let key = CatalogKey {
            owner_scope: owner.owner_scope().to_string(),
            name: name.to_string(),
        };
        self.entries
            .get(&key)
            .map(|entry| entry.value().spec.clone())
    }

    /// Whether no owner has registered any source.
    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// [`ForeignSourceCatalog::resolve_for_plan`] against the server's catalog for a served
/// plan, with the caller's verified carrier taken from its read authority. `Err` is the
/// caller-facing refusal text.
pub(crate) async fn served_foreign_leg(
    state: &Arc<RwLock<ServerState>>,
    plan: &eg_plan::Plan,
    read_authority: Option<&GraphReadAuthority>,
) -> Result<Option<OwnedForeignRegistry>, String> {
    let s = state.read().await;
    s.foreign_sources.resolve_for_plan(
        &plan.ops,
        read_authority.and_then(GraphReadAuthority::carrier),
        &s.isolation,
    )
}

/// `EG-UNIFIED-DATA-PLANE-R002.1` — the registration-time outbound gate. A `Sql` spec's
/// DSN must pass the same SSRF-sensitive destination check `crates/eg-plan/src/federation.rs`
/// already runs at query time, so a disallowed destination is refused before persistence
/// rather than only when a plan later tries to use it. Other variants are untouched here;
/// their own gates still run where they ran before (see the module doc).
fn verify_outbound_before_register(spec: &ForeignSourceSpec) -> Result<(), String> {
    #[cfg(feature = "federation-sql")]
    if let ForeignSourceSpec::Sql { dsn, .. } = spec {
        return eg_plan::federation_ssrf::check_sql_dsn(dsn);
    }
    let _ = spec;
    Ok(())
}

/// Length-prefixed SHA-256 over the owner scope and every resolved `(name, spec)`.
fn resolved_digest<'a>(
    owner_scope: &str,
    resolved: impl Iterator<Item = &'a (String, ForeignSourceSpec)>,
) -> String {
    use sha2::{Digest, Sha256};
    let mut digest = Sha256::new();
    let mut field = |bytes: &[u8]| {
        digest.update((bytes.len() as u64).to_be_bytes());
        digest.update(bytes);
    };
    field(owner_scope.as_bytes());
    for (name, spec) in resolved {
        field(name.as_bytes());
        field(&rmp_serde::to_vec_named(spec).unwrap_or_default());
    }
    hex::encode(digest.finalize())
}

/// The executor registry a resolved leg binds, if any — the `ServedIndexes::foreign`
/// value every served plan path passes to `run_unified`.
pub(crate) fn bound_registry(
    foreign: &Option<OwnedForeignRegistry>,
) -> Option<&ForeignSourceRegistry> {
    foreign.as_ref().map(OwnedForeignRegistry::registry)
}

#[cfg(test)]
mod tests {
    //! EH-373 catalog-level proofs: `(owner, name)` keying, per-owner resolution, and
    //! no cross-owner overwrite. The served-path proofs (UnifiedQuery, UQL text, NL)
    //! live in `server::tests::foreign_tenancy`.
    use super::*;

    /// A verified carrier for `agent` in the deployment's one tenant.
    fn carrier(agent: &str) -> CarrierAuthority {
        CarrierAuthority::verified_for_test(agent)
    }

    fn http_spec(url: &str) -> ForeignSourceSpec {
        ForeignSourceSpec::HttpJson {
            url: url.into(),
            json_path: "data".into(),
            field_map: eg_types::wire::HttpFieldMap {
                id: "id".into(),
                score: None,
                columns: Default::default(),
            },
        }
    }

    fn foreign_plan(name: &str) -> Vec<eg_plan::Op> {
        vec![eg_plan::Op::Foreign { name: name.into() }]
    }

    #[test]
    fn same_name_resolves_to_each_owners_own_spec() {
        let (a, b) = (carrier("agent-a"), carrier("agent-b"));
        assert_eq!(a.tenant_scope(), b.tenant_scope(), "one engine, one tenant");
        let catalog = ForeignSourceCatalog::default();
        catalog
            .register(&a, "src".into(), http_spec("http://a.invalid/"))
            .expect("http sources are not outbound-gated here");
        catalog
            .register(&b, "src".into(), http_spec("http://b.invalid/"))
            .expect("http sources are not outbound-gated here");
        assert_eq!(
            catalog.spec_for(&a, "src"),
            Some(http_spec("http://a.invalid/"))
        );
        assert_eq!(
            catalog.spec_for(&b, "src"),
            Some(http_spec("http://b.invalid/")),
            "principal B's registration must not overwrite principal A's"
        );
        for caller in [&a, &b] {
            let scoped = catalog.registry_for(caller, &IsolationLayer::new());
            assert_eq!(scoped.owner_scope(), caller.owner_scope());
            assert_eq!(scoped.registry().len(), 1, "only the caller's own entry");
        }
    }

    #[test]
    fn another_owners_name_resolves_as_not_registered() {
        let (a, b) = (carrier("agent-a"), carrier("agent-b"));
        let catalog = ForeignSourceCatalog::default();
        catalog
            .register(&a, "secret_src".into(), http_spec("http://a.invalid/"))
            .expect("http sources are not outbound-gated here");
        let scoped = catalog
            .resolve_for_plan(
                &foreign_plan("secret_src"),
                Some(&b),
                &IsolationLayer::new(),
            )
            .expect("a verified caller is never refused outright")
            .expect("a FOREIGN plan binds a registry");
        assert!(scoped.registry().is_empty());
        let err = match scoped.registry().resolve("secret_src") {
            Ok(_) => panic!("principal B must not resolve principal A's source"),
            Err(err) => err,
        };
        assert!(
            err.contains("no foreign source registered") && !err.contains("ACCESS_DENIED"),
            "cross-owner resolution must look exactly like an unregistered name: {err}"
        );
    }

    #[test]
    fn plan_scoping_needs_a_verified_carrier_only_for_named_sources() {
        let catalog = ForeignSourceCatalog::default();
        let refused = catalog.resolve_for_plan(&foreign_plan("src"), None, &IsolationLayer::new());
        assert!(matches!(refused, Err(ref e) if e.starts_with("ACCESS_DENIED")));
        let local = vec![eg_plan::Op::Scan {
            label: "Doc".into(),
        }];
        assert!(matches!(
            catalog.resolve_for_plan(&local, None, &IsolationLayer::new()),
            Ok(None)
        ));
    }

    /// `EG-UNIFIED-DATA-PLANE-R002.1`: a `Sql` spec's DSN runs through the same
    /// SSRF-sensitive destination gate `crates/eg-plan/src/federation.rs` runs at
    /// query time, but now BEFORE the entry is inserted into the catalog.
    #[cfg(feature = "federation-sql")]
    mod outbound_verification {
        use super::*;

        fn sql_spec(dsn: &str) -> ForeignSourceSpec {
            ForeignSourceSpec::Sql {
                dsn: dsn.into(),
                query: "SELECT id FROM t".into(),
                id_field: "id".into(),
                score_field: None,
            }
        }

        #[test]
        fn a_disallowed_sql_destination_is_refused_and_never_persisted() {
            let a = carrier("agent-a");
            let catalog = ForeignSourceCatalog::default();
            // Same fixture `crates/eg-plan/src/federation_ssrf.rs` already proves is
            // refused: loopback on a non-default port, no allow-list entry.
            let err = catalog
                .register(
                    &a,
                    "bad_sql".into(),
                    sql_spec("postgres://u@127.0.0.1:5433/db"),
                )
                .expect_err("a disallowed SQL destination must be refused");
            assert!(!err.is_empty());
            assert_eq!(
                catalog.spec_for(&a, "bad_sql"),
                None,
                "a refused registration must not reach the catalog"
            );
        }

        #[test]
        fn an_allowed_sql_destination_still_registers() {
            let a = carrier("agent-a");
            let catalog = ForeignSourceCatalog::default();
            // Same fixture `crates/eg-plan/src/federation_ssrf.rs` already proves is
            // admitted: loopback on the dialect's own default port.
            catalog
                .register(
                    &a,
                    "ok_sql".into(),
                    sql_spec("postgres://u@127.0.0.1:5432/db"),
                )
                .expect("an allowed SQL destination must still register");
            assert!(catalog.spec_for(&a, "ok_sql").is_some());
        }
    }
}
