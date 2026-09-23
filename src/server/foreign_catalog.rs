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
//!   entries. Every served plan path (`UnifiedQuery`, `UnifiedQueryText`, the
//!   policy-lease text path, in-txn UQL, `NlQuery`, and the wire-protocol UQL path)
//!   builds its registry through [`ForeignSourceCatalog::resolve_for_plan`].
//!
//! Sharing a source with other principals is not supported: each principal registers
//! its own (an explicit use grant through the engine's RBAC grants is a ledgered
//! follow-up). A name another owner registered resolves exactly like a name nobody
//! registered: the eg-plan registry's "no foreign source registered under name" error,
//! listing only the caller's own names. It is deliberately NOT `ACCESS_DENIED`, which
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

use std::sync::Arc;

use dashmap::DashMap;
use eg_plan::federation::ForeignSourceRegistry;
use eg_types::wire::ForeignSourceSpec;
use tokio::sync::RwLock;

use super::access::{CarrierAuthority, GraphReadAuthority};
use super::state::ServerState;

/// `(verified owner scope, source name)` — the catalog key.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct CatalogKey {
    owner_scope: String,
    name: String,
}

/// Every owner's registered foreign sources, partitioned by verified owner
/// (tenant+principal) scope.
#[derive(Default)]
pub struct ForeignSourceCatalog {
    entries: DashMap<CatalogKey, ForeignSourceSpec>,
}

/// A caller's owner-scoped registry, built by [`ForeignSourceCatalog::registry_for`].
/// It remembers the owner scope so result caches can key on it.
pub(crate) struct OwnedForeignRegistry {
    owner_scope: String,
    registry: ForeignSourceRegistry,
}

impl OwnedForeignRegistry {
    /// The verified owner (tenant+principal) scope this registry was built for.
    pub(crate) fn owner_scope(&self) -> &str {
        &self.owner_scope
    }

    /// The executor registry holding only this owner's sources.
    pub(crate) fn registry(&self) -> &ForeignSourceRegistry {
        &self.registry
    }
}

impl ForeignSourceCatalog {
    /// Record (or replace) `owner`'s source `name`. Another owner's entry under the
    /// same name is a different key, so it can be neither read nor overwritten here.
    pub(crate) fn register(&self, owner: &CarrierAuthority, name: String, spec: ForeignSourceSpec) {
        let key = CatalogKey {
            owner_scope: owner.owner_scope().to_string(),
            name,
        };
        self.entries.insert(key, spec);
    }

    /// THE scoping chokepoint: the executor registry holding only `caller`'s own
    /// (tenant+principal) sources.
    pub(crate) fn registry_for(&self, caller: &CarrierAuthority) -> OwnedForeignRegistry {
        let owner_scope = caller.owner_scope();
        let mut registry = ForeignSourceRegistry::new();
        for entry in self.entries.iter() {
            if entry.key().owner_scope == owner_scope {
                registry.register_spec(entry.key().name.clone(), entry.value().clone());
            }
        }
        OwnedForeignRegistry {
            owner_scope: owner_scope.to_string(),
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
        Ok(Some(self.registry_for(caller)))
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
        self.entries.get(&key).map(|entry| entry.value().clone())
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
    let catalog = state.read().await.foreign_sources.clone();
    catalog.resolve_for_plan(
        &plan.ops,
        read_authority.and_then(GraphReadAuthority::carrier),
    )
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
    use crate::server::auth::VerifiedRequestContext;

    /// A verified carrier for `agent` in the deployment's one tenant.
    fn carrier(agent: &str) -> CarrierAuthority {
        let context = VerifiedRequestContext::verified_for_test(agent);
        CarrierAuthority::from_verified(&context).expect("verified test carrier")
    }

    fn http_spec(url: &str) -> ForeignSourceSpec {
        ForeignSourceSpec::HttpJson {
            url: url.into(),
            json_path: "data".into(),
            field_map: eg_types::wire::HttpFieldMap {
                id: "id".into(),
                score: None,
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
        catalog.register(&a, "src".into(), http_spec("http://a.invalid/"));
        catalog.register(&b, "src".into(), http_spec("http://b.invalid/"));
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
            let scoped = catalog.registry_for(caller);
            assert_eq!(scoped.owner_scope(), caller.owner_scope());
            assert_eq!(scoped.registry().len(), 1, "only the caller's own entry");
        }
    }

    #[test]
    fn another_owners_name_resolves_as_not_registered() {
        let (a, b) = (carrier("agent-a"), carrier("agent-b"));
        let catalog = ForeignSourceCatalog::default();
        catalog.register(&a, "secret_src".into(), http_spec("http://a.invalid/"));
        let scoped = catalog
            .resolve_for_plan(&foreign_plan("secret_src"), Some(&b))
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
        let refused = catalog.resolve_for_plan(&foreign_plan("src"), None);
        assert!(matches!(refused, Err(ref e) if e.starts_with("ACCESS_DENIED")));
        let local = vec![eg_plan::Op::Scan {
            label: "Doc".into(),
        }];
        assert!(matches!(catalog.resolve_for_plan(&local, None), Ok(None)));
    }
}
