//! Owner-scoped foreign-source catalog (CONCEPT:MEDULLA-KG.query.query-federation, EH-373).
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
//!   (`MEDULLA_TENANT`; every verified carrier carries it), so the principal is
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
//! addresses the source as `<owner agent>/<name>`. Without that exact grant, a plain or
//! qualified name another owner registered resolves exactly like an entirely unknown name,
//! preserving the centralized authorization contract's no-existence-leak rule. If the caller
//! does hold the exact grant but supplies the unqualified plain name, the catalog returns the
//! stable `ACCESS_DENIED` code and the caller must use the qualified shared name.
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
//! **Outbound verification at registration time (`MEDULLA-UNIFIED-DATA-PLANE-R002.1`).**
//! A `Sql` spec's DSN is checked against the existing SSRF-sensitive destination gate
//! ([`medulla_plan::federation_ssrf::check_sql_dsn`], already enforced at query time by
//! `crates/medulla-plan/src/federation.rs`) BEFORE the entry is inserted, so a disallowed
//! destination is refused at `RegisterForeignSource` time rather than only failing
//! later when a plan tries to use it. Other [`ForeignSourceSpec`] variants are
//! unaffected; their own outbound gates (`HttpJson`'s `validate_http_json_target`,
//! `RemoteEngine`'s dial-time checks) run where they already ran before this change —
//! unifying every variant's registration-time gate is tracked separately as
//! `MEDULLA-UNIFIED-DATA-PLANE-R002.2`/`.3`.

use std::sync::Arc;

use dashmap::DashMap;
use medulla_plan::federation::{ForeignSourceRegistry, SharedForeignSource};
#[cfg(feature = "federation")]
use medulla_types::connector_pack::PackSourceKind;
use medulla_types::connector_pack::{
    ConnectorPackSourceApprovalResult, PackSourceApproval, PackSourceRegistration,
};
use medulla_types::contract::Digest256;
use medulla_types::wire::ForeignSourceSpec;
use tokio::sync::RwLock;

use super::access::{CarrierAuthority, GraphReadAuthority};
use super::foreign_share::{may_use_shared, share_resource, shared_name};
use super::persistence::agent_library::AgentLibraryStore;
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

/// Host-derived identity for one request-scoped foreign-driver binding.
///
/// The descriptor carries no authority. `bind_for` compares it with both the
/// owner catalog entry and the durable approval result before it can reach the
/// trusted Rust callback seam in `medulla-plan`.
#[derive(Clone, Debug)]
pub(crate) struct ForeignSourceBindingDescriptor {
    pub(crate) name: String,
    pub(crate) spec: ForeignSourceSpec,
    pub(crate) registration: PackSourceRegistration,
    pub(crate) contract_digest: Digest256,
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
        sources: std::collections::HashMap<String, medulla_plan::federation_opt::SourceWatermark>,
    ) {
        self.registry.set_cache_scope(std::sync::Arc::new(
            medulla_plan::federation_opt::FragmentCacheScope::new(self.cache_salt.clone(), sources),
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
    /// `MEDULLA-UNIFIED-DATA-PLANE-R002.1`: a `Sql` spec's DSN is checked against the
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

    /// Build a request-scoped registry with one already-admitted driver bound.
    ///
    /// The network request never supplies the callback or the descriptor. A
    /// trusted host adapter supplies both after deriving the carrier from the
    /// verified request. This method still re-checks the durable approval and
    /// the current owner catalog entry immediately before binding so a stale
    /// receipt, another owner's catalog, or a descriptor mismatch fails closed.
    pub(crate) fn bind_for(
        &self,
        caller: &CarrierAuthority,
        store: &AgentLibraryStore,
        isolation: &IsolationLayer,
        receipt: &ConnectorPackSourceApprovalResult,
        descriptor: &ForeignSourceBindingDescriptor,
        driver: SharedForeignSource,
    ) -> Result<OwnedForeignRegistry, String> {
        self.verify_binding(caller, store, receipt, descriptor)?;

        let mut scoped = self.registry_for(caller, isolation);
        scoped.registry.register_bound_trusted_host(
            descriptor.name.clone(),
            descriptor.spec.clone(),
            driver,
        );
        Ok(scoped)
    }

    /// Revalidate an already-open host execution without installing a driver.
    /// Chunks and terminal transitions must observe the same current catalog
    /// and durable approval as the eventual query bind.
    #[cfg(feature = "federation")]
    pub(crate) fn verify_registered_for(
        &self,
        caller: &CarrierAuthority,
        store: &AgentLibraryStore,
        receipt: &ConnectorPackSourceApprovalResult,
        name: &str,
    ) -> Result<(), String> {
        let descriptor = self.registered_descriptor(caller, receipt, name)?;
        self.verify_binding(caller, store, receipt, &descriptor)
    }

    fn verify_binding(
        &self,
        caller: &CarrierAuthority,
        store: &AgentLibraryStore,
        receipt: &ConnectorPackSourceApprovalResult,
        descriptor: &ForeignSourceBindingDescriptor,
    ) -> Result<(), String> {
        if !caller.can_read() {
            return Err(
                "ACCESS_DENIED: a foreign source binding requires verified read authority"
                    .to_string(),
            );
        }
        let (tenant, principal) = caller.log_owner();
        if tenant != receipt.tenant_id {
            return Err("ACCESS_DENIED: source approval belongs to another tenant".to_string());
        }
        if principal.is_empty() || caller.owner_scope().is_empty() {
            return Err("ACCESS_DENIED: source binding has no verified owner".to_string());
        }
        if descriptor.name.is_empty() {
            return Err("SOURCE_BINDING_INVALID: source name is empty".to_string());
        }
        if receipt.registration.approval != PackSourceApproval::Approved {
            return Err("SOURCE_APPROVAL_NOT_ADMITTED: registration is not approved".to_string());
        }
        if descriptor.registration != receipt.registration
            || descriptor.contract_digest != receipt.contract_digest
        {
            return Err(
                "SOURCE_BINDING_MISMATCH: descriptor differs from approved registration"
                    .to_string(),
            );
        }

        #[cfg(feature = "federation")]
        if !approved_operation_matches(&descriptor.name, &descriptor.spec, &receipt.registration) {
            return Err(
                "SOURCE_BINDING_MISMATCH: approved operation differs from registered source"
                    .to_string(),
            );
        }

        // The durable member/head/body and the exact stored registration are
        // authoritative. The receipt is only the candidate reference.
        store.verify_connector_pack_source_approval(receipt)?;

        let key = CatalogKey {
            owner_scope: caller.owner_scope().to_string(),
            name: descriptor.name.clone(),
        };
        let entry = self.entries.get(&key).ok_or_else(|| {
            "SOURCE_BINDING_NOT_REGISTERED: source is absent from the caller catalog".to_string()
        })?;
        if entry.value().spec != descriptor.spec {
            return Err(
                "SOURCE_BINDING_MISMATCH: descriptor differs from owner catalog".to_string(),
            );
        }
        Ok(())
    }

    /// Bind a host-driven page source after deriving the descriptor from the
    /// caller's current catalog entry. The wire request supplies only the
    /// source name and approval receipt; it cannot choose a `ForeignSourceSpec`
    /// or smuggle authority through the callback seam.
    #[cfg(feature = "federation")]
    pub(crate) fn bind_registered_for(
        &self,
        caller: &CarrierAuthority,
        store: &AgentLibraryStore,
        isolation: &IsolationLayer,
        receipt: &ConnectorPackSourceApprovalResult,
        name: &str,
        driver: SharedForeignSource,
    ) -> Result<OwnedForeignRegistry, String> {
        let descriptor = self.registered_descriptor(caller, receipt, name)?;
        self.bind_for(caller, store, isolation, receipt, &descriptor, driver)
    }

    /// Bind several already-completed page sources into one owner-scoped registry.
    /// Every durable approval is checked before any driver is installed.
    #[cfg(feature = "federation")]
    pub(crate) fn bind_registered_many(
        &self,
        caller: &CarrierAuthority,
        store: &AgentLibraryStore,
        isolation: &IsolationLayer,
        sources: &[(
            ConnectorPackSourceApprovalResult,
            String,
            SharedForeignSource,
        )],
    ) -> Result<OwnedForeignRegistry, String> {
        let mut descriptors = Vec::with_capacity(sources.len());
        let mut names = std::collections::HashSet::new();
        for (receipt, name, _) in sources {
            if !names.insert(name.as_str()) {
                return Err("INVALID_ARGUMENT: repeated foreign source name".to_string());
            }
            let descriptor = self.registered_descriptor(caller, receipt, name)?;
            self.verify_binding(caller, store, receipt, &descriptor)?;
            descriptors.push(descriptor);
        }
        let mut scoped = self.registry_for(caller, isolation);
        for ((_, _, driver), descriptor) in sources.iter().zip(descriptors) {
            scoped.registry.register_bound_trusted_host(
                descriptor.name,
                descriptor.spec,
                Arc::clone(driver),
            );
        }
        Ok(scoped)
    }

    fn registered_descriptor(
        &self,
        caller: &CarrierAuthority,
        receipt: &ConnectorPackSourceApprovalResult,
        name: &str,
    ) -> Result<ForeignSourceBindingDescriptor, String> {
        let key = CatalogKey {
            owner_scope: caller.owner_scope().to_string(),
            name: name.to_string(),
        };
        let spec = self
            .entries
            .get(&key)
            .map(|entry| entry.value().spec.clone())
            .ok_or_else(|| {
                "SOURCE_BINDING_NOT_REGISTERED: source is absent from the caller catalog"
                    .to_string()
            })?;
        let descriptor = ForeignSourceBindingDescriptor {
            name: name.to_string(),
            spec,
            registration: receipt.registration.clone(),
            contract_digest: receipt.contract_digest,
        };
        Ok(descriptor)
    }

    /// Resolve the registry a served plan needs: `None` when the plan names no
    /// registered source, the caller's own registry otherwise. A name-resolving plan
    /// without a verified carrier is refused rather than run with no scope.
    pub(crate) fn resolve_for_plan(
        &self,
        ops: &[medulla_plan::Op],
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
        let registry = self.registry_for(caller, isolation);
        if self.plan_names_another_owners_source(ops, caller, isolation) {
            crate::metrics::access_denied();
            return Err(
                "ACCESS_DENIED: foreign source is outside the caller's owner scope".to_string(),
            );
        }
        Ok(Some(registry))
    }

    /// Return whether a plan names another owner's source by its unqualified name while the
    /// caller holds an exact share grant for that source. The grant makes the source identity
    /// independently visible, so this wrong-name path may return `ACCESS_DENIED`; without the
    /// grant, known and unknown names both remain the executor's typed not-found response.
    fn plan_names_another_owners_source(
        &self,
        ops: &[medulla_plan::Op],
        caller: &CarrierAuthority,
        isolation: &IsolationLayer,
    ) -> bool {
        ops.iter().any(|op| match op {
            medulla_plan::Op::Foreign { name } => {
                self.name_belongs_to_another_owner(name, caller, isolation)
            }
            medulla_plan::Op::ForeignScan { source, .. } => match source.as_ref() {
                medulla_types::wire::ForeignSourceSpec::Named { name } => {
                    self.name_belongs_to_another_owner(name, caller, isolation)
                }
                _ => false,
            },
            #[cfg(feature = "text")]
            medulla_plan::Op::FuseRrf { branches, .. } => branches
                .iter()
                .any(|branch| self.plan_names_another_owners_source(branch, caller, isolation)),
            _ => false,
        })
    }

    fn name_belongs_to_another_owner(
        &self,
        name: &str,
        caller: &CarrierAuthority,
        isolation: &IsolationLayer,
    ) -> bool {
        let own_key = CatalogKey {
            owner_scope: caller.owner_scope().to_string(),
            name: name.to_string(),
        };
        !self.entries.contains_key(&own_key)
            && self.entries.iter().any(|entry| {
                entry.key().name == name
                    && entry.key().owner_scope != caller.owner_scope()
                    && may_use_shared(
                        isolation,
                        caller.agent_id(),
                        &share_resource(&entry.value().owner_agent, &entry.key().name),
                    )
            })
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
    plan: &medulla_plan::Plan,
    read_authority: Option<&GraphReadAuthority>,
    execution: Option<&medulla_types::foreign_read::ForeignReadExecutionRef>,
    executions: &[medulla_types::foreign_read::ForeignReadExecutionRef],
) -> Result<Option<OwnedForeignRegistry>, String> {
    let s = state.read().await;
    let caller = read_authority.and_then(GraphReadAuthority::carrier);
    if execution.is_some() && !executions.is_empty() {
        return Err("INVALID_ARGUMENT: conflicting foreign execution forms".into());
    }
    if (execution.is_some() || !executions.is_empty())
        && !crate::server::handlers::query::plan_needs_foreign(&plan.ops)
    {
        return Err(
            "FOREIGN_READ_UNUSED: execution is attached to a plan without a foreign source".into(),
        );
    }
    if !executions.is_empty() {
        let caller = caller.ok_or_else(|| {
            "ACCESS_DENIED: foreign read executions require a verified tenant carrier".to_string()
        })?;
        let store = s.agent_library.as_deref().ok_or_else(|| {
            "SOURCE_APPROVAL_STORE_UNAVAILABLE: foreign read requires the Agent Library".to_string()
        })?;
        let plan_digest = crate::server::foreign_read::canonical_plan_digest(plan)?;
        return s
            .foreign_reads
            .bind_many_for_query(
                caller,
                executions,
                plan,
                plan_digest,
                crate::server::foreign_read::ForeignBindingContext {
                    catalog: &s.foreign_sources,
                    store,
                    isolation: &s.isolation,
                },
            )
            .map(Some);
    }
    if let Some(execution) = execution {
        let caller = caller.ok_or_else(|| {
            "ACCESS_DENIED: a foreign read execution requires a verified tenant carrier".to_string()
        })?;
        let store = s.agent_library.as_deref().ok_or_else(|| {
            "SOURCE_APPROVAL_STORE_UNAVAILABLE: foreign read requires the Agent Library".to_string()
        })?;
        let plan_digest = crate::server::foreign_read::canonical_plan_digest(plan)?;
        return s
            .foreign_reads
            .bind_for_query(
                caller,
                *execution,
                plan_digest,
                &s.foreign_sources,
                store,
                &s.isolation,
            )
            .map(Some);
    }
    s.foreign_sources
        .resolve_for_plan(&plan.ops, caller, &s.isolation)
}

/// `MEDULLA-UNIFIED-DATA-PLANE-R002.1` — the registration-time outbound gate. A `Sql` spec's
/// DSN must pass the same SSRF-sensitive destination check `crates/medulla-plan/src/federation.rs`
/// already runs at query time, so a disallowed destination is refused before persistence
/// rather than only when a plan later tries to use it. Other variants are untouched here;
/// their own gates still run where they ran before (see the module doc).
fn verify_outbound_before_register(spec: &ForeignSourceSpec) -> Result<(), String> {
    #[cfg(feature = "federation-sql")]
    if let ForeignSourceSpec::Sql { dsn, .. } = spec {
        return medulla_plan::federation_ssrf::check_sql_dsn(dsn);
    }
    let _ = spec;
    Ok(())
}

/// Compare the approved operation to the catalog spec before binding host data.
/// The registration is authenticated separately against the durable pack head.
#[cfg(feature = "federation")]
fn approved_operation_matches(
    name: &str,
    spec: &ForeignSourceSpec,
    registration: &PackSourceRegistration,
) -> bool {
    if name != registration.source_id || name.is_empty() || registration.operation.is_empty() {
        return false;
    }
    match (registration.source_kind, spec) {
        (PackSourceKind::McpTool, ForeignSourceSpec::Mcp { server, tool, .. }) => {
            server == registration.connector.as_str() && tool == &registration.operation
        }
        (PackSourceKind::HttpEndpoint, ForeignSourceSpec::Api { operation, .. }) => {
            operation == &registration.operation
        }
        (
            PackSourceKind::GraphqlQuery,
            ForeignSourceSpec::GraphQl {
                endpoint,
                operation,
                ..
            },
        ) => endpoint == &registration.source_id && operation == &registration.operation,
        (PackSourceKind::A2aSkill, ForeignSourceSpec::A2a { agent, skill, .. }) => {
            agent == registration.connector.as_str() && skill == &registration.operation
        }
        // A Named source does not carry an independently checkable operation.
        _ => false,
    }
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
            field_map: medulla_types::wire::HttpFieldMap {
                id: "id".into(),
                score: None,
                columns: Default::default(),
            },
        }
    }

    fn foreign_plan(name: &str) -> Vec<medulla_plan::Op> {
        vec![medulla_plan::Op::Foreign { name: name.into() }]
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
    fn another_owners_name_is_concealed_without_a_share_grant() {
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
        let err = scoped
            .registry()
            .resolve("secret_src")
            .expect_err("known but ungranted source must remain concealed");
        assert!(
            err.contains("no foreign source registered under name 'secret_src'")
                && !err.contains("ACCESS_DENIED"),
            "known ungranted source must look unknown: {err}"
        );
    }

    #[test]
    fn unknown_name_keeps_typed_not_found_error() {
        let catalog = ForeignSourceCatalog::default();
        let caller = carrier("agent-a");
        let scoped = catalog
            .resolve_for_plan(
                &foreign_plan("unknown_src"),
                Some(&caller),
                &IsolationLayer::new(),
            )
            .expect("unknown names still bind the caller's empty registry")
            .expect("a FOREIGN plan binds a registry");
        let err = scoped
            .registry()
            .resolve("unknown_src")
            .expect_err("unknown source must remain a typed not-found refusal");
        assert!(err.contains("no foreign source registered under name 'unknown_src'"));
    }

    #[test]
    fn plan_scoping_needs_a_verified_carrier_only_for_named_sources() {
        let catalog = ForeignSourceCatalog::default();
        let refused = catalog.resolve_for_plan(&foreign_plan("src"), None, &IsolationLayer::new());
        assert!(matches!(refused, Err(ref e) if e.starts_with("ACCESS_DENIED")));
        let local = vec![medulla_plan::Op::Scan {
            label: "Doc".into(),
        }];
        assert!(matches!(
            catalog.resolve_for_plan(&local, None, &IsolationLayer::new()),
            Ok(None)
        ));
    }

    /// `MEDULLA-UNIFIED-DATA-PLANE-R002.1`: a `Sql` spec's DSN runs through the same
    /// SSRF-sensitive destination gate `crates/medulla-plan/src/federation.rs` runs at
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
                columns: Vec::new(),
            }
        }

        // spec: MEDULLA-UNIFIED-DATA-PLANE-R002
        // spec: MEDULLA-UNIFIED-DATA-PLANE-R002.1
        #[test]
        fn a_disallowed_sql_destination_is_refused_and_never_persisted() {
            let _allow = crate::test_support::FederationAllowGuard::blocking("");
            let a = carrier("agent-a");
            let catalog = ForeignSourceCatalog::default();
            // Same fixture `crates/medulla-plan/src/federation_ssrf.rs` already proves is
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

        // spec: MEDULLA-UNIFIED-DATA-PLANE-R002
        // spec: MEDULLA-UNIFIED-DATA-PLANE-R002.1
        #[test]
        fn an_allowed_sql_destination_still_registers() {
            let _allow = crate::test_support::FederationAllowGuard::blocking("127.0.0.1:5432");
            let a = carrier("agent-a");
            let catalog = ForeignSourceCatalog::default();
            // Same fixture `crates/medulla-plan/src/federation_ssrf.rs` already proves is
            // admitted: loopback with an explicit exact host:port allowlist entry.
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
