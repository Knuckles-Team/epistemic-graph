// CONCEPT:EG-KG.txn.access-control-isolation — Graph Access Control / Isolation Layer
//
// Enforces ACL rules for multi-tenant graph access:
// 1. Peer isolation: Agent graphs invisible to peer agents
// 2. Hierarchical access: Managers have full access to subordinate graphs
// 3. Commons is public: __commons__ readable/writable by all authenticated agents
// 4. Team scoping: Read for members, R/W for manager
// 5. Global read-only: System-managed, agent-readable

use crate::protocol::GraphType;
use std::collections::{HashMap, HashSet};

/// Access level for a graph operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccessLevel {
    Read,
    Write,
}

/// Per-row owner/visibility derived from a node's property blob (CONCEPT:EG-KG.sharding.row-level-security).
/// The reserved property keys (`_owner` / `_visibility` / `_grants`) form this
/// crate's own native RLS convention, enforced by [`IsolationLayer::filter_view`].
///
/// **BUG-052 / GOC-61 read-both compatibility.** `agent-utilities`' independent
/// governed-write chokepoint (`knowledge_graph/core/tenant_sharing.py`) stamps a
/// DIFFERENTLY-NAMED but semantically equivalent pair — `_owner_id` (owner) and
/// `_shared_scope` (`private`/`org`/`commons`, superset of this crate's binary
/// `_visibility`) — and is, in practice, the authority that decides ownership for
/// virtually all traffic that reaches the engine through `agent-utilities` (that
/// caller's connection is provisioned as `AgentRole::System`, which bypasses
/// `can_see_row` before a single row is inspected — see
/// `plans/graph-os-completion-program/decisions/GOC-61-ownership-property-convention.md`
/// for the full analysis of which layer actually decides a live access). Any
/// caller that reaches THIS engine directly — Bolt/pgwire/mysql-wire/GraphQL/
/// SPARQL/AMQP/MQTT/STOMP, none of which route through `tenant_sharing.py` — sees
/// ONLY this crate's own `row_visibility` decision, so a node `agent-utilities`
/// stamped `_owner_id`/`_shared_scope='private'` was, before this change,
/// invisible to `can_see_row` as an *owner* row and fell through to the
/// untagged/tagged-public branches instead — a real, not cosmetic, visibility gap
/// for any direct caller. `row_visibility` below now falls back to `_owner_id`/
/// `_shared_scope` whenever `_owner`/`_visibility` are absent, so a node tagged by
/// EITHER convention resolves to the same [`RowVisibility`] regardless of which
/// wire path reads it. This is READ-ONLY compatibility: nothing in this crate
/// writes `_owner`/`_visibility`/`_grants` in production (grep-confirmed — every
/// production write path that could stamp RLS properties is `agent-utilities`',
/// which already writes only `_owner_id`/`_shared_scope`), so there is no
/// persisted-format migration here, and the two decision records intentionally
/// name `_owner_id`/`_shared_scope` the canonical convention going forward — this
/// crate's own `_owner`/`_visibility`/`_grants` constants stay as a read-side
/// fallback for any pre-existing/direct-write data, not the preferred write shape.
#[cfg(feature = "security")]
#[derive(Debug, Clone, Default)]
pub struct RowVisibility {
    /// Owning agent_id (`_owner`, falling back to `_owner_id`); `None` requires
    /// both [`Self::tagged`] and [`Self::public`] under the default-deny posture.
    pub owner: Option<String>,
    /// `true` when visible beyond the owner alone. Derived from `_visibility`
    /// (absent-or-`"public"` ⇒ true, `"private"` ⇒ false) when present; else from
    /// `_shared_scope` (`"org"`/`"commons"` ⇒ true, `"private"` ⇒ false) when
    /// present; else `true` (the pre-existing bare-absent default).
    ///
    /// **BUG-064 trust boundary.** A `_visibility='public'` reading is NOT
    /// taken at face value when it cannot legitimately have been written: a
    /// row also carrying `_owner_id`/`_shared_scope` defers to that
    /// convention's own value instead (only it has a known production
    /// writer), and a fully unowned row with no `_grants` treats a bare
    /// `_visibility` tag alone as uncorroborated. See [`row_visibility`]'s
    /// doc comment for the full incident this closes.
    pub public: bool,
    /// Agent_ids explicitly granted read (`_grants`, comma-separated). No
    /// `agent-utilities`-side equivalent exists yet (GOC-61 designs a distinct
    /// `grant_id`-keyed model, not a per-node CSV list), so there is nothing to
    /// fall back to here.
    pub grants: Vec<String>,
    /// Whether this row carried ANY explicit RLS metadata at all, under EITHER
    /// convention — i.e. the decoded property blob contained at least one of
    /// `_owner` / `_visibility` / `_grants` / `_owner_id` / `_shared_scope`.
    /// `false` for an undecodable blob OR a blob that decoded fine but declares
    /// none of the five keys. Untagged rows fail the default-deny decision.
    pub tagged: bool,
    /// A18 TBox/ABox RLS distinction: `true` when the row's property blob
    /// carries [`RLS_SCHEMA_KEY`] — an OWL/RDFS axiom/class/property-definition
    /// node stamped by `eg_rdf`'s lowering, structurally, never a name
    /// convention. [`IsolationLayer::can_see_row`] treats a schema row as
    /// visible regardless of `owner`/`public`/`tagged`/`grants` — see
    /// [`RLS_SCHEMA_KEY`]'s doc comment for the full ABox/TBox reasoning and
    /// why graph-level ACL (unaffected by this field) remains the real gate.
    /// An ABox row (`schema: false`, the default) is completely unaffected:
    /// default-deny applies exactly as it did before this field existed.
    pub schema: bool,
}

/// Reserved RLS property keys (this crate's own native convention).
#[cfg(feature = "security")]
pub const RLS_OWNER_KEY: &str = "_owner";
#[cfg(feature = "security")]
pub const RLS_VISIBILITY_KEY: &str = "_visibility";
#[cfg(feature = "security")]
pub const RLS_GRANTS_KEY: &str = "_grants";

/// `agent-utilities`' `tenant_sharing.py` convention (BUG-052 / GOC-61
/// read-both compatibility — see [`RowVisibility`]'s doc comment). Named
/// canonical going forward by
/// `decisions/GOC-61-ownership-property-convention.md`; read here as a
/// fallback, never written by this crate.
#[cfg(feature = "security")]
pub const RLS_OWNER_ID_KEY: &str = "_owner_id";
#[cfg(feature = "security")]
pub const RLS_SHARED_SCOPE_KEY: &str = "_shared_scope";

/// Reserved node-property key marking a row as ontology SCHEMA (TBox) rather
/// than instance data (ABox) — CONCEPT:EG-KG.sharding.row-level-security, A18 TBox/ABox RLS
/// distinction.
///
/// Row-level default-deny ([`IsolationLayer::can_see_row`]) exists to protect
/// ABox rows — a row ABOUT someone/something whose owner should control its
/// visibility. An OWL axiom / class / property-definition node is SCHEMA, not
/// a row about anyone; applying row-level default-deny to it is a category
/// error, and doing so made an entire graph's schema invisible to every
/// non-`System` actor (see `tests/pgwire_roundtrip.rs::wire_reason_iri_bridges_string_typed_node`).
/// Schema is already protected at the correct granularity by GRAPH-level ACL
/// (`server::access::check_graph_access`, enforced upstream of every read
/// that reaches row filtering, UNCHANGED by this key's existence): if a
/// caller may read the graph at all, it may see the graph's schema; if it may
/// not, [`IsolationLayer::filter_view`] is never reached for that caller in
/// the first place.
///
/// This key is stamped ONLY by the RDF/OWL lowering layer (`eg_rdf::mapping`,
/// `eg_rdf::update`) on a node that is STRUCTURALLY a class/property/axiom
/// reference — the subject or object of a recognized RDFS/OWL schema
/// predicate (`rdfs:subClassOf`, `owl:equivalentClass`, `owl:onProperty`, …),
/// or the subject of an explicit `rdf:type owl:Class`/`rdfs:Class`/
/// `owl:ObjectProperty`/… declaration — NEVER a name convention on the node
/// id. `eg-core` deliberately does not know RDF/OWL vocabulary (`eg-rdf`
/// depends on `eg-core`, not the reverse); this crate only trusts this ONE
/// narrow, explicit marker set by the layer that DOES know the vocabulary,
/// and does nothing to widen visibility for any row that lacks it — an
/// untagged ABox row is denied exactly as it was before this key existed.
///
/// Deliberately NOT gated on feature `security` (unlike the other `RLS_*_KEY`
/// constants below) — `eg-rdf`'s lowering (feature `rdf`, which does not
/// itself depend on `eg-core/security`) must be able to name this key
/// whenever RDF/OWL lowering compiles at all, independent of whether THIS
/// build additionally happens to enable row-level enforcement. Only the
/// constant is unconditional; every place that INTERPRETS it
/// ([`RowVisibility`], [`row_visibility`], [`IsolationLayer::can_see_row`])
/// stays exactly as gated as it always was.
pub const RLS_SCHEMA_KEY: &str = "_schema";

/// Parse a node's msgpack property blob into its [`RowVisibility`].
///
/// A blob that cannot be decoded as a string-keyed map, or that declares none of
/// the RLS keys (under EITHER naming convention — see [`RowVisibility`]), yields
/// `tagged = false` and is denied. Explicit ownership, visibility, and grants
/// remain the only row authorization inputs.
#[cfg(feature = "security")]
pub fn row_visibility(blob: &[u8]) -> RowVisibility {
    use serde_json::Value;
    let map: std::collections::BTreeMap<String, Value> = match eg_types::msgpack::decode_bounded(
        blob,
        eg_types::msgpack::MsgpackLimits::new(
            eg_types::msgpack::MAX_PROPERTY_BYTES,
            eg_types::msgpack::MAX_PROPERTY_ITEMS,
            eg_types::msgpack::DEFAULT_MAX_DEPTH,
        ),
    ) {
        Ok(m) => m,
        Err(_) => return RowVisibility::default_public(),
    };
    let owner = map
        .get(RLS_OWNER_KEY)
        .or_else(|| map.get(RLS_OWNER_ID_KEY))
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string());
    // BUG-064: whether an au (`tenant_sharing.py`) convention key is present
    // on this row at all — `_owner_id` or `_shared_scope`. Used below to
    // decide how much to trust a native `_visibility` key on the SAME row.
    let au_tagged = map.contains_key(RLS_OWNER_ID_KEY) || map.contains_key(RLS_SHARED_SCOPE_KEY);
    let shared_scope_public = map
        .get(RLS_SHARED_SCOPE_KEY)
        .and_then(|v| v.as_str())
        .map(|v| v.eq_ignore_ascii_case("org") || v.eq_ignore_ascii_case("commons"));
    let public = match map.get(RLS_VISIBILITY_KEY).and_then(|v| v.as_str()) {
        Some(v) => {
            let native_public = !v.eq_ignore_ascii_case("private");
            // BUG-064 (see BUG-LEDGER.md#BUG-064): the native `_visibility`
            // key has ZERO known legitimate production writers in either
            // repo (grep-confirmed across all history) — its only
            // real-world occurrence to date was an out-of-band admin RPC
            // (`engine_lifecycle_batch_update`) that blanket-executed
            // `MATCH (n) WHERE n._visibility IS NULL SET n._visibility =
            // 'public'` on 2026-08-07, stamping 45,478 nodes irrespective of
            // their real ownership/scope. Because this branch used to be
            // consulted BEFORE `_shared_scope` unconditionally, that one
            // write silently converted every reachable owned row —
            // including ones genuinely `_shared_scope='private'` — into
            // `public` for any non-System caller, and made every untagged
            // unowned row look "explicitly public". A `_visibility='public'`
            // read here is therefore trusted only when there is a
            // legitimate story for how it could have been written:
            // * a row also tagged by the au convention (`_owner_id` /
            //   `_shared_scope`) is decided by THAT convention's own
            //   `_shared_scope` instead — the one write chokepoint proven to
            //   write these keys in production (`tenant_sharing.stamp_ownership`).
            //   A `_visibility` key can only have arrived out-of-band on
            //   such a row.
            // * a row with NO owner at all and no `_grants` has no
            //   tenant/ACL context establishing WHO decided it should be
            //   public, so a bare tag alone is not corroboration — see the
            //   BUG-064 disposition: "a bare `_visibility='public'` with no
            //   real owner/tenant must not by itself grant a non-System
            //   caller anything".
            // A genuinely NATIVE `_owner`-tagged row (no au tagging) keeps
            // trusting `_visibility` as before: the incident's writer never
            // sets `_owner`, so that combination cannot be its output, and
            // this is the only pre-existing "owner marks own row public"
            // path this crate itself ever exercises.
            // `_visibility='private'` is always honored outright — fewer
            // false grants is the safe direction and needs no override.
            //
            // BUG-193 follow-up (found during review, before landing —
            // `bug193_stamped_visibility_interaction` pins it): the
            // `unwrap_or` default below only fires when `_shared_scope` is
            // ABSENT (`shared_scope_public == None`) despite `au_tagged`
            // being true via `_owner_id` alone. Before BUG-193, that
            // combination could not occur in production: the only known
            // `_owner_id` writer (`tenant_sharing.stamp_ownership`) ALWAYS
            // sets `_shared_scope` in the same write (§7 of
            // `decisions/GOC-61-ownership-property-convention.md` measures
            // this as a live invariant — 24,414/24,414). `unwrap_or(false)`
            // was therefore a defensive default for a combination that
            // "shouldn't happen" for that population, not a load-bearing
            // decision. BUG-193's own write-side stamp
            // (`stamp_owner_id_if_absent`) breaks that invariant on
            // purpose: it stamps `_owner_id` from the caller's identity but
            // deliberately never invents a `_shared_scope` (visibility
            // breadth is caller-supplied content, not an identity fact the
            // stamp has authority to invent — see that fn's doc). So a
            // native caller who explicitly writes `_visibility: "public"`
            // with no ownership key of their own now ALSO ends up
            // `au_tagged` (via the stamp) with `_shared_scope` still
            // absent — and `unwrap_or(false)` would silently downgrade
            // their explicit "public" declaration to visible-to-owner-only
            // for every other caller, a real regression this exact edit
            // closes. `unwrap_or(native_public)` instead: when there is no
            // au `_shared_scope` to contradict it, trust the row's own
            // explicit native `_visibility` value — exactly the same
            // "trust the caller's stated visibility absent a corroborating,
            // more-authoritative signal" posture the `None` arm below
            // already takes for a row with no `_visibility` key at all.
            // Does NOT reopen BUG-064: every BUG-064 incident row has
            // `_owner_id IS NULL` (confirmed live, `decisions/
            // GOC-61-ownership-property-convention.md` §7 and
            // `BUG-LEDGER.md#BUG-064`), so `au_tagged` is `false` for that
            // population — it never reaches this branch at all and keeps
            // hitting the `owner.is_none()` branch below unchanged
            // (`unowned_bare_visibility_tag_alone_is_no_longer_trusted`
            // pins that). And whenever `_shared_scope` IS present (the
            // ordinary au-mediated-write shape), `shared_scope_public` is
            // `Some(..)` and this default is never consulted — the
            // `au_shared_scope_private_still_overrides_a_conflicting_
            // native_public_tag` test pins that a real, explicit
            // `_shared_scope: private` still wins over a conflicting native
            // tag, unaffected by this change.
            if native_public && au_tagged {
                shared_scope_public.unwrap_or(native_public)
            } else if native_public && owner.is_none() && !map.contains_key(RLS_GRANTS_KEY) {
                false
            } else {
                native_public
            }
        }
        // `tenant_sharing.SCOPE_ORG`/`SCOPE_COMMONS` — visible beyond the
        // owner (tenant/graph-level isolation already happened upstream of
        // this row, at graph selection, so "org" here correctly maps to
        // "public within this graph" exactly as `visibility_predicate`
        // treats it). `SCOPE_PRIVATE` ⇒ false. Absent ⇒ the pre-existing
        // bare-absent default (true).
        None => shared_scope_public.unwrap_or(true),
    };
    let grants = map
        .get(RLS_GRANTS_KEY)
        .and_then(|v| v.as_str())
        .map(|s| {
            s.split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default();
    let tagged = map.contains_key(RLS_OWNER_KEY)
        || map.contains_key(RLS_VISIBILITY_KEY)
        || map.contains_key(RLS_GRANTS_KEY)
        || map.contains_key(RLS_OWNER_ID_KEY)
        || map.contains_key(RLS_SHARED_SCOPE_KEY);
    // A18: schema (TBox) is a SEPARATE dimension from the owner/visibility/
    // grants tagging above — an axiom node is neither owned nor "tagged
    // public" in the ABox sense, it is simply not a row RLS should be
    // deciding at all. See `RLS_SCHEMA_KEY` for the full reasoning.
    //
    // BUG A3 (2026-08-12): `schema` is UNCONDITIONALLY `false` here, never
    // decoded from the blob. It used to read a `_schema: true` property this
    // crate stamped once (`eg_rdf`'s lowering) and never cleared on axiom
    // deletion, so an IRI once used as a schema term stayed schema-visible
    // forever, even after being repurposed as an ordinary ABox individual.
    // Schema-ness is now DERIVED from `GraphCore::schema_refs` (a live
    // reverse index: `iri -> count of live schema-defining triples`),
    // consulted by node id, not decoded from this function's blob-only
    // input. A caller with a live snapshot must set `.schema` on the
    // returned value itself — see `IsolationLayer::filter_view` (the bulk
    // row-filtering path, via `GraphView::schema_node_ids`) and
    // `GraphReadAuthority::can_see_node` (the single-row fallback, via
    // `GraphCore::is_schema_node`) for the two call sites that do.
    let schema = false;
    RowVisibility {
        owner,
        public,
        grants,
        tagged,
        schema,
    }
}

/// Stamp the BUG-052/GOC-61 canonical ownership key (`_owner_id`) onto a
/// freshly-written node's property blob when the writer did not already
/// supply an ownership key of EITHER convention — BUG-193's write-side
/// counterpart to [`row_visibility`]'s read-side fallback.
///
/// Returns `Some(new_blob)` only when a stamp was actually added; `None`
/// means "use the original blob unchanged", covering every case that must
/// NOT be touched:
/// * the blob already carries `_owner` or `_owner_id` (an explicit
///   caller-supplied owner, including an `agent-utilities`-style write, is
///   NEVER overridden — this is additive-only, exactly like the read-side
///   fallback);
/// * the blob does not decode as a string-keyed map (an undecodable blob is
///   already denied by [`row_visibility`]; stamping it would not help and
///   risks masking the real encoding error);
/// * `caller_agent_id` is empty (nothing to stamp).
///
/// Deliberately does NOT set `_visibility`/`_shared_scope` — visibility
/// breadth is caller-supplied content, not an identity fact this function
/// has any authority to invent; an absent scope keeps the pre-existing
/// bare-absent-default (`row_visibility`'s `None => shared_scope_public.
/// unwrap_or(true)`), so a stamped-but-scope-silent row stays visible to its
/// writer AND, by the same pre-existing default, to any other caller who
/// would already have seen an equivalent unstamped-but-tagged row.
///
/// Callers are expected to skip this entirely for a `System`-authority
/// caller ([`IsolationLayer::is_system`]) — System writes (bootstrap,
/// migration, internal maintenance) are not a real per-agent owner and must
/// not be stamped as one.
#[cfg(feature = "security")]
pub fn stamp_owner_id_if_absent(blob: &[u8], caller_agent_id: &str) -> Option<Vec<u8>> {
    if caller_agent_id.is_empty() {
        return None;
    }
    let mut map: std::collections::BTreeMap<String, serde_json::Value> =
        eg_types::msgpack::decode_bounded(
            blob,
            eg_types::msgpack::MsgpackLimits::new(
                eg_types::msgpack::MAX_PROPERTY_BYTES,
                eg_types::msgpack::MAX_PROPERTY_ITEMS,
                eg_types::msgpack::DEFAULT_MAX_DEPTH,
            ),
        )
        .ok()?;
    if map.contains_key(RLS_OWNER_KEY) || map.contains_key(RLS_OWNER_ID_KEY) {
        return None;
    }
    map.insert(
        RLS_OWNER_ID_KEY.to_string(),
        serde_json::Value::String(caller_agent_id.to_string()),
    );
    rmp_serde::to_vec_named(&map).ok()
}

#[cfg(feature = "security")]
impl RowVisibility {
    fn default_public() -> Self {
        RowVisibility {
            owner: None,
            public: true,
            grants: Vec::new(),
            tagged: false,
            schema: false,
        }
    }
}

/// `AgentRole` / `AgentIdentity` are defined in `eg-types::acl` (the `protocol`
/// enum's `RegisterIdentity` carries `AgentRole` over the wire, and `protocol`
/// sits below `isolation` in the DAG); re-exported here so `IsolationLayer` and
/// every call site reference `crate::isolation::AgentRole` unchanged.
pub use crate::acl::{AgentIdentity, AgentRole};

/// Isolation policy engine.
#[derive(Clone)]
pub struct IsolationLayer {
    /// Known agent identities for ACL resolution.
    agents: HashMap<String, AgentIdentity>,
    /// RBAC policy (CONCEPT:EG-KG.compute.feature): the mandatory authorization
    /// decision for every non-System graph access. An empty policy denies all.
    #[cfg(feature = "security")]
    rbac: crate::rbac::RbacPolicy,
    /// Durable one-time bootstrap lifecycle. This is independent of the current
    /// identity count so deleting identities can never reopen first-run authority.
    #[cfg(feature = "security")]
    identity_bootstrap: crate::rbac_persist::IdentityBootstrapState,
    /// RBAC/identity persistence handle
    /// (CONCEPT:EG-KG.compute.durable-rbac-identity-persistence). Current layers always
    /// carry a store: embedded layers use the atomic memory store and served layers
    /// load redb. `Arc` keeps [`IsolationLayer`] `Clone`.
    #[cfg(feature = "security")]
    persist: Option<std::sync::Arc<dyn crate::rbac_persist::RbacPolicyStore>>,
}

impl Default for IsolationLayer {
    fn default() -> Self {
        Self::new()
    }
}

impl IsolationLayer {
    pub fn new() -> Self {
        IsolationLayer {
            agents: HashMap::new(),
            #[cfg(feature = "security")]
            rbac: crate::rbac::RbacPolicy::new(),
            #[cfg(feature = "security")]
            identity_bootstrap: crate::rbac_persist::IdentityBootstrapState::Pending,
            #[cfg(feature = "security")]
            persist: Some(std::sync::Arc::new(
                crate::rbac_persist::MemoryRbacStore::new(),
            )),
        }
    }

    /// Open an [`IsolationLayer`] backed by a durable redb store at `dir`
    /// (CONCEPT:EG-KG.compute.durable-rbac-identity-persistence). Any previously-persisted RBAC policy + registered agent
    /// identities are LOADED at boot; every subsequent `add_role`/`remove_role`/
    /// `add_grant`/`remove_grant`/`register_agent`/`unregister_agent` mutation is
    /// written through to redb. A new store carries an explicit current empty,
    /// default-deny bootstrap image; a partial image fails boot.
    #[cfg(feature = "security")]
    pub fn with_persist_dir<P: AsRef<std::path::Path>>(
        dir: P,
    ) -> Result<Self, crate::rbac_persist::RbacPersistError> {
        let store = crate::rbac_persist::RbacStore::open(dir)?;
        let (rbac, identities, identity_bootstrap) = store.load()?;
        if identity_bootstrap == crate::rbac_persist::IdentityBootstrapState::Pending
            && (!identities.is_empty()
                || rbac.roles().next().is_some()
                || !rbac.grants().is_empty())
        {
            return Err(crate::rbac_persist::RbacPersistError::IncompleteState(
                "pending identity bootstrap requires an empty policy and identity map",
            ));
        }
        let agents: HashMap<String, AgentIdentity> = identities.into_iter().collect();
        Ok(IsolationLayer {
            agents,
            rbac,
            identity_bootstrap,
            persist: Some(std::sync::Arc::new(store)),
        })
    }

    /// Write through the FULL RBAC state (policy + identities) to the configured
    /// store. Absence is an invalid internal state. Secure request handlers
    /// use the fallible `try_*` mutation methods below and roll in-memory state
    /// back if this save fails, so policy changes are never acknowledged unless
    /// their durable representation committed.
    ///
    /// [`with_persist_dir`]: IsolationLayer::with_persist_dir
    #[cfg(feature = "security")]
    fn persist_state(&self) -> Result<(), String> {
        let store = self
            .persist
            .as_ref()
            .ok_or_else(|| "identity/RBAC policy store is not bound".to_string())?;
        let identities: std::collections::BTreeMap<String, AgentIdentity> = self
            .agents
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        store
            .save(&self.rbac, &identities, self.identity_bootstrap)
            .map_err(|e| format!("identity/RBAC policy save failed: {e}"))
    }

    /// Add/replace an RBAC role definition (CONCEPT:EG-KG.compute.feature); written through to the
    /// durable store when configured (CONCEPT:EG-KG.compute.durable-rbac-identity-persistence).
    #[cfg(feature = "security")]
    pub fn add_role(&mut self, role: crate::acl::Role) {
        let _ = self.try_add_role(role);
    }

    #[cfg(feature = "security")]
    pub fn try_add_role(&mut self, role: crate::acl::Role) -> Result<(), String> {
        let previous = self.rbac.clone();
        let previous_bootstrap = self.identity_bootstrap;
        self.rbac.add_role(role);
        self.identity_bootstrap = crate::rbac_persist::IdentityBootstrapState::Consumed;
        if let Err(error) = self.persist_state() {
            self.rbac = previous;
            self.identity_bootstrap = previous_bootstrap;
            return Err(error);
        }
        Ok(())
    }

    /// Remove an RBAC role definition (CONCEPT:EG-KG.compute.feature); written through (EG-303).
    #[cfg(feature = "security")]
    pub fn remove_role(&mut self, name: &str) {
        let _ = self.try_remove_role(name);
    }

    #[cfg(feature = "security")]
    pub fn try_remove_role(&mut self, name: &str) -> Result<(), String> {
        let previous = self.rbac.clone();
        let previous_bootstrap = self.identity_bootstrap;
        self.rbac.remove_role(name);
        self.identity_bootstrap = crate::rbac_persist::IdentityBootstrapState::Consumed;
        if let Err(error) = self.persist_state() {
            self.rbac = previous;
            self.identity_bootstrap = previous_bootstrap;
            return Err(error);
        }
        Ok(())
    }

    /// Add an RBAC grant (CONCEPT:EG-KG.compute.feature); written through (EG-303).
    #[cfg(feature = "security")]
    pub fn add_grant(&mut self, grant: crate::acl::Grant) {
        let _ = self.try_add_grant(grant);
    }

    #[cfg(feature = "security")]
    pub fn try_add_grant(&mut self, grant: crate::acl::Grant) -> Result<(), String> {
        let previous = self.rbac.clone();
        let previous_bootstrap = self.identity_bootstrap;
        self.rbac.add_grant(grant);
        self.identity_bootstrap = crate::rbac_persist::IdentityBootstrapState::Consumed;
        if let Err(error) = self.persist_state() {
            self.rbac = previous;
            self.identity_bootstrap = previous_bootstrap;
            return Err(error);
        }
        Ok(())
    }

    /// Remove an RBAC grant (CONCEPT:EG-KG.compute.feature). Returns true when one was removed;
    /// written through (EG-303).
    #[cfg(feature = "security")]
    pub fn remove_grant(&mut self, grant: &crate::acl::Grant) -> bool {
        self.try_remove_grant(grant).unwrap_or(false)
    }

    #[cfg(feature = "security")]
    pub fn try_remove_grant(&mut self, grant: &crate::acl::Grant) -> Result<bool, String> {
        let previous = self.rbac.clone();
        let previous_bootstrap = self.identity_bootstrap;
        let removed = self.rbac.remove_grant(grant);
        if removed {
            self.identity_bootstrap = crate::rbac_persist::IdentityBootstrapState::Consumed;
            if let Err(error) = self.persist_state() {
                self.rbac = previous;
                self.identity_bootstrap = previous_bootstrap;
                return Err(error);
            }
        }
        Ok(removed)
    }

    /// Read-only access to the RBAC policy (for admin `List` / persistence).
    #[cfg(feature = "security")]
    pub fn rbac(&self) -> &crate::rbac::RbacPolicy {
        &self.rbac
    }

    /// Whether this layer still represents the exact pristine first-run image.
    /// Bootstrap state is durable and independent from identity count, so an
    /// administrator cannot recreate first-run authority by removing identities.
    #[cfg(feature = "security")]
    pub fn identity_bootstrap_pending(&self) -> bool {
        self.identity_bootstrap == crate::rbac_persist::IdentityBootstrapState::Pending
            && self.agents.is_empty()
            && self.rbac.roles().next().is_none()
            && self.rbac.grants().is_empty()
    }

    #[cfg(not(feature = "security"))]
    pub fn identity_bootstrap_pending(&self) -> bool {
        false
    }

    /// Atomically consume first-run authority and persist its sole System identity.
    /// Request-envelope, graph, self-registration, and signature checks live at the
    /// served boundary; this layer independently enforces the identity shape and
    /// one-time durable transition while holding the caller's state write lock.
    #[cfg(feature = "security")]
    pub fn try_bootstrap_system_identity(&mut self, identity: AgentIdentity) -> Result<(), String> {
        if !self.identity_bootstrap_pending() {
            return Err("ACCESS_DENIED: identity bootstrap is not pending".to_string());
        }
        if identity.agent_id.trim().is_empty()
            || !matches!(&identity.role, AgentRole::System)
            || !identity.teams.is_empty()
            || !identity.roles.is_empty()
        {
            return Err(
                "ACCESS_DENIED: bootstrap requires a non-empty System identity with no teams or roles"
                    .to_string(),
            );
        }

        let agent_id = identity.agent_id.clone();
        self.agents.insert(agent_id.clone(), identity);
        self.identity_bootstrap = crate::rbac_persist::IdentityBootstrapState::Consumed;
        if let Err(error) = self.persist_state() {
            self.agents.remove(&agent_id);
            self.identity_bootstrap = crate::rbac_persist::IdentityBootstrapState::Pending;
            return Err(error);
        }
        Ok(())
    }

    #[cfg(not(feature = "security"))]
    pub fn try_bootstrap_system_identity(
        &mut self,
        _identity: AgentIdentity,
    ) -> Result<(), String> {
        Err("ACCESS_DENIED: identity bootstrap requires the security feature".to_string())
    }

    /// Register or update an agent identity; written through to the durable store
    /// when configured (CONCEPT:EG-KG.compute.durable-rbac-identity-persistence).
    pub fn register_agent(&mut self, identity: AgentIdentity) {
        let _ = self.try_register_agent(identity);
    }

    /// Fallible registration used by secure handlers. When a durable adapter is
    /// configured, an unsuccessful commit restores the prior in-memory identity.
    pub fn try_register_agent(&mut self, identity: AgentIdentity) -> Result<(), String> {
        let agent_id = identity.agent_id.clone();
        let previous = self.agents.insert(agent_id.clone(), identity);
        #[cfg(feature = "security")]
        {
            let previous_bootstrap = self.identity_bootstrap;
            self.identity_bootstrap = crate::rbac_persist::IdentityBootstrapState::Consumed;
            if let Err(error) = self.persist_state() {
                match previous {
                    Some(identity) => {
                        self.agents.insert(agent_id, identity);
                    }
                    None => {
                        self.agents.remove(&agent_id);
                    }
                }
                self.identity_bootstrap = previous_bootstrap;
                return Err(error);
            }
        }
        #[cfg(not(feature = "security"))]
        let _ = previous;
        Ok(())
    }

    /// Remove an agent identity; written through (CONCEPT:EG-KG.compute.durable-rbac-identity-persistence).
    pub fn unregister_agent(&mut self, agent_id: &str) {
        let _ = self.try_unregister_agent(agent_id);
    }

    pub fn try_unregister_agent(&mut self, agent_id: &str) -> Result<bool, String> {
        let previous = self.agents.remove(agent_id);
        let removed = previous.is_some();
        #[cfg(feature = "security")]
        if removed {
            if let Err(error) = self.persist_state() {
                if let Some(identity) = previous {
                    self.agents.insert(agent_id.to_string(), identity);
                }
                return Err(error);
            }
        }
        #[cfg(not(feature = "security"))]
        let _ = removed;
        Ok(removed)
    }

    /// True once any identity has been registered. Served graph access requires
    /// provisioned identities; an empty policy is rejected at the boundary.
    pub fn has_rules(&self) -> bool {
        !self.agents.is_empty()
    }

    /// Whether `agent_id` is a known, provisioned identity — independent of any
    /// particular graph. [`Self::check_access`] denies an unregistered agent
    /// unconditionally, before it ever consults a graph's type/owner
    /// (`self.agents.get(agent_id)` is the very first thing it checks), so this is
    /// exactly that identity-only slice of the decision, usable by a caller that
    /// does not yet know (or want to reveal) whether the target graph exists.
    pub fn is_registered(&self, agent_id: &str) -> bool {
        self.agents.contains_key(agent_id)
    }

    /// Check if an agent has the requested access level to a graph.
    pub fn check_access(
        &self,
        agent_id: &str,
        graph_name: &str,
        graph_type: GraphType,
        graph_owner: Option<&str>,
        access: AccessLevel,
    ) -> bool {
        // Served access is current-policy-only: an actor must resolve to a
        // provisioned durable identity before any graph-type rule is evaluated.
        let Some(identity) = self.agents.get(agent_id) else {
            return false;
        };
        if identity.role == AgentRole::System {
            return true;
        }

        // RBAC (CONCEPT:EG-KG.compute.feature) is the mandatory current decision for
        // non-System identities. Missing roles, an empty policy, or no matching grant
        // are all default-deny; there is no pre-RBAC ACL fall-through.
        #[cfg(feature = "security")]
        {
            let _ = (graph_type, graph_owner);
            let ctx = crate::acl::ResourceContext::graph(graph_name);
            let action = match access {
                AccessLevel::Read => crate::acl::RbacAction::Read,
                AccessLevel::Write => crate::acl::RbacAction::Write,
            };
            matches!(
                self.rbac.evaluate(&identity.roles, &ctx, action),
                Some(crate::acl::GrantEffect::Allow)
            )
        }

        #[cfg(not(feature = "security"))]
        match graph_type {
            // Bus: all authenticated agents have full access.
            GraphType::Commons => true,

            // Global: read-only for all agents.
            GraphType::Global => access == AccessLevel::Read,

            // Agent graph: owner has full access, manager of owner has full access,
            // all others denied.
            GraphType::Agent => {
                // Owner always has access.
                if graph_owner == Some(agent_id) {
                    return true;
                }
                // Check if requester is a manager of the owner.
                if let Some(owner_id) = graph_owner {
                    if self.is_manager_of(agent_id, owner_id) {
                        return true;
                    }
                }
                false
            }

            // Team graph: members read, manager R/W.
            GraphType::Team => {
                let team_name = graph_name.strip_prefix("team:").unwrap_or(graph_name);
                let identity = match self.agents.get(agent_id) {
                    Some(id) => id,
                    None => return false,
                };

                // Check membership.
                let is_member = identity.teams.contains(&team_name.to_string());
                if !is_member {
                    return false;
                }

                match access {
                    AccessLevel::Read => true,
                    AccessLevel::Write => {
                        // Only managers can write to team graphs.
                        matches!(identity.role, AgentRole::Manager { .. })
                    }
                }
            }
        }
    }

    /// Check if `agent_id` is a manager of `subordinate_id`.
    fn is_manager_of(&self, agent_id: &str, subordinate_id: &str) -> bool {
        if let Some(identity) = self.agents.get(agent_id) {
            if let AgentRole::Manager { subordinates } = &identity.role {
                return subordinates.contains(&subordinate_id.to_string());
            }
        }
        false
    }

    /// Is `agent_id` registered with [`AgentRole::System`] — the same bypass
    /// [`can_see_row`](Self::can_see_row) checks first, exposed standalone for
    /// write-side callers (BUG-193) that need to know whether to exempt a
    /// caller from owner-stamping without duplicating the private `agents`
    /// lookup. An unregistered `agent_id` is never `System`.
    #[cfg(feature = "security")]
    pub fn is_system(&self, agent_id: &str) -> bool {
        self.agents
            .get(agent_id)
            .is_some_and(|identity| identity.role == AgentRole::System)
    }

    /// Per-agent Row-Level Security (CONCEPT:EG-KG.sharding.row-level-security): may `agent_id` SEE one
    /// node, given that node's owner + visibility convention?
    ///
    /// Visibility convention (carried in the node's property blob; read by
    /// [`row_visibility`]):
    /// * `_owner`      — the owning agent_id (absent requires explicit public visibility).
    /// * `_visibility` — `"public"` (default when absent) or `"private"`.
    /// * `_grants`     — optional comma-separated agent_ids explicitly granted read.
    ///
    /// Default-deny decision (CONCEPT:EG-KG.sharding.row-level-security, EG-P0-6):
    /// an unowned row is visible only when it explicitly carries
    /// `_visibility: "public"` metadata
    /// ([`RowVisibility::tagged`] `&&` [`RowVisibility::public`]); an unowned row
    /// that is undecodable or declares no RLS metadata is denied. Owner, grant,
    /// manager, and System rules remain explicit authorization paths.
    #[cfg(feature = "security")]
    pub fn can_see_row(&self, agent_id: &str, vis: &RowVisibility) -> bool {
        // System role sees everything.
        if let Some(identity) = self.agents.get(agent_id) {
            if identity.role == AgentRole::System {
                return true;
            }
        }
        // A18: ontology SCHEMA (TBox) is exempt from ROW-level default-deny —
        // see `RLS_SCHEMA_KEY` for the full ABox/TBox distinction. This does
        // NOT widen visibility for any other untagged row: `vis.schema` is
        // only ever `true` when `eg_rdf`'s lowering structurally identified
        // the row as a class/property/axiom node, never merely because it is
        // untagged/unowned (that case still falls through to the
        // `vis.tagged && vis.public` default-deny check below, unchanged).
        // Graph-level ACL (`server::access::check_graph_access`), enforced
        // upstream of every caller that reaches row filtering at all, is
        // completely unaffected — a caller with no read access to this graph
        // never reaches this function in the first place.
        if vis.schema {
            return true;
        }
        let owner = match &vis.owner {
            None => return vis.tagged && vis.public,
            Some(o) => o.as_str(),
        };
        if vis.public {
            return true;
        }
        if owner == agent_id {
            return true;
        }
        if vis.grants.iter().any(|g| g == agent_id) {
            return true;
        }
        if self.is_manager_of(agent_id, owner) {
            return true;
        }
        false
    }

    /// Filter a [`GraphView`](crate::graph::GraphView) IN-PLACE down to only the
    /// rows `agent_id` may see (CONCEPT:EG-KG.sharding.row-level-security — RLS in the read/plan path).
    ///
    /// This runs on the owned, off-lock snapshot the query planner (SQL / Cypher /
    /// SPARQL / unified) consumes — NOT at the graph boundary — so NO query surface
    /// can exfiltrate a forbidden row: a hidden node is removed from the view's
    /// topology, node-map, and property map, and every edge incident to a removed
    /// node is dropped too (an edge to an invisible node would otherwise leak its
    /// existence). Default-deny remains active even before identities are provisioned.
    #[cfg(feature = "security")]
    pub fn filter_view(&self, agent_id: &str, view: &mut crate::graph::GraphView) {
        // Decide visibility for EVERY topology node. A topology row with no
        // property blob is untagged and must be hidden like an undecodable row.
        let hidden: HashSet<String> = view
            .node_map
            .keys()
            .filter_map(|id| {
                let mut vis = view
                    .node_properties
                    .get(id)
                    .map(|blob| row_visibility(blob))
                    .unwrap_or_else(RowVisibility::default_public);
                // BUG A3 (2026-08-12): TBox membership is DERIVED from this
                // snapshot's live reverse index (`view.schema_node_ids`,
                // populated from `GraphCore::schema_refs` at snapshot time),
                // never decoded from the blob (`row_visibility`'s `.schema`
                // is always `false`) — so an axiom's deletion, which drops
                // `id` from `schema_refs`, is reflected on the very NEXT
                // snapshot with no separate "clear the marker" step.
                vis.schema = view.schema_node_ids.contains(id);
                if self.can_see_row(agent_id, &vis) {
                    None
                } else {
                    Some(id.clone())
                }
            })
            .collect();
        // `hidden` includes topology-only rows under strict/default-deny, not just
        // rows represented in the property map.
        if hidden.is_empty() {
            return;
        }
        // Drop hidden nodes from the petgraph topology (StableDiGraph keeps other
        // indices valid) + the node_map + node_properties.
        for id in &hidden {
            if let Some(idx) = view.node_map.remove(id) {
                view.graph.remove_node(idx);
            }
            view.node_properties.remove(id);
        }
        // Drop any edge touching a hidden endpoint (do not leak its existence).
        view.edge_properties
            .retain(|(s, t), _| !hidden.contains(s) && !hidden.contains(t));
    }

    /// Does `agent_id` hold ADMIN capability (CONCEPT:EG-KG.compute.feature, EG-P0-6)?
    ///
    /// Used to gate the system-wide administrative methods (`RegisterIdentity`,
    /// `RbacAdmin`, `ApplyMultisigMutation`, the M3 reshard/rebalance/catalog
    /// family, backup/restore) — see `server::access::require_admin_capability`,
    /// which drives WHICH methods this applies to from `eg_capabilities::policy`'s
    /// `authz_action` rather than a second hardcoded list.
    ///
    /// `System` role always qualifies. Otherwise, under the `security` feature, an
    /// explicit RBAC grant of `RbacAction::Admin` for one of the agent's roles
    /// (typically scoped `ResourceSelector::All`, since admin actions are not
    /// graph-scoped) — evaluated against a fixed, non-graph resource context so a
    /// grant written for a specific graph never accidentally satisfies a global
    /// admin check. Without `security` compiled in there is no RBAC evaluator to
    /// consult, so only `System` qualifies.
    pub fn has_admin_capability(&self, agent_id: &str) -> bool {
        if let Some(identity) = self.agents.get(agent_id) {
            if identity.role == AgentRole::System {
                return true;
            }
        }
        #[cfg(feature = "security")]
        {
            if let Some(identity) = self.agents.get(agent_id) {
                let ctx = crate::acl::ResourceContext::graph("__admin__");
                return matches!(
                    self.rbac
                        .evaluate(&identity.roles, &ctx, crate::acl::RbacAction::Admin),
                    Some(crate::acl::GrantEffect::Allow)
                );
            }
        }
        false
    }

    /// Get all agent IDs that a given agent can access.
    pub fn accessible_graphs(&self, agent_id: &str) -> HashSet<String> {
        let mut accessible = HashSet::new();
        accessible.insert("__commons__".to_string());

        if let Some(identity) = self.agents.get(agent_id) {
            // Own agent graph.
            accessible.insert(format!("agent:{}", agent_id));

            // Team graphs (read).
            for team in &identity.teams {
                accessible.insert(format!("team:{}", team));
            }

            // Subordinate graphs (if manager).
            if let AgentRole::Manager { subordinates } = &identity.role {
                for sub in subordinates {
                    accessible.insert(format!("agent:{}", sub));
                }
            }
        }

        accessible
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup() -> IsolationLayer {
        let mut layer = IsolationLayer::new();
        layer.register_agent(AgentIdentity {
            agent_id: "manager".to_string(),
            role: AgentRole::Manager {
                subordinates: vec!["worker1".to_string(), "worker2".to_string()],
            },
            teams: vec!["alpha".to_string()],
            roles: vec![],
        });
        layer.register_agent(AgentIdentity {
            agent_id: "worker1".to_string(),
            role: AgentRole::Agent,
            teams: vec!["alpha".to_string()],
            roles: vec![],
        });
        layer.register_agent(AgentIdentity {
            agent_id: "worker2".to_string(),
            role: AgentRole::Agent,
            teams: vec!["alpha".to_string()],
            roles: vec![],
        });
        layer
    }

    /// BUG A3 diagnostic (2026-08-12): isolate the derive-from-live-reverse-
    /// index mechanism from the whole SPARQL/pgwire stack -- mark two nodes
    /// schema directly on a `GraphCore`, snapshot + `filter_view` for an
    /// ordinary non-System actor, and confirm they survive (schema-exempt).
    /// Then unmark and confirm they revert to ABox default-deny on the NEXT
    /// snapshot, with no separate "clear" step.
    #[test]
    fn schema_ref_marked_node_survives_filter_view_then_reverts_on_unmark() {
        let mut layer = IsolationLayer::new();
        layer.register_agent(AgentIdentity {
            agent_id: "worker1".to_string(),
            role: AgentRole::Agent,
            teams: vec![],
            roles: vec![],
        });
        let core = crate::graph::GraphCore::new();
        core.add_node("<http://ex/Sensor>".to_string(), Vec::new());
        core.add_node("<http://ex/Device>".to_string(), Vec::new());
        core.mark_schema_ref("<http://ex/Sensor>");
        core.mark_schema_ref("<http://ex/Device>");

        let mut view = core.analysis_snapshot();
        assert!(
            view.schema_node_ids.contains("<http://ex/Sensor>"),
            "schema_node_ids must reflect the live mark BEFORE filter_view runs"
        );
        layer.filter_view("worker1", &mut view);
        assert!(
            view.node_map.contains_key("<http://ex/Sensor>"),
            "a schema-marked node must survive filter_view for a non-System actor"
        );
        assert!(view.node_map.contains_key("<http://ex/Device>"));

        core.unmark_schema_ref("<http://ex/Sensor>");
        core.unmark_schema_ref("<http://ex/Device>");
        let mut view2 = core.analysis_snapshot();
        assert!(
            !view2.schema_node_ids.contains("<http://ex/Sensor>"),
            "schema_node_ids must drop the id once its count reaches 0"
        );
        layer.filter_view("worker1", &mut view2);
        assert!(
            !view2.node_map.contains_key("<http://ex/Sensor>"),
            "must revert to ABox default-deny once unmarked -- no separate clear step"
        );
        assert!(!view2.node_map.contains_key("<http://ex/Device>"));
    }

    #[test]
    #[cfg(not(feature = "security"))]
    fn test_bus_access_for_all() {
        let layer = setup();
        assert!(layer.check_access(
            "worker1",
            "__commons__",
            GraphType::Commons,
            None,
            AccessLevel::Write
        ));
        assert!(layer.check_access(
            "manager",
            "__commons__",
            GraphType::Commons,
            None,
            AccessLevel::Read
        ));
    }

    #[test]
    #[cfg(not(feature = "security"))]
    fn test_agent_graph_owner_access() {
        let layer = setup();
        assert!(layer.check_access(
            "worker1",
            "agent:worker1",
            GraphType::Agent,
            Some("worker1"),
            AccessLevel::Write
        ));
    }

    #[test]
    fn test_agent_graph_peer_denied() {
        let layer = setup();
        assert!(!layer.check_access(
            "worker2",
            "agent:worker1",
            GraphType::Agent,
            Some("worker1"),
            AccessLevel::Read
        ));
    }

    #[test]
    #[cfg(not(feature = "security"))]
    fn test_manager_access_to_subordinate() {
        let layer = setup();
        assert!(layer.check_access(
            "manager",
            "agent:worker1",
            GraphType::Agent,
            Some("worker1"),
            AccessLevel::Write
        ));
    }

    #[test]
    #[cfg(not(feature = "security"))]
    fn test_team_member_read_only() {
        let layer = setup();
        assert!(layer.check_access(
            "worker1",
            "team:alpha",
            GraphType::Team,
            None,
            AccessLevel::Read
        ));
        assert!(!layer.check_access(
            "worker1",
            "team:alpha",
            GraphType::Team,
            None,
            AccessLevel::Write
        ));
    }

    #[test]
    #[cfg(not(feature = "security"))]
    fn test_team_manager_can_write() {
        let layer = setup();
        assert!(layer.check_access(
            "manager",
            "team:alpha",
            GraphType::Team,
            None,
            AccessLevel::Write
        ));
    }

    #[test]
    #[cfg(not(feature = "security"))]
    fn test_global_read_only() {
        let layer = setup();
        assert!(layer.check_access(
            "worker1",
            "global:ontology",
            GraphType::Global,
            None,
            AccessLevel::Read
        ));
        assert!(!layer.check_access(
            "worker1",
            "global:ontology",
            GraphType::Global,
            None,
            AccessLevel::Write
        ));
    }

    // ── Per-agent Row-Level Security (CONCEPT:EG-KG.sharding.row-level-security) ──────────────────
    #[cfg(feature = "security")]
    mod rls {
        use super::*;
        use crate::graph::GraphView;

        /// Build a node property blob from a list of (key,value) string pairs.
        fn props(pairs: &[(&str, &str)]) -> std::sync::Arc<Vec<u8>> {
            let map: std::collections::BTreeMap<String, String> = pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
            std::sync::Arc::new(rmp_serde::to_vec_named(&map).unwrap())
        }

        /// A 3-node view: B's private node owned by worker2, a public node, an
        /// unowned node. (Topology only carries the node ids; properties carry RLS.)
        fn view() -> GraphView {
            let mut v = GraphView::default();
            for id in ["b_private", "shared_public", "untagged"] {
                let idx = v.graph.add_node(id.to_string());
                v.node_map.insert(id.to_string(), idx);
            }
            v.node_properties.insert(
                "b_private".to_string(),
                props(&[("_owner", "worker2"), ("_visibility", "private")]),
            );
            v.node_properties.insert(
                "shared_public".to_string(),
                props(&[("_owner", "worker2"), ("_visibility", "public")]),
            );
            v.node_properties
                .insert("untagged".to_string(), props(&[("name", "x")]));
            // An edge from B's private node to the public node — must be dropped for A.
            v.edge_properties
                .insert(("b_private".into(), "shared_public".into()), vec![]);
            v
        }

        #[test]
        fn agent_a_cannot_see_agent_b_private_node() {
            let layer = setup();
            let mut va = view();
            layer.filter_view("worker1", &mut va);
            // worker1 sees the explicitly public node, not the private or untagged rows.
            assert!(!va.node_properties.contains_key("b_private"));
            assert!(va.node_properties.contains_key("shared_public"));
            assert!(!va.node_properties.contains_key("untagged"));
            assert!(!va.node_map.contains_key("b_private"));
            // The edge touching the hidden node is dropped (no existence leak).
            assert!(!va
                .edge_properties
                .contains_key(&("b_private".to_string(), "shared_public".to_string())));
        }

        #[test]
        fn owner_b_sees_own_private_node() {
            let layer = setup();
            let mut vb = view();
            layer.filter_view("worker2", &mut vb);
            assert!(vb.node_properties.contains_key("b_private"));
            assert!(vb.node_properties.contains_key("shared_public"));
        }

        #[test]
        fn manager_sees_subordinate_private_node() {
            let layer = setup();
            let mut vm = view();
            layer.filter_view("manager", &mut vm);
            // manager manages worker2 ⇒ sees its private node.
            assert!(vm.node_properties.contains_key("b_private"));
        }

        #[test]
        fn explicit_grant_is_honored() {
            let mut layer = setup();
            layer.register_agent(AgentIdentity {
                agent_id: "auditor".to_string(),
                role: AgentRole::Agent,
                teams: vec![],
                roles: vec![],
            });
            let mut v = GraphView::default();
            let idx = v.graph.add_node("g".to_string());
            v.node_map.insert("g".to_string(), idx);
            v.node_properties.insert(
                "g".to_string(),
                props(&[
                    ("_owner", "worker2"),
                    ("_visibility", "private"),
                    ("_grants", "auditor, someone_else"),
                ]),
            );
            layer.filter_view("auditor", &mut v);
            assert!(v.node_properties.contains_key("g"), "grant ignored");
        }

        #[test]
        fn no_identities_still_enforces_row_tags() {
            let layer = IsolationLayer::new(); // no identities
            let mut v = view();
            layer.filter_view("anyone", &mut v);
            assert_eq!(v.node_properties.len(), 1);
            assert!(v.node_properties.contains_key("shared_public"));
        }

        // ── RLS default-deny posture (CONCEPT:EG-KG.sharding.row-level-security, EG-P0-6) ──
        mod default_deny {
            use super::*;

            #[test]
            fn untagged_unowned_row_is_hidden() {
                let layer = setup();
                let mut v = view();
                layer.filter_view("worker1", &mut v);
                assert!(
                    !v.node_properties.contains_key("untagged"),
                    "default-deny must hide an untagged row"
                );
            }

            #[test]
            fn no_identities_hides_topology_only_row() {
                let layer = IsolationLayer::new();
                assert!(!layer.has_rules());
                let mut v = GraphView::default();
                let idx = v.graph.add_node("topology_only".to_string());
                v.node_map.insert("topology_only".to_string(), idx);

                layer.filter_view("unregistered", &mut v);

                assert!(!v.node_map.contains_key("topology_only"));
                assert_eq!(v.graph.node_count(), 0);
            }

            #[test]
            fn undecodable_blob_is_hidden() {
                let layer = setup();
                let mut v = GraphView::default();
                let idx = v.graph.add_node("garbage".to_string());
                v.node_map.insert("garbage".to_string(), idx);
                v.node_properties.insert(
                    "garbage".to_string(),
                    std::sync::Arc::new(vec![0xFF, 0x00, 0x01]),
                );
                layer.filter_view("worker1", &mut v);
                assert!(
                    !v.node_properties.contains_key("garbage"),
                    "default-deny must reject an undecodable blob"
                );
            }

            #[test]
            fn explicit_grant_makes_a_bare_visibility_tagged_unowned_row_visible() {
                // BUG-064: a bare `_visibility='public'` tag on a fully
                // unowned row, with NOTHING else corroborating it, is
                // exactly the shape the incident's blanket mis-stamp
                // produced (see `unowned_bare_visibility_tag_alone_is_no_longer_trusted`
                // below) and is no longer sufficient on its own. An
                // explicit `_grants` entry naming the caller is still
                // honored — that IS a genuine per-row ACL decision.
                let layer = setup();
                let mut v = GraphView::default();
                let idx = v.graph.add_node("shared".to_string());
                v.node_map.insert("shared".to_string(), idx);
                v.node_properties.insert(
                    "shared".to_string(),
                    props(&[("_visibility", "public"), ("_grants", "worker1")]),
                );
                layer.filter_view("worker1", &mut v);
                assert!(
                    v.node_properties.contains_key("shared"),
                    "a public tag corroborated by an explicit grant must remain visible"
                );
            }

            #[test]
            fn unowned_bare_visibility_tag_alone_is_no_longer_trusted() {
                // BUG-064 (BUG-LEDGER.md#BUG-064): reproduces the exact shape
                // of the 21,064-row exposure — a fully unowned row whose ONLY
                // RLS signal is a bare native `_visibility='public'` key, with
                // no `_grants` and no au (`_owner_id`/`_shared_scope`) tag.
                // `_visibility` has zero known legitimate production writers
                // in either repo; the only real-world instance of this exact
                // shape was an out-of-band admin RPC that blanket-stamped it
                // onto every previously-untagged node. It must now be denied
                // to a non-System caller.
                let layer = setup();
                let mut v = GraphView::default();
                let idx = v.graph.add_node("mis_stamped".to_string());
                v.node_map.insert("mis_stamped".to_string(), idx);
                v.node_properties.insert(
                    "mis_stamped".to_string(),
                    props(&[("_visibility", "public")]),
                );
                layer.filter_view("worker1", &mut v);
                assert!(
                    !v.node_properties.contains_key("mis_stamped"),
                    "an unowned row whose only signal is a bare `_visibility` tag must stay hidden (BUG-064)"
                );
            }

            #[test]
            fn owner_and_manager_rules_remain_explicit() {
                let layer = setup();
                let mut vb = view();
                layer.filter_view("worker2", &mut vb);
                assert!(vb.node_properties.contains_key("b_private"));
                assert!(vb.node_properties.contains_key("shared_public"));

                let mut vm = view();
                layer.filter_view("manager", &mut vm);
                assert!(vm.node_properties.contains_key("b_private"));
            }

            #[test]
            fn direct_row_decision_requires_explicit_public_tag() {
                let layer = setup();
                let untagged = RowVisibility {
                    owner: None,
                    public: true,
                    grants: Vec::new(),
                    tagged: false,
                    schema: false,
                };
                assert!(
                    !layer.can_see_row("worker1", &untagged),
                    "untagged rows remain denied even when decoded visibility defaults public"
                );

                let explicit_public = RowVisibility {
                    owner: None,
                    public: true,
                    grants: Vec::new(),
                    tagged: true,
                    schema: false,
                };
                assert!(
                    layer.can_see_row("worker1", &explicit_public),
                    "explicit public metadata grants visibility"
                );
            }
        }

        // ── BUG-052 / GOC-61: read-both compatibility with agent-utilities'
        // `_owner_id`/`_shared_scope` convention (`tenant_sharing.py`) ───────
        mod bug_052_read_both_compat {
            use super::*;

            #[test]
            fn owner_id_private_node_is_owner_and_manager_visible_only() {
                let layer = setup();
                let mut v = GraphView::default();
                let idx = v.graph.add_node("au_private".to_string());
                v.node_map.insert("au_private".to_string(), idx);
                v.node_properties.insert(
                    "au_private".to_string(),
                    props(&[("_owner_id", "worker2"), ("_shared_scope", "private")]),
                );

                // Non-owner, non-manager: denied — this is the exact live gap
                // BUG-052 named: before read-both, this node was invisible to
                // `can_see_row` as an OWNED row (no `_owner` key) and fell
                // through to the untagged/default-deny branch for worker1
                // (denied) but would have been silently treated as PUBLIC for
                // any identity landing in the tagged-absent-visibility branch —
                // either way, disagreeing with `tenant_sharing.py`'s verdict
                // that this node is private to worker2.
                let mut v1 = v.clone();
                layer.filter_view("worker1", &mut v1);
                assert!(
                    !v1.node_properties.contains_key("au_private"),
                    "an agent-utilities-tagged private node must stay hidden from a non-owner"
                );

                // Owner sees it.
                let mut v2 = v.clone();
                layer.filter_view("worker2", &mut v2);
                assert!(v2.node_properties.contains_key("au_private"));

                // Owner's manager sees it (manager-of-owner rule composes
                // unchanged with the fallback-derived owner).
                let mut v3 = v.clone();
                layer.filter_view("manager", &mut v3);
                assert!(v3.node_properties.contains_key("au_private"));
            }

            #[test]
            fn shared_scope_org_and_commons_are_visible_beyond_owner() {
                let layer = setup();
                for scope in ["org", "commons"] {
                    let mut v = GraphView::default();
                    let idx = v.graph.add_node("au_shared".to_string());
                    v.node_map.insert("au_shared".to_string(), idx);
                    v.node_properties.insert(
                        "au_shared".to_string(),
                        props(&[("_owner_id", "worker2"), ("_shared_scope", scope)]),
                    );
                    layer.filter_view("worker1", &mut v);
                    assert!(
                        v.node_properties.contains_key("au_shared"),
                        "_shared_scope={scope} must be visible beyond the owner"
                    );
                }
            }

            #[test]
            fn shared_scope_private_denies_non_owner_even_with_no_native_visibility_key() {
                let layer = setup();
                let mut v = GraphView::default();
                let idx = v.graph.add_node("au_private2".to_string());
                v.node_map.insert("au_private2".to_string(), idx);
                // Owner tagged natively (`_owner`), scope tagged the
                // agent-utilities way (`_shared_scope`) — a mixed-convention
                // row, which must still resolve correctly from either side.
                v.node_properties.insert(
                    "au_private2".to_string(),
                    props(&[("_owner", "worker2"), ("_shared_scope", "private")]),
                );
                layer.filter_view("worker1", &mut v);
                assert!(!v.node_properties.contains_key("au_private2"));
            }

            #[test]
            fn shared_scope_wins_over_a_conflicting_native_visibility_public_tag() {
                // BUG-064 (BUG-LEDGER.md#BUG-064): this test used to assert
                // the OPPOSITE — that native `_visibility=public` overrides a
                // conflicting `_shared_scope=private` — and that precedence
                // is exactly the mechanism the incident exploited: an
                // out-of-band write that only ever sets `_visibility` was
                // able to silently invert a legitimate `_shared_scope`
                // private designation on every row it touched. `_shared_scope`
                // is the one convention with a known legitimate production
                // writer (`tenant_sharing.stamp_ownership`) for a row that
                // carries it at all, so it now governs whenever present,
                // regardless of what an untrusted `_visibility` key claims.
                let layer = setup();
                let mut v = GraphView::default();
                let idx = v.graph.add_node("mixed".to_string());
                v.node_map.insert("mixed".to_string(), idx);
                v.node_properties.insert(
                    "mixed".to_string(),
                    props(&[
                        ("_owner", "worker2"),
                        ("_visibility", "public"),
                        ("_shared_scope", "private"),
                    ]),
                );
                layer.filter_view("worker1", &mut v);
                assert!(
                    !v.node_properties.contains_key("mixed"),
                    "`_shared_scope=private` must win over a conflicting native `_visibility=public` (BUG-064)"
                );

                // The owner and their manager remain unaffected either way.
                let mut vb = GraphView::default();
                let idxb = vb.graph.add_node("mixed".to_string());
                vb.node_map.insert("mixed".to_string(), idxb);
                vb.node_properties.insert(
                    "mixed".to_string(),
                    props(&[
                        ("_owner", "worker2"),
                        ("_visibility", "public"),
                        ("_shared_scope", "private"),
                    ]),
                );
                layer.filter_view("worker2", &mut vb);
                assert!(vb.node_properties.contains_key("mixed"));
            }

            #[test]
            fn owner_id_alone_tags_the_row_as_owned_not_untagged() {
                // A row carrying ONLY `_owner_id` (no `_visibility`/`_shared_scope`
                // at all) must resolve via the pre-existing bare-absent-visibility
                // default (public=true), exactly as a native `_owner`-only row
                // already did — read-both must not change that default.
                let vis = row_visibility(&props_bytes(&[("_owner_id", "worker2")]));
                assert!(vis.tagged, "an `_owner_id` key alone must count as tagged");
                assert_eq!(vis.owner.as_deref(), Some("worker2"));
                assert!(
                    vis.public,
                    "bare-absent visibility still defaults to public"
                );
            }

            #[test]
            fn owner_id_and_grants_compose_across_conventions() {
                // `_grants` (native-only — agent-utilities has no per-node grant
                // list yet) must still apply to a row whose owner is tagged the
                // agent-utilities way.
                let mut layer = setup();
                layer.register_agent(AgentIdentity {
                    agent_id: "auditor".to_string(),
                    role: AgentRole::Agent,
                    teams: vec![],
                    roles: vec![],
                });
                let mut v = GraphView::default();
                let idx = v.graph.add_node("g2".to_string());
                v.node_map.insert("g2".to_string(), idx);
                v.node_properties.insert(
                    "g2".to_string(),
                    props(&[
                        ("_owner_id", "worker2"),
                        ("_shared_scope", "private"),
                        ("_grants", "auditor"),
                    ]),
                );
                layer.filter_view("auditor", &mut v);
                assert!(
                    v.node_properties.contains_key("g2"),
                    "an explicit `_grants` entry must still be honored against an `_owner_id`-tagged row"
                );
            }

            fn props_bytes(pairs: &[(&str, &str)]) -> Vec<u8> {
                (*props(pairs)).clone()
            }
        }

        // ── BUG-193 follow-up: owner-stamping interacting with an explicit
        // native `_visibility` tag must not silently downgrade a caller's
        // stated "public" intent to owner-only ─────────────────────────────
        mod bug193_stamped_visibility_interaction {
            use super::*;

            fn props_bytes(pairs: &[(&str, &str)]) -> Vec<u8> {
                (*props(pairs)).clone()
            }

            /// A native caller writes `{_visibility: "public"}` with no
            /// ownership key; the BUG-193 write chokepoint
            /// (`stamp_owner_id_if_absent`) stamps `_owner_id` from the
            /// writer's identity, producing exactly this shape (no
            /// `_shared_scope` — the stamp never invents one). Before the
            /// companion fix in `row_visibility`, this made `au_tagged` true
            /// and the `au_tagged` branch fell back to
            /// `shared_scope_public.unwrap_or(false)` — silently downgrading
            /// the writer's explicit "public" declaration to
            /// visible-to-owner-only for every OTHER caller, even though no
            /// `_shared_scope` was ever set by anyone (a real bug found and
            /// fixed before landing, not a hypothetical). Pins the fixed
            /// behavior: an explicit native `_visibility: "public"` is
            /// trusted as corroboration when `_shared_scope` is absent, so
            /// the row stays visible to a NON-owning peer, exactly as its
            /// author intended.
            #[test]
            fn stamped_owner_with_explicit_native_public_visibility_is_visible_to_a_peer() {
                let layer = setup();
                let blob = props_bytes(&[("_visibility", "public"), ("_owner_id", "worker1")]);
                let vis = row_visibility(&blob);
                assert_eq!(vis.owner.as_deref(), Some("worker1"));
                assert!(
                    vis.public,
                    "an explicit `_visibility: public` with no `_shared_scope` to \
                     contradict it must still resolve `public`, even once `_owner_id` \
                     is present"
                );
                assert!(
                    layer.can_see_row("worker2", &vis),
                    "a peer (not the owner) must still see a row its writer explicitly \
                     marked public, even after BUG-193 stamps the writer's `_owner_id`"
                );
                assert!(
                    layer.can_see_row("worker1", &vis),
                    "the owner itself must obviously still see its own row"
                );
            }

            /// The contrasting, still-correctly-protected case: `_shared_scope`
            /// IS present (a genuine au-mediated write) and says `private` —
            /// that must still win over a conflicting native `_visibility`,
            /// unaffected by the fix above. This is the exact scenario the
            /// `au_tagged` branch exists to protect (BUG-064: the incident's
            /// blanket mis-stamp set `_visibility=public` even on
            /// `_shared_scope=private` au-owned rows), so it must still deny.
            #[test]
            fn au_shared_scope_private_still_overrides_a_conflicting_native_public_tag() {
                let layer = setup();
                let blob = props_bytes(&[
                    ("_visibility", "public"),
                    ("_owner_id", "worker1"),
                    ("_shared_scope", "private"),
                ]);
                let vis = row_visibility(&blob);
                assert!(
                    !vis.public,
                    "an explicit au `_shared_scope: private` must still override a \
                     conflicting native `_visibility: public` tag"
                );
                assert!(
                    !layer.can_see_row("worker2", &vis),
                    "a peer must not see a row whose au-authoritative scope is private"
                );
                assert!(
                    layer.can_see_row("worker1", &vis),
                    "the owner itself must still see its own private row"
                );
            }
        }
    }

    // ── RBAC-at-scale layered on check_access (CONCEPT:EG-KG.compute.feature) ───────────
    #[cfg(feature = "security")]
    mod rbac_access {
        use super::*;
        use crate::acl::{Grant, GrantEffect, RbacAction, ResourceSelector, Role};

        /// Register an agent holding `roles`.
        fn with_roles(layer: &mut IsolationLayer, id: &str, roles: Vec<String>) {
            layer.register_agent(AgentIdentity {
                agent_id: id.to_string(),
                role: AgentRole::Agent,
                teams: vec![],
                roles,
            });
        }

        #[test]
        fn empty_policy_is_default_deny() {
            let layer = setup();
            assert!(!layer.check_access(
                "worker2",
                "agent:worker1",
                GraphType::Agent,
                Some("worker1"),
                AccessLevel::Read
            ));
        }

        #[test]
        fn rbac_grant_allows_access_acl_would_deny() {
            // worker2 normally can't read worker1's agent graph. An RBAC grant on the
            // "auditor" role for that graph flips it to Allow.
            let mut layer = IsolationLayer::new();
            with_roles(&mut layer, "worker2", vec!["auditor".into()]);
            layer.add_grant(Grant {
                role: "auditor".into(),
                resource: ResourceSelector::Graph("agent:worker1".into()),
                action: RbacAction::Read,
                effect: GrantEffect::Allow,
            });
            assert!(layer.check_access(
                "worker2",
                "agent:worker1",
                GraphType::Agent,
                Some("worker1"),
                AccessLevel::Read
            ));
        }

        #[test]
        fn rbac_deny_overrides_base_allow() {
            // Owner would normally get full access to its own graph; an explicit RBAC
            // Deny on the owner's role revokes write.
            let mut layer = IsolationLayer::new();
            with_roles(&mut layer, "worker1", vec!["frozen".into()]);
            layer.add_grant(Grant {
                role: "frozen".into(),
                resource: ResourceSelector::Graph("agent:worker1".into()),
                action: RbacAction::Write,
                effect: GrantEffect::Deny,
            });
            assert!(!layer.check_access(
                "worker1",
                "agent:worker1",
                GraphType::Agent,
                Some("worker1"),
                AccessLevel::Write
            ));
        }

        #[test]
        fn rbac_hierarchy_inherited_grant_honored_by_check_access() {
            // "senior" inherits "reader"; a reader Read grant on the graph lets a
            // senior-only agent read it through check_access.
            let mut layer = IsolationLayer::new();
            with_roles(&mut layer, "sam", vec!["senior".into()]);
            layer.add_role(Role::new("reader"));
            layer.add_role(Role::with_parents("senior", vec!["reader".into()]));
            layer.add_grant(Grant {
                role: "reader".into(),
                resource: ResourceSelector::Graph("agent:other".into()),
                action: RbacAction::Read,
                effect: GrantEffect::Allow,
            });
            assert!(layer.check_access(
                "sam",
                "agent:other",
                GraphType::Agent,
                Some("other"),
                AccessLevel::Read
            ));
        }

        #[test]
        fn rbac_no_applicable_grant_is_denied() {
            let mut layer = setup();
            layer.add_grant(Grant {
                role: "auditor".into(),
                resource: ResourceSelector::Graph("agent:worker1".into()),
                action: RbacAction::Read,
                effect: GrantEffect::Allow,
            });
            assert!(!layer.check_access(
                "worker1",
                "__commons__",
                GraphType::Commons,
                None,
                AccessLevel::Write
            ));
        }

        #[test]
        fn system_role_still_bypasses_rbac_deny() {
            // System bypass precedes RBAC — a Deny cannot lock out System.
            let mut layer = IsolationLayer::new();
            layer.register_agent(AgentIdentity {
                agent_id: "root".to_string(),
                role: AgentRole::System,
                teams: vec![],
                roles: vec!["frozen".into()],
            });
            layer.add_grant(Grant {
                role: "frozen".into(),
                resource: ResourceSelector::All,
                action: RbacAction::Write,
                effect: GrantEffect::Deny,
            });
            assert!(layer.check_access(
                "root",
                "agent:anything",
                GraphType::Agent,
                Some("someone"),
                AccessLevel::Write
            ));
        }
    }

    // ── Durable RBAC/identity persistence (CONCEPT:EG-KG.compute.durable-rbac-identity-persistence) ───────────────
    #[cfg(feature = "security")]
    mod eg303_persist {
        use super::*;
        use crate::acl::{Grant, GrantEffect, RbacAction, ResourceContext, ResourceSelector, Role};
        use crate::rbac_persist::{RbacPersistError, RbacPolicyStore};
        use std::collections::BTreeMap;

        struct FailingPolicyStore;

        impl RbacPolicyStore for FailingPolicyStore {
            fn load(
                &self,
            ) -> Result<
                (
                    crate::rbac::RbacPolicy,
                    BTreeMap<String, AgentIdentity>,
                    crate::rbac_persist::IdentityBootstrapState,
                ),
                RbacPersistError,
            > {
                Ok((
                    crate::rbac::RbacPolicy::new(),
                    BTreeMap::new(),
                    crate::rbac_persist::IdentityBootstrapState::Pending,
                ))
            }

            fn save(
                &self,
                _policy: &crate::rbac::RbacPolicy,
                _identities: &BTreeMap<String, AgentIdentity>,
                _bootstrap: crate::rbac_persist::IdentityBootstrapState,
            ) -> Result<(), RbacPersistError> {
                Err(RbacPersistError::Redb("injected commit failure".into()))
            }
        }

        /// A unique temp dir per test invocation (no external dev-dep needed).
        fn tmp_dir(tag: &str) -> std::path::PathBuf {
            std::env::temp_dir().join(format!(
                "eg303-iso-{}-{}-{}",
                tag,
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_nanos())
                    .unwrap_or(0)
            ))
        }

        #[test]
        fn eg303_roles_grants_and_identities_round_trip_through_redb_reopen() {
            let dir = tmp_dir("round-trip");
            {
                let mut layer = IsolationLayer::with_persist_dir(&dir).unwrap();
                layer.add_role(Role::new("reader"));
                layer.add_role(Role::with_parents("editor", vec!["reader".into()]));
                layer.add_grant(Grant {
                    role: "editor".into(),
                    resource: ResourceSelector::Label("Doc".into()),
                    action: RbacAction::Write,
                    effect: GrantEffect::Allow,
                });
                layer.register_agent(AgentIdentity {
                    agent_id: "sam".into(),
                    role: AgentRole::Agent,
                    teams: vec!["alpha".into()],
                    roles: vec!["editor".into()],
                });
            }
            // Reopen the SAME dir — a fresh layer restores policy + identities from redb.
            let layer = IsolationLayer::with_persist_dir(&dir).unwrap();
            // Identity survived (has_rules + accessible graphs reflect the registration).
            assert!(layer.has_rules());
            assert!(layer.accessible_graphs("sam").contains("agent:sam"));
            assert!(layer.accessible_graphs("sam").contains("team:alpha"));
            // Roles/grants survived — the inherited grant still evaluates.
            assert_eq!(layer.rbac().grants().len(), 1);
            assert!(layer.rbac().is_allowed(
                &["editor"],
                &ResourceContext {
                    graph: "g".into(),
                    label: Some("Doc".into())
                },
                RbacAction::Write
            ));
            let _ = std::fs::remove_dir_all(&dir);
        }

        #[test]
        fn eg303_mutation_write_through_visible_on_reopen() {
            let dir = tmp_dir("write-through");
            // 1) Add a grant, then reopen: the grant is present.
            {
                let mut layer = IsolationLayer::with_persist_dir(&dir).unwrap();
                layer.add_grant(Grant {
                    role: "r".into(),
                    resource: ResourceSelector::All,
                    action: RbacAction::Read,
                    effect: GrantEffect::Allow,
                });
            }
            {
                let layer = IsolationLayer::with_persist_dir(&dir).unwrap();
                assert_eq!(layer.rbac().grants().len(), 1);
            }
            // 2) Remove that grant + register/unregister an identity, then reopen: the
            //    removals are durable too (write-through fires on every mutation).
            {
                let mut layer = IsolationLayer::with_persist_dir(&dir).unwrap();
                assert!(layer.remove_grant(&Grant {
                    role: "r".into(),
                    resource: ResourceSelector::All,
                    action: RbacAction::Read,
                    effect: GrantEffect::Allow,
                }));
                layer.register_agent(AgentIdentity {
                    agent_id: "tmp".into(),
                    role: AgentRole::Agent,
                    teams: vec![],
                    roles: vec![],
                });
                layer.unregister_agent("tmp");
            }
            let layer = IsolationLayer::with_persist_dir(&dir).unwrap();
            assert_eq!(layer.rbac().grants().len(), 0);
            assert!(!layer.has_rules());
            assert!(!layer.identity_bootstrap_pending());
            let _ = std::fs::remove_dir_all(&dir);
        }

        #[test]
        fn exact_system_bootstrap_is_atomic_and_one_time() {
            let mut layer = IsolationLayer::new();
            assert!(layer.identity_bootstrap_pending());
            layer
                .try_bootstrap_system_identity(AgentIdentity {
                    agent_id: "root".into(),
                    role: AgentRole::System,
                    teams: vec![],
                    roles: vec![],
                })
                .unwrap();
            assert!(!layer.identity_bootstrap_pending());
            assert!(layer
                .try_bootstrap_system_identity(AgentIdentity {
                    agent_id: "second".into(),
                    role: AgentRole::System,
                    teams: vec![],
                    roles: vec![],
                })
                .is_err());
        }

        #[test]
        fn eg303_embedded_layer_uses_atomic_memory_policy_store() {
            let mut layer = IsolationLayer::new();
            assert!(layer.persist.is_some());
            layer.add_grant(Grant {
                role: "r".into(),
                resource: ResourceSelector::All,
                action: RbacAction::Read,
                effect: GrantEffect::Allow,
            });
            layer.register_agent(AgentIdentity {
                agent_id: "x".into(),
                role: AgentRole::Agent,
                teams: vec![],
                roles: vec!["r".into()],
            });
            assert!(layer.persist.is_some());
            assert_eq!(layer.rbac().grants().len(), 1);
            assert!(layer.has_rules());
        }

        #[test]
        fn durable_policy_failure_rolls_back_identity_and_rbac_mutations() {
            let mut layer = IsolationLayer::new();
            layer.persist = Some(std::sync::Arc::new(FailingPolicyStore));

            assert!(layer
                .try_register_agent(AgentIdentity {
                    agent_id: "not-committed".into(),
                    role: AgentRole::Agent,
                    teams: vec![],
                    roles: vec![],
                })
                .is_err());
            assert!(!layer.has_rules());

            assert!(layer
                .try_add_grant(Grant {
                    role: "r".into(),
                    resource: ResourceSelector::All,
                    action: RbacAction::Admin,
                    effect: GrantEffect::Allow,
                })
                .is_err());
            assert!(layer.rbac().grants().is_empty());
        }
    }
}
