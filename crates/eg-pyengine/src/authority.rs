//! Per-agent Row-Level Security, threaded in-process (`EG-PYENGINE-PLAN.md`
//! §4.3; design doc `docs/architecture/unified-inprocess-engine.md` §7).
//!
//! ## The DAG constraint (read before touching this file)
//!
//! `src/server/access.rs`'s `GraphReadAuthority::filter_view`
//! (`access.rs:269`) is **not reachable from this crate**:
//! `src/server/access.rs` lives in the FACADE (`src/`), which sits ABOVE
//! `eg-core` in the workspace DAG (`eg-types -> eg-ann -> eg-core ->
//! eg-compute -> epistemic-graph`), and this crate cannot depend on the
//! facade — the facade optionally depends on US (feature `pyo3-engine`), so
//! the reverse edge would cycle.
//!
//! Reading `GraphReadAuthority::filter_view`'s own body
//! (`access.rs:269-274`) shows it does nothing beyond
//! `self.isolation.filter_view(&self.actor, view)`, where `self.isolation` is
//! an `Arc<eg_core::isolation::IsolationLayer>` — i.e. the REAL filtering
//! decision is already an `eg-core`-level primitive
//! (`crates/eg-core/src/isolation.rs:1117`,
//! `IsolationLayer::filter_view`, itself built on `can_see_node` at
//! `isolation.rs:1074` and `can_see_row` at `isolation.rs:1014`), and
//! `GraphReadAuthority` is a thin facade-layer wrapper adding
//! `CarrierAuthority`/`VerifiedRequestContext` — wire-verification concerns
//! that do not apply in-process (design doc §7: identity is *asserted*, not
//! cryptographically proven, when there is no network boundary to forge or
//! replay across). So `EmbeddedAuthority` below calls `IsolationLayer`
//! directly, at the eg-core layer, rather than reimplementing or routing
//! through the facade.
//!
//! ## Why a point check, not `filter_view`, for Wave 0's own methods
//!
//! `IsolationLayer::filter_view`/`can_see_node` operate on a whole
//! `eg_core::graph::GraphView` snapshot — the right shape for a
//! multi-row read (the future `graph_ops`/`query` lanes). The prototype's
//! existing methods this Wave migrates (`get_node_properties`/`has_node`) are
//! single-row point lookups that never materialize a view, exactly the
//! precedent `isolation.rs:1074`'s own doc comment describes for
//! `HasNode`/`HasNodesBatch`: *"a projection is `O(V log V + E log E + V*d)`
//! and a point lookup used to pay all of it"*. `can_see_row`
//! (`isolation.rs:1014`) is the shared, lowest-level decision function BOTH
//! `filter_view` and the point-lookup path call — `filter_view` is
//! `can_see_node` per node, and `can_see_node` is `can_see_row` given that
//! node's decoded `RowVisibility`. `can_see_properties` below calls that same
//! `can_see_row` directly, so it is the identical decision `filter_view`
//! would reach for the same row — not a parallel, drifting reimplementation.
//! A future view-consuming lane (`graph_ops`, `query`, ...) should call
//! `IsolationLayer::filter_view`/`can_see_node` directly against its own
//! `GraphView` (reachable via `authority.isolation`, `pub(crate)` below) — one
//! extra wrapper here would only be indirection, per the simplicity directive
//! (`EG-PYENGINE-PLAN.md` §12.1: *"do not design a policy engine"*).

use std::sync::Arc;

use eg_core::isolation::IsolationLayer;

/// The one RLS contract every domain module threads through: an asserted
/// caller identity plus the SAME `IsolationLayer` the wire dispatch consults.
/// `agent_id: None` is the trusted-caller default (today's
/// prototype/`EmbeddedEngine` behavior — no filtering at all), matching the
/// design doc's stated in-process threat model (§7).
///
/// Cheap to clone (`Arc`-backed `isolation`, a cloned `String` for
/// `agent_id`/`_tenant`) — every domain accessor
/// (`PyEngine::graph_ops()`/`.finance()`/...) clones one of these per call,
/// never a deep copy of registry or policy state.
#[derive(Clone)]
pub(crate) struct EmbeddedAuthority {
    isolation: Arc<IsolationLayer>,
    agent_id: Option<String>,
    /// Accepted and stored at construction time (design doc §7: tenant is a
    /// deployment-identity assertion, bound once, not re-verified per call) —
    /// not yet consulted by anything in Wave 0. An explicit, documented seam
    /// for whichever lane first needs tenant-scoped behavior, not a silent
    /// no-op: the field exists and is threaded through `Engine(tenant=...)`
    /// so no later lane has to change this struct's shape to add it.
    _tenant: Option<String>,
}

impl EmbeddedAuthority {
    /// Construct the trusted-caller-default authority (`agent_id: None`) with
    /// no RBAC roles/grants provisioned — matches `IsolationLayer::new()`'s
    /// own empty, default-deny-once-active posture.
    ///
    /// Called from `lib.rs`'s `mod py` (feature `python`) and from this
    /// module's own `#[cfg(feature = "security")]` tests. A LIB-target build
    /// with `security` on but `python`/`test` off (e.g. plain `cargo check
    /// --features security`) has no caller at all, same "genuinely unused in
    /// this particular build, real contract for the builds that turn a
    /// feature on" situation as `isolation()`/`agent_id()` below — plain
    /// `#[allow(dead_code)]` rather than a `cfg_attr` cross-product of
    /// `python`/`security`/`test` (that combination is exactly the kind of
    /// unnecessary complexity the simplicity directive warns against,
    /// EG-PYENGINE-PLAN.md §12.1), not a signal the method is unwanted.
    #[allow(dead_code)]
    pub(crate) fn new(agent_id: Option<String>, tenant: Option<String>) -> Self {
        EmbeddedAuthority {
            isolation: Arc::new(IsolationLayer::new()),
            agent_id,
            _tenant: tenant,
        }
    }

    /// The `IsolationLayer` this authority carries, for a future view-based
    /// lane to call `filter_view`/`can_see_node` against its own
    /// `GraphView` (see module doc). `pub(crate)` — reachable from every
    /// sibling domain module, not exported past this crate.
    ///
    /// Genuinely unused within Wave 0 itself (no domain method here builds a
    /// `GraphView` yet) — `#[allow(dead_code)]` documents that honestly
    /// rather than deleting a contract every Wave-1 view-consuming lane
    /// (`graph_ops`, `query`, ...) needs on day one, or inventing a call site
    /// here just to appease the lint.
    #[allow(dead_code)]
    pub(crate) fn isolation(&self) -> &Arc<IsolationLayer> {
        &self.isolation
    }

    /// The asserted caller identity, or `None` for the trusted-caller
    /// default. Threaded to `IsolationLayer` calls exactly as the wire
    /// dispatch's already-verified `agent_id` is today — just sourced from
    /// "the caller told us" instead of "the caller cryptographically proved
    /// it" (design doc §7). Same "genuinely unused in Wave 0, real contract
    /// for Wave 1" note as `isolation()` above.
    #[allow(dead_code)]
    pub(crate) fn agent_id(&self) -> Option<&str> {
        self.agent_id.as_deref()
    }

    /// Per-agent Row-Level Security for ONE node's raw property blob
    /// (`None` when the node has no property blob at all, or does not exist)
    /// — the point-lookup equivalent of `IsolationLayer::filter_view`, see
    /// module doc for why this crate uses the point form here.
    ///
    /// `agent_id_override`, when `Some`, takes precedence over the identity
    /// this authority was CONSTRUCTED with (design doc §7: "a fixed identity
    /// ... or per-call as an optional override parameter") — added in
    /// response to a real gap the Wave-0 Python lane's differential harness
    /// surfaced: with construction-time-only identity, two principals
    /// sharing one embedded engine (`EG-PYENGINE-PLAN.md` §3's correctness
    /// bar, point 2 — RLS "must be tested with two distinct principals
    /// against the SAME embedded engine instance") is structurally
    /// impossible without either this override or durable cross-instance
    /// storage (see `lib.rs`'s `persist_dir` handling and this Wave's report
    /// for why the latter isn't available yet). `None` falls back to the
    /// construction-time identity — the common single-tenant case (§7) is
    /// unaffected by this parameter existing.
    ///
    /// `true` when the resolved identity (override, else construction-time)
    /// is unset (trusted-caller default: unchanged, unfiltered behavior) or
    /// when the resolved `IsolationLayer::can_see_row` decision allows it.
    ///
    /// Called from `lib.rs`'s `mod py` (feature `python`) and from this
    /// module's own tests (`cfg(test)`, this same `security` branch) — a
    /// LIB-target `--features security` build with `python` off has no
    /// caller, same "plain `#[allow(dead_code)]`, not a cfg cross-product"
    /// reasoning as `new` above.
    #[cfg(feature = "security")]
    #[allow(dead_code)]
    pub(crate) fn can_see_properties(
        &self,
        agent_id_override: Option<&str>,
        blob: Option<&[u8]>,
    ) -> bool {
        let Some(agent_id) = agent_id_override.or(self.agent_id.as_deref()) else {
            return true;
        };
        let vis = match blob {
            Some(bytes) => eg_core::isolation::row_visibility(bytes),
            // `RowVisibility::default_public()` (`isolation.rs:386`) is the
            // exact value used here, but it is `pub(crate)` to eg-core (not
            // reachable from this crate, same DAG constraint as the module
            // doc above) — every FIELD of `RowVisibility` is `pub`, so this
            // replicates its documented field values (`owner: None, public:
            // true, tagged: false, ...`) rather than its (unreachable) logic,
            // matching the SAME "no property blob at all" input
            // `can_see_node` feeds `can_see_row` with — despite the name,
            // `can_see_row` still DENIES this once an identity is asserted
            // (`owner: None` + `tagged: false` falls through to `vis.tagged
            // && vis.public` = `false`, per default-deny); only the
            // trusted-caller default above (no asserted identity at all)
            // bypasses `can_see_row` entirely. See
            // `no_property_blob_at_all_is_denied_once_an_identity_is_asserted`
            // below for the verified behavior.
            None => eg_core::isolation::RowVisibility {
                owner: None,
                public: true,
                grants: Vec::new(),
                tagged: false,
                schema: false,
            },
        };
        self.isolation.can_see_row(agent_id, &vis)
    }

    /// Without the `security` feature there is no RBAC/RLS evaluator compiled
    /// in at all (`eg-core`'s own `#[cfg(feature = "security")]` gate on
    /// `can_see_row`/`RowVisibility`) — matches `GraphReadAuthority`'s own
    /// `#[cfg(not(feature = "security"))]` branch (`access.rs`): RLS is
    /// unavailable, not silently "on but doing nothing different." Called
    /// from `lib.rs`'s `mod py` (feature `python`) — with `python` also off
    /// there is no caller at all in this build. Same `agent_id_override`
    /// parameter as the `security` branch above, for a stable call site
    /// regardless of which branch compiles in.
    #[cfg(not(feature = "security"))]
    #[allow(dead_code)]
    pub(crate) fn can_see_properties(
        &self,
        _agent_id_override: Option<&str>,
        _blob: Option<&[u8]>,
    ) -> bool {
        true
    }
}

#[cfg(all(test, feature = "security"))]
mod tests {
    use super::*;

    #[test]
    fn trusted_caller_default_sees_everything() {
        let authority = EmbeddedAuthority::new(None, None);
        assert!(authority.can_see_properties(None, None));
        assert!(authority.can_see_properties(None, Some(b"\x80"))); // empty msgpack map
    }

    #[test]
    fn unowned_untagged_row_is_denied_once_an_identity_is_asserted() {
        // Default-deny (isolation.rs:1014's documented posture): an agent_id
        // IS asserted, but the row carries no RLS metadata at all (an empty
        // msgpack map decodes as `tagged: false`) — denied, not "public by
        // absence."
        let authority = EmbeddedAuthority::new(Some("agent-a".to_string()), None);
        assert!(!authority.can_see_properties(None, Some(b"\x80")));
    }

    #[test]
    fn no_property_blob_at_all_is_denied_once_an_identity_is_asserted() {
        // The `None` branch (no blob at all, e.g. a node that was created but
        // never had properties set) constructs the SAME field values
        // `RowVisibility::default_public()` uses (`owner: None, public: true,
        // tagged: false, ...`) — but despite the name, `can_see_row`
        // (`isolation.rs:1014`) denies this once an identity is asserted:
        // `owner: None` falls through to `vis.tagged && vis.public` =
        // `false && true` = `false`. "default_public" describes the FIELD
        // VALUES this constructs (as if publicly tagged), not the ultimate
        // `can_see_row` verdict — default-deny still denies an untagged row
        // (`isolation.rs`'s own module doc: "an unowned row that is
        // undecodable or declares no RLS metadata is denied"). Only the
        // TRUSTED-CALLER default (`agent_id` entirely unset, see
        // `trusted_caller_default_sees_everything` above) bypasses this
        // check — that path returns `true` before `can_see_row` is ever
        // consulted, unaffected by this test's outcome.
        let authority = EmbeddedAuthority::new(Some("agent-a".to_string()), None);
        assert!(!authority.can_see_properties(None, None));
    }

    #[test]
    fn per_call_override_lets_two_principals_share_one_authority() {
        // The gap the Wave-0 Python lane's differential harness surfaced
        // (`tests/parity/test_parity_graph_ops.py::
        // test_get_node_properties_rls_isolation`): ONE `EmbeddedAuthority`
        // (hence one `Engine`/one `SharedRegistry`) needs to answer
        // differently for two DISTINCT principals without constructing two
        // separate instances. Own an owner-tagged, private row; the owner
        // (matching construction-time identity, no override) sees it; an
        // unregistered `other` (per-call override, NOT the construction-time
        // identity) does not — proving the override, not just the
        // construction-time path, reaches `can_see_row`.
        let owner_id = "agent-owner";
        let authority = EmbeddedAuthority::new(Some(owner_id.to_string()), None);
        let mut props = std::collections::BTreeMap::new();
        props.insert(
            eg_core::isolation::RLS_OWNER_KEY,
            serde_json::Value::String(owner_id.to_string()),
        );
        props.insert(
            eg_core::isolation::RLS_VISIBILITY_KEY,
            serde_json::Value::String("private".to_string()),
        );
        let blob = rmp_serde::to_vec_named(&props).unwrap();

        // No override: falls back to the construction-time owner identity.
        assert!(authority.can_see_properties(None, Some(&blob)));
        // Override to an unregistered, non-owning principal: denied, on the
        // SAME `EmbeddedAuthority`/blob — the two-principal case a single
        // construction-time identity cannot represent.
        assert!(!authority.can_see_properties(Some("agent-other"), Some(&blob)));
        // Override BACK to the owner: allowed again, proving this is a true
        // per-call decision, not a one-way downgrade.
        assert!(authority.can_see_properties(Some(owner_id), Some(&blob)));
    }
}
