// CONCEPT:EG-KG.query.wire-protocol — Epistemic Graph Service Wire Protocol
//
// Length-prefixed MessagePack framing for UDS/TCP communication between
// the Python client and the Tokio service layer. Every request
// is authenticated via HMAC-SHA256.

use serde::{Deserialize, Serialize};

// Result-contract wire bodies retain their historical protocol paths.
pub use crate::result_contract::security::LedgerReadResult;
#[cfg(feature = "security")]
pub use crate::result_contract::security::{
    AuditReport, MerkleInclusionReport, MerkleProofStep, MerkleSide,
};

/// Deserialize an explicitly present nullable field.
///
/// Serde otherwise treats a missing `Option<T>` exactly like an explicit null,
/// which would silently admit an older wire shape after a current-only cutover.
fn deserialize_required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

/// Default page size for the bounded ResourceStats surface.
#[cfg(feature = "cost")]
fn default_resource_stats_limit() -> usize {
    128
}

/// `skip_serializing_if` predicate for a `bool` field whose default is
/// `false` (serde requires `fn(&T) -> bool`, so `std::ops::Not::not`'s
/// by-value signature doesn't fit). Omitting a still-`false` field from the
/// server's own re-serialization (`Method::canonical_body_bytes`) matches a
/// client that never sent the key at all -- see `MineAssociate::as_claim`'s
/// use for why this matters to the `eg2.` MAC, not just wire compactness.
#[cfg(all(feature = "mining", feature = "epistemic"))]
fn is_false(value: &bool) -> bool {
    !*value
}

/// `skip_serializing_if` predicate for a `usize` field whose default is `0`
/// (`TransactionSource`/`SequenceSource`/`VectorSource`/`TextSource::limit`,
/// `GraphSource::limit`). A graph-derived source dict built by hand (the
/// common case -- callers pass e.g. `{"node_label": "Paper", "direction":
/// "out"}` with no `limit` key at all) never hashes an unset `limit` into the
/// `eg2.` MAC's canonical body; without this, the server's own
/// re-serialization used to recompute that MAC would emit an explicit
/// `limit: 0` the client never sent, failing every such call with
/// "Authentication failed" -- the same class of bug `is_false` above fixes
/// for `MineAssociate::as_claim`.
#[cfg(any(feature = "mining", feature = "graphlearn"))]
fn is_zero_usize(value: &usize) -> bool {
    *value == 0
}

/// serde default for association-rule `min_support` (CONCEPT:EG-KG.mining.frequent-itemset-mining):
/// keep an itemset supported by ≥10% of transactions.
#[cfg(feature = "mining")]
fn default_min_support() -> f64 {
    0.1
}

/// serde default for association-rule `min_confidence` (CONCEPT:EG-KG.mining.frequent-itemset-mining):
/// keep a rule with ≥50% conditional probability.
#[cfg(feature = "mining")]
fn default_min_confidence() -> f64 {
    0.5
}

/// serde default for [`Method::ClusterHierarchyRefresh::resolution`] (VIZ-1) —
/// GDS-parity default, matching Leiden/Louvain's own default resolution (γ=1.0)
/// everywhere else in this file.
fn default_cluster_hierarchy_resolution() -> f64 {
    1.0
}

/// serde default for `Method::ResolveConflict::semantics` (EPI-P3-7) — the unique,
/// always-defined skeptical (grounded) Dung extension, matching every other
/// epistemic op's "narrowest, always-answerable default" convention (mirrors
/// `is_skeptically_accepted`'s own choice of grounded over preferred/stable).
#[cfg(feature = "epistemic")]
fn default_argumentation_semantics() -> String {
    "grounded".to_string()
}

/// Required execution authority for a Cypher statement.
///
/// The caller must declare whether a statement is a read or mutation. The
/// server parses the statement and rejects a mismatch, so a mutation can never
/// obtain read authorization by hiding a keyword in comments, literals, or an
/// unsupported clause.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum CypherMode {
    Read,
    Write,
}

// ── Request ─────────────────────────────────────────────────────────────

/// Top-level request envelope sent by the Python client.
///
/// `auth_token` carries the current `eg2.` verified request context
/// (CONCEPT:EG-KG.security.signed-request-envelope, EG-P0-5). It binds the
/// request id, graph, method, body hash, effective ACL agent, roles, scopes,
/// active policy version, delegation chain, timestamp, nonce, and idempotency
/// key. The server validates audience, tenant, policy version, and durable
/// replay state before dispatch.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    /// Monotonically increasing request ID for correlation.
    pub id: u64,
    /// Target graph name (e.g., "agent:planner", "__commons__", "channel:p2p:a:b").
    pub graph: String,
    /// Current `eg2.` verified request-context envelope.
    pub auth_token: String,
    /// Optional caller assertion. The server rejects a mismatch and replaces
    /// this value with the signed effective agent before authorization.
    #[serde(deserialize_with = "deserialize_required_option")]
    pub agent_id: Option<String>,
    /// The operation to perform.
    #[serde(flatten)]
    pub method: Method,
}

/// Canonical byte encoding for a verified request-context envelope (v2).
///
/// The current envelope signs every request/replay binding plus the effective
/// agent, roles, scopes, policy version, and ordered delegation
/// chain.  Every scalar and list item is length-prefixed, and list lengths are
/// explicit, so distinct logical claim sets cannot share an encoding.
#[allow(clippy::too_many_arguments)]
pub fn build_envelope_v2_bytes(
    request_id: u64,
    graph: &str,
    method_name: &str,
    body_hash: &str,
    claims: &crate::acl::RequestContextClaims,
    timestamp: u64,
    nonce: &str,
    idempotency_key: &str,
) -> Vec<u8> {
    fn put(buf: &mut Vec<u8>, value: &str) {
        buf.extend_from_slice(&(value.len() as u32).to_be_bytes());
        buf.extend_from_slice(value.as_bytes());
    }
    fn put_list(buf: &mut Vec<u8>, values: &[String]) {
        buf.extend_from_slice(&(values.len() as u32).to_be_bytes());
        for value in values {
            put(buf, value);
        }
    }

    let mut buf = Vec::new();
    put(&mut buf, "eg-envelope-v2");
    buf.extend_from_slice(&request_id.to_be_bytes());
    put(&mut buf, graph);
    put(&mut buf, method_name);
    put(&mut buf, body_hash);
    put(&mut buf, &claims.principal);
    put(&mut buf, &claims.tenant);
    put(&mut buf, &claims.audience);
    put(&mut buf, &claims.agent_id);
    put_list(&mut buf, &claims.roles);
    put_list(&mut buf, &claims.scopes);
    put(&mut buf, &claims.policy_version);
    put_list(&mut buf, &claims.delegation);
    buf.extend_from_slice(&timestamp.to_be_bytes());
    put(&mut buf, nonce);
    put(&mut buf, idempotency_key);
    // ADR-3 / W1.9 node-bound envelopes (`reports/wave1/ADR-scale-trio.md`):
    // appended ONLY when the minting client set a target-node claim, so an
    // envelope from a client that predates node binding — an un-upgraded
    // Python client, or one of the non-Python `clients/{js,go}` bindings,
    // neither of which this change touches — encodes BYTE-FOR-BYTE
    // IDENTICALLY to before. This is what makes the wire change genuinely
    // additive rather than a breaking MAC-format bump: those clients keep
    // verifying with zero changes. Presence itself is MAC-covered (not just
    // the value), so a captured envelope's node claim can never be silently
    // stripped or retargeted to a different node without invalidating the MAC.
    if let Some(node) = claims.node.as_deref() {
        buf.push(1);
        put(&mut buf, node);
    }
    // W2.4 engine-native QoS lanes: the advisory admission-priority claim, MAC-
    // covered so it cannot be forged to jump the admission ordering. Appended as
    // a SECOND optional trailer with a DISTINCT tag byte (`2`, vs the node
    // trailer's `1`) so the two trailers stay mutually unambiguous — a
    // node-only envelope and a priority-only envelope can never collide onto the
    // same MAC input (a bare shared marker would let `node="x"` and
    // `priority="x"` sign identically). Presence-gated exactly like `node`, so an
    // envelope without a priority claim encodes byte-for-byte as before.
    if let Some(priority) = claims.priority.as_deref() {
        buf.push(2);
        put(&mut buf, priority);
    }
    buf
}

/// Canonical bytes for a detached administrative-operation signature scoped to
/// a verified context. `body` is the canonical `Method` encoding with its
/// signature field/list cleared, so the signature binds every operation
/// parameter without recursively signing itself.
pub fn build_context_operation_signature_bytes(
    domain: &str,
    claims: &crate::acl::RequestContextClaims,
    idempotency_key: &str,
    graph: &str,
    body: &[u8],
) -> Vec<u8> {
    fn put(buf: &mut Vec<u8>, bytes: &[u8]) {
        buf.extend_from_slice(&(bytes.len() as u64).to_be_bytes());
        buf.extend_from_slice(bytes);
    }
    fn put_list(buf: &mut Vec<u8>, values: &[String]) {
        put(buf, &(values.len() as u64).to_be_bytes());
        for value in values {
            put(buf, value.as_bytes());
        }
    }

    let mut buf = Vec::new();
    put(&mut buf, domain.as_bytes());
    put(&mut buf, claims.principal.as_bytes());
    put(&mut buf, claims.tenant.as_bytes());
    put(&mut buf, claims.audience.as_bytes());
    put(&mut buf, claims.agent_id.as_bytes());
    put_list(&mut buf, &claims.roles);
    put_list(&mut buf, &claims.scopes);
    put(&mut buf, claims.policy_version.as_bytes());
    put_list(&mut buf, &claims.delegation);
    put(&mut buf, idempotency_key.as_bytes());
    put(&mut buf, graph.as_bytes());
    put(&mut buf, body);
    buf
}

/// One operation on the placement-catalog admin surface (CONCEPT:EG-KG.sharding.placement-catalog-admin-rpc,
/// DIST-P2-5). Nested under `Method::PlacementAdmin { op }` — mirrors
/// [`crate::modality::ServedModalityOp`]'s "one Method variant, many operations" shape.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum PlacementAdminOp {
    /// Assign the WHOLE keyspace of `tenant` to `group` (the placement DECISION leg).
    /// Collapses any prior split. Returns `{"epoch": u64}` — the new routing epoch
    /// every subsequent `PlacementRoute`/read observes immediately.
    Assign { tenant: String, group: u64 },
    /// Online-move `tenant`'s partition `[range_start, range_end]` to `target`.
    /// Returns a `PlacementMoveReport` JSON: `{tenant, range, target, epoch,
    /// graphs: [{graph, from_group, to_group, nodes_transferred}]}`.
    Move {
        tenant: String,
        range_start: u64,
        range_end: u64,
        target: u64,
    },
    /// Abort an in-flight online move identified by `move_id` before its cutover
    /// fence. A move already past its epoch fence is rejected (roll-forward only,
    /// matching `TenantManager::abort_move`'s in-process contract). Returns `Bool`.
    AbortMove { move_id: String },
}

/// CONCEPT:EG-KG.query.obda-predicate-pushdown — a LIVE EXTERNAL relational source registered
/// for an OBDA virtual graph (W4.11): a `TriplesMap::logical_source` NAME bound to a `table`
/// in an external Postgres/MySQL database reachable at `dsn`. On a [`Method::SparqlVirtual`]
/// query the engine exposes it as a virtual RDF graph and pushes BOTH the query's column
/// projection AND its row-level `FILTER`s down into a real `SELECT … WHERE …` against the
/// database — the whole table is never scanned. The live SQL path needs a server built with
/// `federation-sql` (reusing its SSRF-validated, read-only, timeout+row-bounded connector); a
/// build without it returns a clean "rebuild with federation-sql" error. Distinct from
/// `tables` (the engine's OWN user tables).
#[cfg(feature = "obda")]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ObdaExternalSource {
    /// The foreign-source NAME the mapping's `TriplesMap`(s) reference as `logical_source`.
    pub name: String,
    /// The external database DSN (`postgres://…` / `mysql://…`), SSRF-validated server-side.
    pub dsn: String,
    /// The table (or view) name in the external database to expose as the virtual source.
    pub table: String,
}

// ── Method ──────────────────────────────────────────────────────────────

/// The only WorkItem kinds admitted by the development-lane authority.
///
/// The lifecycle and cleanup effects deliberately use different WorkItems and
/// therefore different fences.  A cleanup completion can never be mistaken for
/// a lifecycle attempt, even when both refer to the same immutable lane id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DevelopmentLaneWorkItemKind {
    #[serde(rename = "lane.lifecycle")]
    Lifecycle,
    #[serde(rename = "lane.cleanup")]
    Cleanup,
}

pub const DEVELOPMENT_LANE_LIFECYCLE_KIND: &str = "lane.lifecycle";
pub const DEVELOPMENT_LANE_CLEANUP_KIND: &str = "lane.cleanup";

// Lane request DTOs carry `now_ms` for deterministic replay and test vectors.
// Dispatch must normalize/overwrite that field from the authoritative engine
// clock before authorization or persistence; a client-supplied timestamp is
// never trusted as freshness, expiry, or lease authority (RMDD-27 convention).
// Supporting wire types are split into focused child modules; re-exports preserve
// every historical `eg_types::protocol::*` path used by clients and handlers.
mod support_00;
pub use support_00::*;
mod support_01;
pub use support_01::*;
mod support_02;
pub use support_02::*;
mod support_03;
pub use support_03::*;
mod support_04;
pub use support_04::*;

mod method;
pub use method::Method;

#[cfg(test)]
mod tests;
