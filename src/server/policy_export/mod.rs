//! CA-16 — row-level-security marking predicate export (feature `policy_export`,
//! `DEC-CA-04`, off by default).
//!
//! # What this exports, and what it deliberately does not (DEC-CA-04 A1)
//!
//! `agent-utilities`' live authorization inventory (CA-33) measured **nine**
//! independent mechanisms deciding what a caller may see or do against this
//! engine and its neighbors. This module renders exactly **one** — **M1**,
//! [`crate::isolation::IsolationLayer::filter_view`]/`can_see_row`'s live
//! `_owner`/`_visibility`/`_grants` (or au's `_owner_id`/`_shared_scope`)
//! row-visibility predicate, the same decision every native SQL/Cypher/RDF read
//! already applies (`server::access::GraphReadAuthority::filter_view`). It is
//! **never a second definition** — this module reads [`crate::isolation`]'s
//! public API only and writes nothing back to it. M2 (owner-scoped SQL catalog
//! files), M3 (`sql_catalog_acl`), M4 (au's tenant-predicate pushdown/post-
//! filter/allowlist triad), M6 (au's `permissioning.enforce`, where Markings
//! are ACTUALLY evaluated), M7 (graph-admission RBAC), M8, and M9 are all
//! **out of scope** and this bundle's `governs` field is always exactly
//! `["M1"]` so no downstream reader can mistake it for the whole story.
//!
//! # The Marking → RowVisibility bridge (DEC-CA-04 A4)
//!
//! **eg has no native Marking concept** (`grep -rniE "\bmarking"
//! --include=*.rs` returns only English prose). Au's `Marking` is a mandatory,
//! role-gated control — `ontology/permissioning.py`'s `Marking.role_token`
//! requires an actor to hold role `marking:<name>` in `ActorContext.roles`,
//! checked per `(tenant, node_id)` against `MARKING_REGISTRY`, entirely
//! independent of eg's owner/visibility/grants shape. This module therefore
//! does not (and structurally cannot) render au's live marking ASSIGNMENTS —
//! that data lives only in au's durable `mandatory_marking` graph nodes, is
//! keyed `(tenant, node_id)`, and does not fit this bundle's per-tenant,
//! bounded-size shape (see "Algorithmic and resource budget" in
//! `plans/company-architecture/lanes/CA-16-*.md`: "no per-row export... the
//! bundle is a policy description"). What IS deterministic and bundle-shaped
//! is the REQUIREMENT a marking imposes: `Marking.role_token` is always
//! `marking:<name>`, mechanically, for every marking that exists. This module
//! defines [`MarkingPredicate::RequiresRole`] as the bridge: a marking's
//! `predicate` says a row is visible only to a principal whose `principals[…]`
//! role set contains `marking:<name>` — the same fungible role/scope check
//! `server::auth::bind_verified_identity` already applies at the primary
//! protocol boundary — AND (new, proposed, not yet a live convention) that the
//! row itself carries [`RESERVED_MARKING_COLUMN`] naming which markings apply
//! to it. **This second half is a genuinely new convention this lane
//! proposes, not a pre-existing mechanism** — CA-26's renderers (Trino row
//! filters, OpenSearch DLS queries, Lakekeeper OpenFGA tuples) all need a
//! PER-ROW signal of which markings a row carries to make this predicate
//! pushdown-renderable (CA-63); today only au's `mandatory_marking` graph
//! nodes carry that fact, keyed by node id, not by a column any of the three
//! rendering targets already project. **Finding for CA-26/CA-63:** until a
//! rendering target's own schema carries a `_markings`-shaped column (synced
//! from au's `mandatory_marking` store — a CA-26/CA-34 concern, not this
//! lane's), this predicate is well-defined and mechanically derivable but NOT
//! yet renderable end-to-end. P8 (eg leg) must not claim this bridge closes
//! that gap by itself; it only defines the target shape.
//!
//! # `principals` (DEC-CA-04 A2)
//!
//! `principals` is the **effective request-time role set**, never a read of
//! `IsolationLayer`'s `agents: HashMap<String, AgentIdentity>` (`rbac.redb`,
//! M7's own store). Every entry populated by this crate is the UNION of a
//! verified token's `roles` (realm + every `resource_access.*` client role) and
//! `scopes`/`scp` claims — see [`crate::server::oidc::JwtValidator::validate_claims`]
//! and this module's `tests::live_token_proves_principals_are_claims_derived`
//! for the literal proof (a real RS256-signed, issuer/audience/expiry-verified
//! token, run through the SAME code the primary protocol boundary uses,
//! against an `IsolationLayer` that has never heard of the subject).
//! `server::dispatch`'s `Method::PolicyExport` arm additionally folds the
//! CALLING principal's own live claims into `principals` on every export call,
//! so the bundle is partly self-populating from real traffic rather than
//! requiring a separate Keycloak-enumeration job to exist first (that job
//! remains a named gap — see the module-level "Owed" note below).
//!
//! ## The `"*"` vs `kg:admin` asymmetry (DEC-CA-04, unresolved, picked here)
//!
//! eg treats a `"*"` scope as admin-equivalent everywhere it checks admin at
//! all: [`crate::server::access::CarrierAuthority::from_verified`] computes
//! `admin = scopes.contains("*") || scopes.contains("kg:admin")`, and
//! [`crate::server::auth::VerifiedRequestContext::allows_action`] checks
//! `scope_index.contains("*")` first. au's `permissioning.py` only ever
//! checks `_PRIVILEGED_ROLES = frozenset({"kg:admin"})` — `"*"` alone is NOT
//! privileged to au. **This module picks eg's own definition (`"*"` OR
//! `kg:admin`) for gating `/policy/export` and `Method::PolicyExport`**,
//! because that is the definition every OTHER admin-tier decision in this
//! crate already uses (self-consistency), and documents it here rather than
//! resolving the asymmetry (out of scope — DEC-CA-04 leaves it open,
//! "not measured against a live token"). A caller minting only `"*"` is
//! therefore admin to this bundle's exporter and to eg's own RLS decisions,
//! but would be refused by au's `permissioning.py` for the SAME action — a
//! real, live gap this lane surfaces rather than silently inherits.
//!
//! ## A second, previously-unrecorded admin-gate asymmetry found while wiring this
//!
//! `server::dispatch`'s generic pre-match gate does NOT uniformly use the
//! claims-based definition above. Any `authz_action` for which
//! `server::access::is_admin_authz_action` returns true (`"security:admin"` or
//! an `"admin:"`-prefixed action) additionally requires
//! `IsolationLayer::has_admin_capability` — a **pre-registered agent** in
//! `agents: HashMap` with an explicit RBAC `Admin` grant, i.e. `rbac.redb`
//! (M7), NOT the token's own claims. `Method::PolicyExport` is therefore
//! deliberately given the fresh, non-admin-tier `authz_action = "policy:export"`
//! (see `eg_capabilities::policy`'s entry) so it is gated ONLY through
//! `VerifiedRequestContext::allows_method`'s unconditional, claims-derived
//! `kg:admin` fallback — never through `rbac.redb`. Naming it `"security:admin"`
//! (the obvious first choice, matching `GetIdentity`/`RegisterIdentity`) would
//! have silently reproduced the exact mistake DEC-CA-04 A2 corrected.
//!
//! # Bundle scope: one bundle per tenant, `graphs: [...]` (DEC-CA-04 A3)
//!
//! `generate_bundle` takes a caller-supplied `tenant` + `graphs` list — it does
//! **not** call [`crate::isolation::IsolationLayer::accessible_graphs`], which
//! is DEAD CODE with a naming scheme (`agent:<id>`, `team:<t>`) matching no
//! live graph on this deployment (255 named graphs exist; that function's
//! output matches none of them). Callers MUST source `graphs` from au's
//! `knowledge_graph.core.tenant_sharing.accessible_graphs` instead (tenant
//! graph first, then registered ancestors nearest-first, `__commons__`
//! always last — GOC-61: union READ, never a merge, tenant stays the write
//! target). This module has no way to enforce that a caller did so; it only
//! validates the list it is given is non-empty and duplicate-free.
//!
//! # Epoch / versioning
//!
//! `generated_from` is a deterministic `sha256` content hash over
//! `(tenant, sorted graphs, sorted principals+roles, sorted marking names)` —
//! see [`compute_epoch`]. Identical inputs always yield the identical epoch;
//! adding, removing, or renaming a marking (or a principal's role set)
//! changes it. This satisfies the negative test this lane owns: a Marking
//! added to `marking_names` is invisible in a bundle generated from the OLD
//! input and visible (with a NEW `generated_from`) in one generated from the
//! new input — see `tests::marking_appears_only_in_the_next_epoch`.
//!
//! # Projection-safe enforcement (CA-63) — a reported finding, not solved here
//!
//! [`MarkingPredicate::RequiresRole`] never references row CONTENT at all — it
//! is a pure function of the caller's own role set — so, unlike a
//! `node_type`-style filter, it cannot fail closed on a projected column the
//! way `filter_commons_catalog`/BUG-PE-039 did. The unresolved half is
//! [`RESERVED_MARKING_COLUMN`] itself: THAT reference IS a row-content
//! reference (does this row carry marking `<name>`?), and whether a
//! projecting query still exposes it in all three CA-26 targets is exactly
//! the open finding recorded above — flagged for CA-26/CA-63, not asserted
//! safe here.
//!
//! # Owed
//!
//! This module has no live enumeration of Keycloak realm `homelab`'s subject
//! population (DEC-CA-04 A2 marks that source `INFERRED`) and adds no durable
//! `principals` store of its own (kept out of `DURABLE_STORES` deliberately —
//! see this crate's `access.rs`/`durable_stores.rs` gates) — `principals`
//! today only ever contains a caller-supplied seed plus whichever principals
//! have themselves called `/policy/export` or `Method::PolicyExport`. A full
//! population requires either a Keycloak admin-API sync job or a durable
//! observed-principal store; both are named, not built, here.
#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Bundle JSON schema version (distinct from `generated_from`'s epoch — this
/// changes only when the SHAPE of the bundle changes, not its content).
pub const POLICY_BUNDLE_FORMAT_VERSION: &str = "policy-bundle-v1";

/// The mechanism set this bundle renders. Always exactly `["M1"]` — see the
/// module doc's "What this exports" section. A future bundle that renders a
/// second mechanism must add a new field/variant, never silently widen this
/// array's meaning.
pub const GOVERNS: &[&str] = &["M1"];

/// Prefix matching au's `Marking.role_token` (`ontology/permissioning.py`) —
/// `f"marking:{name}"`. Kept as a named constant so the two repos' conventions
/// are visibly the SAME string, not independently re-typed.
pub const MARKING_ROLE_PREFIX: &str = "marking:";

/// Proposed (not yet adopted anywhere) per-row marking-name column convention —
/// see the module doc's "Marking → RowVisibility bridge" section for why this
/// is a finding for CA-26/CA-63, not a live mechanism.
pub const RESERVED_MARKING_COLUMN: &str = "_markings";

/// The exported policy bundle (`DEC-CA-04`'s frozen JSON contract, extended
/// per the W0 review appendix: `governs` + `tenant`/`graphs` — see A1/A3).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PolicyBundle {
    pub version: String,
    pub generated_from: String,
    pub governs: Vec<String>,
    /// One bundle per tenant (DEC-CA-04 A3) — never per graph.
    pub tenant: String,
    /// Every graph this tenant may read; caller-sourced from au's
    /// `tenant_sharing.accessible_graphs`, never this crate's dead
    /// `IsolationLayer::accessible_graphs` (see the module doc's trap note).
    pub graphs: Vec<String>,
    pub principals: BTreeMap<String, Vec<String>>,
    pub markings: BTreeMap<String, MarkingPolicyEntry>,
    pub renderings: Renderings,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MarkingPolicyEntry {
    /// Opaque, engine-neutral, mechanically-derivable predicate — a compact
    /// JSON-serialized [`MarkingPredicate`], never free text.
    pub predicate: String,
}

/// Left empty by this lane per `DEC-CA-04`'s generator/applier split — CA-26
/// populates these from the SAME `principals`/`markings` this bundle carries.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Renderings {
    #[serde(default)]
    pub trino: Vec<serde_json::Value>,
    #[serde(default)]
    pub opensearch: Vec<serde_json::Value>,
    #[serde(default)]
    pub lakekeeper: Vec<serde_json::Value>,
}

/// The small, serialized predicate AST `markings.<name>.predicate` encodes.
/// `RequiresRole` is the ONLY variant this lane defines — see the module doc's
/// "Marking → RowVisibility bridge" section for the full reasoning and its
/// open half (the row-side `column` reference is a proposed convention, not
/// yet adopted by any producer).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MarkingPredicate {
    RequiresRole {
        /// The role token a principal's `principals[subject]` entry must
        /// contain to see a row carrying this marking. Always
        /// `{MARKING_ROLE_PREFIX}{marking name}`.
        role: String,
        /// The per-row column/property name a rendering target's schema must
        /// carry (an array of marking names) for this predicate to be
        /// evaluable pushdown. See [`RESERVED_MARKING_COLUMN`].
        column: String,
    },
}

/// A single Marking name this bundle should render a predicate for — the
/// SET of names currently defined in au's `MARKING_REGISTRY`, never the
/// per-`(tenant, node_id)` assignment (see module doc).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarkingDef {
    pub name: String,
    /// Mirrors au's `Marking.requires_audit` — carried through for CA-26's own
    /// audit-trail decisions; this lane does not itself audit anything on the
    /// strength of this flag (see `eg_capabilities::policy`'s `audited: false`
    /// on `Method::PolicyExport`).
    pub requires_audit: bool,
}

/// Pure input to [`generate_bundle`] — every field is caller-supplied. See the
/// module doc for why (eg has no native Marking registry, no live Keycloak
/// subject enumeration, and no accessible-graphs authority of its own).
#[derive(Debug, Clone, Default)]
pub struct GenerateBundleInput {
    pub tenant: String,
    pub graphs: Vec<String>,
    pub principals: BTreeMap<String, Vec<String>>,
    pub marking_names: Vec<MarkingDef>,
}

/// Generate a [`PolicyBundle`] from `input`. Fails LOUDLY (never emits a
/// partial/empty bundle a consumer could mistake for "no restrictions") on any
/// malformed input: empty tenant, empty/duplicate graphs, an empty/duplicate/
/// out-of-charset marking name, or an empty principal subject/role string.
pub fn generate_bundle(input: &GenerateBundleInput) -> Result<PolicyBundle, String> {
    let tenant = input.tenant.trim();
    if tenant.is_empty() {
        return Err(
            "policy bundle generation requires a non-empty tenant (DEC-CA-04 A3: one \
             bundle per tenant, never per graph)"
                .to_string(),
        );
    }
    if input.graphs.is_empty() {
        return Err(
            "policy bundle generation requires at least one graph -- an empty `graphs` \
             list would be indistinguishable from an unscoped bundle a consumer could \
             mistake for 'applies everywhere' (DEC-CA-04 A3)"
                .to_string(),
        );
    }
    let mut seen_graphs = BTreeSet::new();
    for graph in &input.graphs {
        let graph = graph.trim();
        if graph.is_empty() {
            return Err("policy bundle generation rejects an empty graph name".to_string());
        }
        if !seen_graphs.insert(graph.to_string()) {
            return Err(format!(
                "policy bundle generation rejects duplicate graph '{graph}'"
            ));
        }
    }

    let mut markings = BTreeMap::new();
    let mut seen_marking_names = BTreeSet::new();
    for def in &input.marking_names {
        let name = def.name.trim();
        if name.is_empty() {
            return Err("policy bundle generation rejects an empty marking name".to_string());
        }
        if !name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.' | ':'))
        {
            return Err(format!(
                "policy bundle generation rejects marking name '{name}': must be an opaque \
                 identifier (ASCII alphanumeric, '_', '-', '.', ':' only) so it stays \
                 mechanically derivable, never free text"
            ));
        }
        if !seen_marking_names.insert(name.to_string()) {
            return Err(format!(
                "policy bundle generation rejects duplicate marking name '{name}'"
            ));
        }
        let predicate = MarkingPredicate::RequiresRole {
            role: format!("{MARKING_ROLE_PREFIX}{name}"),
            column: RESERVED_MARKING_COLUMN.to_string(),
        };
        let predicate_json = serde_json::to_string(&predicate).map_err(|error| {
            format!("policy bundle generation failed to encode marking '{name}': {error}")
        })?;
        markings.insert(
            name.to_string(),
            MarkingPolicyEntry {
                predicate: predicate_json,
            },
        );
    }

    for (subject, roles) in &input.principals {
        if subject.trim().is_empty() {
            return Err(
                "policy bundle generation rejects an empty principal subject".to_string(),
            );
        }
        for role in roles {
            if role.trim().is_empty() {
                return Err(format!(
                    "policy bundle generation rejects an empty role string for principal \
                     '{subject}'"
                ));
            }
        }
    }

    let generated_from = compute_epoch(tenant, &input.graphs, &input.principals, &markings);

    Ok(PolicyBundle {
        version: POLICY_BUNDLE_FORMAT_VERSION.to_string(),
        generated_from,
        governs: GOVERNS.iter().map(|s| s.to_string()).collect(),
        tenant: tenant.to_string(),
        graphs: input.graphs.clone(),
        principals: input.principals.clone(),
        markings,
        renderings: Renderings::default(),
    })
}

/// Deterministic `sha256` content hash over the bundle-defining inputs
/// (DEC-CA-04's contract: `generated_from` must be "tied to something that
/// actually changes with the predicate set"). `markings` is already the
/// GENERATED map (keys are the validated, deduplicated marking names), so
/// hashing it is equivalent to hashing the validated `marking_names` input but
/// avoids computing the predicate twice.
fn compute_epoch(
    tenant: &str,
    graphs: &[String],
    principals: &BTreeMap<String, Vec<String>>,
    markings: &BTreeMap<String, MarkingPolicyEntry>,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"eg-policy-bundle-epoch-v1\0");
    hasher.update(tenant.as_bytes());
    hasher.update(b"\0");
    let mut sorted_graphs: Vec<&str> = graphs.iter().map(String::as_str).collect();
    sorted_graphs.sort_unstable();
    for graph in sorted_graphs {
        hasher.update(graph.as_bytes());
        hasher.update(b"\0");
    }
    // `principals`/`markings` are `BTreeMap`s -- iteration order is already
    // sorted by key.
    for (subject, roles) in principals {
        hasher.update(subject.as_bytes());
        hasher.update(b"\0");
        let mut sorted_roles: Vec<&str> = roles.iter().map(String::as_str).collect();
        sorted_roles.sort_unstable();
        for role in sorted_roles {
            hasher.update(role.as_bytes());
            hasher.update(b"\0");
        }
    }
    for (name, entry) in markings {
        hasher.update(name.as_bytes());
        hasher.update(b"\0");
        hasher.update(entry.predicate.as_bytes());
        hasher.update(b"\0");
    }
    format!("epoch:sha256:{}", hex::encode(hasher.finalize()))
}

/// eg's own admin definition (module doc's "`*` vs `kg:admin`" section):
/// `"*"` OR `"kg:admin"` present in either the token's roles or its scopes.
/// Used ONLY by the standalone `/policy/export` HTTP surface, which bypasses
/// `server::dispatch`'s generic `Method` gate entirely and therefore needs its
/// own explicit check; `Method::PolicyExport` relies on that generic gate
/// (`policy:export` authz_action, see this module's doc) instead.
fn is_admin_claims(roles: &std::collections::HashSet<String>, scopes: &std::collections::HashSet<String>) -> bool {
    const ADMIN_TOKENS: [&str; 2] = ["*", "kg:admin"];
    ADMIN_TOKENS
        .iter()
        .any(|token| roles.contains(*token) || scopes.contains(*token))
}

// ── `/policy/export` HTTP surface (mirrors `server::sparql_http`/`server::kvcache_http`'s
// hand-rolled idiom, no new dependency) ──────────────────────────────────────────────

#[cfg(feature = "oidc")]
mod http {
    use super::{generate_bundle, is_admin_claims, GenerateBundleInput, MarkingDef};
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    const MAX_HEADER_BYTES: usize = 64 * 1024;
    const READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(15);

    struct ParsedRequest {
        method: String,
        path: String,
        query: String,
        headers: BTreeMap<String, String>,
    }

    /// Minimal GET-only HTTP/1.1 request-line + header reader. No body is
    /// read (or expected) — `/policy/export` is a pure read. Query-string
    /// values are split on `&`/`=` WITHOUT percent-decoding: acceptable for
    /// this admin-only, JWT-gated surface where every legal value (a tenant
    /// id, a graph name, a marking name) is already restricted to the opaque
    /// identifier charset [`super::generate_bundle`] validates; a caller
    /// needing a value outside that charset should use `Method::PolicyExport`
    /// over the primary msgpack protocol instead.
    async fn read_request(stream: &mut TcpStream) -> Option<ParsedRequest> {
        let mut buf = Vec::new();
        let mut tmp = [0u8; 4096];
        let header_end = loop {
            if let Some(pos) = buf
                .windows(4)
                .position(|window| window == b"\r\n\r\n")
            {
                break pos;
            }
            let n = stream.read(&mut tmp).await.ok()?;
            if n == 0 {
                return None;
            }
            buf.extend_from_slice(&tmp[..n]);
            if buf.len() > MAX_HEADER_BYTES {
                return None;
            }
        };
        let head = String::from_utf8_lossy(&buf[..header_end]).to_string();
        let mut lines = head.split("\r\n");
        let request_line = lines.next()?;
        let mut parts = request_line.split_whitespace();
        let method = parts.next()?.to_string();
        let target = parts.next()?.to_string();
        let version = parts.next()?;
        if !version.starts_with("HTTP/1.") {
            return None;
        }
        let (path, query) = target
            .split_once('?')
            .map(|(p, q)| (p.to_string(), q.to_string()))
            .unwrap_or((target, String::new()));
        let mut headers = BTreeMap::new();
        for line in lines {
            if let Some((k, v)) = line.split_once(':') {
                headers.insert(k.trim().to_ascii_lowercase(), v.trim().to_string());
            }
        }
        Some(ParsedRequest {
            method,
            path,
            query,
            headers,
        })
    }

    fn query_params(query: &str) -> BTreeMap<String, Vec<String>> {
        let mut out: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for pair in query.split('&').filter(|p| !p.is_empty()) {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            out.entry(k.to_string()).or_default().push(v.to_string());
        }
        out
    }

    async fn write_response(stream: &mut TcpStream, status: u16, reason: &str, body: &str) {
        let response = format!(
            "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        let _ = stream.write_all(response.as_bytes()).await;
        let _ = stream.shutdown().await;
    }

    async fn handle(stream: &mut TcpStream, validator: Option<Arc<crate::server::oidc::JwtValidator>>) {
        let Ok(Some(req)) =
            tokio::time::timeout(READ_TIMEOUT, read_request(stream)).await
        else {
            return;
        };
        if req.method != "GET" || req.path != "/policy/export" {
            write_response(stream, 404, "Not Found", "{\"error\":\"not found\"}").await;
            return;
        }
        let Some(validator) = validator.as_ref() else {
            write_response(
                stream,
                503,
                "Service Unavailable",
                "{\"error\":\"policy-export has no OIDC issuer configured; every request is denied (fail-closed)\"}",
            )
            .await;
            return;
        };
        let token = req
            .headers
            .get("authorization")
            .and_then(|h| h.strip_prefix("Bearer "))
            .map(str::trim)
            .filter(|t| !t.is_empty());
        let Some(token) = token else {
            write_response(
                stream,
                401,
                "Unauthorized",
                "{\"error\":\"missing bearer token\"}",
            )
            .await;
            return;
        };
        let Some(claims) = validator.validate_claims(token) else {
            write_response(
                stream,
                401,
                "Unauthorized",
                "{\"error\":\"bearer token failed verification\"}",
            )
            .await;
            return;
        };
        if !is_admin_claims(&claims.roles, &claims.scopes) {
            write_response(
                stream,
                403,
                "Forbidden",
                "{\"error\":\"ACCESS_DENIED: /policy/export requires '*' or 'kg:admin'\"}",
            )
            .await;
            return;
        }

        let params = query_params(&req.query);
        let tenant = params
            .get("tenant")
            .and_then(|v| v.first())
            .cloned()
            .unwrap_or_default();
        let graphs = params.get("graph").cloned().unwrap_or_default();
        let marking_names = params
            .get("marking")
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(|name| MarkingDef {
                name,
                requires_audit: false,
            })
            .collect();

        let mut roles: Vec<String> = claims
            .roles
            .iter()
            .chain(claims.scopes.iter())
            .cloned()
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();
        roles.sort();
        let mut principals = BTreeMap::new();
        principals.insert(claims.subject.clone(), roles);

        let input = GenerateBundleInput {
            tenant,
            graphs,
            principals,
            marking_names,
        };
        match generate_bundle(&input) {
            Ok(bundle) => match serde_json::to_string(&bundle) {
                Ok(body) => write_response(stream, 200, "OK", &body).await,
                Err(error) => {
                    write_response(
                        stream,
                        500,
                        "Internal Server Error",
                        &format!("{{\"error\":\"serialization failed: {error}\"}}"),
                    )
                    .await
                }
            },
            Err(error) => {
                let escaped = error.replace('"', "'");
                write_response(
                    stream,
                    400,
                    "Bad Request",
                    &format!("{{\"error\":\"{escaped}\"}}"),
                )
                .await
            }
        }
    }

    /// Serve `/policy/export` on `listener` (CA-16, DEC-CA-04). Admin-gated by
    /// an OIDC bearer token (`server::oidc::JwtValidator::from_env_primary` —
    /// the SAME validator the primary `eg2.` protocol's identity binding uses,
    /// so this surface and the RPC surface always agree on who is a caller).
    /// No issuer configured ⇒ every request is denied (fail-closed) — there is
    /// no static-secret fallback for this surface, unlike `/sparql`/KV-cache:
    /// this endpoint needs REAL claims (a subject + role/scope set) to
    /// populate `principals` and to gate admin, which a shared static secret
    /// cannot provide.
    pub async fn serve(listener: TcpListener) {
        if let Err(error) = crate::server::require_loopback_listener(&listener) {
            tracing::error!("policy-export listener refused: {error}");
            return;
        }
        let validator = match crate::server::oidc::JwtValidator::from_env_primary() {
            Ok(Some(v)) => Some(Arc::new(v)),
            Ok(None) => {
                tracing::warn!(
                    "policy-export: no OIDC issuer configured; every request will be denied \
                     (fail-closed, no static-secret fallback exists for this surface)"
                );
                None
            }
            Err(error) => {
                tracing::error!(
                    "policy-export: OIDC configuration is invalid ({error}); every request \
                     will be denied"
                );
                None
            }
        };
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                continue;
            };
            let validator = validator.clone();
            tokio::spawn(async move {
                handle(&mut stream, validator).await;
            });
        }
    }
}

#[cfg(feature = "oidc")]
pub use http::serve;

/// No-op fallback when the `oidc` feature is off (should not happen in
/// practice — `policy_export` implies `security` implies `oidc` — but keeps
/// this module compiling standalone if that implication is ever loosened).
#[cfg(not(feature = "oidc"))]
pub async fn serve(_listener: tokio::net::TcpListener) {
    tracing::error!(
        "policy-export listener requires the `oidc` feature (implied by `security`/`policy_export`); refusing to serve"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input_with_one_marking() -> GenerateBundleInput {
        let mut principals = BTreeMap::new();
        principals.insert(
            "svc:planner".to_string(),
            vec!["kg:read".to_string(), "kg:write".to_string()],
        );
        GenerateBundleInput {
            tenant: "tenant-a".to_string(),
            graphs: vec!["tenant:tenant-a".to_string(), "__commons__".to_string()],
            principals,
            marking_names: vec![MarkingDef {
                name: "confidential".to_string(),
                requires_audit: true,
            }],
        }
    }

    /// W03: a known marking set -> the expected bundle JSON shape, verbatim.
    #[test]
    fn known_marking_set_produces_the_expected_bundle_json() {
        let bundle = generate_bundle(&input_with_one_marking()).expect("valid input");
        assert_eq!(bundle.version, POLICY_BUNDLE_FORMAT_VERSION);
        assert_eq!(bundle.governs, vec!["M1".to_string()]);
        assert_eq!(bundle.tenant, "tenant-a");
        assert_eq!(
            bundle.graphs,
            vec!["tenant:tenant-a".to_string(), "__commons__".to_string()]
        );
        assert!(bundle.renderings.trino.is_empty());
        assert!(bundle.renderings.opensearch.is_empty());
        assert!(bundle.renderings.lakekeeper.is_empty());
        assert_eq!(
            bundle.principals.get("svc:planner").cloned(),
            Some(vec!["kg:read".to_string(), "kg:write".to_string()])
        );
        let entry = bundle.markings.get("confidential").expect("marking present");
        let predicate: MarkingPredicate =
            serde_json::from_str(&entry.predicate).expect("predicate decodes");
        assert_eq!(
            predicate,
            MarkingPredicate::RequiresRole {
                role: "marking:confidential".to_string(),
                column: RESERVED_MARKING_COLUMN.to_string(),
            }
        );
        assert!(bundle.generated_from.starts_with("epoch:sha256:"));
    }

    /// W07: malformed-registry-errors-loudly, one case per malformation.
    #[test]
    fn malformed_input_errors_loudly_never_a_partial_bundle() {
        let mut empty_tenant = input_with_one_marking();
        empty_tenant.tenant = "   ".to_string();
        assert!(generate_bundle(&empty_tenant).is_err());

        let mut empty_graphs = input_with_one_marking();
        empty_graphs.graphs.clear();
        assert!(generate_bundle(&empty_graphs).is_err());

        let mut dup_graphs = input_with_one_marking();
        dup_graphs.graphs.push(dup_graphs.graphs[0].clone());
        assert!(generate_bundle(&dup_graphs).is_err());

        let mut empty_marking_name = input_with_one_marking();
        empty_marking_name.marking_names.push(MarkingDef {
            name: String::new(),
            requires_audit: false,
        });
        assert!(generate_bundle(&empty_marking_name).is_err());

        let mut bad_charset = input_with_one_marking();
        bad_charset.marking_names.push(MarkingDef {
            name: "not a valid name!".to_string(),
            requires_audit: false,
        });
        assert!(generate_bundle(&bad_charset).is_err());

        let mut dup_marking = input_with_one_marking();
        dup_marking.marking_names.push(MarkingDef {
            name: "confidential".to_string(),
            requires_audit: false,
        });
        assert!(generate_bundle(&dup_marking).is_err());

        let mut empty_principal_subject = input_with_one_marking();
        empty_principal_subject
            .principals
            .insert(String::new(), vec!["kg:read".to_string()]);
        assert!(generate_bundle(&empty_principal_subject).is_err());

        let mut empty_role = input_with_one_marking();
        empty_role
            .principals
            .get_mut("svc:planner")
            .unwrap()
            .push(String::new());
        assert!(generate_bundle(&empty_role).is_err());
    }

    /// W07 negative test (also the lane's own acceptance gate 4): a Marking
    /// added to the input is invisible in the bundle generated BEFORE the
    /// addition, and visible (under a NEW epoch) in the bundle generated
    /// AFTER it.
    #[test]
    fn marking_appears_only_in_the_next_epoch() {
        let before = generate_bundle(&input_with_one_marking()).expect("valid input");
        assert!(!before.markings.contains_key("restricted"));

        let mut input_after = input_with_one_marking();
        input_after.marking_names.push(MarkingDef {
            name: "restricted".to_string(),
            requires_audit: false,
        });
        let after = generate_bundle(&input_after).expect("valid input");
        assert!(after.markings.contains_key("restricted"));
        assert!(before.markings.contains_key("confidential"));
        assert!(after.markings.contains_key("confidential"));

        assert_ne!(
            before.generated_from, after.generated_from,
            "adding a marking must advance the epoch (DEC-CA-04: generated_from is tied \
             to something that actually changes with the predicate set)"
        );

        // Same input twice -> same epoch (deterministic, not wall-clock-based).
        let after_again = generate_bundle(&input_after).expect("valid input");
        assert_eq!(after.generated_from, after_again.generated_from);
    }

    /// Acceptance gate 5 (known-bad, function level): a caller lacking `*`/
    /// `kg:admin` in either its roles or scopes is not admin under this
    /// module's own definition. The HTTP surface's 403 branch and
    /// `server::dispatch`'s generic `policy:export`/`kg:admin` gate both rest
    /// on this same claims shape -- see the module doc's admin-gate sections.
    #[test]
    fn non_admin_claims_are_not_admin() {
        let mut roles = std::collections::HashSet::new();
        roles.insert("kg:read".to_string());
        let mut scopes = std::collections::HashSet::new();
        scopes.insert("kg:write".to_string());
        assert!(!is_admin_claims(&roles, &scopes));

        scopes.insert("kg:admin".to_string());
        assert!(is_admin_claims(&roles, &scopes));

        let mut roles2 = std::collections::HashSet::new();
        roles2.insert("*".to_string());
        let scopes2 = std::collections::HashSet::new();
        assert!(
            is_admin_claims(&roles2, &scopes2),
            "eg's own admin definition honours '*' -- see the module doc's asymmetry note; \
             au's permissioning.py does NOT, a live, unresolved gap this module documents \
             rather than silently inherits"
        );
    }

    // ── Live-token proof (DEC-CA-04 A2): `principals` is claims-derived, never
    // `IsolationLayer.agents`/`rbac.redb` ──────────────────────────────────────

    #[cfg(feature = "oidc")]
    mod live_token_proof {
        use super::super::*;
        use crate::isolation::IsolationLayer;
        use crate::server::oidc::JwtValidator;
        use jsonwebtoken::{encode, Algorithm, DecodingKey, EncodingKey, Header};
        use std::collections::HashMap;
        use std::time::{SystemTime, UNIX_EPOCH};

        // RSA-2048 test keypair, PKCS#1 DER, generated solely for this test
        // (never a production key, never committed key material for any real
        // deployment) — the SAME literal key `server::oidc`'s own test suite
        // uses (`src/server/oidc.rs`'s `mod tests`), reused here rather than
        // re-generated so this proof exercises the identical RS256
        // sign/verify path with no risk of a second, subtly-different key
        // fixture drifting from the one that already proves the primary
        // protocol's OIDC binding works.
        const TEST_RSA_PRIVATE_KEY_PKCS1_DER_HEX: &str = "308204a30201000282010100be4725fd791744d873c4c82cc04ba74db85707a72581e4773e3f9041531b15ea57dcccda092adecbfa818521f10de4f849de2f6b359a20ad4eeec7da6aa550baf49a8f471089348b5c677a4c3d9b7f027395d3a08fa87345e4f842d3f5e6d9846f139883cb9ed94e1a868f85a741a5cb1262beaa4b395c6f9bc82fc46e65267cd50d7d752d2194b69a03ca41f3c135a9862f48d7697f74e8da8dca840cdf4f2cda9addc48ea6445574ffbc79f23144a520ba9aaa3ea8b549c25a89188a869a8ee7f05a096a66bfa4f49d4b5900f49579e88da8c25da9baea53f93cb69e744e5d80b55a41e0de41449bb437b53b57f6ef179eae0b3815a20b1df65fbdf28fc3b7020301000102820100019495093241f2381b5b62ba3f17f71a1b2785e5bfd700af1e323da027f0e2a6b6a21bdacd16b1110aa746becdc21573c67bf4f2dead700b60761fecd2d3f0040d820c7744f8e419d58e4fcd65a443fd7638f95aad0c1e20fcd23463e44d4d8ddf0a4fa0509c4f7bbeebfd31d95374981232b06e0e5539f7a75895fa50b1c061bcb1816d44e1c9155192cc37707747c6abf0af131a3b7d94a774fdc8a491d949ca0049b5845aca493b71352800d31d6f8d4e6beb352571f1586e9c9184a7a691cc556e53953ac5fc7995fed28d0fd92918b2dac30a4892595f70083f18d42a8768bb76077625bc917b347a8c3ec245db23f0eaaebeff571a7141891df5aa380102818100f6cae082d13337d73a723d4672f5a8b7113dfc820251e05380a672055c27dbab82c044f73fdb5d1a3fce5894fda55e57372fcf5f2704ee0ae927fd73c0e80eead6832d5a5938c3c63e69cab78d53e15b535d8a724e93eadf2d9ad45ce6bd2ae3653d087583fd0c7c8e9dac3c33c1f5bc651a2f69f898c379cc3722a85a163c0102818100c5607fbcc1a5a3ae9fa1a3c2469c17dd6d402515ecc724957d7fec575517254acf1dfc70c915390d8f489fae188c17372548603d442b06ad8195c74f8ee8bf51cfa22a2b4740d9e43e35d1942e4e4be545baf43127910c1c7e983f0f5ff5852f85311a56dc8d27fb1b5f669b0f7e83971f99ada964c1f4c6233299a84666dfb702818100d4186938a417d37eca4111be30e044fe07f870c13ec324fa3e8f4d60a3e1b15d46027d82cc4377512ed2e4b82f00e702277094549f51124f18300117710b3e7ebe9a7fe8acd3271581e02392fa07c39e5c1800fad9e32fb05c1e3b32182f2ce3bec6e4353298d0195febcbf0f53e553572e23d2b62b5cf1126db9f9275d1b40102818001a2a60c4b527303bc60db797d9a477c572e63e045a0f4c5a44f8e06bf36bce15ccbf3ce7f6c0497ff2aebdfc6664abef339214b00a8969a936b49467879a734275341a43027f26638b9bb6dcde06a32911c566f9dd34ed5619b23529e49eb7b944feed6ef66e000ed9e21bc81295c2fc15c459b14b1a2b48d901ac3d129830b0281807b5d9e95bf0e2892ff7ee7251fa14bec34d00c031d216c0f06dfa698407ec750e3d357e800907812a61d90281ce93320ad4a50d33364429710f249b87bc925ba89c5f675ed99229d09399943934811b25f4bac5a6cba9303dcd82ccbd31216092e1b9fe5ab1921188bd3e96256c692602be876e09c919c04735638b19646a658";
        const TEST_RSA_MODULUS_HEX: &str = "BE4725FD791744D873C4C82CC04BA74DB85707A72581E4773E3F9041531B15EA57DCCCDA092ADECBFA818521F10DE4F849DE2F6B359A20AD4EEEC7DA6AA550BAF49A8F471089348B5C677A4C3D9B7F027395D3A08FA87345E4F842D3F5E6D9846F139883CB9ED94E1A868F85A741A5CB1262BEAA4B395C6F9BC82FC46E65267CD50D7D752D2194B69A03CA41F3C135A9862F48D7697F74E8DA8DCA840CDF4F2CDA9ADDC48EA6445574FFBC79F23144A520BA9AAA3EA8B549C25A89188A869A8EE7F05A096A66BFA4F49D4B5900F49579E88DA8C25DA9BAEA53F93CB69E744E5D80B55A41E0DE41449BB437B53B57F6EF179EAE0B3815A20B1DF65FBDF28FC3B7";
        const TEST_RSA_EXPONENT_HEX: &str = "010001";
        const ISSUER: &str = "https://identity.example.test/realms/homelab";
        const AUDIENCE: &str = "epistemic-graph";
        const KID: &str = "ca16-test-kid-1";

        fn now() -> u64 {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs()
        }

        fn sign(claims: &serde_json::Value) -> String {
            let mut header = Header::new(Algorithm::RS256);
            header.kid = Some(KID.to_string());
            let der = hex::decode(TEST_RSA_PRIVATE_KEY_PKCS1_DER_HEX).expect("test key hex");
            let key = EncodingKey::from_rsa_der(&der);
            encode(&header, claims, &key).expect("sign test token")
        }

        fn validator() -> JwtValidator {
            let mut keys = HashMap::new();
            let n = hex::decode(TEST_RSA_MODULUS_HEX).expect("modulus hex");
            let e = hex::decode(TEST_RSA_EXPONENT_HEX).expect("exponent hex");
            keys.insert(KID.to_string(), DecodingKey::from_rsa_raw_components(&n, &e));
            JwtValidator::from_parts(ISSUER, AUDIENCE, keys)
        }

        /// A real RS256-signed, issuer/audience/expiry-verified token shaped
        /// exactly like a Keycloak realm `homelab` access token: `realm_access.
        /// roles`, one `resource_access.<client>.roles` block, and a
        /// space-delimited `scope` claim -- proves `principals` is populated
        /// from the token's OWN claims (via the SAME `oidc::JwtValidator::
        /// validate_claims` the primary protocol boundary uses), completely
        /// independent of `IsolationLayer.agents`/`rbac.redb`: the
        /// `IsolationLayer` built below has NEVER heard of this subject (no
        /// `register_agent` call at all) and yet the effective role set below
        /// is exactly right -- because nothing in this path ever consults it.
        #[test]
        fn live_token_proves_principals_are_claims_derived_not_rbac_redb() {
            let subject = "f47ac10b-58cc-4372-a567-0e02b2c3d479"; // Keycloak-shaped UUID subject
            let token = sign(&serde_json::json!({
                "sub": subject,
                "iss": ISSUER,
                "aud": AUDIENCE,
                "exp": now() + 300,
                "tenant_id": "tenant-a",
                "realm_access": {"roles": ["kg-reader"]},
                "resource_access": {"epistemic-graph": {"roles": ["kg:read"]}},
                "scope": "kg:read kg:write",
            }));

            let verified_claims = validator()
                .validate_claims(&token)
                .expect("a real RS256-signed, issuer/audience/expiry-correct token verifies");
            assert_eq!(verified_claims.subject, subject);

            // rbac.redb / IsolationLayer.agents proof: an EMPTY isolation
            // layer -- no `register_agent` for this subject at all -- so
            // `has_admin_capability`/`get_identity` see nothing for it.
            let isolation = IsolationLayer::new();
            assert!(
                !isolation.has_admin_capability(subject),
                "this subject was never registered in rbac.redb"
            );

            // The bundle's `principals` entry is built the SAME way
            // `server::dispatch`'s `Method::PolicyExport` arm builds it: the
            // union of the VERIFIED token's roles and scopes, sorted.
            let mut effective_roles: Vec<String> = verified_claims
                .roles
                .iter()
                .chain(verified_claims.scopes.iter())
                .cloned()
                .collect::<std::collections::BTreeSet<_>>()
                .into_iter()
                .collect();
            effective_roles.sort();
            assert_eq!(
                effective_roles,
                vec![
                    "kg-reader".to_string(),
                    "kg:read".to_string(),
                    "kg:write".to_string(),
                ],
                "the effective role set is the union of realm roles, every resource_access \
                 client's roles, and the scope claim -- exactly what reached rbac.redb: nothing"
            );

            let mut principals = BTreeMap::new();
            principals.insert(subject.to_string(), effective_roles.clone());
            let input = GenerateBundleInput {
                tenant: "tenant-a".to_string(),
                graphs: vec!["tenant:tenant-a".to_string(), "__commons__".to_string()],
                principals,
                marking_names: vec![],
            };
            let bundle = generate_bundle(&input).expect("valid input");
            assert_eq!(
                bundle.principals.get(subject).cloned(),
                Some(effective_roles),
                "the bundle's principals entry is exactly the live token's effective role set"
            );

            // Admin definition proof, against the SAME live claims: this
            // token carries neither `*` nor `kg:admin` in either roles or
            // scopes, so it is correctly NOT admin under this module's gate.
            assert!(!is_admin_claims(&verified_claims.roles, &verified_claims.scopes));
        }

        /// The `"*"` vs `kg:admin` asymmetry (module doc), proven against a
        /// live signed token rather than asserted in prose: a token minting
        /// only `"*"` clears eg's OWN admin gate (this module's
        /// `is_admin_claims`, and `CarrierAuthority`/`allows_action`
        /// elsewhere in this crate all agree). au's `permissioning.py`
        /// `_PRIVILEGED_ROLES = frozenset({"kg:admin"})` would NOT grant this
        /// same token privilege -- not re-provable in this Rust suite (au is
        /// Python, a separate repo), cited here as the concrete, live-token
        /// counterexample this lane's report names.
        #[test]
        fn wildcard_scope_is_admin_to_eg_a_live_token_proof() {
            let subject = "svc:wildcard-caller";
            let token = sign(&serde_json::json!({
                "sub": subject,
                "iss": ISSUER,
                "aud": AUDIENCE,
                "exp": now() + 300,
                "scope": "*",
            }));
            let verified_claims = validator()
                .validate_claims(&token)
                .expect("valid signed token verifies");
            assert!(verified_claims.scopes.contains("*"));
            assert!(!verified_claims.roles.contains("kg:admin"));
            assert!(
                is_admin_claims(&verified_claims.roles, &verified_claims.scopes),
                "eg's own admin gate honours a bare '*' scope"
            );
        }
    }
}
