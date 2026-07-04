//! GraphQL enterprise hardening (CONCEPT:EG-KG.domains.graphql-enterprise-hardening): production controls that run BEFORE
//! the resolver, so a hostile or accidentally-pathological query is rejected without ever
//! touching the graph. All of it is a THIN pre-pass over the already-parsed AST — no new
//! resolution path, no async-graphql, and (like the rest of the crate) Pi-excludable: the
//! whole module is gated behind the `hardening` sub-feature (default-on, folded into the
//! facade's `graphql`/`node`/`full` tiers exactly the way `federation` is — see
//! `Cargo.toml`; NO tier-aggregate-list edit needed).
//!
//! What it adds, in the order the policy entry point applies them
//! ([`execute_with_policy`]):
//!   1. **APQ resolve** ([`ApqRegistry`]) — Apollo Automatic Persisted Queries: a request
//!      may send only a `sha256Hash` (run the registered query, or `PersistedQueryNotFound`
//!      when unknown) or `hash + query` (verify `hash == sha256(query)`, register, run).
//!   2. **Introspection toggle** — when `introspection_enabled` is false, a query that
//!      selects `__schema`/`__type` is rejected (locked-down production). `__typename`
//!      stays allowed (it is a per-node meta-field, not schema introspection).
//!   3. **Depth limit** — the max selection-set nesting depth is computed by walking the
//!      desugared field tree; a query deeper than `max_depth` is rejected.
//!   4. **Complexity / cost limit** — each field costs 1, a LIST field (one with a nested
//!      selection — root types + edge traversals both fan out) multiplies its subtree by a
//!      page factor (the `first`/`limit` arg if present, else `list_page_factor`); the sum
//!      must stay within `max_complexity`, and the raw field count within `max_fields`.
//!   5. the EXISTING [`crate::resolver::execute_with_variables`] runs the query unchanged.
//!
//! A [`GraphQlPolicy::default`] is fully PERMISSIVE (no limits, introspection on), so the
//! existing `execute`/`execute_with_variables` callers are untouched and
//! [`execute_with_policy`] with the default policy behaves exactly like a normal execute
//! (plus APQ resolution, which is a no-op for a plain `query`-only request).
//!
//! ## Deferred (documented)
//! Rate limiting (per-client token buckets), field-level authorization, and query
//! ALLOWLISTING (persisted-operation-only mode) are follow-ups; APQ here is the
//! registry/protocol substrate an allowlist would build on.

use std::collections::HashMap;
use std::sync::Mutex;

use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::parser::{parse_raw, Field, GqlValue};
use crate::resolver::{bind_variables, execute_with_variables, flatten_document};

use eg_core::graph::GraphView;

/// The production controls for the GraphQL surface (CONCEPT:EG-KG.domains.graphql-enterprise-hardening): the depth/complexity
/// LIMITS plus the introspection/APQ TOGGLES, bundled into one config the server layer
/// carries. Cheap to clone; holds no state (the APQ store is the separate
/// [`ApqRegistry`]).
///
/// [`GraphQlPolicy::default`] is permissive — every limit is `None` and introspection is
/// on — so wrapping an existing call in [`execute_with_policy`] with the default changes
/// nothing.
#[derive(Clone, Debug, PartialEq)]
pub struct GraphQlPolicy {
    /// Reject a query whose selection-set nesting is deeper than this (`None` = unbounded).
    pub max_depth: Option<usize>,
    /// Reject a query whose weighted cost exceeds this (`None` = unbounded).
    pub max_complexity: Option<usize>,
    /// Reject a query with more than this many total fields (`None` = unbounded).
    pub max_fields: Option<usize>,
    /// The assumed page size for a LIST field with no explicit `first`/`limit` — the
    /// factor its subtree cost is multiplied by in the complexity sum.
    pub list_page_factor: usize,
    /// When false, `__schema`/`__type` introspection is rejected (locked-down prod).
    pub introspection_enabled: bool,
    /// When false, a hash-only (persisted) request is refused even if the hash is known —
    /// APQ registration/lookup is disabled. A plain `query` still runs.
    pub apq_enabled: bool,
}

impl Default for GraphQlPolicy {
    /// A fully PERMISSIVE policy: no limits, introspection + APQ on. Applying it via
    /// [`execute_with_policy`] leaves normal queries working unchanged (CONCEPT:EG-KG.domains.graphql-enterprise-hardening).
    fn default() -> Self {
        Self {
            max_depth: None,
            max_complexity: None,
            max_fields: None,
            list_page_factor: 100,
            introspection_enabled: true,
            apq_enabled: true,
        }
    }
}

impl GraphQlPolicy {
    /// A hardened production preset: bounded depth/complexity/fields and introspection
    /// OFF (CONCEPT:EG-KG.domains.graphql-enterprise-hardening). Callers can tune individual fields afterwards.
    pub fn locked_down() -> Self {
        Self {
            max_depth: Some(10),
            max_complexity: Some(10_000),
            max_fields: Some(1_000),
            list_page_factor: 100,
            introspection_enabled: false,
            apq_enabled: true,
        }
    }
}

/// The result of analysing a query against a policy: its measured depth, weighted
/// complexity, and total field count (CONCEPT:EG-KG.domains.graphql-enterprise-hardening). Returned by [`analyze`] so a
/// caller can log/meter the numbers even when the query is admitted.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QueryCost {
    pub depth: usize,
    pub complexity: usize,
    pub fields: usize,
}

/// The sha256→query store backing Automatic Persisted Queries (CONCEPT:EG-KG.domains.graphql-enterprise-hardening). Thread-
/// safe (an internal `Mutex`), so one registry is shared across the server's request
/// handlers. Keys are lowercase-hex sha256 digests of the query text; Apollo clients send
/// this digest under `extensions.persistedQuery.sha256Hash`.
#[derive(Debug, Default)]
pub struct ApqRegistry {
    map: Mutex<HashMap<String, String>>,
}

impl ApqRegistry {
    /// A new, empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register `query` under its sha256 digest, returning the digest. Idempotent.
    pub fn register(&self, query: &str) -> String {
        let hash = sha256_hex(query);
        self.map
            .lock()
            .unwrap()
            .insert(hash.clone(), query.to_string());
        hash
    }

    /// Look up the query registered under `hash` (lowercase-hex sha256), if any.
    pub fn get(&self, hash: &str) -> Option<String> {
        self.map.lock().unwrap().get(hash).cloned()
    }

    /// Number of registered persisted queries.
    pub fn len(&self) -> usize {
        self.map.lock().unwrap().len()
    }

    /// Whether the registry holds no persisted queries.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// The lowercase-hex sha256 digest of `s` — the APQ key (CONCEPT:EG-KG.domains.graphql-enterprise-hardening).
pub fn sha256_hex(s: &str) -> String {
    let mut h = Sha256::new();
    h.update(s.as_bytes());
    let digest = h.finalize();
    let mut out = String::with_capacity(64);
    for b in digest {
        out.push_str(&format!("{b:02x}"));
    }
    out
}

/// One incoming GraphQL request under the APQ protocol (CONCEPT:EG-KG.domains.graphql-enterprise-hardening): the query text
/// and/or its persisted-query `sha256Hash`, plus the execution variables. This mirrors an
/// Apollo request where `query` and `extensions.persistedQuery.sha256Hash` are each
/// optional (but at least one must be present).
#[derive(Clone, Debug)]
pub struct GraphQlRequest<'a> {
    /// The query text, if the client sent it.
    pub query: Option<&'a str>,
    /// The persisted-query sha256 digest (lowercase hex), if the client sent it.
    pub sha256: Option<&'a str>,
    /// Execution variables (a JSON object, or `Value::Null` for none).
    pub variables: &'a Value,
}

impl<'a> GraphQlRequest<'a> {
    /// A plain query request (no persisted-query hash, no variables).
    pub fn query(query: &'a str) -> Self {
        Self {
            query: Some(query),
            sha256: None,
            variables: &Value::Null,
        }
    }

    /// A hash-only (persisted) request.
    pub fn persisted(sha256: &'a str) -> Self {
        Self {
            query: None,
            sha256: Some(sha256),
            variables: &Value::Null,
        }
    }
}

/// The stable error string an unknown persisted-query hash yields — the Apollo APQ
/// `PersistedQueryNotFound` signal a client keys on to retry with the full query
/// (CONCEPT:EG-KG.domains.graphql-enterprise-hardening).
pub const PERSISTED_QUERY_NOT_FOUND: &str = "PersistedQueryNotFound";

/// Resolve an APQ request to the concrete query text to execute (CONCEPT:EG-KG.domains.graphql-enterprise-hardening),
/// applying Apollo's protocol semantics:
///   * `query` present, no hash → run it (no registration).
///   * `query` + `hash` → verify `hash == sha256(query)`; on mismatch reject; else
///     register and run.
///   * `hash` only → look it up; found → run; unknown → `PersistedQueryNotFound`.
///   * neither → an error.
///
/// When `apq_enabled` is false the registry is bypassed: a `query` still runs (its hash is
/// still validated when supplied, but nothing is registered), and a hash-only request is
/// refused.
pub fn resolve_apq(
    req: &GraphQlRequest<'_>,
    registry: &ApqRegistry,
    apq_enabled: bool,
) -> Result<String, String> {
    match (req.query, req.sha256) {
        (Some(q), Some(h)) => {
            let actual = sha256_hex(q);
            if !actual.eq_ignore_ascii_case(h) {
                return Err(format!(
                    "GraphQL APQ: provided sha256Hash `{h}` does not match the query digest `{actual}`"
                ));
            }
            if apq_enabled {
                registry.register(q);
            }
            Ok(q.to_string())
        }
        (Some(q), None) => Ok(q.to_string()),
        (None, Some(h)) => {
            if !apq_enabled {
                return Err("GraphQL APQ: persisted queries are disabled".to_string());
            }
            registry
                .get(h)
                .ok_or_else(|| PERSISTED_QUERY_NOT_FOUND.to_string())
        }
        (None, None) => {
            Err("GraphQL: a request must carry a `query` or a persisted `sha256Hash`".to_string())
        }
    }
}

// ── AST walks: depth / complexity / introspection (CONCEPT:EG-KG.domains.graphql-enterprise-hardening) ────────────────

/// The nesting depth of a single field: 1 for a scalar leaf, else 1 + the deepest child.
fn field_depth(f: &Field) -> usize {
    1 + f.selection.iter().map(field_depth).max().unwrap_or(0)
}

/// The max selection-set nesting depth over a resolved root set (CONCEPT:EG-KG.domains.graphql-enterprise-hardening). A flat
/// `{ Person { name } }` is depth 2 (`Person` → `name`); `{ a { b { c } } }` is depth 3.
pub fn query_depth(roots: &[Field]) -> usize {
    roots.iter().map(field_depth).max().unwrap_or(0)
}

/// The page factor a LIST field contributes: its explicit `first`/`limit` arg if present,
/// else the policy `list_page_factor`.
fn page_factor(f: &Field, default_factor: usize) -> usize {
    for (k, v) in &f.args {
        if k == "first" || k == "limit" {
            if let GqlValue::Int(n) = v {
                if *n >= 0 {
                    return *n as usize;
                }
            }
        }
    }
    default_factor
}

/// The weighted cost of a single field: a scalar leaf is 1; a field WITH a selection is a
/// list fan-out — 1 plus `page_factor` times the summed cost of its children.
fn field_cost(f: &Field, factor: usize) -> usize {
    if f.selection.is_empty() {
        return 1;
    }
    let child_sum: usize = f
        .selection
        .iter()
        .map(|c| field_cost(c, factor))
        .sum::<usize>();
    let n = page_factor(f, factor);
    1usize.saturating_add(n.saturating_mul(child_sum))
}

/// The weighted complexity of a resolved root set (CONCEPT:EG-KG.domains.graphql-enterprise-hardening): the sum of each root's
/// [`field_cost`], list fields multiplied by their page factor.
pub fn query_complexity(roots: &[Field], list_page_factor: usize) -> usize {
    roots
        .iter()
        .map(|r| field_cost(r, list_page_factor))
        .fold(0usize, |a, b| a.saturating_add(b))
}

/// The total number of fields in the resolved root set (every node in the tree).
fn field_count(f: &Field) -> usize {
    1 + f.selection.iter().map(field_count).sum::<usize>()
}

/// The total field count over a resolved root set (CONCEPT:EG-KG.domains.graphql-enterprise-hardening).
pub fn query_field_count(roots: &[Field]) -> usize {
    roots.iter().map(field_count).sum()
}

/// Whether a resolved root set selects `__schema`/`__type` introspection anywhere
/// (CONCEPT:EG-KG.domains.graphql-enterprise-hardening). `__typename` is NOT introspection (it is a per-node meta-field), so it
/// is not flagged.
pub fn selects_introspection(roots: &[Field]) -> bool {
    fn walk(f: &Field) -> bool {
        if f.name == "__schema" || f.name == "__type" {
            return true;
        }
        f.selection.iter().any(walk)
    }
    roots.iter().any(walk)
}

/// Analyse a resolved query against a policy, returning its [`QueryCost`] or the first
/// limit it violates (CONCEPT:EG-KG.domains.graphql-enterprise-hardening). Order: introspection → depth → complexity → field
/// count. Pure over the AST; runs BEFORE any graph access.
pub fn check_policy(roots: &[Field], policy: &GraphQlPolicy) -> Result<QueryCost, String> {
    if !policy.introspection_enabled && selects_introspection(roots) {
        return Err(
            "GraphQL: introspection is disabled (`__schema`/`__type` are not available)"
                .to_string(),
        );
    }
    let depth = query_depth(roots);
    if let Some(max) = policy.max_depth {
        if depth > max {
            return Err(format!(
                "GraphQL: query depth {depth} exceeds the max_depth limit of {max}"
            ));
        }
    }
    let complexity = query_complexity(roots, policy.list_page_factor);
    if let Some(max) = policy.max_complexity {
        if complexity > max {
            return Err(format!(
                "GraphQL: query complexity {complexity} exceeds the max_complexity limit of {max}"
            ));
        }
    }
    let fields = query_field_count(roots);
    if let Some(max) = policy.max_fields {
        if fields > max {
            return Err(format!(
                "GraphQL: query selects {fields} fields, exceeding the max_fields limit of {max}"
            ));
        }
    }
    Ok(QueryCost {
        depth,
        complexity,
        fields,
    })
}

/// Desugar a query string to its resolved root [`Field`]s (the exact tree the resolver
/// will run), so the policy analyses what actually executes — fragments inlined,
/// `@skip`/`@include` applied, `$var`s bound. A non-query operation is rejected.
fn resolve_roots(query: &str, variables: &Value) -> Result<Vec<Field>, String> {
    let doc = parse_raw(query).map_err(|e| e.to_string())?;
    if doc.op_kind != "query" {
        return Err(format!(
            "GraphQL: expected a query operation, found a {}",
            doc.op_kind
        ));
    }
    let vars = bind_variables(&doc.var_defs, variables);
    flatten_document(&doc, &vars)
}

/// Analyse a raw query string against a policy without executing it (CONCEPT:EG-KG.domains.graphql-enterprise-hardening) —
/// the depth/complexity/introspection gate, exposed for callers that want the verdict
/// (or the [`QueryCost`] numbers) independently of resolution.
pub fn analyze(
    query: &str,
    variables: &Value,
    policy: &GraphQlPolicy,
) -> Result<QueryCost, String> {
    let roots = resolve_roots(query, variables)?;
    check_policy(&roots, policy)
}

/// THE hardened entry point (CONCEPT:EG-KG.domains.graphql-enterprise-hardening): apply the policy, then run the existing
/// resolver. The steps, in order:
///   1. **APQ resolve** — turn the (query | hash | hash+query) request into a query string
///      ([`resolve_apq`]).
///   2. **introspection check → depth → complexity → field count** ([`check_policy`]) over
///      the desugared tree.
///   3. **execute** — the UNCHANGED [`execute_with_variables`] over `view`.
///
/// With [`GraphQlPolicy::default`] (permissive) and a plain `query` request this is
/// behaviourally identical to calling `execute_with_variables` directly.
pub fn execute_with_policy(
    view: &GraphView,
    req: &GraphQlRequest<'_>,
    policy: &GraphQlPolicy,
    registry: &ApqRegistry,
) -> Result<Value, String> {
    let query = resolve_apq(req, registry, policy.apq_enabled)?;
    let roots = resolve_roots(&query, req.variables)?;
    check_policy(&roots, policy)?;
    execute_with_variables(view, &query, req.variables)
}

#[cfg(test)]
mod tests {
    use super::*;
    use eg_core::graph::GraphCore;
    use serde_json::json;

    fn pbytes(v: Value) -> Vec<u8> {
        rmp_serde::to_vec_named(&v).unwrap()
    }

    /// alice/bob/carol People (+ a Doc), alice-KNOWS->bob-KNOWS->carol — the same shape
    /// the crate-level tests use, so hardening is exercised over a real resolvable graph.
    fn view() -> eg_core::graph::GraphView {
        let core = GraphCore::new();
        core.add_node(
            "alice".into(),
            pbytes(json!({"type":"Person","name":"Alice","age":30})),
        );
        core.add_node(
            "bob".into(),
            pbytes(json!({"type":"Person","name":"Bob","age":25})),
        );
        core.add_node(
            "carol".into(),
            pbytes(json!({"type":"Person","name":"Carol","age":40})),
        );
        core.add_node("d1".into(), pbytes(json!({"type":"Doc","title":"Graphs"})));
        core.add_edge(
            "alice".into(),
            "bob".into(),
            pbytes(json!({"relationship":"KNOWS"})),
        )
        .unwrap();
        core.add_edge(
            "bob".into(),
            "carol".into(),
            pbytes(json!({"relationship":"KNOWS"})),
        )
        .unwrap();
        core.analysis_snapshot()
    }

    // ── depth (CONCEPT:EG-KG.domains.graphql-enterprise-hardening) ────────────────────────────────────────────────────

    /// EG-296: a `max_depth` policy REJECTS a query nested deeper than the limit and
    /// ADMITS a shallow one.
    #[test]
    fn eg296_depth_limit_rejects_too_deep_allows_shallow() {
        let v = view();
        let policy = GraphQlPolicy {
            max_depth: Some(2),
            ..GraphQlPolicy::default()
        };
        let reg = ApqRegistry::new();

        // depth 3: Person → KNOWS → name  → rejected.
        let deep = GraphQlRequest::query(r#"{ Person(name: "Alice") { KNOWS { name } } }"#);
        let err = execute_with_policy(&v, &deep, &policy, &reg).unwrap_err();
        assert!(err.contains("depth"), "expected a depth error, got {err}");
        assert!(err.contains("max_depth"), "got {err}");

        // depth 2: Person → name  → admitted.
        let shallow = GraphQlRequest::query(r#"{ Person(name: "Alice") { name } }"#);
        let ok = execute_with_policy(&v, &shallow, &policy, &reg).unwrap();
        assert_eq!(ok["data"]["Person"][0]["name"], json!("Alice"));
    }

    /// EG-296: raw depth measurement — flat is 2, nested edge is 3.
    #[test]
    fn eg296_query_depth_measures_nesting() {
        let flat = resolve_roots("{ Person { name } }", &Value::Null).unwrap();
        assert_eq!(query_depth(&flat), 2);
        let nested = resolve_roots("{ Person { KNOWS { name } } }", &Value::Null).unwrap();
        assert_eq!(query_depth(&nested), 3);
    }

    // ── complexity / cost (CONCEPT:EG-KG.domains.graphql-enterprise-hardening) ────────────────────────────────────────

    /// EG-296: a `max_complexity` policy rejects a costly list query (a list field's
    /// subtree is multiplied by the page factor) while a cheap one passes.
    #[test]
    fn eg296_complexity_limit_rejects_costly_list_query() {
        let v = view();
        let policy = GraphQlPolicy {
            max_complexity: Some(100),
            list_page_factor: 10,
            ..GraphQlPolicy::default()
        };
        let reg = ApqRegistry::new();

        // Person(1 + 10*(KNOWS=1+10*(name+age=2)=21)=211) → over 100, rejected.
        let costly = GraphQlRequest::query("{ Person { KNOWS { name age } } }");
        let err = execute_with_policy(&v, &costly, &policy, &reg).unwrap_err();
        assert!(err.contains("complexity"), "got {err}");
        assert!(err.contains("max_complexity"), "got {err}");

        // Person(1 + 10*(name=1)=11) → under 100, admitted.
        let cheap = GraphQlRequest::query("{ Person { name } }");
        assert!(execute_with_policy(&v, &cheap, &policy, &reg).is_ok());
    }

    /// EG-296: an explicit `first` on a list field is used as its page factor in the cost.
    #[test]
    fn eg296_complexity_uses_explicit_first_as_factor() {
        // Person(first:2) { name } → 1 + 2*1 = 3.
        let roots = resolve_roots("{ Person(first: 2) { name } }", &Value::Null).unwrap();
        assert_eq!(query_complexity(&roots, 100), 3);
    }

    /// EG-296: the total field-count cap rejects a wide query independent of depth/cost.
    #[test]
    fn eg296_field_count_cap_rejects_wide_query() {
        let v = view();
        let policy = GraphQlPolicy {
            max_fields: Some(2),
            ..GraphQlPolicy::default()
        };
        let reg = ApqRegistry::new();
        // Person + name + age + title = 4 fields → over 2.
        let wide = GraphQlRequest::query("{ Person { name age } Doc { title } }");
        let err = execute_with_policy(&v, &wide, &policy, &reg).unwrap_err();
        assert!(err.contains("max_fields"), "got {err}");
    }

    // ── APQ (CONCEPT:EG-KG.domains.graphql-enterprise-hardening) ──────────────────────────────────────────────────────

    /// EG-296: a hash-only request FAILS with `PersistedQueryNotFound` when unregistered,
    /// SUCCEEDS after the query is registered via a hash+query request, and a hash+query
    /// with a MISMATCHED hash is rejected.
    #[test]
    fn eg296_apq_hash_only_lifecycle_and_mismatch() {
        let v = view();
        let policy = GraphQlPolicy::default();
        let reg = ApqRegistry::new();
        let q = "{ Person { name } }";
        let hash = sha256_hex(q);

        // (a) hash-only, unregistered → PersistedQueryNotFound.
        let miss = GraphQlRequest::persisted(&hash);
        let err = execute_with_policy(&v, &miss, &policy, &reg).unwrap_err();
        assert_eq!(err, PERSISTED_QUERY_NOT_FOUND);

        // (b) hash + query → validates + registers + runs.
        let register = GraphQlRequest {
            query: Some(q),
            sha256: Some(&hash),
            variables: &Value::Null,
        };
        let ok = execute_with_policy(&v, &register, &policy, &reg).unwrap();
        assert_eq!(ok["data"]["Person"].as_array().unwrap().len(), 3);
        assert_eq!(reg.len(), 1);

        // (c) hash-only again → now registered, runs.
        let hit = GraphQlRequest::persisted(&hash);
        let ok2 = execute_with_policy(&v, &hit, &policy, &reg).unwrap();
        assert_eq!(ok2["data"]["Person"].as_array().unwrap().len(), 3);

        // (d) hash+query with a WRONG hash → rejected before execution.
        let wrong = "0000000000000000000000000000000000000000000000000000000000000000";
        let bad = GraphQlRequest {
            query: Some(q),
            sha256: Some(wrong),
            variables: &Value::Null,
        };
        let err2 = execute_with_policy(&v, &bad, &policy, &reg).unwrap_err();
        assert!(err2.contains("does not match"), "got {err2}");
    }

    /// EG-296: sha256_hex is the standard lowercase-hex digest Apollo clients compute.
    #[test]
    fn eg296_sha256_hex_matches_known_vector() {
        // sha256("abc") — a well-known NIST test vector.
        assert_eq!(
            sha256_hex("abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    // ── introspection toggle (CONCEPT:EG-KG.domains.graphql-enterprise-hardening) ─────────────────────────────────────

    /// EG-296: with introspection OFF, a `__schema` query is blocked BEFORE execution.
    #[test]
    fn eg296_introspection_off_blocks_schema() {
        let v = view();
        let policy = GraphQlPolicy {
            introspection_enabled: false,
            ..GraphQlPolicy::default()
        };
        let reg = ApqRegistry::new();
        let intro = GraphQlRequest::query("{ __schema { types { name } } }");
        let err = execute_with_policy(&v, &intro, &policy, &reg).unwrap_err();
        assert!(err.contains("introspection is disabled"), "got {err}");

        // __typename is NOT introspection — it stays allowed.
        assert!(!selects_introspection(
            &resolve_roots("{ Person { __typename } }", &Value::Null).unwrap()
        ));
    }

    // ── default policy is permissive (CONCEPT:EG-KG.domains.graphql-enterprise-hardening) ─────────────────────────────

    /// EG-296: the DEFAULT policy leaves a normal query working exactly as a bare
    /// `execute` would — no limits tripped, introspection on, APQ a no-op for a plain
    /// query.
    #[test]
    fn eg296_default_policy_leaves_normal_query_working() {
        let v = view();
        let policy = GraphQlPolicy::default();
        let reg = ApqRegistry::new();
        let req = GraphQlRequest::query(r#"{ Person(name: "Alice") { name KNOWS { name } } }"#);
        let res = execute_with_policy(&v, &req, &policy, &reg).unwrap();
        assert_eq!(res["data"]["Person"][0]["name"], json!("Alice"));
        assert_eq!(res["data"]["Person"][0]["KNOWS"][0]["name"], json!("Bob"));
    }

    /// EG-296: the `locked_down` preset admits a small normal query but its introspection
    /// is off.
    #[test]
    fn eg296_locked_down_preset_admits_normal_blocks_introspection() {
        let v = view();
        let policy = GraphQlPolicy::locked_down();
        let reg = ApqRegistry::new();
        assert!(execute_with_policy(
            &v,
            &GraphQlRequest::query("{ Person { name } }"),
            &policy,
            &reg
        )
        .is_ok());
        assert!(execute_with_policy(
            &v,
            &GraphQlRequest::query("{ __type(name: \"Person\") { name } }"),
            &policy,
            &reg
        )
        .is_err());
    }

    /// EG-296: a request carrying neither a query nor a hash is a clear error.
    #[test]
    fn eg296_request_without_query_or_hash_errors() {
        let reg = ApqRegistry::new();
        let req = GraphQlRequest {
            query: None,
            sha256: None,
            variables: &Value::Null,
        };
        let err = resolve_apq(&req, &reg, true).unwrap_err();
        assert!(err.contains("must carry"), "got {err}");
    }

    /// EG-296: with `apq_enabled = false`, a hash-only request is refused outright.
    #[test]
    fn eg296_apq_disabled_refuses_hash_only() {
        let reg = ApqRegistry::new();
        let req = GraphQlRequest::persisted("deadbeef");
        let err = resolve_apq(&req, &reg, false).unwrap_err();
        assert!(err.contains("disabled"), "got {err}");
    }
}
