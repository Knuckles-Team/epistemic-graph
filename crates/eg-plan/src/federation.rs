//! Query federation — EXTERNAL RowSet sources (CONCEPT:EG-KG.query.query-federation, Lane P).
//!
//! A federated query reads rows from a source OUTSIDE the local engine and composes
//! them with the local graph/vector/SQL ops in ONE plan. The seam is one trait —
//! [`ForeignSource`] — that turns an external source into the SAME [`RowSet`] currency
//! every other op speaks, so a `ForeignScan` is just another leaf source (like
//! `Scan`/`Reason`/`SparqlBgp`): its rows flow straight into a downstream
//! `Filter`/`Traverse`/`Rank`/`Limit`, and a foreign∩local JOIN is the existing
//! [`RowSet::intersect_keep_order`] keyed on id.
//!
//! Two kinds implement the trait, behind one [`eg_types::wire::ForeignSourceSpec`]:
//!
//!  * [`RemoteEngineSource`] — another epistemic-graph engine reached through a LOCAL
//!    verified-TLS tunnel/sidecar over the engine's length-prefixed-MessagePack + HMAC
//!    protocol (a blocking TCP client; the executor already runs on the blocking pool).
//!    Direct routable plaintext endpoints are rejected because request authentication
//!    alone does not provide query/result confidentiality. It sends a UQL
//!    UQL statement (`Uql`, or a `CypherQuery` when no UQL is given) and projects the
//!    remote rows into a local RowSet. This composes the engine with ANOTHER engine
//!    with NO Python round-trip — the cross-engine federation seam.
//!  * [`HttpJsonSource`] — a GENERIC HTTP/JSON API: GET `url`, walk to the JSON array
//!    at `json_path`, map each element to a row via `field_map`. Any REST API becomes
//!    a joinable RowSet. Public destinations must use HTTPS. Internal destinations
//!    are fail-closed unless their exact host/origin is opted in through
//!    [`HTTP_JSON_FEDERATION_ALLOW_ENV`]; DNS is resolved once, vetted, and pinned for
//!    the request. It uses a PURE-RUST rustls HTTP client (`ureq`), never openssl —
//!    and the whole `federation` feature is kept OUT of the Pi tier.
//!
//! The whole module is gated behind `federation` (which implies `query`): a default /
//! Pi build links no ureq/rustls/ring and carries no `ForeignScan` variant.

use std::collections::HashMap;
use std::sync::Arc;

use crate::federation_ssrf::validate_http_json_target;
use crate::rowset::RowSet;
use eg_types::wire::{ForeignSourceSpec, HttpFieldMap};

mod remote_engine;
#[cfg(feature = "federation-sql")]
mod sql;

pub use remote_engine::RemoteEngineSource;
#[cfg(feature = "federation-sql")]
pub use sql::{fetch_sql_columns, SqlSource};
#[cfg(feature = "federation-sql")]
pub(crate) use sql::{validate_federated_sql, SqlDialect};

/// The federation SSRF opt-in (owned by the crate's one outbound-destination gate).
pub use crate::federation_ssrf::HTTP_JSON_FEDERATION_ALLOW_ENV;

const MAX_HTTP_JSON_BODY_BYTES: usize = 64 * 1024 * 1024;
const MAX_HTTP_JSON_DEPTH: usize = 64;
const MAX_HTTP_JSON_ITEMS: usize = 1_000_000;
const MAX_HTTP_JSON_ROWS: usize = 250_000;
const MAX_HTTP_JSON_PATH_BYTES: usize = 4 * 1024;
const MAX_HTTP_JSON_FIELD_BYTES: usize = 1024;
const MAX_HTTP_JSON_ID_BYTES: usize = 64 * 1024;
const HTTP_JSON_CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
const HTTP_JSON_IO_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
const HTTP_JSON_TOTAL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(60);

/// The federation seam: turn an EXTERNAL source into the cross-modal [`RowSet`]
/// currency, so a `ForeignScan` composes with every local op. One method, one shape —
/// exactly what makes federation "just another RowSet source" rather than a bolted-on
/// second engine. CONCEPT:EG-KG.query.closure-backed-source confirms this as the trait the
/// [`ForeignSourceRegistry`] stores by name (`Arc<dyn ForeignSource + Send + Sync>`);
/// `fetch(&self)` is the `scan`-shaped method — the per-source connection spec is
/// captured in the concrete type rather than passed per call.
pub trait ForeignSource {
    /// Pull the foreign rows as a `RowSet`. A network/parse failure is an `Err` (the
    /// plan errors with a clear message rather than silently yielding nothing — a
    /// federated source being unreachable is a real error, not an empty result).
    fn fetch(&self) -> Result<RowSet, String>;
}

/// Build the right [`ForeignSource`] for a wire [`ForeignSourceSpec`]. The executor
/// calls this for an `Op::ForeignScan { source }` and runs `fetch()` on the blocking
/// pool, exactly like the SQL/vector legs.
pub fn source_for(spec: &ForeignSourceSpec) -> Box<dyn ForeignSource + '_> {
    match spec {
        ForeignSourceSpec::RemoteEngine {
            endpoint,
            graph,
            secret,
            context,
            uql,
            cypher,
            id_field,
        } => Box::new(RemoteEngineSource {
            endpoint,
            graph,
            secret,
            context,
            uql,
            cypher,
            id_field,
        }),
        ForeignSourceSpec::HttpJson {
            url,
            json_path,
            field_map,
        } => Box::new(HttpJsonSource {
            url,
            json_path,
            field_map,
        }),
        ForeignSourceSpec::Sql {
            dsn,
            query,
            id_field,
            score_field,
        } => sql_source(dsn, query, id_field, score_field.as_deref()),
        // CONCEPT:EG-KG.query.closure-backed-source — a `Named` spec is a REFERENCE, not a self-describing source:
        // it resolves through the executor's `ForeignSourceRegistry`, which `source_for`
        // (a pure spec→source builder with no registry) cannot reach. Hand-off is via
        // the executor / `ForeignSourceRegistry::resolve`; calling `source_for` on a
        // `Named` yields a clean error rather than a silent empty set.
        ForeignSourceSpec::Named { .. }
        | ForeignSourceSpec::Trino { .. }
        | ForeignSourceSpec::Cypher { .. }
        | ForeignSourceSpec::SparkBatch { .. } => unresolved_source(spec),
    }
}

/// Registry references and OQ-2 specs both require a separately bound source.
fn unresolved_source(spec: &ForeignSourceSpec) -> Box<dyn ForeignSource + '_> {
    match spec {
        ForeignSourceSpec::Named { name } => Box::new(NamedUnresolved { name }),
        other => Box::new(Oq2Unbound {
            kind: crate::federation_opt::oq2::kind(other).unwrap_or("unknown"),
        }),
    }
}

/// An OQ-2 spec alone has no verified credential lease, catalog probe, or
/// bound driver. Never silently use a generic SQL/Cypher transport for it.
struct Oq2Unbound {
    kind: &'static str,
}

impl ForeignSource for Oq2Unbound {
    fn fetch(&self) -> Result<RowSet, String> {
        Err(format!(
            "federation: {} source requires a verified registration and bound driver",
            self.kind
        ))
    }
}

// ── kind (c): an EXTERNAL relational-SQL database (CONCEPT:EG-KG.query.feature) ─────────────

/// Build the SQL foreign source. With `federation-sql` it is the real [`SqlSource`]
/// (a pure-Rust/rustls sqlx client). WITHOUT it — a `federation`-only build — the `Sql`
/// wire variant still exists (it is pure serde, so it registers + serializes fine), but
/// there is no driver linked, so `fetch()` errors with a clear "rebuild with
/// federation-sql" message rather than panicking or silently yielding nothing.
fn sql_source<'a>(
    dsn: &'a str,
    query: &'a str,
    id_field: &'a str,
    score_field: Option<&'a str>,
) -> Box<dyn ForeignSource + 'a> {
    #[cfg(feature = "federation-sql")]
    {
        Box::new(SqlSource {
            dsn,
            query,
            id_field,
            score_field,
        })
    }
    #[cfg(not(feature = "federation-sql"))]
    {
        let _ = (dsn, query, id_field, score_field);
        Box::new(SqlUnavailable)
    }
}

/// The not-built placeholder for the `Sql` kind when `federation-sql` is off.
#[cfg(not(feature = "federation-sql"))]
struct SqlUnavailable;

#[cfg(not(feature = "federation-sql"))]
impl ForeignSource for SqlUnavailable {
    fn fetch(&self) -> Result<RowSet, String> {
        Err(
            "federation: a Sql foreign source needs a server built with the \
             `federation-sql` feature (no SQL driver in this build)"
                .into(),
        )
    }
}

// ── kind (b): a generic HTTP/JSON source ───────────────────────────────────────

/// Reads rows from a generic HTTP/JSON API. Borrows the spec fields (no clones).
pub struct HttpJsonSource<'a> {
    url: &'a str,
    json_path: &'a str,
    field_map: &'a HttpFieldMap,
}

impl ForeignSource for HttpJsonSource<'_> {
    fn fetch(&self) -> Result<RowSet, String> {
        self.validate_projection()?;
        let body = self.fetch_body()?;
        self.project_body(&body)
    }
}

impl HttpJsonSource<'_> {
    fn validate_projection(&self) -> Result<(), String> {
        if self.json_path.len() > MAX_HTTP_JSON_PATH_BYTES
            || self.field_map.id.is_empty()
            || self.field_map.id.len() > MAX_HTTP_JSON_FIELD_BYTES
            || self
                .field_map
                .score
                .as_ref()
                .is_some_and(|field| field.len() > MAX_HTTP_JSON_FIELD_BYTES)
        {
            return Err("federation: invalid HTTP JSON projection".to_string());
        }

        Ok(())
    }

    fn fetch_body(&self) -> Result<Vec<u8>, String> {
        use std::io::Read;

        // Resolve exactly once, vet every answer, then pin the resulting socket list in
        // ureq's per-call resolver. This closes the usual validate-then-resolve DNS
        // rebinding gap. Environment proxies are disabled because routing a pinned
        // request through an unvalidated implicit proxy would invalidate that guarantee.
        let target = validate_http_json_target(self.url)?;
        let pinned_addresses = target.addresses.clone();
        let agent = ureq::AgentBuilder::new()
            .try_proxy_from_env(false)
            .resolver(
                move |_: &str| -> std::io::Result<Vec<std::net::SocketAddr>> {
                    Ok(pinned_addresses.clone())
                },
            )
            .https_only(target.https_only)
            .redirects(0)
            .timeout_connect(HTTP_JSON_CONNECT_TIMEOUT)
            .timeout_read(HTTP_JSON_IO_TIMEOUT)
            .timeout_write(HTTP_JSON_IO_TIMEOUT)
            .timeout(HTTP_JSON_TOTAL_TIMEOUT)
            .build();

        let response = agent
            .get(self.url)
            .call()
            .map_err(|_| "federation: HTTP JSON request failed".to_string())?;
        if !(200..300).contains(&response.status()) {
            return Err("federation: HTTP JSON source returned an error".to_string());
        }
        if response
            .header("Content-Length")
            .and_then(|value| value.parse::<u64>().ok())
            .is_some_and(|length| length > MAX_HTTP_JSON_BODY_BYTES as u64)
        {
            return Err("federation: HTTP JSON response exceeds limit".to_string());
        }

        // `take(max + 1)` applies to the decoded response stream too, so a compressed
        // body cannot inflate past the cap. The extra byte distinguishes exactly-at-cap
        // from oversized without trusting Content-Length or transfer framing.
        let mut body = Vec::new();
        response
            .into_reader()
            .take((MAX_HTTP_JSON_BODY_BYTES + 1) as u64)
            .read_to_end(&mut body)
            .map_err(|_| "federation: HTTP JSON response read failed".to_string())?;
        if body.len() > MAX_HTTP_JSON_BODY_BYTES {
            return Err("federation: HTTP JSON response exceeds limit".to_string());
        }
        Ok(body)
    }

    fn project_body(&self, body: &[u8]) -> Result<RowSet, String> {
        validate_json_shape(body)?;
        let json: serde_json::Value = serde_json::from_slice(body)
            .map_err(|_| "federation: invalid HTTP JSON response".to_string())?;
        let array = walk_json_path(&json, self.json_path)
            .ok_or_else(|| "federation: HTTP JSON projection did not resolve".to_string())?;
        let elems = array
            .as_array()
            .ok_or_else(|| "federation: HTTP JSON projection is not an array".to_string())?;
        if elems.len() > MAX_HTTP_JSON_ROWS {
            return Err("federation: HTTP JSON row count exceeds limit".to_string());
        }

        let mut rows = Vec::with_capacity(elems.len());
        for element in elems {
            let Some(value) = element.get(&self.field_map.id) else {
                continue;
            };
            let Some(id) = bounded_json_id(value)? else {
                continue;
            };
            let score = self
                .field_map
                .score
                .as_ref()
                .and_then(|field| element.get(field))
                .and_then(|v| v.as_f64())
                .filter(|value| value.is_finite() && value.abs() <= f32::MAX as f64)
                .map(|value| value as f32);
            rows.push((id, score));
        }
        Ok(RowSet::from_rows(rows))
    }
}

/// Allocation-free resource preflight. `serde_json` remains the grammar authority; this
/// pass bounds nesting and aggregate structural slots before it can allocate a DOM.
fn validate_json_shape(bytes: &[u8]) -> Result<(), String> {
    if bytes.len() > MAX_HTTP_JSON_BODY_BYTES {
        return Err("federation: HTTP JSON response exceeds limit".to_string());
    }
    let mut scan = JsonShapeScan::new(!bytes.is_empty());
    for byte in bytes.iter().copied() {
        scan.step(byte)?;
    }
    if scan.in_string || scan.depth != 0 {
        return Err("federation: invalid HTTP JSON response".to_string());
    }
    Ok(())
}

/// Running state for [`validate_json_shape`]'s single-pass, allocation-free
/// JSON structural scan: brace/bracket nesting depth, aggregate item count,
/// and whether the scan is currently inside a (possibly escaped) string.
#[derive(Default)]
struct JsonShapeScan {
    depth: usize,
    items: usize,
    in_string: bool,
    escaped: bool,
}

impl JsonShapeScan {
    fn new(bytes_nonempty: bool) -> Self {
        Self {
            items: usize::from(bytes_nonempty),
            ..Self::default()
        }
    }

    /// Advance the scan by one byte, enforcing the nesting/item-count bounds.
    fn step(&mut self, byte: u8) -> Result<(), String> {
        if self.in_string {
            self.step_in_string(byte);
            return Ok(());
        }
        self.step_structural(byte)?;
        if self.items > MAX_HTTP_JSON_ITEMS {
            return Err("federation: HTTP JSON item count exceeds limit".to_string());
        }
        Ok(())
    }

    fn step_in_string(&mut self, byte: u8) {
        if self.escaped {
            self.escaped = false;
        } else if byte == b'\\' {
            self.escaped = true;
        } else if byte == b'"' {
            self.in_string = false;
        }
    }

    fn step_structural(&mut self, byte: u8) -> Result<(), String> {
        match byte {
            b'"' => self.in_string = true,
            b'{' | b'[' => {
                self.depth = self.depth.saturating_add(1);
                self.items = self.items.saturating_add(1);
                if self.depth > MAX_HTTP_JSON_DEPTH {
                    return Err("federation: HTTP JSON nesting exceeds limit".to_string());
                }
            }
            b'}' | b']' => {
                self.depth = self
                    .depth
                    .checked_sub(1)
                    .ok_or_else(|| "federation: invalid HTTP JSON response".to_string())?;
            }
            b',' => self.items = self.items.saturating_add(1),
            _ => {}
        }
        Ok(())
    }
}

fn bounded_json_id(value: &serde_json::Value) -> Result<Option<String>, String> {
    let id = match value {
        serde_json::Value::String(value) => value.clone(),
        serde_json::Value::Number(value) => value.to_string(),
        serde_json::Value::Bool(value) => value.to_string(),
        serde_json::Value::Null | serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            return Ok(None);
        }
    };
    if id.len() > MAX_HTTP_JSON_ID_BYTES {
        return Err("federation: HTTP JSON row identifier exceeds limit".to_string());
    }
    Ok(Some(id))
}

#[cfg(test)]
mod json_preflight_tests {
    use super::{validate_json_shape, MAX_HTTP_JSON_DEPTH};

    #[test]
    fn json_preflight_rejects_excessive_nesting() {
        let mut body = vec![b'['; MAX_HTTP_JSON_DEPTH + 1];
        body.extend(std::iter::repeat_n(b']', MAX_HTTP_JSON_DEPTH + 1));
        assert!(validate_json_shape(&body).is_err());
    }
}

// ── shared helpers ─────────────────────────────────────────────────────────────

/// Walk a dotted JSON path (e.g. `data.items`) into `root`. An empty path returns the
/// root unchanged (the response IS the array). A missing segment ⇒ `None`.
fn walk_json_path<'a>(root: &'a serde_json::Value, path: &str) -> Option<&'a serde_json::Value> {
    let mut cur = root;
    for seg in path.split('.').filter(|s| !s.is_empty()) {
        cur = cur.get(seg)?;
    }
    Some(cur)
}

/// Stringify a JSON scalar to a row id. A JSON string is used verbatim (no quotes); any
/// other scalar is rendered without surrounding quotes (e.g. a number `42` → "42").
fn json_to_id(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

// ── CONCEPT:EG-KG.query.closure-backed-source — the foreign-source NAME REGISTRY + registerable source kinds ─

/// A boxed, thread-safe [`ForeignSource`] stored by name in a [`ForeignSourceRegistry`]
/// (CONCEPT:EG-KG.query.closure-backed-source). It is `Send + Sync` (the registry is shared across the executor's
/// blocking pool) and OWNS its data (`'static`) — unlike the borrow-based sources
/// [`source_for`] builds per-op straight off a wire spec.
pub type SharedForeignSource = Arc<dyn ForeignSource + Send + Sync>;

/// CONCEPT:EG-KG.query.closure-backed-source — the federation SOURCE REGISTRY: maps a foreign-source NAME to a live
/// [`ForeignSource`]. This is the resolution seam the UQL `FOREIGN "<name>"` clause
/// (`Op::Foreign`) and a `Named` [`eg_types::wire::Op::ForeignScan`] resolve through —
/// the piece the wire doc-comment flagged as "the server-side foreign_sources registry
/// that eg-plan (below the server) cannot reach". It now lives IN eg-plan and threads
/// into the executor via `PlanCtx::with_foreign`, so a name → rows resolution needs no
/// per-op inline spec and no Python round-trip.
///
/// A default `PlanCtx` carries no registry. Executing a name-resolving foreign op in
/// that context is a typed error, so a missing binding cannot silently leak local rows.
#[derive(Default, Clone)]
pub struct ForeignSourceRegistry {
    sources: HashMap<String, SharedForeignSource>,
    /// The self-describing spec behind each `register_spec` entry — what lets the federation
    /// optimizer push keys / limits into a named source (EH-563).
    specs: HashMap<String, ForeignSourceSpec>,
    cache_scope: Option<Arc<crate::federation_opt::FragmentCacheScope>>,
}

impl ForeignSourceRegistry {
    /// A new, empty registry (no foreign sources bound). CONCEPT:EG-KG.query.closure-backed-source.
    pub fn new() -> Self {
        Self::default()
    }

    /// Bound only by the served, verified owner registry after EH-400 checks the
    /// queried graph's named-source checkpoints.
    pub fn set_cache_scope(&mut self, scope: Arc<crate::federation_opt::FragmentCacheScope>) {
        self.cache_scope = Some(scope);
    }

    pub fn cache_scope(&self) -> Option<&Arc<crate::federation_opt::FragmentCacheScope>> {
        self.cache_scope.as_ref()
    }

    /// Register (or replace) a source under `name`. CONCEPT:EG-KG.query.closure-backed-source.
    pub fn register(&mut self, name: impl Into<String>, source: SharedForeignSource) -> &mut Self {
        let name = name.into();
        self.specs.remove(&name);
        self.sources.insert(name, source);
        self
    }

    /// Register an owned [`ForeignSourceSpec`] (remote-engine / HTTP-JSON / SQL) under a
    /// name — the kind that "resolves the name to another graph/dataset": a
    /// `RemoteEngine` spec pointed at another graph is exactly that, reached over the
    /// engine's own transport. CONCEPT:EG-KG.query.closure-backed-source.
    pub fn register_spec(&mut self, name: impl Into<String>, spec: ForeignSourceSpec) -> &mut Self {
        let name = name.into();
        self.register(name.clone(), Arc::new(SpecSource { spec: spec.clone() }));
        self.specs.insert(name, spec);
        self
    }

    /// The spec a `register_spec` entry was registered with (`None` for table/closure sources).
    pub fn spec(&self, name: &str) -> Option<&ForeignSourceSpec> {
        self.specs.get(name)
    }

    /// Register a FIXED table of rows (id + optional score) under a name — the
    /// zero-dependency source kind for tests + pre-materialized datasets.
    /// CONCEPT:EG-KG.query.closure-backed-source.
    pub fn register_table<I>(&mut self, name: impl Into<String>, rows: I) -> &mut Self
    where
        I: IntoIterator<Item = (String, Option<f32>)>,
    {
        self.register(
            name,
            Arc::new(TableSource {
                rows: rows.into_iter().collect(),
            }),
        )
    }

    /// Register a CLOSURE that produces the rows on demand under a name. CONCEPT:EG-KG.query.closure-backed-source.
    pub fn register_closure<F>(&mut self, name: impl Into<String>, f: F) -> &mut Self
    where
        F: Fn() -> Result<RowSet, String> + Send + Sync + 'static,
    {
        self.register(name, Arc::new(ClosureSource { f: Box::new(f) }))
    }

    /// Look a source up by name (borrowing the shared handle). CONCEPT:EG-KG.query.closure-backed-source.
    pub fn get(&self, name: &str) -> Option<&SharedForeignSource> {
        self.sources.get(name)
    }

    /// How many sources are registered.
    pub fn len(&self) -> usize {
        self.sources.len()
    }

    /// Whether NO sources are registered.
    pub fn is_empty(&self) -> bool {
        self.sources.is_empty()
    }

    /// Resolve `name` to its foreign rows, or a CLEAN typed error naming the unbound
    /// source (and listing what IS registered). This is what the executor calls for a
    /// `Named` `Op::ForeignScan` / an `Op::Foreign` marker. CONCEPT:EG-KG.query.closure-backed-source.
    pub fn resolve(&self, name: &str) -> Result<RowSet, String> {
        match self.sources.get(name) {
            Some(src) => src.fetch(),
            None => {
                let mut known: Vec<&str> = self.sources.keys().map(String::as_str).collect();
                known.sort_unstable();
                Err(format!(
                    "federation: no foreign source registered under name '{name}' \
                     (registered: {known:?}) (CONCEPT:EG-KG.query.closure-backed-source)"
                ))
            }
        }
    }
}

/// CONCEPT:EG-KG.query.closure-backed-source — a registerable source backed by an owned [`ForeignSourceSpec`]. It
/// delegates to [`source_for`], so the SAME remote-engine / HTTP-JSON / external-SQL
/// machinery becomes name-addressable. A `RemoteEngine` spec pointed at another graph is
/// the "in-engine source that resolves a name to another graph/dataset" kind. (A `Named`
/// spec here would recurse into `source_for`'s clean error rather than loop.)
pub struct SpecSource {
    spec: ForeignSourceSpec,
}

impl ForeignSource for SpecSource {
    fn fetch(&self) -> Result<RowSet, String> {
        source_for(&self.spec).fetch()
    }
}

/// CONCEPT:EG-KG.query.closure-backed-source — a registerable source backed by a FIXED table of rows (id + optional
/// score). The zero-dependency kind for tests and pre-materialized foreign datasets.
pub struct TableSource {
    rows: Vec<(String, Option<f32>)>,
}

impl ForeignSource for TableSource {
    fn fetch(&self) -> Result<RowSet, String> {
        Ok(RowSet::from_rows(self.rows.iter().cloned()))
    }
}

/// CONCEPT:EG-KG.query.closure-backed-source — a registerable source backed by a CLOSURE producing rows on demand
/// (e.g. an in-engine adapter that reads from another dataset the host holds).
pub struct ClosureSource {
    f: Box<dyn Fn() -> Result<RowSet, String> + Send + Sync>,
}

impl ForeignSource for ClosureSource {
    fn fetch(&self) -> Result<RowSet, String> {
        (self.f)()
    }
}

/// CONCEPT:EG-KG.query.closure-backed-source — the placeholder [`ForeignSource`] a `Named` spec resolves to when it
/// reaches [`source_for`] (which has no registry). It always errors, pointing the caller
/// at the `ForeignSourceRegistry`, so a misrouted `Named` fails loudly, never silently.
struct NamedUnresolved<'a> {
    name: &'a str,
}

impl ForeignSource for NamedUnresolved<'_> {
    fn fetch(&self) -> Result<RowSet, String> {
        Err(format!(
            "federation: the Named foreign source '{}' resolves through the \
             ForeignSourceRegistry on the PlanCtx, not source_for (CONCEPT:EG-KG.query.closure-backed-source)",
            self.name
        ))
    }
}

#[cfg(test)]
mod symmetric_scan_oracle {
    //! Compose-oracle (CONCEPT:EG-KG.query.symmetric-foreign-scan): a `ForeignScan` leaf
    //! composes through the executor's `Driver` seam EXACTLY like an internal `Scan` leaf.
    //! Proof: register a foreign source returning EXACTLY the rows an internal
    //! `Scan("Doc")` seeds, then run TWO plans that differ ONLY in the leaf op
    //! (`Scan` vs `ForeignScan{join:false}`) under the SAME downstream
    //! `Filter -> Rank -> Limit`. The two results MUST be byte-identical — the seam does
    //! not distinguish a locally-scanned source from a foreign one.
    use crate::algebra::Op;
    use crate::exec::{execute, PlanCtx};
    use crate::federation::ForeignSourceRegistry;
    use crate::rowset::Row;
    use crate::Plan;
    use eg_types::wire::{ForeignSourceSpec, Pred};

    /// The identical downstream applied to BOTH the internal and foreign leaf.
    fn downstream() -> Vec<Op> {
        vec![
            Op::Filter {
                preds: vec![Pred::GtNum {
                    prop: "year".into(),
                    n: 2023.0,
                }],
            },
            Op::Rank {
                query: crate::fixture::query_vec(),
            },
            Op::Limit { k: 10 },
        ]
    }

    fn as_pairs(rows: &[Row]) -> Vec<(String, Option<f32>)> {
        rows.iter().map(|r| (r.id.clone(), r.score)).collect()
    }

    #[test]
    fn foreign_scan_composes_exactly_like_internal_scan() {
        let fx = crate::fixture::build();

        // Capture the EXACT RowSet an internal `Scan("Doc")` leaf seeds.
        let ctx_plain = PlanCtx::new(&fx.view, &fx.semantic);
        let scanned = execute(
            &Plan::new(vec![Op::Scan {
                label: "Doc".into(),
            }]),
            &ctx_plain,
        )
        .unwrap();
        let scanned_rows = as_pairs(scanned.rows());

        // A foreign source that returns EXACTLY those rows — the symmetric mirror of Scan.
        let mut registry = ForeignSourceRegistry::new();
        registry.register_table("mirror-doc", scanned_rows);

        // Two plans, identical but for the leaf op.
        let mut internal_ops = vec![Op::Scan {
            label: "Doc".into(),
        }];
        internal_ops.extend(downstream());
        let mut foreign_ops = vec![Op::ForeignScan {
            source: Box::new(ForeignSourceSpec::Named {
                name: "mirror-doc".into(),
            }),
            join: false,
        }];
        foreign_ops.extend(downstream());

        let ctx = PlanCtx::new(&fx.view, &fx.semantic).with_foreign(&registry);
        let internal = execute(&Plan::new(internal_ops), &ctx).unwrap();
        let foreign = execute(&Plan::new(foreign_ops), &ctx).unwrap();

        assert_eq!(
            as_pairs(foreign.rows()),
            as_pairs(internal.rows()),
            "a ForeignScan leaf composes byte-identically to an internal Scan leaf"
        );
        // And the composition actually did something (guards a vacuous pass).
        assert!(!internal.ids().is_empty(), "downstream produced rows");
    }
}

#[cfg(test)]
mod envelope_signer_tests {
    //! Native federation must use the same verified v2 identity/policy envelope
    //! as every other served client. These tests pin its canonical bindings and
    //! prove the former v0/empty-secret downgrade no longer exists.
    use super::RemoteEngineSource;
    use eg_types::acl::RequestContextClaims;
    use eg_types::protocol::{Method, Request};

    fn context() -> RequestContextClaims {
        RequestContextClaims {
            principal: "agent:federation".into(),
            tenant: "tenant-a".into(),
            audience: "epistemic-graph".into(),
            agent_id: "agent:federation".into(),
            roles: vec!["federation-reader".into()],
            scopes: vec!["kg:read".into()],
            policy_version: "policy-test".into(),
            delegation: vec![],
            node: None,
            priority: None,
        }
    }

    fn source<'a>(
        secret: &'a str,
        context: &'a RequestContextClaims,
        graph: &'a str,
    ) -> RemoteEngineSource<'a> {
        RemoteEngineSource {
            endpoint: "127.0.0.1:0",
            graph,
            secret,
            context,
            uql: "",
            cypher: "",
            id_field: "id",
        }
    }

    fn request(id: u64, graph: &str, agent_id: &str) -> Request {
        Request {
            id,
            graph: graph.to_string(),
            auth_token: String::new(),
            agent_id: Some(agent_id.to_string()),
            method: Method::Ping,
        }
    }

    #[test]
    fn live_request_path_produces_a_verified_v2_token() {
        let claims = context();
        let src = source("federation-test-secret", &claims, "g");
        let req = src.signed_request(Method::Ping).unwrap();
        assert!(req.auth_token.starts_with("eg2."));
        assert_eq!(req.agent_id.as_deref(), Some("agent:federation"));
    }

    #[test]
    fn empty_secret_fails_before_dial_instead_of_downgrading() {
        let claims = context();
        let src = source("", &claims, "g");
        assert!(src.signed_request(Method::Ping).is_err());
    }

    #[test]
    fn incomplete_context_fails_before_dial() {
        let mut claims = context();
        claims.policy_version.clear();
        let src = source("federation-test-secret", &claims, "g");
        assert!(src.signed_request(Method::Ping).is_err());
    }

    #[test]
    fn graph_binding_changes_the_token() {
        let claims = context();
        let src = source("federation-test-secret", &claims, "g");
        let token_a = src
            .auth_token(&request(1, "g", &claims.agent_id), 1, "nonce", "idem")
            .unwrap();
        let token_b = src
            .auth_token(
                &request(1, "other-graph", &claims.agent_id),
                1,
                "nonce",
                "idem",
            )
            .unwrap();
        assert_ne!(
            token_a, token_b,
            "signing over a different graph must yield a different token"
        );
    }

    #[test]
    fn tenant_binding_changes_the_token() {
        let claims_a = context();
        let mut claims_b = context();
        claims_b.tenant = "tenant-b".into();
        let req = request(1, "g", &claims_a.agent_id);
        let t1 = source("federation-test-secret", &claims_a, "g")
            .auth_token(&req, 1, "nonce", "idem")
            .unwrap();
        let t2 = source("federation-test-secret", &claims_b, "g")
            .auth_token(&req, 1, "nonce", "idem")
            .unwrap();
        assert_ne!(
            t1, t2,
            "signing over a different tenant must yield a different token"
        );
    }

    #[test]
    fn method_body_binding_changes_the_token() {
        let claims = context();
        let src = source("federation-test-secret", &claims, "g");
        let mut req = request(1, "g", &claims.agent_id);
        req.method = Method::Ping;
        let t1 = src.auth_token(&req, 1, "nonce", "idem").unwrap();
        req.method = Method::Health;
        let t2 = src.auth_token(&req, 1, "nonce", "idem").unwrap();
        assert_ne!(
            t1, t2,
            "signing over a different method must yield a different token"
        );
    }
}
