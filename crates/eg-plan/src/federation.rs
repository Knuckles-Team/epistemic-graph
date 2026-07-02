//! Query federation — EXTERNAL RowSet sources (CONCEPT:KG-2.232, Lane P).
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
//!  * [`RemoteEngineSource`] — a REMOTE epistemic-graph engine, queried over the SAME
//!    length-prefixed-MessagePack + HMAC transport this engine speaks (a blocking TCP
//!    client; the executor already runs on the blocking pool). It sends a UQL
//!    `UnifiedQueryText` (or a `CypherQuery` when no UQL is given) and projects the
//!    remote rows into a local RowSet. This composes the engine with ANOTHER engine
//!    with NO Python round-trip — the cross-engine federation seam.
//!  * [`HttpJsonSource`] — a GENERIC HTTP/JSON API: GET `url`, walk to the JSON array
//!    at `json_path`, map each element to a row via `field_map`. Any REST API becomes
//!    a joinable RowSet. It uses a PURE-RUST rustls HTTP client (`ureq`), never
//!    openssl — and the whole `federation` feature is kept OUT of the Pi tier.
//!
//! The whole module is gated behind `federation` (which implies `query`): a default /
//! Pi build links no ureq/rustls/ring and carries no `ForeignScan` variant.

use std::collections::HashMap;
use std::sync::Arc;

use crate::rowset::RowSet;
use eg_types::wire::{ForeignSourceSpec, HttpFieldMap};

/// The federation seam: turn an EXTERNAL source into the cross-modal [`RowSet`]
/// currency, so a `ForeignScan` composes with every local op. One method, one shape —
/// exactly what makes federation "just another RowSet source" rather than a bolted-on
/// second engine. CONCEPT:EG-073 confirms this as the trait the
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
            uql,
            cypher,
            id_field,
        } => Box::new(RemoteEngineSource {
            endpoint,
            graph,
            secret,
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
        // CONCEPT:EG-073 — a `Named` spec is a REFERENCE, not a self-describing source:
        // it resolves through the executor's `ForeignSourceRegistry`, which `source_for`
        // (a pure spec→source builder with no registry) cannot reach. Hand-off is via
        // the executor / `ForeignSourceRegistry::resolve`; calling `source_for` on a
        // `Named` yields a clean error rather than a silent empty set.
        ForeignSourceSpec::Named { name } => Box::new(NamedUnresolved { name }),
    }
}

// ── kind (c): an EXTERNAL relational-SQL database (CONCEPT:KG-2.239) ─────────────

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

// ── kind (a): a remote epistemic-graph engine ──────────────────────────────────

/// Reads rows from a REMOTE epistemic-graph engine over the engine's own transport
/// (length-prefixed MessagePack + HMAC-SHA256). Borrows the spec fields (no clones).
pub struct RemoteEngineSource<'a> {
    endpoint: &'a str,
    graph: &'a str,
    secret: &'a str,
    uql: &'a str,
    cypher: &'a str,
    id_field: &'a str,
}

impl ForeignSource for RemoteEngineSource<'_> {
    fn fetch(&self) -> Result<RowSet, String> {
        if self.uql.trim().is_empty() {
            self.fetch_cypher()
        } else {
            self.fetch_uql()
        }
    }
}

impl RemoteEngineSource<'_> {
    /// HMAC-SHA256 hex token over the request id — the SAME scheme `src/server/auth.rs`
    /// (`compute_auth_token`) verifies. An empty secret yields an empty token (the
    /// remote then runs `--allow-insecure`).
    fn auth_token(&self, request_id: u64) -> String {
        use hmac::{Hmac, Mac};
        use sha2::Sha256;
        if self.secret.is_empty() {
            return String::new();
        }
        let Ok(mut mac) = Hmac::<Sha256>::new_from_slice(self.secret.as_bytes()) else {
            return String::new();
        };
        mac.update(request_id.to_string().as_bytes());
        hex::encode(mac.finalize().into_bytes())
    }

    /// One framed round-trip to the remote: connect TCP, write `[u32 len][msgpack
    /// Request]`, read `[u32 len][msgpack Response]`, return the response's `Raw`
    /// payload bytes (or the remote's error). Blocking — the executor runs it on the
    /// blocking pool, exactly like the local SQL leg.
    fn round_trip(&self, request: &eg_types::protocol::Request) -> Result<Vec<u8>, String> {
        use std::io::{Read, Write};
        use std::net::TcpStream;

        let mut stream = TcpStream::connect(self.endpoint)
            .map_err(|e| format!("federation: connect {} failed: {e}", self.endpoint))?;
        let body =
            rmp_serde::to_vec_named(request).map_err(|e| format!("federation: encode req: {e}"))?;
        let len = (body.len() as u32).to_be_bytes();
        stream
            .write_all(&len)
            .and_then(|()| stream.write_all(&body))
            .map_err(|e| format!("federation: write to {}: {e}", self.endpoint))?;

        let mut len_buf = [0u8; 4];
        stream
            .read_exact(&mut len_buf)
            .map_err(|e| format!("federation: read len from {}: {e}", self.endpoint))?;
        let resp_len = u32::from_be_bytes(len_buf) as usize;
        let mut resp_buf = vec![0u8; resp_len];
        stream
            .read_exact(&mut resp_buf)
            .map_err(|e| format!("federation: read body from {}: {e}", self.endpoint))?;
        let resp: eg_types::protocol::Response = rmp_serde::from_slice(&resp_buf)
            .map_err(|e| format!("federation: decode resp: {e}"))?;
        if let Some(err) = resp.error {
            return Err(format!("federation: remote error: {err}"));
        }
        // `ResultPayload::raw()` and `PropertiesMsgpack` are the SAME msgpack `bin` on
        // the wire; the untagged decoder picks `PropertiesMsgpack` (it is declared
        // first), so accept either — both carry the msgpack body we re-decode.
        match resp.result {
            Some(
                eg_types::protocol::ResultPayload::Raw(bytes)
                | eg_types::protocol::ResultPayload::PropertiesMsgpack(bytes),
            ) => Ok(bytes),
            other => Err(format!(
                "federation: expected a msgpack-bin result, got {other:?}"
            )),
        }
    }

    /// UQL path: the remote runs the query through its own unified planner and returns
    /// `[id, score?]` rows directly — the SAME shape this engine's `UnifiedQuery`
    /// yields, so the projection is the identity.
    fn fetch_uql(&self) -> Result<RowSet, String> {
        let request = eg_types::protocol::Request {
            id: 1,
            graph: self.graph.to_string(),
            auth_token: self.auth_token(1),
            agent_id: None,
            method: eg_types::protocol::Method::UnifiedQueryText {
                text: self.uql.to_string(),
                reorder_filter_selectivity: None,
            },
        };
        let raw = self.round_trip(&request)?;
        let rows: Vec<(String, Option<f32>)> = rmp_serde::from_slice(&raw)
            .map_err(|e| format!("federation: decode unified rows: {e}"))?;
        Ok(RowSet::from_rows(rows))
    }

    /// Cypher path: the remote returns a `QueryResult { columns, rows }`; pick the
    /// `id_field` column out of each row (each row is msgpack `Vec<Value>` aligned to
    /// `columns`) and build an unscored RowSet.
    fn fetch_cypher(&self) -> Result<RowSet, String> {
        let request = eg_types::protocol::Request {
            id: 1,
            graph: self.graph.to_string(),
            auth_token: self.auth_token(1),
            agent_id: None,
            method: eg_types::protocol::Method::CypherQuery {
                query: self.cypher.to_string(),
            },
        };
        let raw = self.round_trip(&request)?;
        let result: eg_types::protocol::QueryResult = rmp_serde::from_slice(&raw)
            .map_err(|e| format!("federation: decode cypher result: {e}"))?;
        let id_field = if self.id_field.is_empty() {
            "id"
        } else {
            self.id_field
        };
        let col = result
            .columns
            .iter()
            .position(|c| c == id_field)
            .ok_or_else(|| {
                format!(
                    "federation: cypher result has no '{id_field}' column (have {:?})",
                    result.columns
                )
            })?;
        let mut ids = Vec::with_capacity(result.rows.len());
        for row in &result.rows {
            let cells: Vec<serde_json::Value> = rmp_serde::from_slice(row)
                .map_err(|e| format!("federation: decode cypher row: {e}"))?;
            if let Some(v) = cells.get(col) {
                ids.push(json_to_id(v));
            }
        }
        Ok(RowSet::from_ids(ids))
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
        // ureq is a blocking, pure-Rust rustls client — the executor runs this on the
        // blocking pool. A 4xx/5xx is an error (the source is unreachable/wrong).
        let body = ureq::get(self.url)
            .call()
            .map_err(|e| format!("federation: GET {} failed: {e}", self.url))?
            .into_string()
            .map_err(|e| format!("federation: read body of {}: {e}", self.url))?;
        let json: serde_json::Value = serde_json::from_str(&body)
            .map_err(|e| format!("federation: parse JSON from {}: {e}", self.url))?;
        let array = walk_json_path(&json, self.json_path).ok_or_else(|| {
            format!(
                "federation: json_path '{}' did not resolve to a value in the response",
                self.json_path
            )
        })?;
        let elems = array.as_array().ok_or_else(|| {
            format!(
                "federation: json_path '{}' resolved to a non-array",
                self.json_path
            )
        })?;
        let rows = elems.iter().filter_map(|el| {
            let id = el.get(&self.field_map.id).map(json_to_id)?;
            let score = self
                .field_map
                .score
                .as_ref()
                .and_then(|sf| el.get(sf))
                .and_then(|v| v.as_f64())
                .map(|f| f as f32);
            Some((id, score))
        });
        Ok(RowSet::from_rows(rows))
    }
}

// ── kind (c) impl: external relational-SQL (CONCEPT:KG-2.239, feature `federation-sql`) ─

/// Reads rows from an EXTERNAL relational DB (Postgres/MySQL) over a pure-Rust/rustls
/// `sqlx` client. Borrows the spec fields (no clones). The DSN scheme picks the dialect
/// (`postgres://`/`postgresql://` ⇒ Postgres, `mysql://` ⇒ MySQL); each row's `id_field`
/// column becomes the row id and the optional `score_field` becomes the row score.
///
/// `fetch()` is SYNC (the executor runs it on the blocking pool, exactly like the SQL /
/// vector legs) but `sqlx` is async, so it spins a small current-thread tokio runtime to
/// drive the connect+query to completion. This pairs a per-call connection — connection
/// pooling + pushdown to the external DB are documented follow-ups.
#[cfg(feature = "federation-sql")]
pub struct SqlSource<'a> {
    dsn: &'a str,
    query: &'a str,
    id_field: &'a str,
    score_field: Option<&'a str>,
}

#[cfg(feature = "federation-sql")]
impl ForeignSource for SqlSource<'_> {
    fn fetch(&self) -> Result<RowSet, String> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| format!("federation: build tokio runtime: {e}"))?;
        rt.block_on(self.fetch_async())
    }
}

#[cfg(feature = "federation-sql")]
impl SqlSource<'_> {
    async fn fetch_async(&self) -> Result<RowSet, String> {
        let scheme = self.dsn.split(':').next().unwrap_or("");
        match scheme {
            "postgres" | "postgresql" => self.fetch_postgres().await,
            "mysql" | "mariadb" => self.fetch_mysql().await,
            other => Err(format!(
                "federation: unsupported SQL dsn scheme '{other}' (expected postgres:// or mysql://)"
            )),
        }
    }

    async fn fetch_postgres(&self) -> Result<RowSet, String> {
        use sqlx::Connection;
        let mut conn = sqlx::postgres::PgConnection::connect(self.dsn)
            .await
            .map_err(|e| format!("federation: connect postgres: {e}"))?;
        let rows = sqlx::query(self.query)
            .fetch_all(&mut conn)
            .await
            .map_err(|e| format!("federation: postgres query: {e}"))?;
        let mut out = Vec::with_capacity(rows.len());
        for row in &rows {
            out.push(self.project_pg_row(row)?);
        }
        Ok(RowSet::from_rows(out))
    }

    async fn fetch_mysql(&self) -> Result<RowSet, String> {
        use sqlx::Connection;
        let mut conn = sqlx::mysql::MySqlConnection::connect(self.dsn)
            .await
            .map_err(|e| format!("federation: connect mysql: {e}"))?;
        let rows = sqlx::query(self.query)
            .fetch_all(&mut conn)
            .await
            .map_err(|e| format!("federation: mysql query: {e}"))?;
        let mut out = Vec::with_capacity(rows.len());
        for row in &rows {
            out.push(self.project_my_row(row)?);
        }
        Ok(RowSet::from_rows(out))
    }

    fn project_pg_row(&self, row: &sqlx::postgres::PgRow) -> Result<(String, Option<f32>), String> {
        use sqlx::Row;
        let id = pg_col_to_id(row, self.id_field)?;
        // Read the score as f64 (float8/numeric-via-double) or f32 (float4) — a NULL or a
        // non-numeric column yields no score rather than erroring (the score is optional).
        let score = self.score_field.and_then(|sf| {
            row.try_get::<f64, _>(sf)
                .map(|v| v as f32)
                .or_else(|_| row.try_get::<f32, _>(sf))
                .ok()
        });
        Ok((id, score))
    }

    fn project_my_row(&self, row: &sqlx::mysql::MySqlRow) -> Result<(String, Option<f32>), String> {
        use sqlx::Row;
        let id = my_col_to_id(row, self.id_field)?;
        let score = self.score_field.and_then(|sf| {
            row.try_get::<f64, _>(sf)
                .map(|v| v as f32)
                .or_else(|_| row.try_get::<f32, _>(sf))
                .ok()
        });
        Ok((id, score))
    }
}

/// Read the `id_field` column of a Postgres row as a String id, trying the common id
/// SQL types in order (text, then integer, then float). A column that decodes as none of
/// these errors clearly (rather than silently dropping the row).
#[cfg(feature = "federation-sql")]
fn pg_col_to_id(row: &sqlx::postgres::PgRow, col: &str) -> Result<String, String> {
    use sqlx::Row;
    if let Ok(s) = row.try_get::<String, _>(col) {
        return Ok(s);
    }
    if let Ok(n) = row.try_get::<i64, _>(col) {
        return Ok(n.to_string());
    }
    if let Ok(n) = row.try_get::<i32, _>(col) {
        return Ok(n.to_string());
    }
    if let Ok(f) = row.try_get::<f64, _>(col) {
        return Ok(f.to_string());
    }
    Err(format!(
        "federation: id column '{col}' is not a string/int/float (cast it to text in the query)"
    ))
}

/// MySQL counterpart of [`pg_col_to_id`].
#[cfg(feature = "federation-sql")]
fn my_col_to_id(row: &sqlx::mysql::MySqlRow, col: &str) -> Result<String, String> {
    use sqlx::Row;
    if let Ok(s) = row.try_get::<String, _>(col) {
        return Ok(s);
    }
    if let Ok(n) = row.try_get::<i64, _>(col) {
        return Ok(n.to_string());
    }
    if let Ok(n) = row.try_get::<i32, _>(col) {
        return Ok(n.to_string());
    }
    if let Ok(f) = row.try_get::<f64, _>(col) {
        return Ok(f.to_string());
    }
    Err(format!(
        "federation: id column '{col}' is not a string/int/float (cast it to text in the query)"
    ))
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

// ── CONCEPT:EG-073 — the foreign-source NAME REGISTRY + registerable source kinds ─

/// A boxed, thread-safe [`ForeignSource`] stored by name in a [`ForeignSourceRegistry`]
/// (CONCEPT:EG-073). It is `Send + Sync` (the registry is shared across the executor's
/// blocking pool) and OWNS its data (`'static`) — unlike the borrow-based sources
/// [`source_for`] builds per-op straight off a wire spec.
pub type SharedForeignSource = Arc<dyn ForeignSource + Send + Sync>;

/// CONCEPT:EG-073 — the federation SOURCE REGISTRY: maps a foreign-source NAME to a live
/// [`ForeignSource`]. This is the resolution seam the UQL `FOREIGN "<name>"` clause
/// (`Op::Foreign`) and a `Named` [`eg_types::wire::Op::ForeignScan`] resolve through —
/// the piece the wire doc-comment flagged as "the server-side foreign_sources registry
/// that eg-plan (below the server) cannot reach". It now lives IN eg-plan and threads
/// into the executor via `PlanCtx::with_foreign`, so a name → rows resolution needs no
/// per-op inline spec and no Python round-trip.
///
/// A default `PlanCtx` carries NO registry (`None`), so every existing plan is
/// unchanged: an `Op::Foreign` with no registry attached still passes its input through,
/// exactly as before this concept.
#[derive(Default, Clone)]
pub struct ForeignSourceRegistry {
    sources: HashMap<String, SharedForeignSource>,
}

impl ForeignSourceRegistry {
    /// A new, empty registry (no foreign sources bound). CONCEPT:EG-073.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register (or replace) a source under `name`. CONCEPT:EG-073.
    pub fn register(&mut self, name: impl Into<String>, source: SharedForeignSource) -> &mut Self {
        self.sources.insert(name.into(), source);
        self
    }

    /// Register an owned [`ForeignSourceSpec`] (remote-engine / HTTP-JSON / SQL) under a
    /// name — the kind that "resolves the name to another graph/dataset": a
    /// `RemoteEngine` spec pointed at another graph is exactly that, reached over the
    /// engine's own transport. CONCEPT:EG-073.
    pub fn register_spec(
        &mut self,
        name: impl Into<String>,
        spec: ForeignSourceSpec,
    ) -> &mut Self {
        self.register(name, Arc::new(SpecSource { spec }))
    }

    /// Register a FIXED table of rows (id + optional score) under a name — the
    /// zero-dependency source kind for tests + pre-materialized datasets.
    /// CONCEPT:EG-073.
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

    /// Register a CLOSURE that produces the rows on demand under a name. CONCEPT:EG-073.
    pub fn register_closure<F>(&mut self, name: impl Into<String>, f: F) -> &mut Self
    where
        F: Fn() -> Result<RowSet, String> + Send + Sync + 'static,
    {
        self.register(name, Arc::new(ClosureSource { f: Box::new(f) }))
    }

    /// Look a source up by name (borrowing the shared handle). CONCEPT:EG-073.
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
    /// `Named` `Op::ForeignScan` / an `Op::Foreign` marker. CONCEPT:EG-073.
    pub fn resolve(&self, name: &str) -> Result<RowSet, String> {
        match self.sources.get(name) {
            Some(src) => src.fetch(),
            None => {
                let mut known: Vec<&str> = self.sources.keys().map(String::as_str).collect();
                known.sort_unstable();
                Err(format!(
                    "federation: no foreign source registered under name '{name}' \
                     (registered: {known:?}) (CONCEPT:EG-073)"
                ))
            }
        }
    }
}

/// CONCEPT:EG-073 — a registerable source backed by an owned [`ForeignSourceSpec`]. It
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

/// CONCEPT:EG-073 — a registerable source backed by a FIXED table of rows (id + optional
/// score). The zero-dependency kind for tests and pre-materialized foreign datasets.
pub struct TableSource {
    rows: Vec<(String, Option<f32>)>,
}

impl ForeignSource for TableSource {
    fn fetch(&self) -> Result<RowSet, String> {
        Ok(RowSet::from_rows(self.rows.iter().cloned()))
    }
}

/// CONCEPT:EG-073 — a registerable source backed by a CLOSURE producing rows on demand
/// (e.g. an in-engine adapter that reads from another dataset the host holds).
pub struct ClosureSource {
    f: Box<dyn Fn() -> Result<RowSet, String> + Send + Sync>,
}

impl ForeignSource for ClosureSource {
    fn fetch(&self) -> Result<RowSet, String> {
        (self.f)()
    }
}

/// CONCEPT:EG-073 — the placeholder [`ForeignSource`] a `Named` spec resolves to when it
/// reaches [`source_for`] (which has no registry). It always errors, pointing the caller
/// at the `ForeignSourceRegistry`, so a misrouted `Named` fails loudly, never silently.
struct NamedUnresolved<'a> {
    name: &'a str,
}

impl ForeignSource for NamedUnresolved<'_> {
    fn fetch(&self) -> Result<RowSet, String> {
        Err(format!(
            "federation: the Named foreign source '{}' resolves through the \
             ForeignSourceRegistry on the PlanCtx, not source_for (CONCEPT:EG-073)",
            self.name
        ))
    }
}
