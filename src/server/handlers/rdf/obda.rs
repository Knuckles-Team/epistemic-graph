use crate::protocol::{Response, ResultPayload};

/// OBDA / R2RML virtual-graph query (CONCEPT:EG-KG.query.r2rml-virtual-graph /
/// CONCEPT:EG-KG.query.obda-query-rewrite). Registers each of `tables` as a foreign
/// [`eg_rdf::obda::ObdaSource`] backed by the engine's own SQL user-table store
/// ([`crate::server::sql_tables::user_table_store`]), parses `mapping` (auto-detecting
/// standard R2RML Turtle vs. the compact EG-101 textual form), and runs `query` through
/// the OBDA query-rewrite path: only the query-relevant columns are scanned from the
/// table(s), only the query-relevant triples are materialized into a TRANSIENT view —
/// the user table itself is never mutated, and nothing is persisted into any graph.
/// Read-only. Gated `obda` (implies `sparql` + `query`).
#[cfg(feature = "obda")]
pub(super) async fn handle_sparql_virtual(
    req_id: u64,
    authority: crate::server::access::CarrierAuthority,
    persist_dir: std::path::PathBuf,
    query: String,
    mapping: String,
    tables: Vec<String>,
    external_sources: Vec<crate::protocol::ObdaExternalSource>,
) -> Response {
    let out = tokio::task::spawn_blocking(move || {
        sparql_virtual(
            &authority,
            &persist_dir,
            &query,
            &mapping,
            &tables,
            &external_sources,
        )
    })
    .await;
    match out {
        Ok(Ok(result)) => Response::ok(
            req_id,
            ResultPayload::of_ref::<eg_types::result_contract::reasoning::SparqlVirtual>(&result),
        ),
        Ok(Err(msg)) => Response::err(req_id, format!("SparqlVirtual error: {msg}")),
        Err(e) => Response::err(req_id, format!("SparqlVirtual task join error: {e}")),
    }
}

/// A [`eg_rdf::obda::ObdaSource`] backed by one table of the caller's TENANT-shared
/// [`eg_query::TableStore`], opened through an authorized [`AuthorizedTable`]
/// (CONCEPT:NE-046 — EG-WIRE-CATALOG; CONCEPT:EG-KG.query.r2rml-virtual-graph). Bridges
/// the SQL-side typed [`eg_query::Cell`] to the OBDA seam's lexical `String` columns
/// (R2RML templates/literal object maps consume the lexical form; typed SQL
/// round-tripping through a `rr:datatype` object map is a documented follow-up,
/// mirrored from the EG-101 `ObdaSource` contract). Rows come from
/// `AuthorizedTable::select`, so the table's row-level predicate (if declared)
/// already constrained them before this layer ever sees them; THIS layer then still
/// applies BOTH the `needed`-column projection AND the pushed-down row-level
/// `filters` (CONCEPT:EG-KG.query.obda-predicate-pushdown) when handing rows back, so
/// the OBDA-side pushdown contract (only query-relevant rows/columns end up in a
/// materialized triple) holds even though the underlying scan itself is not
/// column/predicate-pushed.
#[cfg(feature = "obda")]
struct TableStoreSource {
    schema: eg_query::TableSchema,
    rows: Vec<Vec<eg_query::Cell>>,
}

#[cfg(feature = "obda")]
impl TableStoreSource {
    /// Open `table` through the ACL's authorized read path
    /// (CONCEPT:NE-046 — EG-WIRE-CATALOG): `Select`-authorizes `authority` for
    /// `table` (the SAME generic denial whether it doesn't exist or simply isn't
    /// granted — no existence leak through the OBDA surface) and returns its
    /// RLS-filtered rows.
    fn load(
        authority: &crate::server::access::CarrierAuthority,
        persist_dir: &std::path::Path,
        table: &str,
    ) -> Result<Self, String> {
        let authorized = crate::server::sql_catalog_acl::open_authorized_table(
            authority,
            persist_dir,
            table,
            crate::server::sql_catalog_acl::SqlPrivilege::Select,
        )?;
        let schema = authorized.schema()?;
        let rows = authorized.select(None)?;
        Ok(Self { schema, rows })
    }

    /// The lexical form of one cell (R2RML templates/literal object maps consume a
    /// string; `Null` yields no lexical value so a template/column referencing it
    /// correctly omits the triple, per the OBDA `expand_template`/`term_for` contract).
    fn lexical(cell: &eg_query::Cell) -> Option<String> {
        use eg_query::Cell;
        match cell {
            Cell::Null => None,
            Cell::Int(i) => Some(i.to_string()),
            Cell::Float(f) => Some(f.to_string()),
            Cell::Text(s) => Some(s.clone()),
            Cell::Bool(b) => Some(b.to_string()),
            Cell::Timestamp(t) => Some(t.to_string()),
            Cell::Bytes(b) => Some(hex_lexical(b)),
            Cell::Json(v) => Some(v.to_string()),
            Cell::Vector(v) => Some(format!("{v:?}")),
        }
    }
}

/// Hex-encode bytes for a lexical fallback (a `Bytes` cell has no natural R2RML lexical
/// form; hex is a stable, unambiguous rendering). No new dependency — a tiny hand-rolled
/// encoder, mirroring the idiom the rest of this crate uses to avoid pulling `hex` where
/// a two-line loop suffices.
#[cfg(feature = "obda")]
fn hex_lexical(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(out, "{b:02x}");
    }
    out
}

#[cfg(feature = "obda")]
impl eg_rdf::obda::ObdaSource for TableStoreSource {
    fn columns(&self) -> Vec<String> {
        self.schema
            .columns()
            .iter()
            .map(|c| c.name.clone())
            .collect()
    }

    fn scan(
        &self,
        needed: &std::collections::BTreeSet<String>,
        filters: &[eg_rdf::obda::ObdaFilter],
    ) -> Result<Vec<eg_rdf::obda::ForeignRow>, String> {
        let cols = self.schema.columns();
        let mut out = Vec::with_capacity(self.rows.len());
        for row in &self.rows {
            // Build the full lexical row first — a pushed FILTER may reference a column the
            // query does not PROJECT, so it must be visible for the predicate check.
            let full: eg_rdf::obda::ForeignRow = cols
                .iter()
                .zip(row.iter())
                .filter_map(|(col, cell)| Self::lexical(cell).map(|v| (col.name.clone(), v)))
                .collect();
            // Row-level predicate pushdown (CONCEPT:EG-KG.query.obda-predicate-pushdown): drop
            // any row the pushed-down FILTERs exclude BEFORE projecting — the internal-table
            // analogue of a SQL WHERE (closing the "pushdown stopped at column projection" gap).
            if !filters
                .iter()
                .all(|f| full.get(&f.column).is_some_and(|c| f.matches(c)))
            {
                continue;
            }
            // Projection pushdown: keep only the query-relevant columns.
            let projected: eg_rdf::obda::ForeignRow = if needed.is_empty() {
                full
            } else {
                full.into_iter()
                    .filter(|(k, _)| needed.contains(k))
                    .collect()
            };
            out.push(projected);
        }
        Ok(out)
    }
}

/// The blocking half of [`handle_sparql_virtual`]: build the [`eg_rdf::obda::ObdaSourceRegistry`]
/// from `tables`, parse `mapping`, run the OBDA query-rewrite, and project the SPARQL
/// result to the wire shape. Split out (not `async`) so it runs on the blocking pool —
/// the table scan + SPARQL evaluation are both synchronous CPU/redb-read work.
#[cfg(feature = "obda")]
fn sparql_virtual(
    authority: &crate::server::access::CarrierAuthority,
    persist_dir: &std::path::Path,
    query: &str,
    mapping: &str,
    tables: &[String],
    external_sources: &[crate::protocol::ObdaExternalSource],
) -> Result<crate::protocol::SparqlResult, String> {
    let mut reg = eg_rdf::obda::ObdaSourceRegistry::new();
    for table in tables {
        let src = TableStoreSource::load(authority, persist_dir, table)?;
        reg.register(table.clone(), std::sync::Arc::new(src));
    }
    // W4.11 — register each LIVE external relational source (CONCEPT:EG-KG.query.obda-predicate-pushdown).
    // The query's projection + row-level FILTERs are pushed into a real SELECT … WHERE ….
    for ext in external_sources {
        let src = SqlObdaSource::connect(&ext.dsn, &ext.table)?;
        reg.register(ext.name.clone(), std::sync::Arc::new(src));
    }

    // Auto-detect standard R2RML Turtle (carries the `rr:` namespace) vs. the compact
    // EG-101 textual mapping form (`SOURCE`/`SUBJECT`/… directives).
    let vg = if mapping.contains("r2rml#") || mapping.contains("TriplesMap") {
        eg_rdf::obda::parse_r2rml_turtle(mapping)?
    } else {
        eg_rdf::obda::parse_mapping(mapping)?
    };

    let proj = eg_rdf::sparql::Projection::raw();
    let outcome = eg_rdf::obda::run_outcome_virtual(&vg, &reg, query, &proj)?;
    let (vars, rows) = match outcome {
        eg_rdf::sparql::QueryOutcome::Solutions(res) => res.to_rows(),
        eg_rdf::sparql::QueryOutcome::Boolean(b) => {
            (vec!["_ask".to_string()], vec![vec![Some(b.to_string())]])
        }
        #[cfg(feature = "rdf")]
        eg_rdf::sparql::QueryOutcome::Graph(_) => (Vec::new(), Vec::new()),
    };
    // OBDA answers are rewritten scans over foreign tables, not graph triples, so
    // there is no graph witness to attach (EH-197 proofs are `Sparql`-only).
    Ok(crate::protocol::SparqlResult {
        vars,
        rows,
        proofs: Vec::new(),
    })
}

// ── external relational OBDA source + SPARQL→SQL predicate pushdown (W4.11) ──────────────────
//   CONCEPT:EG-KG.query.obda-predicate-pushdown — a live external Postgres/MySQL table exposed
//   as a virtual RDF graph, with the query's column projection AND row-level FILTERs pushed
//   down into a real `SELECT … WHERE …`. The SQL-generation (`render_obda_select`) is a PURE
//   function proven by unit tests ("assert the pushdown via the query plan"); execution goes
//   through the `ObdaSqlExecutor` seam so it is testable with a mock (no live DB) while the
//   `federation-sql` executor connects to the fleet's database.

/// The SQL dialect an external OBDA source speaks — selects identifier quoting + the
/// lexical-cast form so every SELECTed column comes back as text (the `ForeignRow` currency).
/// One owner for SQL text sent across a federation boundary: [`eg_plan::sql_text`].
#[cfg(feature = "obda")]
pub(super) type ObdaSqlDialect = eg_plan::sql_text::SqlDialect;

/// Infer the dialect from a DSN scheme (`postgres://…` / `mysql://…`).
#[cfg(feature = "obda")]
fn obda_dialect(dsn: &str) -> Result<ObdaSqlDialect, String> {
    ObdaSqlDialect::from_dsn(dsn).ok_or_else(|| {
        "obda: unsupported external SQL scheme (expected postgres:// or mysql://)".into()
    })
}

/// CONCEPT:EG-KG.query.obda-predicate-pushdown — executes a rendered READ-ONLY `SELECT` against
/// an external relational database and returns each row as column→lexical-string. The seam that
/// lets the SQL-generation + pushdown logic be unit-tested with a MOCK executor (no live DB)
/// while a real `federation-sql` executor connects to the fleet's Postgres/MySQL.
#[cfg(feature = "obda")]
pub(super) trait ObdaSqlExecutor: Send + Sync {
    fn run_select(&self, sql: &str) -> Result<Vec<eg_rdf::obda::ForeignRow>, String>;
}

/// An [`eg_rdf::obda::ObdaSource`] backed by a table in an EXTERNAL relational database
/// (CONCEPT:EG-KG.query.obda-predicate-pushdown). On `scan` it renders the projection- and
/// FILTER-pushed `SELECT … WHERE …` for its dialect and runs it through the [`ObdaSqlExecutor`].
#[cfg(feature = "obda")]
pub(super) struct SqlObdaSource {
    pub(super) table: String,
    pub(super) dialect: ObdaSqlDialect,
    pub(super) executor: std::sync::Arc<dyn ObdaSqlExecutor>,
}

#[cfg(feature = "obda")]
impl SqlObdaSource {
    /// Build a live external source over `table` reachable at `dsn` (the `federation-sql`
    /// path). A build without `federation-sql` returns a clean "rebuild" error.
    ///
    /// `EG-UNIFIED-DATA-PLANE-R002.2`: `dsn` runs through the same SSRF-sensitive
    /// destination gate the SQL federation producer path (`R002.1`,
    /// `src/server/foreign_catalog.rs`) already runs before registration, so an OBDA
    /// external source whose DSN names a disallowed destination is refused here —
    /// before `reg.register` in `sparql_virtual` ever reaches it — instead of only
    /// failing the first time `run_select` dials out.
    fn connect(dsn: &str, table: &str) -> Result<Self, String> {
        let dialect = obda_dialect(dsn)?;
        eg_plan::sql_text::validate_identifier(table).map_err(|e| format!("obda: {e}"))?;
        #[cfg(feature = "federation-sql")]
        eg_plan::federation_ssrf::check_sql_dsn(dsn)?;
        let executor = federation_sql_executor(dsn)?;
        Ok(Self {
            table: table.to_string(),
            dialect,
            executor,
        })
    }
}

#[cfg(feature = "obda")]
impl eg_rdf::obda::ObdaSource for SqlObdaSource {
    fn scan(
        &self,
        needed: &std::collections::BTreeSet<String>,
        filters: &[eg_rdf::obda::ObdaFilter],
    ) -> Result<Vec<eg_rdf::obda::ForeignRow>, String> {
        let sql = render_obda_select(&self.table, needed, filters, self.dialect)?;
        self.executor.run_select(&sql)
    }

    fn scan_ordered_distinct(
        &self,
        needed: &std::collections::BTreeSet<String>,
        filters: &[eg_rdf::obda::ObdaFilter],
        order: &eg_rdf::obda::ObdaOrder,
    ) -> Option<Result<Vec<eg_rdf::obda::ForeignRow>, String>> {
        if self.dialect != ObdaSqlDialect::Postgres {
            return None;
        }
        let sql = match render_obda_ordered_distinct(&self.table, needed, filters, order) {
            Ok(sql) => sql,
            Err(error) => return Some(Err(error)),
        };
        let rows = match self.executor.run_select(&sql) {
            Ok(rows) => rows,
            Err(error) => return Some(Err(error)),
        };
        if rows.len() == 1 && rows[0].get("eg_obda_status").map(String::as_str) == Some("invalid") {
            return None;
        }
        let mut out = Vec::with_capacity(rows.len());
        for mut row in rows {
            match row.remove("eg_obda_status").as_deref() {
                Some("ok") => out.push(row),
                _ => return Some(Err("obda: ordered DISTINCT status is invalid".into())),
            }
        }
        Some(Ok(out))
    }

    fn scan_distinct_count(
        &self,
        needed: &std::collections::BTreeSet<String>,
    ) -> Option<Result<u64, String>> {
        if self.dialect != ObdaSqlDialect::Postgres {
            return None;
        }
        let sql = match render_obda_distinct_count(&self.table, needed) {
            Ok(sql) => sql,
            Err(error) => return Some(Err(error)),
        };
        let rows = match self.executor.run_select(&sql) {
            Ok(rows) => rows,
            Err(error) => return Some(Err(error)),
        };
        if rows.len() != 1 {
            return Some(Err(
                "obda: DISTINCT count did not return exactly one row".into()
            ));
        }
        match rows[0].get("eg_obda_status").map(String::as_str) {
            Some("invalid") => None,
            Some("ok") => Some(
                rows[0]
                    .get("eg_obda_count")
                    .ok_or_else(|| "obda: DISTINCT count column missing".to_string())
                    .and_then(|value| {
                        value
                            .parse::<u64>()
                            .map_err(|_| "obda: DISTINCT count is invalid".to_string())
                    }),
            ),
            _ => Some(Err("obda: DISTINCT count status is invalid".into())),
        }
    }
}

/// A validity expression evaluated inside the SAME SQL statement/snapshot as the page
/// or count. A second executor call would race concurrent writes to the foreign table.
#[cfg(feature = "obda")]
fn render_obda_invalid_key_cte<'a>(
    table: &str,
    columns: impl IntoIterator<Item = &'a str>,
) -> Result<String, String> {
    let dialect = ObdaSqlDialect::Postgres;
    let predicates = columns
        .into_iter()
        .map(|column| {
            let q = quote(column, dialect)?;
            Ok(format!(
                "({q} IS NOT NULL AND {q}::text <> '' AND {q}::text !~ '^[A-Za-z0-9._~-]+$')"
            ))
        })
        .collect::<Result<Vec<_>, String>>()?;
    if predicates.is_empty() {
        return Err("obda: key preflight requires projected columns".into());
    }
    Ok(format!(
        "eg_bad AS (SELECT EXISTS (SELECT 1 FROM {} WHERE {}) AS bad)",
        quote(table, dialect)?,
        predicates.join(" OR ")
    ))
}

/// Lexical tuples and filters match the rows that the OBDA mapping can turn into
/// triples; C collation gives Postgres the binary ordering used by SPARQL strings.
#[cfg(feature = "obda")]
fn render_obda_lexical_distinct(
    table: &str,
    needed: &std::collections::BTreeSet<String>,
    filters: &[eg_rdf::obda::ObdaFilter],
) -> Result<String, String> {
    if needed.is_empty() {
        return Err("obda: DISTINCT requires projected columns".into());
    }
    if needed.contains("eg_obda_status") {
        return Err("obda: DISTINCT column collides with reserved status".into());
    }
    if matches!(table, "eg_bad" | "eg_page" | "eg_obda_rows") {
        return Err("obda: DISTINCT table collides with reserved SQL alias".into());
    }
    let dialect = ObdaSqlDialect::Postgres;
    let select_list = needed
        .iter()
        .map(|column| {
            let q = quote(column, dialect)?;
            Ok(format!("{q}::text COLLATE \"C\" AS {q}"))
        })
        .collect::<Result<Vec<_>, String>>()?
        .join(", ");
    let mut predicates = filters
        .iter()
        .map(|filter| render_obda_filter(filter, dialect))
        .collect::<Result<Vec<_>, _>>()?;
    for column in needed {
        let q = quote(column, dialect)?;
        predicates.push(format!("{q} IS NOT NULL AND {q}::text <> ''"));
    }
    Ok(format!(
        "SELECT DISTINCT {select_list} FROM {} WHERE {}",
        quote(table, dialect)?,
        predicates.join(" AND ")
    ))
}

#[cfg(feature = "obda")]
fn render_obda_ordered_distinct(
    table: &str,
    needed: &std::collections::BTreeSet<String>,
    filters: &[eg_rdf::obda::ObdaFilter],
    order: &eg_rdf::obda::ObdaOrder,
) -> Result<String, String> {
    if !needed.contains(&order.column) {
        return Err("obda: ordered DISTINCT requires a projected ordering column".into());
    }
    let start = i64::try_from(order.start)
        .map_err(|_| "obda: ordered DISTINCT offset exceeds SQL range".to_string())?;
    let length = i64::try_from(order.length)
        .map_err(|_| "obda: ordered DISTINCT limit exceeds SQL range".to_string())?;
    start
        .checked_add(length)
        .ok_or_else(|| "obda: ordered DISTINCT page exceeds SQL range".to_string())?;
    let dialect = ObdaSqlDialect::Postgres;
    let distinct = render_obda_lexical_distinct(table, needed, filters)?;
    let bad = render_obda_invalid_key_cte(table, std::iter::once(order.column.as_str()))?;
    let order_columns = std::iter::once(&order.column)
        .chain(needed.iter().filter(|column| *column != &order.column))
        .map(|column| {
            let q = quote(column, dialect)?;
            let direction = if column == &order.column && order.descending {
                "DESC"
            } else {
                "ASC"
            };
            Ok(format!("{q} {direction}"))
        })
        .collect::<Result<Vec<_>, String>>()?
        .join(", ");
    let projected_columns = needed
        .iter()
        .map(|column| quote(column, dialect))
        .collect::<Result<Vec<_>, _>>()?
        .join(", ");
    let null_columns = needed
        .iter()
        .map(|column| Ok(format!("NULL::text AS {}", quote(column, dialect)?)))
        .collect::<Result<Vec<_>, String>>()?
        .join(", ");
    Ok(format!(
        "WITH {bad}, eg_page AS ({distinct} ORDER BY {order_columns} LIMIT {length} OFFSET {start}) \
         SELECT 'ok'::text AS \"eg_obda_status\", {projected_columns} FROM eg_page \
         WHERE NOT (SELECT bad FROM eg_bad) \
         UNION ALL SELECT 'invalid'::text AS \"eg_obda_status\", {null_columns} \
         WHERE (SELECT bad FROM eg_bad) ORDER BY {order_columns}"
    ))
}

#[cfg(feature = "obda")]
fn render_obda_distinct_count(
    table: &str,
    needed: &std::collections::BTreeSet<String>,
) -> Result<String, String> {
    let distinct = render_obda_lexical_distinct(table, needed, &[])?;
    // Without an explicit subject-key parameter, guard every needed column. This
    // may choose the materialization fallback for valid non-IRI literal strings.
    let bad = render_obda_invalid_key_cte(table, needed.iter().map(String::as_str))?;
    Ok(format!(
        "WITH {bad} SELECT \
         CASE WHEN (SELECT bad FROM eg_bad) THEN 'invalid' ELSE 'ok' END::text AS \"eg_obda_status\", \
         CASE WHEN (SELECT bad FROM eg_bad) THEN NULL::text \
         ELSE (SELECT COUNT(*)::text FROM ({distinct}) AS eg_obda_rows) END AS \"eg_obda_count\""
    ))
}

/// CONCEPT:EG-KG.query.obda-predicate-pushdown — render the read-only `SELECT` a [`SqlObdaSource`]
/// issues: the `needed` columns each CAST to text (so the result is uniformly lexical), plus a
/// `WHERE` built from the pushed-down [`eg_rdf::obda::ObdaFilter`]s — the row-level pushdown.
/// Identifiers are validated + quoted and every literal is safely rendered, so the SQL carries
/// no injection surface (and `federation-sql` re-validates the whole statement before running).
/// This is a PURE function — the unit tests assert its `WHERE` clause ("the query plan").
#[cfg(feature = "obda")]
pub(super) fn render_obda_select(
    table: &str,
    needed: &std::collections::BTreeSet<String>,
    filters: &[eg_rdf::obda::ObdaFilter],
    dialect: ObdaSqlDialect,
) -> Result<String, String> {
    if needed.is_empty() {
        // A real OBDA mapping always needs ≥1 column (the subject template); an empty set
        // means a variable-predicate query with no mapped columns — not answerable over a
        // live external source without a full schema fetch. Fail clean rather than SELECT *.
        return Err(
            "obda: external SQL source needs a column-restricted query (no projectable columns)"
                .into(),
        );
    }
    let select_list = needed
        .iter()
        .map(|c| render_select_item(c, dialect))
        .collect::<Result<Vec<_>, _>>()?
        .join(", ");
    let mut sql = format!("SELECT {select_list} FROM {}", quote(table, dialect)?);
    if !filters.is_empty() {
        let where_clause = filters
            .iter()
            .map(|f| render_obda_filter(f, dialect))
            .collect::<Result<Vec<_>, _>>()?
            .join(" AND ");
        sql.push_str(" WHERE ");
        sql.push_str(&where_clause);
    }
    Ok(sql)
}

/// A SELECT item that casts the column to text and re-aliases it to its own name, so every
/// returned value is lexical and the executor maps it back by name.
#[cfg(feature = "obda")]
fn render_select_item(col: &str, dialect: ObdaSqlDialect) -> Result<String, String> {
    let q = quote(col, dialect)?;
    Ok(match dialect {
        ObdaSqlDialect::Postgres => format!("{q}::text AS {q}"),
        ObdaSqlDialect::MySql => format!("CAST({q} AS CHAR) AS {q}"),
    })
}

/// Render one pushed-down [`eg_rdf::obda::ObdaFilter`] as a SQL predicate over the NATIVE
/// (un-cast) column, so a numeric comparison runs against the numeric column type. An `In`
/// filter (a semi-join key batch) renders `col IN ('k1', …)`.
#[cfg(feature = "obda")]
fn render_obda_filter(
    f: &eg_rdf::obda::ObdaFilter,
    dialect: ObdaSqlDialect,
) -> Result<String, String> {
    use eg_rdf::obda::ObdaCompare;
    let col = quote(&f.column, dialect)?;
    let op = match f.op {
        ObdaCompare::Eq => "=",
        ObdaCompare::Lt => "<",
        ObdaCompare::Le => "<=",
        ObdaCompare::Gt => ">",
        ObdaCompare::Ge => ">=",
        ObdaCompare::In => return render_in_list(&col, &f.values, dialect),
    };
    let lit = literal(&f.value, f.numeric, dialect)?;
    Ok(format!("{col} {op} {lit}"))
}

/// `col IN ('v1', …)` for a non-empty member list (an empty one matches nothing).
#[cfg(feature = "obda")]
fn render_in_list(col: &str, values: &[String], dialect: ObdaSqlDialect) -> Result<String, String> {
    if values.is_empty() {
        return Ok("1 = 0".into());
    }
    let members = values
        .iter()
        .map(|v| literal(v, false, dialect))
        .collect::<Result<Vec<_>, _>>()?
        .join(", ");
    Ok(format!("{col} IN ({members})"))
}

/// Validate + quote an identifier through the shared renderer, with the OBDA error prefix.
#[cfg(feature = "obda")]
fn quote(ident: &str, dialect: ObdaSqlDialect) -> Result<String, String> {
    eg_plan::sql_text::quote_identifier(ident, dialect).map_err(|e| format!("obda: {e}"))
}

/// Render a literal through the shared renderer, with the OBDA error prefix.
#[cfg(feature = "obda")]
fn literal(value: &str, numeric: bool, dialect: ObdaSqlDialect) -> Result<String, String> {
    eg_plan::sql_text::render_literal(value, numeric, dialect).map_err(|e| format!("obda: {e}"))
}

/// Build the LIVE `federation-sql` executor for `dsn`, or a clean "rebuild with federation-sql"
/// error when the feature is off (the wire variant + rewrite still exist — only the driver is
/// absent, exactly like `eg_plan::federation::SqlSource`'s not-built placeholder).
#[cfg(all(feature = "obda", feature = "federation-sql"))]
fn federation_sql_executor(dsn: &str) -> Result<std::sync::Arc<dyn ObdaSqlExecutor>, String> {
    Ok(std::sync::Arc::new(FederationSqlExecutor {
        dsn: dsn.to_string(),
    }))
}

#[cfg(all(feature = "obda", not(feature = "federation-sql")))]
fn federation_sql_executor(_dsn: &str) -> Result<std::sync::Arc<dyn ObdaSqlExecutor>, String> {
    Err(
        "obda: a live external SQL source needs a server built with the `federation-sql` \
         feature (no SQL driver in this build)"
            .into(),
    )
}

/// The live executor: reuses `eg_plan::federation`'s SSRF-validated, read-only,
/// timeout+row-bounded column fetch (CONCEPT:EG-KG.query.query-federation) to run the rendered
/// SELECT against the external database.
#[cfg(all(feature = "obda", feature = "federation-sql"))]
struct FederationSqlExecutor {
    dsn: String,
}

#[cfg(all(feature = "obda", feature = "federation-sql"))]
impl ObdaSqlExecutor for FederationSqlExecutor {
    fn run_select(&self, sql: &str) -> Result<Vec<eg_rdf::obda::ForeignRow>, String> {
        eg_plan::federation::fetch_sql_columns(&self.dsn, sql)
    }
}

/// `EG-UNIFIED-DATA-PLANE-R002.2`: an OBDA external source's DSN runs through the same
/// SSRF-sensitive destination gate the SQL federation producer path (`R002.1`) runs at
/// registration time, not only when `run_select` later dials out.
#[cfg(all(test, feature = "obda", feature = "federation-sql"))]
mod outbound_verification_tests {
    use super::SqlObdaSource;

    #[test]
    fn a_disallowed_obda_destination_is_refused_before_registration() {
        // Same fixture `crates/eg-plan/src/federation_ssrf.rs` already proves is
        // refused: loopback on a non-default port, no allow-list entry.
        let err = SqlObdaSource::connect("postgres://u@127.0.0.1:5433/db", "people")
            .expect_err("a disallowed OBDA destination must be refused");
        assert!(err.contains("federation:"), "unexpected error: {err}");
    }

    #[test]
    fn an_allowed_obda_destination_still_connects() {
        // Same fixture `crates/eg-plan/src/federation_ssrf.rs` already proves is
        // admitted: loopback on the dialect's own default port. `connect` only
        // validates and stores the DSN here; it dials lazily inside `run_select`.
        SqlObdaSource::connect("postgres://u@127.0.0.1:5432/db", "people")
            .expect("an allowed OBDA destination must still connect");
    }
}
#[cfg(all(test, feature = "obda"))]
mod ordered_distinct_tests {
    use super::{
        render_obda_distinct_count, render_obda_invalid_key_cte, render_obda_ordered_distinct,
        ObdaSqlDialect, ObdaSqlExecutor, SqlObdaSource,
    };
    use eg_rdf::obda::{ObdaOrder, ObdaSource};
    use std::collections::BTreeSet;
    use std::sync::{Arc, Mutex};
    #[derive(Default)]
    struct RecordingExecutor {
        queries: Mutex<Vec<String>>,
        invalid_key: bool,
        empty_page: bool,
        malformed_status: bool,
    impl RecordingExecutor {
        fn response_rows(&self, sql: &str) -> Vec<eg_rdf::obda::ForeignRow> {
            if self.invalid_key {
                return vec![[("eg_obda_status".into(), "invalid".into())]
                    .into_iter()
                    .collect()];
            }
            if sql.contains("eg_obda_count") {
                return vec![[
                    ("eg_obda_status".into(), "ok".into()),
                    ("eg_obda_count".into(), "3".into()),
                ]
                .into_iter()
                .collect()];
            }
            if self.empty_page {
                return Vec::new();
            }
            let status = if self.malformed_status {
                "unknown"
            } else {
                "ok"
            };
            vec![[
                ("eg_obda_status".into(), status.into()),
                ("id".into(), "a".into()),
            ]
            .into_iter()
            .collect()]
        }
    impl ObdaSqlExecutor for RecordingExecutor {
        fn run_select(&self, sql: &str) -> Result<Vec<eg_rdf::obda::ForeignRow>, String> {
            self.queries.lock().unwrap().push(sql.to_string());
            Ok(self.response_rows(sql))
        }
    fn ordered_page_and_count_share_filtered_lexical_distinct() {
        let needed = BTreeSet::from(["id".into(), "name".into()]);
        let order = ObdaOrder {
            column: "id".into(),
            descending: false,
            start: 4,
            length: 7,
        };
        let page = render_obda_ordered_distinct("people", &needed, &[], &order).unwrap();
        let count = render_obda_distinct_count("people", &needed).unwrap();
        assert!(page.starts_with("WITH eg_bad AS ("), "{page}");
        assert!(page.contains("eg_page AS (SELECT DISTINCT"), "{page}");
        assert!(page.contains("\"id\"::text COLLATE \"C\" AS \"id\""));
        assert!(page.contains("\"name\" IS NOT NULL AND \"name\"::text <> ''"));
        assert!(page.contains("ORDER BY \"id\" ASC, \"name\" ASC LIMIT 7 OFFSET 4"));
        assert!(page.contains("UNION ALL SELECT 'invalid'::text"));
        assert!(page.ends_with("ORDER BY \"id\" ASC, \"name\" ASC"));
        assert!(count.contains("FROM (SELECT DISTINCT"), "{count}");
        assert!(count.contains("\"id\"::text COLLATE \"C\" AS \"id\""));
        assert!(count.contains("\"name\" IS NOT NULL AND \"name\"::text <> ''"));
    fn invalid_key_guard_is_in_same_statement_and_rejects_bad_identifiers() {
        let probe = render_obda_invalid_key_cte("people", ["id", "name"]).unwrap();
        assert!(
            probe.starts_with("eg_bad AS (SELECT EXISTS (SELECT 1"),
            "{probe}"
        );
        assert!(probe.contains("\"id\"::text !~ '^[A-Za-z0-9._~-]+$'"));
        assert!(probe.contains("\"name\"::text !~ '^[A-Za-z0-9._~-]+$'"));
        assert!(render_obda_invalid_key_cte("people; DROP TABLE users", ["id"]).is_err());
        assert!(render_obda_invalid_key_cte("people", ["id; DROP"]).is_err());
    fn postgres_statement_falls_back_on_invalid_key_and_mysql_declines() {
        let needed = BTreeSet::from(["id".into()]);
        let order = ObdaOrder {
            column: "id".into(),
            descending: false,
            start: 0,
            length: 1,
        };
        let invalid = Arc::new(RecordingExecutor {
            invalid_key: true,
            ..RecordingExecutor::default()
        });
        let mut source = SqlObdaSource {
            table: "people".into(),
            dialect: ObdaSqlDialect::Postgres,
            executor: invalid.clone(),
        };
        assert!(source.scan_ordered_distinct(&needed, &[], &order).is_none());
        assert!(source.scan_distinct_count(&needed).is_none());
        assert_eq!(invalid.queries.lock().unwrap().len(), 2);
        source.dialect = ObdaSqlDialect::MySql;
        assert!(source.scan_ordered_distinct(&needed, &[], &order).is_none());
        assert_eq!(invalid.queries.lock().unwrap().len(), 2);
        let valid = Arc::new(RecordingExecutor::default());
        source.dialect = ObdaSqlDialect::Postgres;
        source.executor = valid.clone();
        assert!(source
            .scan_ordered_distinct(&needed, &[], &order)
            .unwrap()
            .is_ok());
        assert_eq!(source.scan_distinct_count(&needed).unwrap().unwrap(), 3);
        assert_eq!(valid.queries.lock().unwrap().len(), 2);
        assert!(render_obda_ordered_distinct("eg_bad", &needed, &[], &order).is_err());
        assert!(render_obda_distinct_count("eg_page", &needed).is_err());
        let reserved = BTreeSet::from(["eg_obda_status".into()]);
        assert!(render_obda_distinct_count("people", &reserved).is_err());
    fn postgres_empty_page_and_malformed_status_are_distinguished() {
        let needed = BTreeSet::from(["id".into()]);
        let order = ObdaOrder {
            column: "id".into(),
            descending: false,
            start: 0,
            length: 1,
        };
        let empty = Arc::new(RecordingExecutor {
            empty_page: true,
            ..RecordingExecutor::default()
        });
        let mut source = SqlObdaSource {
            table: "people".into(),
            dialect: ObdaSqlDialect::Postgres,
            executor: empty.clone(),
        };
        assert!(source
            .scan_ordered_distinct(&needed, &[], &order)
            .unwrap()
            .unwrap()
            .is_empty());
        assert_eq!(empty.queries.lock().unwrap().len(), 1);
        source.executor = Arc::new(RecordingExecutor {
            malformed_status: true,
            ..RecordingExecutor::default()
        });
        assert!(source
            .scan_ordered_distinct(&needed, &[], &order)
            .unwrap()
            .is_err());
