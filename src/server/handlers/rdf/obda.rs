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
    Ok(crate::protocol::SparqlResult { vars, rows })
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
#[cfg(feature = "obda")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ObdaSqlDialect {
    Postgres,
    MySql,
}

#[cfg(feature = "obda")]
impl ObdaSqlDialect {
    /// Infer the dialect from a DSN scheme (`postgres://…` / `mysql://…`).
    fn from_dsn(dsn: &str) -> Result<Self, String> {
        match dsn.split(':').next().unwrap_or("") {
            "postgres" | "postgresql" => Ok(Self::Postgres),
            "mysql" | "mariadb" => Ok(Self::MySql),
            _ => Err(
                "obda: unsupported external SQL scheme (expected postgres:// or mysql://)".into(),
            ),
        }
    }
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
    fn connect(dsn: &str, table: &str) -> Result<Self, String> {
        let dialect = ObdaSqlDialect::from_dsn(dsn)?;
        validate_sql_identifier(table)?;
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
    let mut sql = format!(
        "SELECT {select_list} FROM {}",
        quote_sql_identifier(table, dialect)?
    );
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
    let q = quote_sql_identifier(col, dialect)?;
    Ok(match dialect {
        ObdaSqlDialect::Postgres => format!("{q}::text AS {q}"),
        ObdaSqlDialect::MySql => format!("CAST({q} AS CHAR) AS {q}"),
    })
}

/// Render one pushed-down [`eg_rdf::obda::ObdaFilter`] as a SQL predicate over the NATIVE
/// (un-cast) column, so a numeric comparison runs against the numeric column type.
#[cfg(feature = "obda")]
fn render_obda_filter(
    f: &eg_rdf::obda::ObdaFilter,
    dialect: ObdaSqlDialect,
) -> Result<String, String> {
    use eg_rdf::obda::ObdaCompare;
    let col = quote_sql_identifier(&f.column, dialect)?;
    let op = match f.op {
        ObdaCompare::Eq => "=",
        ObdaCompare::Lt => "<",
        ObdaCompare::Le => "<=",
        ObdaCompare::Gt => ">",
        ObdaCompare::Ge => ">=",
    };
    let lit = render_sql_literal(&f.value, f.numeric, dialect)?;
    Ok(format!("{col} {op} {lit}"))
}

/// Validate a SQL identifier: `[A-Za-z_][A-Za-z0-9_]*`, ≤63 chars. Rejecting anything else is
/// what makes quoting injection-safe.
#[cfg(feature = "obda")]
fn validate_sql_identifier(ident: &str) -> Result<(), String> {
    let ok = !ident.is_empty()
        && ident.len() <= 63
        && ident
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && ident.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
    if ok {
        Ok(())
    } else {
        Err(format!("obda: invalid SQL identifier {ident:?}"))
    }
}

/// Validate then quote a SQL identifier for the dialect.
#[cfg(feature = "obda")]
fn quote_sql_identifier(ident: &str, dialect: ObdaSqlDialect) -> Result<String, String> {
    validate_sql_identifier(ident)?;
    Ok(match dialect {
        ObdaSqlDialect::Postgres => format!("\"{ident}\""),
        ObdaSqlDialect::MySql => format!("`{ident}`"),
    })
}

/// Render a filter's comparison literal: a validated numeric token unquoted, or a safely
/// escaped single-quoted string. Rejects a NUL byte and a non-finite numeric outright.
#[cfg(feature = "obda")]
fn render_sql_literal(
    value: &str,
    numeric: bool,
    dialect: ObdaSqlDialect,
) -> Result<String, String> {
    if value.contains('\0') {
        return Err("obda: filter literal contains a NUL byte".into());
    }
    if numeric {
        let n: f64 = value
            .parse()
            .map_err(|_| format!("obda: non-numeric literal {value:?} for a numeric filter"))?;
        if !n.is_finite() {
            return Err(format!("obda: non-finite numeric literal {value:?}"));
        }
        if !value
            .chars()
            .all(|c| c.is_ascii_digit() || matches!(c, '-' | '+' | '.' | 'e' | 'E'))
        {
            return Err(format!("obda: unsafe numeric literal {value:?}"));
        }
        return Ok(value.to_string());
    }
    // String literal: double every single quote; MySQL also treats backslash as an escape
    // char (unlike standard-conforming Postgres), so double backslashes there too.
    let escaped = match dialect {
        ObdaSqlDialect::Postgres => value.replace('\'', "''"),
        ObdaSqlDialect::MySql => value.replace('\\', "\\\\").replace('\'', "''"),
    };
    Ok(format!("'{escaped}'"))
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
