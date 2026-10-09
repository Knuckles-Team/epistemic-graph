//! Apache AGE federation through the existing guarded PostgreSQL reader.
//!
//! A `Cypher { backend: Age, .. }` wire spec is inert by itself. The host must
//! bind a read-only PostgreSQL credential in process, verify that it names the
//! same endpoint, and probe the declared graph before installing this source.
//! The credential never enters the wire spec or an error message. Every fetch
//! uses the SQL federation destination gate, read-only transaction, timeout,
//! and cardinality bound. AGE mutation clauses therefore cannot commit.

use std::collections::HashMap;
use std::sync::Arc;

use eg_types::wire::{ForeignCypherBackend, ForeignSourceSpec};

use crate::federation::{fetch_sql_columns, ForeignSource, ForeignSourceRegistry};
use crate::rowset::RowSet;

type Rows = Vec<HashMap<String, String>>;
type Reader = dyn Fn(&str, &str) -> Result<Rows, String> + Send + Sync;

const MAX_GRAPH_BYTES: usize = 128;
const MAX_QUERY_BYTES: usize = 1024 * 1024;
const MAX_FIELD_BYTES: usize = 1024;
const MAX_ID_BYTES: usize = 64 * 1024;

/// Bind a verified AGE source under a name. `dsn` is process-owned and must
/// identify precisely the endpoint declared in the credential-free wire spec.
/// The registration probe is an actual read through the guarded SQL transport.
pub fn register_age(
    registry: &mut ForeignSourceRegistry,
    name: impl Into<String>,
    spec: &ForeignSourceSpec,
    dsn: String,
) -> Result<(), String> {
    register_age_with_reader(registry, name, spec, dsn, Arc::new(fetch_sql_columns))
}

fn register_age_with_reader(
    registry: &mut ForeignSourceRegistry,
    name: impl Into<String>,
    spec: &ForeignSourceSpec,
    dsn: String,
    reader: Arc<Reader>,
) -> Result<(), String> {
    let source = AgeSource::new(spec, dsn, reader)?;
    source.probe()?;
    registry.register(name, Arc::new(source));
    Ok(())
}

struct AgeSource {
    dsn: String,
    graph: String,
    query: String,
    id_field: String,
    score_field: Option<String>,
    reader: Arc<Reader>,
}

impl AgeSource {
    fn new(spec: &ForeignSourceSpec, dsn: String, reader: Arc<Reader>) -> Result<Self, String> {
        let ForeignSourceSpec::Cypher {
            backend: ForeignCypherBackend::Age,
            endpoint,
            graph,
            query,
            id_field,
            score_field,
        } = spec
        else {
            return Err("federation: expected an AGE Cypher source".into());
        };
        same_postgres_endpoint(endpoint, &dsn)?;
        identifier(graph, MAX_GRAPH_BYTES, "AGE graph")?;
        identifier(id_field, MAX_FIELD_BYTES, "AGE id field")?;
        if let Some(score) = score_field {
            identifier(score, MAX_FIELD_BYTES, "AGE score field")?;
            if score == id_field {
                return Err("federation: AGE score and id fields must differ".into());
            }
        }
        if query.trim().is_empty() || query.len() > MAX_QUERY_BYTES || query.contains('\0') {
            return Err("federation: invalid AGE query".into());
        }
        Ok(Self {
            dsn,
            graph: graph.clone(),
            query: query.clone(),
            id_field: id_field.clone(),
            score_field: score_field.clone(),
            reader,
        })
    }

    fn probe(&self) -> Result<(), String> {
        let sql = format!(
            "SELECT name::text AS eg_name FROM ag_catalog.ag_graph WHERE name = '{}' LIMIT 1",
            self.graph
        );
        let rows = (self.reader)(&self.dsn, &sql)?;
        if rows.len() != 1 || rows[0].get("eg_name") != Some(&self.graph) {
            return Err("federation: AGE graph probe did not match registration".into());
        }
        Ok(())
    }

    fn query_sql(&self) -> String {
        // A dollar quote is a literal only when its closing tag cannot appear
        // inside the Cypher text. The graph/column names passed into SQL have
        // already passed the strict identifier check.
        let mut suffix = 0_u32;
        let delimiter = loop {
            let candidate = format!("$eg_age_{suffix}$");
            if !self.query.contains(&candidate) {
                break candidate;
            }
            suffix += 1;
        };
        let columns = match &self.score_field {
            Some(score) => format!("{} agtype, {score} agtype", self.id_field),
            None => format!("{} agtype", self.id_field),
        };
        let score = self.score_field.as_ref().map_or_else(String::new, |field| {
            format!(", eg_age.{field}::text AS eg_score")
        });
        format!(
            "SELECT eg_age.{}::text AS eg_id{score} FROM ag_catalog.cypher('{}', {delimiter}{}{delimiter}) AS eg_age({columns})",
            self.id_field, self.graph, self.query
        )
    }
}

impl ForeignSource for AgeSource {
    fn fetch(&self) -> Result<RowSet, String> {
        let rows = (self.reader)(&self.dsn, &self.query_sql())?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let raw_id = row
                .get("eg_id")
                .ok_or_else(|| "federation: AGE row lacks id".to_string())?;
            // AGE's `agtype::text` quotes string scalars. Numeric scalars are
            // unchanged. Decode JSON strings rather than trimming quotes so
            // embedded escapes round-trip correctly.
            let id = if raw_id.starts_with('"') {
                serde_json::from_str::<String>(raw_id)
                    .map_err(|_| "federation: AGE row id is invalid".to_string())?
            } else {
                raw_id.clone()
            };
            if id.is_empty() || id.len() > MAX_ID_BYTES {
                return Err("federation: AGE row id is invalid".into());
            }
            let score = row
                .get("eg_score")
                .map(|value| {
                    value
                        .parse::<f32>()
                        .ok()
                        .filter(|score| score.is_finite())
                        .ok_or_else(|| "federation: AGE row score is invalid".to_string())
                })
                .transpose()?;
            out.push((id, score));
        }
        Ok(RowSet::from_rows(out))
    }
}

fn identifier(value: &str, max_len: usize, name: &str) -> Result<(), String> {
    if value.is_empty()
        || value.len() > max_len
        || !value.as_bytes()[0].is_ascii_alphabetic() && value.as_bytes()[0] != b'_'
        || !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_')
    {
        return Err(format!("federation: invalid {name}"));
    }
    Ok(())
}

fn same_postgres_endpoint(endpoint: &str, dsn: &str) -> Result<(), String> {
    let expected =
        url::Url::parse(endpoint).map_err(|_| "federation: invalid AGE endpoint".to_string())?;
    let bound = url::Url::parse(dsn).map_err(|_| "federation: invalid AGE binding".to_string())?;
    let pg = |scheme: &str| matches!(scheme, "postgres" | "postgresql");
    if !pg(expected.scheme())
        || !pg(bound.scheme())
        || !expected.username().is_empty()
        || expected.password().is_some()
        || expected.query().is_some()
        || expected.fragment().is_some()
        || expected.host_str().is_none()
        || expected.host_str() != bound.host_str()
        || expected.port_or_known_default().unwrap_or(5432)
            != bound.port_or_known_default().unwrap_or(5432)
        || expected.path() != bound.path()
    {
        return Err("federation: AGE binding does not match declared endpoint".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec() -> ForeignSourceSpec {
        ForeignSourceSpec::Cypher {
            backend: ForeignCypherBackend::Age,
            endpoint: "postgres://db.example.invalid:5432/graph".into(),
            graph: "tenant_graph".into(),
            query: "MATCH (n) RETURN n.external_id AS id, n.rank AS score".into(),
            id_field: "id".into(),
            score_field: Some("score".into()),
        }
    }

    #[test]
    fn bound_age_registration_probes_and_resolves_rows() {
        let reader: Arc<Reader> = Arc::new(|_, sql| {
            if sql.contains("ag_catalog.ag_graph") {
                return Ok(vec![HashMap::from([(
                    "eg_name".into(),
                    "tenant_graph".into(),
                )])]);
            }
            assert!(sql.contains("ag_catalog.cypher('tenant_graph'"));
            assert!(sql.contains("AS eg_age(id agtype, score agtype)"));
            Ok(vec![HashMap::from([
                ("eg_id".into(), "\"node\\\"1\"".into()),
                ("eg_score".into(), "0.5".into()),
            ])])
        });
        let mut registry = ForeignSourceRegistry::new();
        register_age_with_reader(
            &mut registry,
            "age",
            &spec(),
            "postgres://service:private@db.example.invalid:5432/graph".into(),
            reader,
        )
        .unwrap();
        let rows = registry.resolve("age").unwrap();
        assert_eq!(rows.ids(), vec!["node\"1".to_string()]);
    }

    #[test]
    fn mismatched_binding_and_unbound_wire_are_refused() {
        let mut registry = ForeignSourceRegistry::new();
        let reader: Arc<Reader> = Arc::new(|_, _| panic!("must not contact backend"));
        let err = register_age_with_reader(
            &mut registry,
            "age",
            &spec(),
            "postgres://service:private@other.example.invalid/graph".into(),
            reader,
        )
        .unwrap_err();
        assert!(err.contains("does not match"));
        assert!(registry.is_empty());
        assert!(crate::federation::source_for(&spec()).fetch().is_err());
    }

    #[test]
    fn dollar_delimiter_cannot_be_closed_by_cypher_text() {
        let mut spec = spec();
        if let ForeignSourceSpec::Cypher { query, .. } = &mut spec {
            *query = "RETURN '$eg_age_0$' AS id, 1 AS score".into();
        }
        let source = AgeSource::new(
            &spec,
            "postgres://service:private@db.example.invalid/graph".into(),
            Arc::new(|_, _| Ok(vec![])),
        )
        .unwrap();
        assert!(source.query_sql().contains("$eg_age_1$"));
    }
}
