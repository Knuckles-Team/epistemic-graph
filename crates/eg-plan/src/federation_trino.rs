//! EH-580: explicitly bound Trino v1 statement reader. A wire `Trino` spec by itself
//! remains unbound: the service identity is supplied only in process at registration.
//! See https://trino.io/docs/current/develop/client-protocol.html.

use std::io::Read;
use std::sync::Arc;
use std::time::Duration;

use eg_types::wire::ForeignSourceSpec;
use serde_json::Value;

use crate::federation::{ForeignSource, ForeignSourceRegistry};
use crate::federation_ssrf::validate_http_json_target;
use crate::rowset::RowSet;

const MAX_PAGE_BYTES: usize = 4 * 1024 * 1024;
const MAX_TOTAL_BYTES: usize = 64 * 1024 * 1024;
const MAX_ROWS: usize = 250_000;
const MAX_PAGES: usize = 2048;
const MAX_ID_BYTES: usize = 64 * 1024;
const MAX_FIELD_BYTES: usize = 1024;

fn validate_origin(endpoint: &str) -> Result<(), String> {
    // Exactly one origin; a URL path/query could change statement routing.
    let rest = endpoint
        .strip_prefix("https://")
        .ok_or_else(|| "federation: Trino bearer transport requires HTTPS".to_string())?;
    if rest.is_empty()
        || rest.bytes().any(|b| matches!(b, b'/' | b'?' | b'#' | b'@'))
        || endpoint.len() > 1024
    {
        return Err("federation: invalid Trino endpoint".into());
    }
    validate_http_json_target(endpoint)?;
    Ok(())
}

fn valid_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_FIELD_BYTES
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

fn valid_service_identity(value: &str) -> bool {
    !value.is_empty() && value.len() <= 4096 && value.bytes().all(|b| (0x21..=0x7e).contains(&b))
}

/// Register a Trino source only after a live, credentialed catalog probe.
/// The returned registry entry is fetch-only until the optimizer has a separate,
/// verified capability binding; it cannot claim the FO-15 target row merely by
/// being present in the wire spec. `bearer` is never placed in that spec or an error.
/// The Trino service principal must have read-only catalog grants.
pub fn register_trino(
    registry: &mut ForeignSourceRegistry,
    name: impl Into<String>,
    spec: &ForeignSourceSpec,
    user: String,
    bearer: String,
) -> Result<(), String> {
    let source = TrinoSource::new(spec, user, bearer)?;
    source.probe(&HttpTransport)?;
    registry.register(name, Arc::new(source));
    Ok(())
}

/// A verified in-process binding, with credentials intentionally absent from Debug.
struct TrinoSource {
    origin: String,
    catalog: String,
    schema: String,
    query: String,
    id_field: String,
    score_field: Option<String>,
    user: String,
    bearer: String,
}

impl TrinoSource {
    fn new(spec: &ForeignSourceSpec, user: String, bearer: String) -> Result<Self, String> {
        let ForeignSourceSpec::Trino {
            endpoint,
            catalog,
            schema,
            query,
            id_field,
            score_field,
        } = spec
        else {
            return Err("federation: expected a Trino source".into());
        };
        validate_origin(endpoint)?;
        if ![catalog, schema, id_field]
            .into_iter()
            .all(|value| valid_identifier(value))
        {
            return Err("federation: invalid Trino identifier".into());
        }
        if score_field.as_ref().is_some_and(|s| !valid_identifier(s)) {
            return Err("federation: invalid Trino score field".into());
        }
        if !valid_service_identity(&user) || !valid_service_identity(&bearer) {
            return Err("federation: invalid Trino service identity".into());
        }
        crate::federation::validate_federated_sql(query, crate::federation::SqlDialect::Postgres)?;
        Ok(Self {
            origin: endpoint.clone(),
            catalog: catalog.clone(),
            schema: schema.clone(),
            query: query.clone(),
            id_field: id_field.clone(),
            score_field: score_field.clone(),
            user,
            bearer,
        })
    }

    fn probe(&self, transport: &dyn Transport) -> Result<(), String> {
        let rows = self.execute("SELECT 1 AS eg_probe", "eg_probe", None, transport)?;
        if rows.ids() != vec!["1".to_string()] {
            return Err("federation: Trino catalog probe returned an unexpected result".into());
        }
        Ok(())
    }

    fn execute(
        &self,
        query: &str,
        id_field: &str,
        score_field: Option<&str>,
        transport: &dyn Transport,
    ) -> Result<RowSet, String> {
        let mut url = format!("{}/v1/statement", self.origin);
        let mut result = TrinoResult::default();
        for page in 0..MAX_PAGES {
            // A nextUri is untrusted response data. Every page must stay at the
            // exact origin and statement path, then be DNS-vetted and pinned anew.
            if page > 0 && !url.starts_with(&format!("{}/v1/statement/", self.origin)) {
                return Err("federation: Trino continuation changed destination".into());
            }
            let (json, bytes) = transport.request(self, &url, (page == 0).then_some(query))?;
            result.accept(&json, bytes, id_field, score_field)?;
            match json.get("nextUri").and_then(Value::as_str) {
                None => return Ok(RowSet::from_rows(result.rows)),
                Some(next) if next.len() <= 2048 => url = next.to_owned(),
                Some(_) => return Err("federation: invalid Trino continuation".into()),
            }
        }
        Err("federation: Trino result exceeds page limit".into())
    }
}

/// Page state belongs together: later pages may omit columns, but may never
/// redefine them or bypass aggregate byte and row limits.
#[derive(Default)]
struct TrinoResult {
    rows: Vec<(String, Option<f32>)>,
    columns: Option<Vec<String>>,
    total_bytes: usize,
}

impl TrinoResult {
    fn accept(
        &mut self,
        page: &Value,
        bytes: usize,
        id_field: &str,
        score_field: Option<&str>,
    ) -> Result<(), String> {
        self.total_bytes = self.total_bytes.saturating_add(bytes);
        if self.total_bytes > MAX_TOTAL_BYTES {
            return Err("federation: Trino result exceeds byte limit".into());
        }
        if page.get("error").is_some_and(|v| !v.is_null()) {
            return Err("federation: Trino query failed".into());
        }
        if let Some(columns) = page.get("columns") {
            self.accept_columns(columns)?;
        }
        if let Some(data) = page.get("data") {
            self.accept_data(data, id_field, score_field)?;
        }
        Ok(())
    }

    fn accept_columns(&mut self, columns: &Value) -> Result<(), String> {
        let found = columns
            .as_array()
            .ok_or("federation: invalid Trino columns")?
            .iter()
            .map(|v| v.get("name").and_then(Value::as_str).map(str::to_string))
            .collect::<Option<Vec<_>>>()
            .ok_or("federation: invalid Trino columns")?;
        if let Some(prior) = &self.columns {
            if prior != &found {
                return Err("federation: Trino columns changed between pages".into());
            }
        } else {
            self.columns = Some(found);
        }
        Ok(())
    }

    fn accept_data(
        &mut self,
        data: &Value,
        id_field: &str,
        score_field: Option<&str>,
    ) -> Result<(), String> {
        let data = data.as_array().ok_or("federation: invalid Trino data")?;
        let columns = self
            .columns
            .as_ref()
            .ok_or("federation: Trino data has no columns")?;
        let id_index = columns
            .iter()
            .position(|column| column == id_field)
            .ok_or("federation: Trino result lacks id column")?;
        let score_index = score_field
            .map(|field| {
                columns
                    .iter()
                    .position(|column| column == field)
                    .ok_or("federation: Trino result lacks score column")
            })
            .transpose()?;
        if self.rows.len().saturating_add(data.len()) > MAX_ROWS {
            return Err("federation: Trino result exceeds row limit".into());
        }
        for item in data {
            self.rows
                .push(Self::parse_row(item, columns.len(), id_index, score_index)?);
        }
        Ok(())
    }

    fn parse_row(
        item: &Value,
        column_count: usize,
        id_index: usize,
        score_index: Option<usize>,
    ) -> Result<(String, Option<f32>), String> {
        let cells = item.as_array().ok_or("federation: invalid Trino row")?;
        if cells.len() != column_count {
            return Err("federation: invalid Trino row width".into());
        }
        let value = cells.get(id_index).ok_or("federation: missing Trino id")?;
        let id = match value {
            Value::String(s) => s.clone(),
            Value::Number(n) => n.to_string(),
            _ => return Err("federation: unsupported Trino id type".into()),
        };
        if id.is_empty() || id.len() > MAX_ID_BYTES {
            return Err("federation: invalid Trino id".into());
        }
        let score = score_index
            .and_then(|index| cells.get(index))
            .and_then(Value::as_f64)
            .map(|value| value as f32);
        if score.is_some_and(|value| !value.is_finite()) {
            return Err("federation: invalid Trino score".into());
        }
        Ok((id, score))
    }
}

impl ForeignSource for TrinoSource {
    fn fetch(&self) -> Result<RowSet, String> {
        self.execute(
            &self.query,
            &self.id_field,
            self.score_field.as_deref(),
            &HttpTransport,
        )
    }
}

trait Transport {
    fn request(
        &self,
        source: &TrinoSource,
        url: &str,
        statement: Option<&str>,
    ) -> Result<(Value, usize), String>;
}

struct HttpTransport;
impl HttpTransport {
    fn agent(url: &str) -> Result<ureq::Agent, String> {
        let target = validate_http_json_target(url)?;
        let pinned = target.addresses;
        Ok(ureq::AgentBuilder::new()
            .try_proxy_from_env(false)
            .resolver(
                move |_: &str| -> std::io::Result<Vec<std::net::SocketAddr>> { Ok(pinned.clone()) },
            )
            .https_only(target.https_only)
            .redirects(0)
            .timeout_connect(Duration::from_secs(10))
            .timeout_read(Duration::from_secs(30))
            .timeout_write(Duration::from_secs(30))
            .timeout(Duration::from_secs(60))
            .build())
    }

    fn read_page(response: ureq::Response) -> Result<(Value, usize), String> {
        if response.status() != 200 {
            return Err("federation: Trino request failed".into());
        }
        if response
            .header("Content-Length")
            .and_then(|v| v.parse::<usize>().ok())
            .is_some_and(|n| n > MAX_PAGE_BYTES)
        {
            return Err("federation: Trino page exceeds byte limit".into());
        }
        let mut body = Vec::new();
        response
            .into_reader()
            .take((MAX_PAGE_BYTES + 1) as u64)
            .read_to_end(&mut body)
            .map_err(|_| "federation: Trino response read failed".to_string())?;
        if body.len() > MAX_PAGE_BYTES {
            return Err("federation: Trino page exceeds byte limit".into());
        }
        let json = serde_json::from_slice(&body)
            .map_err(|_| "federation: invalid Trino response".to_string())?;
        Ok((json, body.len()))
    }
}

impl Transport for HttpTransport {
    fn request(
        &self,
        source: &TrinoSource,
        url: &str,
        statement: Option<&str>,
    ) -> Result<(Value, usize), String> {
        let agent = Self::agent(url)?;
        let request = if statement.is_some() {
            agent.post(url)
        } else {
            agent.get(url)
        }
        .set("Authorization", &format!("Bearer {}", source.bearer))
        .set("X-Trino-User", &source.user)
        .set("X-Trino-Catalog", &source.catalog)
        .set("X-Trino-Schema", &source.schema)
        .set("X-Trino-Source", "epistemic-graph");
        let response = match statement {
            Some(sql) => request.send_string(sql),
            None => request.call(),
        }
        .map_err(|_| "federation: Trino request failed".to_string())?;
        Self::read_page(response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    struct Mock(Mutex<Vec<Value>>);
    impl Transport for Mock {
        fn request(
            &self,
            _source: &TrinoSource,
            _url: &str,
            _statement: Option<&str>,
        ) -> Result<(Value, usize), String> {
            let mut pages = self.0.lock().unwrap();
            if pages.is_empty() {
                return Err("missing mock page".into());
            }
            Ok((pages.remove(0), 100))
        }
    }
    fn source() -> TrinoSource {
        TrinoSource {
            origin: "https://trino.invalid".into(),
            catalog: "lake".into(),
            schema: "public".into(),
            query: "SELECT id FROM objects".into(),
            id_field: "id".into(),
            score_field: None,
            user: "eg-reader".into(),
            bearer: "private".into(),
        }
    }
    fn mock(pages: Vec<Value>) -> Mock {
        Mock(Mutex::new(pages))
    }

    #[test]
    fn statement_pages_stream_into_bounded_rowset() {
        let src = source();
        let pages = mock(vec![
            serde_json::json!({"columns":[{"name":"id"}],"data":[["a"]],"nextUri":"https://trino.invalid/v1/statement/q/1"}),
            serde_json::json!({"data":[["b"]]}),
        ]);
        assert_eq!(
            src.execute(&src.query, "id", None, &pages).unwrap().ids(),
            vec!["a".to_string(), "b".to_string()]
        );
    }
    #[test]
    fn continuation_cannot_redirect_or_return_partial_data() {
        let src = source();
        let pages = mock(vec![
            serde_json::json!({"columns":[{"name":"id"}],"data":[["a"]],"nextUri":"https://evil.invalid/v1/statement/q/1"}),
        ]);
        assert!(src
            .execute(&src.query, "id", None, &pages)
            .unwrap_err()
            .contains("changed destination"));
        let pages = mock(vec![
            serde_json::json!({"error":{"message":"secret"},"data":[["a"]]}),
        ]);
        let err = src.execute(&src.query, "id", None, &pages).unwrap_err();
        assert!(!err.contains("secret"));
    }
    #[test]
    fn shape_and_probe_fail_closed() {
        let src = source();
        let pages = mock(vec![
            serde_json::json!({"columns":[{"name":"eg_probe"}],"data":[[2]]}),
        ]);
        assert!(src.probe(&pages).is_err());
        let pages = mock(vec![
            serde_json::json!({"columns":[{"name":"id"}],"data":[[null]]}),
        ]);
        assert!(src.execute(&src.query, "id", None, &pages).is_err());
    }

    #[test]
    fn bearer_cannot_cross_plaintext_transport() {
        let spec = ForeignSourceSpec::Trino {
            endpoint: "http://trino.invalid".into(),
            catalog: "lake".into(),
            schema: "public".into(),
            query: "SELECT id FROM objects".into(),
            id_field: "id".into(),
            score_field: None,
        };
        let err = TrinoSource::new(&spec, "eg-reader".into(), "private".into())
            .err()
            .unwrap();
        assert!(err.contains("requires HTTPS"));
        assert!(!err.contains("private"));
    }
}
