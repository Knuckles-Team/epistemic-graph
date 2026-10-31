//! The server-side chunk embedder (MEDULLA-UNIFIED-DATA-PLANE-R651.7).
//!
//! Configured from the environment:
//! - `MEDULLA_EMBEDDING_URL`: an OpenAI-compatible base URL
//!   (`http://host:8000`, `http://host:8000/v1`) or the full
//!   `/v1/embeddings` endpoint;
//! - `MEDULLA_EMBEDDING_MODEL`: the model name sent in each request;
//! - `MEDULLA_EMBEDDING_DIMS`: the vector width (default 384); a
//!   response of another width is refused;
//! - `MEDULLA_EMBEDDING_API_KEY`: an optional bearer token, never
//!   logged or echoed.
//!
//! With no URL the deterministic `medulla_plan::HashEmbedder` fills in at the same
//! width and the provenance says `embedder = hash`. The same embedder backs
//! the UQL `RANK BY ~ "text"` seam, so stored chunk vectors and query vectors
//! share one space.

/// The default vector width when none is configured.
pub(crate) const DEFAULT_DIMS: usize = 384;
/// Texts per remote request.
const BATCH: usize = 64;
#[cfg(feature = "federation-search")]
const TIMEOUT_S: u64 = 60;
/// The largest embeddings response read from the endpoint.
#[cfg(feature = "federation-search")]
const MAX_RESPONSE_BYTES: u64 = 256 * 1024 * 1024;

/// The embedder named in provenance.
pub(crate) const HASH: &str = "hash";
pub(crate) const REMOTE: &str = "openai-compatible";

#[derive(Clone)]
pub(crate) struct EmbedderConfig {
    endpoint: Option<String>,
    pub(crate) model: Option<String>,
    pub(crate) dims: usize,
    api_key: Option<String>,
}

impl std::fmt::Debug for EmbedderConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EmbedderConfig")
            .field("endpoint", &self.endpoint)
            .field("model", &self.model)
            .field("dims", &self.dims)
            .field("api_key", &self.api_key.as_ref().map(|_| "<redacted>"))
            .finish()
    }
}

/// The `/v1/embeddings` endpoint for a configured base URL.
fn endpoint(base: &str) -> String {
    let base = base.trim().trim_end_matches('/');
    if base.ends_with("/embeddings") {
        base.to_string()
    } else if base.ends_with("/v1") {
        format!("{base}/embeddings")
    } else {
        format!("{base}/v1/embeddings")
    }
}

fn non_empty(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

impl EmbedderConfig {
    pub(crate) fn from_env() -> Self {
        Self {
            endpoint: non_empty("MEDULLA_EMBEDDING_URL").map(|url| endpoint(&url)),
            model: non_empty("MEDULLA_EMBEDDING_MODEL"),
            dims: non_empty("MEDULLA_EMBEDDING_DIMS")
                .and_then(|dims| dims.parse::<usize>().ok())
                .filter(|dims| (1..=16_384).contains(dims))
                .unwrap_or(DEFAULT_DIMS),
            api_key: non_empty("MEDULLA_EMBEDDING_API_KEY"),
        }
    }

    /// The provenance name of the embedder this config selects.
    pub(crate) fn name(&self) -> &'static str {
        if self.endpoint.is_some() {
            REMOTE
        } else {
            HASH
        }
    }

    /// Embed `texts` in order. Blocking: call it off the reactor.
    pub(crate) fn embed(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, String> {
        match &self.endpoint {
            Some(url) => texts
                .chunks(BATCH)
                .map(|batch| self.remote(url, batch))
                .collect::<Result<Vec<_>, _>>()
                .map(|batches| batches.into_iter().flatten().collect()),
            None => {
                use medulla_plan::TextEmbedder;
                let hash = medulla_plan::HashEmbedder::new(self.dims);
                texts.iter().map(|text| hash.embed(text)).collect()
            }
        }
    }

    #[cfg(feature = "federation-search")]
    fn remote(&self, url: &str, batch: &[&str]) -> Result<Vec<Vec<f32>>, String> {
        let mut body = serde_json::json!({ "input": batch });
        if let Some(model) = &self.model {
            body["model"] = serde_json::Value::String(model.clone());
        }
        let mut request = crate::server::outbound_http_config()
            .build()
            .new_agent()
            .post(url)
            .config()
            .timeout_global(Some(std::time::Duration::from_secs(TIMEOUT_S)))
            .build()
            .header("content-type", "application/json");
        if let Some(key) = &self.api_key {
            request = request.header("authorization", &format!("Bearer {key}"));
        }
        let mut text = String::new();
        std::io::Read::read_to_string(
            &mut std::io::Read::take(
                request
                    .send(&body.to_string())
                    .map_err(|error| {
                        format!("EMBEDDING_UNAVAILABLE: {}", redact(&error.to_string()))
                    })?
                    .into_body()
                    .into_reader(),
                MAX_RESPONSE_BYTES,
            ),
            &mut text,
        )
        .map_err(|error| format!("EMBEDDING_UNAVAILABLE: response read failed: {error}"))?;
        let response: serde_json::Value = serde_json::from_str(&text)
            .map_err(|error| format!("EMBEDDING_UNAVAILABLE: invalid response body: {error}"))?;
        parse_response(&response, batch.len(), self.dims)
    }

    #[cfg(not(feature = "federation-search"))]
    fn remote(&self, _url: &str, _batch: &[&str]) -> Result<Vec<Vec<f32>>, String> {
        Err("EMBEDDING_UNAVAILABLE: this build has no outbound HTTP client".into())
    }
}

impl medulla_plan::TextEmbedder for EmbedderConfig {
    fn embed(&self, text: &str) -> Result<Vec<f32>, String> {
        let mut vectors = EmbedderConfig::embed(self, &[text])?;
        vectors
            .pop()
            .ok_or_else(|| "EMBEDDING_UNAVAILABLE: no vector returned".into())
    }
}

/// Drop anything after a `?` so a query-string credential never reaches an error.
#[cfg(feature = "federation-search")]
fn redact(message: &str) -> String {
    message.split('?').next().unwrap_or_default().to_string()
}

/// Read an OpenAI-shaped `{"data": [{"index", "embedding"}]}` body, in input order.
pub(crate) fn parse_response(
    body: &serde_json::Value,
    expected: usize,
    dims: usize,
) -> Result<Vec<Vec<f32>>, String> {
    let data = body["data"]
        .as_array()
        .ok_or("EMBEDDING_UNAVAILABLE: response has no data array")?;
    if data.len() != expected {
        return Err(format!(
            "EMBEDDING_UNAVAILABLE: asked for {expected} vectors, got {}",
            data.len()
        ));
    }
    let mut out = vec![Vec::new(); expected];
    for (position, item) in data.iter().enumerate() {
        let index = item["index"].as_u64().map_or(position, |i| i as usize);
        let vector: Vec<f32> = item["embedding"]
            .as_array()
            .ok_or("EMBEDDING_UNAVAILABLE: an item has no embedding")?
            .iter()
            .map(|x| x.as_f64().map(|x| x as f32))
            .collect::<Option<_>>()
            .ok_or("EMBEDDING_UNAVAILABLE: a non-numeric embedding value")?;
        if vector.len() != dims {
            return Err(format!(
                "EMBEDDING_DIMENSION_MISMATCH: configured {dims}, model returned {}",
                vector.len()
            ));
        }
        let slot = out
            .get_mut(index)
            .ok_or("EMBEDDING_UNAVAILABLE: an item index is out of range")?;
        *slot = vector;
    }
    if out.iter().any(Vec::is_empty) {
        return Err("EMBEDDING_UNAVAILABLE: a vector is missing".into());
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    // spec: MEDULLA-UNIFIED-DATA-PLANE-R651.7.2
    #[test]
    fn endpoint_forms_and_response_order() {
        assert_eq!(endpoint("http://h:8000"), "http://h:8000/v1/embeddings");
        assert_eq!(endpoint("http://h:8000/v1/"), "http://h:8000/v1/embeddings");
        assert_eq!(endpoint("http://h/v1/embeddings"), "http://h/v1/embeddings");
        let body = serde_json::json!({"data": [
            {"index": 1, "embedding": [0.0, 1.0]},
            {"index": 0, "embedding": [1.0, 0.0]}
        ]});
        assert_eq!(
            parse_response(&body, 2, 2).unwrap(),
            vec![vec![1.0, 0.0], vec![0.0, 1.0]]
        );
        assert!(parse_response(&body, 2, 3)
            .unwrap_err()
            .contains("DIMENSION_MISMATCH"));
        assert!(parse_response(&body, 3, 2).is_err());
    }

    #[test]
    fn hash_fallback_is_deterministic_at_the_configured_width() {
        let config = EmbedderConfig {
            endpoint: None,
            model: None,
            dims: 16,
            api_key: Some("secret".into()),
        };
        assert_eq!(config.name(), HASH);
        let first = config.embed(&["a", "b"]).unwrap();
        assert_eq!(first, config.embed(&["a", "b"]).unwrap());
        assert_eq!(first[0].len(), 16);
        assert!(!format!("{config:?}").contains("secret"));
    }
}
