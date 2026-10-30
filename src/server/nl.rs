//! Natural-language query planner resolution for the facade (CONCEPT:EG-KG.query.fence-stripper).
//!
//! The engine's NL surface is LLM-OPTIONAL and pure-Rust in the core: the
//! [`eg_plan::NlPlanner`] seam (CONCEPT:EG-KG.query.core-query-input) turns NL into a UQL query STRING that then
//! runs through the existing deterministic UQL pipeline. This module owns
//! WHICH planner the facade uses:
//!
//!  * **Injected** — when Arbora (or any embedder) drives the engine it does
//!    NL→query on its own side and can OPT OUT of the engine's built-in planner, or it
//!    can inject its own via [`set_nl_planner`]. An injected planner always wins.
//!  * **Standalone (config)** — a bare engine reads an OpenAI-compatible LLM
//!    endpoint/model/api-key-env from Arbora's `config.json` and builds a
//!    [`eg_plan::UreqNlPlanner`]. If no endpoint is configured the planner stays `None`
//!    and `Method::NlQuery` returns a clear "not configured" error (never a panic).
//!
//! The whole module is gated behind `nl-query`.

use std::sync::{Arc, OnceLock};

use eg_plan::{NlConfigError, NlPlanner, OAuth2ClientCredentials, TokenAuthStyle, UreqNlPlanner};

/// An operator-injected planner (CONCEPT:EG-KG.query.fence-stripper). Set ONCE, before first use, by an
/// embedder that wants the engine to drive its own NL→query. Wins over the config default.
static INJECTED: OnceLock<Arc<dyn NlPlanner>> = OnceLock::new();
/// The lazily-built, config-derived default planner (`None` when nothing is configured).
/// Built at most once, on the first `NlQuery` that finds no injected planner.
/// An invalid request config is kept as a typed error so `NlQuery` can refuse with it.
type ConfigDefault = Result<Option<Arc<dyn NlPlanner>>, NlConfigError>;
static CONFIG_DEFAULT: OnceLock<ConfigDefault> = OnceLock::new();

/// Inject the NL planner the facade should use (CONCEPT:EG-KG.query.fence-stripper). An embedder calls this
/// at startup to opt into the engine driving NL→query with its own planner. Idempotent:
/// a second call after the planner is set (or after the config default was already
/// resolved) is ignored, so the wiring is stable for the process lifetime.
pub fn set_nl_planner(planner: Arc<dyn NlPlanner>) {
    let _ = INJECTED.set(planner);
}

/// Resolve the active planner: the injected one if present, else the lazily-built config
/// default (`Ok(None)` when standalone config names no LLM endpoint). Invalid request
/// config (EG-FEDERATED-QUERY-R058.5) is returned as the typed [`NlConfigError`].
pub fn resolve_planner() -> Result<Option<Arc<dyn NlPlanner>>, NlConfigError> {
    if let Some(p) = INJECTED.get() {
        return Ok(Some(p.clone()));
    }
    CONFIG_DEFAULT
        .get_or_init(build_default_from_config)
        .clone()
}

/// OAuth2 client-credentials settings resolved from config (the client secret is carried
/// as the NAME of an env var, resolved to its value at planner-build time — never a
/// plaintext secret in config).
struct OAuth2Settings {
    token_url: String,
    client_id: String,
    /// Name of the env var holding the client secret.
    client_secret_env: String,
    scope: Option<String>,
    /// "basic" ⇒ HTTP Basic at the token endpoint; anything else ⇒ body params.
    auth_style: TokenAuthStyle,
}

/// LLM settings resolved from Arbora's `config.json` (+ env overrides).
struct NlSettings {
    endpoint: String,
    model: String,
    /// Name of the env var carrying the bearer key (resolved to the key at build time).
    api_key_env: Option<String>,
    /// Static headers sent on every request (e.g. a gateway client-id header).
    headers: Vec<(String, String)>,
    /// Per-endpoint PEM CA bundle path. Verification is always enabled.
    tls_ca_path: Option<String>,
    /// OAuth2 client-credentials token source (mints a bearer instead of a static key).
    oauth2: Option<OAuth2Settings>,
    /// Read timeout in seconds (`None` ⇒ the planner default, 30s).
    read_timeout_secs: Option<u64>,
    /// Extra chat-completions body members (`None` ⇒ the planner default: thinking off).
    extra_body: Option<serde_json::Value>,
    /// The first unparseable request value (env or config), refused when the planner is built.
    request_error: Option<NlConfigError>,
}

/// Env overrides (take precedence over the config file when set).
const ENDPOINT_ENV: &str = "EPISTEMIC_GRAPH_NL_ENDPOINT";
const MODEL_ENV: &str = "EPISTEMIC_GRAPH_NL_MODEL";
const API_KEY_ENV_ENV: &str = "EPISTEMIC_GRAPH_NL_API_KEY_ENV";
const TLS_CA_ENV: &str = "EPISTEMIC_GRAPH_NL_TLS_CA";
const READ_TIMEOUT_ENV: &str = "EPISTEMIC_GRAPH_NL_READ_TIMEOUT_SECS";
const EXTRA_BODY_ENV: &str = "EPISTEMIC_GRAPH_NL_EXTRA_BODY";

/// Build the standalone config default planner (CONCEPT:EG-KG.query.fence-stripper). Reads config +
/// env-overrides; returns `None` (no NL surface) when no endpoint is configured.
fn build_default_from_config() -> ConfigDefault {
    let settings = load_or_scaffold_settings();
    if settings.endpoint.trim().is_empty() {
        // Nothing configured — the NL surface stays inert and NlQuery reports it clearly.
        return Ok(None);
    }
    let api_key = settings
        .api_key_env
        .as_deref()
        .filter(|s| !s.is_empty())
        .and_then(|e| std::env::var(e).ok())
        .unwrap_or_default();
    // Resolve the OAuth2 client secret from its env var (config carries only the NAME).
    // A configured oauth2 block whose secret env var is unset is dropped (falls back to the
    // static api_key) rather than minting with an empty secret.
    let oauth2 = settings.oauth2.and_then(|o| {
        let secret = std::env::var(&o.client_secret_env)
            .ok()
            .filter(|s| !s.is_empty())?;
        Some(OAuth2ClientCredentials {
            token_url: o.token_url,
            client_id: o.client_id,
            client_secret: secret,
            scope: o.scope,
            auth_style: o.auth_style,
        })
    });
    let planner = UreqNlPlanner::new(settings.endpoint, settings.model, api_key)
        .with_headers(settings.headers)
        .with_tls_ca_path(settings.tls_ca_path)
        .with_oauth2(oauth2);
    if let Some(e) = settings.request_error {
        return Err(e);
    }
    let planner = apply_request_settings(planner, settings.read_timeout_secs, settings.extra_body)?;
    Ok(Some(Arc::new(planner) as Arc<dyn NlPlanner>))
}

/// Apply the configured read timeout / extra request body (EG-FEDERATED-QUERY-R058.5),
/// refusing invalid values with a typed [`NlConfigError`].
fn apply_request_settings(
    mut planner: UreqNlPlanner,
    read_timeout_secs: Option<u64>,
    extra_body: Option<serde_json::Value>,
) -> Result<UreqNlPlanner, NlConfigError> {
    if let Some(secs) = read_timeout_secs {
        planner = planner.with_read_timeout_secs(secs)?;
    }
    if let Some(extra) = extra_body {
        planner = planner.with_extra_body(extra)?;
    }
    Ok(planner)
}

/// Resolve NL settings from env overrides + Arbora's `config.json`; when no
/// config file exists at any candidate path, scaffold a minimal one (so an operator has
/// a file to fill in) and return empty settings.
fn load_or_scaffold_settings() -> NlSettings {
    // Env overrides first (highest precedence, and enough to run with NO file at all).
    let env_endpoint = std::env::var(ENDPOINT_ENV).ok().filter(|s| !s.is_empty());
    let env_model = std::env::var(MODEL_ENV).ok().filter(|s| !s.is_empty());
    let env_key_env = std::env::var(API_KEY_ENV_ENV)
        .ok()
        .filter(|s| !s.is_empty());

    let file = find_config_file();
    let from_file = file.as_ref().and_then(|p| parse_config(p));

    // If NO file was found anywhere, scaffold a minimal one at the primary config path so
    // the operator can fill it in (best-effort; failure is non-fatal).
    if file.is_none() {
        scaffold_minimal_config();
    }

    let env_tls_ca = [TLS_CA_ENV, "SSL_CERT_FILE", "REQUESTS_CA_BUNDLE"]
        .into_iter()
        .find_map(|name| std::env::var(name).ok().filter(|value| !value.is_empty()));

    let mut settings = from_file.unwrap_or_else(NlSettings::empty);

    // Env overrides win over the file for the fields that carry one.
    if let Some(e) = env_endpoint {
        settings.endpoint = e;
    }
    if let Some(m) = env_model {
        settings.model = m;
    }
    if env_key_env.is_some() {
        settings.api_key_env = env_key_env;
    }
    apply_env_request_values(
        &mut settings,
        std::env::var(READ_TIMEOUT_ENV).ok(),
        std::env::var(EXTRA_BODY_ENV).ok(),
    );
    if let Some(ca) = env_tls_ca {
        settings.tls_ca_path = Some(ca);
    }
    if settings.model.is_empty() {
        settings.model = "gpt-4o-mini".to_string();
    }
    settings
}

/// Apply the env request overrides. An unparseable value keeps its source and raw text so
/// the refusal names them (EG-FEDERATED-QUERY-R058.5).
fn apply_env_request_values(
    settings: &mut NlSettings,
    timeout: Option<String>,
    extra_body: Option<String>,
) {
    if let Some(v) = timeout.filter(|v| !v.is_empty()) {
        match v.trim().parse() {
            Ok(secs) => settings.read_timeout_secs = Some(secs),
            Err(_) => {
                settings.request_error = Some(NlConfigError::InvalidValue {
                    source: READ_TIMEOUT_ENV.into(),
                    raw: v,
                    reason: "is not an integer".into(),
                })
            }
        }
    }
    if let Some(v) = extra_body.filter(|v| !v.is_empty()) {
        match serde_json::from_str(&v) {
            Ok(body) => settings.extra_body = Some(body),
            Err(_) => {
                settings.request_error = Some(NlConfigError::InvalidValue {
                    source: EXTRA_BODY_ENV.into(),
                    raw: v,
                    reason: "is not valid JSON".into(),
                })
            }
        }
    }
}

impl NlSettings {
    /// Empty settings (no endpoint ⇒ the NL surface stays inert).
    fn empty() -> Self {
        Self {
            endpoint: String::new(),
            model: String::new(),
            api_key_env: None,
            headers: Vec::new(),
            tls_ca_path: None,
            oauth2: None,
            read_timeout_secs: None,
            extra_body: None,
            request_error: None,
        }
    }
}

/// The candidate `config.json` paths, in priority order:
///   1. `$ARBORA_CONFIG_DIR/config.json`
///   2. `$XDG_CONFIG_HOME/arbora/config.json`
///   3. `~/.config/arbora/config.json`
fn config_candidates() -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    if let Ok(dir) = std::env::var("ARBORA_CONFIG_DIR") {
        if !dir.is_empty() {
            out.push(std::path::Path::new(&dir).join("config.json"));
        }
    }
    if let Ok(xdg) = std::env::var("XDG_CONFIG_HOME") {
        if !xdg.is_empty() {
            out.push(std::path::Path::new(&xdg).join("arbora/config.json"));
        }
    }
    if let Ok(home) = std::env::var("HOME") {
        if !home.is_empty() {
            out.push(std::path::Path::new(&home).join(".config/arbora/config.json"));
        }
    }
    out
}

/// Return the first existing candidate config path, if any.
fn find_config_file() -> Option<std::path::PathBuf> {
    config_candidates().into_iter().find(|p| p.is_file())
}

/// Parse the LLM settings out of an Arbora `config.json`. Tolerant of layout:
/// looks under a top-level `nl_query` or `llm` object (then the bare top level) for the
/// `endpoint`/`model`/`api_key_env` string keys plus the optional `headers` map,
/// `tls_ca_path`, and `oauth2` client-credentials block. Certificate
/// verification is always enabled; private roots are supplied through a CA bundle.
fn parse_config(path: &std::path::Path) -> Option<NlSettings> {
    let text = std::fs::read_to_string(path).ok()?;
    let json: serde_json::Value = serde_json::from_str(&text).ok()?;
    // Search the two conventional sections then the bare root.
    let section = json
        .get("nl_query")
        .or_else(|| json.get("llm"))
        .unwrap_or(&json);
    let getstr = |k: &str| section.get(k).and_then(|v| v.as_str()).map(str::to_string);

    // Static headers: an object of string→string; non-string values are skipped.
    let headers = section
        .get("headers")
        .and_then(|v| v.as_object())
        .map(|m| {
            m.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    let tls_ca_path = getstr("tls_ca_path").filter(|s| !s.is_empty());

    // OAuth2 client-credentials block — requires token_url + client_id + client_secret_env.
    let oauth2 = section.get("oauth2").and_then(|o| {
        let gs = |k: &str| o.get(k).and_then(|v| v.as_str()).map(str::to_string);
        let token_url = gs("token_url").filter(|s| !s.is_empty())?;
        let client_id = gs("client_id").filter(|s| !s.is_empty())?;
        let client_secret_env = gs("client_secret_env").filter(|s| !s.is_empty())?;
        let auth_style = match o.get("auth_style").and_then(|v| v.as_str()) {
            Some(s) if s.eq_ignore_ascii_case("basic") => TokenAuthStyle::Basic,
            _ => TokenAuthStyle::Body,
        };
        Some(OAuth2Settings {
            token_url,
            client_id,
            client_secret_env,
            scope: gs("scope").filter(|s| !s.is_empty()),
            auth_style,
        })
    });

    Some(NlSettings {
        endpoint: getstr("endpoint").unwrap_or_default(),
        model: getstr("model").unwrap_or_default(),
        api_key_env: getstr("api_key_env"),
        headers,
        tls_ca_path,
        oauth2,
        read_timeout_secs: section.get("read_timeout_secs").and_then(|v| v.as_u64()),
        extra_body: section.get("extra_body").cloned(),
        request_error: section
            .get("read_timeout_secs")
            .filter(|v| v.as_u64().is_none())
            .map(|v| NlConfigError::InvalidValue {
                source: "read_timeout_secs".into(),
                raw: v.to_string(),
                reason: "is not an unsigned integer".into(),
            }),
    })
}

/// Best-effort scaffold of a minimal `config.json` at `~/.config/arbora/` so an
/// operator has a file to fill in. Never overwrites an existing file; failure is silent
/// (the NL surface simply stays unconfigured).
fn scaffold_minimal_config() {
    let Some(home) = std::env::var("HOME").ok().filter(|s| !s.is_empty()) else {
        return;
    };
    let dir = std::path::Path::new(&home).join(".config/arbora");
    let path = dir.join("config.json");
    if path.exists() {
        return;
    }
    if std::fs::create_dir_all(&dir).is_err() {
        return;
    }
    let scaffold = serde_json::json!({
        "nl_query": {
            "endpoint": "",
            "model": "gpt-4o-mini",
            "api_key_env": "OPENAI_API_KEY",
            "_comment": "epistemic-graph CONCEPT:EG-KG.query.fence-stripper — set `endpoint` to an \
    OpenAI-compatible /chat/completions URL to enable the NL query surface (Method::NlQuery \
    + /nl). `api_key_env` names the env var holding the bearer key (empty for a local \
    keyless endpoint).",
            "_headers_comment": "Optional `headers`: a string→string map sent on every request \
    (e.g. {\"X-Client-Id\": \"my-service\"} for a gateway that requires a static client-id header).",
            "_tls_comment": "Use `tls_ca_path` (or EPISTEMIC_GRAPH_NL_TLS_CA) for \
    private trust. Certificate verification is always enabled.",
            "_oauth2_comment": "Optional `oauth2` client-credentials block minting a short-lived \
    bearer instead of a static key: {\"token_url\": \"...\", \"client_id\": \"...\", \
    \"client_secret_env\": \"NL_OAUTH_CLIENT_SECRET\", \"scope\": \"api://x/.default\", \
    \"auth_style\": \"basic\"}. `auth_style` basic ⇒ HTTP Basic at the token endpoint; the \
    secret is read from the named env var, never stored in this file."
        }
    });
    if let Ok(body) = serde_json::to_string_pretty(&scaffold) {
        let _ = std::fs::write(&path, body);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_tmp(name: &str, body: &str) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(name);
        std::fs::write(&path, body).unwrap();
        path
    }

    #[test]
    fn config_candidates_never_name_the_legacy_agent_utilities_path() {
        for candidate in config_candidates() {
            let text = candidate.to_string_lossy();
            assert!(!text.contains("agent-utilities"), "legacy path: {text}");
        }
    }

    #[test]
    fn parse_config_reads_headers_tls_and_oauth2() {
        let path = write_tmp(
            "eg_nl_full_config.json",
            r#"{
              "nl_query": {
                "endpoint": "https://gw.example/v1/chat/completions",
                "model": "qwen",
                "headers": {"X-Client-Id": "svc-42"},
                "tls_ca_path": "/etc/ssl/internal-ca.pem",
                "oauth2": {
                  "token_url": "https://idp/token",
                  "client_id": "cid",
                  "client_secret_env": "NL_OAUTH_SECRET",
                  "scope": "api://x/.default",
                  "auth_style": "basic"
                }
              }
            }"#,
        );
        let s = parse_config(&path).expect("parsed");
        assert_eq!(s.endpoint, "https://gw.example/v1/chat/completions");
        assert_eq!(
            s.headers,
            vec![("X-Client-Id".to_string(), "svc-42".to_string())]
        );
        assert_eq!(s.tls_ca_path.as_deref(), Some("/etc/ssl/internal-ca.pem"));
        let o = s.oauth2.expect("oauth2 parsed");
        assert_eq!(o.token_url, "https://idp/token");
        assert_eq!(o.client_secret_env, "NL_OAUTH_SECRET");
        assert_eq!(o.auth_style, TokenAuthStyle::Basic);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn parse_config_defaults_are_inert() {
        let path = write_tmp(
            "eg_nl_minimal_config.json",
            r#"{"nl_query": {"endpoint": "http://x/v1", "model": "m"}}"#,
        );
        let s = parse_config(&path).expect("parsed");
        assert!(s.headers.is_empty());
        assert!(s.tls_ca_path.is_none());
        assert!(s.oauth2.is_none());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn parse_config_drops_incomplete_oauth2_block() {
        // Missing client_secret_env ⇒ the block is ignored (no half-configured mint).
        let path = write_tmp(
            "eg_nl_bad_oauth_config.json",
            r#"{"nl_query": {"endpoint": "http://x/v1",
                "oauth2": {"token_url": "https://idp/token", "client_id": "cid"}}}"#,
        );
        let s = parse_config(&path).expect("parsed");
        assert!(s.oauth2.is_none());
        let _ = std::fs::remove_file(&path);
    }

    // spec: EG-FEDERATED-QUERY-R058.5
    #[test]
    fn config_request_settings_are_parsed_and_invalid_ones_refused() {
        let dir = std::env::temp_dir().join(format!("eg-nl-cfg-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("config.json");
        std::fs::write(
            &path,
            r#"{"nl_query":{"endpoint":"http://x","read_timeout_secs":90,"extra_body":{"top_k":3}}}"#,
        )
        .unwrap();
        let s = parse_config(&path).unwrap();
        assert_eq!(s.read_timeout_secs, Some(90));
        assert_eq!(s.extra_body, Some(serde_json::json!({"top_k": 3})));
        let planner = || UreqNlPlanner::new("http://x".into(), "m".into(), String::new());
        assert!(apply_request_settings(planner(), s.read_timeout_secs, s.extra_body).is_ok());
        assert_eq!(
            apply_request_settings(planner(), Some(0), None).err(),
            Some(NlConfigError::ReadTimeoutOutOfRange(0))
        );
        assert_eq!(
            apply_request_settings(planner(), None, Some(serde_json::Value::Null)).err(),
            Some(NlConfigError::ExtraBodyNotObject)
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    // spec: EG-FEDERATED-QUERY-R058.5
    #[test]
    fn unparseable_env_values_are_refused_with_source_and_raw_value() {
        let mut s = NlSettings::empty();
        apply_env_request_values(&mut s, Some("abc".into()), None);
        assert_eq!(
            s.request_error.unwrap().to_string(),
            "nl-query: EPISTEMIC_GRAPH_NL_READ_TIMEOUT_SECS='abc' is not an integer"
        );
        let mut s = NlSettings::empty();
        apply_env_request_values(&mut s, Some("45".into()), Some("{oops".into()));
        assert_eq!(s.read_timeout_secs, Some(45));
        assert!(s
            .request_error
            .unwrap()
            .to_string()
            .contains("EPISTEMIC_GRAPH_NL_EXTRA_BODY='{oops' is not valid JSON"));
        let dir = std::env::temp_dir().join(format!("eg-nl-cfg2-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("config.json");
        std::fs::write(&path, r#"{"nl_query":{"read_timeout_secs":"soon"}}"#).unwrap();
        let err = parse_config(&path).unwrap().request_error.unwrap();
        assert_eq!(
            err.to_string(),
            "nl-query: read_timeout_secs='\"soon\"' is not an unsigned integer"
        );
        std::fs::remove_dir_all(dir).unwrap();
    }
}
