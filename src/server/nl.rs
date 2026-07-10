//! Natural-language query planner resolution for the facade (CONCEPT:EG-KG.query.fence-stripper).
//!
//! The engine's NL surface is LLM-OPTIONAL and pure-Rust in the core: the
//! [`eg_plan::NlPlanner`] seam (CONCEPT:EG-KG.query.core-query-input) turns NL into a UQL query STRING that then
//! runs through the existing deterministic `UnifiedQueryText` pipeline. This module owns
//! WHICH planner the facade uses:
//!
//!  * **Injected** — when `agent-utilities` (or any embedder) drives the engine it does
//!    NL→query on its own side and can OPT OUT of the engine's built-in planner, or it
//!    can inject its own via [`set_nl_planner`]. An injected planner always wins.
//!  * **Standalone (config)** — a bare engine reads an OpenAI-compatible LLM
//!    endpoint/model/api-key-env from `agent-utilities`' `config.json` and builds a
//!    [`eg_plan::UreqNlPlanner`]. If no endpoint is configured the planner stays `None`
//!    and `Method::NlQuery` returns a clear "not configured" error (never a panic).
//!
//! The whole module is gated behind `nl-query`.

use std::sync::{Arc, OnceLock};

use eg_plan::{NlPlanner, OAuth2ClientCredentials, TokenAuthStyle, UreqNlPlanner};

/// An operator-injected planner (CONCEPT:EG-KG.query.fence-stripper). Set ONCE, before first use, by an
/// embedder that wants the engine to drive its own NL→query. Wins over the config default.
static INJECTED: OnceLock<Arc<dyn NlPlanner>> = OnceLock::new();
/// The lazily-built, config-derived default planner (`None` when nothing is configured).
/// Built at most once, on the first `NlQuery` that finds no injected planner.
static CONFIG_DEFAULT: OnceLock<Option<Arc<dyn NlPlanner>>> = OnceLock::new();

/// Inject the NL planner the facade should use (CONCEPT:EG-KG.query.fence-stripper). An embedder calls this
/// at startup to opt into the engine driving NL→query with its own planner. Idempotent:
/// a second call after the planner is set (or after the config default was already
/// resolved) is ignored, so the wiring is stable for the process lifetime.
pub fn set_nl_planner(planner: Arc<dyn NlPlanner>) {
    let _ = INJECTED.set(planner);
}

/// Resolve the active planner: the injected one if present, else the lazily-built config
/// default (which may be `None` when standalone config names no LLM endpoint).
pub fn resolve_planner() -> Option<Arc<dyn NlPlanner>> {
    if let Some(p) = INJECTED.get() {
        return Some(p.clone());
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

/// LLM settings resolved from `agent-utilities`' `config.json` (+ env overrides).
struct NlSettings {
    endpoint: String,
    model: String,
    /// Name of the env var carrying the bearer key (resolved to the key at build time).
    api_key_env: Option<String>,
    /// Static headers sent on every request (e.g. a gateway client-id header).
    headers: Vec<(String, String)>,
    /// Per-endpoint TLS: accept invalid/self-signed certs (internal endpoints only).
    tls_insecure: bool,
    /// Per-endpoint TLS: extra PEM CA bundle path trusted on top of the webpki roots.
    tls_ca_path: Option<String>,
    /// OAuth2 client-credentials token source (mints a bearer instead of a static key).
    oauth2: Option<OAuth2Settings>,
}

/// Env overrides (take precedence over the config file when set).
const ENDPOINT_ENV: &str = "EPISTEMIC_GRAPH_NL_ENDPOINT";
const MODEL_ENV: &str = "EPISTEMIC_GRAPH_NL_MODEL";
const API_KEY_ENV_ENV: &str = "EPISTEMIC_GRAPH_NL_API_KEY_ENV";
const TLS_INSECURE_ENV: &str = "EPISTEMIC_GRAPH_NL_TLS_INSECURE";
const TLS_CA_ENV: &str = "EPISTEMIC_GRAPH_NL_TLS_CA";

/// Build the standalone config default planner (CONCEPT:EG-KG.query.fence-stripper). Reads config +
/// env-overrides; returns `None` (no NL surface) when no endpoint is configured.
fn build_default_from_config() -> Option<Arc<dyn NlPlanner>> {
    let settings = load_or_scaffold_settings();
    if settings.endpoint.trim().is_empty() {
        // Nothing configured — the NL surface stays inert and NlQuery reports it clearly.
        return None;
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
        .with_tls_insecure(settings.tls_insecure)
        .with_tls_ca_path(settings.tls_ca_path)
        .with_oauth2(oauth2);
    Some(Arc::new(planner) as Arc<dyn NlPlanner>)
}

/// Resolve NL settings from env overrides + `agent-utilities`' `config.json`; when no
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

    let env_tls_insecure = std::env::var(TLS_INSECURE_ENV)
        .ok()
        .filter(|s| !s.is_empty())
        .map(|v| parse_bool(&v));
    let env_tls_ca = std::env::var(TLS_CA_ENV).ok().filter(|s| !s.is_empty());

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
    if let Some(insecure) = env_tls_insecure {
        settings.tls_insecure = insecure;
    }
    if let Some(ca) = env_tls_ca {
        settings.tls_ca_path = Some(ca);
    }
    if settings.model.is_empty() {
        settings.model = "gpt-4o-mini".to_string();
    }
    settings
}

/// Parse a JSON-ish boolean string (`true`/`1`/`yes`/`on`, case-insensitive).
fn parse_bool(v: &str) -> bool {
    matches!(
        v.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
}

impl NlSettings {
    /// Empty settings (no endpoint ⇒ the NL surface stays inert).
    fn empty() -> Self {
        Self {
            endpoint: String::new(),
            model: String::new(),
            api_key_env: None,
            headers: Vec::new(),
            tls_insecure: false,
            tls_ca_path: None,
            oauth2: None,
        }
    }
}

/// The candidate `config.json` paths, in priority order:
///   1. `$XDG_CONFIG_HOME/agent-utilities/config.json`
///   2. `~/.config/agent-utilities/config.json`
///   3. `~/.local/share/agent-utilities/config.json`
fn config_candidates() -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    if let Ok(xdg) = std::env::var("XDG_CONFIG_HOME") {
        if !xdg.is_empty() {
            out.push(std::path::Path::new(&xdg).join("agent-utilities/config.json"));
        }
    }
    if let Ok(home) = std::env::var("HOME") {
        if !home.is_empty() {
            out.push(std::path::Path::new(&home).join(".config/agent-utilities/config.json"));
            out.push(std::path::Path::new(&home).join(".local/share/agent-utilities/config.json"));
        }
    }
    out
}

/// Return the first existing candidate config path, if any.
fn find_config_file() -> Option<std::path::PathBuf> {
    config_candidates().into_iter().find(|p| p.is_file())
}

/// Parse the LLM settings out of an `agent-utilities` `config.json`. Tolerant of layout:
/// looks under a top-level `nl_query` or `llm` object (then the bare top level) for the
/// `endpoint`/`model`/`api_key_env` string keys plus the optional `headers` map,
/// `tls_insecure`/`tls_ca_path`, and `oauth2` client-credentials block.
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

    let tls_insecure = section
        .get("tls_insecure")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
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
        tls_insecure,
        tls_ca_path,
        oauth2,
    })
}

/// Best-effort scaffold of a minimal `config.json` at `~/.config/agent-utilities/` so an
/// operator has a file to fill in. Never overwrites an existing file; failure is silent
/// (the NL surface simply stays unconfigured).
fn scaffold_minimal_config() {
    let Some(home) = std::env::var("HOME").ok().filter(|s| !s.is_empty()) else {
        return;
    };
    let dir = std::path::Path::new(&home).join(".config/agent-utilities");
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
            "_tls_comment": "Optional per-endpoint TLS: `tls_insecure` (bool) accepts a \
    self-signed cert on a trusted internal endpoint; `tls_ca_path` trusts an extra PEM CA \
    bundle. Also settable via EPISTEMIC_GRAPH_NL_TLS_INSECURE / EPISTEMIC_GRAPH_NL_TLS_CA.",
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
    fn parse_bool_recognizes_truthy_values() {
        for t in ["1", "true", "TRUE", "yes", "On"] {
            assert!(parse_bool(t), "{t} should be true");
        }
        for f in ["0", "false", "no", "", "off"] {
            assert!(!parse_bool(f), "{f} should be false");
        }
    }

    #[test]
    fn parse_config_reads_headers_tls_and_oauth2() {
        let path = write_tmp(
            "eg_nl_full_config.json",
            r#"{
              "nl_query": {
                "endpoint": "https://gw.arpa/v1/chat/completions",
                "model": "qwen",
                "headers": {"X-Client-Id": "svc-42"},
                "tls_insecure": true,
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
        assert_eq!(s.endpoint, "https://gw.arpa/v1/chat/completions");
        assert_eq!(
            s.headers,
            vec![("X-Client-Id".to_string(), "svc-42".to_string())]
        );
        assert!(s.tls_insecure);
        assert_eq!(s.tls_ca_path.as_deref(), Some("/etc/ssl/internal-ca.pem"));
        let o = s.oauth2.expect("oauth2 parsed");
        assert_eq!(o.token_url, "https://idp/token");
        assert_eq!(o.client_secret_env, "NL_OAUTH_SECRET");
        assert_eq!(o.auth_style, TokenAuthStyle::Basic);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn parse_config_defaults_are_inert_and_backward_compatible() {
        let path = write_tmp(
            "eg_nl_minimal_config.json",
            r#"{"nl_query": {"endpoint": "http://x/v1", "model": "m"}}"#,
        );
        let s = parse_config(&path).expect("parsed");
        assert!(s.headers.is_empty());
        assert!(!s.tls_insecure);
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
}
