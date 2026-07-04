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

use eg_plan::{NlPlanner, UreqNlPlanner};

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

/// LLM settings resolved from `agent-utilities`' `config.json` (+ env overrides).
struct NlSettings {
    endpoint: String,
    model: String,
    /// Name of the env var carrying the bearer key (resolved to the key at build time).
    api_key_env: Option<String>,
}

/// Env overrides (take precedence over the config file when set).
const ENDPOINT_ENV: &str = "EPISTEMIC_GRAPH_NL_ENDPOINT";
const MODEL_ENV: &str = "EPISTEMIC_GRAPH_NL_MODEL";
const API_KEY_ENV_ENV: &str = "EPISTEMIC_GRAPH_NL_API_KEY_ENV";

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
    let planner = UreqNlPlanner::new(settings.endpoint, settings.model, api_key);
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

    let (f_endpoint, f_model, f_key_env) =
        from_file.unwrap_or((String::new(), String::new(), None));

    NlSettings {
        endpoint: env_endpoint.unwrap_or(f_endpoint),
        model: env_model
            .or(if f_model.is_empty() {
                None
            } else {
                Some(f_model)
            })
            .unwrap_or_else(|| "gpt-4o-mini".to_string()),
        api_key_env: env_key_env.or(f_key_env),
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

/// Parse the LLM `(endpoint, model, api_key_env)` out of an `agent-utilities`
/// `config.json`. Tolerant of layout: looks under a top-level `nl_query` or `llm` object
/// (then the bare top level) for `endpoint`/`model`/`api_key_env` string keys.
fn parse_config(path: &std::path::Path) -> Option<(String, String, Option<String>)> {
    let text = std::fs::read_to_string(path).ok()?;
    let json: serde_json::Value = serde_json::from_str(&text).ok()?;
    // Search the two conventional sections then the bare root.
    let section = json
        .get("nl_query")
        .or_else(|| json.get("llm"))
        .unwrap_or(&json);
    let getstr = |k: &str| section.get(k).and_then(|v| v.as_str()).map(str::to_string);
    let endpoint = getstr("endpoint").unwrap_or_default();
    let model = getstr("model").unwrap_or_default();
    let api_key_env = getstr("api_key_env");
    Some((endpoint, model, api_key_env))
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
    keyless endpoint)."
        }
    });
    if let Ok(body) = serde_json::to_string_pretty(&scaffold) {
        let _ = std::fs::write(&path, body);
    }
}
