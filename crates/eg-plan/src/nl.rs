//! Natural-language → query planning seam (CONCEPT:EG-KG.query.core-query-input) + a concrete, LLM-optional
//! planner (CONCEPT:EG-KG.query.fence-stripper).
//!
//! ## The seam (CONCEPT:EG-KG.query.core-query-input)
//!
//! The engine stays PURE-RUST and DETERMINISTIC: NL is turned into an executable query
//! STRING by a pluggable [`NlPlanner`], and that string then runs through the engine's
//! EXISTING deterministic pipeline (`uql::parse` → the fused [`crate::execute`]). There
//! is no new execution path and no LLM in the engine core — the planner is the ONLY
//! non-deterministic step, and it is entirely optional:
//!
//!  * the engine core takes `Option<&dyn NlPlanner>` — see [`plan_and_execute_opt`];
//!    a `None` planner is a **no-op** (`Ok(None)`), so a build/deployment that never
//!    configures a planner still compiles and runs, it just has no NL surface.
//!  * a `Some(planner)` produces a UQL string that is parsed + executed EXACTLY like a
//!    hand-written `Uql` statement — the query language target is UQL (the engine's
//!    text front-end), so the whole downstream is the audited, deterministic path.
//!
//! ## The standalone planner (CONCEPT:EG-KG.query.fence-stripper)
//!
//! [`UreqNlPlanner`] is a concrete [`NlPlanner`] (gated behind `nl-query`) that POSTs to
//! an OpenAI-compatible `/chat/completions` endpoint and extracts the produced query.
//! It reuses the SAME pure-Rust rustls HTTP client (`ureq`) the `federation` foreign
//! sources use — NO new HTTP dep, NO openssl — and it is kept OUT of the Pi tier. When
//! `agent-utilities` drives the engine it does NL→query on its own side and simply does
//! not call the NL surface (opt-out); a STANDALONE engine reads the endpoint/model/
//! api-key-env from `agent-utilities`' `config.json` and builds this planner.
//!
//! ## Safety
//!
//! The LLM endpoint comes from LOCAL config (a trusted operator), so this is not an
//! open SSRF surface — but [`UreqNlPlanner`] still applies connect/read TIMEOUTS and a
//! RESPONSE-SIZE CAP so a slow/hostile endpoint cannot hang or OOM the engine.

use crate::exec::{execute, PlanCtx};
use crate::rowset::RowSet;

/// The NL→query planning seam (CONCEPT:EG-KG.query.core-query-input): turn a natural-language request plus a
/// `schema_hint` (labels / grammar the model should target) into an executable query
/// STRING. Returning a string — not a `Plan` — keeps the planner language-agnostic
/// (UQL / SQL / Cypher / SPARQL) and keeps EXECUTION on the engine's existing
/// deterministic pipeline. `Send + Sync` so a planner can be stored behind an `Arc` and
/// shared across the async request handlers.
pub trait NlPlanner: Send + Sync {
    /// Produce an executable query string for `nl`, given a `schema_hint`. An `Err`
    /// (network / model / empty output) is surfaced to the caller as a clear error
    /// rather than a panic or a silent empty result.
    fn plan(&self, nl: &str, schema_hint: &str) -> Result<String, String>;
}

/// CONCEPT:EG-KG.query.core-query-input — the deterministic seam. Run `planner` to get a UQL query string,
/// then parse + execute it through the engine's EXISTING pipeline (`uql::parse` → the
/// fused [`crate::execute`]). The planner is the only non-deterministic step; a produced
/// query that does not parse as UQL surfaces the caret-annotated parse error (never a
/// panic).
pub fn plan_and_execute(
    planner: &dyn NlPlanner,
    nl: &str,
    schema_hint: &str,
    ctx: &PlanCtx,
) -> Result<RowSet, String> {
    let query = planner.plan(nl, schema_hint)?;
    let plan = crate::uql::parse(&query).map_err(|e| e.render(&query))?;
    execute(&plan, ctx)
}

/// CONCEPT:EG-KG.query.core-query-input — the LLM-OPTIONAL entry point the engine core calls. With `None` the
/// NL feature is a **no-op** (`Ok(None)`): the engine has no planner configured/injected,
/// so there is simply no NL surface — it does not error, it just does nothing. With
/// `Some(planner)` it delegates to [`plan_and_execute`] and wraps the rows in `Some`.
pub fn plan_and_execute_opt(
    planner: Option<&dyn NlPlanner>,
    nl: &str,
    schema_hint: &str,
    ctx: &PlanCtx,
) -> Result<Option<RowSet>, String> {
    match planner {
        None => Ok(None),
        Some(p) => plan_and_execute(p, nl, schema_hint, ctx).map(Some),
    }
}

// ── Disclosed, typed NL→UQL results (CONCEPT:EG-KG.query.core-query-input, EG-FEDERATED-QUERY-R058) ──────
//
// A natural-language request is never query authority itself: it is the configured
// `NlPlanner` that translates it to UQL TEXT, and it is the ENGINE that parses and runs
// that text — identically to any caller-written query. [`NlQueryResult`] makes that
// disclosure a typed guarantee: every result (executed or plan-only) carries the exact
// UQL alongside any data, so a caller never has to trust an opaque NL answer. A planner
// answer that is over budget or fails to parse is refused with a typed [`NlQueryError`]
// rather than silently run.

/// A refusal of a planner's output, BEFORE it is treated as query authority
/// (EG-FEDERATED-QUERY-R058). Every variant is a clean, typed error — never a panic and
/// never a silent fallback to running unparsed/over-budget text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NlQueryError {
    /// The planner itself failed (network / model / empty output) — mirrors
    /// [`NlPlanner::plan`]'s `Err`.
    Planner(String),
    /// The planner's produced UQL text exceeded the configured [`NlQueryBudget`] and was
    /// refused BEFORE it was ever parsed or executed.
    BudgetExceeded {
        /// The configured limit (characters) that was exceeded.
        limit: usize,
        /// The actual length (characters) of the refused text.
        actual: usize,
    },
    /// The planner's produced text does not parse as UQL under the SAME grammar a
    /// caller-written query uses. `query` is the refused text; `message` is the
    /// caret-annotated parse error.
    ParseFailed {
        /// The refused UQL candidate text.
        query: String,
        /// The caret-annotated parse error.
        message: String,
    },
    /// The produced UQL parsed but failed during execution.
    Execution {
        /// The UQL that was executed when it failed.
        query: String,
        /// The execution error.
        message: String,
    },
}

impl std::fmt::Display for NlQueryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Planner(msg) => write!(f, "nl-query: planner failed: {msg}"),
            Self::BudgetExceeded { limit, actual } => write!(
                f,
                "nl-query: planner output refused: {actual} chars exceeds budget of {limit}"
            ),
            Self::ParseFailed { message, .. } => write!(f, "nl-query: {message}"),
            Self::Execution { message, .. } => write!(f, "nl-query: execution failed: {message}"),
        }
    }
}

impl std::error::Error for NlQueryError {}

/// A hard cap on a planner's produced UQL text, enforced BEFORE parsing or execution
/// (EG-FEDERATED-QUERY-R058). Guards against a runaway or hostile planner answer
/// consuming parser/execution resources.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NlQueryBudget {
    /// Maximum length, in characters, of the planner's produced UQL text.
    pub max_uql_len: usize,
}

impl Default for NlQueryBudget {
    /// A generous but bounded default: long enough for any realistic hand-written-style
    /// UQL statement, short enough to refuse a runaway/hostile planner answer.
    fn default() -> Self {
        Self { max_uql_len: 8192 }
    }
}

/// Every natural-language result (EG-FEDERATED-QUERY-R058): carries the EXACT UQL text —
/// executed, or, in plan-only mode, the unexecuted candidate — alongside any data.
/// `rows` is `None` exactly in plan-only mode, so plan-only mode is a property of the
/// typed VALUE, not just of the call site that produced it.
#[derive(Debug, Clone)]
pub struct NlQueryResult {
    /// The exact UQL text: executed (normal mode), or the unexecuted candidate
    /// (plan-only mode).
    pub uql: String,
    /// `Some(rows)` when `uql` was executed; `None` in plan-only mode, when no data was
    /// read at all.
    pub rows: Option<RowSet>,
}

impl NlQueryResult {
    /// `true` when this is an unexecuted plan-only candidate: no data was read.
    pub fn is_plan_only(&self) -> bool {
        self.rows.is_none()
    }
}

/// Plan `nl` into UQL via `planner`, enforcing `budget` and parsing with the SAME grammar
/// [`crate::uql::parse`] uses for any caller-written query — but never execute it
/// (EG-FEDERATED-QUERY-R058). Returns the typed candidate so a caller can review the exact
/// UQL before it ever touches data. Notably, this function takes no [`PlanCtx`]/view at
/// all: there is no data for it to read even by accident.
pub fn plan_only(
    planner: &dyn NlPlanner,
    nl: &str,
    schema_hint: &str,
    budget: NlQueryBudget,
) -> Result<NlQueryResult, NlQueryError> {
    let uql = planner
        .plan(nl, schema_hint)
        .map_err(NlQueryError::Planner)?;
    enforce_budget(&uql, budget)?;
    crate::uql::parse(&uql).map_err(|e| NlQueryError::ParseFailed {
        message: e.render(&uql),
        query: uql.clone(),
    })?;
    Ok(NlQueryResult { uql, rows: None })
}

/// The executing counterpart of [`plan_only`]: plan + budget + parse exactly as
/// [`plan_only`] does, then execute through the engine's EXISTING deterministic pipeline,
/// returning the EXECUTED UQL alongside the rows (EG-FEDERATED-QUERY-R058 — "every
/// natural-language result carries the exact UQL that was executed").
pub fn plan_and_execute_typed(
    planner: &dyn NlPlanner,
    nl: &str,
    schema_hint: &str,
    budget: NlQueryBudget,
    ctx: &PlanCtx,
) -> Result<NlQueryResult, NlQueryError> {
    let uql = planner
        .plan(nl, schema_hint)
        .map_err(NlQueryError::Planner)?;
    enforce_budget(&uql, budget)?;
    let plan = crate::uql::parse(&uql).map_err(|e| NlQueryError::ParseFailed {
        message: e.render(&uql),
        query: uql.clone(),
    })?;
    let rows = execute(&plan, ctx).map_err(|message| NlQueryError::Execution {
        query: uql.clone(),
        message,
    })?;
    Ok(NlQueryResult {
        uql,
        rows: Some(rows),
    })
}

/// Refuse `uql` BEFORE it is parsed or executed when it exceeds `budget` (character count).
fn enforce_budget(uql: &str, budget: NlQueryBudget) -> Result<(), NlQueryError> {
    let actual = uql.chars().count();
    if actual > budget.max_uql_len {
        return Err(NlQueryError::BudgetExceeded {
            limit: budget.max_uql_len,
            actual,
        });
    }
    Ok(())
}

// ── The concrete standalone planner (CONCEPT:EG-KG.query.fence-stripper, feature `nl-query`) ─────────────

/// How client credentials are presented AT the OAuth2 token endpoint (RFC 6749 §2.3.1).
#[cfg(feature = "nl-query")]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum TokenAuthStyle {
    /// `client_secret_post` — id/secret as form params in the POST body (default).
    #[default]
    Body,
    /// `client_secret_basic` — id/secret via HTTP Basic; omitted from the body.
    Basic,
}

/// OAuth2 ``client_credentials`` source for the NL LLM endpoint (CONCEPT:EG-KG.query.nl-oauth-token-exchange).
/// When set on a [`UreqNlPlanner`], a short-lived bearer is EXCHANGED at ``token_url``
/// (and cached until just before expiry) instead of relying on a static ``api_key`` — the
/// engine-side parallel of agent-utilities' outbound OAuth2 client-credentials lifecycle.
#[cfg(feature = "nl-query")]
#[derive(Clone, Debug)]
pub struct OAuth2ClientCredentials {
    /// OIDC/OAuth2 token endpoint (e.g. Azure AD `/oauth2/v2.0/token`).
    pub token_url: String,
    /// Client id (the resolved literal; a config-layer env-ref is resolved before this).
    pub client_id: String,
    /// Client secret (resolved literal; never logged).
    pub client_secret: String,
    /// Optional space-separated scopes (Azure AD v2: `<resource>/.default`).
    pub scope: Option<String>,
    /// Whether the credentials go in the body or via HTTP Basic at the token endpoint.
    pub auth_style: TokenAuthStyle,
}

/// A concrete [`NlPlanner`] (CONCEPT:EG-KG.query.fence-stripper) that asks an OpenAI-compatible
/// `/chat/completions` endpoint to translate NL → a UQL query, over the SAME pure-Rust
/// rustls HTTP client (`ureq`) the federation sources use. Kept OUT of the Pi tier.
///
/// Safety: the endpoint is LOCAL/trusted config, but a connect timeout, a read timeout
/// and a response-size cap are always applied so a slow / hostile endpoint can neither
/// hang the request nor exhaust memory.
#[cfg(feature = "nl-query")]
pub struct UreqNlPlanner {
    /// Full chat-completions URL, e.g. `http://127.0.0.1:8000/v1/chat/completions`.
    endpoint: String,
    /// Model id, e.g. `qwen/qwen3.6-35b-a3b`.
    model: String,
    /// Bearer key (empty ⇒ no `Authorization` header — a local, keyless vLLM/Ollama).
    api_key: String,
    /// TCP connect timeout.
    connect_timeout: std::time::Duration,
    /// Response read timeout.
    read_timeout: std::time::Duration,
    /// Hard cap on the bytes read from the response body (OOM guard).
    max_response_bytes: u64,
    /// The system prompt that pins the model to emit ONE bare UQL query.
    system_prompt: String,
    /// Static headers sent on EVERY request to the endpoint (e.g. a gateway
    /// ``X-Client-Id`` client-id header). Independent of the auth mode.
    headers: Vec<(String, String)>,
    /// TLS: path to an additional PEM CA bundle trusted for THIS endpoint (added on top
    /// of the standard webpki roots).
    tls_ca_path: Option<String>,
    /// Optional OAuth2 client-credentials token source. When set, a minted+cached bearer
    /// is used instead of the static ``api_key`` (CONCEPT:EG-KG.query.nl-oauth-token-exchange).
    oauth2: Option<OAuth2ClientCredentials>,
    /// Cached minted bearer + its monotonic expiry instant (lazy token exchange).
    token_cache: std::sync::Mutex<Option<(String, std::time::Instant)>>,
}

#[cfg(feature = "nl-query")]
impl UreqNlPlanner {
    /// The default system prompt: pin the model to emit exactly one bare UQL query
    /// (no prose, no fences). GENERATED from the UQL grammar
    /// (`crate::uql::grammar::nl_system_prompt`) so it can only advertise syntax the
    /// parser accepts (UQL-10 — it used to promise `!=`/`>=`/`<=` the parser rejected).
    pub fn default_system_prompt() -> String {
        crate::uql::grammar::nl_system_prompt()
    }

    /// Build a planner with the default timeouts (5s connect / 30s read), a 1 MiB
    /// response cap and the [`Self::default_system_prompt`]. `api_key` may be empty for a
    /// local keyless endpoint.
    pub fn new(endpoint: String, model: String, api_key: String) -> Self {
        Self {
            endpoint,
            model,
            api_key,
            connect_timeout: std::time::Duration::from_secs(5),
            read_timeout: std::time::Duration::from_secs(30),
            max_response_bytes: 1024 * 1024,
            system_prompt: Self::default_system_prompt(),
            headers: Vec::new(),
            tls_ca_path: None,
            oauth2: None,
            token_cache: std::sync::Mutex::new(None),
        }
    }

    /// Set static headers sent on every request (e.g. a gateway client-id header) (fluent).
    pub fn with_headers(mut self, headers: Vec<(String, String)>) -> Self {
        self.headers = headers;
        self
    }

    /// Per-endpoint TLS: trust an extra PEM CA bundle on top of the webpki roots (fluent).
    pub fn with_tls_ca_path(mut self, ca_path: Option<String>) -> Self {
        self.tls_ca_path = ca_path;
        self
    }

    /// Mint the bearer via an OAuth2 client-credentials exchange instead of a static key (fluent).
    pub fn with_oauth2(mut self, oauth2: Option<OAuth2ClientCredentials>) -> Self {
        self.oauth2 = oauth2;
        self
    }

    /// Override the connect/read timeouts (fluent).
    pub fn with_timeouts(
        mut self,
        connect: std::time::Duration,
        read: std::time::Duration,
    ) -> Self {
        self.connect_timeout = connect;
        self.read_timeout = read;
        self
    }

    /// Override the response-size cap (fluent).
    pub fn with_max_response_bytes(mut self, cap: u64) -> Self {
        self.max_response_bytes = cap;
        self
    }

    /// Override the system prompt (fluent).
    pub fn with_system_prompt(mut self, prompt: String) -> Self {
        self.system_prompt = prompt;
        self
    }

    /// Build the bounded, timeout-guarded `ureq` agent, applying per-endpoint TLS
    /// (custom CA / insecure) when configured. With neither TLS override set the agent
    /// uses ureq's default rustls + webpki-roots verification (byte-for-byte the prior
    /// behaviour), so only an explicit opt-in changes TLS.
    fn build_agent(&self) -> Result<ureq::Agent, String> {
        let mut builder = ureq::AgentBuilder::new()
            .timeout_connect(self.connect_timeout)
            .timeout_read(self.read_timeout);
        if self.tls_ca_path.is_some() {
            builder = builder.tls_config(std::sync::Arc::new(self.rustls_config()?));
        }
        Ok(builder.build())
    }

    /// Build a rustls `ClientConfig` for the per-endpoint TLS policy. Uses the SAME `ring`
    /// crypto provider ureq already links (no aws-lc / no C toolchain — the Pi-tier
    /// pure-Rust contract). Certificate and hostname verification are mandatory; the
    /// configured PEM CA bundle augments the standard webpki roots.
    fn rustls_config(&self) -> Result<rustls::ClientConfig, String> {
        let provider = std::sync::Arc::new(rustls::crypto::ring::default_provider());
        let mut roots = rustls::RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        if let Some(path) = &self.tls_ca_path {
            use rustls::pki_types::{pem::PemObject, CertificateDer};

            let pem = std::fs::read(path)
                .map_err(|_| "nl-query: TLS CA bundle unavailable".to_string())?;
            for cert in CertificateDer::pem_slice_iter(&pem) {
                let cert = cert.map_err(|_| "nl-query: TLS CA bundle invalid".to_string())?;
                roots
                    .add(cert)
                    .map_err(|_| "nl-query: TLS CA bundle invalid".to_string())?;
            }
        }
        Ok(rustls::ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(|e| format!("nl-query: rustls protocol versions: {e}"))?
            .with_root_certificates(roots)
            .with_no_client_auth())
    }

    /// Resolve the bearer to present to the LLM endpoint: a freshly-minted (cached) OAuth2
    /// client-credentials token when `oauth2` is configured, else the static `api_key`
    /// (empty ⇒ no `Authorization` header for a local keyless endpoint).
    fn resolve_bearer(&self) -> Result<Option<String>, String> {
        if let Some(oauth) = &self.oauth2 {
            return self.mint_bearer(oauth).map(Some);
        }
        if self.api_key.is_empty() {
            Ok(None)
        } else {
            Ok(Some(self.api_key.clone()))
        }
    }

    /// Return a cached OAuth2 bearer, exchanging client credentials at the token endpoint
    /// when the cache is empty or within 60s of expiry. Presents the credentials via HTTP
    /// Basic (`client_secret_basic`) or in the body (`client_secret_post`) per `auth_style`.
    /// Never logs the token or secret.
    fn mint_bearer(&self, oauth: &OAuth2ClientCredentials) -> Result<String, String> {
        use std::io::Read;
        {
            let cached = self
                .token_cache
                .lock()
                .map_err(|_| "nl-query: token cache lock poisoned".to_string())?;
            if let Some((tok, expiry)) = cached.as_ref() {
                if std::time::Instant::now() < *expiry {
                    return Ok(tok.clone());
                }
            }
        }

        let agent = self.build_agent()?;
        let mut req = agent
            .post(&oauth.token_url)
            .set("content-type", "application/x-www-form-urlencoded");
        let mut form: Vec<(&str, &str)> = vec![("grant_type", "client_credentials")];
        // Built outside the match so the `&str` pushed into `form` outlives the send.
        let basic_header;
        match oauth.auth_style {
            TokenAuthStyle::Basic => {
                use base64::Engine as _;
                let raw = format!("{}:{}", oauth.client_id, oauth.client_secret);
                basic_header = format!(
                    "Basic {}",
                    base64::engine::general_purpose::STANDARD.encode(raw)
                );
                req = req.set("authorization", &basic_header);
            }
            TokenAuthStyle::Body => {
                form.push(("client_id", &oauth.client_id));
                form.push(("client_secret", &oauth.client_secret));
            }
        }
        if let Some(scope) = oauth.scope.as_deref() {
            form.push(("scope", scope));
        }

        let resp = req.send_form(&form).map_err(|e| {
            format!(
                "nl-query: OAuth2 token POST {} failed: {e}",
                oauth.token_url
            )
        })?;
        // SAFETY: cap the token-endpoint response like the chat response (OOM guard).
        let mut buf = String::new();
        resp.into_reader()
            .take(self.max_response_bytes)
            .read_to_string(&mut buf)
            .map_err(|e| format!("nl-query: read OAuth2 token response: {e}"))?;
        let json: serde_json::Value = serde_json::from_str(&buf)
            .map_err(|e| format!("nl-query: parse OAuth2 token JSON: {e}"))?;
        let token = json
            .get("access_token")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                format!(
                    "nl-query: OAuth2 token response from {} had no access_token",
                    oauth.token_url
                )
            })?
            .to_string();
        // Renew 60s before the real expiry; default 300s TTL when the IdP omits expires_in.
        let ttl = json
            .get("expires_in")
            .and_then(|v| v.as_u64())
            .unwrap_or(300);
        let lead = ttl.saturating_sub(60).max(1);
        let expiry = std::time::Instant::now() + std::time::Duration::from_secs(lead);
        if let Ok(mut cache) = self.token_cache.lock() {
            *cache = Some((token.clone(), expiry));
        }
        Ok(token)
    }
}

#[cfg(feature = "nl-query")]
impl NlPlanner for UreqNlPlanner {
    fn plan(&self, nl: &str, schema_hint: &str) -> Result<String, String> {
        use std::io::Read;

        // Bounded, timeout-guarded agent with mandatory certificate and hostname
        // verification (SAFETY: no hang on a slow endpoint).
        let agent = self.build_agent()?;

        let user = if schema_hint.trim().is_empty() {
            format!("Question: {nl}")
        } else {
            format!("Schema hint:\n{schema_hint}\n\nQuestion: {nl}")
        };
        let body = serde_json::json!({
            "model": self.model,
            "temperature": 0,
            "messages": [
                { "role": "system", "content": self.system_prompt },
                { "role": "user", "content": user },
            ],
        });
        let body =
            serde_json::to_string(&body).map_err(|e| format!("nl-query: encode request: {e}"))?;

        let mut req = agent
            .post(&self.endpoint)
            .set("content-type", "application/json");
        // Static headers (e.g. a gateway client-id) — sent regardless of the auth mode.
        for (name, value) in &self.headers {
            req = req.set(name, value);
        }
        // Bearer: a minted+cached OAuth2 token when configured, else the static api_key.
        if let Some(bearer) = self.resolve_bearer()? {
            req = req.set("authorization", &format!("Bearer {bearer}"));
        }
        let resp = req
            .send_string(&body)
            .map_err(|e| format!("nl-query: LLM POST {} failed: {e}", self.endpoint))?;

        // SAFETY: cap the bytes read so a hostile/huge response cannot OOM the engine.
        let mut buf = String::new();
        resp.into_reader()
            .take(self.max_response_bytes)
            .read_to_string(&mut buf)
            .map_err(|e| format!("nl-query: read LLM response: {e}"))?;

        let json: serde_json::Value =
            serde_json::from_str(&buf).map_err(|e| format!("nl-query: parse LLM JSON: {e}"))?;
        let content = json
            .pointer("/choices/0/message/content")
            .and_then(|v| v.as_str())
            .ok_or("nl-query: LLM response had no choices[0].message.content")?;
        let query = strip_query(content);
        if query.is_empty() {
            return Err("nl-query: LLM produced an empty query".to_string());
        }
        Ok(query)
    }
}

/// Strip markdown code fences + surrounding whitespace from an LLM answer, leaving the
/// bare query. Handles ```` ```uql … ``` ````, ```` ``` … ``` ```` and no-fence output.
#[cfg(feature = "nl-query")]
fn strip_query(raw: &str) -> String {
    let t = raw.trim();
    let t = t.strip_prefix("```").unwrap_or(t);
    // Drop a leading language tag line (e.g. "uql", "sql") that follows the opening fence.
    let t = match t.split_once('\n') {
        Some((first, rest)) if !first.contains(' ') && first.len() <= 8 && !first.is_empty() => {
            rest
        }
        _ => t,
    };
    let t = t.strip_suffix("```").unwrap_or(t);
    t.trim().to_string()
}

#[cfg(all(test, feature = "nl-query"))]
mod tests {
    use super::*;
    use crate::exec::PlanCtx;

    /// A deterministic mock planner: NL in → a CANNED query out. Proves the EG-078 seam
    /// with NO LLM/network, exactly the shape the engine core drives.
    struct MockPlanner {
        canned: String,
    }
    impl NlPlanner for MockPlanner {
        fn plan(&self, _nl: &str, _hint: &str) -> Result<String, String> {
            Ok(self.canned.clone())
        }
    }

    // ── EG-FEDERATED-QUERY-R058.1: the typed NL→UQL disclosure model ──────────────────

    /// Plan-only mode (EG-FEDERATED-QUERY-R058) returns the typed candidate UQL without
    /// ever executing it. [`plan_only`] takes no `PlanCtx`/view at all, so this is a
    /// structural guarantee, not just an assertion: there is no data for it to read even
    /// by accident.
    #[test]
    fn r058_plan_only_returns_candidate_without_executing() {
        let planner = MockPlanner {
            canned: "MATCH (:Doc) WHERE year > 2024 |> LIMIT 5".into(),
        };
        let result = plan_only(
            &planner,
            "recent docs",
            "labels: Doc",
            NlQueryBudget::default(),
        )
        .expect("valid candidate must be accepted");
        assert_eq!(result.uql, "MATCH (:Doc) WHERE year > 2024 |> LIMIT 5");
        assert!(result.is_plan_only());
        assert!(
            result.rows.is_none(),
            "plan-only mode must never carry data"
        );
    }

    /// The executed result carries the EXACT UQL that ran, alongside the rows
    /// (EG-FEDERATED-QUERY-R058).
    #[test]
    fn r058_executed_result_carries_the_exact_uql_that_ran() {
        let fx = crate::fixture::build();
        let ctx = PlanCtx::new(&fx.view, &fx.semantic);
        let planner = MockPlanner {
            canned: "MATCH (:Doc) WHERE year > 2024 |> LIMIT 5".into(),
        };
        let result =
            plan_and_execute_typed(&planner, "recent docs", "", NlQueryBudget::default(), &ctx)
                .expect("plan+execute ok");
        assert_eq!(result.uql, "MATCH (:Doc) WHERE year > 2024 |> LIMIT 5");
        assert!(!result.is_plan_only());
        let rows = result.rows.expect("executed result must carry rows");
        assert!(rows.id_set().contains("d1"));
    }

    /// Planner output that fails to parse is refused with a typed `ParseFailed` error —
    /// never silently run and never a panic (EG-FEDERATED-QUERY-R058).
    #[test]
    fn r058_malformed_planner_output_is_refused_not_executed() {
        let fx = crate::fixture::build();
        let ctx = PlanCtx::new(&fx.view, &fx.semantic);
        let planner = MockPlanner {
            canned: "this is not a query".into(),
        };
        let err = plan_and_execute_typed(&planner, "x", "", NlQueryBudget::default(), &ctx)
            .expect_err("malformed UQL must be refused");
        match err {
            NlQueryError::ParseFailed { query, .. } => {
                assert_eq!(query, "this is not a query");
            }
            other => panic!("expected ParseFailed, got {other:?}"),
        }

        // The plan-only path refuses identically, before ever touching a view.
        let err = plan_only(&planner, "x", "", NlQueryBudget::default())
            .expect_err("malformed UQL must be refused in plan-only mode too");
        assert!(matches!(err, NlQueryError::ParseFailed { .. }));
    }

    /// Planner output that exceeds the configured budget is refused with a typed
    /// `BudgetExceeded` error BEFORE it is ever parsed or executed
    /// (EG-FEDERATED-QUERY-R058).
    #[test]
    fn r058_over_budget_candidate_is_refused_before_parse_or_execute() {
        let fx = crate::fixture::build();
        let ctx = PlanCtx::new(&fx.view, &fx.semantic);
        // Syntactically valid UQL, but padded past a tiny configured budget.
        let huge = format!("MATCH (:Doc) |> LIMIT 1{}", " ".repeat(10_000));
        let planner = MockPlanner { canned: huge };
        let budget = NlQueryBudget { max_uql_len: 64 };

        let err = plan_and_execute_typed(&planner, "x", "", budget, &ctx)
            .expect_err("over-budget output must be refused");
        assert!(matches!(
            err,
            NlQueryError::BudgetExceeded { limit: 64, .. }
        ));

        let err = plan_only(&planner, "x", "", budget)
            .expect_err("over-budget output must be refused in plan-only mode too");
        assert!(matches!(
            err,
            NlQueryError::BudgetExceeded { limit: 64, .. }
        ));
    }

    /// CONCEPT:EG-KG.query.core-query-input — NL → (mock planner) → canned UQL → EXISTING uql::parse + execute
    /// → rows. The seam runs the produced query through the deterministic pipeline.
    #[test]
    fn eg078_mock_planner_nl_to_uql_executes_to_rows() {
        let fx = crate::fixture::build();
        let ctx = PlanCtx::new(&fx.view, &fx.semantic);
        let planner = MockPlanner {
            canned: "MATCH (:Doc) WHERE year > 2024 |> LIMIT 5".into(),
        };
        let rows = plan_and_execute(&planner, "recent docs please", "labels: Doc", &ctx)
            .expect("plan+execute ok");
        let ids = rows.id_set();
        // Doc nodes with year > 2024 are d1,d2,d5 (all 2025). d3(2023)/d4(2024)/old(2020)
        // are excluded and t1 is a Tool — proving the produced query really executed.
        assert!(ids.contains("d1") && ids.contains("d2") && ids.contains("d5"));
        assert!(!ids.contains("d3") && !ids.contains("d4") && !ids.contains("old"));
        assert!(!ids.contains("t1"));
    }

    /// CONCEPT:EG-KG.query.core-query-input — the engine core takes `Option<&dyn NlPlanner>`; a `None` planner
    /// is a NO-OP (`Ok(None)`), so the NL feature is inert when unconfigured.
    #[test]
    fn eg078_none_planner_is_noop() {
        let fx = crate::fixture::build();
        let ctx = PlanCtx::new(&fx.view, &fx.semantic);
        let out = plan_and_execute_opt(None, "anything at all", "", &ctx).expect("ok");
        assert!(out.is_none(), "a None planner must be a no-op");
    }

    /// CONCEPT:EG-KG.query.core-query-input — a `Some(planner)` path yields `Some(rows)` (the mirror of the
    /// no-op case), confirming the Option seam threads the executed result through.
    #[test]
    fn eg078_some_planner_yields_rows() {
        let fx = crate::fixture::build();
        let ctx = PlanCtx::new(&fx.view, &fx.semantic);
        let planner = MockPlanner {
            canned: "MATCH (:Doc) WHERE year > 2024 |> LIMIT 5".into(),
        };
        let out = plan_and_execute_opt(Some(&planner), "recent docs", "", &ctx).expect("ok");
        assert!(out.map(|r| r.len()).unwrap_or(0) >= 3);
    }

    /// CONCEPT:EG-KG.query.fence-stripper — a planner that emits INVALID UQL surfaces a clean parse error
    /// (never a panic), so a bad model answer is a graceful failure.
    #[test]
    fn eg080_invalid_uql_from_planner_is_clean_error() {
        let fx = crate::fixture::build();
        let ctx = PlanCtx::new(&fx.view, &fx.semantic);
        let planner = MockPlanner {
            canned: "this is not a query".into(),
        };
        let err = plan_and_execute(&planner, "x", "", &ctx).expect_err("must be an error");
        assert!(!err.is_empty());
    }

    /// CONCEPT:EG-KG.query.fence-stripper — the fence-stripper recovers a bare query from a fenced answer.
    #[test]
    fn eg080_strip_query_unwraps_code_fences() {
        assert_eq!(
            strip_query("```uql\nMATCH (:Doc) |> LIMIT 1\n```"),
            "MATCH (:Doc) |> LIMIT 1"
        );
        assert_eq!(
            strip_query("  MATCH (:Doc) |> LIMIT 1  "),
            "MATCH (:Doc) |> LIMIT 1"
        );
        assert_eq!(strip_query("```\nMATCH (:Doc)\n```"), "MATCH (:Doc)");
    }

    fn planner() -> UreqNlPlanner {
        UreqNlPlanner::new(
            "http://vllm.local/v1/chat/completions".into(),
            "qwen".into(),
            String::new(),
        )
    }

    /// The default OAuth2 token-endpoint auth style is body params (client_secret_post).
    #[test]
    fn token_auth_style_defaults_to_body() {
        assert_eq!(TokenAuthStyle::default(), TokenAuthStyle::Body);
    }

    /// The fluent builders record static headers, a verified custom CA, and the oauth2 source.
    #[test]
    fn builders_set_auth_tls_and_headers() {
        let p = planner()
            .with_headers(vec![("X-Client-Id".into(), "svc-42".into())])
            .with_tls_ca_path(Some("test-fixtures/internal-ca.pem".into()))
            .with_oauth2(Some(OAuth2ClientCredentials {
                token_url: "https://idp/token".into(),
                client_id: "cid".into(),
                client_secret: "sec".into(),
                scope: Some("api://x/.default".into()),
                auth_style: TokenAuthStyle::Basic,
            }));
        assert_eq!(
            p.headers,
            vec![("X-Client-Id".to_string(), "svc-42".to_string())]
        );
        assert_eq!(
            p.tls_ca_path.as_deref(),
            Some("test-fixtures/internal-ca.pem")
        );
        assert!(p.oauth2.is_some());
    }

    /// A static api_key is presented as the bearer; a keyless planner presents none.
    #[test]
    fn resolve_bearer_uses_static_api_key_or_none() {
        let keyless = planner();
        assert_eq!(keyless.resolve_bearer().unwrap(), None);

        let keyed = UreqNlPlanner::new("http://x/v1".into(), "m".into(), "tok-123".into());
        assert_eq!(keyed.resolve_bearer().unwrap(), Some("tok-123".to_string()));
    }

    /// A non-existent CA bundle path fails loudly at config-build time (not a silent skip).
    #[test]
    fn tls_ca_missing_file_is_error() {
        let missing = std::env::temp_dir()
            .join("epistemic-graph-test-missing-ca-bundle.pem")
            .to_string_lossy()
            .into_owned();
        let p = planner().with_tls_ca_path(Some(missing));
        assert!(p.rustls_config().is_err());
    }

    /// With neither TLS override the agent still builds (default webpki verification path).
    #[test]
    fn default_tls_builds_agent() {
        assert!(planner().build_agent().is_ok());
    }
}
