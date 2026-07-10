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
//!    hand-written `UnifiedQueryText` — the query language target is UQL (the engine's
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
    /// TLS: accept invalid/self-signed certs for THIS endpoint (danger — internal use).
    tls_insecure: bool,
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
    /// (no prose, no fences), targeting the engine's text front-end grammar.
    pub const DEFAULT_SYSTEM_PROMPT: &'static str = "You translate a natural-language \
question into ONE Unified Query Language (UQL) query for the epistemic-graph engine. \
UQL grammar (stages piped with `|>`):\n  MATCH (:Label) [WHERE field OP value]\n  |> \
TRAVERSE -[:REL]->{min,max}\n  |> RANK BY ~[f0, f1, ...]\n  |> LIMIT n\nOP is one of = \
!= > >= < <=. Use ONLY labels/fields named in the provided schema hint. Reply with the \
UQL query ONLY — no explanation, no markdown code fences.";

    /// Build a planner with the default timeouts (5s connect / 30s read), a 1 MiB
    /// response cap and the [`Self::DEFAULT_SYSTEM_PROMPT`]. `api_key` may be empty for a
    /// local keyless endpoint.
    pub fn new(endpoint: String, model: String, api_key: String) -> Self {
        Self {
            endpoint,
            model,
            api_key,
            connect_timeout: std::time::Duration::from_secs(5),
            read_timeout: std::time::Duration::from_secs(30),
            max_response_bytes: 1024 * 1024,
            system_prompt: Self::DEFAULT_SYSTEM_PROMPT.to_string(),
            headers: Vec::new(),
            tls_insecure: false,
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

    /// Per-endpoint TLS: accept invalid/self-signed certs (danger; internal endpoints) (fluent).
    pub fn with_tls_insecure(mut self, insecure: bool) -> Self {
        self.tls_insecure = insecure;
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
        if self.tls_insecure || self.tls_ca_path.is_some() {
            builder = builder.tls_config(std::sync::Arc::new(self.rustls_config()?));
        }
        Ok(builder.build())
    }

    /// Build a rustls `ClientConfig` for the per-endpoint TLS policy. Uses the SAME `ring`
    /// crypto provider ureq already links (no aws-lc / no C toolchain — the Pi-tier
    /// pure-Rust contract). `tls_insecure` installs a no-verify certificate verifier
    /// (danger, internal endpoints only); otherwise the webpki roots plus any configured
    /// PEM CA bundle are trusted.
    fn rustls_config(&self) -> Result<rustls::ClientConfig, String> {
        let provider = std::sync::Arc::new(rustls::crypto::ring::default_provider());
        if self.tls_insecure {
            return Ok(
                rustls::ClientConfig::builder_with_provider(provider.clone())
                    .with_safe_default_protocol_versions()
                    .map_err(|e| format!("nl-query: rustls protocol versions: {e}"))?
                    .dangerous()
                    .with_custom_certificate_verifier(std::sync::Arc::new(NoCertVerification::new(
                        provider,
                    )))
                    .with_no_client_auth(),
            );
        }
        let mut roots = rustls::RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        if let Some(path) = &self.tls_ca_path {
            let pem = std::fs::read(path)
                .map_err(|e| format!("nl-query: read TLS CA bundle {path}: {e}"))?;
            let mut reader = std::io::BufReader::new(&pem[..]);
            for cert in rustls_pemfile::certs(&mut reader) {
                let cert =
                    cert.map_err(|e| format!("nl-query: parse TLS CA bundle {path}: {e}"))?;
                roots
                    .add(cert)
                    .map_err(|e| format!("nl-query: add TLS CA from {path}: {e}"))?;
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

        // Bounded, timeout-guarded agent with the per-endpoint TLS policy (SAFETY: no hang
        // on a slow endpoint; verification unchanged unless explicitly overridden).
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

/// A rustls certificate verifier that accepts ANY server certificate (CONCEPT:EG-KG.query.nl-tls-insecure).
///
/// DANGER: used ONLY when a `UreqNlPlanner` is explicitly configured `tls_insecure` for a
/// trusted internal endpoint (e.g. a self-signed vLLM on the LAN). It skips chain/hostname
/// validation but still verifies the handshake signatures via the real `ring` provider
/// algorithms, so the TLS session itself is well-formed. Never wired on the default path.
#[cfg(feature = "nl-query")]
#[derive(Debug)]
struct NoCertVerification {
    provider: std::sync::Arc<rustls::crypto::CryptoProvider>,
}

#[cfg(feature = "nl-query")]
impl NoCertVerification {
    fn new(provider: std::sync::Arc<rustls::crypto::CryptoProvider>) -> Self {
        Self { provider }
    }
}

#[cfg(feature = "nl-query")]
impl rustls::client::danger::ServerCertVerifier for NoCertVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
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

    /// The fluent builders record static headers, per-endpoint TLS, and the oauth2 source.
    #[test]
    fn builders_set_auth_tls_and_headers() {
        let p = planner()
            .with_headers(vec![("X-Client-Id".into(), "svc-42".into())])
            .with_tls_insecure(true)
            .with_tls_ca_path(Some("/etc/ssl/internal-ca.pem".into()))
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
        assert!(p.tls_insecure);
        assert_eq!(p.tls_ca_path.as_deref(), Some("/etc/ssl/internal-ca.pem"));
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

    /// The insecure TLS policy builds a rustls config (no-verify verifier) without error.
    #[test]
    fn tls_insecure_builds_client_config() {
        let p = planner().with_tls_insecure(true);
        assert!(p.rustls_config().is_ok());
        assert!(p.build_agent().is_ok());
    }

    /// A non-existent CA bundle path fails loudly at config-build time (not a silent skip).
    #[test]
    fn tls_ca_missing_file_is_error() {
        let p = planner().with_tls_ca_path(Some("/no/such/ca-bundle.pem".into()));
        assert!(p.rustls_config().is_err());
    }

    /// With neither TLS override the agent still builds (default webpki verification path).
    #[test]
    fn default_tls_builds_agent() {
        assert!(planner().build_agent().is_ok());
    }
}
