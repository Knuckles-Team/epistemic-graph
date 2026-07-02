//! Natural-language → query planning seam (CONCEPT:EG-078) + a concrete, LLM-optional
//! planner (CONCEPT:EG-080).
//!
//! ## The seam (CONCEPT:EG-078)
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
//! ## The standalone planner (CONCEPT:EG-080)
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

/// The NL→query planning seam (CONCEPT:EG-078): turn a natural-language request plus a
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

/// CONCEPT:EG-078 — the deterministic seam. Run `planner` to get a UQL query string,
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

/// CONCEPT:EG-078 — the LLM-OPTIONAL entry point the engine core calls. With `None` the
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

// ── The concrete standalone planner (CONCEPT:EG-080, feature `nl-query`) ─────────────

/// A concrete [`NlPlanner`] (CONCEPT:EG-080) that asks an OpenAI-compatible
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
        }
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
}

#[cfg(feature = "nl-query")]
impl NlPlanner for UreqNlPlanner {
    fn plan(&self, nl: &str, schema_hint: &str) -> Result<String, String> {
        use std::io::Read;

        // Bounded, timeout-guarded agent (SAFETY: no hang on a slow endpoint).
        let agent = ureq::AgentBuilder::new()
            .timeout_connect(self.connect_timeout)
            .timeout_read(self.read_timeout)
            .build();

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
        if !self.api_key.is_empty() {
            req = req.set("authorization", &format!("Bearer {}", self.api_key));
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

    /// CONCEPT:EG-078 — NL → (mock planner) → canned UQL → EXISTING uql::parse + execute
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

    /// CONCEPT:EG-078 — the engine core takes `Option<&dyn NlPlanner>`; a `None` planner
    /// is a NO-OP (`Ok(None)`), so the NL feature is inert when unconfigured.
    #[test]
    fn eg078_none_planner_is_noop() {
        let fx = crate::fixture::build();
        let ctx = PlanCtx::new(&fx.view, &fx.semantic);
        let out = plan_and_execute_opt(None, "anything at all", "", &ctx).expect("ok");
        assert!(out.is_none(), "a None planner must be a no-op");
    }

    /// CONCEPT:EG-078 — a `Some(planner)` path yields `Some(rows)` (the mirror of the
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

    /// CONCEPT:EG-080 — a planner that emits INVALID UQL surfaces a clean parse error
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

    /// CONCEPT:EG-080 — the fence-stripper recovers a bare query from a fenced answer.
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
}
