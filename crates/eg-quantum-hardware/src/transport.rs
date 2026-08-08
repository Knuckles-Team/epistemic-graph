//! A thin HTTP-transport seam between provider adapters and the network.
//!
//! Every adapter (`ibm.rs`/`braket.rs`/`azure.rs`) talks to its provider ONLY through
//! [`HttpTransport`], never directly through `reqwest`. Two reasons:
//!
//! 1. **Testability without spending real budget.** This lane must not exercise real
//!    provider endpoints (no credentials are available, and doing so risks consuming
//!    the very trivially-exhausted quotas Q10's charter says to protect). Every test
//!    in this crate instead injects [`MockTransport`], so the full submit/poll/
//!    result/quota-refusal/error-mapping logic is exercised deterministically and
//!    for free.
//! 2. **One place to keep TLS/timeout/retry policy consistent** across three
//!    otherwise-unrelated provider clients.

use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Debug, Clone)]
pub struct HttpResponse {
    pub status: u16,
    pub body: serde_json::Value,
}

#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    #[error("request failed: {0}")]
    Request(String),
    #[error("response body was not valid JSON: {0}")]
    InvalidJson(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Method {
    Get,
    Post,
    Put,
    Delete,
}

/// A request body. Two shapes cover every call this crate's adapters make: JSON
/// (every provider's job-lifecycle API) and form-urlencoded (OAuth2 token endpoints
/// -- IBM Cloud IAM's apikey grant, Azure AD's client-credentials grant -- which
/// neither accepts nor returns a job-shaped JSON request body).
#[derive(Debug, Clone)]
pub enum Body {
    Json(serde_json::Value),
    Form(Vec<(String, String)>),
}

/// One HTTP request, fully materialized (no builder pattern needed -- callers are
/// this crate's own adapters, not external users).
#[derive(Debug, Clone)]
pub struct HttpRequest {
    pub method: Method,
    pub url: String,
    pub headers: Vec<(String, String)>,
    /// `None` for GET/DELETE.
    pub body: Option<Body>,
}

pub trait HttpTransport: Send + Sync {
    fn send(&self, req: HttpRequest) -> Result<HttpResponse, TransportError>;
}

/// Forwarding impl so an `Arc<MockTransport>` (or any other shared transport) is
/// itself a valid [`HttpTransport`] -- lets a test keep an `Arc` clone for
/// inspecting `calls` after handing the other clone to a backend constructor that
/// takes `T: HttpTransport` by value.
impl<X: HttpTransport + ?Sized> HttpTransport for std::sync::Arc<X> {
    fn send(&self, req: HttpRequest) -> Result<HttpResponse, TransportError> {
        (**self).send(req)
    }
}

/// The real, network-backed transport. Only compiled when a provider feature that
/// needs it is on (`ibm`/`braket`/`azure`, all implying `reqwest-transport`).
#[cfg(feature = "reqwest-transport")]
pub struct ReqwestTransport {
    client: reqwest::blocking::Client,
}

#[cfg(feature = "reqwest-transport")]
impl Default for ReqwestTransport {
    fn default() -> Self {
        ReqwestTransport {
            // A conservative fixed timeout: a hung HTTP call must not hang a
            // synchronous `QuantumBackend::run` forever. Individual adapters apply
            // their OWN, typically much longer, polling-loop timeout on top of this
            // per-request ceiling (this is the per-HTTP-call ceiling, not the
            // per-job one).
            client: reqwest::blocking::Client::builder()
                .timeout(std::time::Duration::from_secs(30))
                .build()
                .expect("reqwest client construction with only a timeout set cannot fail"),
        }
    }
}

#[cfg(feature = "reqwest-transport")]
impl HttpTransport for ReqwestTransport {
    fn send(&self, req: HttpRequest) -> Result<HttpResponse, TransportError> {
        let method = match req.method {
            Method::Get => reqwest::Method::GET,
            Method::Post => reqwest::Method::POST,
            Method::Put => reqwest::Method::PUT,
            Method::Delete => reqwest::Method::DELETE,
        };
        let mut builder = self.client.request(method, &req.url);
        for (k, v) in &req.headers {
            builder = builder.header(k, v);
        }
        match &req.body {
            Some(Body::Json(v)) => builder = builder.json(v),
            Some(Body::Form(pairs)) => builder = builder.form(pairs),
            None => {}
        }
        let resp = builder
            .send()
            .map_err(|e| TransportError::Request(e.to_string()))?;
        let status = resp.status().as_u16();
        // A body-less terminal response (e.g. a 204 from DELETE) is valid; treat it
        // as JSON `null` rather than an error.
        let text = resp
            .text()
            .map_err(|e| TransportError::Request(e.to_string()))?;
        let body = if text.trim().is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::from_str(&text).map_err(|e| TransportError::InvalidJson(e.to_string()))?
        };
        Ok(HttpResponse { status, body })
    }
}

/// A canned, in-memory transport for tests: pre-load `(method, url) -> HttpResponse`
/// pairs (or a queue of responses for the same key, for a submit-then-poll-N-times
/// sequence) and every adapter code path exercises for real, deterministically, with
/// zero network access.
#[derive(Default)]
pub struct MockTransport {
    responses: Mutex<HashMap<(Method, String), std::collections::VecDeque<HttpResponse>>>,
    /// Every request actually sent, in order -- assertable by tests (e.g. "did we
    /// send the Authorization header we expected", "did we stop polling once
    /// Completed was returned").
    pub calls: Mutex<Vec<HttpRequest>>,
}

impl MockTransport {
    pub fn new() -> Self {
        Self::default()
    }

    /// Queue a response for one `(method, url)`. Multiple calls with the same key
    /// queue in FIFO order (e.g. `Queued` then `Running` then `Completed` for a poll
    /// loop).
    pub fn push(
        &self,
        method: Method,
        url: impl Into<String>,
        status: u16,
        body: serde_json::Value,
    ) {
        self.responses
            .lock()
            .expect("mock transport mutex poisoned")
            .entry((method, url.into()))
            .or_default()
            .push_back(HttpResponse { status, body });
    }
}

impl HttpTransport for MockTransport {
    fn send(&self, req: HttpRequest) -> Result<HttpResponse, TransportError> {
        let key = (req.method, req.url.clone());
        self.calls
            .lock()
            .expect("mock transport mutex poisoned")
            .push(req);
        let mut guard = self
            .responses
            .lock()
            .expect("mock transport mutex poisoned");
        match guard.get_mut(&key).and_then(|q| q.pop_front()) {
            Some(resp) => Ok(resp),
            None => Err(TransportError::Request(format!(
                "MockTransport: no queued response for {:?}",
                key
            ))),
        }
    }
}
