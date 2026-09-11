//! SPARQL request extraction and execution routing.

use std::sync::Arc;

use crate::server::http1::HttpMessage;
use crate::server::ServerState;

use super::{parse_form, run_query, signed_request, DEFAULT_GRAPH_ENV, SPARQL_HTTP_UPDATE_EVENT};

enum InputError {
    UnsupportedMediaType,
    MethodNotAllowed,
}

/// Execute a request already classified as the `/sparql` route.
pub(super) async fn handle_sparql(
    state: &Arc<tokio::sync::RwLock<ServerState>>,
    bearer: &Option<crate::server::auth::BearerCredential>,
    req: &HttpMessage,
    query_string: &str,
    content_type: &str,
    body: &str,
) -> (&'static str, &'static str, String) {
    let params = parse_form(query_string);
    let (is_update, text) = match parse_input(&req.method, content_type, body, &params) {
        Ok(input) => input,
        Err(InputError::UnsupportedMediaType) => {
            return (
                "415 Unsupported Media Type",
                "text/plain",
                "use application/sparql-query, application/sparql-update or form-encoded"
                    .to_string(),
            )
        }
        Err(InputError::MethodNotAllowed) => {
            return ("405 Method Not Allowed", "text/plain", "method".to_string())
        }
    };

    if text.trim().is_empty() {
        return (
            "400 Bad Request",
            "text/plain",
            "empty query/update".to_string(),
        );
    }

    let default_graph = params
        .get("default-graph-uri")
        .cloned()
        .or_else(|| std::env::var(DEFAULT_GRAPH_ENV).ok())
        .unwrap_or_else(|| "__commons__".to_string());

    if is_update {
        execute_update(state, req, default_graph, text).await
    } else {
        let fmt_override = params
            .get("output")
            .or_else(|| params.get("format"))
            .map(|s| s.as_str());
        run_query(
            state,
            &text,
            &default_graph,
            req.header("accept"),
            fmt_override,
            bearer.as_ref(),
            req.header("authorization"),
        )
        .await
    }
}

fn parse_input(
    method: &str,
    content_type: &str,
    body: &str,
    params: &std::collections::HashMap<String, String>,
) -> Result<(bool, String), InputError> {
    match method {
        "GET" => Ok((false, params.get("query").cloned().unwrap_or_default())),
        "POST" if content_type.contains("application/sparql-update") => {
            Ok((true, body.to_string()))
        }
        "POST" if content_type.contains("application/sparql-query") => {
            Ok((false, body.to_string()))
        }
        "POST" if content_type.contains("application/x-www-form-urlencoded") => {
            let form = parse_form(body);
            Ok(match form.get("update") {
                Some(update) => (true, update.clone()),
                None => (false, form.get("query").cloned().unwrap_or_default()),
            })
        }
        "POST" => Err(InputError::UnsupportedMediaType),
        _ => Err(InputError::MethodNotAllowed),
    }
}

async fn execute_update(
    state: &Arc<tokio::sync::RwLock<ServerState>>,
    req: &HttpMessage,
    default_graph: String,
    text: String,
) -> (&'static str, &'static str, String) {
    let method = crate::protocol::Method::ApplyMutation {
        event_type: SPARQL_HTTP_UPDATE_EVENT.to_string(),
        query: text,
    };
    let request = match signed_request(req, default_graph, method) {
        Ok(request) => request,
        Err(error) => return ("401 Unauthorized", "text/plain", error),
    };
    let response = crate::server::dispatch::dispatch(state, request).await;
    match response.error {
        None => ("204 No Content", "text/plain", String::new()),
        Some(error) if error.starts_with("ACCESS_DENIED") => ("403 Forbidden", "text/plain", error),
        Some(error) => ("400 Bad Request", "text/plain", error),
    }
}
