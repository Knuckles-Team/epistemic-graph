//! Enforce the exact method error set at the served response boundary.

use crate::protocol::Response;

pub(super) fn enforce(method: &str, response: Response) -> Response {
    let Some(code) = response.error.as_deref() else {
        return response;
    };
    if eg_capabilities::error_routing::method_allows_error(method, code) {
        return response;
    }
    tracing::error!(
        method,
        code,
        "served response code absent from method contract"
    );
    Response::err(
        response.id,
        "INTERNAL: method response code is not declared for this operation",
    )
}

#[cfg(test)]
mod tests {
    use super::enforce;
    use crate::protocol::Response;

    #[test]
    fn a_method_emits_only_its_declared_codes() {
        let allowed = enforce(
            "CreateGraph",
            Response::err(1, "ACCESS_DENIED: denied by policy"),
        );
        assert_eq!(allowed.error.as_deref(), Some("ACCESS_DENIED"));
        assert_eq!(allowed.error_detail.as_deref(), Some("denied by policy"));

        let denied = enforce(
            "CreateGraph",
            Response::err(2, "UNSUPPORTED_COALITION: not a graph operation"),
        );
        assert_eq!(denied.error.as_deref(), Some("INTERNAL"));
        assert_eq!(
            denied.error_detail.as_deref(),
            Some("method response code is not declared for this operation")
        );
        assert_eq!(denied.id, 2);

        let repository_limit = enforce(
            "IndexRepository",
            Response::err(5, "REPOSITORY_BATCH_TOO_LARGE: split the batch"),
        );
        assert_eq!(
            repository_limit.error.as_deref(),
            Some("REPOSITORY_BATCH_TOO_LARGE")
        );
        assert_eq!(
            repository_limit.error_detail.as_deref(),
            Some("split the batch")
        );

        let source_limit = enforce(
            "SourceIngest",
            Response::err(6, "SOURCE_INGESTION_BATCH_TOO_LARGE: split the page"),
        );
        assert_eq!(
            source_limit.error.as_deref(),
            Some("SOURCE_INGESTION_BATCH_TOO_LARGE")
        );
        assert_eq!(source_limit.error_detail.as_deref(), Some("split the page"));
    }

    #[test]
    fn an_unknown_method_fails_closed_and_success_is_unchanged() {
        let denied = enforce("NotADeclaredMethod", Response::err(3, "ACCESS_DENIED"));
        assert_eq!(denied.error.as_deref(), Some("INTERNAL"));
        let success = Response::ok(
            4,
            crate::protocol::ResultPayload::Json(serde_json::json!(1)),
        );
        assert!(enforce("CreateGraph", success).error.is_none());
    }
}
