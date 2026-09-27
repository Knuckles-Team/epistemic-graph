use super::{request_validation_refusal, SemanticIndexError};

#[test]
fn request_validation_separates_authority_from_bad_input_without_echoing_it() {
    let cases = [
        (SemanticIndexError::ApprovalRequired, "ACCESS_DENIED"),
        (
            SemanticIndexError::AuthorizationContextMismatch,
            "ACCESS_DENIED",
        ),
        (
            SemanticIndexError::InvalidField {
                field: "private-field".into(),
                reason: "private-value".into(),
            },
            "INVALID_ARGUMENT",
        ),
    ];
    for (error, code) in cases {
        let refusal = request_validation_refusal(&error);
        assert!(refusal.starts_with(code));
        assert!(!refusal.contains("private"));
        assert!(eg_capabilities::error_routing::method_allows_error(
            "SemanticIndex",
            code
        ));
    }
}
