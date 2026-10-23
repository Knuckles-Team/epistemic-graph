//! EG-FEDERATED-QUERY-R722.1: the pack source model.

use eg_types::connector_pack::{
    class_matches, PackFieldBinding, PackPagination, PackSourceApproval, PackSourceKind,
    PackSourceRegistration, VirtualClassQuery,
};

fn registration() -> PackSourceRegistration {
    PackSourceRegistration {
        connector: "github-mcp".into(),
        contract_iri: "contract://github-mcp#issues".into(),
        mapping_iri: "vg://github-mcp/Issue".into(),
        resource_uri: "data://github-mcp/issues".into(),
        source_kind: PackSourceKind::McpTool,
        operation: "github_issues".into(),
        class_iri: "https://knuckles-team.github.io/ontology/github#Issue".into(),
        key_field: "number".into(),
        fields: vec![PackFieldBinding {
            field: "title".into(),
            property_iri: "https://schema.org/name".into(),
        }],
        pagination: PackPagination::Page,
        key_filter_pushdown: false,
        read_only: true,
        credential_ref: Some("openbao://kv/github-mcp/token".into()),
        contract_digest: "sha256:aa".into(),
        approval: PackSourceApproval::Held,
    }
}

// spec: EG-FEDERATED-QUERY-R722.1
#[test]
fn access_kinds_map_to_foreign_source_kinds() {
    for (access, foreign) in [
        ("mcp_tool", "mcp"),
        ("http_endpoint", "api"),
        ("graphql_query", "graphql"),
        ("a2a_skill", "a2a"),
        ("virtual_graph", "virtual_graph"),
    ] {
        let kind = PackSourceKind::parse(access).expect(access);
        assert_eq!(kind.as_str(), access);
        assert_eq!(kind.foreign_kind(), foreign);
    }
    assert_eq!(PackSourceKind::parse("sql_dsn"), None);

    let reg = registration();
    let wire = rmp_serde::to_vec_named(&reg).unwrap();
    let back: PackSourceRegistration = rmp_serde::from_slice(&wire).unwrap();
    assert_eq!(back, reg);
    assert_eq!(back.approval, PackSourceApproval::Held);
    reg.validate()
        .expect("a read-only contract with a reference registers");
}

// spec: EG-FEDERATED-QUERY-R722.1
#[test]
fn literal_secret_and_write_operation_are_refused_by_code() {
    let mut secret = registration();
    secret.credential_ref = Some("ghp_literal_token".into());
    assert_eq!(secret.validate().unwrap_err().code(), "CONTRACT_SECRET");

    let mut write = registration();
    write.read_only = false;
    assert_eq!(
        write.validate().unwrap_err().code(),
        "SOURCE_WRITE_OPERATION"
    );

    let mut unbound = registration();
    unbound.fields.clear();
    assert_eq!(unbound.validate().unwrap_err().code(), "INVALID_ARGUMENT");
}

// spec: EG-FEDERATED-QUERY-R722.1
#[test]
fn bare_class_names_match_the_local_name_and_bounds_clamp() {
    let iri = "https://knuckles-team.github.io/ontology/github#Issue";
    assert!(class_matches(iri, "Issue"));
    assert!(class_matches(iri, iri));
    assert!(!class_matches(iri, "Incident"));
    assert!(!class_matches(iri, "https://other.example/ontology#Issue"));

    let mut query = VirtualClassQuery::new("Issue");
    query.max_pages_per_source = 0;
    query.page_size = 1_000_000;
    let bounded = query.bounded();
    assert_eq!(bounded.max_pages_per_source, 1);
    assert_eq!(
        bounded.page_size,
        eg_types::connector_pack::pack_source::MAX_SOURCE_PAGE_SIZE
    );
}
