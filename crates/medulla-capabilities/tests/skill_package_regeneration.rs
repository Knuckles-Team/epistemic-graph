//! Model-level proof for the rendered skill package contract.


use medulla_types::fleet_catalog::RenderedSkillPackage;

#[test]
fn rendered_package_round_trip() {
    let expected = RenderedSkillPackage {
        uri: "skill://triage".into(),
        digest: "sha256:0123456789abcdef".into(),
        ontology_refs: vec!["eg:ontology/task".into(), "eg:ontology/tool".into()],
        graph_revision: 42,
    };
    let encoded = serde_json::to_vec(&expected).expect("encode");
    let decoded: RenderedSkillPackage = serde_json::from_slice(&encoded).expect("decode");
    assert_eq!(decoded, expected);
}
