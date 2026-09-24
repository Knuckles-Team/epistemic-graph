use super::*;

fn msgpack(value: serde_json::Value) -> Vec<u8> {
    rmp_serde::to_vec_named(&value).unwrap()
}

fn sealed() -> Vec<u8> {
    msgpack(serde_json::json!({
        "type": "AnalysisSnapshot", "record": "{}", "analysisDigest": "sha256:ab"
    }))
}

fn tombstone() -> Vec<u8> {
    msgpack(serde_json::json!({"type": "SealedRecordTombstone", "digest": "sha256:ab"}))
}

fn replace(properties: Vec<u8>) -> NodeWrite<'static> {
    NodeWrite::Replace("snap".into(), properties.into())
}

fn merge(updates: serde_json::Value) -> NodeWrite<'static> {
    NodeWrite::Merge("snap".into(), msgpack(updates).into())
}

/// Rows keyed by node id, for judging whole `Method`s.
struct Rows(Vec<(&'static str, Vec<u8>)>);

impl StoredNodeRows for Rows {
    fn stored(&self, node_id: &str) -> Result<Option<Vec<u8>>, String> {
        Ok(self
            .0
            .iter()
            .find(|(id, _)| *id == node_id)
            .map(|(_, blob)| blob.clone()))
    }
}

#[test]
fn only_rows_of_a_sealed_class_are_guarded() {
    assert!(row_class(&sealed()).is_some());
    assert!(row_class(&msgpack(serde_json::json!({"type": "Doc"}))).is_none());
    assert!(row_class(&msgpack(serde_json::json!({"type": 3}))).is_none());
    assert!(row_class(b"opaque").is_none());
}

#[test]
fn creating_or_rewriting_identical_content_is_allowed() {
    check_node_write(None, &replace(sealed())).unwrap();
    check_node_write(None, &NodeWrite::Remove("snap".into())).unwrap();
    check_node_write(Some(&sealed()), &replace(sealed())).unwrap();
    check_node_write(Some(&sealed()), &merge(serde_json::json!({"record": "{}"}))).unwrap();
    let ordinary = msgpack(serde_json::json!({"type": "Doc"}));
    check_node_write(Some(&ordinary), &NodeWrite::Remove("snap".into())).unwrap();
}

#[test]
fn changing_or_removing_a_stored_sealed_row_is_refused() {
    let forged = msgpack(serde_json::json!({
        "type": "AnalysisSnapshot", "record": "{\"forged\":1}", "analysisDigest": "sha256:ab"
    }));
    let refusals = [
        replace(forged),
        replace(msgpack(serde_json::json!({"type": "Doc"}))),
        replace(b"opaque".to_vec()),
        merge(serde_json::json!({"record": "{\"forged\":1}"})),
        merge(serde_json::json!({"note": "added"})),
        NodeWrite::Upsert("snap".into(), msgpack(serde_json::json!({"record": "x"}))),
        NodeWrite::Remove("snap".into()),
    ];
    for write in &refusals {
        let error = check_node_write(Some(&sealed()), write).unwrap_err();
        assert!(error.contains("create-only"), "{error}");
        let error = check_node_write(Some(&tombstone()), write).unwrap_err();
        assert!(error.contains("create-only"), "{error}");
    }
}

#[test]
fn a_generic_write_cannot_forge_a_tombstone() {
    let ordinary = msgpack(serde_json::json!({"type": "Doc"}));
    for stored in [None, Some(ordinary.as_slice())] {
        let error = check_node_write(stored, &replace(tombstone())).unwrap_err();
        assert!(error.contains("owning op"), "{error}");
        let turned = merge(serde_json::json!({"type": "SealedRecordTombstone"}));
        assert!(check_node_write(stored, &turned).is_err());
    }
    let create = Method::CreateNodeIfAbsent {
        node_id: "t".into(),
        properties_msgpack: tombstone(),
    };
    assert!(refuse_generic_sealed_write(&create, &Rows(Vec::new())).is_err());
}

#[test]
fn methods_are_judged_per_node_against_the_stored_rows() {
    let rows = Rows(vec![
        ("snap", sealed()),
        ("doc", msgpack(serde_json::json!({"type": "Doc"}))),
    ]);
    let batch = |operations: serde_json::Value| Method::BatchUpdate {
        operations_msgpack: msgpack(operations),
    };
    let allowed = [
        batch(serde_json::json!([
            {"op": "add_node", "id": "doc", "properties": {"type": "Doc", "v": 2}},
            {"op": "remove_node", "id": "other"},
            {"op": "add_edge", "source": "doc", "target": "snap"},
        ])),
        Method::CreateNodeIfAbsent {
            node_id: "snap".into(),
            properties_msgpack: msgpack(
                serde_json::json!({"type": "AnalysisSnapshot", "record": "x"}),
            ),
        },
        Method::RemoveNode {
            node_id: "doc".into(),
        },
    ];
    for method in &allowed {
        refuse_generic_sealed_write(method, &rows).unwrap();
    }
    let refused = [
        batch(serde_json::json!([{"op": "remove_node", "id": "snap"}])),
        batch(serde_json::json!([{"op": "upsert_node", "id": "snap", "properties": {"note": 1}}])),
        Method::RemoveNode {
            node_id: "snap".into(),
        },
    ];
    for method in &refused {
        assert!(
            refuse_generic_sealed_write(method, &rows).is_err(),
            "{method:?}"
        );
    }
}
