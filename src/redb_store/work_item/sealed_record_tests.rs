//! Store-level proof of sealed-record retirement over a real shard.

use eg_types::sealed_record::{RetireSealedRecordRequest, SealedRecordRetirement};

use super::super::test_shard::{open, with_nodes, GRAPH};
use super::*;

fn request(digest: &str) -> RetireSealedRecordRequest {
    RetireSealedRecordRequest {
        tenant: "tenant-a".into(),
        node_id: "snap".into(),
        digest: digest.into(),
        reason: "share revoked".into(),
        retired_at_ms: 7,
        idempotency_key: format!("retire:{digest}"),
        retired_by: "principal:sha256:actor".into(),
    }
}

fn retired(shard: &Shard, tag: &str, request: RetireSealedRecordRequest) -> SealedRecordRetirement {
    match with_nodes(shard, tag, |nodes| {
        apply_retire_sealed_record_row(GRAPH, &request, nodes, DurableCrypto::none())
    }) {
        Some(crate::protocol::ResultPayload::Json(value)) => serde_json::from_value(value).unwrap(),
        other => panic!("retirement answers JSON, got {other:?}"),
    }
}

fn stored(shard: &Shard, tag: &str) -> Option<NodeRow> {
    with_nodes(shard, tag, |nodes| {
        Ok(nodes
            .get((GRAPH, "snap"))?
            .map(|value| decode_durable::<NodeRow>(value.value()).unwrap()))
    })
}

#[test]
fn retirement_replaces_the_record_by_its_tombstone_in_the_same_row() {
    use eg_types::sealed_record::SealedRecordRetireOutcome as Outcome;

    let temp = open("sealed-retire");
    let record = serde_json::json!({
        "type": "AnalysisSnapshot", "analysisDigest": "sha256:ab", "record": "{}",
    });
    let bytes = rmp_serde::to_vec_named(&record).unwrap();
    with_nodes(&temp.shard, "seed", |nodes| {
        nodes
            .insert((GRAPH, "snap"), bytes.as_slice())
            .map(|_| ())
            .map_err(|error| error.to_string())
    });

    let wrong = retired(&temp.shard, "wrong", request("sha256:ff"));
    assert_eq!(wrong.outcome, Outcome::DigestMismatch);
    assert_eq!(stored(&temp.shard, "after-wrong").unwrap()["record"], "{}");

    let done = retired(&temp.shard, "retire", request("sha256:ab"));
    assert_eq!(done.outcome, Outcome::Retired);
    assert_eq!(done.changed_work_item_ids, ["snap"]);
    let row = stored(&temp.shard, "after-retire").unwrap();
    assert_eq!(row["type"], "SealedRecordTombstone");
    assert_eq!(row["retired_by"], "principal:sha256:actor");
    assert!(!row.contains_key("record"));

    let again = retired(&temp.shard, "again", request("sha256:ab"));
    assert_eq!(again.outcome, Outcome::AlreadyRetired);
    assert!(again.changed_work_item_ids.is_empty());
}
