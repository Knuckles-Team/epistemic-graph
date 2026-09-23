//! Store-level proof of the tenant-bound WorkItem reads over a real shard:
//! rows are written by the SAME native row writer the transitions use, so the
//! revision the read projects is the one the writer maintains.

use super::*;
use eg_types::work_item_read::WorkItemStatus;

use super::super::test_shard::{open, with_nodes, GRAPH};

fn work_item(tenant: &str, kind: &str, status: &str) -> serde_json::Map<String, serde_json::Value> {
    serde_json::json!({
        "status": status,
        "kind": kind,
        "tenant": tenant,
        "node_type": "WorkItem",
        "updated_at": 2.5,
        "metadata": {"step": 1},
    })
    .as_object()
    .cloned()
    .expect("fixture row is an object")
}

/// Write `rows` through the native WorkItem row writer.
fn write_rows(
    shard: &Shard,
    tag: &str,
    rows: Vec<(&str, serde_json::Map<String, serde_json::Value>)>,
) {
    with_nodes(shard, tag, |nodes| {
        for (id, mut props) in rows {
            write_work_item_props(nodes, GRAPH, id, &mut props, DurableCrypto::none())?;
        }
        Ok(())
    });
}

fn get(shard: &Shard, tenant: &str, id: &str) -> Option<WorkItemView> {
    read_work_item(shard, GRAPH, tenant, id, DurableCrypto::none()).unwrap()
}

fn list(limit: u32, cursor: Option<String>) -> WorkItemListRequest {
    WorkItemListRequest {
        tenant: "tenant-a".to_string(),
        cursor,
        limit,
        kind: None,
        metadata_match: None,
    }
}

#[test]
fn a_point_read_sees_only_its_own_tenant_and_every_write_is_a_new_revision() {
    let temp = open("point");
    write_rows(
        &temp.shard,
        "seed",
        vec![
            ("wi-a", work_item("tenant-a", "au.task", "ready")),
            ("wi-b", work_item("tenant-b", "au.task", "ready")),
        ],
    );
    let view = get(&temp.shard, "tenant-a", "wi-a").expect("own row is visible");
    assert_eq!(view.version, 1);
    assert_eq!(view.updated_at_ms, 2_500);
    assert_eq!(view.metadata["step"], 1);
    assert_eq!(
        get(&temp.shard, "tenant-a", "wi-b"),
        None,
        "cross-tenant read"
    );
    assert_eq!(get(&temp.shard, "tenant-a", "wi-missing"), None);

    // A second native write of the same row advances its revision.
    let mut rewritten = work_item("tenant-a", "au.task", "running");
    rewritten.insert("row_revision".into(), serde_json::json!(view.version));
    write_rows(&temp.shard, "rewrite", vec![("wi-a", rewritten)]);
    let view = get(&temp.shard, "tenant-a", "wi-a").unwrap();
    assert_eq!(view.version, 2);
    assert_eq!(view.status, WorkItemStatus::Running);
}

#[test]
fn listing_pages_resume_by_seek_and_skip_other_tenants_and_node_types() {
    let temp = open("list");
    let mut rows = Vec::new();
    for (id, tenant) in [
        ("wi-1", "tenant-a"),
        ("wi-2", "tenant-b"),
        ("wi-3", "tenant-a"),
        ("wi-4", "tenant-a"),
    ] {
        rows.push((id, work_item(tenant, "au.task", "ready")));
    }
    let mut document = work_item("tenant-a", "au.task", "ready");
    document.insert("node_type".into(), serde_json::json!("Document"));
    rows.push(("wi-0-doc", document));
    write_rows(&temp.shard, "seed", rows);

    let first = list_work_items(&temp.shard, GRAPH, &list(2, None), DurableCrypto::none()).unwrap();
    let ids: Vec<&str> = first
        .items
        .iter()
        .map(|item| item.work_item_id.as_str())
        .collect();
    assert_eq!(ids, ["wi-1", "wi-3"]);
    let cursor = first.next_cursor.clone().expect("a full page resumes");

    let second = list_work_items(
        &temp.shard,
        GRAPH,
        &list(2, Some(cursor.clone())),
        DurableCrypto::none(),
    )
    .unwrap();
    let ids: Vec<&str> = second
        .items
        .iter()
        .map(|item| item.work_item_id.as_str())
        .collect();
    assert_eq!(ids, ["wi-4"]);
    assert_eq!(second.next_cursor, None);

    let mut foreign = list(2, Some(cursor));
    foreign.tenant = "tenant-b".to_string();
    let error = list_work_items(&temp.shard, GRAPH, &foreign, DurableCrypto::none()).unwrap_err();
    assert!(error.contains("not minted for this tenant"), "{error}");
}

#[test]
fn an_outcome_read_returns_only_digest_verified_provenance_of_the_tenants_item() {
    use sha2::{Digest, Sha256};
    let temp = open("outcome");
    let evaluation = rmp_serde::to_vec_named(&serde_json::json!({
        "node_type": "OutcomeEvaluation",
        "status": "succeeded",
    }))
    .unwrap();
    let mut item = work_item("tenant-a", "browser.control.call", "succeeded");
    item.insert("outcome_ref".into(), "oe-1".into());
    item.insert(
        "outcome_digest".into(),
        hex::encode(Sha256::digest(&evaluation)).into(),
    );
    item.insert("trace_ref".into(), "rt-1".into());
    item.insert("tool_call_refs".into(), serde_json::json!(["tc-1"]));
    write_rows(&temp.shard, "item", vec![("wi-o", item)]);
    let receipt = evaluation.clone();
    with_nodes(&temp.shard, "receipt", |nodes| {
        nodes.insert((GRAPH, "oe-1"), receipt.as_slice())?;
        Ok(())
    });

    let read = |tenant: &str| {
        read_work_item_outcome(&temp.shard, GRAPH, tenant, "wi-o", DurableCrypto::none())
    };
    let outcome = read("tenant-a")
        .unwrap()
        .expect("committed outcome is visible");
    assert_eq!(outcome.work_item.work_item_id, "wi-o");
    assert_eq!(
        (outcome.trace_ref.as_str(), outcome.outcome_ref.as_str()),
        ("rt-1", "oe-1")
    );
    assert_eq!(outcome.tool_call_refs, ["tc-1"]);
    assert_eq!(outcome.outcome.unwrap()["status"], "succeeded");
    assert_eq!(read("tenant-b").unwrap(), None, "cross-tenant outcome read");

    let forged = rmp_serde::to_vec_named(&serde_json::json!({"status": "failed"})).unwrap();
    with_nodes(&temp.shard, "forge", |nodes| {
        nodes.insert((GRAPH, "oe-1"), forged.as_slice())?;
        Ok(())
    });
    assert!(read("tenant-a").is_err(), "an altered receipt is refused");
}
