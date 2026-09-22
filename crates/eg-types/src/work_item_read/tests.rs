use super::*;
use serde_json::json;

fn row(tenant: &str, kind: &str, status: &str) -> Map<String, Value> {
    json!({
        "node_type": "WorkItem",
        "tenant": tenant,
        "kind": kind,
        "status": status,
        "payload_ref": "au-task:sha256:abc",
        "metadata": {"au:description": "summarize"},
        "row_revision": 3,
        "updated_at": 12.3456,
        "lease_owner": "worker-a",
        "lease_epoch": 4,
        "fencing_token": 9,
    })
    .as_object()
    .cloned()
    .expect("fixture row is an object")
}

fn list(limit: u32, cursor: Option<String>, kind: Option<&str>) -> WorkItemListRequest {
    WorkItemListRequest {
        tenant: "tenant-a".to_string(),
        cursor,
        limit,
        kind: kind.map(str::to_string),
    }
}

/// Drive one page the way the store does: skip the cursor row, check the
/// bounds before each row, consume, close.
fn page(rows: &[(String, Map<String, Value>)], request: &WorkItemListRequest) -> WorkItemPage {
    let resume_after = request.resume_after().expect("cursor decodes");
    let mut scan = WorkItemPageScan::new(request);
    for (id, props) in rows {
        if resume_after.as_deref() >= Some(id.as_str()) {
            continue;
        }
        if !scan.admits_another_row() {
            break;
        }
        scan.consume(id, 100, props).expect("row projects");
    }
    scan.finish()
}

fn mixed_rows() -> Vec<(String, Map<String, Value>)> {
    let mut rows = Vec::new();
    for index in 0..7 {
        let tenant = if index % 2 == 0 {
            "tenant-a"
        } else {
            "tenant-b"
        };
        rows.push((format!("wi-{index}"), row(tenant, "au.task", "ready")));
    }
    rows.push(("wi-7".to_string(), row("tenant-a", "other", "running")));
    rows
}

#[test]
fn the_view_carries_no_lease_authority_and_projects_revision_and_time() {
    let view =
        WorkItemView::from_tenant_row("wi-1", &row("tenant-a", "au.task", "leased"), "tenant-a")
            .unwrap()
            .expect("own tenant's WorkItem is visible");
    assert_eq!(view.status, WorkItemStatus::Leased);
    assert_eq!(view.version, 3);
    assert_eq!(view.updated_at_ms, 12_346);
    assert_eq!(view.input_ref, "au-task:sha256:abc");
    let wire = serde_json::to_value(&view).unwrap();
    let mut keys: Vec<&str> = wire
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    keys.sort_unstable();
    assert_eq!(
        keys,
        [
            "input_ref",
            "kind",
            "metadata",
            "status",
            "updated_at_ms",
            "version",
            "work_item_id"
        ]
    );
}

#[test]
fn another_tenants_row_and_a_non_work_item_are_invisible() {
    let foreign = row("tenant-b", "au.task", "ready");
    assert_eq!(
        WorkItemView::from_tenant_row("wi-1", &foreign, "tenant-a").unwrap(),
        None
    );
    let mut plain = row("tenant-a", "au.task", "ready");
    plain.insert("node_type".into(), json!("Document"));
    assert_eq!(
        WorkItemView::from_tenant_row("n-1", &plain, "tenant-a").unwrap(),
        None
    );
    let legacy = row("tenant-a", "au.task", "completed");
    assert!(WorkItemView::from_tenant_row("wi-1", &legacy, "tenant-a").is_err());
}

#[test]
fn a_row_without_a_revision_reads_as_revision_one() {
    let mut unrevised = row("tenant-a", "au.task", "ready");
    unrevised.remove(WORK_ITEM_ROW_REVISION);
    let view = WorkItemView::from_tenant_row("wi-1", &unrevised, "tenant-a")
        .unwrap()
        .unwrap();
    assert_eq!(view.version, 1);
}

#[test]
fn paging_walks_every_own_row_exactly_once_and_ends_without_a_cursor() {
    let rows = mixed_rows();
    let mut seen = Vec::new();
    let mut cursor = None;
    for _ in 0..rows.len() {
        let current = page(&rows, &list(2, cursor.take(), None));
        seen.extend(current.items.into_iter().map(|item| item.work_item_id));
        cursor = current.next_cursor;
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(cursor, None);
    assert_eq!(seen, ["wi-0", "wi-2", "wi-4", "wi-6", "wi-7"]);
}

#[test]
fn a_kind_filter_selects_within_the_same_scan() {
    let only_other = page(&mixed_rows(), &list(100, None, Some("other")));
    let ids: Vec<&str> = only_other
        .items
        .iter()
        .map(|item| item.work_item_id.as_str())
        .collect();
    assert_eq!(ids, ["wi-7"]);
    assert_eq!(only_other.next_cursor, None);
}

#[test]
fn a_cursor_is_bound_to_the_tenant_it_was_minted_for() {
    let first = page(&mixed_rows(), &list(1, None, None));
    let cursor = first.next_cursor.expect("a limited page resumes");
    let mut foreign = list(1, Some(cursor), None);
    foreign.tenant = "tenant-b".to_string();
    let error = foreign.resume_after().unwrap_err();
    assert_eq!(error, "WorkItem list cursor was not minted for this tenant");
}

#[test]
fn the_scan_bound_closes_an_empty_page_with_a_cursor() {
    let rows: Vec<(String, Map<String, Value>)> = (0..MAX_WORK_ITEM_LIST_SCAN + 1)
        .map(|index| (format!("n-{index:05}"), row("tenant-b", "au.task", "ready")))
        .collect();
    let first = page(&rows, &list(100, None, None));
    assert!(first.items.is_empty());
    let cursor = first.next_cursor.expect("an exhausted scan bound resumes");
    let second = page(&rows, &list(100, Some(cursor), None));
    assert!(second.items.is_empty());
    assert_eq!(second.next_cursor, None);
}

#[test]
fn request_bounds_are_enforced() {
    assert!(list(0, None, None).validate().is_err());
    assert!(list(MAX_WORK_ITEM_LIST_LIMIT + 1, None, None)
        .validate()
        .is_err());
    assert!(list(MAX_WORK_ITEM_LIST_LIMIT, None, Some(" "))
        .validate()
        .is_err());
    list(MAX_WORK_ITEM_LIST_LIMIT, None, Some("au.task"))
        .validate()
        .unwrap();
    assert!(validate_work_item_get("tenant-a", "").is_err());
    assert!(validate_work_item_get("", "wi-1").is_err());
    validate_work_item_get("tenant-a", "wi-1").unwrap();
}
