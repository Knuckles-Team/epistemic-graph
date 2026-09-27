//! EH-509: tenant-bound, RLS-filtered section retrieval over persisted graph rows.
//!
//! `DocumentSection` nodes carry only section metadata. The request supplies a
//! document identity and search terms, never evidence or a tenant assertion.

use eg_query::document_retrieval::{retrieve_sections, Section};
use eg_types::protocol::{DocumentSectionCitation, DocumentSectionRetrieval};

use super::*;

const MAX_QUERY_CHARS: usize = 4_096;

pub(crate) async fn handle_retrieve_document_sections(
    ctx: &QueryHandlerCtx<'_>,
    document_id: String,
    query: String,
    top_k: usize,
    beam_width: usize,
) -> Response {
    let Some(carrier) = ctx.read_authority.and_then(GraphReadAuthority::carrier) else {
        crate::metrics::access_denied();
        return Response::err(
            ctx.req_id,
            "ACCESS_DENIED: verified tenant carrier required",
        );
    };
    if !carrier.can_read() {
        crate::metrics::access_denied();
        return Response::err(ctx.req_id, "ACCESS_DENIED: kg:read scope required");
    }
    if document_id.trim().is_empty()
        || document_id.chars().count() > 256
        || query.chars().count() > MAX_QUERY_CHARS
        || top_k == 0
        || top_k > 100
        || beam_width == 0
        || beam_width > 64
    {
        return Response::err(ctx.req_id, "invalid document retrieval bounds");
    }
    let Some(authority) = ctx.read_authority else {
        crate::metrics::access_denied();
        return Response::err(
            ctx.req_id,
            "ACCESS_DENIED: verified read authority required",
        );
    };
    // Filter the graph before examining document identity or section metadata;
    // hidden nodes must not affect result cardinality, ranking, or errors.
    let mut view = ctx.core.analysis_snapshot();
    authority.filter_view(&mut view);
    let tenant = carrier.tenant_scope().to_string();
    let req_id = ctx.req_id;
    let result = compute_off_lock(req_id, move || {
        retrieve_from_view(&view, &tenant, &document_id, &query, top_k, beam_width)
    })
    .await;
    match result {
        Ok(Ok(body)) => result_response::<query_results::RetrieveDocumentSections>(req_id, &body),
        Ok(Err(message)) => Response::err(req_id, message),
        Err(response) => response,
    }
}

fn retrieve_from_view(
    view: &crate::graph::GraphView,
    tenant: &str,
    document_id: &str,
    query: &str,
    top_k: usize,
    beam_width: usize,
) -> Result<DocumentSectionRetrieval, String> {
    // Read the active tree marker from the Document in this same authorized
    // snapshot. A prior write can leave old section IDs behind when a newer
    // tree has fewer headings; those rows must never re-enter retrieval.
    let document = view
        .node_row_object(document_id)
        .ok_or_else(|| "document section tree unavailable".to_string())?;
    if document
        .get("node_type")
        .and_then(serde_json::Value::as_str)
        != Some("Document")
        && document.get("type").and_then(serde_json::Value::as_str) != Some("Document")
    {
        return Err("document section tree unavailable".to_string());
    }
    let tree_version = document
        .get("section_tree_version")
        .and_then(serde_json::Value::as_str)
        .filter(|value| value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit()))
        .ok_or_else(|| "document section tree unavailable".to_string())?;
    let mut sections = Vec::new();
    for node_id in view.node_map.keys() {
        let Some(row) = view.node_row_object(node_id) else {
            continue;
        };
        if row.get("type").and_then(serde_json::Value::as_str) != Some("DocumentSection")
            || row.get("document_id").and_then(serde_json::Value::as_str) != Some(document_id)
            || row.get("tenant_scope").and_then(serde_json::Value::as_str) != Some(tenant)
            || row
                .get("section_tree_version")
                .and_then(serde_json::Value::as_str)
                != Some(tree_version)
        {
            continue;
        }
        sections.push(section_from_row(node_id, document_id, &row)?);
        if sections.len() > 4_096 {
            return Err("document retrieval section limit exceeded".to_string());
        }
    }
    sections.sort_by(|a, b| {
        a.char_start
            .cmp(&b.char_start)
            .then(a.node_id.cmp(&b.node_id))
    });
    let hits = retrieve_sections(document_id, &sections, query, top_k, beam_width)
        .map_err(|error| format!("invalid document section tree: {error:?}"))?;
    Ok(DocumentSectionRetrieval {
        document_id: document_id.to_string(),
        citations: hits
            .into_iter()
            .map(|hit| DocumentSectionCitation {
                node_id: hit.node_id,
                title: hit.title,
                score: hit.score,
                char_start: hit.char_start,
                char_end: hit.char_end,
                page_start: hit.page_start,
                page_end: hit.page_end,
                path: hit.path,
            })
            .collect(),
    })
}

fn section_from_row(
    node_id: &str,
    document_id: &str,
    row: &serde_json::Map<String, serde_json::Value>,
) -> Result<Section, String> {
    let invalid = || "invalid persisted document section metadata".to_string();
    let string = |key| {
        row.get(key)
            .and_then(serde_json::Value::as_str)
            .map(str::to_string)
            .ok_or_else(invalid)
    };
    let page = |key| -> Result<Option<u32>, String> {
        row.get(key)
            .filter(|value| !value.is_null())
            .map(|value| {
                value
                    .as_u64()
                    .and_then(|number| number.try_into().ok())
                    .ok_or_else(invalid)
            })
            .transpose()
    };
    Ok(Section {
        document_id: document_id.to_string(),
        node_id: node_id.to_string(),
        parent_id: row
            .get("parent_id")
            .filter(|value| !value.is_null())
            .map(|value| value.as_str().map(str::to_string).ok_or_else(invalid))
            .transpose()?,
        title: string("title")?,
        summary: string("summary")?,
        char_start: row
            .get("char_start")
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(invalid)?,
        char_end: row
            .get("char_end")
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(invalid)?,
        page_start: page("page_start")?,
        page_end: page("page_end")?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const CURRENT: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const STALE: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    fn add_document(core: &GraphCore, version: Option<&str>) {
        let row = serde_json::json!({
            "node_type": "Document", "section_tree_version": version,
        });
        core.add_node("doc".to_string(), rmp_serde::to_vec_named(&row).unwrap());
    }

    fn add(
        core: &GraphCore,
        id: &str,
        tenant: &str,
        document: &str,
        parent: Option<&str>,
        title: &str,
        start: u64,
        end: u64,
        version: &str,
    ) {
        let row = serde_json::json!({
            "type": "DocumentSection", "tenant_scope": tenant, "document_id": document,
            "parent_id": parent, "title": title, "summary": "", "char_start": start,
            "char_end": end, "page_start": null, "page_end": null,
            "section_tree_version": version,
        });
        core.add_node(id.to_string(), rmp_serde::to_vec_named(&row).unwrap());
    }

    #[test]
    fn citations_use_persisted_ranges_and_tenant_scope() {
        let core = GraphCore::new();
        add_document(&core, Some(CURRENT));
        add(
            &core, "root", "tenant-a", "doc", None, "manual", 0, 200, CURRENT,
        );
        add(
            &core,
            "match",
            "tenant-a",
            "doc",
            Some("root"),
            "refund",
            100,
            150,
            CURRENT,
        );
        add(
            &core, "other", "tenant-b", "doc", None, "refund", 0, 200, CURRENT,
        );
        let result =
            retrieve_from_view(&core.analysis_snapshot(), "tenant-a", "doc", "refund", 3, 3)
                .unwrap();
        assert_eq!(result.citations[0].node_id, "match");
        assert_eq!(
            (result.citations[0].char_start, result.citations[0].char_end),
            (100, 150)
        );
        assert_eq!(result.citations[0].path, ["manual"]);
        assert!(result.citations.iter().all(|hit| hit.node_id != "other"));
    }

    #[test]
    fn missing_parent_fails_closed_instead_of_citing_partial_tree() {
        let core = GraphCore::new();
        add_document(&core, Some(CURRENT));
        add(
            &core,
            "orphan",
            "tenant-a",
            "doc",
            Some("hidden-parent"),
            "refund",
            0,
            10,
            CURRENT,
        );
        assert!(
            retrieve_from_view(&core.analysis_snapshot(), "tenant-a", "doc", "refund", 3, 3)
                .is_err()
        );
    }

    #[test]
    fn rewrite_excludes_stale_sections_before_tree_validation_and_ranking() {
        let core = GraphCore::new();
        add_document(&core, Some(CURRENT));
        add(
            &core, "current", "tenant-a", "doc", None, "refund", 0, 80, CURRENT,
        );
        add(
            &core,
            "stale",
            "tenant-a",
            "doc",
            Some("deleted-parent"),
            "refund",
            0,
            90,
            STALE,
        );
        let result =
            retrieve_from_view(&core.analysis_snapshot(), "tenant-a", "doc", "refund", 5, 3)
                .unwrap();
        assert_eq!(result.citations.len(), 1);
        assert_eq!(result.citations[0].node_id, "current");
    }

    #[test]
    fn document_without_current_tree_marker_fails_closed() {
        let core = GraphCore::new();
        add_document(&core, None);
        add(
            &core, "old", "tenant-a", "doc", None, "refund", 0, 90, CURRENT,
        );
        assert!(
            retrieve_from_view(&core.analysis_snapshot(), "tenant-a", "doc", "refund", 5, 3)
                .is_err()
        );
    }
}
