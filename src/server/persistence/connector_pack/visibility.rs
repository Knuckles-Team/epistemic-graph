//! The readiness gate of a connector's pack (PB1, pack design §7.2 alternative 2).
//!
//! An import commits its component revisions at once, but a pack that carries
//! ontologies or shapes is not USABLE until its graph projection has applied:
//! a reader must see either the previous pack with its projection or the new
//! pack with its projection, never new components next to the old schema. The
//! head therefore carries `visible_record_id` beside `record_id`, and every
//! pack-owned component (`mcp:<connector>/...`) is visible to search and to
//! new pins exactly while its connector's head is visible
//! (`visible_record_id == record_id`). The flip happens in the projection's
//! own Agent Library commit, so visibility and the projected graph version are
//! one fact.
//!
//! Existing pins keep resolving (history is never hidden); only NEW pins and
//! search are gated, which is the design's "connector is dark during its
//! projection window".

use eg_types::connector_pack::PACK_COMPONENT_ID_PREFIX;

use super::ConnectorPackHeadRow;

/// Refusal code for a new pin on a pack component whose projection has not
/// applied yet.
pub(crate) const PACK_REVISION_NOT_VISIBLE: &str = "PACK_REVISION_NOT_VISIBLE";

/// The connector a pack-owned component id belongs to, or `None` for a
/// component no pack owns.
pub(crate) fn pack_connector(component_id: &str) -> Option<&str> {
    component_id
        .strip_prefix(PACK_COMPONENT_ID_PREFIX)
        .and_then(|rest| rest.split_once('/'))
        .map(|(connector, _)| connector)
}

/// Whether `component_id` is visible, given a lookup of its connector's
/// encoded head row. A component no pack owns is always visible; a pack
/// component with no head is not (only the import path writes `mcp:` ids, so
/// it can only be a row that predates its head).
pub(crate) fn pack_component_visible(
    component_id: &str,
    head: impl FnOnce(&str) -> Result<Option<Vec<u8>>, String>,
) -> Result<bool, String> {
    let Some(connector) = pack_connector(component_id) else {
        return Ok(true);
    };
    let Some(bytes) = head(connector)? else {
        return Ok(false);
    };
    let row: ConnectorPackHeadRow =
        crate::server::persistence::agent_row::decode(&bytes, "connector pack head")?;
    Ok(row.head.visible_record_id.as_deref() == Some(row.head.record_id.as_str()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_mcp_ids_name_a_connector() {
        assert_eq!(pack_connector("mcp:demo/tool/search"), Some("demo"));
        assert_eq!(pack_connector("mcp:demo"), None);
        assert_eq!(pack_connector("agent:demo/tool"), None);
    }

    #[test]
    fn a_native_component_is_always_visible_and_a_headless_pack_one_is_not() {
        assert!(pack_component_visible("prompt:native", |_| unreachable!()).unwrap());
        assert!(!pack_component_visible("mcp:demo/tool/a", |_| Ok(None)).unwrap());
    }
}
