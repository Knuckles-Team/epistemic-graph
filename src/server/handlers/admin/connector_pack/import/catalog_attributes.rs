//! The catalog attributes a pack member carries for the fleet catalog
//! projection (contract request eg-fleet-catalog R1).
//!
//! Each is a pure function of the entry's own content, so each belongs in the
//! definition digest like every other pack attribute:
//!
//! * `mcp.media_type` on MCP resources, resource templates and skill files;
//! * `skill.type` on skills, the `SKILL.md` front-matter `type` verbatim;
//! * `sdk.tool_mode` on tools, the pack builder's declaration (EG never
//!   re-derives it from the schema).

use std::collections::BTreeMap;

use eg_types::connector_pack::digest::tool_mode_token;
use eg_types::connector_pack::{PackEntry, PackEntryKind};

use super::front_matter::declared_skill_type;

/// Add the catalog attributes `entry` declares.
pub(super) fn add_catalog_attributes(
    attributes: &mut BTreeMap<String, String>,
    entry: &PackEntry,
    body: &[u8],
) {
    if matches!(
        entry.kind,
        PackEntryKind::Resource | PackEntryKind::ResourceTemplate | PackEntryKind::SkillFile
    ) {
        attributes.insert("mcp.media_type".into(), entry.media_type.clone());
    }
    if entry.kind == PackEntryKind::Skill {
        if let Ok(Some(skill_type)) = declared_skill_type(body) {
            attributes.insert("skill.type".into(), skill_type.into());
        }
    }
    if let Some(mode) = entry.annotations.tool_mode {
        attributes.insert("sdk.tool_mode".into(), tool_mode_token(mode).into());
    }
}
