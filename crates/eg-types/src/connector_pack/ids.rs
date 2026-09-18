//! How a pack entry's name becomes a component id.
//!
//! `mcp:<connector>/<kind_token>/<escaped_name>`, where the escaped name
//! percent-encodes every UTF-8 byte outside `[A-Za-z0-9._-]` with uppercase
//! hex. That set is chosen so `/`, `~`, `%`, spaces and every non-ASCII byte
//! are encoded: the id is a path-shaped key in a durable store, and a name
//! that could inject a separator could address another entry's row.
//!
//! The mapping is total and injective for a given kind, so two entries collide
//! only when they really are the same `(kind, name)` -- which the importer
//! refuses as `DUPLICATE_COMPONENT_ID` rather than silently revising one.

use super::digest::entry_kind_token;
use super::index::PackEntryKind;

/// The prefix every pack-owned component id carries.
pub const PACK_COMPONENT_ID_PREFIX: &str = "mcp:";

const HEX: &[u8; 16] = b"0123456789ABCDEF";

/// Percent-encode `name` under the pack's unreserved set.
pub fn escape_pack_name(name: &str) -> String {
    let mut escaped = String::with_capacity(name.len());
    for byte in name.as_bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-') {
            escaped.push(*byte as char);
        } else {
            escaped.push('%');
            escaped.push(HEX[usize::from(byte >> 4)] as char);
            escaped.push(HEX[usize::from(byte & 0x0f)] as char);
        }
    }
    escaped
}

/// The component id one pack entry publishes under.
pub fn pack_component_id(connector: &str, kind: PackEntryKind, name: &str) -> String {
    format!(
        "{PACK_COMPONENT_ID_PREFIX}{connector}/{}/{}",
        entry_kind_token(kind),
        escape_pack_name(name)
    )
}
