//! A node's type/label as read from an eg-compute property blob.
//!
//! Lifted verbatim out of `algorithms.rs` when that module was decomposed, so
//! the `algorithms` submodules that need it share one copy instead of each
//! carrying its own. Behaviour is unchanged from the original private helper.

/// Extract a node's type/label from its property blob — same key precedence
/// as `server::handlers::mining::node_type_label` (kept as an independent
/// small copy: that one reads a `GraphCore` node blob directly under the
/// server crate, this one reads a `GraphView::node_properties` blob from
/// eg-compute; duplicating a 6-line lookup is cheaper than a new cross-crate
/// dependency for it).
pub(crate) fn node_type_label(blob: &[u8]) -> Option<String> {
    let val = eg_types::msgpack::decode_property_value(blob).ok()?;
    for key in ["type", "node_type", "label"] {
        if let Some(s) = val.get(key).and_then(|v| v.as_str()) {
            return Some(s.to_string());
        }
    }
    None
}
