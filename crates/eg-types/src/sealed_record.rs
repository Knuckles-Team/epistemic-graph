//! Sealed record classes (EH-558): content-addressed records stored as ordinary
//! graph nodes whose content must never change after they are written.
//!
//! A sealed record carries its own seal (a digest over its content, usually also
//! its node id). Re-sealing on read makes such a record tamper-EVIDENT; this
//! registry makes it tamper-PROOF. The durable generic row writer refuses every
//! generic write that would change or remove a stored row of a sealed class, in the
//! same transaction the write would commit in. Creating a new record is allowed, and
//! so is re-writing identical content, which keeps retries and create-if-absent
//! idempotent. A whole-graph clear or delete still removes sealed rows with the
//! graph.
//!
//! A row's class is its `type` property, the label convention every scan uses.
//! To seal a class, add its name to [`SEALED_RECORD_CLASSES`]. No class has an owning
//! write op today: the op that mints a record (for `AnalysisSnapshot`, the
//! `FinanceMarket` `analysis_snapshot` op) only computes it, and the caller stores it
//! with a create-if-absent node write.

use serde_json::{Map, Value};

/// Classes whose stored rows are create-only.
///
/// * `AnalysisSnapshot` — a sealed markets analysis record (finance-v1); its node id
///   is derived from its digest and share links verify it by re-sealing.
pub const SEALED_RECORD_CLASSES: &[&str] = &["AnalysisSnapshot"];

/// Whether `class` (a row's `type` value) is a sealed record class.
pub fn is_sealed_record_class(class: &str) -> bool {
    SEALED_RECORD_CLASSES.contains(&class)
}

/// The sealed class of a stored row, or `None` for an ordinary row.
pub fn sealed_record_class(row: &Map<String, Value>) -> Option<&str> {
    row.get("type")
        .and_then(Value::as_str)
        .filter(|class| is_sealed_record_class(class))
}

/// Whether a stored row belongs to a sealed record class.
pub fn is_sealed_record_row(row: &Map<String, Value>) -> bool {
    sealed_record_class(row).is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(value: Value) -> Map<String, Value> {
        value.as_object().cloned().unwrap()
    }

    #[test]
    fn only_registered_classes_under_the_type_key_are_sealed() {
        let sealed = row(serde_json::json!({"type": "AnalysisSnapshot", "record": "{}"}));
        assert_eq!(sealed_record_class(&sealed), Some("AnalysisSnapshot"));
        assert!(is_sealed_record_row(&sealed));
        for plain in [
            serde_json::json!({"type": "Doc"}),
            serde_json::json!({"node_type": "AnalysisSnapshot"}),
            serde_json::json!({"type": "analysissnapshot"}),
            serde_json::json!({"type": 7}),
            serde_json::json!({}),
        ] {
            assert!(!is_sealed_record_row(&row(plain.clone())), "{plain}");
        }
    }
}
