//! Native freshness metadata without changing the upstream claim protocol DTO.
use crate::epistemic_operations::ClaimWorkItemResult;
use serde::de::{Error, MapAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Map, Value};

/// Engine-owned extension. The flattened upstream fields remain unchanged.
/// Freshness is not a bearer credential and cannot replace caller/fence checks.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct NativeClaimWorkItemResult {
    #[serde(flatten)]
    pub claim: ClaimWorkItemResult,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fresh_execution_grant: Option<bool>,
}

impl std::ops::Deref for NativeClaimWorkItemResult {
    type Target = ClaimWorkItemResult;

    fn deref(&self) -> &Self::Target {
        &self.claim
    }
}

// A derived flatten decoder cannot safely be combined with the upstream DTO's
// deny_unknown_fields. Remove exactly our extension, then let that unchanged
// strict DTO validate every remaining field. Reject duplicate keys before the
// intermediate map could collapse them.
impl<'de> Deserialize<'de> for NativeClaimWorkItemResult {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct ClaimVisitor;
        impl<'de> Visitor<'de> for ClaimVisitor {
            type Value = NativeClaimWorkItemResult;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a native claim result object")
            }

            fn visit_map<A: MapAccess<'de>>(self, mut access: A) -> Result<Self::Value, A::Error> {
                let mut fields = Map::new();
                while let Some((key, value)) = access.next_entry::<String, Value>()? {
                    if fields.insert(key, value).is_some() {
                        return Err(A::Error::custom("duplicate native claim result field"));
                    }
                }
                let fresh_execution_grant = fields
                    .remove("fresh_execution_grant")
                    .map(serde_json::from_value::<Option<bool>>)
                    .transpose()
                    .map_err(A::Error::custom)?
                    .flatten();
                let claim =
                    serde_json::from_value(Value::Object(fields)).map_err(A::Error::custom)?;
                Ok(NativeClaimWorkItemResult {
                    claim,
                    fresh_execution_grant,
                })
            }
        }
        deserializer.deserialize_map(ClaimVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn legacy() -> Value {
        serde_json::json!({
            "schema_version":"1", "claimed":false, "reason":"empty",
            "work_item_id":null, "kind":null, "payload_ref":null,
            "lease_holder_ref":null, "lease_epoch":null, "fencing_token":null,
            "lease_expires_at_ms":null, "attempt":null, "max_attempts":null,
            "tenant_in_flight":0, "changed_work_item_ids":[]
        })
    }

    #[test]
    fn legacy_shape_round_trips_without_native_extension() {
        let value = legacy();
        let result: NativeClaimWorkItemResult = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(result.fresh_execution_grant, None);
        assert_eq!(serde_json::to_value(result).unwrap(), value);
    }

    #[test]
    fn only_boolean_native_marker_is_accepted() {
        for marker in [Value::Bool(true), Value::Bool(false), Value::Null] {
            let mut value = legacy();
            value["fresh_execution_grant"] = marker;
            assert!(serde_json::from_value::<NativeClaimWorkItemResult>(value).is_ok());
        }
        for marker in [serde_json::json!(1), serde_json::json!("true")] {
            let mut value = legacy();
            value["fresh_execution_grant"] = marker;
            assert!(serde_json::from_value::<NativeClaimWorkItemResult>(value).is_err());
        }
    }

    #[test]
    fn upstream_unknown_fields_and_duplicate_keys_still_refuse() {
        let mut value = legacy();
        value["unrecognized"] = true.into();
        assert!(serde_json::from_value::<NativeClaimWorkItemResult>(value).is_err());
        let encoded = serde_json::to_string(&legacy()).unwrap();
        let duplicated = format!("{{\"claimed\":false,{}", &encoded[1..]);
        assert!(serde_json::from_str::<NativeClaimWorkItemResult>(&duplicated).is_err());
    }

    #[test]
    fn native_marker_round_trips_the_named_msgpack_result() {
        let mut value = legacy();
        value["fresh_execution_grant"] = true.into();
        let original: NativeClaimWorkItemResult = serde_json::from_value(value).unwrap();
        let bytes = rmp_serde::to_vec_named(&original).unwrap();
        let restored: NativeClaimWorkItemResult = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(restored, original);
    }

    #[cfg(feature = "contract-schema")]
    #[test]
    fn native_schema_flattens_claim_without_opening_unknown_fields() {
        let schema =
            serde_json::to_value(schemars::schema_for!(NativeClaimWorkItemResult)).unwrap();
        assert_eq!(schema["additionalProperties"], false);
        let properties = schema["properties"].as_object().unwrap();
        assert!(properties.contains_key("claimed"));
        assert!(properties.contains_key("lease_epoch"));
        assert!(properties.contains_key("fresh_execution_grant"));
        assert!(!properties.contains_key("claim"));
        assert!(!schema["required"]
            .as_array()
            .unwrap()
            .iter()
            .any(|field| field == "fresh_execution_grant"));
    }
}
