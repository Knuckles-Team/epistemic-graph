use serde::{Deserialize, Deserializer};

pub(super) fn deserialize_required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

/// Preserve explicit-null wire fields while making their presence mandatory.
#[cfg(feature = "contract-schema")]
pub(super) fn require_marked_nullable_fields(schema: &mut schemars::Schema) {
    let Some(object) = schema.as_object_mut() else {
        return;
    };
    let fields: Vec<_> = object
        .get("properties")
        .and_then(serde_json::Value::as_object)
        .into_iter()
        .flat_map(|properties| properties.iter())
        .filter(|(_, field)| {
            field
                .get("x-eg-required-presence")
                .and_then(serde_json::Value::as_bool)
                == Some(true)
        })
        .map(|(name, _)| serde_json::Value::String(name.clone()))
        .collect();
    let required = object
        .entry("required")
        .or_insert_with(|| serde_json::json!([]))
        .as_array_mut()
        .expect("object required must be an array");
    for name in fields {
        if !required.contains(&name) {
            required.push(name);
        }
    }
}
