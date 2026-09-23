//! Python identifier derivation shared by the DTO and model renderers (EH-192).
//!
//! Wire names are not always Python identifiers: a field may be a keyword (`from`,
//! `class`) or shadow a `BaseModel` attribute (`schema`), and an enum value may carry
//! punctuation (`transfer-root`). One owner maps each to a valid identifier.

/// Python keywords and `BaseModel` attributes a field name must not take verbatim.
const RESERVED_FIELD_NAMES: &[&str] = &[
    "False",
    "None",
    "True",
    "and",
    "as",
    "assert",
    "async",
    "await",
    "break",
    "class",
    "construct",
    "continue",
    "copy",
    "def",
    "del",
    "dict",
    "elif",
    "else",
    "except",
    "fields",
    "finally",
    "for",
    "from",
    "global",
    "if",
    "import",
    "in",
    "is",
    "json",
    "lambda",
    "nonlocal",
    "not",
    "or",
    "pass",
    "raise",
    "return",
    "schema",
    "try",
    "validate",
    "while",
    "with",
    "yield",
];

/// The Python attribute a wire field is exposed under, and whether it needs a
/// pydantic alias back to the wire name.
pub(super) fn field_identifier(wire: &str) -> (String, bool) {
    if RESERVED_FIELD_NAMES.contains(&wire) {
        (format!("{wire}_"), true)
    } else {
        (wire.to_string(), false)
    }
}

/// `str` enum member name for a wire value: upper-cased, punctuation to `_`, and a
/// `V_` prefix when the result would not start with a letter or underscore.
pub(super) fn enum_member(value: &str) -> String {
    let member: String = value
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_uppercase()
            } else {
                '_'
            }
        })
        .collect();
    match member.chars().next() {
        Some(first) if first.is_ascii_alphabetic() || first == '_' => member,
        _ => format!("V_{member}"),
    }
}

/// `get_state` / `get-state` / `GetState` -> `GetState`: split on anything that is not
/// ASCII alphanumeric and capitalize each part.
pub(super) fn pascal(value: &str) -> String {
    value
        .split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|part| !part.is_empty())
        .map(|part| {
            let mut chars = part.chars();
            chars
                .next()
                .map(|first| first.to_ascii_uppercase().to_string() + chars.as_str())
                .unwrap_or_default()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reserved_and_punctuated_names_become_identifiers() {
        assert_eq!(field_identifier("from"), ("from_".to_string(), true));
        assert_eq!(field_identifier("schema"), ("schema_".to_string(), true));
        assert_eq!(
            field_identifier("model_id"),
            ("model_id".to_string(), false)
        );
        assert_eq!(enum_member("transfer-root"), "TRANSFER_ROOT");
        assert_eq!(enum_member("2d"), "V_2D");
        assert_eq!(pascal("get_state"), "GetState");
        assert_eq!(pascal("GetState"), "GetState");
        assert_eq!(pascal("retry-scheduled"), "RetryScheduled");
    }
}
