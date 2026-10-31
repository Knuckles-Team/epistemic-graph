//! Declared model input limits carried by an immutable ModelProfile revision.
//!
//! These are publisher facts, not a grant. Admission must bind the containing
//! component's definition digest and caller authority before applying them.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

pub const MAX_MODEL_INPUT_BYTES: u64 = 64 * 1024 * 1024;
pub const MAX_MODEL_INPUT_PARTS: u32 = 128;

/// Closed set of inline input representations supported by the run contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ModelInputMediaType {
    #[serde(rename = "text/plain")]
    Text,
    #[serde(rename = "image/png")]
    Png,
    #[serde(rename = "image/jpeg")]
    Jpeg,
    #[serde(rename = "image/webp")]
    Webp,
}

impl ModelInputMediaType {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Text => "text/plain",
            Self::Png => "image/png",
            Self::Jpeg => "image/jpeg",
            Self::Webp => "image/webp",
        }
    }
}

/// Optional on a profile: absence means unknown, never unrestricted input.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ModelInputCapabilities {
    pub media_types: Vec<ModelInputMediaType>,
    pub max_aggregate_bytes: u64,
    pub max_parts: u32,
}

impl ModelInputCapabilities {
    /// Validate the declaration before publishing its containing component.
    pub fn validate(&self) -> Result<(), String> {
        if self.media_types.is_empty()
            || self.media_types.len() > 4
            || self.media_types.iter().collect::<BTreeSet<_>>().len() != self.media_types.len()
        {
            return Err("model input media types must be nonempty and unique".into());
        }
        if !(1..=MAX_MODEL_INPUT_BYTES).contains(&self.max_aggregate_bytes)
            || !(1..=MAX_MODEL_INPUT_PARTS).contains(&self.max_parts)
        {
            return Err("model input byte or part bound is invalid".into());
        }
        Ok(())
    }

    /// Check decoded inline part sizes against an already selected revision.
    /// This validates facts only; it neither resolves a profile nor grants access.
    pub fn validate_parts(&self, parts: &[(&str, u64)]) -> Result<(), String> {
        self.validate()?;
        if parts.len() > self.max_parts as usize {
            return Err("model input part count exceeds the selected profile".into());
        }
        let mut total = 0_u64;
        for (media_type, bytes) in parts {
            if !self
                .media_types
                .iter()
                .any(|media| media.as_str() == *media_type)
            {
                return Err(
                    "model input media type is not declared by the selected profile".into(),
                );
            }
            total = total
                .checked_add(*bytes)
                .ok_or("model input byte count overflow")?;
            if *bytes == 0 || total > self.max_aggregate_bytes {
                return Err("model input bytes exceed the selected profile or are empty".into());
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn capabilities() -> ModelInputCapabilities {
        ModelInputCapabilities {
            media_types: vec![ModelInputMediaType::Text, ModelInputMediaType::Png],
            max_aggregate_bytes: 16,
            max_parts: 2,
        }
    }

    // spec: MEDULLA-DECISION-ENGINE-R050.1
    #[test]
    fn selected_profile_enforces_exact_decoded_bounds() {
        let caps = capabilities();
        assert!(caps
            .validate_parts(&[("text/plain", 6), ("image/png", 10)])
            .is_ok());
        for parts in [
            vec![("text/plain", 6), ("image/png", 11)],
            vec![("image/jpeg", 1)],
            vec![("text/plain", 0)],
            vec![("text/plain", 1); 3],
            vec![("text/plain", u64::MAX)],
        ] {
            assert!(caps.validate_parts(&parts).is_err());
        }
    }

    // spec: MEDULLA-DECISION-ENGINE-R050.1
    #[test]
    fn declarations_reject_unknown_media_duplicates_and_unbounded_limits() {
        let wire = serde_json::to_value(capabilities()).unwrap();
        for invalid in ["image/gif", "application/octet-stream", "image/*"] {
            let mut value = wire.clone();
            value["media_types"][0] = invalid.into();
            assert!(serde_json::from_value::<ModelInputCapabilities>(value).is_err());
        }
        for (field, value) in [
            ("media_types", serde_json::json!([])),
            ("media_types", serde_json::json!(["image/png", "image/png"])),
            ("max_parts", serde_json::json!(0)),
            ("max_parts", serde_json::json!(129)),
            ("max_aggregate_bytes", serde_json::json!(0)),
            (
                "max_aggregate_bytes",
                serde_json::json!(MAX_MODEL_INPUT_BYTES + 1),
            ),
        ] {
            let mut input = wire.clone();
            input[field] = value;
            let invalid: ModelInputCapabilities = serde_json::from_value(input).unwrap();
            assert!(invalid.validate().is_err());
        }
    }
}
