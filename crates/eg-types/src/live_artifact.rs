//! Live artifacts (MEDULLA-FEDERATED-QUERY-R701.1, R701.2, R701.4): a bounded JSON
//! document rendered from a template and re-derived from a query source.
//!
//! A refresh is compare-and-set on the artifact version. A failed refresh
//! keeps the prior data and records the failure. Interpolation is one
//! non-recursive pass: a substituted value is never scanned again, so data
//! cannot inject further placeholders.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Largest serialized `data` an artifact may hold.
pub const MAX_LIVE_ARTIFACT_DATA_BYTES: usize = 1024 * 1024;
/// Deepest JSON nesting `data` may reach.
pub const MAX_LIVE_ARTIFACT_DEPTH: usize = 32;
/// Largest template.
pub const MAX_LIVE_ARTIFACT_TEMPLATE_BYTES: usize = 64 * 1024;

pub const LIVE_ARTIFACT_INVALID: &str = "LIVE_ARTIFACT_INVALID";
pub const LIVE_ARTIFACT_TEMPLATE_UNSAFE: &str = "LIVE_ARTIFACT_TEMPLATE_UNSAFE";
pub const LIVE_ARTIFACT_DEPTH_EXCEEDED: &str = "LIVE_ARTIFACT_DEPTH_EXCEEDED";
pub const LIVE_ARTIFACT_SIZE_EXCEEDED: &str = "LIVE_ARTIFACT_SIZE_EXCEEDED";
pub const LIVE_ARTIFACT_VERSION_CONFLICT: &str = "LIVE_ARTIFACT_VERSION_CONFLICT";

/// Template constructs that execute code in some renderer. A template is plain
/// text with `{{path}}` placeholders and nothing else.
const UNSAFE_TEMPLATE_MARKERS: [&str; 7] =
    ["<script", "javascript:", "{%", "{{{", "{{#", "{{!", "{{>"];

/// A typed live-artifact refusal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
#[error("{code}: {detail}")]
pub struct LiveArtifactRefusal {
    pub code: String,
    pub detail: String,
}

fn refuse(code: &str, detail: impl Into<String>) -> LiveArtifactRefusal {
    LiveArtifactRefusal {
        code: code.to_string(),
        detail: detail.into(),
    }
}

/// The query language a source is written in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum QueryLanguage {
    Uql,
    Sql,
    Sparql,
}

/// The query a refresh re-runs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct QuerySource {
    pub language: QueryLanguage,
    pub text: String,
}

/// One refresh attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct RefreshRecord {
    pub started_at_ms: u64,
    pub ended_at_ms: u64,
    pub row_count: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Where the current data came from.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct ArtifactProvenance {
    /// The refresh that produced `data`, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub derived_by: Option<RefreshRecord>,
    /// The latest attempt, successful or not.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_attempt: Option<RefreshRecord>,
}

/// A live artifact (MEDULLA-FEDERATED-QUERY-R701.1).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct LiveArtifact {
    pub id: String,
    pub version: u64,
    pub template: String,
    pub source: QuerySource,
    pub data: Value,
    pub provenance: ArtifactProvenance,
}

fn depth(value: &Value) -> usize {
    match value {
        Value::Array(items) => 1 + items.iter().map(depth).max().unwrap_or(0),
        Value::Object(map) => 1 + map.values().map(depth).max().unwrap_or(0),
        _ => 0,
    }
}

fn check_data(data: &Value) -> Result<(), LiveArtifactRefusal> {
    if depth(data) > MAX_LIVE_ARTIFACT_DEPTH {
        return Err(refuse(
            LIVE_ARTIFACT_DEPTH_EXCEEDED,
            format!("data nests deeper than {MAX_LIVE_ARTIFACT_DEPTH}"),
        ));
    }
    let bytes = serde_json::to_vec(data)
        .map(|b| b.len())
        .unwrap_or(usize::MAX);
    if bytes > MAX_LIVE_ARTIFACT_DATA_BYTES {
        return Err(refuse(
            LIVE_ARTIFACT_SIZE_EXCEEDED,
            format!("data is {bytes} bytes, above {MAX_LIVE_ARTIFACT_DATA_BYTES}"),
        ));
    }
    Ok(())
}

/// Refuse a template that executes code or exceeds the size bound
/// (MEDULLA-FEDERATED-QUERY-R701.4).
pub fn check_template(template: &str) -> Result<(), LiveArtifactRefusal> {
    if template.len() > MAX_LIVE_ARTIFACT_TEMPLATE_BYTES {
        return Err(refuse(LIVE_ARTIFACT_SIZE_EXCEEDED, "template too large"));
    }
    let lower = template.to_ascii_lowercase();
    if let Some(marker) = UNSAFE_TEMPLATE_MARKERS.iter().find(|m| lower.contains(*m)) {
        return Err(refuse(
            LIVE_ARTIFACT_TEMPLATE_UNSAFE,
            format!("template contains `{marker}`"),
        ));
    }
    Ok(())
}

fn lookup<'a>(data: &'a Value, path: &str) -> Option<&'a Value> {
    path.split('.').try_fold(data, |value, key| match value {
        Value::Object(map) => map.get(key),
        Value::Array(items) => key.parse::<usize>().ok().and_then(|i| items.get(i)),
        _ => None,
    })
}

fn render_value(value: Option<&Value>) -> String {
    match value {
        None | Some(Value::Null) => String::new(),
        Some(Value::String(s)) => s.clone(),
        Some(other) => other.to_string(),
    }
}

impl LiveArtifact {
    /// Validate the artifact's bounds (MEDULLA-FEDERATED-QUERY-R701.1, R701.4).
    pub fn validate(&self) -> Result<(), LiveArtifactRefusal> {
        if self.id.trim().is_empty() {
            return Err(refuse(LIVE_ARTIFACT_INVALID, "id is required"));
        }
        if self.source.text.trim().is_empty() {
            return Err(refuse(LIVE_ARTIFACT_INVALID, "source text is required"));
        }
        check_template(&self.template)?;
        check_data(&self.data)
    }

    /// Render the template in one non-recursive pass over its text.
    pub fn interpolate(&self) -> String {
        let mut out = String::with_capacity(self.template.len());
        let mut rest = self.template.as_str();
        while let Some(start) = rest.find("{{") {
            out.push_str(&rest[..start]);
            let after = &rest[start + 2..];
            let Some(end) = after.find("}}") else {
                out.push_str(&rest[start..]);
                return out;
            };
            out.push_str(&render_value(lookup(&self.data, after[..end].trim())));
            rest = &after[end + 2..];
        }
        out.push_str(rest);
        out
    }

    /// Apply one refresh outcome if `expected_version` is current
    /// (MEDULLA-FEDERATED-QUERY-R701.2). A failed or out-of-bounds result keeps
    /// the prior data and is recorded as the latest attempt.
    pub fn refresh(
        &mut self,
        expected_version: u64,
        outcome: Result<Value, String>,
        started_at_ms: u64,
        ended_at_ms: u64,
    ) -> Result<RefreshRecord, LiveArtifactRefusal> {
        if expected_version != self.version {
            return Err(refuse(
                LIVE_ARTIFACT_VERSION_CONFLICT,
                format!(
                    "expected version {expected_version}, current {}",
                    self.version
                ),
            ));
        }
        let checked = outcome.and_then(|data| {
            check_data(&data)
                .map(|()| data)
                .map_err(|refusal| refusal.to_string())
        });
        let mut record = RefreshRecord {
            started_at_ms,
            ended_at_ms,
            row_count: 0,
            error: None,
        };
        match checked {
            Ok(data) => {
                record.row_count = match &data {
                    Value::Array(rows) => rows.len() as u64,
                    Value::Null => 0,
                    _ => 1,
                };
                self.data = data;
                self.version += 1;
                self.provenance.derived_by = Some(record.clone());
            }
            Err(error) => record.error = Some(error),
        }
        self.provenance.last_attempt = Some(record.clone());
        Ok(record)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn artifact() -> LiveArtifact {
        LiveArtifact {
            id: "dash-1".into(),
            version: 1,
            template: "Open: {{summary.open}} / {{rows.0.name}}".into(),
            source: QuerySource {
                language: QueryLanguage::Uql,
                text: "MATCH (:Incident) RETURN count".into(),
            },
            data: json!({"rows": [{"name": "db"}], "summary": {"open": 3}}),
            provenance: ArtifactProvenance::default(),
        }
    }

    const GOLDEN: &str = r#"{"id":"dash-1","version":1,"template":"Open: {{summary.open}} / {{rows.0.name}}","source":{"language":"uql","text":"MATCH (:Incident) RETURN count"},"data":{"rows":[{"name":"db"}],"summary":{"open":3}},"provenance":{}}"#;

    // spec: MEDULLA-FEDERATED-QUERY-R701.1
    #[test]
    fn live_artifact_golden_vector_round_trips_and_bounds_hold() {
        let a = artifact();
        a.validate().unwrap();
        assert_eq!(serde_json::to_string(&a).unwrap(), GOLDEN);
        assert_eq!(serde_json::from_str::<LiveArtifact>(GOLDEN).unwrap(), a);
        assert!(serde_json::from_str::<QueryLanguage>("\"python\"").is_err());

        let mut deep = artifact();
        let mut value = json!(1);
        for _ in 0..=MAX_LIVE_ARTIFACT_DEPTH {
            value = json!([value]);
        }
        deep.data = value;
        assert_eq!(
            deep.validate().unwrap_err().code,
            LIVE_ARTIFACT_DEPTH_EXCEEDED
        );
        let mut big = artifact();
        big.data = json!("x".repeat(MAX_LIVE_ARTIFACT_DATA_BYTES));
        assert_eq!(
            big.validate().unwrap_err().code,
            LIVE_ARTIFACT_SIZE_EXCEEDED
        );
        let mut blank = artifact();
        blank.id = String::new();
        assert_eq!(blank.validate().unwrap_err().code, LIVE_ARTIFACT_INVALID);
    }

    // spec: MEDULLA-FEDERATED-QUERY-R701.2
    #[test]
    fn refresh_is_compare_and_set_and_interpolation_is_one_pass() {
        let mut a = artifact();
        assert_eq!(a.interpolate(), "Open: 3 / db");

        let record = a
            .refresh(1, Ok(json!([{"n": 1}, {"n": 2}])), 10, 20)
            .unwrap();
        assert_eq!((record.row_count, a.version), (2, 2));
        assert_eq!(a.provenance.derived_by, Some(record));
        let stale = a.refresh(1, Ok(json!([])), 30, 40).unwrap_err();
        assert_eq!(stale.code, LIVE_ARTIFACT_VERSION_CONFLICT);
        assert_eq!(a.data, json!([{"n": 1}, {"n": 2}]));

        // A substituted value is not rescanned.
        a.template = "{{0.n}}{{v}}".into();
        a.data = json!({"0": {"n": 7}, "v": "{{0.n}}"});
        assert_eq!(a.interpolate(), "7{{0.n}}");

        // Linear in the template size: 10, 1,000 and 100,000 placeholders.
        for n in [10usize, 1_000, 100_000] {
            let mut wide = artifact();
            wide.template = "{{summary.open}},".repeat(n);
            wide.data = json!({"summary": {"open": 1}});
            assert_eq!(wide.interpolate().len(), 2 * n);
        }
    }

    // spec: MEDULLA-FEDERATED-QUERY-R701.4
    #[test]
    fn unsafe_templates_refuse_and_failed_refresh_keeps_data() {
        for template in [
            "<script>alert(1)</script>",
            "{% for x in rows %}",
            "{{{raw}}}",
            "{{#each rows}}",
            "<a href=\"JavaScript:run()\">",
        ] {
            let mut a = artifact();
            a.template = template.into();
            assert_eq!(
                a.validate().unwrap_err().code,
                LIVE_ARTIFACT_TEMPLATE_UNSAFE,
                "{template}"
            );
        }
        let mut a = artifact();
        let before = a.data.clone();
        let record = a
            .refresh(1, Err("source unavailable".into()), 5, 6)
            .unwrap();
        assert_eq!(record.error.as_deref(), Some("source unavailable"));
        assert_eq!((a.data.clone(), a.version), (before.clone(), 1));
        assert_eq!(a.provenance.last_attempt, Some(record));
        assert!(a.provenance.derived_by.is_none());

        let mut deep = json!(0);
        for _ in 0..=MAX_LIVE_ARTIFACT_DEPTH {
            deep = json!({"k": deep});
        }
        let record = a.refresh(1, Ok(deep), 7, 8).unwrap();
        assert!(record.error.unwrap().contains(LIVE_ARTIFACT_DEPTH_EXCEEDED));
        assert_eq!(a.data, before);
    }
}
