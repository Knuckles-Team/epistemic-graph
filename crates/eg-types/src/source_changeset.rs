//! The source changeset record (EG-SCM-R002): one commit-shaped change to a
//! repository, with its structured trailers (EG-SCM-R002.1).
//!
//! Pure data. Acceptance, rebase and git import and export live above this
//! crate; EG-SDLC derivation reads the typed trailer fields.

use serde::{Deserialize, Serialize};

/// What a changeset did to one path.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum PathOp {
    Add,
    Modify,
    Delete,
    Rename,
}

/// One path that changed against the first parent.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct ChangedPath {
    pub path: String,
    pub op: PathOp,
    pub old_blob: Option<String>,
    pub new_blob: Option<String>,
    pub mode: u32,
}

/// The structured form of the `Spec:`, `Fixes:` and `Spec-Withdraw:` trailers.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct Trailers {
    pub spec_ids: Vec<String>,
    pub fixes: Vec<String>,
    pub withdraw: Vec<String>,
    pub withdraw_reason: Option<String>,
}

fn split_ids(value: &str) -> impl Iterator<Item = String> + '_ {
    value
        .split(',')
        .map(str::trim)
        .filter(|id| !id.is_empty())
        .map(str::to_owned)
}

impl Trailers {
    /// Parse the trailers from the last paragraph of a commit message. A
    /// `Spec: none (reason)` line cites no requirement.
    pub fn parse(message: &str) -> Self {
        let mut out = Self::default();
        let Some(block) = message.trim_end().rsplit("\n\n").next() else {
            return out;
        };
        for line in block.lines() {
            let Some((key, value)) = line.split_once(':') else {
                continue;
            };
            let value = value.trim();
            match key.trim() {
                "Spec" if !value.starts_with("none") => out.spec_ids.extend(split_ids(value)),
                "Fixes" => out.fixes.extend(split_ids(value)),
                "Spec-Withdraw" => {
                    let (ids, reason) = match value.split_once(" (") {
                        Some((ids, reason)) => (ids, reason.strip_suffix(')')),
                        None => (value, None),
                    };
                    out.withdraw.extend(split_ids(ids));
                    out.withdraw_reason = reason.map(str::to_owned);
                }
                _ => {}
            }
        }
        out
    }

    /// Render the trailer lines, in the order `Spec`, `Fixes`, `Spec-Withdraw`.
    pub fn render(&self) -> String {
        let mut lines = Vec::new();
        if !self.spec_ids.is_empty() {
            lines.push(format!("Spec: {}", self.spec_ids.join(", ")));
        }
        if !self.fixes.is_empty() {
            lines.push(format!("Fixes: {}", self.fixes.join(", ")));
        }
        if !self.withdraw.is_empty() {
            let reason = self
                .withdraw_reason
                .as_ref()
                .map(|r| format!(" ({r})"))
                .unwrap_or_default();
            lines.push(format!(
                "Spec-Withdraw: {}{reason}",
                self.withdraw.join(", ")
            ));
        }
        lines.join("\n")
    }
}

/// A changeset: the paths that changed against the first parent.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct Changeset {
    pub id: String,
    pub repo: String,
    pub parents: Vec<String>,
    pub author_principal: String,
    pub message: String,
    pub changed_paths: Vec<ChangedPath>,
    pub created_at: u64,
    pub trailers: Trailers,
}

mod chain;
pub use chain::*;

#[cfg(test)]
mod tests {
    use super::*;

    // spec: EG-SCM-R002.1
    #[test]
    fn trailers_parse_and_render_round_trip() {
        let message = "feat: x\n\nbody\n\nSpec: EG-A-R001, EG-A-R002.1\nFixes: DEF-7\nSpec-Withdraw: EG-A-R009 (superseded)";
        let trailers = Trailers::parse(message);
        assert_eq!(trailers.spec_ids, ["EG-A-R001", "EG-A-R002.1"]);
        assert_eq!(trailers.fixes, ["DEF-7"]);
        assert_eq!(trailers.withdraw, ["EG-A-R009"]);
        assert_eq!(trailers.withdraw_reason.as_deref(), Some("superseded"));
        assert_eq!(Trailers::parse(&trailers.render()), trailers);
        assert!(Trailers::parse("fix\n\nSpec: none (ci)")
            .spec_ids
            .is_empty());
    }
}
