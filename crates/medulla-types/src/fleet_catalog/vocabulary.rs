//! Closed vocabulary shared by fleet catalog requests and rows.

use serde::{Deserialize, Serialize};

use crate::contract::Digest256;

/// Under whose authority a discovery observation was made.
///
/// Closed, and deliberately two-valued. A local/stdio probe has no provider
/// grant to fingerprint, so it is tenant-local and visible to the tenant; a
/// probe made with a principal's OAuth grant saw what THAT grant allows, so it
/// is visible only to that principal while that grant is current. There is no
/// third "global" value: an observation that cannot say which of the two it is
/// is not recorded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "authority", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum DiscoveryScope {
    TenantLocal,
    /// `grant_digest` is the broker-minted fingerprint of the grant the probe
    /// used. The principal is NOT a field: the server binds it from the
    /// verified request context.
    OauthGrant {
        grant_digest: Digest256,
    },
}

impl DiscoveryScope {
    /// The scope's stable, id-safe spelling.
    pub fn key(&self) -> String {
        match self {
            Self::TenantLocal => "tenant_local".to_string(),
            Self::OauthGrant { grant_digest } => format!("grant-{}", grant_digest.to_hex()),
        }
    }
}

/// What the probe found.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum DiscoveryOutcome {
    Reachable,
    /// Recorded rather than omitted: "unavailable" must never look identical to
    /// "empty". `error` is caller-redacted text; it is bounded here and never
    /// interpreted.
    Unreachable {
        error: String,
    },
}

/// How a skill is run. The four values the agent ecosystem declares.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum SkillType {
    Skill,
    Workflow,
    Graph,
    McpSkill,
}

impl SkillType {
    /// Parse a declared value (`SKILL.md` front matter), case-insensitively.
    /// `None` for anything outside the closed set -- the caller decides what an
    /// undeclared type becomes, and says so in [`SkillTypeSource`].
    pub fn from_declared(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "skill" => Some(Self::Skill),
            "workflow" => Some(Self::Workflow),
            "graph" => Some(Self::Graph),
            "mcp_skill" => Some(Self::McpSkill),
            _ => None,
        }
    }

    /// The display classification, computed from the type rather than stored,
    /// so the two can never disagree.
    pub fn label(self) -> &'static str {
        match self {
            Self::Skill => "Atomic Skill",
            Self::Workflow => "Workflow",
            Self::Graph => "Skill Graph",
            Self::McpSkill => "MCP Skill",
        }
    }
}

/// Which authority a skill row's type came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum SkillTypeSource {
    /// A durable operator override. Wins over everything else.
    Override,
    /// The skill's own declaration, as its pack published it.
    Declared,
    /// Nothing declared one; an ordinary atomic skill.
    Default,
}

/// Whether a tool takes the condensed `action` + `params_json` calling shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ToolMode {
    Condensed,
    Verbose,
    /// The publisher did not declare one. Never guessed from the schema.
    Undeclared,
}

impl ToolMode {
    pub fn from_declared(value: Option<&str>) -> Self {
        match value.map(str::trim) {
            Some("condensed") => Self::Condensed,
            Some("verbose") => Self::Verbose,
            _ => Self::Undeclared,
        }
    }
}

/// What an MCP resource carries, from its URI scheme.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ResourceKind {
    Skill,
    Prompt,
    Resource,
}

impl ResourceKind {
    pub fn from_uri(uri: &str) -> Self {
        if uri.starts_with("skill://") {
            Self::Skill
        } else if uri.starts_with("prompt://") {
            Self::Prompt
        } else {
            Self::Resource
        }
    }
}

/// Which kind of row a page lists.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum FleetCatalogKind {
    Discoveries,
    Tools,
    Prompts,
    Resources,
    Skills,
}

impl FleetCatalogKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Discoveries => "discoveries",
            Self::Tools => "tools",
            Self::Prompts => "prompts",
            Self::Resources => "resources",
            Self::Skills => "skills",
        }
    }
}

/// Who may see a row, as the engine decided it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "scope", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum FleetVisibility {
    /// Every verified principal of the tenant.
    Tenant,
    /// Only `principal` (an opaque persistence id), while the grant that made
    /// the observation is current.
    Principal { principal: String },
}

/// What a fleet catalog write did.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum FleetWriteDisposition {
    /// A new revision was committed.
    Written,
    /// The stored record already said exactly this; nothing was committed.
    Replayed,
}

/// One durable operator override. Closed, one variant per field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "field", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum FleetOverride {
    /// Reclassify a skill. Survives every re-import of its pack, because the
    /// import never writes override records and the projection applies them at
    /// read time.
    SkillType { skill_type: SkillType },
}

impl FleetOverride {
    pub fn field(&self) -> FleetOverrideField {
        match self {
            Self::SkillType { .. } => FleetOverrideField::SkillType,
        }
    }
}

/// The field an override record governs; one record per `(component, field)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum FleetOverrideField {
    SkillType,
}

impl FleetOverrideField {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SkillType => "skill_type",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn skill_type_parses_the_declared_set_and_labels_it() {
        assert_eq!(
            SkillType::from_declared(" Workflow "),
            Some(SkillType::Workflow)
        );
        assert_eq!(
            SkillType::from_declared("mcp_skill"),
            Some(SkillType::McpSkill)
        );
        assert_eq!(SkillType::from_declared("unheard-of"), None);
        assert_eq!(SkillType::Skill.label(), "Atomic Skill");
        assert_eq!(SkillType::Graph.label(), "Skill Graph");
    }

    #[test]
    fn resource_kind_comes_from_the_uri_scheme() {
        assert_eq!(
            ResourceKind::from_uri("skill://triage/SKILL.md"),
            ResourceKind::Skill
        );
        assert_eq!(
            ResourceKind::from_uri("prompt://summarize"),
            ResourceKind::Prompt
        );
        assert_eq!(
            ResourceKind::from_uri("file:///etc/motd"),
            ResourceKind::Resource
        );
    }

    #[test]
    fn tool_mode_is_never_guessed() {
        assert_eq!(
            ToolMode::from_declared(Some("condensed")),
            ToolMode::Condensed
        );
        assert_eq!(ToolMode::from_declared(Some("other")), ToolMode::Undeclared);
        assert_eq!(ToolMode::from_declared(None), ToolMode::Undeclared);
    }

    #[test]
    fn scope_keys_are_disjoint_and_id_safe() {
        let grant = DiscoveryScope::OauthGrant {
            grant_digest: Digest256::from_bytes([7; 32]),
        };
        assert_eq!(DiscoveryScope::TenantLocal.key(), "tenant_local");
        assert_eq!(grant.key(), format!("grant-{}", "07".repeat(32)));
        assert!(DiscoveryScope::TenantLocal < grant);
    }
}
