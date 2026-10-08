//! Roles, groups and identity-provider records (§3.2, §4).

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::requests_admin::RoleGraphGrant;

/// A role: a named set of registry scopes plus graph grants projected into
/// the RBAC image as role `idm:<role_id>`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RoleRecord {
    pub role_id: String,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default)]
    pub builtin: bool,
    #[serde(default)]
    pub scopes: BTreeSet<String>,
    #[serde(default)]
    pub graph_grants: Vec<RoleGraphGrant>,
}

/// A group: members plus the roles every member holds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct GroupRecord {
    pub group_id: String,
    pub name: String,
    /// `local`, `ldap:<idp>`, `scim:<idp>`, `oidc:<idp>`.
    pub source: String,
    #[serde(default)]
    pub builtin: bool,
    /// principal id → membership source (`local` or `<idp_id>`); an
    /// IdP-sourced membership is recomputed at every login/sync.
    #[serde(default)]
    pub members: BTreeMap<String, String>,
    #[serde(default)]
    pub roles: BTreeSet<String>,
    /// Operator ruling 2026-09-24: MFA is optional for everyone, and an
    /// administrator may require it per group. A member of a group with this
    /// set cannot complete a sign-in without a confirmed second factor.
    #[serde(default)]
    pub mfa_required: bool,
}

/// Which protocol an identity provider speaks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum IdpKind {
    Oidc,
    Saml,
    Ldap,
    Scim,
}

/// What happens on the first sign-in of an unlinked subject.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum JitPolicy {
    /// Refuse: users are pre-provisioned by an admin, SCIM or LDAP sync.
    Deny,
    /// Create a new principal on first sign-in.
    Create,
    /// Link to an existing principal with the same VERIFIED e-mail. Off by
    /// default: cross-IdP e-mail linking is an account-takeover vector.
    LinkByVerifiedEmail,
}

/// One ordered claim → role/group mapping rule. The target is `role:<id>` or
/// `group:<id>`; a rule reaching an approver- or admin-class scope must be
/// marked `privileged`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct MappingRule {
    pub rule_id: String,
    pub claim_path: String,
    /// `equals`, `prefix` or `regex`.
    pub match_kind: String,
    pub value: String,
    pub target: String,
    #[serde(default)]
    pub privileged: bool,
}

/// An identity-provider configuration. `secret_ref` names a secret in the
/// secrets backend; a secret value is never stored here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct IdpConfig {
    pub idp_id: String,
    pub kind: IdpKind,
    pub display_name: String,
    pub enabled: bool,
    /// Protocol configuration (issuer, endpoints, metadata) as a JSON object.
    pub config_json: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret_ref: Option<String>,
    pub jit_policy: JitPolicy,
    #[serde(default)]
    pub email_domains: Vec<String>,
    #[serde(default)]
    pub order: u32,
    #[serde(default)]
    pub rules: Vec<MappingRule>,
}
