//! Request bodies of the role, group, policy and read ops.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use super::config::{LocalFallback, RegistrationPolicy};
use crate::acl::{GrantEffect, RbacAction, ResourceSelector};

/// One graph grant of a role, projected into the RBAC image under the role
/// `idm:<role_id>`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RoleGraphGrant {
    pub resource: ResourceSelector,
    pub action: RbacAction,
    pub effect: GrantEffect,
}

/// `upsert_role`. A built-in role's id cannot be reused for another shape;
/// its scopes and grants may be edited.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RoleUpsert {
    pub role_id: String,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default)]
    pub scopes: BTreeSet<String>,
    #[serde(default)]
    pub graph_grants: Vec<RoleGraphGrant>,
}

/// `upsert_group`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct GroupUpsert {
    pub group_id: String,
    pub name: String,
    #[serde(default)]
    pub roles: BTreeSet<String>,
    #[serde(default)]
    pub mfa_required: bool,
}

/// Whether a membership or role binding is added or removed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum BindingChange {
    Add,
    Remove,
}

/// `change_group_membership`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct GroupMembershipChange {
    pub group_id: String,
    pub principal_id: String,
    pub change: BindingChange,
}

/// `change_user_role`: a role granted to one principal directly.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct UserRoleChange {
    pub principal_id: String,
    pub role_id: String,
    pub change: BindingChange,
}

/// `update_policy`: each present field replaces the stored one. The mode
/// itself only moves through `transition`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PolicyUpdate {
    pub expected_epoch: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub registration_policy: Option<RegistrationPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local_fallback: Option<LocalFallback>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub password_min_chars: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idle_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub absolute_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub privileged_idle_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub privileged_absolute_ms: Option<u64>,
}

/// A read or admin op naming one object.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ObjectRef {
    pub id: String,
}

/// A paged listing: entries strictly after `after`, at most `limit`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ListQuery {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<String>,
    pub limit: u32,
}

/// Search users by normalized username, display name, email or principal id.
/// The cursor is the principal id, matching the stable user-list order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct UserSearch {
    pub query: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<String>,
    pub limit: u32,
}

/// Redacted API-key listing for one principal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PrincipalListQuery {
    pub principal_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<String>,
    pub limit: u32,
}

/// `import_sql`: an administrator's dump (see `sql_dump`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SqlDump {
    pub sql: String,
}

/// Largest page any listing returns.
pub const MAX_PAGE: u32 = 500;
