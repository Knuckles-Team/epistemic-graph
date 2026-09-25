//! Directory provisioning (IDM-14 wire shapes): SCIM `Users` / `Groups` and
//! the LDAP group sync push subjects and directory groups into the store.
//!
//! A directory group is NOT an engine group and grants nothing by itself:
//! only the IdP's mapping rules (claim path `groups`, value = the directory
//! group's display name) turn membership into roles or engine groups, so the
//! privileged-rule gate stays the one grant path.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::views::UserView;

/// `idp.provision`: create, update or deprovision the principal a directory
/// subject is linked to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ProvisionSubject {
    pub idp_id: String,
    /// The link key within the IdP (SCIM `externalId` / LDAP DN or GUID).
    pub subject: String,
    pub username: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
    pub active: bool,
    /// `claim_path → values`, the same shape as `external_login`.
    #[serde(default)]
    pub claims: BTreeMap<String, Vec<String>>,
}

/// `idp.list_provisioned`: the subjects linked to one IdP, ordered by
/// principal id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ProvisionedQuery {
    pub idp_id: String,
    /// Principal id to page after.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<String>,
    pub limit: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub principal_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
}

/// One linked subject and its principal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ProvisionedUser {
    pub subject: String,
    pub user: UserView,
}

/// A directory group (SCIM `Groups`, an LDAP group). Its membership is
/// authoritative: an upsert replaces it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct DirectoryGroup {
    pub idp_id: String,
    /// Caller-generated, unique per IdP.
    pub group_id: String,
    pub display_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub external_id: Option<String>,
    /// Principal ids; each must be linked to this IdP.
    #[serde(default)]
    pub members: BTreeSet<String>,
}

/// `idp.remove_directory_group`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct DirectoryGroupRef {
    pub idp_id: String,
    pub group_id: String,
}

/// `idp.list_directory_groups`, ordered by group id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct DirectoryGroupQuery {
    pub idp_id: String,
    /// Group id to page after.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<String>,
    pub limit: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub external_id: Option<String>,
}
