//! Declared results of the `security` contract domain.

use serde::{Deserialize, Serialize};

use crate::acl::{AgentIdentity, Grant, Role};
use crate::protocol::LedgerReadResult;
#[cfg(feature = "security")]
use crate::protocol::{AuditReport, MerkleInclusionReport};

/// `RbacAdmin` op `RemoveGrant`: whether the grant was present.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RbacGrantRemoval {
    pub removed: bool,
}

/// `RbacAdmin` op `List`: the whole current RBAC policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RbacPolicyListing {
    pub roles: Vec<Role>,
    pub grants: Vec<Grant>,
}

method_results! {
    visit_security;
    GetLedger(GetLedger) => Json<LedgerReadResult>;
    #[cfg(feature = "security")]
    AuditVerify(AuditVerify) => Raw<AuditReport>;
    #[cfg(feature = "security")]
    AuditProveInclusion(AuditProveInclusion) => Raw<MerkleInclusionReport>;
    RegisterIdentity(RegisterIdentity) => Text<String>;
    RbacAddRole(RbacAdmin / "AddRole") => Text<String>;
    RbacRemoveRole(RbacAdmin / "RemoveRole") => Text<String>;
    RbacAddGrant(RbacAdmin / "AddGrant") => Text<String>;
    RbacRemoveGrant(RbacAdmin / "RemoveGrant") => Json<RbacGrantRemoval>;
    RbacList(RbacAdmin / "List") => Json<RbacPolicyListing>;
    // `null` when no identity is registered for the agent; an identity holding no
    // roles is a present identity with an empty `roles` list.
    GetIdentity(GetIdentity) => Json<Option<AgentIdentity>>;
}
