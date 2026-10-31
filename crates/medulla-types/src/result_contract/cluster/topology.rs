//! Placement and cluster topology result bodies of the `cluster` domain.

use serde::{Deserialize, Serialize};

use crate::epistemic_operations::PlacementRouteSchemaVersion;

/// The `PlacementRoute` wire response: the schema-locked route fields plus
/// `endpoints`, the client-reachable members of the resolved group, leader first
/// (empty when no cluster topology is known).
///
/// Deliberately a separate type from [`crate::epistemic_operations::PlacementRoute`],
/// whose cross-repo DTO carries no deployment endpoint material and denies unknown
/// fields: a consumer of a route response must decode this tolerant superset.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PlacementRouteWire {
    pub schema_version: PlacementRouteSchemaVersion,
    pub route_id: String,
    pub tenant_ref: String,
    pub partition_ref: String,
    pub authoritative: bool,
    pub placed: bool,
    pub group: u64,
    pub epoch: u64,
    pub fencing_token: u64,
    pub stale: bool,
    pub leader_ref: Option<String>,
    pub endpoints: Vec<String>,
}

/// The current leader of one Raft group.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ClusterLeader {
    pub group_id: u64,
    pub node_id: u64,
}

/// Certificate metadata a member self-reported. Never key material.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ClusterMemberCertificate {
    pub id: Option<String>,
    pub rotation_epoch: u64,
    pub not_before_ms: Option<u64>,
    pub not_after_ms: Option<u64>,
}

/// One member of a Raft group.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ClusterMember {
    pub node_id: u64,
    pub member_identity: String,
    /// `leader`, `follower` or `learner`.
    pub role: String,
    pub client_endpoint: String,
    pub tls_name: Option<String>,
    /// `healthy` when the group has an elected leader, else `degraded`.
    pub health: String,
    pub certificate: ClusterMemberCertificate,
}

/// One Raft group and its members.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ClusterGroup {
    pub group_id: u64,
    pub leader_id: Option<u64>,
    pub members: Vec<ClusterMember>,
}

/// Digests of the verified request context a discovery snapshot is signed for.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct DiscoveryAuthBinding {
    pub tenant_digest: String,
    pub principal_digest: String,
    pub agent_digest: String,
}

/// `ClusterMembers`: a signed, context-bound cluster topology snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ClusterDiscoverySnapshot {
    pub schema_version: u64,
    pub cluster_id: String,
    pub membership_epoch: u64,
    pub placement_epoch: u64,
    pub leader: Option<ClusterLeader>,
    pub leaders: Vec<ClusterLeader>,
    pub groups: Vec<ClusterGroup>,
    pub auth_binding: DiscoveryAuthBinding,
    /// `hmac-sha256:<hex>` over the canonical snapshot.
    pub signature: String,
}

/// `PlacementAdmin` op `assign`: the new routing epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PlacementEpoch {
    pub epoch: u64,
}

/// One graph a placement move transferred between Raft groups.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct GroupReshardResult {
    pub graph: String,
    pub from_group: u64,
    pub to_group: u64,
    pub nodes_transferred: u64,
}

/// `PlacementAdmin` op `move`: the settled online partition move.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PlacementMoveResult {
    pub tenant: String,
    /// `[range_start, range_end]`.
    pub range: (u64, u64),
    pub target: u64,
    pub epoch: u64,
    pub graphs: Vec<GroupReshardResult>,
}
