use super::*;
#[cfg(feature = "security")]
use std::collections::HashSet;

/// One graph-access question, as the chokepoint receives it. `now_ms` is the
/// clock an elevation's hard expiry is checked against; the ownership fields
/// matter only to the non-RBAC ACL build.
#[derive(Debug, Clone, Copy)]
pub struct AccessQuery<'a> {
    pub agent_id: &'a str,
    pub graph_name: &'a str,
    pub graph_type: GraphType,
    pub graph_owner: Option<&'a str>,
    pub access: AccessLevel,
    pub now_ms: u64,
}

/// Why the chokepoint allowed (or refused) one graph access.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AccessBasis {
    Denied,
    /// A System identity, a standing RBAC grant, or (non-security builds) the
    /// ownership ACL.
    Standing,
    /// Allowed only by the named just-in-time elevation (EH-404). Callers
    /// audit every such use.
    Elevation(String),
}

impl AccessBasis {
    pub fn is_allowed(&self) -> bool {
        !matches!(self, Self::Denied)
    }
}

/// Stable explanation of the decision made by the access chokepoint.
/// Token scope is enforced by the request envelope before this graph decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccessReasonCode {
    UnknownPrincipal,
    SystemIdentity,
    StandingGrant,
    ExplicitDeny,
    NoMatchingGrant,
    Elevation,
    OwnershipAcl,
    AclDenied,
}

impl AccessReasonCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::UnknownPrincipal => "UNKNOWN_PRINCIPAL",
            Self::SystemIdentity => "SYSTEM_IDENTITY",
            Self::StandingGrant => "STANDING_GRANT",
            Self::ExplicitDeny => "EXPLICIT_DENY",
            Self::NoMatchingGrant => "NO_MATCHING_GRANT",
            Self::Elevation => "ELEVATION",
            Self::OwnershipAcl => "OWNERSHIP_ACL",
            Self::AclDenied => "ACL_DENIED",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccessDecision {
    pub basis: AccessBasis,
    pub reason_code: AccessReasonCode,
}

impl AccessDecision {
    pub fn is_allowed(&self) -> bool {
        self.basis.is_allowed()
    }
}

/// Wall-clock milliseconds: the clock an elevation's hard expiry is checked
/// against on every access decision.
pub fn access_clock_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| {
            u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
        })
}

impl IsolationLayer {
    pub fn check_access(
        &self,
        agent_id: &str,
        graph_name: &str,
        graph_type: GraphType,
        graph_owner: Option<&str>,
        access: AccessLevel,
    ) -> bool {
        self.access_decision(&AccessQuery {
            agent_id,
            graph_name,
            graph_type,
            graph_owner,
            access,
            now_ms: access_clock_ms(),
        })
        .is_allowed()
    }

    /// The access decision, naming what allowed it. An elevation is consulted
    /// only when no standing grant decided: an explicit RBAC `Deny` is never
    /// overridden by an elevation.
    pub fn access_basis(&self, query: &AccessQuery<'_>) -> AccessBasis {
        self.access_decision(query).basis
    }

    /// A decision and its reason from one evaluation of the engine's policy.
    pub fn access_decision(&self, query: &AccessQuery<'_>) -> AccessDecision {
        let Some(identity) = self.agents.get(query.agent_id) else {
            return AccessDecision {
                basis: AccessBasis::Denied,
                reason_code: AccessReasonCode::UnknownPrincipal,
            };
        };
        if identity.role == AgentRole::System {
            return AccessDecision {
                basis: AccessBasis::Standing,
                reason_code: AccessReasonCode::SystemIdentity,
            };
        }
        self.check_non_system_access(identity, query)
    }

    #[cfg(feature = "security")]
    fn check_non_system_access(
        &self,
        identity: &AgentIdentity,
        query: &AccessQuery<'_>,
    ) -> AccessDecision {
        // Mandatory RBAC means no pre-RBAC ACL fall-through on empty/no-match.
        let context = crate::acl::ResourceContext::graph(query.graph_name);
        let (action, elevation_action) = match query.access {
            AccessLevel::Read => (
                crate::acl::RbacAction::Read,
                eg_types::rbac_elevation::ElevationAction::Read,
            ),
            AccessLevel::Write => (
                crate::acl::RbacAction::Write,
                eg_types::rbac_elevation::ElevationAction::Write,
            ),
        };
        match self.rbac.evaluate(&identity.roles, &context, action) {
            Some(crate::acl::GrantEffect::Allow) => AccessDecision {
                basis: AccessBasis::Standing,
                reason_code: AccessReasonCode::StandingGrant,
            },
            Some(crate::acl::GrantEffect::Deny) => AccessDecision {
                basis: AccessBasis::Denied,
                reason_code: AccessReasonCode::ExplicitDeny,
            },
            None => match self.rbac.elevations().permitting(
                query.agent_id,
                query.graph_name,
                elevation_action,
                query.now_ms,
            ) {
                Some(id) => AccessDecision {
                    basis: AccessBasis::Elevation(id.to_string()),
                    reason_code: AccessReasonCode::Elevation,
                },
                None => AccessDecision {
                    basis: AccessBasis::Denied,
                    reason_code: AccessReasonCode::NoMatchingGrant,
                },
            },
        }
    }

    #[cfg(not(feature = "security"))]
    fn check_non_system_access(
        &self,
        identity: &AgentIdentity,
        query: &AccessQuery<'_>,
    ) -> AccessDecision {
        let allowed = match query.graph_type {
            GraphType::Commons => true,
            GraphType::Global => query.access == AccessLevel::Read,
            GraphType::Agent => {
                query.graph_owner == Some(query.agent_id)
                    || query
                        .graph_owner
                        .is_some_and(|owner| self.is_manager_of(query.agent_id, owner))
            }
            GraphType::Team => {
                let team_name = query
                    .graph_name
                    .strip_prefix("team:")
                    .unwrap_or(query.graph_name);
                identity.teams.contains(&team_name.to_string())
                    && (query.access == AccessLevel::Read
                        || matches!(identity.role, AgentRole::Manager { .. }))
            }
        };
        if allowed {
            AccessDecision {
                basis: AccessBasis::Standing,
                reason_code: AccessReasonCode::OwnershipAcl,
            }
        } else {
            AccessDecision {
                basis: AccessBasis::Denied,
                reason_code: AccessReasonCode::AclDenied,
            }
        }
    }

    fn is_manager_of(&self, agent_id: &str, subordinate_id: &str) -> bool {
        self.agents.get(agent_id).is_some_and(|identity| {
            matches!(
                &identity.role,
                AgentRole::Manager { subordinates }
                    if subordinates.iter().any(|id| id == subordinate_id)
            )
        })
    }

    #[cfg(feature = "security")]
    pub fn is_system(&self, agent_id: &str) -> bool {
        self.agents
            .get(agent_id)
            .is_some_and(|identity| identity.role == AgentRole::System)
    }

    #[cfg(feature = "security")]
    pub fn can_see_row(&self, agent_id: &str, visibility: &RowVisibility) -> bool {
        let Some(owner) = visibility.owner.as_deref() else {
            return self.is_system(agent_id)
                || visibility.schema
                || (visibility.tagged && visibility.public);
        };
        self.is_system(agent_id)
            || visibility.schema
            || visibility.public
            || owner == agent_id
            || visibility.grants.iter().any(|grant| grant == agent_id)
            || self.is_manager_of(agent_id, owner)
    }

    #[cfg(feature = "security")]
    pub fn can_see_node(&self, agent_id: &str, view: &crate::graph::GraphView, id: &str) -> bool {
        let mut visibility = view
            .visibility_index
            .get(id)
            .cloned()
            .or_else(|| {
                view.node_properties
                    .get(id)
                    .map(|blob| row_visibility(blob))
            })
            .unwrap_or_else(RowVisibility::default_public);
        visibility.schema = view.schema_node_ids.contains(id);
        self.can_see_row(agent_id, &visibility)
    }

    #[cfg(feature = "security")]
    pub fn filter_view(&self, agent_id: &str, view: &mut crate::graph::GraphView) {
        let hidden: HashSet<String> = view
            .node_map
            .keys()
            .filter(|id| !self.can_see_node(agent_id, view, id))
            .cloned()
            .collect();
        for id in &hidden {
            if let Some(index) = view.node_map.remove(id) {
                view.graph.remove_node(index);
            }
            view.node_properties.remove(id);
        }
        view.edge_properties
            .retain(|(source, target), _| !hidden.contains(source) && !hidden.contains(target));
    }

    pub fn has_admin_capability(&self, agent_id: &str) -> bool {
        let Some(identity) = self.agents.get(agent_id) else {
            return false;
        };
        if identity.role == AgentRole::System {
            return true;
        }
        #[cfg(feature = "security")]
        {
            let context = crate::acl::ResourceContext::graph("__admin__");
            matches!(
                self.rbac
                    .evaluate(&identity.roles, &context, crate::acl::RbacAction::Admin,),
                Some(crate::acl::GrantEffect::Allow)
            )
        }
        #[cfg(not(feature = "security"))]
        false
    }
}
