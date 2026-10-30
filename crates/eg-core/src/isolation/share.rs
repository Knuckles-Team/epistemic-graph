//! Pure node-share transition (EG-IDENTITY-R023.13.2).
//!
//! [`share_transition`] decides the next `_shared_scope` and `_grants` values
//! for one node from its current values, a share op and the acting principal.
//! It touches no graph: the cost is O(g) in the grantee count. The caller
//! commits the result through the mutation gateway (EG-IDENTITY-R023.13.3).

use crate::acl::{RbacAdminOp, ShareScope};

/// A node's current share state: the committed `_shared_scope` and `_grants`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeShare {
    /// The node's owner (`_owner`), if tagged.
    pub owner: Option<String>,
    /// The committed `_shared_scope`.
    pub scope: ShareScope,
    /// The committed `_grants`.
    pub grants: Vec<String>,
}

/// The acting principal: its id and whether it holds `kg:admin`.
#[derive(Debug, Clone, Copy)]
pub struct ShareActor<'a> {
    pub principal: &'a str,
    pub kg_admin: bool,
}

/// The next share values for a node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareOutcome {
    /// Next `_shared_scope`.
    pub scope: ShareScope,
    /// Next `_grants`.
    pub grants: Vec<String>,
    /// True when the commit must also copy the node to the commons graph.
    pub promote_to_commons: bool,
}

/// Why a transition was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShareRefusal {
    /// Only the owner or a `kg:admin` holder may change a scope.
    NotOwnerOrAdmin,
    /// The op is not a share op.
    NotAShareOp,
}

/// Compute the next share values. Only the owner or a `kg:admin` holder may
/// change a scope. A revoke clears the grantees and sets `private`. A grant
/// to `commons` also requests a copy of the node into the commons graph.
pub fn share_transition(
    current: &NodeShare,
    op: &RbacAdminOp,
    actor: ShareActor<'_>,
) -> Result<ShareOutcome, ShareRefusal> {
    let (scope, grants) = match op {
        RbacAdminOp::ShareGrant {
            scope, grantees, ..
        } => (*scope, grantees.as_slice().to_vec()),
        RbacAdminOp::ShareRevoke { .. } => (ShareScope::Private, Vec::new()),
        _ => return Err(ShareRefusal::NotAShareOp),
    };
    let is_owner = current.owner.as_deref() == Some(actor.principal);
    if !(is_owner || actor.kg_admin) {
        return Err(ShareRefusal::NotOwnerOrAdmin);
    }
    Ok(ShareOutcome {
        promote_to_commons: scope == ShareScope::Commons,
        scope,
        grants,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::acl::ShareGrantees;

    fn node(scope: ShareScope, grants: &[&str]) -> NodeShare {
        NodeShare {
            owner: Some("owner".into()),
            scope,
            grants: grants.iter().map(|g| g.to_string()).collect(),
        }
    }

    fn grant(scope: ShareScope, ids: &[&str]) -> RbacAdminOp {
        RbacAdminOp::ShareGrant {
            node_id: "n".into(),
            scope,
            grantees: ShareGrantees::new(ids.iter().map(|g| g.to_string()).collect()).unwrap(),
        }
    }

    const OWNER: ShareActor<'static> = ShareActor {
        principal: "owner",
        kg_admin: false,
    };

    // spec: EG-IDENTITY-R023.13.2
    #[test]
    fn revoke_clears_grants() {
        let current = node(ShareScope::Org, &["a", "b"]);
        let revoke = RbacAdminOp::ShareRevoke {
            node_id: "n".into(),
        };
        let out = share_transition(&current, &revoke, OWNER).unwrap();
        assert_eq!(out.scope, ShareScope::Private);
        assert!(out.grants.is_empty());
        assert!(!out.promote_to_commons);
    }

    // spec: EG-IDENTITY-R023.13.2
    #[test]
    fn grant_commons_and_non_owner() {
        let current = node(ShareScope::Private, &[]);
        let org = share_transition(&current, &grant(ShareScope::Org, &["a"]), OWNER).unwrap();
        assert_eq!(
            (org.scope, org.grants.as_slice()),
            (ShareScope::Org, &["a".to_string()][..])
        );
        assert!(!org.promote_to_commons);
        let commons = share_transition(&current, &grant(ShareScope::Commons, &[]), OWNER).unwrap();
        assert!(commons.promote_to_commons);
        let stranger = ShareActor {
            principal: "other",
            kg_admin: false,
        };
        assert_eq!(
            share_transition(&current, &grant(ShareScope::Org, &[]), stranger),
            Err(ShareRefusal::NotOwnerOrAdmin)
        );
        let admin = ShareActor {
            principal: "other",
            kg_admin: true,
        };
        assert!(share_transition(&current, &grant(ShareScope::Org, &[]), admin).is_ok());
        assert_eq!(
            share_transition(&current, &RbacAdminOp::List, OWNER),
            Err(ShareRefusal::NotAShareOp)
        );
    }
}
