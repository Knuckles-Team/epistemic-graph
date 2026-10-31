//! Tenant sharing, parent chains, secured reads and ownership claims
//! (MEDULLA-IDENTITY-R610). Pure data and rules; the UQL planner applies the scope
//! predicate this module builds.
//!
//! * A tenant sees its own graph, its ancestors' *shared* graphs nearest
//!   first, and the commons last. Sharing is private by default.
//! * Reads are filtered by tenant and marking before results leave, and every
//!   read yields an audit line.
//! * An admin previews, then applies, a claim over unowned nodes. Applying
//!   stamps owner and classification and is itself audited.
//! * A query is scoped by a tenant predicate on its first bound node variable;
//!   one with no such variable is refused as `QUERY_UNSCOPABLE`.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

/// The commons graph every tenant reads last.
pub const COMMONS_GRAPH: &str = "__commons__";

const MAX_IDENTIFIER_BYTES: usize = 128;

/// A refusal from the tenancy rules.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum TenancyError {
    #[error("INVALID_IDENTIFIER: {field} {value:?}")]
    InvalidIdentifier { field: &'static str, value: String },
    #[error("TENANT_PARENT_CYCLE: {0}")]
    ParentCycle(String),
    #[error("QUERY_UNSCOPABLE: no bindable node variable")]
    QueryUnscopable,
    #[error("CLAIM_NOT_ADMIN: {0}")]
    ClaimNotAdmin(String),
}

impl TenancyError {
    /// The stable refusal code.
    pub fn code(&self) -> &'static str {
        match self {
            Self::InvalidIdentifier { .. } => "INVALID_IDENTIFIER",
            Self::ParentCycle(_) => "TENANT_PARENT_CYCLE",
            Self::QueryUnscopable => "QUERY_UNSCOPABLE",
            Self::ClaimNotAdmin(_) => "CLAIM_NOT_ADMIN",
        }
    }
}

fn validate(field: &'static str, value: &str) -> Result<String, TenancyError> {
    let ok = !value.is_empty()
        && value.len() <= MAX_IDENTIFIER_BYTES
        && value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.' | ':'));
    if ok {
        Ok(value.to_owned())
    } else {
        Err(TenancyError::InvalidIdentifier {
            field,
            value: value.to_owned(),
        })
    }
}

macro_rules! identifier {
    ($(#[$meta:meta])* $name:ident, $field:literal) => {
        $(#[$meta])*
        #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(try_from = "String", into = "String")]
        pub struct $name(String);

        impl $name {
            pub fn new(value: &str) -> Result<Self, TenancyError> {
                validate($field, value).map(Self)
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl TryFrom<String> for $name {
            type Error = TenancyError;
            fn try_from(value: String) -> Result<Self, Self::Error> {
                Self::new(&value)
            }
        }

        impl From<$name> for String {
            fn from(value: $name) -> String {
                value.0
            }
        }
    };
}

identifier!(
    /// A validated tenant identifier.
    TenantId,
    "tenant"
);
identifier!(
    /// A validated marking (classification label).
    Marking,
    "marking"
);
identifier!(
    /// A validated actor identifier.
    ActorId,
    "actor"
);

/// One edge of the tenant parent chain.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TenantParent {
    pub tenant: TenantId,
    pub parent: TenantId,
}

impl TenantParent {
    /// A tenant cannot be its own parent.
    pub fn new(tenant: &str, parent: &str) -> Result<Self, TenancyError> {
        let (tenant, parent) = (TenantId::new(tenant)?, TenantId::new(parent)?);
        if tenant == parent {
            return Err(TenancyError::ParentCycle(tenant.0));
        }
        Ok(Self { tenant, parent })
    }
}

/// The graphs one tenant may read, in precedence order.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccessibleGraphs {
    pub tenant: TenantId,
    pub graphs: Vec<String>,
}

/// The graph a tenant owns.
pub fn tenant_graph(tenant: &TenantId) -> String {
    format!("tenant__{}", tenant.as_str())
}

/// Parent links and which tenants share their graph with descendants.
#[derive(Clone, Debug, Default)]
pub struct TenantTree {
    parents: BTreeMap<TenantId, TenantId>,
    shared: BTreeSet<TenantId>,
}

impl TenantTree {
    /// Register a parent link; a link that closes a cycle refuses.
    pub fn link(&mut self, link: TenantParent) -> Result<(), TenancyError> {
        let mut cursor = Some(&link.parent);
        while let Some(current) = cursor {
            if *current == link.tenant {
                return Err(TenancyError::ParentCycle(link.tenant.0));
            }
            cursor = self.parents.get(current);
        }
        self.parents.insert(link.tenant, link.parent);
        Ok(())
    }

    /// Share a tenant's graph with its descendants. Private until called.
    pub fn share(&mut self, tenant: TenantId) {
        self.shared.insert(tenant);
    }

    /// Ancestors, nearest first.
    pub fn ancestors(&self, tenant: &TenantId) -> Vec<&TenantId> {
        let mut out = Vec::new();
        let mut cursor = self.parents.get(tenant);
        while let Some(parent) = cursor {
            out.push(parent);
            cursor = self.parents.get(parent);
        }
        out
    }

    /// Own graph, then each sharing ancestor's graph nearest first, then the
    /// commons. Union read: the tenant graph stays the only write target.
    pub fn accessible_graphs(&self, tenant: &TenantId) -> AccessibleGraphs {
        let mut graphs = vec![tenant_graph(tenant)];
        graphs.extend(
            self.ancestors(tenant)
                .into_iter()
                .filter(|ancestor| self.shared.contains(*ancestor))
                .map(tenant_graph),
        );
        graphs.push(COMMONS_GRAPH.to_owned());
        AccessibleGraphs {
            tenant: tenant.clone(),
            graphs,
        }
    }
}

/// A node as a secured read sees it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SecuredNode {
    pub id: String,
    /// The graph the node lives in.
    pub graph: String,
    pub marking: Option<Marking>,
}

/// The audit line of one read or claim.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadAudit {
    pub actor: ActorId,
    pub tenant: TenantId,
    pub count: usize,
}

/// Filter nodes to the reader's accessible graphs and held markings, and audit
/// the read. An unmarked node needs no marking; a marked one needs the marking.
pub fn secured_read<'a>(
    actor: &ActorId,
    accessible: &AccessibleGraphs,
    held: &BTreeSet<Marking>,
    nodes: &'a [SecuredNode],
) -> (Vec<&'a SecuredNode>, ReadAudit) {
    let visible: Vec<&SecuredNode> = nodes
        .iter()
        .filter(|node| accessible.graphs.contains(&node.graph))
        .filter(|node| node.marking.as_ref().is_none_or(|m| held.contains(m)))
        .collect();
    let audit = ReadAudit {
        actor: actor.clone(),
        tenant: accessible.tenant.clone(),
        count: visible.len(),
    };
    (visible, audit)
}

/// A node's ownership stamp.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OwnedNode {
    pub id: String,
    pub owner: Option<ActorId>,
    pub classification: Option<Marking>,
}

/// An admin's claim over nodes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OwnershipClaim {
    pub claimant: ActorId,
    pub tenant: TenantId,
    pub classification: Marking,
    pub node_ids: Vec<String>,
}

/// What applying a claim would do: the unowned nodes it takes, and the owned
/// ones it leaves alone.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimPreview {
    pub claim: OwnershipClaim,
    pub claimable: Vec<String>,
    pub already_owned: Vec<String>,
}

/// Preview a claim. Only an admin may claim.
pub fn preview_claim(
    claim: &OwnershipClaim,
    is_admin: bool,
    nodes: &[OwnedNode],
) -> Result<ClaimPreview, TenancyError> {
    if !is_admin {
        return Err(TenancyError::ClaimNotAdmin(
            claim.claimant.as_str().to_owned(),
        ));
    }
    let (mut claimable, mut already_owned) = (Vec::new(), Vec::new());
    for id in &claim.node_ids {
        match nodes.iter().find(|n| &n.id == id) {
            Some(node) if node.owner.is_none() => claimable.push(id.clone()),
            Some(_) => already_owned.push(id.clone()),
            None => {}
        }
    }
    Ok(ClaimPreview {
        claim: claim.clone(),
        claimable,
        already_owned,
    })
}

/// Apply a previewed claim: stamp owner and classification on the claimable
/// nodes only, and audit the claim.
pub fn apply_claim(preview: &ClaimPreview, nodes: &mut [OwnedNode]) -> ReadAudit {
    let mut count = 0;
    for node in nodes
        .iter_mut()
        .filter(|n| n.owner.is_none() && preview.claimable.contains(&n.id))
    {
        node.owner = Some(preview.claim.claimant.clone());
        node.classification = Some(preview.claim.classification.clone());
        count += 1;
    }
    ReadAudit {
        actor: preview.claim.claimant.clone(),
        tenant: preview.claim.tenant.clone(),
        count,
    }
}

/// The tenant predicate injected on the first bound node variable.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TenantPredicate {
    pub variable: String,
    pub tenant: TenantId,
}

/// Scope a query: bind the tenant predicate to the first bound node variable.
/// A query with none is refused rather than run unscoped.
pub fn scope_predicate(
    bound_node_variables: &[String],
    tenant: &TenantId,
) -> Result<TenantPredicate, TenancyError> {
    let variable = bound_node_variables
        .iter()
        .find(|v| !v.is_empty())
        .ok_or(TenancyError::QueryUnscopable)?;
    Ok(TenantPredicate {
        variable: variable.clone(),
        tenant: tenant.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tenant(id: &str) -> TenantId {
        TenantId::new(id).unwrap()
    }

    fn actor(id: &str) -> ActorId {
        ActorId::new(id).unwrap()
    }

    fn marking(id: &str) -> Marking {
        Marking::new(id).unwrap()
    }

    // spec: MEDULLA-IDENTITY-R610.1
    #[test]
    fn types_round_trip_and_reject_invalid_identifiers() {
        let link = TenantParent::new("acme", "root").unwrap();
        let graphs = TenantTree::default().accessible_graphs(&tenant("acme"));
        let preview = ClaimPreview {
            claim: OwnershipClaim {
                claimant: actor("usr:alice"),
                tenant: tenant("acme"),
                classification: marking("internal"),
                node_ids: vec!["n1".into()],
            },
            claimable: vec!["n1".into()],
            already_owned: vec![],
        };
        let audit = ReadAudit {
            actor: actor("usr:alice"),
            tenant: tenant("acme"),
            count: 3,
        };
        macro_rules! round_trip {
            ($value:expr, $ty:ty) => {{
                let json = serde_json::to_string(&$value).unwrap();
                assert_eq!(serde_json::from_str::<$ty>(&json).unwrap(), $value);
            }};
        }
        round_trip!(link, TenantParent);
        round_trip!(graphs, AccessibleGraphs);
        round_trip!(preview.claim, OwnershipClaim);
        round_trip!(preview, ClaimPreview);
        round_trip!(audit, ReadAudit);
        round_trip!(marking("secret"), Marking);

        for bad in ["", "has space", "semi;colon", &"x".repeat(129)] {
            assert_eq!(TenantId::new(bad).unwrap_err().code(), "INVALID_IDENTIFIER");
        }
        assert!(Marking::new("a/b").is_err() && ActorId::new("").is_err());
        assert!(serde_json::from_str::<TenantId>("\"bad id\"").is_err());
        assert!(TenantParent::new("bad id", "root").is_err());
        assert!(TenantParent::new("a", "a").is_err());
    }

    fn secured(id: &str, graph: &str, mark: Option<&str>) -> SecuredNode {
        SecuredNode {
            id: id.into(),
            graph: graph.into(),
            marking: mark.map(marking),
        }
    }

    // spec: MEDULLA-IDENTITY-R610.2
    #[test]
    fn graph_precedence_marking_filter_claims_and_scope() {
        // Precedence: own, sharing ancestors nearest-first, commons last.
        let mut tree = TenantTree::default();
        tree.link(TenantParent::new("team", "dept").unwrap())
            .unwrap();
        tree.link(TenantParent::new("dept", "corp").unwrap())
            .unwrap();
        // Private by default: nothing shared, only own + commons.
        assert_eq!(
            tree.accessible_graphs(&tenant("team")).graphs,
            ["tenant__team", "__commons__"]
        );
        tree.share(tenant("corp"));
        tree.share(tenant("dept"));
        let accessible = tree.accessible_graphs(&tenant("team"));
        assert_eq!(
            accessible.graphs,
            [
                "tenant__team",
                "tenant__dept",
                "tenant__corp",
                "__commons__"
            ]
        );
        // A parent never sees a child's graph; a cycle is refused.
        assert_eq!(
            tree.accessible_graphs(&tenant("dept")).graphs,
            ["tenant__dept", "tenant__corp", "__commons__"]
        );
        let cycle = tree.link(TenantParent::new("corp", "team").unwrap());
        assert_eq!(cycle.unwrap_err().code(), "TENANT_PARENT_CYCLE");

        // Reads filter by graph and marking, then audit the count.
        let nodes = [
            secured("own", "tenant__team", None),
            secured("secret", "tenant__team", Some("secret")),
            secured("up", "tenant__corp", Some("internal")),
            secured("sibling", "tenant__other", None),
            secured("common", "__commons__", None),
        ];
        let held: BTreeSet<Marking> = [marking("internal")].into();
        let (seen, audit) = secured_read(&actor("usr:bob"), &accessible, &held, &nodes);
        let ids: Vec<&str> = seen.iter().map(|n| n.id.as_str()).collect();
        assert_eq!(ids, ["own", "up", "common"]);
        assert_eq!((audit.count, audit.tenant.as_str()), (3, "team"));
        let (none, empty_audit) =
            secured_read(&actor("usr:bob"), &accessible, &BTreeSet::new(), &[]);
        assert!(none.is_empty() && empty_audit.count == 0);

        // Claims: preview then apply, only unowned nodes, only by an admin.
        let mut owned = vec![
            OwnedNode {
                id: "a".into(),
                owner: None,
                classification: None,
            },
            OwnedNode {
                id: "b".into(),
                owner: Some(actor("usr:zed")),
                classification: None,
            },
        ];
        let claim = OwnershipClaim {
            claimant: actor("usr:alice"),
            tenant: tenant("team"),
            classification: marking("internal"),
            node_ids: vec!["a".into(), "b".into(), "ghost".into()],
        };
        assert_eq!(
            preview_claim(&claim, false, &owned).unwrap_err().code(),
            "CLAIM_NOT_ADMIN"
        );
        let preview = preview_claim(&claim, true, &owned).unwrap();
        assert_eq!(
            (
                preview.claimable.as_slice(),
                preview.already_owned.as_slice()
            ),
            (&["a".to_string()][..], &["b".to_string()][..])
        );
        assert_eq!(owned[0].owner, None, "a preview changes nothing");
        let audit = apply_claim(&preview, &mut owned);
        assert_eq!(audit.count, 1);
        assert_eq!(owned[0].owner, Some(actor("usr:alice")));
        assert_eq!(owned[0].classification, Some(marking("internal")));
        assert_eq!(
            owned[1].owner,
            Some(actor("usr:zed")),
            "an owned node is never retaken"
        );

        // Scope predicate: first bound variable, else refuse.
        let predicate =
            scope_predicate(&["".into(), "n".into(), "m".into()], &tenant("team")).unwrap();
        assert_eq!(predicate.variable, "n");
        assert_eq!(
            scope_predicate(&[], &tenant("team")).unwrap_err().code(),
            "QUERY_UNSCOPABLE"
        );
    }
}
