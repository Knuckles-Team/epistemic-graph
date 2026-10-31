//! Principal-owned knowledge units with explicit sharing scope.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

/// The closed sharing scope of one knowledge unit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ShareScope {
    Private,
    Placement,
    Tenant,
}

/// One versioned unit of principal-owned knowledge.
///
/// The visibility behavior is a separate criterion; this model only carries
/// the identity, lineage and explicit sharing declaration that behavior reads.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct KnowledgeUnit {
    pub id: String,
    pub owner: String,
    pub version: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supersedes: Option<String>,
    #[serde(default)]
    pub access_labels: BTreeSet<String>,
    pub share_scope: ShareScope,
}

/// A principal as the visibility rules see it: identity plus the placement and
/// tenant a `Placement` or `Tenant` share is matched against.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnitPrincipal {
    pub id: String,
    pub placement: String,
    pub tenant: String,
}

/// Whether `reader` may see `unit`: its own units, or a unit whose owner shares
/// it at a scope the reader matches. `Private` never leaves its owner.
pub type ShareRule = fn(&UnitPrincipal, &KnowledgeUnit, &BTreeMap<String, UnitPrincipal>) -> bool;

/// Which of the candidate units of one lineage survive.
pub type LineageRule = fn(Vec<&KnowledgeUnit>) -> Vec<&KnowledgeUnit>;

/// The two named, replaceable visibility rules.
#[derive(Clone, Copy)]
pub struct VisibilityRules {
    pub share: ShareRule,
    pub lineage: LineageRule,
}

/// Default share rule: owner, or a matching `Placement`/`Tenant` scope.
pub fn owner_or_matching_scope(
    reader: &UnitPrincipal,
    unit: &KnowledgeUnit,
    owners: &BTreeMap<String, UnitPrincipal>,
) -> bool {
    if unit.owner == reader.id {
        return true;
    }
    let Some(owner) = owners.get(&unit.owner) else {
        return false;
    };
    match unit.share_scope {
        ShareScope::Private => false,
        ShareScope::Placement => {
            owner.tenant == reader.tenant && owner.placement == reader.placement
        }
        ShareScope::Tenant => owner.tenant == reader.tenant,
    }
}

/// Default lineage rule: keep the highest version, the greater id on a tie.
pub fn highest_version(mut lineage: Vec<&KnowledgeUnit>) -> Vec<&KnowledgeUnit> {
    lineage.sort_by(|a, b| (a.version, &a.id).cmp(&(b.version, &b.id)));
    lineage.pop().into_iter().collect()
}

/// The defaults: [`owner_or_matching_scope`] and [`highest_version`].
pub const DEFAULT_VISIBILITY_RULES: VisibilityRules = VisibilityRules {
    share: owner_or_matching_scope,
    lineage: highest_version,
};

fn lineage_root<'a>(
    unit: &'a KnowledgeUnit,
    by_id: &BTreeMap<&'a str, &'a KnowledgeUnit>,
) -> &'a str {
    let mut current = unit;
    let mut hops = 0;
    while let Some(parent) = current.supersedes.as_deref().and_then(|id| by_id.get(id)) {
        current = parent;
        hops += 1;
        if hops > by_id.len() {
            break;
        }
    }
    &current.id
}

/// The units `reader` may see, with every superseded version removed. Lineage
/// is judged over the units the reader can already see, so a foreign unit the
/// reader may not read never hides a version it may. Output is ordered by id.
pub fn visible_units_with<'a>(
    reader: &UnitPrincipal,
    owners: &BTreeMap<String, UnitPrincipal>,
    units: &'a [KnowledgeUnit],
    rules: &VisibilityRules,
) -> Vec<&'a KnowledgeUnit> {
    let shared: Vec<&KnowledgeUnit> = units
        .iter()
        .filter(|unit| (rules.share)(reader, unit, owners))
        .collect();
    let by_id: BTreeMap<&str, &KnowledgeUnit> =
        shared.iter().map(|u| (u.id.as_str(), *u)).collect();
    let mut lineages: BTreeMap<&str, Vec<&KnowledgeUnit>> = BTreeMap::new();
    for unit in &shared {
        lineages
            .entry(lineage_root(unit, &by_id))
            .or_default()
            .push(unit);
    }
    let mut visible: Vec<&KnowledgeUnit> = lineages
        .into_values()
        .flat_map(|lineage| (rules.lineage)(lineage))
        .collect();
    visible.sort_by(|a, b| a.id.cmp(&b.id));
    visible
}

/// [`visible_units_with`] under the default rules.
pub fn visible_units<'a>(
    reader: &UnitPrincipal,
    owners: &BTreeMap<String, UnitPrincipal>,
    units: &'a [KnowledgeUnit],
) -> Vec<&'a KnowledgeUnit> {
    visible_units_with(reader, owners, units, &DEFAULT_VISIBILITY_RULES)
}

#[cfg(test)]
mod tests {
    use super::*;

    // spec: MEDULLA-IDENTITY-R023.1
    #[test]
    fn unit_round_trips() {
        let unit = KnowledgeUnit {
            id: "knowledge:unit-1".to_string(),
            owner: "usr:alice".to_string(),
            version: 3,
            supersedes: Some("knowledge:unit-0".to_string()),
            access_labels: BTreeSet::from(["sensitive".to_string(), "research".to_string()]),
            share_scope: ShareScope::Placement,
        };
        let encoded = serde_json::to_vec(&unit).expect("knowledge unit serializes");
        let decoded: KnowledgeUnit =
            serde_json::from_slice(&encoded).expect("knowledge unit deserializes");
        assert_eq!(decoded, unit);

        let mut unknown_scope = serde_json::to_value(&unit).expect("knowledge unit is JSON");
        unknown_scope["share_scope"] = serde_json::Value::String("org".to_string());
        assert!(serde_json::from_value::<KnowledgeUnit>(unknown_scope).is_err());
    }

    fn principal(id: &str, placement: &str, tenant: &str) -> UnitPrincipal {
        UnitPrincipal {
            id: id.into(),
            placement: placement.into(),
            tenant: tenant.into(),
        }
    }

    fn unit(
        id: &str,
        owner: &str,
        version: u64,
        supersedes: Option<&str>,
        scope: ShareScope,
    ) -> KnowledgeUnit {
        KnowledgeUnit {
            id: id.into(),
            owner: owner.into(),
            version,
            supersedes: supersedes.map(str::to_owned),
            access_labels: BTreeSet::new(),
            share_scope: scope,
        }
    }

    fn deny_everyone(
        _: &UnitPrincipal,
        _: &KnowledgeUnit,
        _: &BTreeMap<String, UnitPrincipal>,
    ) -> bool {
        false
    }

    fn keep_all_versions(lineage: Vec<&KnowledgeUnit>) -> Vec<&KnowledgeUnit> {
        lineage
    }

    // spec: MEDULLA-IDENTITY-R023.2
    #[test]
    fn superseded_and_foreign_units_hidden() {
        let alice = principal("usr:alice", "p1", "t1");
        let owners: BTreeMap<String, UnitPrincipal> = [
            alice.clone(),
            principal("usr:bob", "p1", "t1"),
            principal("usr:carol", "p2", "t1"),
            principal("usr:dave", "p9", "t2"),
        ]
        .into_iter()
        .map(|p| (p.id.clone(), p))
        .collect();
        let units = vec![
            unit("u1", "usr:alice", 1, None, ShareScope::Private),
            unit("u2", "usr:alice", 2, Some("u1"), ShareScope::Private),
            unit("b-priv", "usr:bob", 1, None, ShareScope::Private),
            unit("b-place", "usr:bob", 1, None, ShareScope::Placement),
            unit("c-place", "usr:carol", 1, None, ShareScope::Placement),
            unit("c-tenant", "usr:carol", 1, None, ShareScope::Tenant),
            unit("d-tenant", "usr:dave", 1, None, ShareScope::Tenant),
            unit("t1", "usr:bob", 1, None, ShareScope::Tenant),
            unit("t2", "usr:bob", 5, Some("t1"), ShareScope::Tenant),
        ];
        let ids = |reader: &UnitPrincipal| -> Vec<String> {
            visible_units(reader, &owners, &units)
                .iter()
                .map(|u| u.id.clone())
                .collect()
        };
        // Own newest version, same-placement and same-tenant shares; no superseded
        // version, no private foreign unit, no other placement's placement share,
        // no other tenant's unit.
        assert_eq!(ids(&alice), ["b-place", "c-tenant", "t2", "u2"]);
        let bob = owners["usr:bob"].clone();
        assert_eq!(ids(&bob), ["b-place", "b-priv", "c-tenant", "t2"]);
        let dave = owners["usr:dave"].clone();
        assert_eq!(ids(&dave), ["d-tenant"]);

        // A foreign private newer version never hides a version the reader may see.
        let hidden_newer = vec![
            unit("s1", "usr:bob", 1, None, ShareScope::Tenant),
            unit("s2", "usr:bob", 2, Some("s1"), ShareScope::Private),
        ];
        let visible = visible_units(&alice, &owners, &hidden_newer);
        assert_eq!(
            visible.iter().map(|u| u.id.as_str()).collect::<Vec<_>>(),
            ["s1"]
        );

        // Each rule is replaceable.
        let deny_all = VisibilityRules {
            share: deny_everyone,
            lineage: highest_version,
        };
        assert!(visible_units_with(&alice, &owners, &units, &deny_all).is_empty());
        let keep_all = VisibilityRules {
            share: owner_or_matching_scope,
            lineage: keep_all_versions,
        };
        assert_eq!(
            visible_units_with(&alice, &owners, &units, &keep_all).len(),
            6
        );
    }
}
