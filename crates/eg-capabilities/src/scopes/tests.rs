use super::*;

/// Actions an op decides at run time (op-level `authz_action`s and the
/// request boundary's controller scopes) on top of the ledger rows.
const OP_LEVEL_ACTIONS: &[&str] = &[
    "admin:connector-pack",
    "admin:decision-log",
    "admin:fleet-catalog",
    "agent:component-read",
    "agent:decision-evaluate",
    "agent:decision-read",
    "agent:graph-read",
    "agent:library-read",
    "agent:pack-read",
    "agent:template-read",
    "capacity:admin",
    "capacity:lease",
    "capacity:read",
    "capacity:throttle",
    "connector:write-back-read",
    "identity:admin",
    "identity:authenticate",
    "identity:provision",
    "identity:read",
    "identity:self",
    "kg:admin",
    "modality:read",
    "rbac:approve-elevation",
    "rbac:elevation",
    "rbac:elevation-read",
    "semantic:binding-read",
    "semantic:source-admit",
    "semantic:stage-claim",
    "semantic:stage-complete",
    "semantic:stage-read",
];

#[test]
fn the_registry_is_strictly_sorted_so_lookup_is_exact() {
    for pair in SCOPES.windows(2) {
        assert!(
            pair[0].scope < pair[1].scope,
            "{} !< {}",
            pair[0].scope,
            pair[1].scope
        );
    }
}

#[test]
fn every_authorized_action_is_registered() {
    let mut missing: Vec<String> = crate::method_policy_entries()
        .map(|(_, policy, _)| policy.authz_action)
        .chain(OP_LEVEL_ACTIONS.iter().copied())
        .filter(|action| scope_entry(action).is_none())
        .map(str::to_string)
        .collect();
    missing.sort();
    missing.dedup();
    assert!(missing.is_empty(), "unregistered scopes: {missing:?}");
}

#[test]
fn the_rot_check_catches_an_unregistered_scope() {
    assert!(scope_entry("made:up").is_none());
    assert!(ScopeRegistry.class_of("made:up").is_none());
    assert_eq!(ScopeRegistry.class_of("kg:read"), Some(ScopeClass::User));
}

#[test]
fn every_approver_scope_has_exactly_its_built_in_group() {
    for entry in SCOPES {
        let group = ScopeRegistry.approver_group_of(entry.scope);
        assert_eq!(
            entry.class == ScopeClass::Approver,
            group.is_some(),
            "{}",
            entry.scope
        );
    }
    for (scope, _) in APPROVER_GROUPS {
        assert_eq!(ScopeRegistry.class_of(scope), Some(ScopeClass::Approver));
    }
    let built_in = [
        eg_types::identity::ELEVATION_APPROVERS_GROUP,
        eg_types::identity::LIVE_ORDER_APPROVERS_GROUP,
        eg_types::identity::SCHEMA_APPROVERS_GROUP,
        "action-approvers",
    ];
    for (_, group) in APPROVER_GROUPS {
        assert!(
            built_in.contains(group),
            "{group} is not an identity-store built-in"
        );
    }
    for scope in ["approvals:read", "approvals:decide"] {
        assert_eq!(ScopeRegistry.class_of(scope), Some(ScopeClass::Approver));
        assert_eq!(
            ScopeRegistry.approver_group_of(scope),
            Some("action-approvers")
        );
    }
}

/// GRAPHOS identity-access (R002, R020): GraphOS's `project_domain_scopes`
/// (graph-os commit 51bf4fd511) requires these exact classes and owners from
/// the EG contract, or it fails API registry construction. Pin them here so a
/// future reclassification is caught at the source, not at GraphOS startup.
#[test]
fn graph_os_scope_classes_and_owners_match_the_api_contract() {
    for (scope, class, owner) in [
        ("finance:read", ScopeClass::Domain, "finance"),
        ("fleet:read", ScopeClass::User, "graph-os"),
        ("fleet:control", ScopeClass::Admin, "graph-os"),
        ("loops:read", ScopeClass::User, "graph-os"),
        ("loops:control", ScopeClass::Admin, "graph-os"),
        ("ops:read", ScopeClass::Admin, "graph-os"),
        ("ops:admin", ScopeClass::Admin, "graph-os"),
        ("mcp:discover", ScopeClass::User, "graph-os"),
        ("mcp:delegate", ScopeClass::User, "graph-os"),
        ("mcp:admin", ScopeClass::Admin, "graph-os"),
        ("approvals:read", ScopeClass::Approver, "graph-os"),
        ("approvals:decide", ScopeClass::Approver, "graph-os"),
    ] {
        let entry = scope_entry(scope).expect("GraphOS API scope is registered");
        assert_eq!(entry.class, class, "{scope}");
        assert_eq!(entry.owner, owner, "{scope}");
    }
}

#[test]
fn the_rulings_hold_in_the_registry() {
    for infrastructure in [
        "broker:publish",
        "timeseries:write",
        "compute:finance",
        "capacity:throttle",
        "security:check",
        "telemetry:write",
    ] {
        assert_eq!(
            ScopeRegistry.class_of(infrastructure),
            Some(ScopeClass::ServiceOnly),
            "{infrastructure}"
        );
    }
    for domain in [
        "finance:alerts",
        "finance:track",
        "finance:backfill",
        "finance:propose-order",
    ] {
        assert_eq!(
            ScopeRegistry.class_of(domain),
            Some(ScopeClass::Domain),
            "{domain}"
        );
    }
    for admin in ["kg:admin", "webui:admin", "identity:admin"] {
        assert_eq!(
            ScopeRegistry.class_of(admin),
            Some(ScopeClass::Admin),
            "{admin}"
        );
    }
}
