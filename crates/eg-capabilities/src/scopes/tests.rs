use super::*;

/// Actions an op decides at run time (op-level `authz_action`s and the
/// request boundary's controller scopes) on top of the ledger rows.
const OP_LEVEL_ACTIONS: &[&str] = &[
    "admin:connector-pack",
    "admin:decision-catalog",
    "admin:decision-head",
    "admin:decision-log",
    "admin:decision-policy",
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
        "action-approvers",
        "elevation-approvers",
        "live-order-approvers",
        "schema-approvers",
    ];
    for (_, group) in APPROVER_GROUPS {
        assert!(
            built_in.contains(group),
            "{group} is not an identity-store built-in"
        );
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
    for scope in ["approvals:read", "approvals:decide"] {
        assert_eq!(ScopeRegistry.class_of(scope), Some(ScopeClass::Approver));
        assert_eq!(
            ScopeRegistry.approver_group_of(scope),
            Some("action-approvers")
        );
    }
}

#[test]
fn graph_os_scope_classes_and_owners_match_the_api_contract() {
    for (scope, class, owner) in [
        ("finance:read", ScopeClass::Domain, "finance"),
        ("finance:paper-trade", ScopeClass::Domain, "finance"),
        ("fleet:read", ScopeClass::User, "graph-os"),
        ("fleet:control", ScopeClass::Admin, "graph-os"),
        ("loops:read", ScopeClass::User, "graph-os"),
        ("loops:control", ScopeClass::Admin, "graph-os"),
        ("ops:read", ScopeClass::Admin, "graph-os"),
        ("ops:admin", ScopeClass::Admin, "graph-os"),
        ("mcp:discover", ScopeClass::User, "graph-os"),
        ("mcp:delegate", ScopeClass::User, "graph-os"),
        ("mcp:admin", ScopeClass::Admin, "graph-os"),
    ] {
        let entry = scope_entry(scope).expect("GraphOS API scope is registered");
        assert_eq!(entry.class, class, "{scope}");
        assert_eq!(entry.owner, owner, "{scope}");
    }
    assert!(scope_entry("mcp:*").is_none());
    assert!(scope_entry("fleet:*").is_none());
}

#[test]
fn newly_exposed_engine_actions_have_least_privilege_classes() {
    let audit_write = scope_entry("security:audit-write").expect("audit append scope is registered");
    assert_eq!(audit_write.class, ScopeClass::User);
    assert_eq!(audit_write.owner, "engine");
    for scope in [
        "gap:read",
        "gap:write",
        "telemetry:derive",
        "work:offer-write",
        "policy:read",
        "usage:read",
    ] {
        assert_eq!(ScopeRegistry.class_of(scope), Some(ScopeClass::User));
    }
    assert_eq!(
        ScopeRegistry.class_of("policy:capture-write"),
        Some(ScopeClass::ServiceOnly)
    );
    for scope in [
        "record:retire",
        "admin:policy-capability",
        "admin:model-policy-register",
        "admin:training-run-write",
        "admin:policy-evaluation-write",
        "admin:policy-evolution-store",
    ] {
        assert_eq!(ScopeRegistry.class_of(scope), Some(ScopeClass::Admin));
    }
    for scope in ["usage:read", "admin:policy-evolution-store"] {
        assert_eq!(scope_entry(scope).map(|entry| entry.owner), Some("engine"));
    }
}
