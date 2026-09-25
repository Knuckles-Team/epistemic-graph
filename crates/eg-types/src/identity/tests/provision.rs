//! Directory provisioning (SCIM / LDAP sync): the IdP binding, the subject
//! lifecycle, and directory groups that grant only through mapping rules.
//! Every refusal is tested both ways.

use std::collections::{BTreeMap, BTreeSet};

use super::*;

const SCIM_BOT: &str = "svc:scim-okta";

fn upsert_idp(idp_id: &str, kind: IdpKind, config_json: &str, enabled: bool) -> IdentityOp {
    IdentityOp::Idp(IdpOp::Upsert {
        request: IdpConfig {
            idp_id: idp_id.to_string(),
            kind,
            display_name: idp_id.to_string(),
            enabled,
            config_json: config_json.to_string(),
            secret_ref: None,
            jit_policy: JitPolicy::Deny,
            email_domains: Vec::new(),
            order: 0,
            rules: vec![MappingRule {
                rule_id: "engineers".to_string(),
                claim_path: "groups".to_string(),
                match_kind: "equals".to_string(),
                value: "Engineers".to_string(),
                target: "role:eng".to_string(),
                privileged: false,
            }],
        },
    })
}

/// A local store with role `eng`, a SCIM IdP bound to [`SCIM_BOT`], an LDAP
/// IdP, an OIDC IdP and a disabled SCIM IdP.
fn directory_store() -> IdentityStore {
    let mut store = store_in(AuthMode::Local);
    let eng = IdentityOp::Access(AccessOp::UpsertRole {
        request: RoleUpsert {
            role_id: "eng".to_string(),
            name: "eng".to_string(),
            description: None,
            scopes: BTreeSet::from(["kg:read".to_string()]),
            graph_grants: Vec::new(),
        },
    });
    apply_kept(&mut store, &eng, &admin(), NOW).unwrap();
    let bound = format!("{{\"provisioner\":\"{SCIM_BOT}\"}}");
    for op in [
        upsert_idp("scim", IdpKind::Scim, &bound, true),
        upsert_idp("scim-off", IdpKind::Scim, &bound, false),
        upsert_idp("ldap", IdpKind::Ldap, "{}", true),
        upsert_idp("oidc", IdpKind::Oidc, "{}", true),
    ] {
        apply_kept(&mut store, &op, &admin(), NOW).unwrap();
    }
    store
}

fn provisioner(principal: &str) -> IdentityStamp {
    let mut stamp = IdentityStamp::for_actor(actor(principal, &[IDENTITY_PROVISION_SCOPE]));
    stamp.minted_principal_id = Some("usr:minted".to_string());
    stamp
}

fn ldap_broker() -> IdentityStamp {
    let mut stamp = broker();
    stamp.minted_principal_id = Some("usr:minted".to_string());
    stamp
}

fn subject(idp_id: &str, username: &str, active: bool) -> ProvisionSubject {
    ProvisionSubject {
        idp_id: idp_id.to_string(),
        subject: format!("ext-{username}"),
        username: username.to_string(),
        display_name: Some(username.to_string()),
        email: Some(format!("{username}@example.org")),
        active,
        claims: BTreeMap::new(),
    }
}

fn provision(request: ProvisionSubject) -> IdentityOp {
    IdentityOp::Idp(IdpOp::Provision { request })
}

fn provisioned_user(reply: IdentityReply) -> UserView {
    match reply {
        IdentityReply::User(user) => user,
        other => panic!("expected a user, got {other:?}"),
    }
}

#[test]
fn scim_client_admin_binding_is_typed_and_revocable() {
    let mut store = directory_store();
    let service = create(&mut store, "scim-client", UserKind::Service).unwrap();
    let human = create(&mut store, "human", UserKind::Human).unwrap();
    let upsert = |principal_id| {
        IdentityOp::Idp(IdpOp::UpsertScimClient {
            request: ScimClientBinding {
                idp_id: "scim".to_string(),
                principal_id,
            },
        })
    };
    assert_eq!(
        apply_kept(&mut store, &upsert(human), &admin(), NOW),
        Err(IdentityRefusal::KindMismatch)
    );
    assert_eq!(
        apply_kept(&mut store, &upsert(service.clone()), &admin(), NOW),
        Ok(IdentityReply::Done { changed: true })
    );
    let get = IdentityOp::Idp(IdpOp::GetScimClient {
        request: ObjectRef {
            id: "scim".to_string(),
        },
    });
    let reader = IdentityStamp::for_actor(actor("usr:reader", &[IDENTITY_READ_SCOPE]));
    let IdentityReply::ScimClient(binding) = apply_kept(&mut store, &get, &reader, NOW).unwrap()
    else {
        panic!("expected binding")
    };
    assert_eq!(binding.principal_id, service);
    let IdentityReply::ScimClients(all) = apply_kept(
        &mut store,
        &IdentityOp::Idp(IdpOp::ListScimClients),
        &reader,
        NOW,
    )
    .unwrap() else {
        panic!("expected bindings")
    };
    assert!(all
        .iter()
        .any(|row| row.idp_id == "scim" && row.principal_id == service));
    let directory_write = provision(subject("scim", "bound", true));
    assert!(apply_kept(&mut store, &directory_write, &provisioner(&service), NOW).is_ok());
    let remove = IdentityOp::Idp(IdpOp::RemoveScimClient {
        request: ObjectRef {
            id: "scim".to_string(),
        },
    });
    assert_eq!(
        apply_kept(&mut store, &remove, &admin(), NOW),
        Ok(IdentityReply::Done { changed: true })
    );
    assert_eq!(
        apply_kept(&mut store, &get, &reader, NOW),
        Err(IdentityRefusal::NotFound)
    );
    assert_eq!(
        apply_kept(&mut store, &directory_write, &provisioner(&service), NOW),
        Err(IdentityRefusal::NotAuthorized)
    );
}

fn group(members: &[&str]) -> IdentityOp {
    IdentityOp::Idp(IdpOp::ProvisionGroup {
        request: DirectoryGroup {
            idp_id: "scim".to_string(),
            group_id: "g-1".to_string(),
            display_name: "Engineers".to_string(),
            external_id: Some("okta-g-1".to_string()),
            members: members.iter().map(|member| member.to_string()).collect(),
        },
    })
}

#[test]
fn the_broker_reads_the_idp_directory_and_a_non_reader_does_not() {
    let mut store = directory_store();
    let list = IdentityOp::Idp(IdpOp::List);
    for allowed in [
        broker(),
        admin(),
        IdentityStamp::for_actor(actor("usr:auditor", &[IDENTITY_READ_SCOPE])),
    ] {
        let reply = apply_kept(&mut store, &list, &allowed, NOW).unwrap();
        assert!(matches!(reply, IdentityReply::Idps(ref idps) if idps.len() == 4));
    }
    for refused in [IDENTITY_SELF_SCOPE, IDENTITY_PROVISION_SCOPE, "kg:admin"] {
        let stamp = IdentityStamp::for_actor(actor("usr:x", &[refused]));
        assert_eq!(
            apply_kept(&mut store, &list, &stamp, NOW),
            Err(IdentityRefusal::NotAuthorized),
            "{refused} must not read the IdP directory"
        );
    }
}

#[test]
fn a_provisioner_is_bound_to_its_own_scim_idp_and_the_broker_to_ldap() {
    let mut store = directory_store();
    let refused = [
        (provisioner("svc:someone-else"), "scim"),
        (provisioner(SCIM_BOT), "ldap"),
        (provisioner(SCIM_BOT), "oidc"),
        (provisioner(SCIM_BOT), "scim-off"),
        (provisioner(SCIM_BOT), "no-such-idp"),
        (ldap_broker(), "scim"),
        (ldap_broker(), "oidc"),
    ];
    for (stamp, idp_id) in refused {
        assert_eq!(
            apply_kept(
                &mut store,
                &provision(subject(idp_id, "ann", true)),
                &stamp,
                NOW
            ),
            Err(IdentityRefusal::NotAuthorized),
            "{} on {idp_id}",
            stamp.actor.principal_id
        );
    }
    let mut delegated = provisioner(SCIM_BOT);
    delegated.actor.delegated = true;
    let op = provision(subject("scim", "ann", true));
    assert_eq!(
        apply_kept(&mut store, &op, &delegated, NOW),
        Err(IdentityRefusal::NotAuthorized)
    );
    let wrong_scope = IdentityStamp::for_actor(actor(SCIM_BOT, &[IDENTITY_ADMIN_SCOPE]));
    assert_eq!(
        apply_kept(&mut store, &op, &wrong_scope, NOW),
        Err(IdentityRefusal::NotAuthorized),
        "identity:admin does not stand in for identity:provision"
    );
    assert!(!store.manages("usr:minted"), "no refusal created anyone");
    let scim = provisioned_user(apply_kept(&mut store, &op, &provisioner(SCIM_BOT), NOW).unwrap());
    assert_eq!(scim.source, "scim:scim");
    let mut second = ldap_broker();
    second.minted_principal_id = Some("usr:minted-2".to_string());
    let ldap = apply_kept(
        &mut store,
        &provision(subject("ldap", "bea", true)),
        &second,
        NOW,
    );
    assert_eq!(provisioned_user(ldap.unwrap()).source, "ldap:ldap");
}

#[test]
fn a_subject_is_created_updated_deprovisioned_and_restored() {
    let mut store = directory_store();
    let bot = provisioner(SCIM_BOT);
    let never = apply_kept(
        &mut store,
        &provision(subject("scim", "ann", false)),
        &bot,
        NOW,
    );
    assert_eq!(never, Ok(IdentityReply::Done { changed: false }));
    assert!(
        !store.manages("usr:minted"),
        "a deprovisioned subject is never created"
    );
    let created = provisioned_user(
        apply_kept(
            &mut store,
            &provision(subject("scim", "ann", true)),
            &bot,
            NOW,
        )
        .unwrap(),
    );
    assert_eq!(created.principal_id, "usr:minted");
    assert_eq!(created.status, UserStatus::Active);
    assert!(created.roles.contains(USER_ROLE));
    let mut renamed = subject("scim", "ann", true);
    renamed.username = "Ann.Lee".to_string();
    let updated = provisioned_user(apply_kept(&mut store, &provision(renamed), &bot, NOW).unwrap());
    assert_eq!(updated.username, "ann.lee");
    let mut clash = subject("scim", "ann", true);
    clash.username = "root-admin".to_string();
    assert_eq!(
        apply_kept(&mut store, &provision(clash), &bot, NOW),
        Err(IdentityRefusal::Collision)
    );
    let gone = apply_kept(
        &mut store,
        &provision(subject("scim", "ann", false)),
        &bot,
        NOW,
    );
    assert_eq!(
        provisioned_user(gone.unwrap()).status,
        UserStatus::Deprovisioned
    );
    let back = apply_kept(
        &mut store,
        &provision(subject("scim", "ann", true)),
        &bot,
        NOW,
    );
    assert_eq!(provisioned_user(back.unwrap()).status, UserStatus::Active);
    let events: Vec<IdentityEvent> = store.audit_trail().entries().map(|e| e.event).collect();
    assert!(events.contains(&IdentityEvent::UserProvisioned));
    assert!(events.contains(&IdentityEvent::UserDeprovisioned));
}

#[test]
fn a_directory_never_rewrites_a_principal_it_does_not_manage() {
    let mut store = directory_store();
    let local = create(&mut store, "carol", UserKind::Human).unwrap();
    let link = IdentityOp::Idp(IdpOp::Link {
        request: LinkRequest {
            idp_id: "ldap".to_string(),
            subject: "ext-carol".to_string(),
            principal_id: local.clone(),
        },
    });
    apply_kept(&mut store, &link, &admin(), NOW).unwrap();
    let mut renamed = subject("ldap", "carol", true);
    renamed.username = "someone-new".to_string();
    let kept =
        provisioned_user(apply_kept(&mut store, &provision(renamed), &ldap_broker(), NOW).unwrap());
    assert_eq!(
        kept.username, "carol",
        "a local principal keeps its profile"
    );
    let off = apply_kept(
        &mut store,
        &provision(subject("ldap", "carol", false)),
        &ldap_broker(),
        NOW,
    );
    assert_eq!(
        provisioned_user(off.unwrap()).status,
        UserStatus::Deprovisioned
    );
    let on = apply_kept(
        &mut store,
        &provision(subject("ldap", "carol", true)),
        &ldap_broker(),
        NOW,
    );
    assert_eq!(
        provisioned_user(on.unwrap()).status,
        UserStatus::Deprovisioned,
        "only a principal this IdP manages is restored by it"
    );
}

#[test]
fn a_directory_group_grants_only_through_a_mapping_rule() {
    let mut store = directory_store();
    let bot = provisioner(SCIM_BOT);
    let ann = provisioned_user(
        apply_kept(
            &mut store,
            &provision(subject("scim", "ann", true)),
            &bot,
            NOW,
        )
        .unwrap(),
    )
    .principal_id;
    let roles = |store: &IdentityStore| store.resolve(&ann, &TestRegistry).unwrap().roles;
    assert!(!roles(&store).contains("eng"));
    assert_eq!(
        apply_kept(&mut store, &group(&[&ann, BOOTSTRAP_PRINCIPAL]), &bot, NOW),
        Err(IdentityRefusal::NotFound),
        "a member must be linked to this IdP"
    );
    apply_kept(&mut store, &group(&[&ann]), &bot, NOW).unwrap();
    assert!(
        roles(&store).contains("eng"),
        "the rule maps the group name"
    );
    apply_kept(&mut store, &group(&[]), &bot, NOW).unwrap();
    assert!(
        !roles(&store).contains("eng"),
        "membership is authoritative"
    );
    apply_kept(&mut store, &group(&[&ann]), &bot, NOW).unwrap();
    let remove = IdentityOp::Idp(IdpOp::RemoveDirectoryGroup {
        request: DirectoryGroupRef {
            idp_id: "scim".to_string(),
            group_id: "g-1".to_string(),
        },
    });
    assert_eq!(
        apply_kept(&mut store, &remove, &ldap_broker(), NOW),
        Err(IdentityRefusal::NotAuthorized)
    );
    apply_kept(&mut store, &remove, &bot, NOW).unwrap();
    assert!(!roles(&store).contains("eng"));
    assert_eq!(
        apply_kept(&mut store, &remove, &bot, NOW),
        Err(IdentityRefusal::NotFound)
    );
}

#[test]
fn the_provisioned_listings_filter_page_and_stay_bound() {
    let mut store = directory_store();
    for name in ["ann", "bob"] {
        let mut stamp = provisioner(SCIM_BOT);
        stamp.minted_principal_id = Some(format!("usr:{name}"));
        apply_kept(
            &mut store,
            &provision(subject("scim", name, true)),
            &stamp,
            NOW,
        )
        .unwrap();
    }
    let query = |after: Option<&str>, username: Option<&str>| {
        IdentityOp::Idp(IdpOp::ListProvisioned {
            request: ProvisionedQuery {
                idp_id: "scim".to_string(),
                after: after.map(str::to_string),
                limit: 10,
                subject: None,
                principal_id: None,
                username: username.map(str::to_string),
            },
        })
    };
    let listed = |reply: IdentityReply| match reply {
        IdentityReply::Provisioned(rows) => rows
            .into_iter()
            .map(|row| row.user.principal_id)
            .collect::<Vec<_>>(),
        other => panic!("expected provisioned rows, got {other:?}"),
    };
    let bot = provisioner(SCIM_BOT);
    let all = apply_kept(&mut store, &query(None, None), &bot, NOW).unwrap();
    assert_eq!(listed(all), ["usr:ann", "usr:bob"]);
    let paged = apply_kept(&mut store, &query(Some("usr:ann"), None), &bot, NOW).unwrap();
    assert_eq!(listed(paged), ["usr:bob"]);
    let named = apply_kept(&mut store, &query(None, Some("BOB")), &bot, NOW).unwrap();
    assert_eq!(listed(named), ["usr:bob"]);
    assert_eq!(
        apply_kept(
            &mut store,
            &query(None, None),
            &provisioner("svc:other"),
            NOW
        ),
        Err(IdentityRefusal::NotAuthorized)
    );
    apply_kept(&mut store, &group(&["usr:ann"]), &bot, NOW).unwrap();
    let groups = IdentityOp::Idp(IdpOp::ListDirectoryGroups {
        request: DirectoryGroupQuery {
            idp_id: "scim".to_string(),
            after: None,
            limit: 10,
            group_id: None,
            display_name: Some("Engineers".to_string()),
            external_id: None,
        },
    });
    let reply = apply_kept(&mut store, &groups, &bot, NOW).unwrap();
    assert!(matches!(reply, IdentityReply::DirectoryGroups(ref found) if found.len() == 1));
    assert_eq!(
        apply_kept(&mut store, &groups, &ldap_broker(), NOW),
        Err(IdentityRefusal::NotAuthorized)
    );
}

#[test]
fn provisioning_needs_its_minted_id_and_bounded_claims() {
    let mut store = directory_store();
    let unstamped = IdentityStamp::for_actor(actor(SCIM_BOT, &[IDENTITY_PROVISION_SCOPE]));
    let op = provision(subject("scim", "ann", true));
    assert_eq!(
        apply_kept(&mut store, &op, &unstamped, NOW),
        Err(IdentityRefusal::Unstamped)
    );
    let mut huge = subject("scim", "ann", true);
    huge.claims.insert(
        "groups".to_string(),
        vec!["x".repeat(MAX_PROVISIONED_CLAIM_BYTES)],
    );
    assert_eq!(
        apply_kept(&mut store, &provision(huge), &provisioner(SCIM_BOT), NOW),
        Err(IdentityRefusal::InvalidRequest)
    );
    assert!(apply_kept(&mut store, &op, &provisioner(SCIM_BOT), NOW).is_ok());
}
