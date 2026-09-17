// Existing Agent Template durability, replay, instantiation, and pin tests.

use super::super::agent_fixtures::{
    layers_holding, ledger_record, mutation_context as context, open_agent_store as open_store,
};
use super::super::agent_revision::{decode_status_result, validate_status_record};
use super::*;
use eg_types::agent_component::{AgentComponentKind, ComponentDependency};
use eg_types::agent_library::AgentLibraryEntryDraft;
use eg_types::agent_template::{
    AgentTemplateDraft, AgentTemplateInstantiateRequest, TemplateParam,
};
use eg_types::mutation_batch::MutationBatchStatus;
use std::collections::BTreeMap;

fn digest(byte: char) -> String {
    format!("sha256:{}", byte.to_string().repeat(64))
}

fn dep(component_id: &str, kind: AgentComponentKind, seed: char) -> ComponentDependency {
    ComponentDependency {
        component_id: component_id.to_string(),
        kind,
        definition_digest: digest(seed),
    }
}

/// The shared agent draft, pinned to this module's own components.
fn base(tenant_id: &str, agent_id: &str) -> AgentLibraryEntryDraft {
    AgentLibraryEntryDraft {
        system_prompt: dep("prompt:agent-v1", AgentComponentKind::SystemPrompt, '2'),
        tools: vec![dep("tool:search", AgentComponentKind::Tool, '3')],
        skills: vec![dep("skill:reason", AgentComponentKind::Skill, '4')],
        model_profile: dep("model-profile:opus", AgentComponentKind::ModelProfile, '5'),
        model_identity: "model:opus".to_string(),
        ontologies: vec![dep("ontology:agent", AgentComponentKind::Ontology, '6')],
        source_revision: "source-revision:42".to_string(),
        ..super::super::agent_library::seed_agent_draft_for_test(tenant_id, agent_id, 0)
    }
}

fn template(tenant_id: &str, template_id: &str) -> AgentTemplateDraft {
    AgentTemplateDraft {
        template_id: template_id.to_string(),
        version: "1.0.0".to_string(),
        base: base(tenant_id, "agent:researcher"),
        params: vec![
            TemplateParam {
                name: "model".to_string(),
                replaces: "model-profile:opus".to_string(),
                kind: AgentComponentKind::ModelProfile,
                required: false,
                summary: "which model the agent runs on".to_string(),
            },
            TemplateParam {
                name: "search".to_string(),
                replaces: "tool:search".to_string(),
                kind: AgentComponentKind::Tool,
                required: false,
                summary: "which search tool the agent uses".to_string(),
            },
        ],
        tenant_id: tenant_id.to_string(),
        actor_scope: "definition:builder-a".to_string(),
        purpose_id: "agent-template:definition".to_string(),
        policy_digest: digest('9'),
    }
}

/// The seeding nonce the five components [`base`] pins are published
/// under. Fixed rather than per-call: the ids are fixed too, and
/// `seed_component_for_test` is idempotent per tenant, so a second
/// fixture in the same tenant finds them rather than republishing. The
/// five consecutive indices it consumes (30..=34) are why a test that
/// seeds a component of its own uses a clearly separated nonce.
const SEEDED_COMPONENT_NONCE: u8 = 30;

/// Publish every component `draft.base` pins and rewrite each pin to the
/// component's real digest.
///
/// Publishing a template now RESOLVES its base's component pins, so a
/// fixture can no longer invent a digest -- it has to be the component's
/// real `definition_digest`, exactly as a real template's is. Separate
/// from [`try_publish`] so a refusal test can seed a publishable base and
/// then break exactly ONE pin, rather than having the fixture helpfully
/// seed the very ghost it is testing.
fn seeded(store: &AgentLibraryStore, mut draft: AgentTemplateDraft) -> AgentTemplateDraft {
    super::super::agent_component::seed_draft_components_for_test(
        store,
        &mut draft.base,
        SEEDED_COMPONENT_NONCE,
    );
    draft
}

fn publish(
    store: &AgentLibraryStore,
    key: &str,
    nonce: u8,
    expected_revision: u64,
    draft: AgentTemplateDraft,
) -> AgentTemplateWriteResult {
    let draft = seeded(store, draft);
    try_publish(store, key, nonce, expected_revision, draft).unwrap()
}

/// [`publish`] without the seeding and without the unwrap: for the tests
/// that assert a refusal.
fn try_publish(
    store: &AgentLibraryStore,
    key: &str,
    nonce: u8,
    expected_revision: u64,
    draft: AgentTemplateDraft,
) -> Result<AgentTemplateWriteResult, String> {
    store.publish_template(AgentTemplatePublishRequest {
        context: context(
            store,
            &draft.tenant_id.clone(),
            key,
            nonce,
            expected_revision,
            "agent-template:publish",
        ),
        template: draft,
    })
}

fn instantiate(
    tenant_id: &str,
    template_id: &str,
    agent_id: &str,
    entry_revision: Option<u64>,
    bindings: BTreeMap<String, ComponentDependency>,
) -> AgentTemplateInstantiateRequest {
    AgentTemplateInstantiateRequest {
        tenant_id: tenant_id.to_string(),
        template_id: template_id.to_string(),
        entry_revision,
        agent_id: agent_id.to_string(),
        bindings,
    }
}

#[test]
fn a_published_template_is_durable_and_reads_back() {
    let (_dir, store) = open_store();
    let published = publish(
        &store,
        "key-1",
        1,
        0,
        template("tenant-a", "template:researcher"),
    );
    assert!(!published.replayed);
    assert_eq!(published.result.template.entry_revision, 1);

    let current = store
        .current_template("tenant-a", "template:researcher")
        .unwrap()
        .unwrap();
    assert_eq!(current, published.result.template);
}

#[test]
fn a_byte_identical_retry_replays_rather_than_publishing_twice() {
    let (_dir, store) = open_store();
    let first = publish(&store, "key-1", 1, 0, template("tenant-a", "template:a"));
    let mut retry = context(&store, "tenant-a", "key-1", 2, 0, "agent-template:publish");
    retry.created_at_ms = 99;
    let replayed = store
        .publish_template(AgentTemplatePublishRequest {
            context: retry,
            // Byte-identical to what `publish` admitted, seeds included:
            // the replay identity is minted from the DRAFT digest, so a
            // retry carrying unseeded pins would be a different operation.
            template: seeded(&store, template("tenant-a", "template:a")),
        })
        .unwrap();
    assert!(replayed.replayed);
    assert_eq!(replayed.result, first.result);
    assert_eq!(
        store
            .template_revisions("tenant-a", "template:a")
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn a_replayed_fresh_nonce_is_consumed_before_returning() {
    let (_dir, store) = open_store();
    publish(&store, "key-1", 1, 0, template("tenant-a", "template:a"));
    let replayed = publish(&store, "key-1", 2, 0, template("tenant-a", "template:a"));
    assert!(replayed.replayed);

    let error = try_publish(&store, "key-2", 2, 1, template("tenant-a", "template:b")).unwrap_err();
    assert!(error.contains("REPLAY_NONCE_CONSUMED"), "got: {error}");
}

#[test]
fn template_status_reads_publish_and_retire_domain_envelopes() {
    let (_dir, store) = open_store();
    let status = |context, kind| {
        store
            .template_status(AgentTemplateStatusRequest {
                context,
                template_id: "template:status".to_string(),
                kind,
            })
            .unwrap()
            .expect("durable status")
    };
    let publish_context = context(
        &store,
        "tenant-a",
        "status-publish",
        1,
        0,
        "agent-template:publish",
    );
    let published = publish(
        &store,
        "status-publish",
        1,
        0,
        template("tenant-a", "template:status"),
    );
    let publish_status = status(publish_context, AgentTemplateMutationKind::Publish);
    assert!(publish_status.replayed);
    assert_eq!(publish_status.result, published.result);

    let retire_context = context(
        &store,
        "tenant-a",
        "status-retire",
        2,
        1,
        "agent-template:retire",
    );
    let retired = store
        .retire_template(AgentTemplateRetireRequest {
            context: retire_context.clone(),
            template_id: "template:status".to_string(),
        })
        .unwrap();
    let retire_status = status(retire_context, AgentTemplateMutationKind::Retire);
    assert!(retire_status.replayed);
    assert_eq!(retire_status.result, retired.result);
    assert_eq!(
        retire_status.result.template.lifecycle,
        AgentLibraryLifecycle::Retired
    );
}

#[test]
fn template_status_rejects_an_unwrapped_result() {
    let (_dir, store) = open_store();
    let published = publish(
        &store,
        "status-invalid",
        1,
        0,
        template("tenant-a", "template:invalid"),
    );
    let raw_result =
        eg_storage::encode_bounded(&published.result, "unwrapped agent template status result")
            .unwrap();
    let error = decode_status_result::<TemplateLayer>(&raw_result).unwrap_err();
    assert!(!error.is_empty(), "an unwrapped result must fail closed");
}

#[test]
fn template_status_rejects_redirected_identity_state_version_and_kind() {
    let (_dir, store) = open_store();
    let context_a = context(
        &store,
        "tenant-a",
        "status-bindings",
        1,
        0,
        "agent-template:publish",
    );
    let published = publish(
        &store,
        "status-bindings",
        1,
        0,
        template("tenant-a", "template:bindings"),
    );
    let owner_a = store.scope_handle("tenant-a").unwrap();
    let record = ledger_record(&store, "tenant-a", "status-bindings");
    let refused = |record: &eg_types::MutationBatchRecord,
                   template_id: &str,
                   kind: AgentTemplateMutationKind,
                   committed: &AgentTemplateCommittedResult| {
        validate_status_record::<TemplateLayer>(
            record,
            owner_a.identity(),
            &context_a,
            (template_id, kind),
            committed,
        )
        .is_err()
    };

    let mut wrong_key = published.result.clone();
    wrong_key.batch_id = "agent-library/v1/redirected".to_string();
    assert!(refused(
        &record,
        "template:bindings",
        AgentTemplateMutationKind::Publish,
        &wrong_key
    ));

    let other_tenant = publish(
        &store,
        "status-other-tenant",
        2,
        0,
        template("tenant-b", "template:other-tenant"),
    );
    let mut wrong_tenant = other_tenant.result.clone();
    wrong_tenant.batch_id = super::super::agent_library::batch_id("status-bindings").unwrap();
    assert!(refused(
        &record,
        "template:other-tenant",
        AgentTemplateMutationKind::Publish,
        &wrong_tenant
    ));

    let mut stale_version = published.result.clone();
    stale_version.committed_version = 0;
    assert!(refused(
        &record,
        "template:bindings",
        AgentTemplateMutationKind::Publish,
        &stale_version
    ));

    let mut uncommitted = record.clone();
    uncommitted.status = MutationBatchStatus::Prepared;
    assert!(refused(
        &uncommitted,
        "template:bindings",
        AgentTemplateMutationKind::Publish,
        &published.result
    ));

    assert!(refused(
        &record,
        "template:bindings",
        AgentTemplateMutationKind::Retire,
        &published.result
    ));
}

#[test]
fn a_retired_template_is_a_tombstone_that_cannot_be_resurrected() {
    let (_dir, store) = open_store();
    publish(&store, "key-1", 1, 0, template("tenant-a", "template:a"));
    store
        .retire_template(AgentTemplateRetireRequest {
            context: context(&store, "tenant-a", "key-2", 2, 1, "agent-template:retire"),
            template_id: "template:a".to_string(),
        })
        .unwrap();
    let draft = seeded(&store, template("tenant-a", "template:a"));
    let error = try_publish(&store, "key-3", 3, 2, draft).unwrap_err();
    assert_eq!(error, "retired agent templates cannot be resurrected");
}

#[test]
fn the_four_layers_share_one_owner_without_colliding() {
    // Components, agents, graphs and templates all live in
    // `agent_library.redb`. If any pair shared a key space, publishing one
    // would overwrite another -- and a template's base is literally an
    // agent draft, so a collision here would be easy to miss.
    let (_dir, store) = open_store();
    publish(&store, "key-1", 1, 0, template("tenant-a", "same-id"));
    assert_eq!(
        layers_holding(&store, "tenant-a", "same-id"),
        [false, false, false, true],
        "only the template layer holds the id"
    );
}

#[test]
fn the_store_reopens_after_a_template_commit() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap();
    {
        let store = AgentLibraryStore::open(path).unwrap();
        publish(&store, "key-1", 1, 0, template("tenant-a", "template:a"));
    }
    let reopened = AgentLibraryStore::open(path).expect("owner reopens");
    assert!(reopened
        .current_template("tenant-a", "template:a")
        .unwrap()
        .is_some());
}

#[test]
fn a_history_read_is_scoped_to_its_own_template() {
    // `range_from` is open-ended. Without a per-row prefix re-check a
    // history read walks into the NEXT template's revisions and returns
    // them as if they belonged to this one.
    let (_dir, store) = open_store();
    publish(&store, "key-1", 1, 0, template("tenant-a", "template:a"));
    publish(&store, "key-2", 2, 0, template("tenant-a", "template:b"));
    publish(&store, "key-3", 3, 0, template("tenant-b", "template:a"));
    for (tenant, id) in [
        ("tenant-a", "template:a"),
        ("tenant-a", "template:b"),
        ("tenant-b", "template:a"),
    ] {
        let revisions = store.template_revisions(tenant, id).unwrap();
        assert_eq!(revisions.len(), 1, "{tenant}/{id}");
        assert_eq!(revisions[0].tenant_id, tenant);
        assert_eq!(revisions[0].template_id, id);
    }
}

// ---- the operation the layer exists for ----

#[test]
fn an_instance_is_an_ordinary_library_entry_and_publishes_like_one() {
    // THE property: instantiation yields a normal agent draft, so the
    // instance is admitted, delegated and pinned with no template-aware
    // branch anywhere. The only thing it carries is its provenance.
    let (_dir, store) = open_store();
    // The instance publishes through the ordinary library path, which
    // RESOLVES every pinned component against the durable store -- so the
    // binding has to name a component that really exists, exactly as a
    // real one does. `publish` seeds the base's own pins.
    let haiku = super::super::agent_component::seed_component_for_test(
        &store,
        "tenant-a",
        "model-profile:haiku",
        AgentComponentKind::ModelProfile,
        40,
    );
    publish(
        &store,
        "key-1",
        1,
        0,
        template("tenant-a", "template:researcher"),
    );
    let bindings = BTreeMap::from([("model".to_string(), haiku)]);
    let draft = store
        .instantiate_template(&instantiate(
            "tenant-a",
            "template:researcher",
            "agent:researcher-cheap",
            None,
            bindings,
        ))
        .expect("instantiates");
    assert_eq!(draft.model_profile.component_id, "model-profile:haiku");
    // Unbound optional parameter: the base component stands.
    assert_eq!(draft.tools[0].component_id, "tool:search");
    let provenance = draft
        .instantiated_from
        .clone()
        .expect("an instance records where it came from");
    assert_eq!(provenance.template_id, "template:researcher");
    assert_eq!(provenance.entry_revision, 1);

    // And it publishes through the ordinary library path, unchanged.
    let published = store
        .publish(eg_types::agent_library::AgentLibraryPublishRequest {
            context: context(
                &store,
                "tenant-a",
                "key-2",
                2,
                0,
                "agent-library:definition",
            ),
            entry: draft,
        })
        .expect("an instance publishes like any other agent");
    let stored = store
        .current("tenant-a", "agent:researcher-cheap")
        .unwrap()
        .unwrap();
    assert_eq!(stored.entry_revision, published.entry.entry_revision);
    assert_eq!(stored.instantiated_from, Some(provenance));
}

#[test]
fn a_retired_template_stops_instantiating_at_every_revision_it_ever_had() {
    // The lifecycle consulted is the HEAD's. A tombstone is a separate
    // LATER revision, so revision 1 stays `Published` in its own row
    // forever -- resolving the pinned revision's lifecycle instead would
    // let a withdrawn template keep minting agents indefinitely.
    let (_dir, store) = open_store();
    publish(
        &store,
        "key-1",
        1,
        0,
        template("tenant-a", "template:researcher"),
    );
    assert!(store
        .instantiate_template(&instantiate(
            "tenant-a",
            "template:researcher",
            "agent:a",
            Some(1),
            BTreeMap::new(),
        ))
        .is_ok());

    store
        .retire_template(AgentTemplateRetireRequest {
            context: context(&store, "tenant-a", "key-2", 2, 1, "agent-template:retire"),
            template_id: "template:researcher".to_string(),
        })
        .unwrap();

    for pinned in [None, Some(1), Some(2)] {
        let error = store
            .instantiate_template(&instantiate(
                "tenant-a",
                "template:researcher",
                "agent:a",
                pinned,
                BTreeMap::new(),
            ))
            .expect_err("a retired template must instantiate nothing");
        assert!(error.contains("retired"), "pinned={pinned:?}: {error}");
    }
    // Retiring withdraws it; it stays readable, because an agent that
    // already recorded it as its provenance still needs to resolve it.
    assert_eq!(
        store
            .template_revisions("tenant-a", "template:researcher")
            .unwrap()
            .len(),
        2
    );
}

#[test]
fn an_instantiate_cannot_reach_another_tenants_template() {
    let (_dir, store) = open_store();
    publish(
        &store,
        "key-1",
        1,
        0,
        template("tenant-a", "template:researcher"),
    );
    let error = store
        .instantiate_template(&instantiate(
            "tenant-b",
            "template:researcher",
            "agent:a",
            None,
            BTreeMap::new(),
        ))
        .expect_err("another tenant's template must not resolve");
    assert!(error.contains("no such agent template"), "got: {error}");
}

#[test]
fn an_instantiate_of_a_revision_that_was_never_retained_is_refused() {
    let (_dir, store) = open_store();
    publish(
        &store,
        "key-1",
        1,
        0,
        template("tenant-a", "template:researcher"),
    );
    let error = store
        .instantiate_template(&instantiate(
            "tenant-a",
            "template:researcher",
            "agent:a",
            Some(7),
            BTreeMap::new(),
        ))
        .expect_err("an unretained revision must be refused");
    assert!(error.contains("no such retained revision"), "got: {error}");
}

#[test]
fn an_unknown_binding_is_refused_by_the_store_too() {
    // The type layer refuses it; this proves the store path does not
    // bypass that check on its way through.
    let (_dir, store) = open_store();
    publish(
        &store,
        "key-1",
        1,
        0,
        template("tenant-a", "template:researcher"),
    );
    let bindings = BTreeMap::from([(
        "temperature".to_string(),
        dep("model-profile:haiku", AgentComponentKind::ModelProfile, 'b'),
    )]);
    let error = store
        .instantiate_template(&instantiate(
            "tenant-a",
            "template:researcher",
            "agent:a",
            None,
            bindings,
        ))
        .expect_err("an undeclared parameter must be refused");
    assert!(error.contains("declares no parameter"), "got: {error}");
}

// ---- TEMPLATE -> L1: resolved at THIS layer's admission ----

#[test]
fn a_template_whose_base_pins_a_component_that_does_not_exist_is_refused() {
    // `publish_template` resolved NOTHING. Its base is a complete agent
    // draft, and the components that draft pins were resolved only when an
    // INSTANCE of the template was published, at L2 -- so a template whose
    // base named a component that does not exist was admitted and became
    // DURABLE. Not an execution grant on its own, because nothing runs
    // until an instance is published, but an unresolvable record is exactly
    // what this contract exists to refuse, and every sibling layer resolves
    // at its own admission.
    //
    // The ghost is added to `skills`, which no parameter `replaces`: a
    // parameter whose target vanished is refused by `validate()` first, and
    // this test has to reach the resolver.
    let (_dir, store) = open_store();
    let mut draft = seeded(&store, template("tenant-a", "template:researcher"));
    draft
        .base
        .skills
        .push(dep("skill:ghost", AgentComponentKind::Skill, 'c'));
    let error = try_publish(&store, "key-1", 1, 0, draft)
        .expect_err("a base pinning a component that does not exist is refused");
    assert!(
        error.contains("agent template base pins component 'skill:ghost'")
            && error.contains("does not exist in this tenant"),
        "got: {error}"
    );
    assert!(store
        .current_template("tenant-a", "template:researcher")
        .unwrap()
        .is_none());
}

#[test]
fn a_template_whose_base_pins_an_invented_digest_is_refused() {
    // The other half of the pin: the component EXISTS, and the digest is
    // 64 well-formed hex characters no revision of it ever carried. Local
    // validation cannot tell the two apart; only a read of the component
    // store can.
    let (_dir, store) = open_store();
    let mut draft = seeded(&store, template("tenant-a", "template:researcher"));
    draft.base.system_prompt.definition_digest = digest('c');
    let error = try_publish(&store, "key-1", 1, 0, draft)
        .expect_err("a digest no component revision carries is refused");
    assert!(error.contains("that was never published"), "got: {error}");
    assert!(store
        .current_template("tenant-a", "template:researcher")
        .unwrap()
        .is_none());
}

#[test]
fn a_template_whose_base_pins_another_tenants_component_is_refused() {
    // Resolution is per tenant. A component published in tenant-b is not
    // reachable from a tenant-a template, however real its digest is.
    let (_dir, store) = open_store();
    let foreign = super::super::agent_component::seed_component_for_test(
        &store,
        "tenant-b",
        "skill:other-tenant",
        AgentComponentKind::Skill,
        41,
    );
    let mut draft = seeded(&store, template("tenant-a", "template:researcher"));
    draft.base.skills.push(foreign);
    let error = try_publish(&store, "key-1", 1, 0, draft)
        .expect_err("another tenant's component must not resolve");
    assert!(
        error.contains("does not exist in this tenant"),
        "got: {error}"
    );
}
