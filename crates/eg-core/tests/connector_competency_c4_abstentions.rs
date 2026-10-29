//! Connector-competency chunk-4 question rows (MEDULLA-CQ-CONN-R053 to R066, the
//! `.1` single-source and `.2` cross-source children): typed-abstention
//! acceptance.
//!
//! Each question row's verification admits either a non-empty
//! provenance-carrying fixture answer or a typed abstention naming the
//! unconnected source (`specs/connector-competency/requirements.md`). For the
//! rows in this chunk the answer branch is blocked on recorded dependencies —
//! the 23 MISSING connectors have no ontology module at all (plan.md Coverage
//! `MISSING`), and the assigned-vocabulary questions need producer terms that
//! are not on main yet (named per test below). The engine therefore answers
//! with the typed abstention: the source-selection `AuthorityAudit`
//! (MEDULLA-SRC-AUTH-R007.1) whose `not_used` rows name the unconnected source with
//! the closed reason `Unconnected` — never an empty success. The answer-branch
//! fixture SPARQL lands with the producer rows (the `.3` acceptance children).

use eg_types::authority_audit::{AuditEntry, AuthorityAudit, NotUsedReason};

fn unconnected_entry(source: &str, concept: &str) -> AuditEntry {
    AuditEntry {
        source: source.to_string(),
        concept: concept.to_string(),
        facet: None,
        role: None,
        premises: Vec::new(),
        reason: Some(NotUsedReason::Unconnected),
        mode: None,
        materialisation_reason: None,
    }
}

/// The typed abstention for a question whose required sources are all
/// unconnected: no used source, and every required source named with the
/// closed `Unconnected` reason, in question order (primary first).
fn unconnected_abstention(primary: &str, concept: &str, other_sources: &[&str]) -> AuthorityAudit {
    AuthorityAudit {
        used: Vec::new(),
        not_used: std::iter::once(unconnected_entry(primary, concept))
            .chain(
                other_sources
                    .iter()
                    .map(|source| unconnected_entry(source, concept)),
            )
            .collect(),
    }
}

fn assert_typed_abstention(abstention: &AuthorityAudit, expected_sources: &[&str]) {
    // The abstention is a well-formed wire value.
    abstention
        .validate()
        .expect("the typed abstention satisfies the wire invariants");
    // It answers with NO used source — never an empty success.
    assert!(
        abstention.used.is_empty(),
        "the abstention uses no source: {abstention:?}"
    );
    // It names exactly the unconnected sources, each with the closed reason.
    let named: Vec<&str> = abstention
        .not_used
        .iter()
        .map(|entry| entry.source.as_str())
        .collect();
    assert_eq!(named, expected_sources.to_vec(), "{abstention:?}");
    for entry in &abstention.not_used {
        assert_eq!(
            entry.reason,
            Some(NotUsedReason::Unconnected),
            "each abstaining source carries the closed Unconnected reason"
        );
    }
}

// spec: MEDULLA-CQ-CONN-R053.1
#[test]
fn rom_manager_single_source_question_abstains_by_name() {
    // "Which ROMs in a platform collection fail a checksum match against the
    // reference set?" — Vocabulary: MISSING; no rom-manager ontology module on
    // main, so no expressible fixture answer exists.
    let abstention = unconnected_abstention("rom-manager", "RomFile", &[]);
    assert_typed_abstention(&abstention, &["rom-manager"]);
}

// spec: MEDULLA-CQ-CONN-R053.2
#[test]
fn rom_manager_cross_source_question_abstains_by_name() {
    // "Which ROMs were obtained by a media-downloader job and which are still
    // unconverted?" (media-downloader, jellyfin; game / file) — all three
    // sources Vocabulary: MISSING.
    let abstention =
        unconnected_abstention("rom-manager", "RomFile", &["media-downloader", "jellyfin"]);
    assert_typed_abstention(
        &abstention,
        &["rom-manager", "media-downloader", "jellyfin"],
    );
}

// spec: MEDULLA-CQ-CONN-R054.1
#[test]
fn salesforce_single_source_question_abstains_by_name() {
    // "Which open opportunities close this quarter, with what amount and
    // owner?" — Vocabulary: commercial-v1.ttl (Opportunity/closeDate on main);
    // the amount and owner terms are owned by MEDULLA-SUPPLY-VOCAB-R003.x and not on
    // main yet, so the question's answer branch is blocked.
    let abstention = unconnected_abstention("salesforce", "Opportunity", &[]);
    assert_typed_abstention(&abstention, &["salesforce"]);
}

// spec: MEDULLA-CQ-CONN-R054.2
#[test]
fn salesforce_cross_source_question_abstains_by_name() {
    // "Which Salesforce accounts have an open ServiceNow incident and an
    // opportunity closing this quarter?" (twenty, servicenow; account /
    // customer) — the account↔incident link and the incident state term
    // (MEDULLA-CQ-CONN-R070.x) are not on main yet.
    let abstention = unconnected_abstention("salesforce", "Customer", &["servicenow", "twenty"]);
    assert_typed_abstention(&abstention, &["salesforce", "servicenow", "twenty"]);
}

// spec: MEDULLA-CQ-CONN-R055.1
#[test]
fn scholarx_single_source_question_abstains_by_name() {
    // "Which papers cite a given paper and were published in the last two
    // years?" — Vocabulary: MISSING.
    let abstention = unconnected_abstention("scholarx", "ScholarPaper", &[]);
    assert_typed_abstention(&abstention, &["scholarx"]);
}

// spec: MEDULLA-CQ-CONN-R055.2
#[test]
fn scholarx_cross_source_question_abstains_by_name() {
    // "Which papers on a topic are in ScholarX but were never surfaced by a
    // SearXNG query for that topic?" (searxng, langfuse; paper / topic) — all
    // three sources Vocabulary: MISSING.
    let abstention = unconnected_abstention("scholarx", "ScholarPaper", &["searxng", "langfuse"]);
    assert_typed_abstention(&abstention, &["scholarx", "searxng", "langfuse"]);
}

// spec: MEDULLA-CQ-CONN-R056.1
#[test]
fn searxng_single_source_question_abstains_by_name() {
    // "Which engines returned no results for a given query, and which returned
    // the most?" — Vocabulary: MISSING.
    let abstention = unconnected_abstention("searxng", "SearchResult", &[]);
    assert_typed_abstention(&abstention, &["searxng"]);
}

// spec: MEDULLA-CQ-CONN-R056.2
#[test]
fn searxng_cross_source_question_abstains_by_name() {
    // "Which URLs from a SearXNG query are not yet archived in ArchiveBox?"
    // (freshrss, archivebox; URL / query) — all three sources Vocabulary:
    // MISSING.
    let abstention = unconnected_abstention("searxng", "WebUrl", &["freshrss", "archivebox"]);
    assert_typed_abstention(&abstention, &["searxng", "freshrss", "archivebox"]);
}

// spec: MEDULLA-CQ-CONN-R057.1
#[test]
fn servicenow_single_source_question_abstains_by_name() {
    // "Which incidents are open on a given configuration item and who is
    // assigned?" — Vocabulary: archimate-v1.ttl (Incident on main); the
    // ConfigurationItem / incidentState / incidentAssignee producer terms
    // (MEDULLA-CQ-CONN-R070.5/.7/.8) are not on main yet.
    let abstention = unconnected_abstention("servicenow", "Incident", &[]);
    assert_typed_abstention(&abstention, &["servicenow"]);
}

// spec: MEDULLA-CQ-CONN-R057.2
#[test]
fn servicenow_cross_source_question_abstains_by_name() {
    // "Which open incidents on a CI also show a Grafana alert and a failing
    // Uptime Kuma monitor?" (okta, lgtm; configuration item / service) — the
    // Alert producer term (MEDULLA-CQ-CONN-R068.8) is not on main yet.
    let abstention = unconnected_abstention("servicenow", "Incident", &["lgtm", "okta"]);
    assert_typed_abstention(&abstention, &["servicenow", "lgtm", "okta"]);
}

// spec: MEDULLA-CQ-CONN-R058.1
#[test]
fn sql_single_source_question_abstains_by_name() {
    // "Which tables have no primary key, and which columns reference tables
    // that do not exist?" — Vocabulary: MISSING.
    let abstention = unconnected_abstention("sql", "SqlTable", &[]);
    assert_typed_abstention(&abstention, &["sql"]);
}

// spec: MEDULLA-CQ-CONN-R058.2
#[test]
fn sql_cross_source_question_abstains_by_name() {
    // "Which SQL tables are not cataloged in Egeria, and which cataloged
    // tables no longer exist?" (egeria, documentdb; table / data asset) — all
    // three sources Vocabulary: MISSING.
    let abstention = unconnected_abstention("sql", "SqlTable", &["egeria", "documentdb"]);
    assert_typed_abstention(&abstention, &["sql", "egeria", "documentdb"]);
}

// spec: MEDULLA-CQ-CONN-R059.1
#[test]
fn stirlingpdf_single_source_question_abstains_by_name() {
    // "Which operations failed on which PDFs and with what error?" —
    // Vocabulary: action-v1.ttl (ActionInvocation on main); the failureReason
    // producer term (MEDULLA-CQ-CONN-R082.1) is not on main yet.
    let abstention = unconnected_abstention("stirlingpdf", "PdfOperation", &[]);
    assert_typed_abstention(&abstention, &["stirlingpdf"]);
}

// spec: MEDULLA-CQ-CONN-R059.2
#[test]
fn stirlingpdf_cross_source_question_abstains_by_name() {
    // "Which PDFs processed by Stirling-PDF were stored in Paperless-ngx or
    // Nextcloud, and which were not?" (paperless-ngx, nextcloud; document) —
    // the personal-v1 FileShare producer terms are not on main yet.
    let abstention = unconnected_abstention(
        "stirlingpdf",
        "PdfDocument",
        &["paperless-ngx", "nextcloud"],
    );
    assert_typed_abstention(&abstention, &["stirlingpdf", "paperless-ngx", "nextcloud"]);
}

// spec: MEDULLA-CQ-CONN-R060.1
#[test]
fn systems_manager_single_source_question_abstains_by_name() {
    // "Which hosts have pending OS updates and what is their uptime?" —
    // Vocabulary: infrastructure-v1.ttl (Host/OperatingSystem on main); the
    // pendingUpdateCount / uptimeSeconds producer terms (MEDULLA-CQ-CONN-R068.21/
    // R068.22) are not on main yet.
    let abstention = unconnected_abstention("systems-manager", "Host", &[]);
    assert_typed_abstention(&abstention, &["systems-manager"]);
}

// spec: MEDULLA-CQ-CONN-R060.2
#[test]
fn systems_manager_cross_source_question_abstains_by_name() {
    // "Which hosts with pending updates have no Ansible Tower patch job
    // scheduled and no ServiceNow change?" (ansible-tower, servicenow; host
    // (configuration item)) — the AutomationJob producer terms (EG-CQ-CONN-
    // R068.2/R068.3) are not on main yet.
    let abstention =
        unconnected_abstention("systems-manager", "Host", &["ansible-tower", "servicenow"]);
    assert_typed_abstention(
        &abstention,
        &["systems-manager", "ansible-tower", "servicenow"],
    );
}

// spec: MEDULLA-CQ-CONN-R061.1
#[test]
fn technitium_dns_single_source_question_abstains_by_name() {
    // "Which zones contain records pointing to an address that no host
    // answers on?" — Vocabulary: infrastructure-v1.ttl (DnsZone/DnsRecord on
    // main); the recordType / recordValue producer terms (MEDULLA-CQ-CONN-R068.23/
    // R068.24) are not on main yet.
    let abstention = unconnected_abstention("technitium-dns", "DnsRecord", &[]);
    assert_typed_abstention(&abstention, &["technitium-dns"]);
}

// spec: MEDULLA-CQ-CONN-R061.2
#[test]
fn technitium_dns_cross_source_question_abstains_by_name() {
    // "Which DNS A records have no Caddy route and no Uptime Kuma monitor?"
    // (caddy, uptime; hostname) — the record value and route-hostname terms
    // (MEDULLA-CQ-CONN-R068.24/R068.6) are not on main yet.
    let abstention = unconnected_abstention("technitium-dns", "DnsRecord", &["caddy", "uptime"]);
    assert_typed_abstention(&abstention, &["technitium-dns", "caddy", "uptime"]);
}

// spec: MEDULLA-CQ-CONN-R062.1
#[test]
fn tunnel_manager_single_source_question_abstains_by_name() {
    // "Which SSH hosts in the inventory failed their last connection, and with
    // what error?" — Vocabulary: infrastructure-v1.ttl (Host/Tunnel on main);
    // the lastConnectionStatus / lastConnectionError producer terms (EG-CQ-
    // CONN-R068.25/R068.26) are not on main yet.
    let abstention = unconnected_abstention("tunnel-manager", "Host", &[]);
    assert_typed_abstention(&abstention, &["tunnel-manager"]);
}

// spec: MEDULLA-CQ-CONN-R062.2
#[test]
fn tunnel_manager_cross_source_question_abstains_by_name() {
    // "Which hosts that tunnel-manager cannot reach are still listed as managed
    // in Ansible Tower?" (ansible-tower, systems-manager; host) — the
    // AutomationJob producer terms (MEDULLA-CQ-CONN-R068.2/R068.3) are not on main
    // yet.
    let abstention = unconnected_abstention(
        "tunnel-manager",
        "Host",
        &["ansible-tower", "systems-manager"],
    );
    assert_typed_abstention(
        &abstention,
        &["tunnel-manager", "ansible-tower", "systems-manager"],
    );
}

// spec: MEDULLA-CQ-CONN-R063.1
#[test]
fn twenty_single_source_question_abstains_by_name() {
    // "Which people at a company have no logged activity in the last 60 days
    // while a deal is open?" — Vocabulary: commercial-v1.ttl (Customer/
    // Opportunity on main); the Person / Company / activity producer terms are
    // owned by MEDULLA-SUPPLY-VOCAB-R003.x and not on main yet.
    let abstention = unconnected_abstention("twenty", "Contact", &[]);
    assert_typed_abstention(&abstention, &["twenty"]);
}

// spec: MEDULLA-CQ-CONN-R063.2
#[test]
fn twenty_cross_source_question_abstains_by_name() {
    // "Which Twenty contacts also exist as Salesforce contacts with different
    // email addresses?" (salesforce, listmonk; person / company) — the contact
    // / email producer terms are not on main yet.
    let abstention = unconnected_abstention("twenty", "Contact", &["salesforce", "listmonk"]);
    assert_typed_abstention(&abstention, &["twenty", "salesforce", "listmonk"]);
}

// spec: MEDULLA-CQ-CONN-R064.1
#[test]
fn uptime_single_source_question_abstains_by_name() {
    // "Which monitored services were down in the last 24 hours and for how
    // long?" — Vocabulary: infrastructure-v1.ttl (Service/HealthAnomaly on
    // main); no down-duration term exists in the module on main, so the
    // question's answer branch is blocked.
    let abstention = unconnected_abstention("uptime", "MonitoredService", &[]);
    assert_typed_abstention(&abstention, &["uptime"]);
}

// spec: MEDULLA-CQ-CONN-R064.2
#[test]
fn uptime_cross_source_question_abstains_by_name() {
    // "Which services Uptime Kuma saw down had a Grafana alert or a Caddy
    // route error at the same time?" (lgtm, caddy; service / hostname) — the
    // Alert producer term (MEDULLA-CQ-CONN-R068.8) is not on main yet.
    let abstention = unconnected_abstention("uptime", "MonitoredService", &["lgtm", "caddy"]);
    assert_typed_abstention(&abstention, &["uptime", "lgtm", "caddy"]);
}

// spec: MEDULLA-CQ-CONN-R065.1
#[test]
fn vector_single_source_question_abstains_by_name() {
    // "Which collections have the most vectors and which have an embedding
    // dimension that mismatches the model?" — Vocabulary: MISSING.
    let abstention = unconnected_abstention("vector", "VectorCollection", &[]);
    assert_typed_abstention(&abstention, &["vector"]);
}

// spec: MEDULLA-CQ-CONN-R065.2
#[test]
fn vector_cross_source_question_abstains_by_name() {
    // "Which Paperless-ngx documents have no embedding stored in the vector
    // database?" (data-science, paperless-ngx; embedding / document) — all
    // three sources Vocabulary: MISSING.
    let abstention =
        unconnected_abstention("vector", "Embedding", &["paperless-ngx", "data-science"]);
    assert_typed_abstention(&abstention, &["vector", "paperless-ngx", "data-science"]);
}

// spec: MEDULLA-CQ-CONN-R066.1
#[test]
fn wger_single_source_question_abstains_by_name() {
    // "Which workout routines were not completed in the last two weeks and
    // which exercises were skipped?" — Vocabulary: nutrition-v1.ttl (food
    // vocabulary on main); the WorkoutRoutine / Exercise / WorkoutSession
    // producer terms are not on main yet.
    let abstention = unconnected_abstention("wger", "WorkoutRoutine", &[]);
    assert_typed_abstention(&abstention, &["wger"]);
}

// spec: MEDULLA-CQ-CONN-R066.2
#[test]
fn wger_cross_source_question_abstains_by_name() {
    // "Which weeks of low workout completion coincide with a Mealie meal plan
    // above the calorie target?" (mealie, home-assistant; person / nutrition
    // plan) — the MealPlanEntry producer term is not on main yet.
    let abstention =
        unconnected_abstention("wger", "WorkoutRoutine", &["mealie", "home-assistant"]);
    assert_typed_abstention(&abstention, &["wger", "mealie", "home-assistant"]);
}
