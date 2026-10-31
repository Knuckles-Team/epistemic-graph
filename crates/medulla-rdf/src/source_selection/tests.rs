use super::*;

/// Two-source fixture: `Invoice` is mapped directly by `source-a`. `Contractor`
/// is a subclass of `Person`, which is mapped by `source-b` — proving a
/// subclass mapping serves its superclass. `Invoice` relates to `Customer` (no
/// mapping anywhere in its closure): uncovered. `Legacy` has no relation to
/// anything: disconnected from the queried roots.
fn two_source_fixture() -> CoverageFixture {
    CoverageFixture::new()
        .with_relation("Invoice", "billedTo", "Customer")
        .with_relation("Invoice", "signedBy", "Contractor")
        .with_subclass("Contractor", "Person")
        .with_mapping("Invoice", "source-a-invoices", "source-a", 7)
        .with_mapping("Person", "source-b-people", "source-b", 3)
}

fn roots(class: &str) -> Vec<String> {
    vec![class.to_string()]
}

fn verdict_for(results: &[CoverageResult], class: &str) -> CoverageResult {
    results
        .iter()
        .find(|r| {
            r.class() == class && !matches!(r, CoverageResult::Covered { facet: Some(_), .. })
        })
        .cloned()
        .unwrap()
}

fn select_one(fixture: &CoverageFixture, class: &str) -> CoverageResult {
    verdict_for(&select_sources(fixture, &roots(class), 1), class)
}

fn select_one_with(
    fixture: &CoverageFixture,
    class: &str,
    query: &SelectionQuery,
) -> CoverageResult {
    verdict_for(
        &select_sources_with(fixture, &roots(class), 1, query),
        class,
    )
}

fn covered_source(result: &CoverageResult) -> &str {
    match result {
        CoverageResult::Covered { source, .. } => source,
        other => panic!("expected Covered, got {other:?}"),
    }
}

fn abstain_reason(result: &CoverageResult) -> (NotUsedReason, Option<String>) {
    match result {
        CoverageResult::Abstained { reason, source, .. } => (*reason, source.clone()),
        other => panic!("expected Abstained, got {other:?}"),
    }
}

fn tiebreak_rule(result: &CoverageResult) -> TiebreakRule {
    result
        .premises()
        .iter()
        .find_map(|p| match p {
            Premise::Tiebreak { rule, .. } => Some(*rule),
            _ => None,
        })
        .expect("a tiebreak premise")
}

// spec: MEDULLA-CQ-XSRC-R005.1
#[test]
fn authority_rank_selects_winner_and_lists_lower_ranked_alternate() {
    let fixture = CoverageFixture::new()
        .with_relation("Invoice", "billedTo", "Customer")
        .with_authority("Invoice", "source-b-invoices", "source-b", 3, 2)
        .with_authority("Invoice", "source-a-invoices", "source-a", 7, 1);
    let results = select_sources(&fixture, &["Invoice".to_string()], 1);
    let invoice = results.iter().find(|r| r.class() == "Invoice").unwrap();

    match invoice {
        CoverageResult::Covered {
            mapping_name,
            alternates,
            premises,
            ..
        } => {
            assert_eq!(mapping_name, "source-a-invoices");
            assert_eq!(alternates, &["source-b-invoices".to_string()]);
            assert!(premises.contains(&Premise::Authority {
                row: "Invoice::source-a-invoices".to_string(),
                source: "source-a".to_string(),
                class: "Invoice".to_string(),
                facet: None,
                rank: 1,
                scope: None,
                origin: "fixture".to_string(),
                generation: 7,
            }));
            assert!(premises.contains(&Premise::Authority {
                row: "Invoice::source-b-invoices".to_string(),
                source: "source-b".to_string(),
                class: "Invoice".to_string(),
                facet: None,
                rank: 2,
                scope: None,
                origin: "fixture".to_string(),
                generation: 3,
            }));
        }
        other => panic!("expected ranked Covered, got {other:?}"),
    }
}

// spec: MEDULLA-FEDERATED-QUERY-R072.1
#[test]
fn direct_mapping_is_covered_with_source_generation_premise() {
    let fixture = two_source_fixture();
    let results = select_sources(&fixture, &["Invoice".to_string()], 2);
    let invoice = results.iter().find(|r| r.class() == "Invoice").unwrap();
    match invoice {
        CoverageResult::Covered {
            mapping_name,
            premises,
            ..
        } => {
            assert_eq!(mapping_name, "source-a-invoices");
            assert!(premises.contains(&Premise::SourceGeneration {
                source: "source-a".to_string(),
                generation: 7,
            }));
        }
        other => panic!("expected Covered, got {other:?}"),
    }
}

// spec: MEDULLA-FEDERATED-QUERY-R072.1
#[test]
fn subclass_mapping_serves_its_superclass() {
    let fixture = two_source_fixture();
    let results = select_sources(&fixture, &["Invoice".to_string()], 2);
    let contractor = results.iter().find(|r| r.class() == "Contractor").unwrap();
    match contractor {
        CoverageResult::Covered {
            mapping_name,
            premises,
            ..
        } => {
            assert_eq!(mapping_name, "source-b-people");
            assert!(premises.contains(&Premise::Subclass {
                sub_class: "Contractor".to_string(),
                super_class: "Person".to_string(),
            }));
            assert!(premises.contains(&Premise::SourceGeneration {
                source: "source-b".to_string(),
                generation: 3,
            }));
        }
        other => panic!("expected Covered via subclass entailment, got {other:?}"),
    }
}

// spec: MEDULLA-FEDERATED-QUERY-R072.1
#[test]
fn reachable_unmapped_class_is_uncovered() {
    let fixture = two_source_fixture();
    let results = select_sources(&fixture, &["Invoice".to_string()], 2);
    let customer = results.iter().find(|r| r.class() == "Customer").unwrap();
    assert!(matches!(customer, CoverageResult::Uncovered { .. }));
}

// spec: MEDULLA-FEDERATED-QUERY-R072.1
#[test]
fn unlinked_class_is_disconnected_not_uncovered() {
    let fixture = two_source_fixture().with_relation("Legacy", "notes", "Legacy");
    let results = select_sources(&fixture, &["Invoice".to_string()], 2);
    let legacy = results.iter().find(|r| r.class() == "Legacy").unwrap();
    assert!(matches!(legacy, CoverageResult::Disconnected { .. }));
}

// spec: MEDULLA-FEDERATED-QUERY-R072.1
#[test]
fn selection_plans_no_execution() {
    let fixture = two_source_fixture();
    let results = select_sources(&fixture, &["Invoice".to_string()], 2);
    for result in &results {
        if let CoverageResult::Covered { mapping_name, .. } = result {
            assert!(!mapping_name.is_empty());
        }
    }
}

// spec: MEDULLA-SRC-AUTH-R003.4
#[test]
fn effectiveness_comes_from_origin_not_from_the_flag() {
    let row = AuthorityRow::new("Incident", "m", "pack-src", 1, 1).approved(true);
    assert!(!row_is_effective(AuthorityOrigin::Pack, &row));
    assert!(!row_is_effective(AuthorityOrigin::Ingestion, &row));
    assert!(row_is_effective(AuthorityOrigin::Approved, &row));
    assert!(!row_is_effective(
        AuthorityOrigin::Approved,
        &row.approved(false)
    ));
}

// spec: MEDULLA-SRC-AUTH-R003.5
#[test]
fn a_proposed_row_is_listed_never_selected() {
    let fixture = CoverageFixture::new()
        .with_row(
            AuthorityRow::new("Incident", "proposed", "pagerduty", 1, 1)
                .origin(AuthorityOrigin::Pack),
        )
        .with_row(
            AuthorityRow::new("Incident", "approved", "servicenow", 1, 2)
                .origin(AuthorityOrigin::Approved),
        );
    let verdict = select_one(&fixture, "Incident");
    assert_eq!(covered_source(&verdict), "servicenow");
    assert!(verdict.premises().contains(&Premise::NotUsed {
        row: "Incident::proposed".to_string(),
        source: "pagerduty".to_string(),
        class: "Incident".to_string(),
        facet: None,
        reason: NotUsedReason::Proposed,
    }));
}

// spec: MEDULLA-SRC-AUTH-R004.1
#[test]
fn two_mappings_on_one_class_are_both_kept() {
    let fixture = CoverageFixture::new()
        .with_authority("Incident", "a", "servicenow", 1, 1)
        .with_authority("Incident", "b", "pagerduty", 1, 2);
    let rows: Vec<_> = fixture.rows().map(|r| r.mapping_name.as_str()).collect();
    assert_eq!(rows, ["a", "b"]);
}

// spec: MEDULLA-SRC-AUTH-R004.2
#[test]
fn a_covered_class_carries_the_authority_premise() {
    let fixture = CoverageFixture::new().with_row(
        AuthorityRow::new("Incident", "m", "servicenow", 4, 1)
            .tenant("acme")
            .origin(AuthorityOrigin::Approved),
    );
    let query = SelectionQuery {
        tenant: Some("acme".to_string()),
        ..SelectionQuery::default()
    };
    let verdict = select_one_with(&fixture, "Incident", &query);
    assert!(verdict.premises().contains(&Premise::Authority {
        row: "Incident::m".to_string(),
        source: "servicenow".to_string(),
        class: "Incident".to_string(),
        facet: None,
        rank: 1,
        scope: Some("acme".to_string()),
        origin: "approved".to_string(),
        generation: 4,
    }));
}

// spec: MEDULLA-SRC-AUTH-R004.3
#[test]
fn covered_lists_alternates_in_rank_order() {
    let fixture = CoverageFixture::new()
        .with_authority("Incident", "third", "c", 1, 3)
        .with_authority("Incident", "first", "a", 1, 1)
        .with_authority("Incident", "second", "b", 1, 2);
    match select_one(&fixture, "Incident") {
        CoverageResult::Covered {
            mapping_name,
            alternates,
            ..
        } => {
            assert_eq!(mapping_name, "first");
            assert_eq!(alternates, ["second", "third"]);
        }
        other => panic!("expected Covered, got {other:?}"),
    }
}

// spec: MEDULLA-SRC-AUTH-R004.4
#[test]
fn superclass_authority_is_inherited_and_a_subclass_row_overrides() {
    let base = CoverageFixture::new()
        .with_subclass("Incident", "ApplicationEvent")
        .with_authority("ApplicationEvent", "events", "lgtm", 1, 1);
    let inherited = select_one(&base, "Incident");
    assert_eq!(covered_source(&inherited), "lgtm");
    assert!(inherited.premises().contains(&Premise::Subclass {
        sub_class: "Incident".to_string(),
        super_class: "ApplicationEvent".to_string(),
    }));
    let overridden = select_one(
        &base.with_authority("Incident", "incidents", "servicenow", 1, 5),
        "Incident",
    );
    assert_eq!(covered_source(&overridden), "servicenow");
    assert_eq!(tiebreak_rule(&overridden), TiebreakRule::NearerClass);
}

fn host_fixture() -> CoverageFixture {
    CoverageFixture::new()
        .with_authority("Host", "cmdb-hosts", "cmdb", 1, 1)
        .with_row(AuthorityRow::new("Host", "hw-hosts", "hardware", 1, 1).facet("temperature"))
}

// spec: MEDULLA-SRC-AUTH-R004.7
#[test]
fn each_property_selects_its_own_source() {
    let query = SelectionQuery {
        facets: vec![
            ("Host".to_string(), "temperature".to_string()),
            ("Host".to_string(), "hostname".to_string()),
        ],
        ..SelectionQuery::default()
    };
    let results = select_sources_with(&host_fixture(), &roots("Host"), 1, &query);
    let by_facet = |facet: Option<&str>| {
        results
            .iter()
            .find_map(|r| match r {
                CoverageResult::Covered {
                    facet: f, source, ..
                } if f.as_deref() == facet => Some(source.clone()),
                _ => None,
            })
            .unwrap()
    };
    assert_eq!(by_facet(None), "cmdb");
    assert_eq!(by_facet(Some("temperature")), "hardware");
    assert_eq!(by_facet(Some("hostname")), "cmdb");
}

// spec: MEDULLA-SRC-AUTH-R005.1
#[test]
fn facet_beats_class_beats_nearest_superclass() {
    let query = SelectionQuery {
        facets: vec![("Server".to_string(), "temperature".to_string())],
        ..SelectionQuery::default()
    };
    // facet over class, both on the same class.
    let facet_vs_class = host_fixture();
    let facet_query = SelectionQuery {
        facets: vec![("Host".to_string(), "temperature".to_string())],
        ..SelectionQuery::default()
    };
    let results = select_sources_with(&facet_vs_class, &roots("Host"), 1, &facet_query);
    let facet_verdict = results.last().unwrap();
    assert_eq!(covered_source(facet_verdict), "hardware");
    assert_eq!(tiebreak_rule(facet_verdict), TiebreakRule::FacetOverClass);
    // class over nearest superclass (and nearer superclass over farther).
    let chain = CoverageFixture::new()
        .with_subclass("Server", "Host")
        .with_subclass("Host", "Asset")
        .with_authority("Asset", "assets", "erp", 1, 1)
        .with_authority("Host", "hosts", "cmdb", 1, 1)
        .with_authority("Server", "servers", "inventory", 1, 9);
    let class_verdict = select_one(&chain, "Server");
    assert_eq!(covered_source(&class_verdict), "inventory");
    assert_eq!(tiebreak_rule(&class_verdict), TiebreakRule::NearerClass);
    let results = select_sources_with(&chain, &roots("Server"), 1, &query);
    let nearer = results.last().unwrap();
    assert_eq!(covered_source(nearer), "inventory");
}

// spec: MEDULLA-SRC-AUTH-R005.2
#[test]
fn a_matching_scope_beats_an_unscoped_lower_rank() {
    let fixture = CoverageFixture::new()
        .with_authority("Incident", "global", "pagerduty", 1, 1)
        .with_row(AuthorityRow::new("Incident", "tenant", "servicenow", 1, 7).tenant("acme"));
    let acme = SelectionQuery {
        tenant: Some("acme".to_string()),
        ..SelectionQuery::default()
    };
    let verdict = select_one_with(&fixture, "Incident", &acme);
    assert_eq!(covered_source(&verdict), "servicenow");
    assert_eq!(tiebreak_rule(&verdict), TiebreakRule::ScopeSpecificity);
    // Another tenant never sees the scoped row.
    let other = SelectionQuery {
        tenant: Some("globex".to_string()),
        ..SelectionQuery::default()
    };
    assert_eq!(
        covered_source(&select_one_with(&fixture, "Incident", &other)),
        "pagerduty"
    );
}

// spec: MEDULLA-SRC-AUTH-R005.3
#[test]
fn an_exact_tie_abstains_and_never_chooses_by_name() {
    let tie = |a: &str, b: &str| {
        CoverageFixture::new()
            .with_authority("Incident", "m-a", a, 1, 1)
            .with_authority("Incident", "m-b", b, 1, 1)
    };
    for fixture in [tie("alpha", "omega"), tie("omega", "alpha")] {
        let verdict = select_one(&fixture, "Incident");
        assert_eq!(abstain_reason(&verdict), (NotUsedReason::Ambiguous, None));
        let rows: Vec<_> = verdict
            .premises()
            .iter()
            .filter(|p| matches!(p, Premise::Authority { .. }))
            .collect();
        assert_eq!(rows.len(), 2, "both tied rows are listed");
    }
}

// spec: MEDULLA-SRC-AUTH-R005.4
#[test]
fn the_tiebreak_premise_records_each_rule() {
    let rank = CoverageFixture::new()
        .with_authority("Incident", "one", "a", 1, 1)
        .with_authority("Incident", "two", "b", 1, 2);
    let verdict = select_one(&rank, "Incident");
    assert!(verdict.premises().contains(&Premise::Tiebreak {
        winner: "Incident::one".to_string(),
        loser: "Incident::two".to_string(),
        rule: TiebreakRule::AuthorityRank,
    }));
    let role = rank
        .clone()
        .with_source_role("a", SourceRole::HistoricalContext);
    assert_eq!(
        tiebreak_rule(&select_one(&role, "Incident")),
        TiebreakRule::SourceRole
    );
}

// spec: MEDULLA-SRC-AUTH-R006.1
#[test]
fn one_abstention_per_reason() {
    let base = || CoverageFixture::new().with_relation("Incident", "on", "Service");
    let cases = [
        (
            base()
                .with_authority("Incident", "m", "servicenow", 1, 1)
                .with_unconnected_source("servicenow"),
            NotUsedReason::Unconnected,
        ),
        (
            base().with_row(AuthorityRow::new("Incident", "m", "servicenow", 1, 1).approved(false)),
            NotUsedReason::UnapprovedMapping,
        ),
        (
            base()
                .with_row(AuthorityRow::new("Incident", "m", "lake", 1, 1).accelerated("cost", 60))
                .with_copy_capability("lake", false),
            NotUsedReason::Stale,
        ),
        (
            base()
                .with_row(AuthorityRow::new("Incident", "m", "servicenow", 1, 1).contract("api"))
                .with_source_kind("servicenow", "mcp"),
            NotUsedReason::ContractMismatch,
        ),
        (
            base()
                .with_authority("Incident", "a", "x", 1, 1)
                .with_authority("Incident", "b", "y", 1, 1),
            NotUsedReason::Ambiguous,
        ),
    ];
    for (fixture, expected) in cases {
        let verdict = select_one(&fixture, "Incident");
        assert_eq!(abstain_reason(&verdict).0, expected);
    }
}

fn incident_ranked(allow_fallback: bool) -> CoverageFixture {
    let first = AuthorityRow::new("Incident", "snow", "servicenow", 2, 1);
    let first = if allow_fallback {
        first.allow_fallback()
    } else {
        first
    };
    CoverageFixture::new()
        .with_relation("Incident", "affects", "Service")
        .with_authority("Service", "cmdb", "cmdb", 1, 1)
        .with_row(first)
        .with_authority("Incident", "pd", "pagerduty", 3, 2)
        .with_unconnected_source("servicenow")
}

// spec: MEDULLA-SRC-AUTH-R006.2
#[test]
fn an_unconnected_rank_one_authority_abstains_naming_source_and_concept() {
    let results = select_sources(&incident_ranked(false), &roots("Incident"), 1);
    let verdict = verdict_for(&results, "Incident");
    assert_eq!(
        abstain_reason(&verdict),
        (NotUsedReason::Unconnected, Some("servicenow".to_string()))
    );
    assert_eq!(verdict.class(), "Incident");
    // Other classes still report.
    assert_eq!(covered_source(&verdict_for(&results, "Service")), "cmdb");
}

// spec: MEDULLA-SRC-AUTH-R006.3
#[test]
fn allow_fallback_answers_from_the_next_row_and_records_it() {
    let verdict = select_one(&incident_ranked(true), "Incident");
    assert_eq!(covered_source(&verdict), "pagerduty");
    assert!(verdict.premises().contains(&Premise::FallbackFrom {
        from_row: "Incident::snow".to_string(),
        from_source: "servicenow".to_string(),
        reason: NotUsedReason::Unconnected,
        to_row: "Incident::pd".to_string(),
    }));
    let without = select_one(&incident_ranked(false), "Incident");
    assert!(matches!(without, CoverageResult::Abstained { .. }));
}

fn accelerated(live_route: bool, watermark: u64) -> CoverageFixture {
    CoverageFixture::new()
        .with_row(AuthorityRow::new("Metric", "lake", "lake", 1, 1).accelerated("query cost", 300))
        .with_copy_capability("lake", live_route)
        .with_copy_watermark("Metric::lake", watermark)
}

fn at(now: u64) -> SelectionQuery {
    SelectionQuery {
        now_epoch_seconds: now,
        ..SelectionQuery::default()
    }
}

// spec: MEDULLA-SRC-AUTH-R006.4
#[test]
fn a_stale_copy_falls_back_to_live_or_abstains_stale() {
    let stale_now = 10_000;
    let live = select_one_with(&accelerated(true, 1_000), "Metric", &at(stale_now));
    match &live {
        CoverageResult::Covered { mode, .. } => assert_eq!(*mode, AuditSourceMode::Live),
        other => panic!("expected live fallback, got {other:?}"),
    }
    let no_live = select_one_with(&accelerated(false, 1_000), "Metric", &at(stale_now));
    assert_eq!(
        abstain_reason(&no_live),
        (NotUsedReason::Stale, Some("lake".to_string()))
    );
}

// spec: MEDULLA-SRC-AUTH-R006.5
#[test]
fn without_authority_one_mapping_is_implicit_and_several_are_ambiguous() {
    let one = CoverageFixture::new().with_mapping("Ticket", "jira-tickets", "jira", 1);
    let verdict = select_one(&one, "Ticket");
    assert!(verdict.premises().contains(&Premise::Implicit {
        class: "Ticket".to_string(),
        mapping_name: "jira-tickets".to_string(),
        source: "jira".to_string(),
    }));
    let several = one.with_mapping("Ticket", "gitlab-issues", "gitlab", 1);
    assert_eq!(
        abstain_reason(&select_one(&several, "Ticket")),
        (NotUsedReason::Ambiguous, None)
    );
}

// spec: MEDULLA-SRC-AUTH-R007.2
#[test]
fn the_audit_is_built_from_verdict_premises_only() {
    let fixture = CoverageFixture::new()
        .with_relation("Incident", "affects", "Service")
        .with_authority("Incident", "snow", "servicenow", 2, 1)
        .with_authority("Incident", "pd", "pagerduty", 3, 2)
        .with_authority("Service", "cmdb", "cmdb", 1, 1);
    let verdicts = select_sources(&fixture, &roots("Incident"), 1);
    let audit = audit_from(&verdicts);
    audit
        .validate()
        .expect("the audit meets its wire invariants");
    let covered: Vec<_> = verdicts
        .iter()
        .filter(|v| matches!(v, CoverageResult::Covered { .. }))
        .collect();
    assert_eq!(audit.used.len(), covered.len());
    for (entry, verdict) in audit.used.iter().zip(covered) {
        let rendered: Vec<_> = verdict.premises().iter().map(Premise::render).collect();
        assert_eq!(entry.premises, rendered, "no premise added or dropped");
    }
}

// spec: MEDULLA-SRC-AUTH-R007.3
#[test]
fn the_audit_lists_not_used_rows_with_their_reason() {
    let fixture = CoverageFixture::new()
        .with_relation("Incident", "measures", "Metric")
        .with_row(
            AuthorityRow::new("Incident", "pack", "pagerduty", 1, 1).origin(AuthorityOrigin::Pack),
        )
        .with_authority("Incident", "snow", "servicenow", 1, 2)
        .with_row(AuthorityRow::new("Metric", "lake", "lake", 1, 1).accelerated("cost", 60))
        .with_copy_capability("lake", false);
    let audit = audit_from(&select_sources(&fixture, &roots("Incident"), 1));
    audit.validate().unwrap();
    let reasons: Vec<_> = audit
        .not_used
        .iter()
        .map(|e| (e.source.as_str(), e.reason))
        .collect();
    assert!(reasons.contains(&("pagerduty", Some(NotUsedReason::Proposed))));
    assert!(reasons.contains(&("lake", Some(NotUsedReason::Stale))));
}

// spec: MEDULLA-SRC-AUTH-R008.1
#[test]
fn a_virtual_row_is_always_live() {
    // Even with copy capability and a fresh watermark, a virtual row never
    // resolves to a copy.
    let fixture = CoverageFixture::new()
        .with_authority("Metric", "lake", "lake", 1, 1)
        .with_copy_capability("lake", true)
        .with_copy_watermark("Metric::lake", 100);
    match select_one_with(&fixture, "Metric", &at(100)) {
        CoverageResult::Covered { mode, .. } => assert_eq!(mode, AuditSourceMode::Live),
        other => panic!("expected Covered, got {other:?}"),
    }
}

// spec: MEDULLA-SRC-AUTH-R008.2
#[test]
fn an_accelerated_row_carries_its_reason_into_the_audit() {
    let verdicts = select_sources_with(&accelerated(true, 1_000), &roots("Metric"), 1, &at(1_100));
    let audit = audit_from(&verdicts);
    audit.validate().unwrap();
    let entry = &audit.used[0];
    assert_eq!(entry.mode, Some(AuditSourceMode::Accelerated));
    assert_eq!(entry.materialisation_reason.as_deref(), Some("query cost"));
}

// spec: MEDULLA-SRC-AUTH-R010.1
#[test]
fn source_role_round_trips() {
    for role in [
        SourceRole::LiveTruth,
        SourceRole::HistoricalContext,
        SourceRole::Inspiration,
    ] {
        let text = serde_json::to_string(&role).unwrap();
        assert_eq!(serde_json::from_str::<SourceRole>(&text).unwrap(), role);
    }
    assert_eq!(
        serde_json::to_string(&SourceRole::LiveTruth).unwrap(),
        "\"live_truth\""
    );
    assert!(serde_json::from_str::<SourceRole>("\"rumour\"").is_err());
}

fn conflicting_roles() -> CoverageFixture {
    CoverageFixture::new()
        .with_authority("Service", "history", "warehouse", 1, 1)
        .with_authority("Service", "live", "uptime", 1, 5)
        .with_source_role("warehouse", SourceRole::HistoricalContext)
        .with_source_role("uptime", SourceRole::LiveTruth)
}

// spec: MEDULLA-SRC-AUTH-R010.2
#[test]
fn live_truth_beats_historical_on_conflict() {
    assert_eq!(role_order(), RoleOrder::LIVE_TRUTH_FIRST);
    let verdict = select_one(&conflicting_roles(), "Service");
    assert_eq!(covered_source(&verdict), "uptime");
    // The order is replaceable: historical first flips the choice.
    let historical_first = SelectionQuery {
        role_order: RoleOrder([
            SourceRole::HistoricalContext,
            SourceRole::LiveTruth,
            SourceRole::Inspiration,
        ]),
        ..SelectionQuery::default()
    };
    let flipped = select_one_with(&conflicting_roles(), "Service", &historical_first);
    assert_eq!(covered_source(&flipped), "warehouse");
}

// spec: MEDULLA-SRC-AUTH-R010.4
#[test]
fn inspiration_only_abstains_role_insufficient() {
    let fixture = CoverageFixture::new()
        .with_authority("Price", "blog", "blog", 1, 1)
        .with_source_role("blog", SourceRole::Inspiration);
    let current = SelectionQuery {
        needs_current_state: true,
        ..SelectionQuery::default()
    };
    let verdict = select_one_with(&fixture, "Price", &current);
    assert_eq!(
        abstain_reason(&verdict),
        (NotUsedReason::RoleInsufficient, Some("blog".to_string()))
    );
    assert_eq!(verdict.class(), "Price");
}

// spec: MEDULLA-SRC-AUTH-R010.5
#[test]
fn role_premise_names_both_sources() {
    let verdicts = select_sources(&conflicting_roles(), &roots("Service"), 1);
    let verdict = verdict_for(&verdicts, "Service");
    assert!(verdict.premises().contains(&Premise::Role {
        source: "uptime".to_string(),
        role: SourceRole::LiveTruth,
        preferred_over: "warehouse".to_string(),
        displaced_role: SourceRole::HistoricalContext,
    }));
    let audit = audit_from(&verdicts);
    let entry = &audit.used[0];
    assert_eq!(entry.role, Some(SourceRole::LiveTruth));
    assert!(entry.premises.iter().any(|p| p.contains("warehouse")
        && p.contains("HistoricalContext")
        && p.contains("LiveTruth")));
    assert!(entry.premises.iter().any(|p| p.contains("SourceRole")));
}

// spec: MEDULLA-SRC-AUTH-R011.1
#[test]
fn trust_mark_round_trips() {
    for mark in [TrustMark::Trusted, TrustMark::Untrusted] {
        let text = serde_json::to_string(&mark).unwrap();
        assert_eq!(serde_json::from_str::<TrustMark>(&text).unwrap(), mark);
    }
    assert!(serde_json::from_str::<TrustMark>("\"maybe\"").is_err());
}

// spec: MEDULLA-SRC-AUTH-R011.2
#[test]
fn derived_content_inherits_untrusted() {
    use TrustMark::{Trusted, Untrusted};
    assert_eq!(mark_for_origin(&ContentOrigin::ToolResult, None), Untrusted);
    assert_eq!(
        mark_for_origin(&ContentOrigin::RetrievedDocument, None),
        Untrusted
    );
    assert_eq!(mark_for_origin(&ContentOrigin::Upload, None), Untrusted);
    let live = ContentOrigin::Source {
        approved_mapping: true,
    };
    assert_eq!(mark_for_origin(&live, Some(SourceRole::LiveTruth)), Trusted);
    assert_eq!(
        mark_for_origin(&live, Some(SourceRole::HistoricalContext)),
        Untrusted
    );
    let derived = ContentOrigin::Derived {
        inputs: vec![Trusted, Untrusted],
    };
    assert_eq!(mark_for_origin(&derived, None), Untrusted);
    let clean = ContentOrigin::Derived {
        inputs: vec![Trusted, Trusted],
    };
    assert_eq!(mark_for_origin(&clean, None), Trusted);
}

// spec: MEDULLA-SRC-AUTH-R011.4
#[test]
fn downgrade_without_approval_refused() {
    let stored =
        StoredTrustMark::stamp(&DefaultTrustMarkPolicy, &ContentOrigin::ToolResult, None, 7);
    let refusal =
        change_trust_mark("urn:node:1", &stored, TrustMark::Trusted, None, 8).unwrap_err();
    assert_eq!(refusal.code, TRUST_MARK_DOWNGRADE_REFUSED);
    assert_eq!(refusal.node, "urn:node:1");
    assert_eq!(refusal.missing, "operator_approval_id");
    assert!(change_trust_mark("urn:node:1", &stored, TrustMark::Trusted, Some("  "), 8).is_err());
    let approved =
        change_trust_mark("urn:node:1", &stored, TrustMark::Trusted, Some("appr-1"), 8).unwrap();
    assert_eq!(approved.mark, TrustMark::Trusted);
    assert_eq!(approved.approval_id.as_deref(), Some("appr-1"));
}

// spec: MEDULLA-SRC-AUTH-R011.5
#[test]
fn mark_carries_rule_and_generation() {
    let stored = StoredTrustMark::stamp(
        &DefaultTrustMarkPolicy,
        &ContentOrigin::ToolResult,
        None,
        42,
    );
    let wire = serde_json::to_value(&stored).unwrap();
    assert_eq!(wire["markedBy"], "tool_result_untrusted");
    assert_eq!(wire["markedAtGeneration"], 42);
    let read: StoredTrustMark = serde_json::from_value(wire).unwrap();
    assert_eq!(read, stored);
}

#[test]
fn a_question_over_an_unconnected_source_abstains_naming_it() {
    let connected: BTreeSet<String> = ["radarr".to_string()].into();
    assert!(question_abstention(&[("radarr", "Video")], &connected).is_none());
    let audit = question_abstention(
        &[("radarr", "Video"), ("qbittorrent", "Torrent")],
        &connected,
    )
    .expect("an unconnected required source abstains");
    audit.validate().unwrap();
    assert!(audit.used.is_empty());
    assert_eq!(audit.not_used.len(), 1);
    assert_eq!(audit.not_used[0].source, "qbittorrent");
    assert_eq!(audit.not_used[0].concept, "Torrent");
    assert_eq!(audit.not_used[0].reason, Some(NotUsedReason::Unconnected));
}

#[cfg(feature = "rdf")]
#[test]
fn connected_sources_need_an_approved_mapping() {
    let document = r#"
@prefix vg: <http://knuckles.team/kg/virtual-graph#> .
@prefix dc: <http://purl.org/dc/terms/> .
@prefix k: <urn:t:> .
k:a a vg:SourceConnection ; dc:identifier "a" .
k:ca a vg:MetadataContract ; vg:contractSource k:a .
k:ma a vg:VirtualMapping ; vg:mapsContract k:ca ; vg:approved true .
k:b a vg:SourceConnection ; dc:identifier "b" .
k:cb a vg:MetadataContract ; vg:contractSource k:b .
k:mb a vg:VirtualMapping ; vg:mapsContract k:cb ; vg:approved false .
k:c a vg:SourceConnection ; dc:identifier "c" .
"#;
    let connected = connected_sources_from_turtle(&[document]).unwrap();
    assert_eq!(connected, BTreeSet::from(["a".to_string()]));
}
