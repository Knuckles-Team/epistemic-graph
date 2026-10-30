//! Connector competency questions (EG-CQ-CONN), bulk chunk 2 (P12 question pairs,
//! R018–R034): fixture SPARQL answers over the connector's assigned ontology
//! vocabulary, in the established pattern of
//! `crates/eg-rdf/tests/connector_competency.rs`.
//!
//! Each test records a disposable fixture (no live credentials, no network —
//! `specs/connector-competency/test-spec.md`), loads it together with the
//! vocabulary module the question row assigns, executes the question through
//! the native SPARQL surface, and asserts a non-empty answer whose rows carry
//! provenance (source, record id, as-of). The `dc:` provenance triples stand in
//! for the fields the connector's virtual mapping attaches to fetched records
//! at query time.
//!
//! This chunk lands only the rows fully expressible in the vocabulary on main.
//! The remaining R018–R034 question rows are blocked on their producer
//! vocabulary rows (EG-CQ-CONN-R068–R082 and EG-SUPPLY-VOCAB-R003), which are
//! not yet on main, and the freshrss / jellyfin / kafka rows (R022, R027, R029)
//! are among the plan's 23 MISSING connectors with no assigned vocabulary.
//!
//! For the blocked rows the engine answers with the typed abstention: the
//! declared access contracts carry no approved `vg:VirtualMapping` (the
//! producer terms are not on main), so `question_abstention` names each
//! unconnected source with the closed reason `Unconnected` — never an empty
//! success. R033 (legal-peripherals) is the chunk's exception: every one of
//! its producer terms (EG-CQ-CONN-R074.4–R074.6) is on main, so its two
//! questions run as fixture SPARQL answers and its acceptance row also binds
//! the `connector_audit_reassignment::legal_peripherals` audit.
#![cfg(feature = "sparql")]

mod common;

use common::{
    assert_acceptance, assert_unconnected_question, parse_all,
    unconnected_contract_fixture, AcceptanceQuestion,
};
use eg_core::graph::{GraphCore, GraphView};
use eg_rdf::mapping::{load_triples, parse_turtle, IriStore};
use eg_rdf::source_selection::connected_sources_from_turtle;
use eg_rdf::sparql::{execute, Dataset, Projection, QueryOutcome};

/// Load the assigned vocabulary documents and the fixture into one graph and
/// return the analysis view the SPARQL evaluator scans.
fn graph(schema: &[&str], fixture: &str) -> GraphView {
    let core = GraphCore::new();
    let mut iris = IriStore::default();
    load_triples(&core, &mut iris, "schema", parse_all(schema)).unwrap();
    load_triples(&core, &mut iris, "fixture", parse_turtle(fixture).unwrap()).unwrap();
    core.analysis_snapshot()
}

/// Execute a SELECT and return `(columns, rows)` with every cell as its lexical form.
fn select(view: &GraphView, query: &str) -> (Vec<String>, Vec<Vec<String>>) {
    let outcome = execute(
        &Dataset::new(view, Vec::new()),
        query,
        &Projection::raw(),
        None,
    )
    .expect("query executes");
    let table = match outcome {
        QueryOutcome::Solutions(table) => table,
        other => panic!("expected a SELECT solution table, got {other:?}"),
    };
    let (vars, rows) = table.to_rows();
    let rows = rows
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|cell| cell.expect("every selected variable binds"))
                .collect()
        })
        .collect();
    (vars, rows)
}

/// The local name of a bracketed node-id cell (`<urn:…/client-portal>` → `client-portal`).
fn local(cell: &str) -> &str {
    cell.trim_start_matches('<')
        .trim_end_matches('>')
        .rsplit('/')
        .next()
        .unwrap()
}

// spec: EG-CQ-CONN-R030.1
#[test]
fn keycloak_user_clients_and_roles_answer() {
    // "Which clients can a given user sign in to, and through which role?" —
    // answered from the keycloak connector alone (Vocabulary: identity-v1.ttl,
    // with :Application from the shared service-operations core). Frozen as-of:
    // 2026-01-03T00:00:00Z.
    let fixture = r#"
@prefix : <http://knuckles.team/kg#> .
@prefix dc: <http://purl.org/dc/terms/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix f: <urn:eg-cq-conn:keycloak:fixture/> .

# alice signs in to the portal client through the editor role — the answer row.
f:user-alice a :IdentityUser ;
    :assignedApp f:client-portal ;
    :hasRole f:role-editor ;
    dc:source "keycloak" ;
    dc:identifier "kc:rec:user-alice" ;
    dc:date "2026-01-03T00:00:00Z"^^xsd:dateTime .
f:client-portal a :Application .
f:role-editor a :IdentityRole .

# bob signs in to the archive client through the viewer role — a different user,
# so his assignment must not answer alice's question.
f:user-bob a :IdentityUser ;
    :assignedApp f:client-archive ;
    :hasRole f:role-viewer .
f:client-archive a :Application .
f:role-viewer a :IdentityRole .
"#;
    let view = graph(
        &[
            include_str!("../../eg-core/ontology/identity-v1.ttl"),
            include_str!("../../eg-core/ontology/service_operations-v1.ttl"),
        ],
        fixture,
    );
    let (vars, rows) = select(
        &view,
        r#"
PREFIX : <http://knuckles.team/kg#>
PREFIX dc: <http://purl.org/dc/terms/>
PREFIX f: <urn:eg-cq-conn:keycloak:fixture/>
SELECT ?client ?role ?source ?record ?asof WHERE {
  f:user-alice a :IdentityUser ;
               :assignedApp ?client ;
               :hasRole ?role ;
               dc:source ?source ;
               dc:identifier ?record ;
               dc:date ?asof .
}"#,
    );
    let idx = |col: &str| vars.iter().position(|v| v == col).unwrap();
    let i_client = idx("client");
    let i_role = idx("role");
    let i_source = idx("source");
    let i_record = idx("record");
    let i_asof = idx("asof");
    assert_eq!(
        rows.len(),
        1,
        "exactly one client/role pair answers for alice: {rows:?}"
    );
    let row = &rows[0];
    assert_eq!(local(&row[i_client]), "client-portal");
    assert_eq!(local(&row[i_role]), "role-editor");
    // Provenance: source, record id, as-of all carry on the answer row.
    assert_eq!(row[i_source], "keycloak");
    assert_eq!(row[i_record], "kc:rec:user-alice");
    assert_eq!(row[i_asof], "2026-01-03T00:00:00Z");
}

// ---------------------------------------------------------------------------
// leanix (R032): blocked on the enterprise-v1 producer terms
// (EG-CQ-CONN-R080.2–R080.4) — typed abstention through the declared access
// contract, naming the unconnected source.
// ---------------------------------------------------------------------------

// spec: EG-CQ-CONN-R032.1
#[test]
fn leanix_end_of_life_fact_sheets_with_active_users_abstain_by_name() {
    // "Which fact sheets of type application have lifecycle phase
    // end-of-life but still have active users?" — the leanix connector alone
    // (Vocabulary: enterprise-v1.ttl). The producer terms `factSheetType`,
    // `lifecyclePhase` and `activeUserCount` (EG-CQ-CONN-R080.2–R080.4) are
    // not on main, so the declared access contract carries no approved
    // virtual mapping and the engine answers with the typed abstention
    // naming the unconnected source — never an empty success.
    let contract = unconnected_contract_fixture(&["leanix"]);
    let connected =
        connected_sources_from_turtle(&[&contract]).expect("the declared contract parses");
    assert!(
        connected.is_empty(),
        "no approved mapping exists on main: {connected:?}"
    );
    assert_unconnected_question(
        "EG-CQ-CONN-R032.1",
        &connected,
        &[("leanix", "EAFactSheet")],
    );
}

// spec: EG-CQ-CONN-R032.2
#[test]
fn leanix_end_of_life_apps_with_active_ci_and_incidents_abstain_by_name() {
    // "Which end-of-life LeanIX applications still have an active ServiceNow
    // CI and open incidents?" — leanix linked to archimate and servicenow
    // through the application concept (Vocabulary: enterprise-v1.ttl).
    // Blocked the same way as R032.1: the leanix producer terms
    // (EG-CQ-CONN-R080.x) and the archimate `ApplicationComponent` term
    // (EG-CQ-CONN-R070.2) are not on main.
    let contract = unconnected_contract_fixture(&["leanix", "archimate", "servicenow"]);
    let connected =
        connected_sources_from_turtle(&[&contract]).expect("the declared contract parses");
    assert!(
        connected.is_empty(),
        "no approved mapping exists on main: {connected:?}"
    );
    assert_unconnected_question(
        "EG-CQ-CONN-R032.2",
        &connected,
        &[
            ("leanix", "EAFactSheet"),
            ("archimate", "ApplicationComponent"),
            ("servicenow", "ConfigurationItem"),
            ("servicenow", "Incident"),
        ],
    );
}

// spec: EG-CQ-CONN-R032.3
#[test]
fn leanix_acceptance_through_declared_contracts() {
    // Both questions run against the connectors' declared access contracts:
    // read-only MCP connections, nothing in the contract asks the engine to
    // hold a copy of the source. With the producer terms not on main every
    // required source is unconnected, so each question gets its typed
    // abstention naming exactly the sources it reads — never an empty
    // success.
    let contract = unconnected_contract_fixture(&["leanix", "archimate", "servicenow"]);
    let connected =
        connected_sources_from_turtle(&[&contract]).expect("the declared contract parses");
    assert!(
        connected.is_empty(),
        "no approved mapping exists on main: {connected:?}"
    );
    assert_unconnected_question(
        "EG-CQ-CONN-R032.1",
        &connected,
        &[("leanix", "EAFactSheet")],
    );
    assert_unconnected_question(
        "EG-CQ-CONN-R032.2",
        &connected,
        &[
            ("leanix", "EAFactSheet"),
            ("archimate", "ApplicationComponent"),
            ("servicenow", "ConfigurationItem"),
            ("servicenow", "Incident"),
        ],
    );
}

// ---------------------------------------------------------------------------
// legal-peripherals (R033): every producer term is on main
// (EG-CQ-CONN-R074.4–R074.6), so both questions run as fixture SPARQL answers
// over core-foundation-v1.ttl.
// ---------------------------------------------------------------------------

const LP_ASOF: &str = "2026-01-15T00:00:00Z";

const LP_EXPIRING_FIXTURE: &str = r#"
@prefix : <http://knuckles.team/kg#> .
@prefix dc: <http://purl.org/dc/terms/> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix f: <urn:eg-cq-conn:legal-peripherals:fixture/> .

# The answer: expires within 90 days of the frozen as-of, no renewal action.
f:agreement-atlas a :Agreement ;
    :agreementExpiresAt "2026-02-10"^^xsd:date ;
    :counterparty f:vendor-atlas ;
    dc:source "legal-peripherals" ;
    dc:identifier "lp:rec:agreement-atlas" ;
    dc:date "2026-01-15T00:00:00Z"^^xsd:dateTime .
f:vendor-atlas a :LegalEntity ;
    rdfs:label "Atlas Legal Services" .

# Adversarial: expires beyond the 90-day window — no row.
f:agreement-meridian a :Agreement ;
    :agreementExpiresAt "2026-08-01"^^xsd:date ;
    :counterparty f:vendor-meridian ;
    dc:source "legal-peripherals" ;
    dc:identifier "lp:rec:agreement-meridian" ;
    dc:date "2026-01-15T00:00:00Z"^^xsd:dateTime .
f:vendor-meridian a :LegalEntity .

# Adversarial: expires inside the window but has a recorded renewal action —
# no row.
f:agreement-northwind a :Agreement ;
    :agreementExpiresAt "2026-03-05"^^xsd:date ;
    :counterparty f:vendor-northwind ;
    :hasRenewalAction f:renewal-northwind ;
    dc:source "legal-peripherals" ;
    dc:identifier "lp:rec:agreement-northwind" ;
    dc:date "2026-01-15T00:00:00Z"^^xsd:dateTime .
f:vendor-northwind a :LegalEntity .
f:renewal-northwind a :Action ;
    rdfs:label "Renew the Northwind agreement" .

# Adversarial: already expired before the frozen as-of — no row.
f:agreement-sunset a :Agreement ;
    :agreementExpiresAt "2026-01-10"^^xsd:date ;
    :counterparty f:vendor-sunset ;
    dc:source "legal-peripherals" ;
    dc:identifier "lp:rec:agreement-sunset" ;
    dc:date "2026-01-15T00:00:00Z"^^xsd:dateTime .
f:vendor-sunset a :LegalEntity .
"#;

const LP_EXPIRING_QUERY: &str = r#"
PREFIX : <http://knuckles.team/kg#>
PREFIX dc: <http://purl.org/dc/terms/>
SELECT ?agreement ?counterparty ?expiry ?source ?record ?asof WHERE {
  ?agreement a :Agreement ;
             :agreementExpiresAt ?expiry ;
             :counterparty ?counterparty ;
             dc:source ?source ; dc:identifier ?record ; dc:date ?asof .
  BIND(YEAR(?expiry) * 10000 + MONTH(?expiry) * 100 + DAY(?expiry) AS ?expiryKey)
  FILTER(?expiryKey > 20260115 && ?expiryKey <= 20260415)
  FILTER NOT EXISTS { ?agreement :hasRenewalAction ?action }
}"#;

fn lp_expiring_view() -> GraphView {
    graph(
        &[include_str!("../../eg-core/ontology/core-foundation-v1.ttl")],
        LP_EXPIRING_FIXTURE,
    )
}

fn lp_expiring_answer() -> (Vec<String>, Vec<Vec<String>>) {
    let view = lp_expiring_view();
    select(&view, LP_EXPIRING_QUERY)
}

// spec: EG-CQ-CONN-R033.1
#[test]
fn legal_peripherals_expiring_contracts_without_renewal_answer() {
    // "Which contracts expire within 90 days and have no renewal action
    // recorded?" — answered from the legal-peripherals connector alone
    // (Vocabulary: core-foundation-v1.ttl, with :Agreement, :counterparty,
    // :agreementExpiresAt and :hasRenewalAction from the landed
    // EG-CQ-CONN-R074.4–R074.6 producer rows). Frozen as-of:
    // 2026-01-15T00:00:00Z; the 90-day window is the open interval
    // (2026-01-15, 2026-04-15] because the engine's comparator does not
    // order date lexicals.
    let (vars, rows) = lp_expiring_answer();
    let idx = |col: &str| vars.iter().position(|v| v == col).unwrap();
    assert_eq!(
        rows.len(),
        1,
        "exactly one expiring contract without a renewal action: {rows:?}"
    );
    let row = &rows[0];
    assert_eq!(local(&row[idx("agreement")]), "agreement-atlas");
    assert_eq!(local(&row[idx("counterparty")]), "vendor-atlas");
    assert_eq!(row[idx("expiry")], "2026-02-10");
    // Provenance: source, record id, as-of all carry on the answer row.
    assert_eq!(row[idx("source")], "legal-peripherals");
    assert_eq!(row[idx("record")], "lp:rec:agreement-atlas");
    assert_eq!(row[idx("asof")], LP_ASOF);
}

const LP_CROSS_FIXTURE: &str = r#"
@prefix : <http://knuckles.team/kg#> .
@prefix dc: <http://purl.org/dc/terms/> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix f: <urn:eg-cq-conn:legal-peripherals:fixture/> .

# The answer: an expiring contract, its scanned copy in Paperless-ngx, and an
# OneTrust assessment of the counterparty vendor — joined through the
# approved identity-resolution contract (owl:sameAs), never through copied
# source data.
f:agreement-atlas a :Agreement ;
    :agreementExpiresAt "2026-02-10"^^xsd:date ;
    :counterparty f:vendor-atlas ;
    dc:source "legal-peripherals" ;
    dc:identifier "lp:rec:agreement-atlas" ;
    dc:date "2026-01-15T00:00:00Z"^^xsd:dateTime .
f:vendor-atlas a :LegalEntity ;
    rdfs:label "Atlas Legal Services" .
f:scan-atlas a :Document ;
    :documentType "contract" ;
    owl:sameAs f:agreement-atlas ;
    dc:source "paperless-ngx" ;
    dc:identifier "px:rec:scan-atlas" ;
    dc:date "2026-01-15T00:00:00Z"^^xsd:dateTime .
f:vendor-atlas-record a :Vendor ;
    rdfs:label "Atlas Legal Services" ;
    owl:sameAs f:vendor-atlas .
f:assessment-atlas a :PrivacyAssessment ;
    :assessedVendor f:vendor-atlas-record ;
    :assessmentStatus "approved" ;
    dc:source "onetrust" ;
    dc:identifier "ot:rec:assessment-atlas" ;
    dc:date "2026-01-15T00:00:00Z"^^xsd:dateTime .

# Adversarial: an expiring contract with a scanned copy, but its
# counterparty has no OneTrust assessment — no row.
f:agreement-bravo a :Agreement ;
    :agreementExpiresAt "2026-03-20"^^xsd:date ;
    :counterparty f:vendor-bravo ;
    dc:source "legal-peripherals" ;
    dc:identifier "lp:rec:agreement-bravo" ;
    dc:date "2026-01-15T00:00:00Z"^^xsd:dateTime .
f:vendor-bravo a :LegalEntity .
f:scan-bravo a :Document ;
    :documentType "contract" ;
    owl:sameAs f:agreement-bravo ;
    dc:source "paperless-ngx" ;
    dc:identifier "px:rec:scan-bravo" ;
    dc:date "2026-01-15T00:00:00Z"^^xsd:dateTime .

# Adversarial: an expiring contract with a matching OneTrust assessment, but
# no scanned copy in Paperless-ngx — no row.
f:agreement-cipher a :Agreement ;
    :agreementExpiresAt "2026-04-01"^^xsd:date ;
    :counterparty f:vendor-cipher ;
    dc:source "legal-peripherals" ;
    dc:identifier "lp:rec:agreement-cipher" ;
    dc:date "2026-01-15T00:00:00Z"^^xsd:dateTime .
f:vendor-cipher a :LegalEntity ;
    owl:sameAs f:vendor-cipher-record .
f:vendor-cipher-record a :Vendor .
f:assessment-cipher a :PrivacyAssessment ;
    :assessedVendor f:vendor-cipher-record ;
    :assessmentStatus "in_review" ;
    dc:source "onetrust" ;
    dc:identifier "ot:rec:assessment-cipher" ;
    dc:date "2026-01-15T00:00:00Z"^^xsd:dateTime .

# Adversarial: the full picture, but the contract expires beyond the
# 90-day window — no row.
f:agreement-quiet a :Agreement ;
    :agreementExpiresAt "2026-09-30"^^xsd:date ;
    :counterparty f:vendor-quiet ;
    dc:source "legal-peripherals" ;
    dc:identifier "lp:rec:agreement-quiet" ;
    dc:date "2026-01-15T00:00:00Z"^^xsd:dateTime .
f:vendor-quiet a :LegalEntity ;
    owl:sameAs f:vendor-quiet-record .
f:vendor-quiet-record a :Vendor .
f:scan-quiet a :Document ;
    :documentType "contract" ;
    owl:sameAs f:agreement-quiet ;
    dc:source "paperless-ngx" ;
    dc:identifier "px:rec:scan-quiet" ;
    dc:date "2026-01-15T00:00:00Z"^^xsd:dateTime .
f:assessment-quiet a :PrivacyAssessment ;
    :assessedVendor f:vendor-quiet-record ;
    :assessmentStatus "approved" ;
    dc:source "onetrust" ;
    dc:identifier "ot:rec:assessment-quiet" ;
    dc:date "2026-01-15T00:00:00Z"^^xsd:dateTime .
"#;

const LP_CROSS_QUERY: &str = r#"
PREFIX : <http://knuckles.team/kg#>
PREFIX dc: <http://purl.org/dc/terms/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
SELECT ?agreement ?scan ?assessment ?source ?record ?asof WHERE {
  ?agreement a :Agreement ;
             :agreementExpiresAt ?expiry ;
             :counterparty ?party ;
             dc:source ?source ; dc:identifier ?record ; dc:date ?asof .
  BIND(YEAR(?expiry) * 10000 + MONTH(?expiry) * 100 + DAY(?expiry) AS ?expiryKey)
  FILTER(?expiryKey > 20260115 && ?expiryKey <= 20260415)
  ?scan a :Document ;
        owl:sameAs ?agreement ;
        :documentType "contract" ;
        dc:source "paperless-ngx" ;
        dc:identifier ?scanRecord .
  ?assessment a :PrivacyAssessment ;
              :assessedVendor ?vendor ;
              dc:source "onetrust" ;
              dc:identifier ?assessRecord .
  ?vendor owl:sameAs ?party .
}"#;

fn lp_cross_answer() -> (Vec<String>, Vec<Vec<String>>) {
    let view = graph(
        &[
            include_str!("../../eg-core/ontology/core-foundation-v1.ttl"),
            include_str!("../../eg-core/ontology/personal-v1.ttl"),
            include_str!("../../eg-core/ontology/government-v1.ttl"),
            include_str!("../../eg-core/ontology/trm-v1.ttl"),
        ],
        LP_CROSS_FIXTURE,
    );
    select(&view, LP_CROSS_QUERY)
}

// spec: EG-CQ-CONN-R033.2
#[test]
fn legal_peripherals_expiring_contracts_with_scan_and_assessment_answer() {
    // "Which expiring contracts have a scanned copy in Paperless-ngx and a
    // matching OneTrust vendor assessment?" — legal-peripherals linked to
    // paperless-ngx and onetrust through the contract / document concept
    // (Vocabulary: core-foundation-v1.ttl). The producer terms are all on
    // main: :Agreement/:counterparty/:agreementExpiresAt (core-foundation,
    // EG-CQ-CONN-R074.x), :documentType/:Document (personal-v1 and
    // core-foundation) and :PrivacyAssessment/:assessedVendor
    // (government-v1, EG-CQ-CONN-R073.x). The two cross-source links ride
    // owl:sameAs, the output of the approved identity-resolution contract.
    // Frozen as-of: 2026-01-15T00:00:00Z.
    let (vars, rows) = lp_cross_answer();
    let idx = |col: &str| vars.iter().position(|v| v == col).unwrap();
    assert_eq!(
        rows.len(),
        1,
        "exactly one expiring contract with both the scanned copy and the assessment: {rows:?}"
    );
    let row = &rows[0];
    assert_eq!(local(&row[idx("agreement")]), "agreement-atlas");
    assert_eq!(local(&row[idx("scan")]), "scan-atlas");
    assert_eq!(local(&row[idx("assessment")]), "assessment-atlas");
    // Provenance: source, record id, as-of all carry on the answer row.
    assert_eq!(row[idx("source")], "legal-peripherals");
    assert_eq!(row[idx("record")], "lp:rec:agreement-atlas");
    assert_eq!(row[idx("asof")], LP_ASOF);
}

// spec: EG-CQ-CONN-R033.3
#[test]
fn legal_peripherals_acceptance_through_declared_contracts() {
    // Both questions run against the connectors' declared access contracts
    // (approved `vg:VirtualMapping` per class, read-only — no copy
    // capability). With every source connected, each question answers
    // non-empty with provenance through its own test; with any one required
    // source unconnected, the question returns the typed abstention naming
    // exactly that source — never an empty success.
    assert_acceptance(&[
        AcceptanceQuestion {
            id: "EG-CQ-CONN-R033.1",
            required: &[("legal-peripherals", "Agreement")],
            answer: legal_peripherals_expiring_contracts_without_renewal_answer,
        },
        AcceptanceQuestion {
            id: "EG-CQ-CONN-R033.2",
            required: &[
                ("legal-peripherals", "Agreement"),
                ("paperless-ngx", "Document"),
                ("onetrust", "PrivacyAssessment"),
            ],
            answer: legal_peripherals_expiring_contracts_with_scan_and_assessment_answer,
        },
    ]);
}

/// Noncommercial mapping acceptance for legal-peripherals
/// (`specs/connector-competency/test-spec.md`): the audit binds the exact
/// `legal-peripherals` Coverage row of plan.md and the `Vocabulary:` cells of
/// both question rows in requirements.md to `core-foundation-v1.ttl`, and
/// resolves every listed producer IRI through its registered CoreSpec module.
/// A missing producer term, a same-local-name IRI from another module, or an
/// unrelated-row-only edit must each fail.
#[allow(non_snake_case)]
mod connector_audit_reassignment {
    use eg_core::graph::{GraphSchemaSources, SchemaSourceOrigin};
    use eg_rdf::mapping::parse_turtle;
    use eg_rdf::oxrdf::{NamedNode, Triple};

    const KG: &str = "http://knuckles.team/kg#";
    const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
    const OWL_OBJECT_PROPERTY: &str = "http://www.w3.org/2002/07/owl#ObjectProperty";
    const OWL_DATATYPE_PROPERTY: &str = "http://www.w3.org/2002/07/owl#DatatypeProperty";

    const PLAN: &str = include_str!("../../../specs/connector-competency/plan.md");
    const REQUIREMENTS: &str =
        include_str!("../../../specs/connector-competency/requirements.md");

    /// The producer terms of the legal-peripherals questions, as full IRIs of
    /// the module their producer rows (EG-CQ-CONN-R074.4–R074.6) declare
    /// them in.
    const PRODUCERS: [&str; 3] = [
        "http://knuckles.team/kg#counterparty",
        "http://knuckles.team/kg#agreementExpiresAt",
        "http://knuckles.team/kg#hasRenewalAction",
    ];

    fn has_iri(triples: &[Triple], subject: &str, predicate: &str, object: &str) -> bool {
        triples.contains(&Triple::new(
            NamedNode::new(subject).unwrap(),
            NamedNode::new(predicate).unwrap(),
            NamedNode::new(object).unwrap(),
        ))
    }

    /// The exact `legal-peripherals` Coverage row of plan.md as
    /// `(domain, vocabulary)`. The row must exist exactly once; any other
    /// row is irrelevant to this connector's acceptance.
    fn coverage_row(plan: &str) -> Result<(&str, &str), String> {
        let mut found = 0;
        for line in plan.lines() {
            if !line.starts_with("| legal-peripherals |") {
                continue;
            }
            found += 1;
            let cells: Vec<&str> = line.split('|').map(str::trim).collect();
            let cells = &cells[1..cells.len() - 1];
            if cells.len() != 3 {
                return Err(format!("legal-peripherals Coverage row has no 3 cells: {line}"));
            }
            return Ok((cells[1], cells[2]));
        }
        if found == 0 {
            return Err("no legal-peripherals Coverage row in plan.md".into());
        }
        Err(format!("legal-peripherals has {found} Coverage rows; expected exactly one"))
    }

    /// The `Vocabulary:` cell of one question row of requirements.md.
    fn vocabulary_cell(requirements: &str, row_id: &str) -> Result<String, String> {
        let line = requirements
            .lines()
            .find(|line| line.starts_with(&format!("| `{row_id}` |")))
            .ok_or_else(|| format!("no requirements row {row_id}"))?;
        let cells: Vec<&str> = line.split('|').map(str::trim).collect();
        let body = cells.get(2).ok_or("the row has no body cell")?;
        let value = body
            .split("Vocabulary: ")
            .nth(1)
            .ok_or_else(|| format!("the {row_id} row has no Vocabulary cell"))?;
        Ok(value
            .trim()
            .trim_end_matches('.')
            .trim_matches('`')
            .to_string())
    }

    /// The ontology document of the registered CoreSpec module, with module
    /// ownership asserted (a source id stays bound to its module).
    fn registered_ontology(source_id: &str, expected_module: &str) -> String {
        let sources = GraphSchemaSources::default();
        sources.validate().expect("the default core catalog validates");
        let source = sources
            .core
            .get(source_id)
            .unwrap_or_else(|| panic!("registered source {source_id} is missing"));
        match &source.origin {
            SchemaSourceOrigin::Core { module, version, .. } => {
                assert_eq!(module, expected_module, "source module ownership changed");
                assert_eq!(*version, 1, "unexpected source version");
            }
            origin => panic!("{source_id} has non-core origin {origin:?}"),
        }
        source
            .ontology_ttl
            .clone()
            .unwrap_or_else(|| panic!("registered source {source_id} has no ontology"))
            .to_string()
    }

    /// The acceptance: `core-foundation-v1.ttl` in the exact Coverage row and
    /// in both question Vocabulary cells, and every producer IRI declared by
    /// the registered core-foundation module.
    fn acceptance(plan: &str, requirements: &str, producers: &[&str]) -> Result<(), String> {
        let (_domain, vocabulary) = coverage_row(plan)?;
        if vocabulary != "core-foundation-v1.ttl" {
            return Err(format!(
                "the legal-peripherals Coverage row assigns {vocabulary}, not core-foundation-v1.ttl"
            ));
        }
        for row_id in ["EG-CQ-CONN-R033.1", "EG-CQ-CONN-R033.2"] {
            let cell = vocabulary_cell(requirements, row_id)?;
            if cell != "crates/eg-core/ontology/core-foundation-v1.ttl" {
                return Err(format!("{row_id} assigns {cell}, not core-foundation-v1.ttl"));
            }
        }
        let ontology = registered_ontology("core:core-foundation@1", "core-foundation");
        let triples = parse_turtle(&ontology)
            .map_err(|error| format!("the registered core-foundation ontology parses: {error}"))?;
        for producer in producers {
            let declared = has_iri(triples.as_slice(), producer, RDF_TYPE, OWL_OBJECT_PROPERTY)
                || has_iri(triples.as_slice(), producer, RDF_TYPE, OWL_DATATYPE_PROPERTY);
            if !declared {
                return Err(format!(
                    "producer {producer} ({}) is not declared by the registered core-foundation module",
                    producer.trim_start_matches(&format!("{KG}"))
                ));
            }
        }
        Ok(())
    }

    // spec: EG-CQ-CONN-R033.3
    #[test]
    fn legal_peripherals() {
        // Positive: the exact Coverage row, both question Vocabulary cells
        // and every producer IRI resolve through the registered module.
        acceptance(PLAN, REQUIREMENTS, &PRODUCERS)
            .expect("the legal-peripherals acceptance holds on main");

        // Negative: each producer term removed — the acceptance must fail.
        for missing in PRODUCERS {
            let rest: Vec<&str> = PRODUCERS.iter().copied().filter(|iri| *iri != missing).collect();
            assert!(
                acceptance(PLAN, REQUIREMENTS, &rest).is_err(),
                "removing the producer term {missing} must fail the acceptance"
            );
        }

        // Negative: a same-local-name IRI from another module (the
        // infrastructure module's namespace) — the core-foundation module
        // does not declare it.
        let wrong_iri = "http://knuckles.team/kg/infrastructure#counterparty";
        let wrong_module = [wrong_iri, PRODUCERS[1], PRODUCERS[2]];
        assert!(
            acceptance(PLAN, REQUIREMENTS, &wrong_module).is_err(),
            "a same-local-name producer IRI from another module must fail"
        );

        // Negative: an unrelated-row-only edit. The egeria row keeps
        // core-foundation-v1.ttl, but the legal-peripherals row is repointed
        // — editing another connector's row must never satisfy this
        // connector's acceptance.
        let repointed = PLAN.replacen(
            "| legal-peripherals | governance | core-foundation-v1.ttl |",
            "| legal-peripherals | governance | enterprise-v1.ttl |",
            1,
        );
        assert_ne!(
            repointed, PLAN,
            "the fixture must alter the legal-peripherals row"
        );
        assert!(
            acceptance(&repointed, REQUIREMENTS, &PRODUCERS).is_err(),
            "an unrelated-row-only edit must never satisfy the target connector's acceptance"
        );

        // Negative: one incorrect target cell retained — the R033.2
        // Vocabulary cell no longer names core-foundation-v1.ttl.
        let mut lines: Vec<String> = REQUIREMENTS.lines().map(String::from).collect();
        let target = lines
            .iter()
            .position(|line| line.starts_with("| `EG-CQ-CONN-R033.2` |"))
            .expect("the R033.2 row exists");
        lines[target] = lines[target].replacen(
            "Vocabulary: `crates/eg-core/ontology/core-foundation-v1.ttl`.",
            "Vocabulary: MISSING.",
            1,
        );
        assert_ne!(
            lines[target], REQUIREMENTS.lines().nth(target).unwrap(),
            "the fixture must alter the R033.2 cell"
        );
        let wrong_cell = lines.join("\n");
        assert!(
            acceptance(PLAN, &wrong_cell, &PRODUCERS).is_err(),
            "one incorrect target cell must fail the acceptance"
        );
    }
}

// ---------------------------------------------------------------------------
// lgtm (R034): blocked on the infrastructure-v1 Alert producer terms
// (EG-CQ-CONN-R068.8–R068.12) — typed abstention through the declared access
// contract, naming the unconnected source.
// ---------------------------------------------------------------------------

// spec: EG-CQ-CONN-R034.1
#[test]
fn lgtm_firing_alerts_abstain_by_name() {
    // "Which alerts are firing right now, with what severity and for how
    // long?" — the lgtm connector alone (Vocabulary:
    // infrastructure-v1.ttl). The producer terms `Alert`, `alertSeverity`,
    // `alertState`, `firingSince` and `alertsOn` (EG-CQ-CONN-R068.8–R068.12)
    // are not on main, so the declared access contract carries no approved
    // virtual mapping and the engine answers with the typed abstention
    // naming the unconnected source — never an empty success.
    let contract = unconnected_contract_fixture(&["lgtm"]);
    let connected =
        connected_sources_from_turtle(&[&contract]).expect("the declared contract parses");
    assert!(
        connected.is_empty(),
        "no approved mapping exists on main: {connected:?}"
    );
    assert_unconnected_question("EG-CQ-CONN-R034.1", &connected, &[("lgtm", "Alert")]);
}

// spec: EG-CQ-CONN-R034.2
#[test]
fn lgtm_firing_alerts_without_incidents_abstain_by_name() {
    // "Which firing Grafana alerts have no open ServiceNow incident, and
    // which incidents have no alert?" — lgtm linked to uptime and
    // servicenow through the service / alert concept (Vocabulary:
    // infrastructure-v1.ttl). Blocked the same way as R034.1: the Alert
    // producer terms (EG-CQ-CONN-R068.8–R068.12) are not on main.
    let contract = unconnected_contract_fixture(&["lgtm", "uptime", "servicenow"]);
    let connected =
        connected_sources_from_turtle(&[&contract]).expect("the declared contract parses");
    assert!(
        connected.is_empty(),
        "no approved mapping exists on main: {connected:?}"
    );
    assert_unconnected_question(
        "EG-CQ-CONN-R034.2",
        &connected,
        &[
            ("lgtm", "Alert"),
            ("uptime", "MonitoredService"),
            ("servicenow", "Incident"),
        ],
    );
}

// spec: EG-CQ-CONN-R034.3
#[test]
fn lgtm_acceptance_through_declared_contracts() {
    // Both questions run against the connectors' declared access contracts:
    // read-only MCP connections, nothing in the contract asks the engine to
    // hold a copy of the source. With the Alert producer terms not on main
    // every required source is unconnected, so each question gets its typed
    // abstention naming exactly the sources it reads — never an empty
    // success.
    let contract = unconnected_contract_fixture(&["lgtm", "uptime", "servicenow"]);
    let connected =
        connected_sources_from_turtle(&[&contract]).expect("the declared contract parses");
    assert!(
        connected.is_empty(),
        "no approved mapping exists on main: {connected:?}"
    );
    assert_unconnected_question("EG-CQ-CONN-R034.1", &connected, &[("lgtm", "Alert")]);
    assert_unconnected_question(
        "EG-CQ-CONN-R034.2",
        &connected,
        &[
            ("lgtm", "Alert"),
            ("uptime", "MonitoredService"),
            ("servicenow", "Incident"),
        ],
    );
}
