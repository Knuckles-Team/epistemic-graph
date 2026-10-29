//! Cross-source question fixtures (MEDULLA-CQ-XSRC-R005.2, R005.3, R008.1.1,
//! R008.2.3, R008.3.3): the class-only SPARQL questions parse and name no
//! connector, and their plan — derived from the seeded `vg:SourceAuthority`
//! rows, not from the query text — names exactly the sources the authority
//! rows declare for the classes they ask about.
#![cfg(feature = "sparql")]

mod common;

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use eg_core::graph::GraphCore;
use eg_rdf::mapping::parse_turtle;
use eg_rdf::oxrdf::{NamedOrBlankNode, Term};
use eg_rdf::source_selection::{select_sources, CoverageFixture, CoverageResult, Premise};
use eg_rdf::sparql::{execute, Dataset, Projection, QueryOutcome};

const KG: &str = "http://knuckles.team/kg#";
const INFRA: &str = "http://knuckles.team/kg/infrastructure#";
const VG: &str = "http://knuckles.team/kg/virtual-graph#";
const SEED: &str = include_str!("../../eg-core/tests/fixtures/cross_source_authority_seed.ttl");

/// Every connector name that may not appear in a class-only question.
const CONNECTOR_NAMES: &[&str] = &[
    "erpnext",
    "leanix",
    "dockerhub",
    "gitlab",
    "github",
    "container-manager",
    "portainer",
    "uptime",
    "lgtm",
    "servicenow",
    "systems-manager",
    "salesforce",
    "twenty",
    "archimate",
    "onetrust",
    "ciso",
];

/// One `vg:SourceAuthority` row parsed from the seed fixture.
struct SeedRow {
    source: String,
    class: String,
    rank: u64,
    allow_fallback: bool,
}

fn seed_rows() -> Vec<SeedRow> {
    let triples = parse_turtle(SEED).unwrap();
    let mut labels = BTreeMap::new();
    for triple in &triples {
        if triple.predicate.as_str() == "http://www.w3.org/2000/01/rdf-schema#label" {
            if let (NamedOrBlankNode::NamedNode(subject), Term::Literal(literal)) =
                (&triple.subject, &triple.object)
            {
                labels.insert(subject.as_str().to_string(), literal.value().to_string());
            }
        }
    }
    let mut rows = Vec::new();
    for triple in &triples {
        let is_authority_type = triple.predicate.as_str()
            == "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
            && matches!(&triple.object, Term::NamedNode(node)
                    if node.as_str() == format!("{VG}SourceAuthority"));
        if !is_authority_type {
            continue;
        }
        let subject = match &triple.subject {
            NamedOrBlankNode::NamedNode(node) => node.as_str().to_string(),
            NamedOrBlankNode::BlankNode(_) => continue,
        };
        let mut row = SeedRow {
            source: String::new(),
            class: String::new(),
            rank: u64::MAX,
            allow_fallback: false,
        };
        for fact in triples.iter().filter(|fact| {
            matches!(&fact.subject,
                NamedOrBlankNode::NamedNode(node)
                if node.as_str() == subject)
        }) {
            match fact.predicate.as_str() {
                pred if pred == format!("{VG}authoritativeSource") => {
                    if let Term::NamedNode(node) = &fact.object {
                        row.source = labels[&node.as_str().to_string()].clone();
                    }
                }
                pred if pred == format!("{VG}authorityFor") => {
                    if let Term::NamedNode(node) = &fact.object {
                        row.class = node.as_str().to_string();
                    }
                }
                pred if pred == format!("{VG}authorityRank") => {
                    if let Term::Literal(literal) = &fact.object {
                        row.rank = literal.value().parse().unwrap();
                    }
                }
                pred if pred == format!("{VG}allowFallback") => {
                    if let Term::Literal(literal) = &fact.object {
                        row.allow_fallback = literal.value() == "true";
                    }
                }
                _ => {}
            }
        }
        rows.push(row);
    }
    rows
}

/// The ontology IRI of a plan helper class shorthand (`infra:Host` style).
fn plan_class(class: &str) -> String {
    if let Some(local) = class.strip_prefix("infra:") {
        format!("{INFRA}{local}")
    } else {
        format!("{KG}{class}")
    }
}

/// The plan fixture for a question: only the authority rows whose class the
/// question asks about (the planner reads the ontology, not the seed file).
fn plan_fixture_for(classes: &[&str]) -> CoverageFixture {
    let wanted: BTreeSet<String> = classes.iter().map(|class| plan_class(class)).collect();
    let mut fixture = CoverageFixture::new();
    for row in seed_rows()
        .into_iter()
        .filter(|row| wanted.contains(&row.class))
    {
        fixture = if row.allow_fallback {
            fixture.with_authority_allow_fallback(&row.class, &row.source, &row.source, 1, row.rank)
        } else {
            fixture.with_authority(&row.class, &row.source, &row.source, 1, row.rank)
        };
    }
    fixture
}

/// The sources a planned class names: the winner plus every alternate, read
/// off the authority premises.
fn planned_sources(result: &CoverageResult) -> BTreeSet<String> {
    let CoverageResult::Covered { premises, .. } = result else {
        return BTreeSet::new();
    };
    premises
        .iter()
        .filter_map(|premise| {
            if let Premise::Authority { source, .. } = premise {
                Some(source.clone())
            } else {
                None
            }
        })
        .collect()
}

/// Prove the fixture parses: it executes against an empty graph and returns an
/// empty solution table, never a parse or compile error.
fn assert_fixture_parses(fixture: &str) {
    let view = GraphCore::new().analysis_snapshot();
    let outcome = execute(
        &Dataset::new(&view, Vec::new()),
        fixture,
        &Projection::raw(),
        None,
    )
    .expect("the class-only fixture parses and executes");
    assert!(matches!(outcome, QueryOutcome::Solutions(_)));
}

fn assert_no_connector_name(fixture: &str, label: &str) {
    for name in CONNECTOR_NAMES {
        assert!(
            !fixture.contains(name),
            "{label}: the class-only question must not name the connector '{name}'"
        );
    }
}

/// Plan `classes` and return the planned source set for each (plus the union).
fn plan_and_collect(
    fixture: &CoverageFixture,
    classes: &[&str],
) -> (BTreeMap<String, BTreeSet<String>>, BTreeSet<String>) {
    let roots: Vec<String> = classes.iter().map(|class| plan_class(class)).collect();
    let results = select_sources(fixture, &roots, 1);
    let mut per_class = BTreeMap::new();
    let mut all = BTreeSet::new();
    for class in classes {
        let class_iri = plan_class(class);
        let result = results
            .iter()
            .find(|result| result.class() == class_iri)
            .unwrap_or_else(|| panic!("{class} must appear in the plan universe"));
        let sources = planned_sources(result);
        all.extend(sources.iter().cloned());
        per_class.insert(class_iri, sources);
    }
    (per_class, all)
}

// spec: MEDULLA-CQ-XSRC-R005.2
#[test]
fn the_supply_chain_question_names_classes_only_and_parses() {
    let fixture = include_str!("fixtures/xsrc_supply_chain_impact.sparql");
    for class in ["Service", "SoftwareComponent", "Supplier", "Vulnerability"] {
        assert!(
            fixture.contains(class),
            "the question asks for the {class} class"
        );
    }
    assert_no_connector_name(fixture, "R005.2");
    assert_fixture_parses(fixture);
}

// spec: MEDULLA-CQ-XSRC-R005.3
#[test]
fn the_planned_sources_equal_the_authority_derived_set() {
    let classes = [
        "infra:Service",
        "SoftwareComponent",
        "Supplier",
        "Vulnerability",
    ];
    let fixture = plan_fixture_for(&classes);
    let roots: Vec<String> = classes.iter().map(|class| plan_class(class)).collect();
    let results = select_sources(&fixture, &roots, 1);
    let seed = seed_rows();
    for class in classes {
        let class_iri = plan_class(class);
        let result = results
            .iter()
            .find(|result| result.class() == class_iri)
            .unwrap();
        let CoverageResult::Covered {
            mapping_name,
            premises,
            ..
        } = result
        else {
            panic!("{class} must be covered by its authority rows");
        };
        let planned = premises
            .iter()
            .filter_map(|premise| {
                if let Premise::Authority { source, .. } = premise {
                    Some(source.clone())
                } else {
                    None
                }
            })
            .collect::<BTreeSet<_>>();
        let derived = seed
            .iter()
            .filter(|row| row.class == class_iri)
            .map(|row| row.source.clone())
            .collect::<BTreeSet<_>>();
        assert_eq!(
            planned, derived,
            "the plan for {class} must equal the sources its authority rows declare"
        );
        let lowest = seed
            .iter()
            .filter(|row| row.class == class_iri)
            .min_by_key(|row| row.rank)
            .unwrap();
        assert_eq!(
            mapping_name, &lowest.source,
            "the lowest-ranked row for {class} wins the plan"
        );
    }
}

// spec: MEDULLA-CQ-XSRC-R008.1.1
#[test]
fn the_vulnerable_image_question_plans_to_exactly_the_named_sources() {
    let fixture = include_str!("fixtures/xsrc_vulnerable_image.sparql");
    for class in ["ContainerImage", "Workload", "Service", "Vulnerability"] {
        assert!(
            fixture.contains(class),
            "the question asks for the {class} class"
        );
    }
    assert_no_connector_name(fixture, "R008.1.1");
    assert_fixture_parses(fixture);
    let plan_fixture = plan_fixture_for(&[
        "infra:ContainerImage",
        "infra:Workload",
        "infra:Service",
        "Vulnerability",
    ]);
    let (_, all) = plan_and_collect(
        &plan_fixture,
        &[
            "infra:ContainerImage",
            "infra:Workload",
            "infra:Service",
            "Vulnerability",
        ],
    );
    assert_eq!(
        all,
        [
            "dockerhub-mcp",
            "gitlab-mcp",
            "github-mcp",
            "container-manager-mcp",
            "portainer-mcp",
            "uptime-mcp",
        ]
        .into_iter()
        .map(String::from)
        .collect::<BTreeSet<String>>()
    );
}

// spec: MEDULLA-CQ-XSRC-R008.2.3
#[test]
fn the_capability_host_limit_question_plans_to_exactly_the_named_sources() {
    let fixture = include_str!("fixtures/xsrc_capability_host_limit.sparql");
    for class in ["BusinessCapability", "Workload", "Host"] {
        assert!(
            fixture.contains(class),
            "the question asks for the {class} class"
        );
    }
    assert_no_connector_name(fixture, "R008.2.3");
    assert_fixture_parses(fixture);
    let plan_fixture = plan_fixture_for(&["BusinessCapability", "infra:Workload", "infra:Host"]);
    let (_, all) = plan_and_collect(
        &plan_fixture,
        &["BusinessCapability", "infra:Workload", "infra:Host"],
    );
    assert_eq!(
        all,
        [
            "leanix-mcp",
            "archimate-mcp",
            "container-manager-mcp",
            "portainer-mcp",
            "lgtm-mcp",
            "systems-manager-mcp",
        ]
        .into_iter()
        .map(String::from)
        .collect::<BTreeSet<String>>()
    );
}

// spec: MEDULLA-CQ-XSRC-R008.3.3
#[test]
fn the_incident_contract_question_plans_to_exactly_the_named_sources() {
    let fixture = include_str!("fixtures/xsrc_incident_contract.sparql");
    for class in ["Incident", "Change", "Service", "CustomerContract"] {
        assert!(
            fixture.contains(class),
            "the question asks for the {class} class"
        );
    }
    assert_no_connector_name(fixture, "R008.3.3");
    assert_fixture_parses(fixture);
    let plan_fixture =
        plan_fixture_for(&["Incident", "Change", "infra:Service", "CustomerContract"]);
    let (_, all) = plan_and_collect(
        &plan_fixture,
        &["Incident", "Change", "infra:Service", "CustomerContract"],
    );
    assert_eq!(
        all,
        [
            "servicenow-mcp",
            "erpnext-mcp",
            "uptime-mcp",
            "salesforce-mcp",
            "twenty-mcp"
        ]
        .into_iter()
        .map(String::from)
        .collect::<BTreeSet<String>>()
    );
}
