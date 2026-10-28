//! Native parser, reasoner and shape contracts for the shared supply-chain
//! vocabulary (EG-SUPPLY-VOCAB).

use eg_rdf::mapping::parse_turtle;
use eg_rdf::oxrdf::{NamedNode, NamedOrBlankNode, Term, Triple};

const KG: &str = "http://knuckles.team/kg#";
const INFRA: &str = "http://knuckles.team/kg/infrastructure#";
const EX: &str = "http://knuckles.team/test/supply-chain#";
const RDF: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
const RDFS: &str = "http://www.w3.org/2000/01/rdf-schema#";
const OWL: &str = "http://www.w3.org/2002/07/owl#";
const XSD: &str = "http://www.w3.org/2001/XMLSchema#";

fn has_iri(triples: &[Triple], subject: &str, predicate: &str, object: &str) -> bool {
    triples.contains(&Triple::new(
        NamedNode::new(subject).unwrap(),
        NamedNode::new(predicate).unwrap(),
        NamedNode::new(object).unwrap(),
    ))
}

/// Canonical node id for a subject term (`<iri>` or `_:bnode`).
fn subject_key(triple: &Triple) -> String {
    match &triple.subject {
        NamedOrBlankNode::NamedNode(n) => format!("<{}>", n.as_str()),
        NamedOrBlankNode::BlankNode(b) => format!("_:{}", b.as_str()),
        #[allow(unreachable_patterns)]
        _ => String::new(),
    }
}

/// Canonical node id for an object term; `None` for a literal.
fn object_key(triple: &Triple) -> Option<String> {
    match &triple.object {
        Term::NamedNode(n) => Some(format!("<{}>", n.as_str())),
        Term::BlankNode(b) => Some(format!("_:{}", b.as_str())),
        _ => None,
    }
}

/// The lexical value of a literal object term; panics on a non-literal.
fn literal_value(term: &Term) -> &str {
    match term {
        Term::Literal(l) => l.value(),
        other => panic!("expected a literal, found {other:?}"),
    }
}

/// Every predicate-field pair a virtual mapping declares, as
/// `(predicate local name, discovered field name)`.
fn pair_fields(triples: &[Triple], mapping_key: &str) -> Vec<(String, String)> {
    triples
        .iter()
        .filter(|triple| {
            subject_key(triple) == mapping_key
                && triple.predicate.as_str().ends_with("hasPredicateFieldPair")
        })
        .filter_map(object_key)
        .filter_map(|pair| {
            let predicate = triples
                .iter()
                .find(|triple| {
                    subject_key(triple) == pair
                        && triple.predicate.as_str().ends_with("#predicate")
                })
                .and_then(object_key)?;
            let field = triples
                .iter()
                .find(|triple| {
                    subject_key(triple) == pair
                        && triple.predicate.as_str().ends_with("#field")
                })
                .map(|triple| literal_value(&triple.object).to_string())?;
            Some((
                predicate.trim_matches(['<', '>']).rsplit('#').next().unwrap().to_string(),
                field,
            ))
        })
        .collect()
}

fn load_supply_chain() -> Vec<Triple> {
    parse_turtle(include_str!("../ontology/supply_chain-v1.ttl"))
        .expect("the supply-chain vocabulary parses")
}

fn load_commercial() -> Vec<Triple> {
    parse_turtle(include_str!("../ontology/commercial-v1.ttl"))
        .expect("the commercial vocabulary parses")
}

fn prefixed(local: &str, prefix: &str) -> String {
    if let Some(tail) = local.strip_prefix("xsd:") {
        format!("{XSD}{tail}")
    } else if let Some(tail) = local.strip_prefix("infra:") {
        format!("{INFRA}{tail}")
    } else {
        format!("{prefix}{local}")
    }
}

fn assert_domain_range(
    triples: &[Triple],
    local: &str,
    domain: &str,
    range: &str,
) {
    let subject = format!("{KG}{local}");
    assert!(
        has_iri(
            triples,
            &subject,
            &format!("{RDFS}domain"),
            &prefixed(domain, KG)
        ),
        "{local} domain is {domain}"
    );
    assert!(
        has_iri(triples, &subject, &format!("{RDFS}range"), &prefixed(range, KG)),
        "{local} range is {range}"
    );
}

const SHACL: &str = "http://www.w3.org/ns/shacl#";

const SUPPLY_SHAPES: &str = include_str!("../ontology/supply_chain-v1.shapes.ttl");

const SUPPLY_SHAPE_DATA_PREFIX: &str = r#"
@prefix kg:   <http://knuckles.team/kg#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix ex:   <http://knuckles.team/test/supply-chain#> .
"#;

fn shape_violation_path(report: &eg_shacl::ValidationReport, predicate: &str) -> bool {
    report.results.iter().any(|result| {
        result
            .path
            .as_deref()
            .is_some_and(|path| path.ends_with(predicate))
    })
}

// The TBox the physical chain closes across: the shared foundation, the TRM
// vendor/asset classes, the infrastructure runtime and the supply-chain module
// itself (the EG-CQ-XSRC-R002 document set).
fn physical_documents() -> Vec<Triple> {
    [
        include_str!("../ontology/core-foundation-v1.ttl"),
        include_str!("../ontology/trm-v1.ttl"),
        include_str!("../ontology/infrastructure-v1.ttl"),
        include_str!("../ontology/supply_chain-v1.ttl"),
    ]
    .iter()
    .flat_map(|document| parse_turtle(document).unwrap())
    .collect()
}

// spec: EG-SUPPLY-VOCAB-R001.3
#[test]
fn physical_chain_terms_declared() {
    let triples = load_supply_chain();
    assert!(has_iri(
        &triples,
        &format!("{INFRA}serviceRunsOn"),
        &format!("{RDF}type"),
        &format!("{OWL}ObjectProperty")
    ));
    assert!(has_iri(
        &triples,
        &format!("{INFRA}serviceRunsOn"),
        &format!("{RDFS}subPropertyOf"),
        &format!("{INFRA}runsOn")
    ));
    assert!(has_iri(
        &triples,
        &format!("{INFRA}serviceRunsOn"),
        &format!("{RDFS}domain"),
        &format!("{INFRA}Service")
    ));
    assert!(has_iri(
        &triples,
        &format!("{INFRA}serviceRunsOn"),
        &format!("{RDFS}range"),
        &format!("{INFRA}Host")
    ));
    assert!(has_iri(
        &triples,
        &format!("{KG}suppliesService"),
        &format!("{RDF}type"),
        &format!("{OWL}ObjectProperty")
    ));
    assert!(has_iri(
        &triples,
        &format!("{KG}suppliesService"),
        &format!("{RDFS}domain"),
        &format!("{KG}Supplier")
    ));
    assert!(has_iri(
        &triples,
        &format!("{KG}suppliesService"),
        &format!("{RDFS}range"),
        &format!("{INFRA}Service")
    ));
    // The property-chain axiom names the four physical links in order, the
    // last one the inverse of serviceRunsOn.
    let list_iri = triples
        .iter()
        .find(|triple| {
            subject_key(triple) == format!("<{KG}suppliesService>")
                && triple
                    .predicate
                    .as_str()
                    .ends_with("propertyChainAxiom")
        })
        .and_then(object_key);
    let Some(head) = list_iri else {
        panic!("suppliesService declares a propertyChainAxiom list");
    };
    let mut members: Vec<String> = Vec::new();
    let mut cursor = Some(head);
    while let Some(node) = cursor {
        let member = triples
            .iter()
            .find(|triple| subject_key(triple) == node && triple.predicate.as_str().ends_with("first"))
            .and_then(object_key)
            .expect("every chain-list head carries a first");
        members.push(member);
        // A list cell carries a rest; the final member does not.
        cursor = triples
            .iter()
            .find(|triple| subject_key(triple) == node && triple.predicate.as_str().ends_with("rest"))
            .and_then(object_key)
            .filter(|next| !next.ends_with("#nil>"));
    }
    assert!(
        members.iter().any(|m| m.ends_with("#suppliesItem>")),
        "chain starts with suppliesItem: {members:?}"
    );
    assert!(
        members.iter().any(|m| m.ends_with("#realizedByAsset>")),
        "chain carries realizedByAsset: {members:?}"
    );
    assert!(
        members.iter().any(|m| m.ends_with("#assetHost>")),
        "chain carries assetHost: {members:?}"
    );
    let inverse_member = members
        .iter()
        .find(|m| m.starts_with("_:"))
        .expect("the last chain member is the blank-node inverse link");
    let inverse_of = triples
        .iter()
        .find(|triple| subject_key(triple) == *inverse_member && triple.predicate.as_str().ends_with("inverseOf"))
        .expect("the blank-node chain member declares its inverseOf");
    match &inverse_of.object {
        Term::NamedNode(n) => assert_eq!(n.as_str(), format!("{INFRA}serviceRunsOn")),
        other => panic!("inverse target is not an IRI: {other:?}"),
    }
}

// spec: EG-SUPPLY-VOCAB-R001.3
#[test]
fn physical_chain_infers_supplies_service() {
    let mut triples = physical_documents();
    triples.extend(
        parse_turtle(include_str!(
            "fixtures/supply_chain_service_chain.ttl"
        ))
        .expect("the physical-chain fixture parses"),
    );
    let supplier = format!("<{EX}acmeSupplier>");
    let service = format!("<{EX}payrollService>");
    let chain = format!("<{KG}suppliesService>");
    let entailed = eg_rdf::owl::saturate_object_properties(&triples);
    let Some(derived) = entailed
        .iter()
        .find(|fact| fact.subject == supplier && fact.predicate == chain && fact.object == service)
    else {
        panic!("the four-edge service path must derive {supplier} {chain} {service}");
    };
    assert!(!derived.asserted, "the edge is inferred, not asserted");
    assert_eq!(derived.rule, "RL-propertyChain");
}

// spec: EG-SUPPLY-VOCAB-R001.3
#[test]
fn container_only_runs_on_neither_infers_nor_retires() {
    let mut triples = physical_documents();
    triples.extend(
        parse_turtle(
            r#"
            @prefix : <http://knuckles.team/kg#> .
            @prefix infra: <http://knuckles.team/kg/infrastructure#> .
            @prefix ex: <http://knuckles.team/test/supply-chain#> .

            ex:sidecarContainer a infra:Container .
            ex:sidecarContainer infra:runsOn ex:rackHost .
            ex:sidecarPod a infra:Pod .
            ex:sidecarPod infra:podRunsOn ex:rackHost .
            ex:rackHost a infra:Host .
            "#,
        )
        .expect("the container-only fixture parses"),
    );
    let entailed = eg_rdf::owl::saturate_object_properties(&triples);
    let chain = format!("<{KG}suppliesService>");
    assert!(
        !entailed.iter().any(|fact| fact.predicate == chain),
        "a container-only or pod-only runsOn path must not infer suppliesService"
    );
    // Neither runtime entity is retyped as a Service by the chain's domains.
    let mut asserted: std::collections::HashMap<String, std::collections::HashSet<String>> =
        std::collections::HashMap::new();
    asserted
        .entry(format!("<{EX}sidecarContainer>"))
        .or_default()
        .insert(format!("<{INFRA}Container>"));
    asserted
        .entry(format!("<{EX}sidecarPod>"))
        .or_default()
        .insert(format!("<{INFRA}Pod>"));
    let classification = eg_rdf::owl::Reasoner::from_triples(&triples).classify();
    let service = format!("<{INFRA}Service>");
    assert!(
        eg_rdf::owl::instances_of(&classification, &asserted, &service).is_empty(),
        "a container-only or pod-only runsOn path must not retype its endpoint as a Service"
    );
}

// spec: EG-SUPPLY-VOCAB-R002.7
#[test]
fn component_version_vulnerability_and_uses_component_shapes() {
    let shape_triples =
        parse_turtle(SUPPLY_SHAPES).expect("the supply-chain shapes parse");
    for shape in [
        "ComponentVersionShape",
        "VulnerabilityShape",
        "UsesComponentShape",
        "UsesComponentObjectShape",
    ] {
        assert!(
            has_iri(
                &shape_triples,
                &format!("{KG}{shape}"),
                &format!("{RDF}type"),
                &format!("{SHACL}NodeShape")
            ),
            "{shape} is a node shape"
        );
    }
    // A well-formed version, vulnerability and service link are accepted.
    let valid = format!(
        "{SUPPLY_SHAPE_DATA_PREFIX}
@prefix infra: <http://knuckles.team/kg/infrastructure#> .

ex:openssl111k a kg:ComponentVersion ;
    kg:supplyChainVersionOf ex:openssl ;
    kg:versionString \"1.1.1k\" .

ex:openssl a kg:SoftwareComponent .

ex:cve20241234 a kg:Vulnerability ;
    kg:cveId \"CVE-2024-1234\" ;
    kg:severity \"high\" ;
    kg:affectsComponent ex:openssl111k .

ex:checkoutService a infra:Service ;
    kg:usesComponent ex:openssl111k ."
    );
    let report =
        eg_shacl::validate_turtle(SUPPLY_SHAPES, &valid).expect("valid fixture parses");
    assert!(
        report.conforms,
        "a well-formed version, vulnerability and service link: {:?}",
        report.results
    );
    // A versionless component version is refused on the version link.
    let versionless = format!(
        "{SUPPLY_SHAPE_DATA_PREFIX}
ex:orphanVersion a kg:ComponentVersion ;
    kg:versionString \"0.0.1\" ."
    );
    let report =
        eg_shacl::validate_turtle(SUPPLY_SHAPES, &versionless).expect("fixture parses");
    assert!(
        !report.conforms,
        "a versionless component version must be refused: {:?}",
        report.results
    );
    // A severity outside the closed vocabulary is refused.
    let bad_severity = format!(
        "{SUPPLY_SHAPE_DATA_PREFIX}
ex:apocalyptic a kg:Vulnerability ;
    kg:cveId \"CVE-2024-9999\" ;
    kg:severity \"apocalyptic\" ;
    kg:affectsComponent ex:openssl111k .

ex:openssl111k a kg:ComponentVersion ."
    );
    let report =
        eg_shacl::validate_turtle(SUPPLY_SHAPES, &bad_severity).expect("fixture parses");
    assert!(
        !report.conforms,
        "a severity outside {{low, medium, high, critical, unknown}} must be refused: {:?}",
        report.results
    );
    assert!(
        report
            .results
            .iter()
            .any(|result| result.constraint_component.ends_with("InConstraintComponent")),
        "the refusal lands on the closed severity vocabulary: {:?}",
        report.results
    );
    // A usesComponent from a non-service subject is refused.
    let non_service = format!(
        "{SUPPLY_SHAPE_DATA_PREFIX}
@prefix infra: <http://knuckles.team/kg/infrastructure#> .

ex:checkoutWorkload a infra:Workload ;
    kg:usesComponent ex:openssl111k .

ex:openssl111k a kg:ComponentVersion ;
    kg:supplyChainVersionOf ex:openssl ;
    kg:versionString \"1.1.1k\" .

ex:openssl a kg:SoftwareComponent ."
    );
    let report =
        eg_shacl::validate_turtle(SUPPLY_SHAPES, &non_service).expect("fixture parses");
    assert!(
        !report.conforms,
        "a usesComponent from a non-service must be refused: {:?}",
        report.results
    );
    assert!(
        report.results.iter().any(|result| {
            result
                .focus_node
                .ends_with("#checkoutWorkload>")
                && result.constraint_component.ends_with("ClassConstraintComponent")
        }),
        "the non-service subject is refused on its class: {:?}",
        report.results
    );
}

// spec: EG-SUPPLY-VOCAB-R001.4
#[test]
fn supplier_and_item_shapes_require_a_label() {
    // The companion parses and declares both shapes as node shapes.
    let shape_triples =
        parse_turtle(SUPPLY_SHAPES).expect("the supply-chain shapes parse");
    for shape in ["SupplierShape", "SupplyChainItemShape"] {
        assert!(
            has_iri(
                &shape_triples,
                &format!("{KG}{shape}"),
                &format!("{RDF}type"),
                &format!("{SHACL}NodeShape")
            ),
            "{shape} is a node shape"
        );
    }
    // A labelled supplier and item are accepted.
    let valid = format!(
        "{SUPPLY_SHAPE_DATA_PREFIX}\nex:namedSupplier a kg:Supplier ;\n    rdfs:label \"Acme Corp\" .\nex:namedItem a kg:SupplyChainItem ;\n    rdfs:label \"Power supply unit\" ."
    );
    let report =
        eg_shacl::validate_turtle(SUPPLY_SHAPES, &valid).expect("valid fixture parses");
    assert!(report.conforms, "named supplier and item: {:?}", report.results);
    // A nameless supplier and a nameless item are each refused on the label.
    for (class, message) in [
        ("kg:Supplier", "a nameless supplier"),
        ("kg:SupplyChainItem", "a nameless item"),
    ] {
        let nameless = format!(
            "{SUPPLY_SHAPE_DATA_PREFIX}\nex:nameless a {class} ."
        );
        let report =
            eg_shacl::validate_turtle(SUPPLY_SHAPES, &nameless).expect("fixture parses");
        assert!(!report.conforms, "{message} must be refused");
        assert!(
            shape_violation_path(&report, "<http://www.w3.org/2000/01/rdf-schema#label>"),
            "{message} is refused on the missing label: {:?}",
            report.results
        );
    }
}

// spec: EG-SUPPLY-VOCAB-R003.2
#[test]
fn contract_terms_declared_with_domain_range() {
    let triples = load_commercial();
    assert!(
        has_iri(
            &triples,
            &format!("{KG}CustomerContract"),
            &format!("{RDF}type"),
            &format!("{OWL}Class")
        ),
        "CustomerContract is a class"
    );
    // The five contract terms carry their declared domain and range.
    assert!(
        has_iri(
            &triples,
            &format!("{KG}contractCustomer"),
            &format!("{RDF}type"),
            &format!("{OWL}ObjectProperty")
        ),
        "contractCustomer is an object property"
    );
    assert_domain_range(&triples, "contractCustomer", "CustomerContract", "Customer");
    assert!(
        has_iri(
            &triples,
            &format!("{KG}coversService"),
            &format!("{RDF}type"),
            &format!("{OWL}ObjectProperty")
        ),
        "coversService is an object property"
    );
    assert_domain_range(&triples, "coversService", "CustomerContract", "infra:Service");
    for property in ["contractStatus", "contractEnd"] {
        assert!(
            has_iri(
                &triples,
                &format!("{KG}{property}"),
                &format!("{RDF}type"),
                &format!("{OWL}DatatypeProperty")
            ),
            "{property} is a datatype property"
        );
    }
    assert_domain_range(&triples, "contractStatus", "CustomerContract", "xsd:string");
    assert_domain_range(&triples, "contractEnd", "CustomerContract", "xsd:date");
}

// spec: EG-SUPPLY-VOCAB-R003.3
#[test]
fn affects_service_declared_over_incident_or_change() {
    let triples = load_commercial();
    assert!(
        has_iri(
            &triples,
            &format!("{KG}affectsService"),
            &format!("{RDF}type"),
            &format!("{OWL}ObjectProperty")
        ),
        "affectsService is an object property"
    );
    assert!(
        has_iri(
            &triples,
            &format!("{KG}affectsService"),
            &format!("{RDFS}range"),
            &format!("{INFRA}Service")
        ),
        "affectsService ranges to infra:Service"
    );
    // The declared domain is the union of the shared :Incident and :Change.
    let domain = triples
        .iter()
        .find(|triple| {
            subject_key(triple) == format!("<{KG}affectsService>")
                && triple.predicate.as_str().ends_with("domain")
        })
        .and_then(object_key)
        .expect("affectsService declares its domain");
    let union_list = triples
        .iter()
        .find(|triple| {
            subject_key(triple) == domain && triple.predicate.as_str().ends_with("unionOf")
        })
        .and_then(object_key)
        .expect("the domain is an owl:unionOf class");
    let mut members: Vec<String> = Vec::new();
    let mut cursor = Some(union_list);
    while let Some(node) = cursor {
        let member = triples
            .iter()
            .find(|triple| {
                subject_key(triple) == node && triple.predicate.as_str().ends_with("first")
            })
            .and_then(object_key)
            .expect("every union-list head carries a first");
        members.push(member);
        // A list cell carries a rest; the final member does not.
        cursor = triples
            .iter()
            .find(|triple| {
                subject_key(triple) == node && triple.predicate.as_str().ends_with("rest")
            })
            .and_then(object_key)
            .filter(|next| !next.ends_with("#nil>"));
    }
    assert!(
        members.iter().any(|m| m == &format!("<{KG}Incident>")),
        "the domain union names :Incident: {members:?}"
    );
    assert!(
        members.iter().any(|m| m == &format!("<{KG}Change>")),
        "the domain union names the shared :Change: {members:?}"
    );
    // No commercial :ChangeRequest is redeclared (Decision D3).
    let change_request = format!("{KG}ChangeRequest");
    assert!(
        !triples
            .iter()
            .any(|triple| subject_key(triple) == change_request),
        "commercial-v1.ttl must not redeclare :ChangeRequest"
    );
}

// spec: EG-SUPPLY-VOCAB-R003.4
#[test]
fn supports_capability_declared_and_not_the_shared_supports() {
    let triples = load_commercial();
    assert!(
        has_iri(
            &triples,
            &format!("{KG}supportsCapability"),
            &format!("{RDF}type"),
            &format!("{OWL}ObjectProperty")
        ),
        "supportsCapability is an object property"
    );
    assert_domain_range(&triples, "supportsCapability", "infra:Service", "BusinessCapability");
    // The commercial module never redeclares or narrows the shared :supports.
    assert!(
        !triples
            .iter()
            .any(|triple| subject_key(triple) == format!("{KG}supports")),
        "the shared :supports is not redeclared in the commercial module"
    );
    // And a fixture using kg:supports is not accepted by the capability shape:
    // the shape document paths only supportsCapability, never the shared
    // supports, so a supports-only link is not a capability link to it.
    let shape_triples =
        parse_turtle(include_str!("../ontology/commercial-v1.shapes.ttl"))
            .expect("the commercial shapes parse");
    assert!(
        !shape_triples
            .iter()
            .any(|triple| {
                triple.predicate.as_str().ends_with("path")
                    && matches!(
                        &triple.object,
                        Term::NamedNode(n) if n.as_str() == format!("{KG}supports")
                    )
            }),
        "the capability shape never paths the shared :supports"
    );
}

fn commercial_shapes() -> std::sync::Arc<str> {
    let sources = eg_core::graph::GraphSchemaSources::default();
    sources
        .validate()
        .expect("the default core catalog validates");
    sources
        .core
        .get("core:commercial@1")
        .and_then(|source| source.shapes_ttl.clone())
        .expect("the commercial module carries its shapes companion")
}

const COMMERCE_DATA_PREFIX: &str = r#"
@prefix kg:    <http://knuckles.team/kg#> .
@prefix infra: <http://knuckles.team/kg/infrastructure#> .
@prefix xsd:   <http://www.w3.org/2001/XMLSchema#> .
@prefix ex:    <http://knuckles.team/test/commercial#> .
"#;

// spec: EG-SUPPLY-VOCAB-R003.5
#[test]
fn contract_and_capability_shape_fixtures() {
    let shapes = commercial_shapes();
    // A complete contract plus a service capability link are accepted.
    let valid = format!(
        "{COMMERCE_DATA_PREFIX}
ex:acmeContract a kg:CustomerContract ;
    kg:contractCustomer ex:acmeCustomer ;
    kg:coversService ex:checkoutService .

ex:acmeCustomer a kg:Customer .

ex:checkoutService a infra:Service ;
    kg:supportsCapability ex:orderManagement .

ex:orderManagement a kg:BusinessCapability ."
    );
    let report =
        eg_shacl::validate_turtle(&shapes, &valid).expect("valid fixture parses");
    assert!(
        report.conforms,
        "a complete contract and a service capability link: {:?}",
        report.results
    );
    // A contract with no customer is refused on contractCustomer.
    let no_customer = format!(
        "{COMMERCE_DATA_PREFIX}
ex:ghostContract a kg:CustomerContract ;
    kg:coversService ex:checkoutService .

ex:checkoutService a infra:Service ."
    );
    let report =
        eg_shacl::validate_turtle(&shapes, &no_customer).expect("fixture parses");
    assert!(!report.conforms, "a contract with no customer must be refused");
    assert!(
        report.results.iter().any(|result| {
            result
                .path
                .as_deref()
                .is_some_and(|path| path.ends_with("#contractCustomer>"))
                && result.constraint_component.ends_with("MinCountConstraintComponent")
        }),
        "the refusal lands on the missing customer: {:?}",
        report.results
    );
    // A contract with no covered service is refused on coversService.
    let no_service = format!(
        "{COMMERCE_DATA_PREFIX}
ex:ghostContract a kg:CustomerContract ;
    kg:contractCustomer ex:acmeCustomer .

ex:acmeCustomer a kg:Customer ."
    );
    let report =
        eg_shacl::validate_turtle(&shapes, &no_service).expect("fixture parses");
    assert!(!report.conforms, "a contract with no covered service must be refused");
    assert!(
        report.results.iter().any(|result| {
            result
                .path
                .as_deref()
                .is_some_and(|path| path.ends_with("#coversService>"))
                && result.constraint_component.ends_with("MinCountConstraintComponent")
        }),
        "the refusal lands on the missing covered service: {:?}",
        report.results
    );
    // A capability link from a non-service subject is refused.
    let non_service_link = format!(
        "{COMMERCE_DATA_PREFIX}
ex:processDoc a kg:Document ;
    kg:supportsCapability ex:orderManagement .

ex:orderManagement a kg:BusinessCapability ."
    );
    let report =
        eg_shacl::validate_turtle(&shapes, &non_service_link).expect("fixture parses");
    assert!(
        !report.conforms,
        "a capability link from a non-service must be refused: {:?}",
        report.results
    );
    assert!(
        report.results.iter().any(|result| {
            result.focus_node.ends_with("#processDoc>")
                && result.constraint_component.ends_with("ClassConstraintComponent")
        }),
        "the non-service link subject is refused on its class: {:?}",
        report.results
    );
}

// spec: EG-SUPPLY-VOCAB-R003.7
#[test]
fn supplier_and_customer_entail_organization_without_a_party_class() {
    let mut triples = physical_documents();
    triples.extend(load_commercial());
    let classification = eg_rdf::owl::Reasoner::from_triples(&triples).classify();
    assert!(
        classification.consistent,
        "the commercial vocabulary classifies consistently: {:?}",
        classification.unsatisfiable
    );
    let organization = format!("<{KG}Organization>");
    assert!(
        classification.entails_subclass(&format!("<{KG}Supplier>"), &organization),
        "Supplier entails Organization through the shared Vendor"
    );
    assert!(
        classification.entails_subclass(&format!("<{KG}Customer>"), &organization),
        "Customer entails Organization"
    );
    // No :Party class is declared anywhere in the core ontology set.
    let party = format!("{KG}Party");
    for entry in std::fs::read_dir("ontology").expect("the ontology directory is present") {
        let path = entry.unwrap().path();
        let document = std::fs::read_to_string(&path)
            .unwrap_or_else(|err| panic!("{} reads: {err}", path.display()));
        let parsed = parse_turtle(&document)
            .unwrap_or_else(|err| panic!("{} parses: {err}", path.display()));
        assert!(
            !parsed
                .iter()
                .any(|triple| {
                    subject_key(triple) == format!("<{party}>")
                        && triple.predicate.as_str().ends_with("type")
                }),
            "{:?} declares a :Party class",
            path.display()
        );
    }
}

// spec: EG-SUPPLY-VOCAB-R003.8
#[test]
fn purchase_order_declared_exactly_once() {
    let triples = load_commercial();
    assert!(
        has_iri(
            &triples,
            &format!("{KG}PurchaseOrder"),
            &format!("{RDF}type"),
            &format!("{OWL}Class")
        ),
        "PurchaseOrder is a class"
    );
    // The core ontology set declares exactly one :PurchaseOrder.
    let mut declarations = 0;
    for entry in std::fs::read_dir("ontology").expect("the ontology directory is present") {
        let path = entry.unwrap().path();
        let document = std::fs::read_to_string(&path)
            .unwrap_or_else(|err| panic!("{} reads: {err}", path.display()));
        let parsed = parse_turtle(&document)
            .unwrap_or_else(|err| panic!("{} parses: {err}", path.display()));
        declarations += parsed
            .iter()
            .filter(|triple| {
                subject_key(triple) == format!("<{KG}PurchaseOrder>")
                    && triple.predicate.as_str().ends_with("type")
            })
            .count();
    }
    assert_eq!(
        declarations, 1,
        "exactly one :PurchaseOrder declaration in crates/eg-core/ontology"
    );
}

// spec: EG-SUPPLY-VOCAB-R003.9
#[test]
fn purchase_invoice_and_invoice_supplier_declared() {
    let triples = load_commercial();
    assert!(
        has_iri(
            &triples,
            &format!("{KG}PurchaseInvoice"),
            &format!("{RDF}type"),
            &format!("{OWL}Class")
        ),
        "PurchaseInvoice is a class"
    );
    assert!(
        has_iri(
            &triples,
            &format!("{KG}invoiceSupplier"),
            &format!("{RDF}type"),
            &format!("{OWL}ObjectProperty")
        ),
        "invoiceSupplier is an object property"
    );
    assert_domain_range(&triples, "invoiceSupplier", "PurchaseInvoice", "Supplier");
}

// spec: EG-SUPPLY-VOCAB-R003.11
#[test]
fn contact_of_declared_with_no_contact_class() {
    let triples = load_commercial();
    assert!(
        has_iri(
            &triples,
            &format!("{KG}contactOf"),
            &format!("{RDF}type"),
            &format!("{OWL}ObjectProperty")
        ),
        "contactOf is an object property"
    );
    assert_domain_range(&triples, "contactOf", "Person", "Customer");
    // No :Contact class exists in the core ontology set.
    let mut contact_classes = 0;
    for entry in std::fs::read_dir("ontology").expect("the ontology directory is present") {
        let path = entry.unwrap().path();
        let document = std::fs::read_to_string(&path)
            .unwrap_or_else(|err| panic!("{} reads: {err}", path.display()));
        let parsed = parse_turtle(&document)
            .unwrap_or_else(|err| panic!("{} parses: {err}", path.display()));
        contact_classes += parsed
            .iter()
            .filter(|triple| {
                subject_key(triple) == format!("<{KG}Contact>")
                    && triple.predicate.as_str().ends_with("type")
            })
            .count();
    }
    assert_eq!(
        contact_classes, 0,
        "no :Contact class is declared; :Person is reused for contacts"
    );
}

// spec: EG-SUPPLY-VOCAB-R003.12
#[test]
fn last_activity_at_declared_and_datatyped() {
    let triples = load_commercial();
    assert!(
        has_iri(
            &triples,
            &format!("{KG}lastActivityAt"),
            &format!("{RDF}type"),
            &format!("{OWL}DatatypeProperty")
        ),
        "lastActivityAt is a datatype property"
    );
    assert_domain_range(&triples, "lastActivityAt", "Customer", "xsd:dateTime");
    let shapes = commercial_shapes();
    // A dateTime last activity is accepted.
    let valid = format!(
        "{COMMERCE_DATA_PREFIX}
ex:acmeCustomer a kg:Customer ;
    kg:lastActivityAt \"2026-10-01T09:30:00\"^^xsd:dateTime ."
    );
    let report =
        eg_shacl::validate_turtle(&shapes, &valid).expect("valid fixture parses");
    assert!(
        report.conforms,
        "a dateTime last activity is accepted: {:?}",
        report.results
    );
    // A plain string is refused on the datatype.
    let plain_string = format!(
        "{COMMERCE_DATA_PREFIX}
ex:acmeCustomer a kg:Customer ;
    kg:lastActivityAt \"yesterday\" ."
    );
    let report =
        eg_shacl::validate_turtle(&shapes, &plain_string).expect("fixture parses");
    assert!(
        !report.conforms,
        "a non-dateTime last activity must be refused: {:?}",
        report.results
    );
    assert!(
        report.results.iter().any(|result| {
            result
                .path
                .as_deref()
                .is_some_and(|path| path.ends_with("#lastActivityAt>"))
                && result.constraint_component.ends_with("DatatypeConstraintComponent")
        }),
        "the refusal lands on the lastActivityAt datatype: {:?}",
        report.results
    );
}

// spec: EG-SUPPLY-VOCAB-R003.17
#[test]
fn opportunity_date_shape_requires_a_close_date() {
    let shapes = commercial_shapes();
    // The registered shape declares the closeDate constraint on Opportunity.
    let shape_triples =
        parse_turtle(&shapes).expect("the commercial shapes parse");
    assert!(
        has_iri(
            &shape_triples,
            &format!("{KG}OpportunityShape"),
            &format!("{RDF}type"),
            &format!("{SHACL}NodeShape")
        ),
        "OpportunityShape is a registered node shape"
    );
    // A valid dated opportunity is accepted.
    let valid = format!(
        "{COMMERCE_DATA_PREFIX}
ex:acmeOpportunity a kg:Opportunity ;
    kg:closeDate \"2026-12-31T00:00:00\"^^xsd:dateTime ."
    );
    let report =
        eg_shacl::validate_turtle(&shapes, &valid).expect("valid fixture parses");
    assert!(
        report.conforms,
        "a dated opportunity is accepted: {:?}",
        report.results
    );
    // A missing closeDate is refused.
    let missing = format!(
        "{COMMERCE_DATA_PREFIX}
ex:acmeOpportunity a kg:Opportunity ."
    );
    let report =
        eg_shacl::validate_turtle(&shapes, &missing).expect("fixture parses");
    assert!(
        !report.conforms,
        "a missing closeDate must be refused: {:?}",
        report.results
    );
    assert!(
        report.results.iter().any(|result| {
            result
                .path
                .as_deref()
                .is_some_and(|path| path.ends_with("#closeDate>"))
                && result.constraint_component.ends_with("MinCountConstraintComponent")
        }),
        "the refusal lands on the missing closeDate: {:?}",
        report.results
    );
    // An invalid date datatype is refused.
    let bad_date = format!(
        "{COMMERCE_DATA_PREFIX}
ex:acmeOpportunity a kg:Opportunity ;
    kg:closeDate \"next friday\" ."
    );
    let report =
        eg_shacl::validate_turtle(&shapes, &bad_date).expect("fixture parses");
    assert!(
        !report.conforms,
        "an invalid closeDate datatype must be refused: {:?}",
        report.results
    );
    assert!(
        report.results.iter().any(|result| {
            result
                .path
                .as_deref()
                .is_some_and(|path| path.ends_with("#closeDate>"))
                && result.constraint_component.ends_with("DatatypeConstraintComponent")
        }),
        "the refusal lands on the closeDate datatype: {:?}",
        report.results
    );
}

const INFRA_SHAPES: &str =
    include_str!("../ontology/infrastructure-v1.shapes.ttl");

const INFRA_DATA_PREFIX: &str = r#"
@prefix infra: <http://knuckles.team/kg/infrastructure#> .
@prefix ex:    <http://knuckles.team/test/host-limit#> .
"#;

// spec: EG-SUPPLY-VOCAB-R004.2
#[test]
fn host_limit_shape_requires_reading_and_closed_kind() {
    let shape_triples =
        parse_turtle(INFRA_SHAPES).expect("the infrastructure shapes parse");
    assert!(
        has_iri(
            &shape_triples,
            &format!("{INFRA}HostLimitShape"),
            &format!("{RDF}type"),
            &format!("{SHACL}NodeShape")
        ),
        "HostLimitShape is a node shape"
    );
    // A complete row is accepted.
    let valid = format!(
        "{INFRA_DATA_PREFIX}
ex:diskLimit a infra:HostLimit ;
    infra:limitOf ex:rackHost ;
    infra:limitKind \"disk\" ;
    infra:readingValue 1200000000000 ;
    infra:limitValue 2000000000000 .

ex:rackHost a infra:Host ."
    );
    let report =
        eg_shacl::validate_turtle(INFRA_SHAPES, &valid).expect("valid fixture parses");
    assert!(report.conforms, "a complete HostLimit row: {:?}", report.results);
    // A missing limit value is refused.
    let missing_limit = format!(
        "{INFRA_DATA_PREFIX}
ex:diskLimit a infra:HostLimit ;
    infra:limitOf ex:rackHost ;
    infra:limitKind \"disk\" ;
    infra:readingValue 1200000000000 .
ex:rackHost a infra:Host ."
    );
    let report =
        eg_shacl::validate_turtle(INFRA_SHAPES, &missing_limit).expect("fixture parses");
    assert!(
        !report.conforms,
        "a missing limit value must be refused: {:?}",
        report.results
    );
    assert!(
        report.results.iter().any(|result| {
            result
                .path
                .as_deref()
                .is_some_and(|path| path.ends_with("#limitValue>"))
                && result.constraint_component.ends_with("MinCountConstraintComponent")
        }),
        "the refusal lands on the missing limitValue: {:?}",
        report.results
    );
    // An unknown limit kind is refused.
    let unknown_kind = format!(
        "{INFRA_DATA_PREFIX}
ex:diskLimit a infra:HostLimit ;
    infra:limitOf ex:rackHost ;
    infra:limitKind \"fan\" ;
    infra:readingValue 1200000000000 ;
    infra:limitValue 2000000000000 .
ex:rackHost a infra:Host ."
    );
    let report =
        eg_shacl::validate_turtle(INFRA_SHAPES, &unknown_kind).expect("fixture parses");
    assert!(
        !report.conforms,
        "an unknown limit kind must be refused: {:?}",
        report.results
    );
    assert!(
        report.results.iter().any(|result| {
            result
                .path
                .as_deref()
                .is_some_and(|path| path.ends_with("#limitKind>"))
                && result.constraint_component.ends_with("InConstraintComponent")
        }),
        "the refusal lands on the closed limitKind vocabulary: {:?}",
        report.results
    );
}

// spec: EG-SUPPLY-VOCAB-R004.3
#[test]
fn declared_capacity_thresholds_pass_the_host_limit_shape() {
    let fixture = parse_turtle(include_str!(
        "fixtures/host_limit_thresholds.ttl"
    ))
    .expect("the declared-threshold fixture parses");
    // The fixture declares one row per capacity kind (D6).
    for kind in ["disk", "memory", "cpu"] {
        assert!(
            fixture
                .iter()
                .any(|triple| {
                    triple.predicate.as_str().ends_with("limitKind")
                        && matches!(&triple.object, Term::Literal(l) if l.value() == kind)
                }),
            "the fixture declares a {kind} threshold"
        );
    }
    // Every declared row passes the HostLimit shape.
    let ttl = include_str!("fixtures/host_limit_thresholds.ttl");
    let report =
        eg_shacl::validate_turtle(INFRA_SHAPES, ttl).expect("fixture parses");
    assert!(
        report.conforms,
        "the declared capacity thresholds pass HostLimitShape: {:?}",
        report.results
    );
}

const VG: &str = "http://knuckles.team/kg/virtual-graph#";

const VM_ERPNEXT: &str = "http://knuckles.team/test/vm-erpnext#";
const VM_LEANIX: &str = "http://knuckles.team/test/vm-leanix#";

const VIRTUAL_SHAPES: &str = include_str!("../ontology/virtual_graph-v1.shapes.ttl");

// spec: EG-SUPPLY-VOCAB-R005.1
#[test]
fn erpnext_supplier_mapping_fixture_passes_the_virtual_shapes() {
    let fixture = include_str!("fixtures/vm_erpnext_supplier.ttl");
    let report =
        eg_shacl::validate_turtle(VIRTUAL_SHAPES, fixture).expect("the fixture parses");
    assert!(
        report.conforms,
        "the erpnext Supplier mapping fixture passes the virtual-graph shapes: {:?}",
        report.results
    );
    let triples = parse_turtle(fixture).expect("the fixture parses");
    // The Supplier doctype projects onto the shared kg:Supplier class.
    assert!(
        has_iri(
            &triples,
            &format!("{VM_ERPNEXT}supplierMapping"),
            &format!("{VG}mapsOntologyClass"),
            &format!("{KG}Supplier")
        ),
        "the mapping maps the erpnext Supplier doctype to kg:Supplier"
    );
    // Rows are keyed by `name`, with the label read from `supplier_name`.
    let mapping_key = format!("<{VM_ERPNEXT}supplierMapping>");
    let key_field = triples
        .iter()
        .find(|triple| {
            subject_key(triple) == mapping_key
                && triple.predicate.as_str().ends_with("keyField")
        })
        .expect("the mapping names its keyField");
    assert_eq!(literal_value(&key_field.object), "name");
    let pair = triples
        .iter()
        .find(|triple| {
            subject_key(triple) == mapping_key
                && triple.predicate.as_str().ends_with("hasPredicateFieldPair")
        })
        .and_then(object_key)
        .expect("the mapping declares a predicate-field pair");
    assert!(
        triples.iter().any(|triple| {
            subject_key(triple) == pair
                && triple.predicate.as_str().ends_with("predicate")
                && matches!(&triple.object, Term::NamedNode(n) if n.as_str() == format!("{RDFS}label"))
        }),
        "the pair reads the label predicate"
    );
    let pair_field = triples
        .iter()
        .find(|triple| subject_key(triple) == pair && triple.predicate.as_str().ends_with("field"))
        .expect("the predicate-field pair names its field");
    assert_eq!(
        literal_value(&pair_field.object),
        "supplier_name",
        "the label is read from `supplier_name`"
    );
    // The mapping rides the supplierAccess access contract, whose recorded
    // operation is erpnext's list_documents.
    assert!(
        has_iri(
            &triples,
            &format!("{VM_ERPNEXT}supplierMapping"),
            &format!("{VG}mapsContract"),
            &format!("{VM_ERPNEXT}supplierAccess")
        ),
        "the mapping rides the supplierAccess contract"
    );
    let operation = triples
        .iter()
        .find(|triple| {
            subject_key(triple) == format!("<{VM_ERPNEXT}supplierAccess>")
                && triple.predicate.as_str().ends_with("discoveredOperation")
        })
        .expect("the contract records its discovered operation");
    assert_eq!(literal_value(&operation.object), "list_documents");
}

// spec: EG-SUPPLY-VOCAB-R005.2
#[test]
fn erpnext_item_and_po_mapping_declares_the_lines_child_fetch() {
    let fixture = include_str!("fixtures/vm_erpnext_item_po.ttl");
    let report =
        eg_shacl::validate_turtle(VIRTUAL_SHAPES, fixture).expect("the fixture parses");
    assert!(
        report.conforms,
        "the erpnext Item/Purchase Order fixture passes the virtual-graph shapes: {:?}",
        report.results
    );
    let triples = parse_turtle(fixture).expect("the fixture parses");
    // Item projects onto kg:SupplyChainItem.
    assert!(
        has_iri(
            &triples,
            &format!("{VM_ERPNEXT}itemMapping"),
            &format!("{VG}mapsOntologyClass"),
            &format!("{KG}SupplyChainItem")
        ),
        "the Item doctype maps to kg:SupplyChainItem"
    );
    // Purchase Order projects onto kg:PurchaseOrder with exactly the two
    // derivation edges suppliesItem is built from (Decision D5): suppliedBy
    // from `supplier`, contains from the `items` lines.
    assert!(
        has_iri(
            &triples,
            &format!("{VM_ERPNEXT}purchaseOrderMapping"),
            &format!("{VG}mapsOntologyClass"),
            &format!("{KG}PurchaseOrder")
        ),
        "the Purchase Order doctype maps to kg:PurchaseOrder"
    );
    let mut pairs = pair_fields(&triples, &format!("<{VM_ERPNEXT}purchaseOrderMapping>"));
    pairs.sort();
    assert_eq!(
        pairs,
        vec![
            ("contains".to_string(), "items".to_string()),
            ("suppliedBy".to_string(), "supplier".to_string()),
        ],
        "the PO mapping projects exactly the two suppliesItem derivation edges"
    );
    // The `items` lines are child records: `list_documents` does not return
    // them, so the purchaseOrderAccess contract declares the per-order
    // get_document child fetch (Decision D2).
    let po_contract = format!("<{VM_ERPNEXT}purchaseOrderAccess>");
    for (predicate, expected) in [
        ("discoveredOperation", "get_document"),
        ("discoveredField", "items"),
        ("discoveredRelationship", "items"),
    ] {
        assert!(
            triples.iter().any(|triple| {
                subject_key(triple) == po_contract
                    && triple.predicate.as_str().ends_with(predicate)
                    && literal_value(&triple.object) == expected
            }),
            "the PO contract declares {predicate} `{expected}`"
        );
    }
}


// spec: EG-SUPPLY-VOCAB-R005.4
#[test]
fn leanix_provider_maps_to_supplier_rank_two_with_supports_capability() {
    let fixture = include_str!("fixtures/vm_leanix.ttl");
    let report =
        eg_shacl::validate_turtle(VIRTUAL_SHAPES, fixture).expect("the fixture parses");
    assert!(
        report.conforms,
        "the leanix Provider mapping fixture passes the virtual-graph shapes: {:?}",
        report.results
    );
    let triples = parse_turtle(fixture).expect("the fixture parses");
    assert!(
        has_iri(
            &triples,
            &format!("{VM_LEANIX}providerMapping"),
            &format!("{VG}mapsOntologyClass"),
            &format!("{KG}Supplier")
        ),
        "the leanix Provider maps to the shared kg:Supplier"
    );
    // The connector-side `supports` relation projects onto the shared
    // service-to-capability property (Decision D1).
    let mut pairs = pair_fields(&triples, &format!("<{VM_LEANIX}providerMapping>"));
    pairs.sort();
    assert_eq!(
        pairs,
        vec![
            ("label".to_string(), "title".to_string()),
            ("supportsCapability".to_string(), "supports".to_string()),
        ],
        "the leanix mapping projects title and the supports relation"
    );
    // LeanIX is the rank-2 authority for Supplier, over the same connection.
    let authority = format!("<{VM_LEANIX}providerAuthority>");
    assert!(
        triples
            .iter()
            .any(|triple| subject_key(triple) == authority
                && triple.predicate.as_str().ends_with("type")
                && matches!(&triple.object, Term::NamedNode(n) if n.as_str() == format!("{VG}SourceAuthority"))),
        "the rank declaration is a vg:SourceAuthority"
    );
    assert!(
        has_iri(
            &triples,
            &format!("{VM_LEANIX}providerAuthority"),
            &format!("{VG}authorityFor"),
            &format!("{KG}Supplier")
        )
    );
    assert!(
        has_iri(
            &triples,
            &format!("{VM_LEANIX}providerAuthority"),
            &format!("{VG}authoritativeSource"),
            &format!("{VM_LEANIX}leanixConnection")
        )
    );
    let rank = triples
        .iter()
        .find(|triple| {
            subject_key(triple) == authority
                && triple.predicate.as_str().ends_with("authorityRank")
        })
        .expect("the authority declares its rank");
    assert_eq!(
        literal_value(&rank.object),
        "2",
        "leanix is the rank-2 source for Supplier"
    );
}

const VM_SERVICE_NOW: &str = "http://knuckles.team/test/vm-servicenow#";

// spec: EG-SUPPLY-VOCAB-R005.5
#[test]
fn servicenow_incident_change_ci_maps_to_affects_service() {
    let fixture = include_str!("fixtures/vm_servicenow.ttl");
    let report =
        eg_shacl::validate_turtle(VIRTUAL_SHAPES, fixture).expect("the fixture parses");
    assert!(
        report.conforms,
        "the servicenow fixture passes the virtual-graph shapes: {:?}",
        report.results
    );
    let triples = parse_turtle(fixture).expect("the fixture parses");
    // Incident and Change each project their CI field onto kg:affectsService.
    for (mapping, class) in [
        ("incidentMapping", format!("{KG}Incident")),
        ("changeMapping", format!("{KG}Change")),
    ] {
        assert!(
            has_iri(
                &triples,
                &format!("{VM_SERVICE_NOW}{mapping}"),
                &format!("{VG}mapsOntologyClass"),
                &class
            ),
            "{mapping} maps to {class}"
        );
        let mut pairs = pair_fields(&triples, &format!("<{VM_SERVICE_NOW}{mapping}>"));
        pairs.sort();
        assert_eq!(
            pairs,
            vec![
                ("affectsService".to_string(), "ci".to_string()),
                ("label".to_string(), "name".to_string()),
            ],
            "{mapping} projects its CI field onto affectsService"
        );
    }
    // The change request's CI list is a per-change child fetch, and the CMDB
    // instance is what resolves a CI to the service record.
    let change_contract = format!("<{VM_SERVICE_NOW}changeAccess>");
    assert!(
        triples.iter().any(|triple| {
            subject_key(triple) == change_contract
                && triple.predicate.as_str().ends_with("discoveredOperation")
                && literal_value(&triple.object) == "get_change_request_ci"
        }),
        "the change contract declares the get_change_request_ci child fetch"
    );
    assert!(
        has_iri(
            &triples,
            &format!("{VM_SERVICE_NOW}cmdbServiceMapping"),
            &format!("{VG}mapsOntologyClass"),
            &format!("{INFRA}Service")
        ),
        "the CMDB instance mapping resolves a CI to a service record"
    );
    let cmdb_contract = format!("<{VM_SERVICE_NOW}cmdbAccess>");
    assert!(
        triples.iter().any(|triple| {
            subject_key(triple) == cmdb_contract
                && triple.predicate.as_str().ends_with("discoveredOperation")
                && literal_value(&triple.object) == "get_cmdb_instances"
        }),
        "the CMDB contract declares get_cmdb_instances"
    );
}

const VM_SALESFORCE: &str = "http://knuckles.team/test/vm-salesforce#";

// spec: EG-SUPPLY-VOCAB-R005.6
#[test]
fn salesforce_account_and_contract_map_via_soql() {
    let fixture = include_str!("fixtures/vm_salesforce.ttl");
    let report =
        eg_shacl::validate_turtle(VIRTUAL_SHAPES, fixture).expect("the fixture parses");
    assert!(
        report.conforms,
        "the salesforce mapping fixture passes the virtual-graph shapes: {:?}",
        report.results
    );
    let triples = parse_turtle(fixture).expect("the fixture parses");
    // The standard Account sObject is the Customer source.
    assert!(
        has_iri(
            &triples,
            &format!("{VM_SALESFORCE}accountMapping"),
            &format!("{VG}mapsOntologyClass"),
            &format!("{KG}Customer")
        ),
        "salesforce Account maps to kg:Customer"
    );
    // The Contract sObject is read by SOQL (the connector ontology declares no
    // Contract class today) and projects the contract terms.
    assert!(
        has_iri(
            &triples,
            &format!("{VM_SALESFORCE}contractMapping"),
            &format!("{VG}mapsOntologyClass"),
            &format!("{KG}CustomerContract")
        ),
        "salesforce Contract maps to kg:CustomerContract"
    );
    let mut pairs = pair_fields(&triples, &format!("<{VM_SALESFORCE}contractMapping>"));
    pairs.sort();
    assert_eq!(
        pairs,
        vec![
            ("contractCustomer".to_string(), "AccountName".to_string()),
            ("contractEnd".to_string(), "EndDate".to_string()),
            ("contractStatus".to_string(), "Status".to_string()),
        ],
        "the contract mapping projects the customer, status and end date"
    );
    // Both mappings ride the salesforce_soql contract.
    for mapping in ["accountMapping", "contractMapping"] {
        assert!(
            has_iri(
                &triples,
                &format!("{VM_SALESFORCE}{mapping}"),
                &format!("{VG}mapsContract"),
                &format!("{VM_SALESFORCE}salesforceSoql")
            ),
            "{mapping} rides the salesforce_soql contract"
        );
    }
    let contract = format!("<{VM_SALESFORCE}salesforceSoql>");
    assert!(
        triples.iter().any(|triple| {
            subject_key(triple) == contract
                && triple.predicate.as_str().ends_with("discoveredOperation")
                && literal_value(&triple.object) == "salesforce_soql"
        }),
        "the contract records the salesforce_soql operation"
    );
}

const VM_TWENTY: &str = "http://knuckles.team/test/vm-twenty#";

// spec: EG-SUPPLY-VOCAB-R005.8
#[test]
fn twenty_company_maps_to_customer_rank_two_without_a_contract_mapping() {
    let fixture = include_str!("fixtures/vm_twenty.ttl");
    let report =
        eg_shacl::validate_turtle(VIRTUAL_SHAPES, fixture).expect("the fixture parses");
    assert!(
        report.conforms,
        "the twenty Company mapping fixture passes the virtual-graph shapes: {:?}",
        report.results
    );
    let triples = parse_turtle(fixture).expect("the fixture parses");
    assert!(
        has_iri(
            &triples,
            &format!("{VM_TWENTY}companyMapping"),
            &format!("{VG}mapsOntologyClass"),
            &format!("{KG}Customer")
        ),
        "twenty Company maps to kg:Customer"
    );
    let authority = format!("{VM_TWENTY}companyAuthority");
    assert!(
        has_iri(
            &triples,
            &authority,
            &format!("{VG}authorityFor"),
            &format!("{KG}Customer")
        )
    );
    let rank = triples
        .iter()
        .find(|triple| {
            subject_key(triple) == format!("<{authority}>")
                && triple.predicate.as_str().ends_with("authorityRank")
        })
        .expect("the authority declares its rank");
    assert_eq!(
        literal_value(&rank.object),
        "2",
        "twenty is the rank-2 source for Customer"
    );
    // Twenty has no contract object in its manifest, so no mapping projects
    // onto kg:CustomerContract: a contract query against twenty abstains.
    let contract_class = format!("{KG}CustomerContract");
    assert!(
        !triples
            .iter()
            .any(|triple| {
                triple.predicate.as_str().ends_with("mapsOntologyClass")
                    && matches!(&triple.object, Term::NamedNode(n) if n.as_str() == contract_class)
            }),
        "the fixture declares no CustomerContract mapping"
    );
}

const VM_CONTAINER_MANAGER: &str = "http://knuckles.team/test/vm-container-manager#";

// spec: EG-SUPPLY-VOCAB-R006.3
#[test]
fn container_manager_workload_and_image_map_deployed_as_and_uses_image() {
    let fixture = include_str!("fixtures/vm_container_manager.ttl");
    let report =
        eg_shacl::validate_turtle(VIRTUAL_SHAPES, fixture).expect("the fixture parses");
    assert!(
        report.conforms,
        "the container-manager mapping fixture passes the virtual-graph shapes: {:?}",
        report.results
    );
    let triples = parse_turtle(fixture).expect("the fixture parses");
    // The Deployment projects onto the workload the service is deployed as,
    // with its image field projecting onto kg:usesImage.
    assert!(
        has_iri(
            &triples,
            &format!("{VM_CONTAINER_MANAGER}deploymentMapping"),
            &format!("{VG}mapsOntologyClass"),
            &format!("{INFRA}Workload")
        ),
        "the Deployment maps to infra:Workload"
    );
    let mut pairs = pair_fields(
        &triples,
        &format!("<{VM_CONTAINER_MANAGER}deploymentMapping>"),
    );
    pairs.sort();
    assert_eq!(
        pairs,
        vec![
            ("label".to_string(), "name".to_string()),
            ("usesImage".to_string(), "image".to_string()),
        ],
        "the deployment mapping projects its image onto usesImage"
    );
    // The K8sService record's workload reference projects onto kg:deployedAs
    // (entity resolution between the service and its workload).
    assert!(
        has_iri(
            &triples,
            &format!("{VM_CONTAINER_MANAGER}k8sServiceMapping"),
            &format!("{VG}mapsOntologyClass"),
            &format!("{INFRA}Service")
        ),
        "the K8sService maps to infra:Service"
    );
    let mut pairs = pair_fields(
        &triples,
        &format!("<{VM_CONTAINER_MANAGER}k8sServiceMapping>"),
    );
    pairs.sort();
    assert_eq!(
        pairs,
        vec![
            ("deployedAs".to_string(), "workload".to_string()),
            ("label".to_string(), "name".to_string()),
        ],
        "the K8sService mapping projects its workload onto deployedAs"
    );
    // The image entity projects onto the container image class.
    assert!(
        has_iri(
            &triples,
            &format!("{VM_CONTAINER_MANAGER}imageMapping"),
            &format!("{VG}mapsOntologyClass"),
            &format!("{INFRA}ContainerImage")
        ),
        "the Image maps to infra:ContainerImage"
    );
    // Both contracts ride the confirmed operations.
    for (contract, operation) in [
        ("cmWorkloadsAccess", "list_pods"),
        ("cmImagesAccess", "list_images"),
    ] {
        let node = format!("<{VM_CONTAINER_MANAGER}{contract}>");
        assert!(
            triples.iter().any(|triple| {
                subject_key(triple) == node
                    && triple.predicate.as_str().ends_with("discoveredOperation")
                    && literal_value(&triple.object) == operation
            }),
            "the {contract} contract declares {operation}"
        );
    }
}

const VM_UPTIME: &str = "http://knuckles.team/test/vm-uptime#";

// spec: EG-SUPPLY-VOCAB-R006.4
#[test]
fn uptime_monitor_resolves_to_service_by_url_host_key() {
    let fixture = include_str!("fixtures/vm_uptime.ttl");
    let report =
        eg_shacl::validate_turtle(VIRTUAL_SHAPES, fixture).expect("the fixture parses");
    assert!(
        report.conforms,
        "the uptime mapping fixture passes the virtual-graph shapes: {:?}",
        report.results
    );
    let triples = parse_turtle(fixture).expect("the fixture parses");
    // A monitor is not a Service: it resolves to one by the host of the URL
    // it monitors, so the Service projection is keyed on the `url` field.
    assert!(
        has_iri(
            &triples,
            &format!("{VM_UPTIME}monitorServiceMapping"),
            &format!("{VG}mapsOntologyClass"),
            &format!("{INFRA}Service")
        ),
        "the monitor resolves to infra:Service"
    );
    let key_field = triples
        .iter()
        .find(|triple| {
            subject_key(triple) == format!("<{VM_UPTIME}monitorServiceMapping>")
                && triple.predicate.as_str().ends_with("keyField")
        })
        .expect("the monitor mapping names its keyField");
    assert_eq!(
        literal_value(&key_field.object),
        "url",
        "the monitor resolves to its service by the URL host key"
    );
    // The heartbeat stat is the service's health signal.
    assert!(
        has_iri(
            &triples,
            &format!("{VM_UPTIME}heartbeatTrendMapping"),
            &format!("{VG}mapsOntologyClass"),
            &format!("{INFRA}HealthTrend")
        ),
        "the heartbeat stat maps to infra:HealthTrend"
    );
    // Both contracts ride the confirmed operations.
    for (contract, operation) in [
        ("uptimeMonitorsAccess", "get_monitors"),
        ("uptimeHeartbeatsAccess", "get_heartbeats"),
    ] {
        let node = format!("<{VM_UPTIME}{contract}>");
        assert!(
            triples.iter().any(|triple| {
                subject_key(triple) == node
                    && triple.predicate.as_str().ends_with("discoveredOperation")
                    && literal_value(&triple.object) == operation
            }),
            "the {contract} contract declares {operation}"
        );
    }
}

const VM_DOCKERHUB: &str = "http://knuckles.team/test/vm-dockerhub#";

// spec: EG-SUPPLY-VOCAB-R006.5
#[test]
fn dockerhub_image_sbom_and_cves_map_components_and_vulnerabilities() {
    let fixture = include_str!("fixtures/vm_dockerhub.ttl");
    let report =
        eg_shacl::validate_turtle(VIRTUAL_SHAPES, fixture).expect("the fixture parses");
    assert!(
        report.conforms,
        "the dockerhub mapping fixture passes the virtual-graph shapes: {:?}",
        report.results
    );
    let triples = parse_turtle(fixture).expect("the fixture parses");
    // The image record carries both child fetches: the SBOM projects onto
    // kg:containsComponent, the CVE list onto kg:hasVulnerability.
    assert!(
        has_iri(
            &triples,
            &format!("{VM_DOCKERHUB}imageMapping"),
            &format!("{VG}mapsOntologyClass"),
            &format!("{INFRA}ContainerImage")
        ),
        "the dockerhub image maps to infra:ContainerImage"
    );
    let mut pairs = pair_fields(&triples, &format!("<{VM_DOCKERHUB}imageMapping>"));
    pairs.sort();
    assert_eq!(
        pairs,
        vec![
            ("containsComponent".to_string(), "sbom".to_string()),
            ("hasVulnerability".to_string(), "cves".to_string()),
            ("label".to_string(), "name".to_string()),
        ],
        "the image mapping declares the sbom and cves child fetches"
    );
    // Each SBOM row is a component version.
    assert!(
        has_iri(
            &triples,
            &format!("{VM_DOCKERHUB}componentVersionMapping"),
            &format!("{VG}mapsOntologyClass"),
            &format!("{KG}ComponentVersion")
        ),
        "the SBOM row maps to kg:ComponentVersion"
    );
    let mut pairs = pair_fields(
        &triples,
        &format!("<{VM_DOCKERHUB}componentVersionMapping>"),
    );
    pairs.sort();
    assert_eq!(
        pairs,
        vec![("versionString".to_string(), "version".to_string())],
        "the SBOM row projects its version string"
    );
    // Each Scout CVE row is a vulnerability affecting a component.
    assert!(
        has_iri(
            &triples,
            &format!("{VM_DOCKERHUB}vulnerabilityMapping"),
            &format!("{VG}mapsOntologyClass"),
            &format!("{KG}Vulnerability")
        ),
        "the Scout CVE row maps to kg:Vulnerability"
    );
    let mut pairs = pair_fields(
        &triples,
        &format!("<{VM_DOCKERHUB}vulnerabilityMapping>"),
    );
    pairs.sort();
    assert_eq!(
        pairs,
        vec![
            ("affectsComponent".to_string(), "component".to_string()),
            ("cveId".to_string(), "cve".to_string()),
            ("severity".to_string(), "severity".to_string()),
        ],
        "the CVE row projects id, severity and its affected component"
    );
    // The hub_scout actions are the confirmed operations.
    for (contract, operation) in [
        ("hubScoutSummary", "summary"),
        ("hubScoutSbom", "sbom"),
        ("hubScoutCves", "cves"),
    ] {
        let node = format!("<{VM_DOCKERHUB}{contract}>");
        assert!(
            triples.iter().any(|triple| {
                subject_key(triple) == node
                    && triple.predicate.as_str().ends_with("discoveredOperation")
                    && literal_value(&triple.object) == operation
            }),
            "the {contract} contract declares {operation}"
        );
    }
}

const VM_GITLAB_GITHUB: &str = "http://knuckles.team/test/vm-gitlab-github#";

// spec: EG-SUPPLY-VOCAB-R006.6
#[test]
fn gitlab_dependencies_and_github_dependabot_map_rank_two() {
    let fixture = include_str!("fixtures/vm_gitlab_github.ttl");
    let report =
        eg_shacl::validate_turtle(VIRTUAL_SHAPES, fixture).expect("the fixture parses");
    assert!(
        report.conforms,
        "the gitlab/github mapping fixture passes the virtual-graph shapes: {:?}",
        report.results
    );
    let triples = parse_turtle(fixture).expect("the fixture parses");
    // The gitlab dependency list projects onto kg:ComponentVersion.
    assert!(
        has_iri(
            &triples,
            &format!("{VM_GITLAB_GITHUB}gitlabComponentVersionMapping"),
            &format!("{VG}mapsOntologyClass"),
            &format!("{KG}ComponentVersion")
        ),
        "the gitlab dependency maps to kg:ComponentVersion"
    );
    let mut pairs = pair_fields(
        &triples,
        &format!("<{VM_GITLAB_GITHUB}gitlabComponentVersionMapping>"),
    );
    pairs.sort();
    assert_eq!(
        pairs,
        vec![("versionString".to_string(), "version".to_string())],
        "the gitlab dependency row projects its version string"
    );
    // The gitlab advisory and the github Dependabot alert each project onto
    // kg:Vulnerability, affecting their package.
    for (mapping, label_field) in [
        ("gitlabVulnerabilityMapping", "id"),
        ("dependabotVulnerabilityMapping", "number"),
    ] {
        assert!(
            has_iri(
                &triples,
                &format!("{VM_GITLAB_GITHUB}{mapping}"),
                &format!("{VG}mapsOntologyClass"),
                &format!("{KG}Vulnerability")
            ),
            "{mapping} maps to kg:Vulnerability"
        );
        let mut pairs = pair_fields(&triples, &format!("<{VM_GITLAB_GITHUB}{mapping}>"));
        pairs.sort();
        assert_eq!(
            pairs,
            vec![
                ("affectsComponent".to_string(), "package".to_string()),
                ("label".to_string(), label_field.to_string()),
                ("severity".to_string(), "severity".to_string()),
            ],
            "{mapping} projects severity, label and its affected package"
        );
    }
    // Both sources are rank 2 (dockerhub is rank 1).
    for authority in [
        "gitlabComponentAuthority",
        "gitlabVulnerabilityAuthority",
        "githubVulnerabilityAuthority",
    ] {
        let node = format!("<{VM_GITLAB_GITHUB}{authority}>");
        assert!(
            triples
                .iter()
                .any(|triple| subject_key(triple) == node
                    && triple.predicate.as_str().ends_with("authorityRank")
                    && literal_value(&triple.object) == "2"),
            "{authority} is rank 2"
        );
    }
    // The confirmed operations.
    for (contract, operation) in [
        ("gitlabDependenciesAccess", "dependencies"),
        ("gitlabVulnerabilitiesAccess", "vulnerabilities"),
    ] {
        let node = format!("<{VM_GITLAB_GITHUB}{contract}>");
        assert!(
            triples.iter().any(|triple| {
                subject_key(triple) == node
                    && triple.predicate.as_str().ends_with("discoveredOperation")
                    && literal_value(&triple.object) == operation
            }),
            "the {contract} contract declares {operation}"
        );
    }
    let github_contract = format!("<{VM_GITLAB_GITHUB}githubDependabotAccess>");
    for operation in ["list", "get"] {
        assert!(
            triples.iter().any(|triple| {
                subject_key(triple) == github_contract
                    && triple.predicate.as_str().ends_with("discoveredOperation")
                    && literal_value(&triple.object) == operation
            }),
            "the github_dependabot contract declares {operation}"
        );
    }
}

const VM_HOST_LIMITS: &str = "http://knuckles.team/test/vm-host-limits#";

// spec: EG-SUPPLY-VOCAB-R006.7
#[test]
fn fan_manager_and_systems_manager_assemble_host_limit_rows() {
    let fixture = include_str!("fixtures/vm_host_limits.ttl");
    let report =
        eg_shacl::validate_turtle(VIRTUAL_SHAPES, fixture).expect("the fixture parses");
    assert!(
        report.conforms,
        "the host-limits mapping fixture passes the virtual-graph shapes: {:?}",
        report.results
    );
    let triples = parse_turtle(fixture).expect("the fixture parses");
    // Every mapping projects onto the normalized HostLimit pair.
    for mapping in [
        "temperatureReadingMapping",
        "capacityReadingMapping",
        "fanPolicyMapping",
    ] {
        assert!(
            has_iri(
                &triples,
                &format!("{VM_HOST_LIMITS}{mapping}"),
                &format!("{VG}mapsOntologyClass"),
                &format!("{INFRA}HostLimit")
            ),
            "{mapping} maps to infra:HostLimit"
        );
    }
    // Temperature: the reading side is celsius, the limit side the policy
    // ceiling (Decision D6).
    let mut pairs = pair_fields(
        &triples,
        &format!("<{VM_HOST_LIMITS}temperatureReadingMapping>"),
    );
    pairs.sort();
    assert_eq!(
        pairs,
        vec![
            ("limitOf".to_string(), "hostname".to_string()),
            ("readingValue".to_string(), "celsius".to_string()),
        ],
        "the temperature reading projects celsius"
    );
    let mut pairs = pair_fields(&triples, &format!("<{VM_HOST_LIMITS}fanPolicyMapping>"));
    pairs.sort();
    assert_eq!(
        pairs,
        vec![
            ("limitOf".to_string(), "hostname".to_string()),
            ("limitValue".to_string(), "maxTemperature".to_string()),
        ],
        "the fan policy projects the ceiling"
    );
    // Capacity: the freeBytes reading pairs with the declared R004.3
    // thresholds (no source exposes a capacity limit).
    let mut pairs = pair_fields(&triples, &format!("<{VM_HOST_LIMITS}capacityReadingMapping>"));
    pairs.sort();
    assert_eq!(
        pairs,
        vec![
            ("limitOf".to_string(), "hostname".to_string()),
            ("readingValue".to_string(), "freeBytes".to_string()),
        ],
        "the capacity reading projects freeBytes"
    );
    // A HostLimit row has both values: the mappings together cover
    // readingValue and limitValue.
    let all: Vec<(String, String)> = [
        "temperatureReadingMapping",
        "capacityReadingMapping",
        "fanPolicyMapping",
    ]
    .iter()
    .flat_map(|mapping| pair_fields(&triples, &format!("<{VM_HOST_LIMITS}{mapping}>")))
    .collect();
    let predicates: Vec<&str> = all.iter().map(|(predicate, _)| predicate.as_str()).collect();
    assert!(
        predicates.iter().any(|p| *p == "readingValue"),
        "some mapping projects the reading side"
    );
    assert!(
        predicates.iter().any(|p| *p == "limitValue"),
        "some mapping projects the limit side"
    );
}

const BLOCKING: &str = "http://knuckles.team/test/blocking-keys#";

// spec: EG-SUPPLY-VOCAB-R007.1
#[test]
fn blocking_key_fixture_covers_each_concept_with_two_keys() {
    let triples =
        parse_turtle(include_str!("fixtures/supply_chain_blocking_keys.ttl"))
            .expect("the blocking-key fixture parses");
    for (concept, keyset) in [
        (format!("{KG}Supplier"), "supplierKeys"),
        (format!("{INFRA}Service"), "serviceKeys"),
        (format!("{INFRA}Host"), "hostKeys"),
        (format!("{INFRA}ContainerImage"), "containerImageKeys"),
        (format!("{KG}Customer"), "customerKeys"),
    ] {
        assert!(
            has_iri(
                &triples,
                &format!("{BLOCKING}{keyset}"),
                &format!("{BLOCKING}forConcept"),
                &concept
            ),
            "{keyset} is the key set for {concept}"
        );
        let keys = triples
            .iter()
            .filter(|triple| {
                subject_key(triple) == format!("<{BLOCKING}{keyset}>")
                    && triple.predicate.as_str().ends_with("hasKey")
            })
            .count();
        assert!(keys >= 2, "{keyset} lists at least two keys (found {keys})");
    }
}
// spec: EG-SUPPLY-VOCAB-R007.2
#[test]
fn digest_key_is_exact_name_only_key_is_a_proposal() {
    use eg_types::entity_link_proposal::{
        AttachedApplication, CannotAssert, EntityLinkProposal, EntityRef, MatchClass,
    };

    let triples =
        parse_turtle(include_str!("fixtures/supply_chain_blocking_keys.ttl"))
            .expect("the blocking-key fixture parses");
    // The digest is the one exact key in the ContainerImage set; no key in
    // the Supplier set is exact.
    let exact_keys = |keyset: &str| -> usize {
        triples
            .iter()
            .filter(|triple| {
                subject_key(triple) == format!("<{BLOCKING}{keyset}>")
                    && triple.predicate.as_str().ends_with("hasKey")
            })
            .filter_map(object_key)
            .filter(|key| {
                triples.iter().any(|triple| {
                    subject_key(triple) == *key
                        && triple.predicate.as_str().ends_with("exact")
                        && literal_value(&triple.object) == "true"
                })
            })
            .count()
    };
    assert_eq!(
        exact_keys("containerImageKeys"),
        1,
        "the digest is the one exact key"
    );
    assert_eq!(
        exact_keys("supplierKeys"),
        0,
        "no supplier key is exact"
    );
    // The fixture's two image rows share the digest value (the exact key).
    let key_value = |row: &str, predicate: &str| -> String {
        triples
            .iter()
            .find(|triple| {
                subject_key(triple) == format!("<{BLOCKING}{row}>")
                    && triple.predicate.as_str().ends_with(predicate)
            })
            .map(|triple| literal_value(&triple.object).to_string())
            .unwrap_or_default()
    };
    let scout = key_value("scoutImage", "digestValue");
    let mirror = key_value("mirrorImage", "digestValue");
    assert_eq!(scout, mirror, "the two image rows share the exact digest key");
    assert!(
        scout.starts_with("sha256:"),
        "the shared key is a sha256 digest"
    );
    // An exact digest match classifies as Match -- but a good score is never
    // itself the approval.
    let digest_proposal = EntityLinkProposal {
        left: EntityRef {
            application: AttachedApplication::Immich,
            entity_id: "scoutImage".to_string(),
        },
        right: EntityRef {
            application: AttachedApplication::Twenty,
            entity_id: "mirrorImage".to_string(),
        },
        match_class: MatchClass::Match,
        match_score_milli: 1000,
        approved_by: None,
    };
    assert_eq!(
        digest_proposal.assert_same_as(),
        Err(CannotAssert::NotApproved),
        "a digest match still needs approval"
    );
    let approved_digest = EntityLinkProposal {
        approved_by: Some("operator".to_string()),
        ..digest_proposal.clone()
    };
    assert!(
        approved_digest.assert_same_as().is_ok(),
        "an approved digest match asserts"
    );
    // The fixture's two supplier rows share only the name key, which is
    // never exact: the candidate is an Ambiguous proposal, and an approval
    // cannot override the classifier.
    assert_eq!(
        key_value("acmeRow", "legalName"),
        key_value("acmeAltRow", "legalName"),
        "the supplier rows share the normalized legal name"
    );
    let name_proposal = EntityLinkProposal {
        left: EntityRef {
            application: AttachedApplication::Firefly,
            entity_id: "acmeRow".to_string(),
        },
        right: EntityRef {
            application: AttachedApplication::Gramps,
            entity_id: "acmeAltRow".to_string(),
        },
        match_class: MatchClass::Ambiguous,
        match_score_milli: 620,
        approved_by: None,
    };
    assert_eq!(
        name_proposal.assert_same_as(),
        Err(CannotAssert::NotAMatch),
        "a name-only match is refused until approved"
    );
    let approved_name = EntityLinkProposal {
        approved_by: Some("operator".to_string()),
        ..name_proposal.clone()
    };
    assert_eq!(
        approved_name.assert_same_as(),
        Err(CannotAssert::NotAMatch),
        "a name-only key stays a proposal even with an approver"
    );
}

// spec: EG-SUPPLY-VOCAB-R008.1
#[test]
fn impact_query_is_class_only_and_parses() {
    let query = include_str!("fixtures/supply_chain_impact.rq");
    // The query parses as SPARQL 1.1.
    eg_rdf::sparql::parse_query(query).expect("the impact query parses");
    // The query text (comments stripped) names only the shared vocabulary
    // and infra:Service -- no connector, tool or doctype name.
    let query_only: String = query
        .lines()
        .map(|line| line.split_once('#').map_or(line, |(code, _)| code))
        .collect::<Vec<_>>()
        .join("\n");
    let lower = query_only.to_lowercase();
    for name in [
        "erpnext",
        "dockerhub",
        "hub_scout",
        "leanix",
        "servicenow",
        "salesforce",
        "twenty",
        "gitlab",
        "github",
        "dependabot",
        "container-manager",
        "k8s",
        "uptime",
        "kuma",
        "fan-manager",
        "systems-manager",
        "cmdb",
        "soql",
    ] {
        assert!(!lower.contains(name), "the query names no source: {name}");
    }
    // Every prefixed name is under the two vocabulary namespaces.
    let mut prefixes: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    let bytes = query_only.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b':' && !(i + 1 < bytes.len() && bytes[i + 1] == b'/') {
            let mut start = i;
            while start > 0
                && (bytes[start - 1].is_ascii_alphanumeric() || bytes[start - 1] == b'_')
            {
                start -= 1;
            }
            if start < i {
                prefixes.insert(String::from_utf8_lossy(&bytes[start..i]).to_string());
            }
        }
        i += 1;
    }
    assert_eq!(
        prefixes,
        std::collections::BTreeSet::from(["kg".to_string(), "infra".to_string()]),
        "the query uses only the kg and infra namespaces: {prefixes:?}"
    );
    assert!(
        query_only.contains("infra:Service"),
        "the query selects over infra:Service"
    );
}

/// The D4 table rows from plan.md: (concept, rank 1, rank 2, operation,
/// not-available note), markdown stripped.
fn d4_rows() -> Vec<(String, String, String, String, String)> {
    let plan = include_str!("../../../specs/supply-chain-vocabulary/plan.md");
    let section = plan
        .split_once("**D4.")
        .expect("plan.md carries the D4 table")
        .1
        .split("\n**D5")
        .next()
        .expect("the D4 table ends before D5");
    let mut rows = Vec::new();
    for line in section.lines() {
        let line = line.trim();
        if !line.starts_with('|') || line.starts_with("|---") {
            continue;
        }
        let cells: Vec<String> = line
            .trim_matches('|')
            .split('|')
            .map(|cell| cell.trim().to_string())
            .collect();
        if cells.len() != 5 || cells[0] == "Concept" {
            continue;
        }
        rows.push((
            cells[0].replace('`', ""),
            cells[1].clone(),
            cells[2].clone(),
            cells[3].clone(),
            cells[4].clone(),
        ));
    }
    rows
}

/// The recorded-response file name a D4 concept maps to.
fn concept_file(concept: &str) -> String {
    let lower = concept.to_lowercase();
    let mut out = String::new();
    for c in lower.chars() {
        out.push(if c.is_alphanumeric() { c } else { '_' });
    }
    while out.contains("__") {
        out = out.replace("__", "_");
    }
    out.trim_matches('_').to_string()
}

fn recorded_files() -> Vec<String> {
    let mut files = Vec::new();
    for entry in
        std::fs::read_dir("tests/fixtures/supply_chain_recorded")
            .expect("the recorded-response directory is present")
    {
        let entry = entry.unwrap();
        let name = entry.file_name().to_string_lossy().to_string();
        if name.ends_with(".ttl") {
            files.push(name);
        }
    }
    files
}

// spec: EG-SUPPLY-VOCAB-R008.2
#[test]
fn recorded_response_set_covers_every_available_d4_row() {
    // Every recorded file parses and records at least one row.
    let files = recorded_files();
    assert!(!files.is_empty(), "the recorded-response set is not empty");
    for name in &files {
        let document =
            std::fs::read_to_string(format!("tests/fixtures/supply_chain_recorded/{name}"))
                .unwrap_or_else(|err| panic!("{name} reads: {err}"));
        let parsed =
            parse_turtle(&document).unwrap_or_else(|err| panic!("{name} parses: {err}"));
        assert!(!parsed.is_empty(), "{name} records at least one triple");
    }
    // The set covers every D4 row that is not marked not available -- and
    // no row that is.
    for (concept, rank1, rank2, _operation, not_available) in d4_rows() {
        let available = !rank1.is_empty() || !rank2.is_empty();
        let file = format!("{}.ttl", concept_file(&concept));
        if available {
            assert!(
                files.contains(&file),
                "the available D4 row {concept} has a recorded response ({not_available})"
            );
        } else {
            assert!(
                !files.contains(&file),
                "the not-available D4 row {concept} carries no recorded response"
            );
        }
    }
}
// spec: EG-SUPPLY-VOCAB-R008.3
#[test]
fn every_d4_concept_is_covered_or_carries_a_typed_abstention() {
    let files = recorded_files();
    let mut typed_abstentions = 0;
    for (concept, rank1, rank2, _operation, not_available) in d4_rows() {
        let available = !rank1.is_empty() || !rank2.is_empty();
        let file = format!("{}.ttl", concept_file(&concept));
        if available {
            assert!(
                files.contains(&file),
                "{concept} has an authority row (recorded response)"
            );
        }
        // A not-available note is a typed abstention: it must be present for
        // every row without an authority source, and no concept is silently
        // uncovered.
        if not_available.trim().is_empty() {
            assert!(available, "{concept} is silently uncovered");
            continue;
        }
        assert!(
            !available || files.contains(&file),
            "{concept} abstains but has no authority row to abstain from"
        );
        typed_abstentions += 1;
    }
    // The two abstentions the row names: the twenty contracts gap inside the
    // CustomerContract row, and the coversService BLOCKED row.
    let covers = d4_rows()
        .into_iter()
        .find(|(concept, _, _, _, _)| concept == "coversService")
        .expect("the D4 table names coversService");
    assert!(
        covers.4.contains("BLOCKED"),
        "the coversService abstention is BLOCKED with a reason: {}",
        covers.4
    );
    let contract = d4_rows()
        .into_iter()
        .find(|(concept, _, _, _, _)| concept == "CustomerContract")
        .expect("the D4 table names CustomerContract");
    assert!(
        contract.4.to_lowercase().contains("twenty"),
        "the twenty contracts gap is typed: {}",
        contract.4
    );
    assert!(typed_abstentions >= 2, "the register lists the typed abstentions");
}
