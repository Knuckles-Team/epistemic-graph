//! Native SHACL acceptance for the connector-pack surface shapes
//! (EG-TYPED-PACKS-R102.1).

use eg_shacl::validate_turtle;

const SHAPES: &str = include_str!("../../eg-core/ontology/connector_pack-v1.shapes.ttl");

const CP: &str = "http://knuckles.team/kg/connector-pack#";

fn tool(component_id: Option<&str>, domain_link: Option<&str>) -> String {
    let mut parts = vec!["a cp:Tool".to_string()];
    if let Some(id) = component_id {
        parts.push(format!(r#"cp:componentId "{id}"^^xsd:string"#));
    }
    if let Some(link) = domain_link {
        parts.push(link.to_string());
    }
    format!(
        r#"@prefix cp: <http://knuckles.team/kg/connector-pack#> .
@prefix kg: <http://knuckles.team/kg#> .
@prefix ex: <http://knuckles.team/test/connector-pack#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

ex:checkoutTool {} .

ex:checkout a kg:Service .
"#,
        parts.join(" ;\n    ")
    )
}

fn feature(realized_by: Option<&str>) -> String {
    format!(
        r#"@prefix cp: <http://knuckles.team/kg/connector-pack#> .
@prefix ex: <http://knuckles.team/test/connector-pack#> .

ex:billingFeature a cp:Feature{} .
"#,
        realized_by
            .map(|member| format!(" ; cp:realizedBy ex:{member}"))
            .unwrap_or_default()
    )
}

// spec: EG-TYPED-PACKS-R102.1
#[test]
fn surface_shapes_accept_valid_and_refuse_invalid() {
    // A tool with one component id and one operatesOn domain class conforms.
    let valid = validate_turtle(
        SHAPES,
        &tool(Some("tool:checkout"), Some("cp:operatesOn ex:checkout")),
    )
    .expect("the shapes and a valid tool parse");
    assert!(valid.conforms, "valid tool: {:?}", valid.results);

    // The providesCapability branch is accepted in place of operatesOn.
    let via_capability = validate_turtle(
        SHAPES,
        &tool(
            Some("tool:checkout"),
            Some("cp:providesCapability ex:billingCap"),
        ),
    )
    .expect("the shapes and a capability tool parse");
    assert!(
        via_capability.conforms,
        "capability tool: {:?}",
        via_capability.results
    );

    // A feature with one realised-by member conforms.
    let valid_feature = validate_turtle(SHAPES, &feature(Some("tool")))
        .expect("the shapes and a valid feature parse");
    assert!(
        valid_feature.conforms,
        "valid feature: {:?}",
        valid_feature.results
    );

    // A tool without its component id is refused.
    let no_component = validate_turtle(SHAPES, &tool(None, Some("cp:operatesOn ex:checkout")))
        .expect("the shapes and the componentless tool parse");
    assert!(
        !no_component.conforms,
        "a componentless tool must be refused"
    );
    assert!(
        no_component.results.iter().any(|result| {
            result
                .path
                .as_deref()
                .map(|path| path.trim_matches(['<', '>']))
                == Some(&format!("{CP}componentId"))
        }),
        "the refusal names the componentId path: {:?}",
        no_component.results
    );

    // A tool with a component id but no domain link is refused.
    let no_domain = validate_turtle(SHAPES, &tool(Some("tool:checkout"), None))
        .expect("the shapes and the domainless tool parse");
    assert!(
        !no_domain.conforms,
        "a tool without operatesOn/providesCapability must be refused"
    );

    // A feature without a realised-by member is refused.
    let bare_feature =
        validate_turtle(SHAPES, &feature(None)).expect("the shapes and the bare feature parse");
    assert!(
        !bare_feature.conforms,
        "a feature without a realised-by member must be refused"
    );
    assert!(
        bare_feature.results.iter().any(|result| {
            result
                .path
                .as_deref()
                .map(|path| path.trim_matches(['<', '>']))
                == Some(&format!("{CP}realizedBy"))
        }),
        "the refusal names the realizedBy path: {:?}",
        bare_feature.results
    );
}
