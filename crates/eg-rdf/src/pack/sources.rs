//! Closed projection of SDK access contracts; admission approval is never a claim.

use crate::oxrdf::{NamedOrBlankNode, Term};
use eg_types::connector_pack::{PackSourceApproval, PackSourceKind, PackSourceRegistration};
use eg_types::contract::ResourceId;
use std::collections::BTreeMap;

const AC: &str = "https://knuckles-team.github.io/agent-connector-sdk/access#";
const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";

/// Extract source claims only from the digest-verified ontology body the import reads.
/// Missing declarations remain absent. Every extracted source starts held.
pub fn declared_sources(
    connector: &ResourceId,
    resource_uri: &str,
    text: &str,
) -> Result<Vec<PackSourceRegistration>, String> {
    let triples = crate::mapping::parse_turtle(text)
        .map_err(|_| "MALFORMED_BODY: access contract RDF is invalid".to_string())?;
    let mut contracts = std::collections::BTreeSet::new();
    for triple in &triples {
        if triple.predicate.as_str() == RDF_TYPE
            && matches!(&triple.object, Term::NamedNode(n) if n.as_str() == format!("{AC}AccessContract"))
        {
            let NamedOrBlankNode::NamedNode(subject) = &triple.subject else {
                return Err("MALFORMED_BODY: access contract needs a named IRI".into());
            };
            contracts.insert(subject.as_str().to_string());
        }
    }
    if contracts.len() > eg_types::connector_pack::MAX_PACK_ENTRIES {
        return Err("MALFORMED_BODY: too many access contracts".into());
    }
    let mut out = Vec::new();
    for subject in contracts {
        let mut props: BTreeMap<&str, Vec<&Term>> = BTreeMap::new();
        for triple in &triples {
            if matches!(&triple.subject, NamedOrBlankNode::NamedNode(n) if n.as_str() == subject) {
                if let Some(key) = triple.predicate.as_str().strip_prefix(AC) {
                    props.entry(key).or_default().push(&triple.object);
                }
            }
        }
        if props.contains_key("approval") || props.contains_key("approved") {
            return Err("CONTRACT_APPROVAL: publisher cannot approve a source".into());
        }
        if let Some(values) = props.get("authRef") {
            if values.len() != 1
                || !matches!(values[0], Term::Literal(v) if valid_auth_ref(v.value()))
            {
                return Err("CONTRACT_SECRET: source credentials must be references".into());
            }
        }
        let field = |key: &str| -> Result<String, String> {
            match props.get(key).map(Vec::as_slice) {
                Some([Term::Literal(value)]) if !value.value().is_empty() => {
                    Ok(value.value().to_string())
                }
                Some([Term::NamedNode(value)]) => Ok(value.as_str().to_string()),
                _ => Err(format!("MALFORMED_BODY: access contract needs one {key}")),
            }
        };
        let source_kind = match field("accessKind")?.as_str() {
            "mcp_tool" => PackSourceKind::McpTool,
            "http_endpoint" => PackSourceKind::HttpEndpoint,
            "graphql_query" => PackSourceKind::GraphqlQuery,
            "a2a_skill" => PackSourceKind::A2aSkill,
            "virtual_graph" => PackSourceKind::VirtualGraph,
            _ => return Err("MALFORMED_BODY: unknown access kind".into()),
        };
        out.push(PackSourceRegistration {
            connector: connector.clone(),
            contract_iri: subject,
            resource_uri: resource_uri.to_string(),
            source_kind,
            approval: PackSourceApproval::Held,
            class_iri: field("classIri")?,
            source_id: field("sourceId")?,
            operation: field("operation")?,
            entity: field("entity")?,
            key_field: field("keyField")?,
        });
    }
    Ok(out)
}

fn valid_auth_ref(value: &str) -> bool {
    if let Some(name) = value.strip_prefix("env://") {
        return !name.is_empty()
            && name.bytes().enumerate().all(|(i, b)| {
                b == b'_' || b.is_ascii_alphabetic() || (i > 0 && b.is_ascii_digit())
            });
    }
    value.strip_prefix("openbao://").is_some_and(|reference| {
        reference.split_once('#').is_some_and(|(path, field)| {
            path.split_once('/')
                .is_some_and(|(mount, path)| !mount.is_empty() && !path.is_empty())
                && !field.is_empty()
                && reference
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"_-/.*#".contains(&b))
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    fn document(extra: &str) -> String {
        format!(
            r#"@prefix ac: <{AC}> . <urn:contract> a ac:AccessContract ; ac:classIri <urn:Supplier> ; ac:sourceId "suppliers" ; ac:accessKind "mcp_tool" ; ac:operation "list_suppliers" ; ac:entity "Supplier" ; ac:keyField "id" {extra} ."#
        )
    }
    // spec: MEDULLA-FEDERATED-QUERY-R722.1
    #[test]
    fn imported_contract_is_held_and_identifies_its_authoritative_body() {
        let rows = declared_sources(
            &ResourceId::new("erpnext").unwrap(),
            "urn:ontology",
            &document(""),
        )
        .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].approval, PackSourceApproval::Held);
        assert_eq!(rows[0].contract_iri, "urn:contract");
        assert_eq!(rows[0].resource_uri, "urn:ontology");
        assert_eq!(rows[0].operation, "list_suppliers");
        assert!(declared_sources(
            &ResourceId::new("erpnext").unwrap(),
            "urn:ontology",
            "<urn:a> <urn:b> <urn:c> ."
        )
        .unwrap()
        .is_empty());
    }
    // spec: MEDULLA-FEDERATED-QUERY-R722.4
    #[test]
    fn publisher_approval_and_literal_credentials_refuse() {
        let connector = ResourceId::new("erpnext").unwrap();
        assert!(
            declared_sources(&connector, "urn:ontology", &document("; ac:approved true"))
                .unwrap_err()
                .starts_with("CONTRACT_APPROVAL")
        );
        assert!(declared_sources(
            &connector,
            "urn:ontology",
            &document("; ac:authRef \"secret\"")
        )
        .unwrap_err()
        .starts_with("CONTRACT_SECRET"));
    }
}
