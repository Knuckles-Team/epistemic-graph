//! RDF/XML codec over maintained `quick-xml`.
//!
//! The former `oxrdfxml` dependency pinned quick-xml below the security-fixed
//! line. This codec keeps the public `oxrdf::Triple` surface and supports the
//! interoperable RDF/XML core: URI/blank subjects, typed node elements,
//! resource/blank/literal properties, datatypes, and language tags. DTDs are
//! rejected instead of expanded, so untrusted documents cannot trigger XXE.

use std::collections::{BTreeMap, BTreeSet};

use oxrdf::{BlankNode, Literal, NamedNode, NamedOrBlankNode, Term, Triple};
use quick_xml::events::{BytesDecl, BytesEnd, BytesStart, BytesText, Event};
use quick_xml::name::{NamespaceResolver, QName, ResolveResult};
use quick_xml::{NsReader, Writer, XmlVersion};

const RDF_NS: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
const XML_NS: &str = "http://www.w3.org/XML/1998/namespace";
const XSD_STRING: &str = "http://www.w3.org/2001/XMLSchema#string";

pub(crate) fn serialize(triples: &[Triple]) -> Result<String, String> {
    let mut namespaces = BTreeSet::new();
    let mut predicates = Vec::with_capacity(triples.len());
    for triple in triples {
        let (namespace, local) = split_predicate(triple.predicate.as_str())?;
        if namespace != RDF_NS {
            namespaces.insert(namespace.clone());
        }
        predicates.push((namespace, local));
    }
    let prefixes: BTreeMap<String, String> = namespaces
        .into_iter()
        .enumerate()
        .map(|(index, namespace)| (namespace, format!("ns{index}")))
        .collect();

    let mut writer = Writer::new(Vec::new());
    writer
        .write_event(Event::Decl(BytesDecl::new("1.0", Some("UTF-8"), None)))
        .map_err(xml_write_error)?;
    let mut root = BytesStart::new("rdf:RDF");
    root.push_attribute(("xmlns:rdf", RDF_NS));
    let bindings: Vec<(String, String)> = prefixes
        .iter()
        .map(|(namespace, prefix)| (format!("xmlns:{prefix}"), namespace.clone()))
        .collect();
    for (attribute, namespace) in &bindings {
        root.push_attribute((attribute.as_str(), namespace.as_str()));
    }
    writer
        .write_event(Event::Start(root))
        .map_err(xml_write_error)?;

    for (triple, (namespace, local)) in triples.iter().zip(predicates) {
        let mut description = BytesStart::new("rdf:Description");
        match &triple.subject {
            NamedOrBlankNode::NamedNode(node) => {
                description.push_attribute(("rdf:about", node.as_str()));
            }
            NamedOrBlankNode::BlankNode(node) => {
                description.push_attribute(("rdf:nodeID", node.as_str()));
            }
        }
        writer
            .write_event(Event::Start(description))
            .map_err(xml_write_error)?;

        let qname = if namespace == RDF_NS {
            format!("rdf:{local}")
        } else {
            format!(
                "{}:{local}",
                prefixes
                    .get(&namespace)
                    .ok_or_else(|| format!("rdfxml: namespace not declared: {namespace}"))?
            )
        };
        match &triple.object {
            Term::NamedNode(node) => {
                let mut property = BytesStart::new(qname.as_str());
                property.push_attribute(("rdf:resource", node.as_str()));
                writer
                    .write_event(Event::Empty(property))
                    .map_err(xml_write_error)?;
            }
            Term::BlankNode(node) => {
                let mut property = BytesStart::new(qname.as_str());
                property.push_attribute(("rdf:nodeID", node.as_str()));
                writer
                    .write_event(Event::Empty(property))
                    .map_err(xml_write_error)?;
            }
            Term::Literal(literal) => {
                let mut property = BytesStart::new(qname.as_str());
                if let Some(language) = literal.language() {
                    property.push_attribute(("xml:lang", language));
                } else if literal.datatype().as_str() != XSD_STRING {
                    property.push_attribute(("rdf:datatype", literal.datatype().as_str()));
                }
                writer
                    .write_event(Event::Start(property))
                    .map_err(xml_write_error)?;
                writer
                    .write_event(Event::Text(BytesText::new(literal.value())))
                    .map_err(xml_write_error)?;
                writer
                    .write_event(Event::End(BytesEnd::new(qname.as_str())))
                    .map_err(xml_write_error)?;
            }
            #[allow(unreachable_patterns)]
            _ => {
                return Err(
                    "rdfxml: RDF-star quoted triple objects have no RDF/XML encoding".into(),
                )
            }
        }
        writer
            .write_event(Event::End(BytesEnd::new("rdf:Description")))
            .map_err(xml_write_error)?;
    }

    writer
        .write_event(Event::End(BytesEnd::new("rdf:RDF")))
        .map_err(xml_write_error)?;
    String::from_utf8(writer.into_inner()).map_err(|error| format!("rdfxml utf8: {error}"))
}

pub(crate) fn parse(document: &str) -> Result<Vec<Triple>, String> {
    let mut reader = NsReader::from_str(document);
    reader.config_mut().trim_text(false);
    let mut depth = 0usize;
    let mut subject: Option<NamedOrBlankNode> = None;
    let mut property: Option<Property> = None;
    let mut triples = Vec::new();

    loop {
        match reader
            .read_event()
            .map_err(|error| format!("rdfxml parse: {error}"))?
        {
            Event::Start(element) => {
                match depth {
                    0 => require_rdf_root(reader.resolver(), element.name())?,
                    1 => {
                        let (node, node_type) = parse_node(&reader, &element)?;
                        if let Some(node_type) = node_type {
                            triples.push(Triple::new(
                                node.clone(),
                                NamedNode::new(format!("{RDF_NS}type"))
                                    .map_err(|error| format!("rdfxml rdf:type: {error}"))?,
                                node_type,
                            ));
                        }
                        subject = Some(node);
                    }
                    2 => {
                        let current = subject
                            .as_ref()
                            .ok_or_else(|| "rdfxml: property without a subject".to_string())?;
                        property = Some(parse_property(&reader, &element)?);
                        if property
                            .as_ref()
                            .is_some_and(|value| value.object.is_some())
                        {
                            // Resource-valued properties are still finalized on End,
                            // ensuring malformed mixed resource/text content is rejected.
                        }
                        let _ = current;
                    }
                    _ => {
                        return Err(
                            "rdfxml: nested parseType/resource nodes are not supported".into()
                        )
                    }
                }
                depth += 1;
            }
            Event::Empty(element) => match depth {
                1 => {
                    let (node, node_type) = parse_node(&reader, &element)?;
                    if let Some(node_type) = node_type {
                        triples.push(Triple::new(
                            node,
                            NamedNode::new(format!("{RDF_NS}type"))
                                .map_err(|error| format!("rdfxml rdf:type: {error}"))?,
                            node_type,
                        ));
                    }
                }
                2 => {
                    let current = subject
                        .as_ref()
                        .ok_or_else(|| "rdfxml: property without a subject".to_string())?;
                    let value = parse_property(&reader, &element)?;
                    triples.push(value.finish(current.clone())?);
                }
                _ => return Err("rdfxml: unexpected empty element".into()),
            },
            Event::Text(text) if depth == 3 => {
                let value = text
                    .decode()
                    .map_err(|error| format!("rdfxml text: {error}"))?;
                property
                    .as_mut()
                    .ok_or_else(|| "rdfxml: text outside a property".to_string())?
                    .text
                    .push_str(&value);
            }
            Event::CData(text) if depth == 3 => {
                let value = text
                    .decode()
                    .map_err(|error| format!("rdfxml cdata: {error}"))?;
                property
                    .as_mut()
                    .ok_or_else(|| "rdfxml: CDATA outside a property".to_string())?
                    .text
                    .push_str(&value);
            }
            Event::GeneralRef(reference) if depth == 3 => {
                let value = decode_reference(&reference)?;
                property
                    .as_mut()
                    .ok_or_else(|| "rdfxml: entity outside a property".to_string())?
                    .text
                    .push_str(&value);
            }
            Event::End(_) => {
                if depth == 0 {
                    return Err("rdfxml: unmatched closing element".into());
                }
                depth -= 1;
                if depth == 2 {
                    let current = subject
                        .as_ref()
                        .ok_or_else(|| "rdfxml: property without a subject".to_string())?;
                    let value = property
                        .take()
                        .ok_or_else(|| "rdfxml: closing property without state".to_string())?;
                    triples.push(value.finish(current.clone())?);
                } else if depth == 1 {
                    subject = None;
                }
            }
            Event::DocType(_) => {
                return Err("rdfxml: DTDs and external entities are forbidden".into())
            }
            Event::Eof => break,
            _ => {}
        }
    }
    if depth != 0 || subject.is_some() || property.is_some() {
        return Err("rdfxml: truncated document".into());
    }
    Ok(triples)
}

struct Property {
    predicate: NamedNode,
    object: Option<Term>,
    datatype: Option<NamedNode>,
    language: Option<String>,
    text: String,
}

impl Property {
    fn finish(self, subject: NamedOrBlankNode) -> Result<Triple, String> {
        let object = match self.object {
            Some(object) => {
                if !self.text.is_empty() || self.datatype.is_some() || self.language.is_some() {
                    return Err("rdfxml: resource property also contains literal content".into());
                }
                object
            }
            None => {
                let literal = if let Some(language) = self.language {
                    Literal::new_language_tagged_literal(self.text, language)
                        .map_err(|error| format!("rdfxml language tag: {error}"))?
                } else if let Some(datatype) = self.datatype {
                    Literal::new_typed_literal(self.text, datatype)
                } else {
                    Literal::new_simple_literal(self.text)
                };
                literal.into()
            }
        };
        Ok(Triple::new(subject, self.predicate, object))
    }
}

fn require_rdf_root(resolver: &NamespaceResolver, name: QName<'_>) -> Result<(), String> {
    let (namespace, local) = expanded_element(resolver, name)?;
    if namespace.as_deref() == Some(RDF_NS) && local == "RDF" {
        Ok(())
    } else {
        Err("rdfxml: root element must be rdf:RDF".into())
    }
}

fn parse_node(
    reader: &NsReader<&[u8]>,
    element: &BytesStart<'_>,
) -> Result<(NamedOrBlankNode, Option<Term>), String> {
    let (namespace, local) = expanded_element(reader.resolver(), element.name())?;
    let mut about = None;
    let mut node_id = None;
    for (attr_namespace, attr_local, value) in attributes(reader, element)? {
        if attr_namespace.as_deref() == Some(RDF_NS) {
            match attr_local.as_str() {
                "about" => about = Some(value),
                "nodeID" => node_id = Some(value),
                _ => {}
            }
        }
    }
    let subject = match (about, node_id) {
        (Some(_), Some(_)) => return Err("rdfxml: node has both rdf:about and rdf:nodeID".into()),
        (Some(iri), None) => NamedNode::new(iri)
            .map(NamedOrBlankNode::NamedNode)
            .map_err(|error| format!("rdfxml subject IRI: {error}"))?,
        (None, Some(id)) => BlankNode::new(id)
            .map(NamedOrBlankNode::BlankNode)
            .map_err(|error| format!("rdfxml blank node: {error}"))?,
        (None, None) => NamedOrBlankNode::BlankNode(BlankNode::default()),
    };
    let node_type = if namespace.as_deref() == Some(RDF_NS) && local == "Description" {
        None
    } else {
        let namespace =
            namespace.ok_or_else(|| "rdfxml: typed node has no namespace".to_string())?;
        Some(
            NamedNode::new(format!("{namespace}{local}"))
                .map(Term::NamedNode)
                .map_err(|error| format!("rdfxml node type: {error}"))?,
        )
    };
    Ok((subject, node_type))
}

fn parse_property(reader: &NsReader<&[u8]>, element: &BytesStart<'_>) -> Result<Property, String> {
    let (namespace, local) = expanded_element(reader.resolver(), element.name())?;
    let namespace = namespace.ok_or_else(|| "rdfxml: property has no namespace".to_string())?;
    let predicate = NamedNode::new(format!("{namespace}{local}"))
        .map_err(|error| format!("rdfxml predicate: {error}"))?;
    let mut resource = None;
    let mut node_id = None;
    let mut datatype = None;
    let mut language = None;
    for (attr_namespace, attr_local, value) in attributes(reader, element)? {
        match (attr_namespace.as_deref(), attr_local.as_str()) {
            (Some(RDF_NS), "resource") => resource = Some(value),
            (Some(RDF_NS), "nodeID") => node_id = Some(value),
            (Some(RDF_NS), "datatype") => datatype = Some(value),
            (Some(XML_NS), "lang") => language = Some(value),
            _ => {}
        }
    }
    if resource.is_some() && node_id.is_some() {
        return Err("rdfxml: property has both rdf:resource and rdf:nodeID".into());
    }
    if datatype.is_some() && language.is_some() {
        return Err("rdfxml: property has both rdf:datatype and xml:lang".into());
    }
    let object = match (resource, node_id) {
        (Some(iri), None) => Some(
            NamedNode::new(iri)
                .map(Term::NamedNode)
                .map_err(|error| format!("rdfxml object IRI: {error}"))?,
        ),
        (None, Some(id)) => Some(
            BlankNode::new(id)
                .map(Term::BlankNode)
                .map_err(|error| format!("rdfxml object blank node: {error}"))?,
        ),
        (None, None) => None,
        (Some(_), Some(_)) => unreachable!("checked above"),
    };
    let datatype = datatype
        .map(NamedNode::new)
        .transpose()
        .map_err(|error| format!("rdfxml datatype: {error}"))?;
    Ok(Property {
        predicate,
        object,
        datatype,
        language,
        text: String::new(),
    })
}

fn attributes(
    reader: &NsReader<&[u8]>,
    element: &BytesStart<'_>,
) -> Result<Vec<(Option<String>, String, String)>, String> {
    let mut out = Vec::new();
    for attribute in element.attributes().with_checks(true) {
        let attribute = attribute.map_err(|error| format!("rdfxml attribute: {error}"))?;
        if attribute.key.as_ref() == b"xmlns" || attribute.key.as_ref().starts_with(b"xmlns:") {
            continue;
        }
        let (namespace, local) = expanded_attribute(reader.resolver(), attribute.key)?;
        let value = attribute
            .decoded_and_normalized_value(XmlVersion::Explicit1_0, reader.decoder())
            .map_err(|error| format!("rdfxml attribute value: {error}"))?
            .into_owned();
        out.push((namespace, local, value));
    }
    Ok(out)
}

fn expanded_element(
    resolver: &NamespaceResolver,
    name: QName<'_>,
) -> Result<(Option<String>, String), String> {
    let (namespace, local) = resolver.resolve_element(name);
    Ok((resolved_namespace(namespace)?, decode_name(local.as_ref())?))
}

fn expanded_attribute(
    resolver: &NamespaceResolver,
    name: QName<'_>,
) -> Result<(Option<String>, String), String> {
    let (namespace, local) = resolver.resolve_attribute(name);
    Ok((resolved_namespace(namespace)?, decode_name(local.as_ref())?))
}

fn resolved_namespace(result: ResolveResult<'_>) -> Result<Option<String>, String> {
    match result {
        ResolveResult::Unbound => Ok(None),
        ResolveResult::Bound(namespace) => decode_name(namespace.as_ref()).map(Some),
        ResolveResult::Unknown(prefix) => Err(format!(
            "rdfxml: unknown namespace prefix {}",
            String::from_utf8_lossy(&prefix)
        )),
    }
}

fn decode_name(bytes: &[u8]) -> Result<String, String> {
    std::str::from_utf8(bytes)
        .map(str::to_owned)
        .map_err(|error| format!("rdfxml name is not UTF-8: {error}"))
}

fn decode_reference(reference: &quick_xml::events::BytesRef<'_>) -> Result<String, String> {
    if let Some(character) = reference
        .resolve_char_ref()
        .map_err(|error| format!("rdfxml character reference: {error}"))?
    {
        return Ok(character.to_string());
    }
    let name = reference
        .decode()
        .map_err(|error| format!("rdfxml entity reference: {error}"))?;
    match name.as_ref() {
        "amp" => Ok("&".into()),
        "lt" => Ok("<".into()),
        "gt" => Ok(">".into()),
        "apos" => Ok("'".into()),
        "quot" => Ok("\"".into()),
        _ => Err(format!("rdfxml: undeclared entity &{name};")),
    }
}

fn split_predicate(iri: &str) -> Result<(String, String), String> {
    let split = iri
        .char_indices()
        .rev()
        .find(|(_, character)| matches!(character, '#' | '/' | ':'))
        .map(|(index, character)| index + character.len_utf8())
        .ok_or_else(|| format!("rdfxml: predicate cannot be expressed as a QName: {iri}"))?;
    let (namespace, local) = iri.split_at(split);
    if !valid_ncname(local) {
        return Err(format!(
            "rdfxml: predicate local name is not an XML NCName: {iri}"
        ));
    }
    Ok((namespace.to_string(), local.to_string()))
}

fn valid_ncname(value: &str) -> bool {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    (first == '_' || first.is_alphabetic())
        && chars.all(|character| {
            character == '_' || character == '-' || character == '.' || character.is_alphanumeric()
        })
}

fn xml_write_error(error: std::io::Error) -> String {
    format!("rdfxml serialize: {error}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_dtd_and_external_entity_declarations() {
        let document = r#"<?xml version="1.0"?>
<!DOCTYPE rdf:RDF [<!ENTITY xxe SYSTEM "file:///etc/passwd">]>
<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"/>"#;
        let error = parse(document).expect_err("DTD must be rejected before expansion");
        assert!(error.contains("DTD"));
    }

    #[test]
    fn rejects_undeclared_named_entities() {
        let document = r#"<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
            xmlns:ex="https://example.invalid/">
          <rdf:Description rdf:about="https://example.invalid/subject">
            <ex:value>&unknown;</ex:value>
          </rdf:Description>
        </rdf:RDF>"#;
        assert!(parse(document).is_err());
    }
}
