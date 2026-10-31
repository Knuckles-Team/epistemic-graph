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
    write_rdfxml_header(&mut writer, &prefixes)?;

    for (triple, (namespace, local)) in triples.iter().zip(predicates) {
        write_rdfxml_description(&mut writer, triple, &namespace, &local, &prefixes)?;
    }

    writer
        .write_event(Event::End(BytesEnd::new("rdf:RDF")))
        .map_err(xml_write_error)?;
    String::from_utf8(writer.into_inner()).map_err(|error| format!("rdfxml utf8: {error}"))
}

/// Write the `<?xml?>` decl + `<rdf:RDF>` root start tag with its `xmlns:` prefix
/// bindings. Extracted from [`serialize`]'s header emission.
fn write_rdfxml_header(
    writer: &mut Writer<Vec<u8>>,
    prefixes: &BTreeMap<String, String>,
) -> Result<(), String> {
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
        .map_err(xml_write_error)
}

/// Write one `<rdf:Description>` element for `triple`. Extracted from [`serialize`]'s
/// per-triple loop.
fn write_rdfxml_description(
    writer: &mut Writer<Vec<u8>>,
    triple: &Triple,
    namespace: &str,
    local: &str,
    prefixes: &BTreeMap<String, String>,
) -> Result<(), String> {
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
                .get(namespace)
                .ok_or_else(|| format!("rdfxml: namespace not declared: {namespace}"))?
        )
    };
    write_rdfxml_object(writer, &qname, &triple.object)?;

    writer
        .write_event(Event::End(BytesEnd::new("rdf:Description")))
        .map_err(xml_write_error)
}

/// Write the property element for a triple's object under `qname`. Extracted from
/// [`write_rdfxml_description`].
fn write_rdfxml_object(
    writer: &mut Writer<Vec<u8>>,
    qname: &str,
    object: &Term,
) -> Result<(), String> {
    match object {
        Term::NamedNode(node) => {
            let mut property = BytesStart::new(qname);
            property.push_attribute(("rdf:resource", node.as_str()));
            writer
                .write_event(Event::Empty(property))
                .map_err(xml_write_error)
        }
        Term::BlankNode(node) => {
            let mut property = BytesStart::new(qname);
            property.push_attribute(("rdf:nodeID", node.as_str()));
            writer
                .write_event(Event::Empty(property))
                .map_err(xml_write_error)
        }
        Term::Literal(literal) => {
            let mut property = BytesStart::new(qname);
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
                .write_event(Event::End(BytesEnd::new(qname)))
                .map_err(xml_write_error)
        }
        #[allow(unreachable_patterns)]
        _ => Err("rdfxml: RDF-star quoted triple objects have no RDF/XML encoding".into()),
    }
}

/// Mutable state threaded through [`parse`]'s per-event state machine: the current
/// nesting depth relative to `<rdf:RDF>`, the in-progress subject/property (when inside
/// a node/property element), and the triples produced so far.
struct ParseState {
    depth: usize,
    subject: Option<NamedOrBlankNode>,
    property: Option<Property>,
    triples: Vec<Triple>,
}

pub(crate) fn parse(document: &str) -> Result<Vec<Triple>, String> {
    let mut reader = NsReader::from_str(document);
    reader.config_mut().trim_text(false);
    let mut state = ParseState {
        depth: 0,
        subject: None,
        property: None,
        triples: Vec::new(),
    };

    loop {
        let event = reader
            .read_event()
            .map_err(|error| format!("rdfxml parse: {error}"))?;
        if matches!(event, Event::Eof) {
            break;
        }
        handle_rdfxml_event(&reader, &mut state, event)?;
    }
    if state.depth != 0 || state.subject.is_some() || state.property.is_some() {
        return Err("rdfxml: truncated document".into());
    }
    Ok(state.triples)
}

/// Dispatch one XML event against the parser state. Extracted from [`parse`]'s loop.
fn handle_rdfxml_event(
    reader: &NsReader<&[u8]>,
    state: &mut ParseState,
    event: Event<'_>,
) -> Result<(), String> {
    match event {
        Event::Start(element) => handle_rdfxml_start(reader, state, &element)?,
        Event::Empty(element) => handle_rdfxml_empty(reader, state, &element)?,
        Event::Text(text) if state.depth == 3 => {
            let value = text
                .decode()
                .map_err(|error| format!("rdfxml text: {error}"))?;
            state
                .property
                .as_mut()
                .ok_or_else(|| "rdfxml: text outside a property".to_string())?
                .text
                .push_str(&value);
        }
        Event::CData(text) if state.depth == 3 => {
            let value = text
                .decode()
                .map_err(|error| format!("rdfxml cdata: {error}"))?;
            state
                .property
                .as_mut()
                .ok_or_else(|| "rdfxml: CDATA outside a property".to_string())?
                .text
                .push_str(&value);
        }
        Event::GeneralRef(reference) if state.depth == 3 => {
            let value = decode_reference(&reference)?;
            state
                .property
                .as_mut()
                .ok_or_else(|| "rdfxml: entity outside a property".to_string())?
                .text
                .push_str(&value);
        }
        Event::End(_) => handle_rdfxml_end(state)?,
        Event::DocType(_) => return Err("rdfxml: DTDs and external entities are forbidden".into()),
        _ => {}
    }
    Ok(())
}

/// The `Event::Start` arm of [`handle_rdfxml_event`]: root validation at depth 0, a new
/// subject node at depth 1, a new property at depth 2; deeper nesting is rejected. Bumps
/// `state.depth` on success.
fn handle_rdfxml_start(
    reader: &NsReader<&[u8]>,
    state: &mut ParseState,
    element: &BytesStart<'_>,
) -> Result<(), String> {
    match state.depth {
        0 => require_rdf_root(reader.resolver(), element.name())?,
        1 => {
            let (node, node_type) = parse_node(reader, element)?;
            if let Some(node_type) = node_type {
                state.triples.push(Triple::new(
                    node.clone(),
                    NamedNode::new(format!("{RDF_NS}type"))
                        .map_err(|error| format!("rdfxml rdf:type: {error}"))?,
                    node_type,
                ));
            }
            state.subject = Some(node);
        }
        2 => {
            // Resource-valued properties are still finalized on End, ensuring
            // malformed mixed resource/text content is rejected.
            if state.subject.is_none() {
                return Err("rdfxml: property without a subject".to_string());
            }
            state.property = Some(parse_property(reader, element)?);
        }
        _ => return Err("rdfxml: nested parseType/resource nodes are not supported".into()),
    }
    state.depth += 1;
    Ok(())
}

/// The `Event::Empty` arm of [`handle_rdfxml_event`]: a self-closing subject node at
/// depth 1, or a self-closing (resource-valued) property at depth 2.
fn handle_rdfxml_empty(
    reader: &NsReader<&[u8]>,
    state: &mut ParseState,
    element: &BytesStart<'_>,
) -> Result<(), String> {
    match state.depth {
        1 => {
            let (node, node_type) = parse_node(reader, element)?;
            if let Some(node_type) = node_type {
                state.triples.push(Triple::new(
                    node,
                    NamedNode::new(format!("{RDF_NS}type"))
                        .map_err(|error| format!("rdfxml rdf:type: {error}"))?,
                    node_type,
                ));
            }
            Ok(())
        }
        2 => {
            let current = state
                .subject
                .as_ref()
                .ok_or_else(|| "rdfxml: property without a subject".to_string())?;
            let value = parse_property(reader, element)?;
            state.triples.push(value.finish(current.clone())?);
            Ok(())
        }
        _ => Err("rdfxml: unexpected empty element".into()),
    }
}

/// The `Event::End` arm of [`handle_rdfxml_event`]: closing a property (depth 3→2, emit
/// its finished triple) or a subject node (depth 2→1, clear it). Decrements
/// `state.depth`.
fn handle_rdfxml_end(state: &mut ParseState) -> Result<(), String> {
    if state.depth == 0 {
        return Err("rdfxml: unmatched closing element".into());
    }
    state.depth -= 1;
    if state.depth == 2 {
        let current = state
            .subject
            .as_ref()
            .ok_or_else(|| "rdfxml: property without a subject".to_string())?;
        let value = state
            .property
            .take()
            .ok_or_else(|| "rdfxml: closing property without state".to_string())?;
        state.triples.push(value.finish(current.clone())?);
    } else if state.depth == 1 {
        state.subject = None;
    }
    Ok(())
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
    let subject = resolve_node_subject(about, node_id)?;
    let node_type = resolve_node_type(namespace, &local)?;
    Ok((subject, node_type))
}

/// Resolve a node's subject from its `rdf:about`/`rdf:nodeID` attributes (mutually
/// exclusive; neither present ⇒ a fresh blank node). Extracted from [`parse_node`].
fn resolve_node_subject(
    about: Option<String>,
    node_id: Option<String>,
) -> Result<NamedOrBlankNode, String> {
    match (about, node_id) {
        (Some(_), Some(_)) => Err("rdfxml: node has both rdf:about and rdf:nodeID".into()),
        (Some(iri), None) => NamedNode::new(iri)
            .map(NamedOrBlankNode::NamedNode)
            .map_err(|error| format!("rdfxml subject IRI: {error}")),
        (None, Some(id)) => BlankNode::new(id)
            .map(NamedOrBlankNode::BlankNode)
            .map_err(|error| format!("rdfxml blank node: {error}")),
        (None, None) => Ok(NamedOrBlankNode::BlankNode(BlankNode::default())),
    }
}

/// Resolve a node's `rdf:type` from its element name — `None` for the generic
/// `rdf:Description`, else the qualified type IRI. Extracted from [`parse_node`].
fn resolve_node_type(namespace: Option<String>, local: &str) -> Result<Option<Term>, String> {
    if namespace.as_deref() == Some(RDF_NS) && local == "Description" {
        return Ok(None);
    }
    let namespace = namespace.ok_or_else(|| "rdfxml: typed node has no namespace".to_string())?;
    Ok(Some(
        NamedNode::new(format!("{namespace}{local}"))
            .map(Term::NamedNode)
            .map_err(|error| format!("rdfxml node type: {error}"))?,
    ))
}

/// The rdf:/xml:-namespaced attributes recognized on a property element. Bundles what
/// [`parse_property`]'s attribute scan collects (one typed object rather than four
/// threaded values).
#[derive(Default)]
struct PropertyAttrs {
    resource: Option<String>,
    node_id: Option<String>,
    datatype: Option<String>,
    language: Option<String>,
}

fn parse_property(reader: &NsReader<&[u8]>, element: &BytesStart<'_>) -> Result<Property, String> {
    let (namespace, local) = expanded_element(reader.resolver(), element.name())?;
    let namespace = namespace.ok_or_else(|| "rdfxml: property has no namespace".to_string())?;
    let predicate = NamedNode::new(format!("{namespace}{local}"))
        .map_err(|error| format!("rdfxml predicate: {error}"))?;
    let attrs = scan_property_attributes(reader, element)?;
    if attrs.resource.is_some() && attrs.node_id.is_some() {
        return Err("rdfxml: property has both rdf:resource and rdf:nodeID".into());
    }
    if attrs.datatype.is_some() && attrs.language.is_some() {
        return Err("rdfxml: property has both rdf:datatype and xml:lang".into());
    }
    let object = resolve_property_object(attrs.resource, attrs.node_id)?;
    let datatype = attrs
        .datatype
        .map(NamedNode::new)
        .transpose()
        .map_err(|error| format!("rdfxml datatype: {error}"))?;
    Ok(Property {
        predicate,
        object,
        datatype,
        language: attrs.language,
        text: String::new(),
    })
}

/// Scan a property element's `rdf:resource`/`rdf:nodeID`/`rdf:datatype`/`xml:lang`
/// attributes. Extracted from [`parse_property`].
fn scan_property_attributes(
    reader: &NsReader<&[u8]>,
    element: &BytesStart<'_>,
) -> Result<PropertyAttrs, String> {
    let mut attrs = PropertyAttrs::default();
    for (attr_namespace, attr_local, value) in attributes(reader, element)? {
        match (attr_namespace.as_deref(), attr_local.as_str()) {
            (Some(RDF_NS), "resource") => attrs.resource = Some(value),
            (Some(RDF_NS), "nodeID") => attrs.node_id = Some(value),
            (Some(RDF_NS), "datatype") => attrs.datatype = Some(value),
            (Some(XML_NS), "lang") => attrs.language = Some(value),
            _ => {}
        }
    }
    Ok(attrs)
}

/// Resolve a property's object term from its `rdf:resource`/`rdf:nodeID` attributes
/// (mutually exclusive, already checked by the caller); neither present ⇒ a literal
/// property (its object is resolved later, from text content). Extracted from
/// [`parse_property`].
fn resolve_property_object(
    resource: Option<String>,
    node_id: Option<String>,
) -> Result<Option<Term>, String> {
    match (resource, node_id) {
        (Some(iri), None) => Ok(Some(
            NamedNode::new(iri)
                .map(Term::NamedNode)
                .map_err(|error| format!("rdfxml object IRI: {error}"))?,
        )),
        (None, Some(id)) => {
            Ok(Some(BlankNode::new(id).map(Term::BlankNode).map_err(
                |error| format!("rdfxml object blank node: {error}"),
            )?))
        }
        (None, None) => Ok(None),
        (Some(_), Some(_)) => unreachable!("checked above"),
    }
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
