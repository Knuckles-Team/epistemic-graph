//! SHACL / RDF / XSD IRI constants (CONCEPT:EG-KG.ontology.concept-6).
//!
//! Plain `&str` IRIs; the parser wraps them in `oxrdf::NamedNodeRef` on demand. Keeping
//! them as bare strings (not `NamedNode`) means the vocab table allocates nothing.

/// The SHACL namespace.
pub const SH: &str = "http://www.w3.org/ns/shacl#";
/// The RDF namespace.
pub const RDF: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
/// The XSD namespace.
pub const XSD: &str = "http://www.w3.org/2001/XMLSchema#";

macro_rules! sh {
    ($name:ident, $local:expr) => {
        pub const $name: &str = concat!("http://www.w3.org/ns/shacl#", $local);
    };
}

// Shape types.
sh!(NODE_SHAPE, "NodeShape");
sh!(PROPERTY_SHAPE, "PropertyShape");

// Targets.
sh!(TARGET_CLASS, "targetClass");
sh!(TARGET_NODE, "targetNode");
sh!(TARGET_SUBJECTS_OF, "targetSubjectsOf");
sh!(TARGET_OBJECTS_OF, "targetObjectsOf");

// Structure.
sh!(PATH, "path");
sh!(PROPERTY, "property");
sh!(NODE, "node");
sh!(DEACTIVATED, "deactivated");
sh!(SEVERITY, "severity");
sh!(MESSAGE, "message");

// Severities.
sh!(VIOLATION, "Violation");
sh!(WARNING, "Warning");
sh!(INFO, "Info");

// Cardinality.
sh!(MIN_COUNT, "minCount");
sh!(MAX_COUNT, "maxCount");

// Value type.
sh!(DATATYPE, "datatype");
sh!(CLASS, "class");
sh!(NODE_KIND, "nodeKind");

// Node kinds.
sh!(K_BLANK_NODE, "BlankNode");
sh!(K_IRI, "IRI");
sh!(K_LITERAL, "Literal");
sh!(K_BLANK_NODE_OR_IRI, "BlankNodeOrIRI");
sh!(K_BLANK_NODE_OR_LITERAL, "BlankNodeOrLiteral");
sh!(K_IRI_OR_LITERAL, "IRIOrLiteral");

// Value range.
sh!(MIN_INCLUSIVE, "minInclusive");
sh!(MAX_INCLUSIVE, "maxInclusive");
sh!(MIN_EXCLUSIVE, "minExclusive");
sh!(MAX_EXCLUSIVE, "maxExclusive");

// String.
sh!(MIN_LENGTH, "minLength");
sh!(MAX_LENGTH, "maxLength");
sh!(PATTERN, "pattern");
sh!(FLAGS, "flags");
sh!(LANGUAGE_IN, "languageIn");

// Value / membership.
sh!(IN, "in");
sh!(HAS_VALUE, "hasValue");

// Logical.
sh!(AND, "and");
sh!(OR, "or");
sh!(NOT, "not");
sh!(XONE, "xone");

// SPARQL-based constraints (parsed to a deferred marker — see EG-132 follow-ups).
sh!(SPARQL, "sparql");
sh!(SELECT, "select");

// Constraint-component IRIs reported in a `sh:ValidationResult`.
sh!(CC_MIN_COUNT, "MinCountConstraintComponent");
sh!(CC_MAX_COUNT, "MaxCountConstraintComponent");
sh!(CC_DATATYPE, "DatatypeConstraintComponent");
sh!(CC_CLASS, "ClassConstraintComponent");
sh!(CC_NODE_KIND, "NodeKindConstraintComponent");
sh!(CC_MIN_INCLUSIVE, "MinInclusiveConstraintComponent");
sh!(CC_MAX_INCLUSIVE, "MaxInclusiveConstraintComponent");
sh!(CC_MIN_EXCLUSIVE, "MinExclusiveConstraintComponent");
sh!(CC_MAX_EXCLUSIVE, "MaxExclusiveConstraintComponent");
sh!(CC_MIN_LENGTH, "MinLengthConstraintComponent");
sh!(CC_MAX_LENGTH, "MaxLengthConstraintComponent");
sh!(CC_PATTERN, "PatternConstraintComponent");
sh!(CC_LANGUAGE_IN, "LanguageInConstraintComponent");
sh!(CC_IN, "InConstraintComponent");
sh!(CC_HAS_VALUE, "HasValueConstraintComponent");
sh!(CC_AND, "AndConstraintComponent");
sh!(CC_OR, "OrConstraintComponent");
sh!(CC_NOT, "NotConstraintComponent");
sh!(CC_XONE, "XoneConstraintComponent");
sh!(CC_NODE, "NodeConstraintComponent");

// RDF list + typing.
pub const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
pub const RDF_FIRST: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#first";
pub const RDF_REST: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#rest";
pub const RDF_NIL: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#nil";
pub const RDF_LANG_STRING: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString";
pub const XSD_STRING: &str = "http://www.w3.org/2001/XMLSchema#string";
