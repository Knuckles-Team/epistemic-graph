//! Bounded connect-time seed graph preparation (MEDULLA-TYPED-PACKS-R104.2).
//!
//! This module owns the pure validation and per-part replacement plan.  The
//! ConnectorPack import handler and projection worker remain the integration
//! owners (R104.3); keeping this step independent means a rejected seed never
//! reaches either commit authority.

use std::collections::{BTreeMap, BTreeSet};

use medulla_rdf::oxrdf::{BlankNode, Graph, NamedOrBlankNode, Term, Triple};
use medulla_types::connector_pack::{SeedGraphImport, SeedPart, SeedPartKey};
use medulla_types::contract::Digest256;

/// Maximum number of triples emitted by one seed part parser.
pub(crate) const MAX_SEED_TRIPLES: usize = 50_000;
/// Maximum UTF-8 byte size of all seed parts in one import.
pub(crate) const MAX_SEED_BYTES: usize = 8 * 1024 * 1024;

/// The canonical representation held by the later persistence/projection step.
/// It deliberately keeps only the canonical N-Quads and digest; no caller-owned
/// Turtle bytes can become a second graph representation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SeedPartState {
    pub(crate) digest: Digest256,
    pub(crate) canonical_nquads: String,
    pub(crate) triple_count: usize,
}

/// The current seed state used to make a partial replacement plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SeedGraphState {
    pub(crate) content_digest: Digest256,
    pub(crate) parts: BTreeMap<SeedPartKey, SeedPartState>,
}

/// What the one eventual commit must do with the seed parts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SeedImportOutcome {
    /// The caller supplied the already-visible content digest.
    Unchanged,
    /// No prior seed state exists; all supplied parts are new.
    Inserted,
    /// Only the listed parts differ from the current state.
    Replaced {
        changed_parts: BTreeSet<SeedPartKey>,
    },
}

/// A validated, atomic replacement plan.  R104.3 supplies the durable commit
/// around this plan; this module never mutates graph state itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SeedImportPlan {
    pub(crate) outcome: SeedImportOutcome,
    pub(crate) state: SeedGraphState,
}

/// Parse, validate and prepare one seed graph against the composed SHACL graph.
///
/// The parser and validator run before the returned state is exposed to a
/// caller.  A changed import copies unchanged parts from `current`, so a
/// later commit can replace one part without rewriting the other two.  The
/// content digest is compared after parsing so a reused digest cannot bypass
/// Turtle or SHACL validation.
pub(crate) fn prepare_seed_import(
    seed: &SeedGraphImport,
    current: Option<&SeedGraphState>,
    shapes: &Graph,
) -> Result<SeedImportPlan, String> {
    let total_bytes = seed.parts.values().try_fold(0usize, |total, part| {
        total
            .checked_add(part.turtle.len())
            .ok_or_else(|| "SEED_TOO_LARGE: seed byte size overflow".to_string())
    })?;
    if total_bytes > MAX_SEED_BYTES {
        return Err(format!(
            "SEED_TOO_LARGE: seed graph exceeds {MAX_SEED_BYTES} bytes"
        ));
    }

    let mut parts = current.map(|state| state.parts.clone()).unwrap_or_default();
    let mut changed_parts = BTreeSet::new();
    let mut total_triples = 0usize;
    for (key, part) in &seed.parts {
        if part.key != *key {
            return Err(format!(
                "SEED_INVALID: map key {:?} does not match part key {:?}",
                key, part.key
            ));
        }
        let validated = validate_seed_part(part, shapes)?;
        total_triples = total_triples
            .checked_add(validated.triple_count)
            .ok_or_else(|| "SEED_TOO_LARGE: seed triple count overflow".to_string())?;
        if total_triples > MAX_SEED_TRIPLES {
            return Err(format!(
                "SEED_TOO_LARGE: seed graph exceeds {MAX_SEED_TRIPLES} triples"
            ));
        }
        if parts.get(key) != Some(&validated) {
            changed_parts.insert(*key);
            parts.insert(*key, validated);
        }
    }

    let outcome = match current {
        None => SeedImportOutcome::Inserted,
        Some(state) if changed_parts.is_empty() && state.content_digest == seed.content_digest => {
            return Ok(SeedImportPlan {
                outcome: SeedImportOutcome::Unchanged,
                state: state.clone(),
            });
        }
        Some(state) if changed_parts.is_empty() => {
            return Err(format!(
                "SEED_DIGEST_MISMATCH: content digest {} has no changed part relative to {}",
                seed.content_digest, state.content_digest
            ));
        }
        Some(state) if state.content_digest == seed.content_digest => {
            return Err(format!(
                "SEED_DIGEST_MISMATCH: content digest {} has changed parts",
                seed.content_digest
            ));
        }
        Some(_) => SeedImportOutcome::Replaced { changed_parts },
    };
    Ok(SeedImportPlan {
        outcome,
        state: SeedGraphState {
            content_digest: seed.content_digest,
            parts,
        },
    })
}

/// Compute the digest expected in [`SeedPart::digest`] from a Turtle body.
/// This is exposed to the focused handler test and keeps the exact digest rule
/// in one production function used by validation.
pub(crate) fn seed_turtle_digest(turtle: &str) -> Result<Digest256, String> {
    let triples = medulla_rdf::mapping::parse_turtle_bounded(turtle, MAX_SEED_TRIPLES)
        .map_err(|error| format!("SEED_TURTLE_INVALID: {error}"))?;
    let canonical = canonical_nquads(&triples)?;
    Ok(Digest256::sha256(canonical.as_bytes()))
}

fn validate_seed_part(part: &SeedPart, shapes: &Graph) -> Result<SeedPartState, String> {
    let triples = medulla_rdf::mapping::parse_turtle_bounded(&part.turtle, MAX_SEED_TRIPLES)
        .map_err(|error| format!("SEED_TURTLE_INVALID: {error}"))?;
    let mut data = Graph::new();
    for triple in &triples {
        data.insert(triple);
    }
    let report = medulla_shacl::validate(shapes, &data)
        .map_err(|error| format!("SEED_SHACL_VIOLATION: {error}"))?;
    if !report.conforms {
        let detail = report
            .results
            .first()
            .map(|result| {
                format!(
                    "focus {}: {}",
                    result.focus_node,
                    result
                        .message
                        .as_deref()
                        .unwrap_or("shape constraint failed")
                )
            })
            .unwrap_or_else(|| "shape constraint failed".to_string());
        return Err(format!("SEED_SHACL_VIOLATION: {detail}"));
    }
    let canonical_nquads = canonical_nquads(&triples)?;
    let digest = Digest256::sha256(canonical_nquads.as_bytes());
    if digest != part.digest {
        return Err(format!(
            "SEED_DIGEST_MISMATCH: {:?} declares {}, computed {}",
            part.key, part.digest, digest
        ));
    }
    Ok(SeedPartState {
        digest,
        canonical_nquads,
        triple_count: triples.len(),
    })
}

/// Serialize a triple set as sorted, duplicate-free N-Quads with stable local
/// blank-node labels.  The sorted lines make source statement order irrelevant;
/// labels are normalized per document before serialization.
fn canonical_nquads(triples: &[Triple]) -> Result<String, String> {
    let labels = blank_node_labels(triples);
    let mut lines = BTreeSet::new();
    for triple in triples {
        let triple = remap_triple(triple, &labels);
        lines.insert(medulla_rdf::mapping::to_nquads(&[triple], None)?);
    }
    Ok(lines.into_iter().collect::<Vec<_>>().concat())
}

fn blank_node_labels(triples: &[Triple]) -> BTreeMap<String, String> {
    let mut source_labels = BTreeSet::new();
    for triple in triples {
        if let NamedOrBlankNode::BlankNode(node) = &triple.subject {
            source_labels.insert(node.as_str().to_string());
        }
        collect_term_labels(&triple.object, &mut source_labels);
    }
    source_labels
        .into_iter()
        .enumerate()
        .map(|(index, label)| (label, format!("b{index}")))
        .collect()
}

fn collect_term_labels(term: &Term, labels: &mut BTreeSet<String>) {
    match term {
        Term::BlankNode(node) => {
            labels.insert(node.as_str().to_string());
        }
        #[cfg(feature = "sparql-star")]
        Term::Triple(triple) => {
            if let NamedOrBlankNode::BlankNode(node) = &triple.subject {
                labels.insert(node.as_str().to_string());
            }
            collect_term_labels(&triple.object, labels);
        }
        _ => {}
    }
}

fn remap_triple(triple: &Triple, labels: &BTreeMap<String, String>) -> Triple {
    Triple::new(
        remap_subject(&triple.subject, labels),
        triple.predicate.clone(),
        remap_term(&triple.object, labels),
    )
}

fn remap_subject(
    subject: &NamedOrBlankNode,
    labels: &BTreeMap<String, String>,
) -> NamedOrBlankNode {
    match subject {
        NamedOrBlankNode::NamedNode(node) => NamedOrBlankNode::NamedNode(node.clone()),
        NamedOrBlankNode::BlankNode(node) => NamedOrBlankNode::BlankNode(BlankNode::new_unchecked(
            labels
                .get(node.as_str())
                .cloned()
                .unwrap_or_else(|| node.as_str().to_string()),
        )),
        #[allow(unreachable_patterns)]
        _ => subject.clone(),
    }
}

fn remap_term(term: &Term, labels: &BTreeMap<String, String>) -> Term {
    match term {
        Term::NamedNode(node) => Term::NamedNode(node.clone()),
        Term::BlankNode(node) => Term::BlankNode(BlankNode::new_unchecked(
            labels
                .get(node.as_str())
                .cloned()
                .unwrap_or_else(|| node.as_str().to_string()),
        )),
        Term::Literal(literal) => Term::Literal(literal.clone()),
        #[cfg(feature = "sparql-star")]
        Term::Triple(triple) => Term::Triple(Box::new(remap_triple(triple, labels))),
        #[allow(unreachable_patterns)]
        _ => term.clone(),
    }
}
