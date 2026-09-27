//! EH-509 / AUD-26: engine-owned, deterministic section-tree retrieval.
//!
//! The caller must supply rows already filtered by EG's tenant/document read
//! authority. This kernel validates their structure and never fetches section
//! bodies or calls an LLM. The server binding must preserve that authorization
//! boundary before the AU `graph_document_tree` route can be retired.

use std::collections::{HashMap, HashSet};

const MAX_SECTIONS: usize = 4_096;
const MAX_DEPTH: usize = 64;
const MAX_TEXT_CHARS: usize = 4_096;
const MAX_RESULTS: usize = 100;

/// One authorized, persisted section metadata row. No body text belongs here.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Section {
    pub document_id: String,
    pub node_id: String,
    pub parent_id: Option<String>,
    pub title: String,
    pub summary: String,
    pub char_start: u64,
    pub char_end: u64,
    pub page_start: Option<u32>,
    pub page_end: Option<u32>,
}

/// A cited section, with its title breadcrumb but without source body text.
#[derive(Clone, Debug, PartialEq)]
pub struct SectionMatch {
    pub node_id: String,
    pub title: String,
    pub score: f64,
    pub char_start: u64,
    pub char_end: u64,
    pub page_start: Option<u32>,
    pub page_end: Option<u32>,
    pub path: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RetrievalError {
    InvalidLimit,
    InvalidSection,
    WrongDocument,
    DuplicateNode,
    InvalidTree,
    TooManySections,
}

type SectionChildren<'a> = HashMap<Option<&'a str>, Vec<&'a Section>>;

/// Rank sections by a bounded lexical beam walk of title and summary metadata.
///
/// This is the EG counterpart of AU's `HierarchicalDocumentRetriever._walk`.
/// `sections` must be the complete *authorized* tree of exactly `document_id`.
/// A malformed or mixed-document tree fails closed instead of returning partial
/// evidence. `beam_width` is per sibling group; all candidates on kept paths
/// compete for the final `top_k` places. Ties use document order and node ID.
pub fn retrieve_sections(
    document_id: &str,
    sections: &[Section],
    query: &str,
    top_k: usize,
    beam_width: usize,
) -> Result<Vec<SectionMatch>, RetrievalError> {
    validate_request(document_id, sections.len(), top_k, beam_width)?;
    let children = validate_sections(document_id, sections)?;
    let query_terms = terms(query);
    let Some(roots) = children.get(&None) else {
        return Ok(Vec::new());
    };
    if query_terms.is_empty() {
        return Ok(Vec::new());
    }
    Ok(rank_sections(
        roots,
        &children,
        &query_terms,
        top_k,
        beam_width,
    ))
}

fn validate_request(
    document_id: &str,
    section_count: usize,
    top_k: usize,
    beam_width: usize,
) -> Result<(), RetrievalError> {
    if document_id.is_empty() || top_k == 0 || top_k > MAX_RESULTS || beam_width == 0 {
        return Err(RetrievalError::InvalidLimit);
    }
    if section_count > MAX_SECTIONS {
        return Err(RetrievalError::TooManySections);
    }
    Ok(())
}

fn validate_sections<'a>(
    document_id: &str,
    sections: &'a [Section],
) -> Result<SectionChildren<'a>, RetrievalError> {
    let (by_id, children) = index_sections(document_id, sections)?;
    validate_parent_ranges(sections, &by_id)?;
    if sections.is_empty() {
        return Ok(children);
    }
    let roots = children.get(&None).ok_or(RetrievalError::InvalidTree)?;
    let mut visited = HashSet::with_capacity(sections.len());
    validate_tree(roots, &children, &mut visited, 0)?;
    if visited.len() != sections.len() {
        return Err(RetrievalError::InvalidTree);
    }
    Ok(children)
}

fn index_sections<'a>(
    document_id: &str,
    sections: &'a [Section],
) -> Result<(HashMap<&'a str, &'a Section>, SectionChildren<'a>), RetrievalError> {
    let mut by_id = HashMap::with_capacity(sections.len());
    let mut children: SectionChildren<'a> = HashMap::new();
    for section in sections {
        if section.document_id != document_id {
            return Err(RetrievalError::WrongDocument);
        }
        if !valid_section(section) {
            return Err(RetrievalError::InvalidSection);
        }
        if by_id.insert(section.node_id.as_str(), section).is_some() {
            return Err(RetrievalError::DuplicateNode);
        }
        children
            .entry(section.parent_id.as_deref())
            .or_default()
            .push(section);
    }
    Ok((by_id, children))
}

fn validate_parent_ranges(
    sections: &[Section],
    by_id: &HashMap<&str, &Section>,
) -> Result<(), RetrievalError> {
    for section in sections {
        if let Some(parent_id) = section.parent_id.as_deref() {
            let Some(parent) = by_id.get(parent_id) else {
                return Err(RetrievalError::InvalidTree);
            };
            if parent.char_start > section.char_start || section.char_end > parent.char_end {
                return Err(RetrievalError::InvalidTree);
            }
        }
    }
    Ok(())
}

fn valid_section(section: &Section) -> bool {
    !section.node_id.is_empty()
        && !section.title.trim().is_empty()
        && section.title.chars().count() <= MAX_TEXT_CHARS
        && section.summary.chars().count() <= MAX_TEXT_CHARS
        && section.char_end > section.char_start
        && section.page_start.is_some() == section.page_end.is_some()
        && section
            .page_start
            .zip(section.page_end)
            .is_none_or(|(start, end)| start <= end)
}

fn rank_sections(
    roots: &[&Section],
    children: &SectionChildren<'_>,
    terms: &HashSet<String>,
    top_k: usize,
    beam_width: usize,
) -> Vec<SectionMatch> {
    let mut candidates = Vec::new();
    walk(roots, &children, &terms, beam_width, &[], &mut candidates);
    candidates.sort_by(|a: &SectionMatch, b: &SectionMatch| {
        b.score
            .total_cmp(&a.score)
            .then(a.char_start.cmp(&b.char_start))
            .then(a.node_id.cmp(&b.node_id))
    });
    candidates.retain(|section| section.score > 0.0);
    candidates.truncate(top_k);
    candidates
}

fn validate_tree<'a>(
    siblings: &[&'a Section],
    children: &HashMap<Option<&'a str>, Vec<&'a Section>>,
    visited: &mut HashSet<&'a str>,
    depth: usize,
) -> Result<(), RetrievalError> {
    if depth >= MAX_DEPTH {
        return Err(RetrievalError::InvalidTree);
    }
    for node in siblings {
        if !visited.insert(node.node_id.as_str()) {
            return Err(RetrievalError::InvalidTree);
        }
        if let Some(nested) = children.get(&Some(node.node_id.as_str())) {
            validate_tree(nested, children, visited, depth + 1)?;
        }
    }
    Ok(())
}

fn walk<'a>(
    siblings: &[&'a Section],
    children: &HashMap<Option<&'a str>, Vec<&'a Section>>,
    terms: &HashSet<String>,
    beam_width: usize,
    path: &[String],
    out: &mut Vec<SectionMatch>,
) {
    let mut ranked: Vec<_> = siblings
        .iter()
        .map(|section| (score(section, terms), *section))
        .collect();
    ranked.sort_by(|(a_score, a), (b_score, b)| {
        b_score
            .total_cmp(a_score)
            .then(a.char_start.cmp(&b.char_start))
            .then(a.node_id.cmp(&b.node_id))
    });
    for (score, section) in ranked.into_iter().take(beam_width) {
        out.push(SectionMatch {
            node_id: section.node_id.clone(),
            title: section.title.clone(),
            score,
            char_start: section.char_start,
            char_end: section.char_end,
            page_start: section.page_start,
            page_end: section.page_end,
            path: path.to_vec(),
        });
        if let Some(nested) = children.get(&Some(section.node_id.as_str())) {
            let mut next_path = path.to_vec();
            next_path.push(section.title.clone());
            walk(nested, children, terms, beam_width, &next_path, out);
        }
    }
}

fn score(section: &Section, query_terms: &HashSet<String>) -> f64 {
    let title = terms(&section.title);
    let summary = terms(&section.summary);
    query_terms
        .iter()
        .map(|term| {
            if title.contains(term) {
                3.0
            } else if summary.contains(term) {
                1.0
            } else {
                0.0
            }
        })
        .sum::<f64>()
        / query_terms.len() as f64
}

fn terms(text: &str) -> HashSet<String> {
    eg_text::unicode_alphanumeric_tokens(text)
        .into_iter()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(id: &str, parent: Option<&str>, title: &str, start: u64, end: u64) -> Section {
        Section {
            document_id: "manual".into(),
            node_id: id.into(),
            parent_id: parent.map(str::to_owned),
            title: title.into(),
            summary: String::new(),
            char_start: start,
            char_end: end,
            page_start: None,
            page_end: None,
        }
    }

    #[test]
    fn picks_relevant_branch_and_cites_breadcrumb_and_range() {
        let mut sections = vec![
            row("root", None, "Product manual", 0, 300),
            row("install", Some("root"), "Installation", 0, 100),
            row("retrieval", Some("root"), "Retrieval", 100, 200),
            row("billing", Some("root"), "Billing and refunds", 200, 300),
        ];
        sections[3].summary = "Payment methods, invoices, subscription tiers".into();
        let hits =
            retrieve_sections("manual", &sections, "refund my subscription payment", 2, 2).unwrap();
        assert_eq!(hits[0].node_id, "billing");
        assert_eq!(hits[0].path, ["Product manual"]);
        assert_eq!((hits[0].char_start, hits[0].char_end), (200, 300));
        assert!(hits.iter().all(|hit| hit.node_id != "install"));
    }

    #[test]
    fn beam_prunes_siblings_and_ties_are_stable() {
        let sections = vec![
            row("root", None, "manual", 0, 300),
            row("a", Some("root"), "refund", 0, 100),
            row("b", Some("root"), "refund", 100, 200),
            row("c", Some("root"), "refund", 200, 300),
        ];
        let hits = retrieve_sections("manual", &sections, "refund", 3, 2).unwrap();
        assert_eq!(
            hits.iter().map(|h| h.node_id.as_str()).collect::<Vec<_>>(),
            ["a", "b"]
        );
    }

    #[test]
    fn rejects_mixed_documents_and_broken_trees() {
        let mut sections = vec![row("root", None, "manual", 0, 100)];
        sections.push(row("child", Some("root"), "item", 0, 20));
        sections[1].document_id = "other".into();
        assert_eq!(
            retrieve_sections("manual", &sections, "item", 1, 1),
            Err(RetrievalError::WrongDocument)
        );
        sections[1].document_id = "manual".into();
        sections[1].parent_id = Some("missing".into());
        assert_eq!(
            retrieve_sections("manual", &sections, "item", 1, 1),
            Err(RetrievalError::InvalidTree)
        );
        sections[1].parent_id = Some("root".into());
        sections[1].char_end = 101;
        assert_eq!(
            retrieve_sections("manual", &sections, "item", 1, 1),
            Err(RetrievalError::InvalidTree)
        );
    }

    #[test]
    fn empty_query_and_empty_tree_return_no_evidence() {
        let sections = vec![row("root", None, "manual", 0, 100)];
        assert!(retrieve_sections("manual", &sections, "", 1, 1)
            .unwrap()
            .is_empty());
        assert!(retrieve_sections("manual", &[], "manual", 1, 1)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn unicode_query_and_title_use_the_same_terms() {
        let sections = vec![row("root", None, "ÉTÉ", 0, 100)];
        let hits = retrieve_sections("manual", &sections, "été", 1, 1).unwrap();
        assert_eq!(hits[0].node_id, "root");
        assert_eq!(hits[0].score, 3.0);
    }
}
