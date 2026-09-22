//! Capability search over published components, and its opaque page cursor.
//!
//! Split out of the parent module because the search surface is a self-
//! contained read contract: a request, its bounds, the page it returns, and
//! the tenant-bound cursor that resumes it. Nothing here participates in the
//! definition digest.

use serde::{Deserialize, Serialize};

use super::facts::{AgentComponentFacts, ToolEffect};
use super::{
    validate_names, validate_text, AgentComponentEntry, AgentComponentKind,
    AgentComponentMutationKind, AGENT_COMPONENT_SEARCH_CURSOR_DOMAIN,
    MAX_AGENT_COMPONENT_SEARCH_CURSOR_BYTES, MAX_AGENT_COMPONENT_SEARCH_LIMIT, MAX_CAPABILITIES,
};
use crate::agent_library::AgentLibraryLifecycle;
use crate::tenant_cursor::CursorFamily;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AgentComponentStatusRequest {
    pub context: crate::agent_library::AgentLibraryMutationContext,
    pub component_id: String,
    pub kind: AgentComponentMutationKind,
}

/// Most component kinds one search may restrict to.
const MAX_SEARCH_KINDS: usize = 16;

/// Find components by what they can do.
///
/// The wire form of *"what does an agent trying to do XYZ need?"*. A caller
/// supplies a `task` (resolved through the native ontology), explicit
/// `capabilities`, or at least one `kind`. A kind-only request is the bounded,
/// paginated catalog-listing form; a request with no selector is refused, and
/// so is a `task` the native ontology does not know (it would resolve to no
/// capability and widen into an unfiltered listing).
///
/// # Why this is paginated
///
/// This is the capability-discovery query the whole component layer exists to
/// serve, so it is the one read whose result set grows with the tenant rather
/// than with the request. An unpaginated form has two failure modes and no
/// recovery from either: the response size is bounded only by the corpus, and a
/// tenant whose component count passes the scan bound is refused on EVERY
/// search forever. [`AgentComponentSearchPage`] exists so neither is possible,
/// and it is an object rather than a bare array because turning an array into
/// an object later is a read-side wire break.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AgentComponentSearchRequest {
    pub tenant_id: String,
    /// An ontology task term, e.g. `eg:task/research`.
    #[serde(default)]
    pub task: Option<String>,
    /// Capability terms the component must satisfy by subsumption.
    #[serde(default)]
    pub capabilities: Vec<String>,
    /// Restrict to these kinds. Empty means every kind.
    #[serde(default)]
    pub kinds: Vec<AgentComponentKind>,
    /// When true, exclude anything side-effecting. The reason this is a
    /// first-class filter rather than a caller-side one: assembling a
    /// read-only agent is a common, security-relevant request, and a caller
    /// that has to filter afterwards can forget to.
    #[serde(default)]
    pub read_only: bool,
    /// How many entries this page may carry, at most
    /// [`MAX_AGENT_COMPONENT_SEARCH_LIMIT`]. `None` requests the maximum.
    #[serde(default)]
    pub limit: Option<u32>,
    /// Where to resume, from a previous page's
    /// [`AgentComponentSearchPage::next_cursor`].
    ///
    /// OPAQUE: it is produced by the engine and only ever handed back
    /// unmodified. It carries a binding to the tenant it was minted for, so one
    /// tenant's cursor is refused by name against another's search rather than
    /// silently resuming somewhere.
    #[serde(default)]
    pub cursor: Option<String>,
}

impl AgentComponentSearchRequest {
    pub fn validate(&self) -> Result<(), String> {
        self.validate_selection()?;
        self.validate_page()
    }

    /// What the search selects on: the tenant, the task or capability terms it
    /// resolves, and the kinds it restricts to.
    fn validate_selection(&self) -> Result<(), String> {
        validate_text("tenant_id", &self.tenant_id)?;
        if let Some(task) = &self.task {
            validate_task(task)?;
        }
        validate_names("capabilities", &self.capabilities, MAX_CAPABILITIES)?;
        if self.kinds.len() > MAX_SEARCH_KINDS {
            return Err("agent component search names too many kinds".to_string());
        }
        if !self.has_selector() {
            return Err("agent component search needs a task, capability, or kind".to_string());
        }
        Ok(())
    }

    fn has_selector(&self) -> bool {
        self.task.is_some() || !self.capabilities.is_empty() || !self.kinds.is_empty()
    }

    /// How much of the result set one call may take, and where it resumes.
    fn validate_page(&self) -> Result<(), String> {
        if let Some(limit) = self.limit {
            if limit == 0 || limit > MAX_AGENT_COMPONENT_SEARCH_LIMIT {
                return Err(format!(
                    "agent component search limit must be 1..={MAX_AGENT_COMPONENT_SEARCH_LIMIT}"
                ));
            }
        }
        if let Some(cursor) = &self.cursor {
            if cursor.is_empty() || cursor.len() > MAX_AGENT_COMPONENT_SEARCH_CURSOR_BYTES {
                return Err("agent component search cursor is outside its bound".to_string());
            }
        }
        Ok(())
    }

    /// The page size this request asks for, defaulted and already bounded.
    pub fn page_limit(&self) -> usize {
        self.limit
            .unwrap_or(MAX_AGENT_COMPONENT_SEARCH_LIMIT)
            .min(MAX_AGENT_COMPONENT_SEARCH_LIMIT) as usize
    }

    /// The capabilities a component must satisfy to match.
    pub fn required_capabilities(&self) -> Vec<String> {
        let mut required: Vec<String> = self.capabilities.clone();
        if let Some(task) = &self.task {
            for capability in crate::agent_ontology::capabilities_for_task(task) {
                if !required.iter().any(|existing| existing == capability) {
                    required.push((*capability).to_string());
                }
            }
        }
        required
    }

    /// Whether one component answers this search.
    pub fn matches(&self, component: &AgentComponentEntry) -> bool {
        if !self.matches_static_filters(component) {
            return false;
        }
        // ANY, not ALL: a component is a part. A research agent needs
        // retrieval AND summarization, and no single tool provides both --
        // requiring every capability of a task would return nothing.
        let required = self.required_capabilities();
        required.is_empty()
            || required
                .iter()
                .any(|capability| component.satisfies_capability(capability))
    }

    fn matches_static_filters(&self, component: &AgentComponentEntry) -> bool {
        if component.lifecycle != AgentLibraryLifecycle::Published {
            return false;
        }
        if !self.kinds.is_empty() && !self.kinds.contains(&component.kind) {
            return false;
        }
        if self.read_only && component.is_side_effecting() {
            return false;
        }
        true
    }
}

/// A task resolves to capabilities only through the native ontology. A term it
/// does not know resolves to NOTHING, and an empty requirement set matches
/// every published component -- so an unknown task is refused by name rather
/// than silently widened into an unfiltered listing.
fn validate_task(task: &str) -> Result<(), String> {
    validate_text("task", task)?;
    if crate::agent_ontology::capabilities_for_task(task).is_empty() {
        return Err(format!(
            "agent component search task '{task}' is not a native task term"
        ));
    }
    Ok(())
}

/// One page of a capability search.
///
/// An object, never a bare array: `next_cursor` has to live somewhere, and a
/// read that starts life as a JSON array can only grow one by breaking every
/// reader.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct AgentComponentSearchPage {
    pub entries: Vec<AgentComponentEntry>,
    /// `Some` when more of the tenant remains to be scanned. Hand it back
    /// unmodified to continue; `None` means the corpus is exhausted.
    ///
    /// A page may be EMPTY and still carry a cursor: the engine bounds how much
    /// it scans per page, so a sparse match over a large tenant makes progress
    /// across several pages instead of doing unbounded work in one. A caller
    /// therefore loops until `next_cursor` is `None`, not until a page is empty.
    pub next_cursor: Option<String>,
}

impl AgentComponentEntry {
    /// Whether this component answers a need for `required_capability`.
    ///
    /// Subsumption, not equality: a component classified
    /// `eg:capability/retrieval/web-search` satisfies a need for
    /// `eg:capability/retrieval`. The direction matters and is asymmetric --
    /// see [`crate::agent_ontology::satisfies`].
    pub fn satisfies_capability(&self, required_capability: &str) -> bool {
        self.classification
            .iter()
            .any(|provided| crate::agent_ontology::satisfies(provided, required_capability))
    }

    /// Whether this component is usable for `task_iri` -- it satisfies at
    /// least one capability that task requires.
    ///
    /// The native half of *"what does an agent trying to do XYZ need?"*: the
    /// task resolves to capabilities through the baked-in ontology, and those
    /// match components by subsumption. No model in the loop, and the answer
    /// is reproducible.
    pub fn is_applicable_to_task(&self, task_iri: &str) -> bool {
        crate::agent_ontology::capabilities_for_task(task_iri)
            .iter()
            .any(|required| self.satisfies_capability(required))
    }

    /// Whether using this component can change anything.
    ///
    /// True when it is a write tool, or when it is classified anywhere under
    /// `eg:capability/action`. Both are checked because the two facts come
    /// from different places -- the typed [`ToolEffect`] is declared by the
    /// ingest, the classification by whoever curated it -- and a component is
    /// side-effecting if EITHER says so. Treating a disagreement as "safe"
    /// would be the wrong default for the one property you cannot take back.
    pub fn is_side_effecting(&self) -> bool {
        let declared_write = matches!(
            self.facts,
            AgentComponentFacts::Tool {
                effect: ToolEffect::Write,
                ..
            }
        );
        declared_write
            || self
                .classification
                .iter()
                .any(|term| crate::agent_ontology::is_a(term, "eg:capability/action"))
    }
}

/// The capability search's cursor family. The framing and the tenant-bound
/// tag are owned by [`crate::tenant_cursor`]; this row only names the search.
const SEARCH_CURSOR: CursorFamily = CursorFamily {
    domain: AGENT_COMPONENT_SEARCH_CURSOR_DOMAIN,
    max_bytes: MAX_AGENT_COMPONENT_SEARCH_CURSOR_BYTES,
    noun: "agent component search",
};

/// Mint the opaque cursor that resumes a search after `component_id`.
///
/// The encoded form is a tenant-bound tag followed by the resume key. The tag
/// is what stops a cursor from being transplanted: resumption uses the
/// REQUEST's tenant for the scan prefix, so a foreign cursor could never reach
/// another tenant's rows, but it could silently resume at a meaningless offset,
/// and a named refusal is better than a quiet wrong answer.
pub fn encode_search_cursor(tenant_id: &str, component_id: &str) -> String {
    SEARCH_CURSOR.encode(tenant_id, component_id)
}

/// Recover the resume key from an opaque cursor, or refuse it by name.
pub fn decode_search_cursor(tenant_id: &str, cursor: &str) -> Result<String, String> {
    SEARCH_CURSOR.decode(tenant_id, cursor, |component_id| {
        validate_text("cursor component_id", component_id).is_ok()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(kinds: Vec<AgentComponentKind>) -> AgentComponentSearchRequest {
        AgentComponentSearchRequest {
            tenant_id: "tenant-a".to_string(),
            task: None,
            capabilities: Vec::new(),
            kinds,
            read_only: false,
            limit: Some(10),
            cursor: None,
        }
    }

    #[test]
    fn kind_only_listing_is_a_valid_bounded_search() {
        request(vec![AgentComponentKind::Tool]).validate().unwrap();
    }

    #[test]
    fn a_task_outside_the_native_ontology_is_refused_not_widened() {
        let mut search = request(Vec::new());
        search.task = Some("eg:task/research".to_string());
        search.validate().unwrap();
        search.task = Some("summarize the quarterly report".to_string());
        let error = search.validate().unwrap_err();
        assert!(error.contains("is not a native task term"), "{error}");
        search.task = Some(crate::agent_ontology::TASK_ROOT.to_string());
        assert!(search.validate().is_err());
    }

    #[test]
    fn unfiltered_listing_remains_refused() {
        let error = request(Vec::new()).validate().unwrap_err();
        assert!(error.contains("task, capability, or kind"));
    }
}
