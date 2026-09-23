//! `CandidateSource`: where a statistical decision's options come from (EH-059).
//!
//! Library candidates are the tenant's published HEAD components in the
//! request's kind and classification scope, read through the tenant-bound
//! Agent Library search -- visibility for library rows is tenant-wide
//! (DECIDE-LAYER-DESIGN §4.3), so step 1a is the tenant check plus the scope.
//! The set is bounded BEFORE any feature is computed; a scope that names more
//! options than one decision may hold is refused, never truncated, so no
//! option is silently dropped by read order.

use eg_types::agent_component::{AgentComponentEntry, AgentComponentSearchRequest};
use eg_types::decision::statistical::{CandidateSource, StatisticalErrorCode};
use eg_types::decision::{CandidateSourceRecord, LibraryCandidateScope, MAX_ASSEMBLY_CANDIDATES};

use super::stat_support::refusal;
use crate::server::persistence::agent_library::AgentLibraryStore;

/// Page size of the library read.
const PAGE: u32 = 256;
/// Most pages one read walks before it refuses as unbounded.
const MAX_PAGES: usize = 64;

/// The options of one decision, sorted by component id.
pub(super) struct ReadCandidates {
    pub(super) entries: Vec<AgentComponentEntry>,
    pub(super) record: CandidateSourceRecord,
}

fn search_request(
    tenant_id: &str,
    scope: &LibraryCandidateScope,
    cursor: Option<String>,
) -> AgentComponentSearchRequest {
    AgentComponentSearchRequest {
        tenant_id: tenant_id.to_string(),
        task: None,
        capabilities: scope.classification_under.iter().cloned().collect(),
        kinds: scope.kinds.iter().copied().collect(),
        read_only: false,
        limit: Some(PAGE),
        cursor,
    }
}

fn read_library(
    store: &AgentLibraryStore,
    tenant_id: &str,
    scope: &LibraryCandidateScope,
) -> Result<Vec<AgentComponentEntry>, String> {
    let mut entries = Vec::new();
    let mut cursor = None;
    for _ in 0..MAX_PAGES {
        let page = store
            .search_components(&search_request(tenant_id, scope, cursor))
            .map_err(|detail| refusal(StatisticalErrorCode::ParameterInvalid, detail))?;
        entries.extend(page.entries);
        if entries.len() > MAX_ASSEMBLY_CANDIDATES {
            return Err(refusal(
                StatisticalErrorCode::CandidateSetTooLarge,
                format!("the scope names more than {MAX_ASSEMBLY_CANDIDATES} options; narrow it"),
            ));
        }
        match page.next_cursor {
            Some(next) => cursor = Some(next),
            None => return Ok(entries),
        }
    }
    Err(refusal(
        StatisticalErrorCode::CandidateSetTooLarge,
        "the candidate scan did not finish",
    ))
}

/// Read the options `source` names for `tenant_id`.
pub(super) fn read_candidates(
    store: &AgentLibraryStore,
    tenant_id: &str,
    source: &CandidateSource,
) -> Result<ReadCandidates, String> {
    // `Graph` exists whenever eg-types' `query` wire feature is on, which the
    // contract crate turns on in every server build; the refusal below is the
    // one answer for it until graph-sourced records (EH-060) land.
    let CandidateSource::AgentLibrary { scope } = source else {
        return Err(refusal(
            StatisticalErrorCode::CandidatePlanRefused,
            "graph-sourced candidates need the RLS-filtered plan executor and graph-sourced \
             records (EH-060), which this build does not serve",
        ));
    };
    let mut entries = read_library(store, tenant_id, scope)?;
    entries.sort_by(|a, b| a.component_id.cmp(&b.component_id));
    Ok(ReadCandidates {
        entries,
        record: CandidateSourceRecord::AgentLibrary {
            kinds: scope.kinds.clone(),
            classification_under: scope.classification_under.clone(),
        },
    })
}
