//! `CandidateSource`: where a statistical decision's options come from (EH-059).
//!
//! * **Library** candidates are the tenant's published HEAD components in the
//!   request's kind and classification scope, read through the tenant-bound
//!   Agent Library search. Visibility for library rows is tenant-wide
//!   (DECIDE-LAYER-DESIGN §4.3), so step 1a is the tenant check plus the scope.
//! * **Graph** candidates are the rows an ordinary plan returns over ONE graph
//!   snapshot the caller's RLS already filtered: the graph ACL is checked, the
//!   snapshot is filtered BEFORE the plan runs, and the plan may only select
//!   (`Scan`, `Filter`, `Traverse`, `AsOf`, `Limit`) -- a ranking stage would
//!   read global ANN/BM25 statistics or truncate before visibility, both leak
//!   vectors, so it is refused. Features then read exactly those rows.
//!
//! Either set is bounded BEFORE any feature is computed; a scope naming more
//! options than one decision may hold is refused, never truncated.

#[cfg(feature = "query")]
use std::collections::BTreeMap;

use eg_numeric::decision::candidate::CandidateView;
use eg_types::agent_component::{AgentComponentEntry, AgentComponentSearchRequest};
use eg_types::decision::statistical::declared::{check_declared, DeclaredOption};
use eg_types::decision::statistical::enrichment_schedule::{
    EnrichmentCandidate, EnrichmentScheduleRequest, COST_KEY, EXPECTED_VALUE_KEY,
};
use eg_types::decision::statistical::ingestion_lane::IngestionLaneRequest;
use eg_types::decision::statistical::log::RecordVisibility;
use eg_types::decision::statistical::retrieval_plan::RetrievalPlanKind;
use eg_types::decision::statistical::{CandidateSource, QuestionKind, StatisticalErrorCode};
use eg_types::decision::{CandidateSourceRecord, LibraryCandidateScope, MAX_ASSEMBLY_CANDIDATES};

use super::stat_classes::{current_rules, derive, DerivedClasses};
use super::stat_support::refusal;
use crate::server::persistence::agent_library::AgentLibraryStore;

/// Domain of a declared option set's digest.
const DECLARED_OPTIONS_DOMAIN: &str = "eg/decide-declared-options/v1";
/// Page size of the library read.
const PAGE: u32 = 256;
/// Most pages one read walks before it refuses as unbounded.
const MAX_PAGES: usize = 64;

/// The options of one decision, sorted by id.
pub(super) struct ReadCandidates {
    pub(super) views: Vec<CandidateView>,
    /// The library entries behind `views`; empty for graph candidates.
    pub(super) entries: Vec<AgentComponentEntry>,
    pub(super) record: CandidateSourceRecord,
    pub(super) visibility: RecordVisibility,
    /// Rule-derived classes of library candidates (EH-200); empty for graph rows.
    pub(super) derived: DerivedClasses,
}

fn too_many() -> String {
    refusal(
        StatisticalErrorCode::CandidateSetTooLarge,
        format!("the scope names more than {MAX_ASSEMBLY_CANDIDATES} options; narrow it"),
    )
}

fn search_request(
    tenant_id: &str,
    scope: &LibraryCandidateScope,
    cursor: Option<String>,
) -> AgentComponentSearchRequest {
    AgentComponentSearchRequest {
        tenant_id: tenant_id.to_string(),
        task: None,
        // With kinds to page by, `classification_under` is applied locally
        // over declared AND rule-derived classes (EH-200); without kinds the
        // declared classification is the only selector the search has.
        capabilities: if scope.kinds.is_empty() {
            scope.classification_under.iter().cloned().collect()
        } else {
            Vec::new()
        },
        kinds: scope.kinds.iter().copied().collect(),
        read_only: false,
        limit: Some(PAGE),
        cursor,
    }
}

/// Whether `entry` falls under `root` by its declared or derived classes.
fn under_root(entry: &AgentComponentEntry, derived: &DerivedClasses, root: Option<&str>) -> bool {
    let Some(root) = root else { return true };
    let derived_classes = derived
        .by_component
        .get(&entry.component_id)
        .into_iter()
        .flatten()
        .map(|(class, _)| class);
    entry
        .classification
        .iter()
        .chain(derived_classes)
        .any(|class| eg_types::agent_ontology::satisfies(class, root))
}

fn read_library(
    store: &AgentLibraryStore,
    tenant_id: &str,
    scope: &LibraryCandidateScope,
) -> Result<(Vec<AgentComponentEntry>, DerivedClasses), String> {
    let mut entries = Vec::new();
    let mut derived = DerivedClasses {
        rules: current_rules(),
        ..DerivedClasses::default()
    };
    let mut cursor = None;
    for _ in 0..MAX_PAGES {
        let page = store
            .search_components(&search_request(tenant_id, scope, cursor))
            .map_err(|detail| refusal(StatisticalErrorCode::ParameterInvalid, detail))?;
        let classes = derive(&page.entries);
        for entry in page.entries {
            if under_root(&entry, &classes, scope.classification_under.as_deref()) {
                let own = classes
                    .by_component
                    .get(&entry.component_id)
                    .cloned()
                    .unwrap_or_default();
                derived.by_component.insert(entry.component_id.clone(), own);
                entries.push(entry);
            }
        }
        if entries.len() > MAX_ASSEMBLY_CANDIDATES {
            return Err(too_many());
        }
        match page.next_cursor {
            Some(next) => cursor = Some(next),
            None => return Ok((entries, derived)),
        }
    }
    Err(refusal(
        StatisticalErrorCode::CandidateSetTooLarge,
        "the candidate scan did not finish",
    ))
}

fn view_of(entry: &AgentComponentEntry, derived: &DerivedClasses) -> CandidateView {
    let mut view = CandidateView::from_component(entry);
    for (class, _) in derived
        .by_component
        .get(&entry.component_id)
        .into_iter()
        .flatten()
    {
        if !view.classification.contains(class) {
            view.classification.push(class.clone());
        }
    }
    view
}

/// Read library candidates for `tenant_id`.
pub(super) fn library_candidates(
    store: &AgentLibraryStore,
    tenant_id: &str,
    scope: &LibraryCandidateScope,
) -> Result<ReadCandidates, String> {
    let (mut entries, derived) = read_library(store, tenant_id, scope)?;
    entries.sort_by(|a, b| a.component_id.cmp(&b.component_id));
    Ok(ReadCandidates {
        views: entries
            .iter()
            .map(|entry| view_of(entry, &derived))
            .collect(),
        entries,
        derived,
        record: CandidateSourceRecord::AgentLibrary {
            kinds: scope.kinds.clone(),
            classification_under: scope.classification_under.clone(),
        },
        visibility: RecordVisibility::Tenant,
    })
}

/// Whether one plan stage only SELECTS rows of the already-filtered snapshot.
#[cfg(feature = "query")]
fn selects_only(op: &eg_types::wire::Op) -> bool {
    use eg_types::wire::Op;
    matches!(
        op,
        Op::Scan { .. }
            | Op::Filter { .. }
            | Op::Traverse { .. }
            | Op::AsOf { .. }
            | Op::Limit { .. }
    )
}

/// Refuse a graph plan that could rank or truncate over invisible rows.
#[cfg(feature = "query")]
pub(super) fn check_selecting_plan(plan: &eg_types::wire::Plan) -> Result<(), String> {
    if plan.ops.is_empty() || !plan.ops.iter().all(selects_only) {
        return Err(refusal(
            StatisticalErrorCode::CandidatePlanRefused,
            "a graph candidate plan may only Scan, Filter, Traverse, AsOf and Limit: ranking \
             stages read global index statistics before visibility",
        ));
    }
    Ok(())
}

/// One visible row's properties as a candidate: numbers on `Q32`, strings as
/// text fields. Other property shapes are not features.
#[cfg(feature = "query")]
fn row_view(id: String, properties: &serde_json::Value) -> CandidateView {
    let mut numbers = BTreeMap::new();
    let mut texts = BTreeMap::new();
    for (key, value) in properties.as_object().into_iter().flatten() {
        if let Some(number) = value.as_f64() {
            if let Ok(q) = eg_numeric::decision::quant::q32(number) {
                numbers.insert(key.clone(), q.value);
            }
        } else if let Some(text) = value.as_str() {
            texts.insert(key.clone(), text.to_string());
        }
    }
    CandidateView::from_row(id, numbers, texts)
}

/// Graph candidates: the rows `plan` selects from `view`, which the caller's
/// RLS has already filtered.
#[cfg(feature = "query")]
pub(super) fn graph_candidates(
    view: &eg_core::graph::GraphView,
    graph: &str,
    plan: &eg_types::wire::Plan,
    principal: String,
) -> Result<ReadCandidates, String> {
    check_selecting_plan(plan)?;
    let semantic = eg_core::compute::semantic::SemanticStore::new();
    let rows = eg_plan::execute(plan, &eg_plan::PlanCtx::new(view, &semantic))
        .map_err(|detail| refusal(StatisticalErrorCode::CandidatePlanRefused, detail))?;
    let mut ids = rows.ids();
    ids.sort_unstable();
    ids.dedup();
    if ids.len() > MAX_ASSEMBLY_CANDIDATES {
        return Err(too_many());
    }
    let views = ids
        .into_iter()
        .map(|id| {
            let properties = view
                .node_properties
                .get(&id)
                .and_then(|blob| eg_types::msgpack::decode_property_value(blob.as_slice()).ok())
                .unwrap_or(serde_json::Value::Null);
            row_view(id, &properties)
        })
        .collect();
    Ok(ReadCandidates {
        views,
        entries: Vec::new(),
        record: CandidateSourceRecord::Graph {
            plan_digest: eg_types::decision::digest::digest_text(
                "eg/decide-candidate-plan/v1",
                &(graph, plan),
            ),
        },
        visibility: RecordVisibility::Principal { principal },
        derived: DerivedClasses::default(),
    })
}

/// The Q32 fact named `key` on `option`, or 0 when the option does not
/// carry it -- absence is a feature-reading concern elsewhere, not a shape
/// refusal here.
fn declared_number(option: &DeclaredOption, key: &str) -> i64 {
    option
        .numbers
        .iter()
        .find(|n| n.key == key)
        .map_or(0, |n| n.q32)
}

/// Map a question-specific registry check's failure to its wire refusal.
///
/// Each such `check()` (`IngestionLaneRequest`, `EnrichmentScheduleRequest`)
/// reports its MOST SPECIFIC failure as free text prefixed with its own
/// closed-vocabulary code (e.g. `"UNSUPPORTED_INGESTION_LANE: "`), mirrored
/// by a matching `StatisticalErrorCode` variant; a less specific failure
/// (an empty or malformed candidate list) carries no such prefix. Passing
/// the whole string through `refusal(StatisticalErrorCode::ParameterInvalid,
/// ...)` regardless would nest a specific code two levels deep
/// (`"PARAMETER_INVALID: UNSUPPORTED_INGESTION_LANE: ..."`), and
/// `crate::protocol::Response::err` only ever promotes the FIRST
/// `"CODE: detail"` segment to the wire-visible `error` field -- the served
/// response would read `PARAMETER_INVALID`, silently dropping the specific
/// code a caller needs to branch on (caught by
/// `consumer_tests::an_unregistered_ingestion_lane_is_refused_through_decide`
/// and `consumer_tests::a_negative_enrichment_cost_is_refused_through_decide`,
/// which assert on the served response, not the type's own unit test).
fn prefixed_refusal(detail: &str, prefix: &str, code: StatisticalErrorCode) -> String {
    match detail.strip_prefix(prefix) {
        Some(rest) => refusal(code, rest),
        None => refusal(StatisticalErrorCode::ParameterInvalid, detail),
    }
}

/// Declared candidates: the caller's own options, each fact a claim, visible
/// to the declaring principal only. Validated before any feature reads them.
///
/// `kind` additionally gates a question-specific declared-option registry
/// where one exists: `QuestionKind::IngestionLane` candidates must each name
/// a registered [`eg_types::decision::statistical::ingestion_lane::IngestionLane`],
/// refused here rather than discovered later as an opaque abstention
/// (EG-DECISION-ENGINE-R030). `QuestionKind::EnrichmentSchedule` candidates
/// may not declare a negative `enrichment.cost_q32` -- it has no meaning on
/// that scale and would let a candidate manufacture unbounded net value
/// (EG-DECISION-ENGINE-R031).
/// A `QuestionKind::RetrievalPlan` candidate whose
/// option id claims the reserved
/// [`eg_types::decision::statistical::retrieval_plan::OPTION_ID_PREFIX`]
/// namespace must name a registered
/// [`eg_types::decision::statistical::retrieval_plan::RetrievalPlanKind`],
/// refused the same way (EG-DECISION-ENGINE-R029). An option id outside that
/// namespace is an ordinary declared option, unaffected by this registry.
pub(super) fn declared_candidates(
    options: &[DeclaredOption],
    principal: &str,
    kind: QuestionKind,
) -> Result<ReadCandidates, String> {
    check_declared(options)
        .map_err(|detail| refusal(StatisticalErrorCode::ParameterInvalid, detail))?;
    if kind == QuestionKind::IngestionLane {
        IngestionLaneRequest {
            candidate_lanes: options.iter().map(|o| o.option_id.clone()).collect(),
        }
        .check()
        .map_err(|detail| prefixed_refusal(&detail, "UNSUPPORTED_INGESTION_LANE: ", StatisticalErrorCode::UnsupportedIngestionLane))?;
    } else if kind == QuestionKind::EnrichmentSchedule {
        let candidates: Vec<EnrichmentCandidate> = options
            .iter()
            .map(|option| EnrichmentCandidate {
                work_id: option.option_id.clone(),
                expected_value_q32: declared_number(option, EXPECTED_VALUE_KEY),
                cost_q32: declared_number(option, COST_KEY),
            })
            .collect();
        EnrichmentScheduleRequest { candidates }
            .check()
            .map_err(|detail| prefixed_refusal(&detail, "UNSUPPORTED_ENRICHMENT_COST: ", StatisticalErrorCode::UnsupportedEnrichmentCost))?;
    }
    if kind == QuestionKind::RetrievalPlan {
        for option in options {
            if RetrievalPlanKind::claims_registry_namespace(&option.option_id)
                && RetrievalPlanKind::from_option_id(&option.option_id).is_none()
            {
                return Err(refusal(
                    StatisticalErrorCode::ParameterInvalid,
                    format!(
                        "UNSUPPORTED_RETRIEVAL_PLAN: '{}' is not in the retrieval plan \
                         registry (leanrag, reciprocal-rank-fusion, direct-sparql)",
                        option.option_id
                    ),
                ));
            }
        }
    }
    let views = options
        .iter()
        .map(|option| {
            let mut view = CandidateView::from_row(
                option.option_id.clone(),
                option
                    .numbers
                    .iter()
                    .map(|n| (n.key.clone(), n.q32))
                    .collect(),
                option
                    .texts
                    .iter()
                    .map(|t| (t.key.clone(), t.text.clone()))
                    .collect(),
            );
            view.classification = option.classification.iter().cloned().collect();
            view
        })
        .collect();
    Ok(ReadCandidates {
        views,
        entries: Vec::new(),
        record: CandidateSourceRecord::Declared {
            options_digest: eg_types::decision::digest::digest_text(
                DECLARED_OPTIONS_DOMAIN,
                &options,
            ),
        },
        visibility: RecordVisibility::Principal {
            principal: principal.to_string(),
        },
        derived: DerivedClasses::default(),
    })
}

/// Read the options `source` names. Graph candidates need the caller's
/// filtered snapshot, which [`super::stat_decide`] takes under the state lock.
/// `kind` is the question's kind, passed through to [`declared_candidates`]
/// for its question-specific registry gate.
pub(super) fn read_candidates(
    store: &AgentLibraryStore,
    tenant_id: &str,
    source: &CandidateSource,
    graph_view: Option<&eg_core::graph::GraphView>,
    principal: &str,
    kind: QuestionKind,
) -> Result<ReadCandidates, String> {
    match source {
        CandidateSource::AgentLibrary { scope } => library_candidates(store, tenant_id, scope),
        CandidateSource::Declared { options } => {
            declared_candidates(options.as_slice(), principal, kind)
        }
        CandidateSource::Graph { graph, plan } => {
            let view = graph_view.ok_or_else(|| {
                refusal(
                    StatisticalErrorCode::CandidatePlanRefused,
                    "graph candidates need the query feature",
                )
            })?;
            #[cfg(feature = "query")]
            {
                graph_candidates(view, graph, plan, principal.to_string())
            }
            #[cfg(not(feature = "query"))]
            {
                let _ = (view, graph, plan, principal);
                Err(refusal(
                    StatisticalErrorCode::CandidatePlanRefused,
                    "graph candidates need the query feature",
                ))
            }
        }
    }
}

/// The caller's RLS-filtered snapshot of the graph a graph source names, or
/// `None` for a library source. The graph ACL is checked first; the snapshot
/// is filtered before any plan stage sees it.
#[cfg(feature = "query")]
pub(super) async fn filtered_graph_view(
    state: &std::sync::Arc<tokio::sync::RwLock<crate::server::state::ServerState>>,
    agent_id: &str,
    source: &CandidateSource,
) -> Result<Option<eg_core::graph::GraphView>, String> {
    let CandidateSource::Graph { graph, .. } = source else {
        return Ok(None);
    };
    let (core, isolation) = super::stat_support::readable_graph(
        state,
        agent_id,
        graph,
        StatisticalErrorCode::CandidatePlanRefused,
    )
    .await?;
    #[cfg(not(feature = "security"))]
    let _ = isolation;
    let view = core.analysis_snapshot();
    #[cfg(feature = "security")]
    let view = {
        let mut view = view;
        isolation.filter_view(agent_id, &mut view);
        view
    };
    Ok(Some(view))
}

/// Without the query feature a graph source is refused before any read.
#[cfg(not(feature = "query"))]
pub(super) async fn filtered_graph_view(
    _state: &std::sync::Arc<tokio::sync::RwLock<crate::server::state::ServerState>>,
    _agent_id: &str,
    _source: &CandidateSource,
) -> Result<Option<eg_core::graph::GraphView>, String> {
    Ok(None)
}
