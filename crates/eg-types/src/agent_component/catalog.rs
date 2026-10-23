//! The metadata catalog's read contract over connector-pack assets
//! (EG-TYPED-PACKS-R106, R107, R108.16; EG-DECISION-ENGINE-R814).
//!
//! A connector pack import already publishes one `AgentComponent` revision
//! per tool, prompt, skill and resource. Those revisions ARE the catalog's
//! asset records: this module projects them into catalog views and adds no
//! second store or index. Every view is a pure function of the published
//! heads, so the served read and its tests compute the same answer.
//!
//! * [`CatalogSearchRequest`] -> [`CatalogSearchPage`]: one ranked search over
//!   assets and the glossary terms bound to them.
//! * [`CatalogLineageRequest`] -> [`CatalogLineage`]: connector -> server ->
//!   operation -> resource edges around one asset.
//! * [`PromptCandidatesRequest`] -> [`PromptCandidatePage`]: connector prompts
//!   as system-prompt candidates for agent assembly.
//!
//! What each asset says about owner, glossary term and contract version is the
//! engine's current knowledge and is labelled with its source. A pack declares
//! no owner yet, so the owner is the importing principal and `owner_declared`
//! is false. Glossary terms come from the engine's native capability
//! vocabulary (a broader/narrower concept scheme) and from the asset's
//! declared capability IRIs; an asset with none is flagged `glossary_missing`.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::{validate_text, AgentComponentEntry, AgentComponentKind, ComponentProvenance};
use crate::agent_library::AgentLibraryLifecycle;

/// Format identity of every catalog view.
pub const CATALOG_SCHEMA_VERSION: u16 = 1;
/// Most hits one search page carries, and its default.
pub const MAX_CATALOG_SEARCH_LIMIT: u32 = 200;
/// Most candidates one prompt-candidate page carries, and its default.
pub const MAX_PROMPT_CANDIDATE_LIMIT: u32 = 500;
/// Most connectors one request may restrict to.
pub const MAX_CATALOG_CONNECTORS: usize = 64;
/// Longest search query.
pub const MAX_CATALOG_QUERY_BYTES: usize = 256;
/// Most pack members one catalog read scans. A tenant past it is refused by
/// name, never silently truncated.
pub const MAX_CATALOG_SCAN_ROWS: usize = 65_536;
/// The `AgentComponent` attribute naming a pack member's connector.
pub const PACK_CONNECTOR_ATTRIBUTE: &str = "pack.connector";
/// The `AgentComponent` attribute carrying a pack member's entry digest.
pub const PACK_ENTRY_DIGEST_ATTRIBUTE: &str = "pack.entry_digest";
/// An optional prompt attribute declaring the prompt's role.
pub const PROMPT_ROLE_ATTRIBUTE: &str = "prompt.role";

/// What a catalog asset is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum CatalogAssetKind {
    Tool,
    Prompt,
    Skill,
    Resource,
}

impl CatalogAssetKind {
    /// The asset kind a component kind catalogs as, or `None` for a kind the
    /// catalog does not list as an asset (servers, ontologies, shapes, ...).
    pub fn of(kind: AgentComponentKind) -> Option<Self> {
        match kind {
            AgentComponentKind::Tool => Some(Self::Tool),
            AgentComponentKind::McpPrompt | AgentComponentKind::SystemPrompt => Some(Self::Prompt),
            AgentComponentKind::Skill => Some(Self::Skill),
            AgentComponentKind::McpResource => Some(Self::Resource),
            _ => None,
        }
    }
}

/// How an asset's owner was decided.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum CatalogOwnerSource {
    /// The principal that imported the pack. Used while a pack declares no
    /// owner of its own.
    Importer,
}

/// Who answers for an asset.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct CatalogOwner {
    /// The opaque persistence id of the owning principal.
    pub principal: String,
    /// `owner`; the stewardship role this assignment carries.
    pub role: String,
    pub source: CatalogOwnerSource,
    /// Whether the pack itself declared this owner.
    pub owner_declared: bool,
}

/// Where a glossary term came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum GlossarySource {
    /// A term of the engine's native capability vocabulary.
    NativeVocabulary,
    /// A capability IRI the pack declared outside the native vocabulary.
    DeclaredCapability,
}

/// One business term bound to an asset.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct CatalogGlossaryTerm {
    /// The concept IRI.
    pub term_id: String,
    pub label: String,
    /// The broader concepts, nearest first.
    pub broader: Vec<String>,
    pub source: GlossarySource,
}

/// The hop an edge records.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum LineageHop {
    /// Connector -> the MCP server its pack publishes.
    PublishesServer,
    /// Server -> one operation, prompt, skill or resource it serves.
    Serves,
    /// Asset -> a pack member it names (a skill's tool, a tool's resource).
    Uses,
}

/// One lineage edge. Nodes are component ids, or `connector:<id>`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct CatalogLineageEdge {
    pub from: String,
    pub to: String,
    pub hop: LineageHop,
}

/// The pack contract an asset was published under (EG-TYPED-PACKS-R107).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct CatalogPackVersion {
    /// The entry's declared contract version, or `entry-<digest prefix>`.
    pub contract_version: String,
    pub entry_revision: u64,
    pub definition_digest: String,
    /// The pack entry digest the import recorded.
    pub entry_digest: Option<String>,
}

/// One catalog asset: a published pack member, as the catalog sees it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct CatalogAsset {
    /// The `AgentComponent` id.
    pub asset_id: String,
    pub kind: CatalogAssetKind,
    pub name: String,
    pub description: String,
    pub connector: String,
    /// The serving MCP server's component id, when the pack named one.
    pub server_component_id: Option<String>,
    pub owner: CatalogOwner,
    pub glossary_terms: Vec<CatalogGlossaryTerm>,
    /// Ontology concept IRIs the asset is classified under or declares.
    pub concepts: Vec<String>,
    /// The edges into and out of this asset.
    pub lineage: Vec<CatalogLineageEdge>,
    pub pack_version: CatalogPackVersion,
    /// Named gaps: `owner_missing`, `glossary_missing`.
    pub flags: Vec<String>,
}

/// Why one hit ranked where it did.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct CatalogRankComponents {
    pub name: u32,
    pub glossary: u32,
    pub concept: u32,
    pub description: u32,
}

impl CatalogRankComponents {
    pub fn total(&self) -> u32 {
        self.name + self.glossary + self.concept + self.description
    }
}

/// One ranked search hit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct CatalogSearchHit {
    pub asset: CatalogAsset,
    /// The sum of `rank`; higher ranks first.
    pub score: u32,
    pub rank: CatalogRankComponents,
    /// The glossary term ids the query matched.
    pub matched_terms: Vec<String>,
}

/// Search the catalog by business term.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct CatalogSearchRequest {
    pub tenant_id: String,
    /// Free text; matched word by word, case-insensitively.
    pub query: String,
    /// Restrict to these kinds. Empty means every kind.
    #[serde(default)]
    pub kinds: Vec<CatalogAssetKind>,
    /// Restrict to these connectors. Empty means every connector.
    #[serde(default)]
    pub connectors: Vec<String>,
    #[serde(default)]
    pub limit: Option<u32>,
}

/// One ranked page.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct CatalogSearchPage {
    pub schema_version: u16,
    pub hits: Vec<CatalogSearchHit>,
    /// Every asset that matched, before the limit.
    pub total_matched: u32,
    /// Every asset the search ranked.
    pub scanned: u32,
}

/// The lineage around one asset.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct CatalogLineageRequest {
    pub tenant_id: String,
    pub asset_id: String,
}

/// Upstream and downstream edges of one asset, with the hop on each.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct CatalogLineage {
    pub schema_version: u16,
    pub asset_id: String,
    /// `false` when no published asset has this id; both lists are then empty.
    pub found: bool,
    pub upstream: Vec<CatalogLineageEdge>,
    pub downstream: Vec<CatalogLineageEdge>,
}

/// A prompt's role for agent assembly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum PromptCandidateRole {
    /// Fills an agent slot's system prompt.
    System,
    /// A task or template prompt: tool guidance, not a system prompt.
    ToolGuidance,
}

/// List connector prompts as system-prompt candidates.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PromptCandidatesRequest {
    pub tenant_id: String,
    /// Restrict to these connectors. Empty means every connector.
    #[serde(default)]
    pub connectors: Vec<String>,
    /// Keep only candidates that cover this capability (by subsumption).
    #[serde(default)]
    pub capability: Option<String>,
    /// Keep only `system` candidates.
    #[serde(default)]
    pub system_only: bool,
    #[serde(default)]
    pub limit: Option<u32>,
}

/// One system-prompt candidate (EG-DECISION-ENGINE-R814.1).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PromptCandidate {
    pub component_id: String,
    pub connector: String,
    pub name: String,
    pub description: String,
    pub role: PromptCandidateRole,
    /// The capability and concept IRIs the prompt serves.
    pub serves: Vec<String>,
    /// The revision's definition digest.
    pub digest: String,
    pub entry_revision: u64,
}

/// One page of prompt candidates, in component-id order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PromptCandidatePage {
    pub schema_version: u16,
    pub candidates: Vec<PromptCandidate>,
    pub total: u32,
}

fn validate_connectors(connectors: &[String]) -> Result<(), String> {
    if connectors.len() > MAX_CATALOG_CONNECTORS {
        return Err("CATALOG_REQUEST_INVALID: too many connectors".to_string());
    }
    connectors
        .iter()
        .try_for_each(|connector| validate_text("connector", connector))
}

fn validate_limit(limit: Option<u32>, max: u32) -> Result<(), String> {
    match limit {
        Some(limit) if limit == 0 || limit > max => {
            Err(format!("CATALOG_REQUEST_INVALID: limit must be 1..={max}"))
        }
        _ => Ok(()),
    }
}

impl CatalogSearchRequest {
    pub fn validate(&self) -> Result<(), String> {
        validate_text("tenant_id", &self.tenant_id)?;
        if self.query.len() > MAX_CATALOG_QUERY_BYTES || query_words(&self.query).is_empty() {
            return Err(format!(
                "CATALOG_REQUEST_INVALID: query must hold a word and at most \
                 {MAX_CATALOG_QUERY_BYTES} bytes"
            ));
        }
        validate_connectors(&self.connectors)?;
        validate_limit(self.limit, MAX_CATALOG_SEARCH_LIMIT)
    }
}

impl CatalogLineageRequest {
    pub fn validate(&self) -> Result<(), String> {
        validate_text("tenant_id", &self.tenant_id)?;
        validate_text("asset_id", &self.asset_id)
    }
}

impl PromptCandidatesRequest {
    pub fn validate(&self) -> Result<(), String> {
        validate_text("tenant_id", &self.tenant_id)?;
        validate_connectors(&self.connectors)?;
        if let Some(capability) = &self.capability {
            validate_text("capability", capability)?;
        }
        validate_limit(self.limit, MAX_PROMPT_CANDIDATE_LIMIT)
    }
}

/// Lower-cased alphanumeric words of `text`, deduplicated, in first-seen order.
fn query_words(text: &str) -> Vec<String> {
    let mut seen = BTreeSet::new();
    text.split(|c: char| !c.is_alphanumeric())
        .filter(|word| !word.is_empty())
        .map(str::to_lowercase)
        .filter(|word| seen.insert(word.clone()))
        .collect()
}

/// The connector a pack member belongs to.
fn connector_of(entry: &AgentComponentEntry) -> Option<&str> {
    entry
        .attributes
        .get(PACK_CONNECTOR_ATTRIBUTE)
        .map(String::as_str)
}

/// The name the serving server uses, else the id's last segment.
fn name_of(entry: &AgentComponentEntry) -> String {
    if let ComponentProvenance::McpServer { upstream_name, .. } = &entry.provenance {
        return upstream_name.clone();
    }
    entry
        .component_id
        .rsplit('/')
        .next()
        .unwrap_or(&entry.component_id)
        .to_string()
}

fn server_of(entry: &AgentComponentEntry) -> Option<String> {
    match &entry.provenance {
        ComponentProvenance::McpServer { server, .. } => Some(server.component_id.clone()),
        _ => None,
    }
}

/// Every concept IRI an entry is classified under or declares, sorted.
fn concepts_of(entry: &AgentComponentEntry) -> Vec<String> {
    entry
        .classification
        .iter()
        .chain(entry.declared_capabilities.iter())
        .cloned()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

/// The last path or fragment segment of an IRI, as words.
fn iri_label(iri: &str) -> String {
    iri.rsplit(['/', '#', ':'])
        .next()
        .unwrap_or(iri)
        .replace(['-', '_'], " ")
}

/// The glossary terms bound to an entry's concepts.
fn glossary_of(concepts: &[String]) -> Vec<CatalogGlossaryTerm> {
    let mut terms = BTreeMap::new();
    for concept in concepts {
        let term = match crate::agent_ontology::term(concept) {
            Some(native) => CatalogGlossaryTerm {
                term_id: native.iri.to_string(),
                label: native.label.to_string(),
                broader: crate::agent_ontology::ancestors(native.iri)
                    .into_iter()
                    .filter(|ancestor| *ancestor != native.iri)
                    .map(str::to_string)
                    .collect(),
                source: GlossarySource::NativeVocabulary,
            },
            None => CatalogGlossaryTerm {
                term_id: concept.clone(),
                label: iri_label(concept),
                broader: Vec::new(),
                source: GlossarySource::DeclaredCapability,
            },
        };
        terms.entry(term.term_id.clone()).or_insert(term);
    }
    terms.into_values().collect()
}

fn pack_version_of(entry: &AgentComponentEntry) -> CatalogPackVersion {
    CatalogPackVersion {
        contract_version: entry.version.clone(),
        entry_revision: entry.entry_revision,
        definition_digest: entry.definition_digest.clone(),
        entry_digest: entry.attributes.get(PACK_ENTRY_DIGEST_ATTRIBUTE).cloned(),
    }
}

/// The published pack members the catalog lists, by component id.
struct CatalogView<'a> {
    by_id: BTreeMap<&'a str, &'a AgentComponentEntry>,
    /// Reverse `requires`: target id -> the ids that use it.
    used_by: BTreeMap<&'a str, BTreeSet<&'a str>>,
}

impl<'a> CatalogView<'a> {
    fn new(heads: &'a [AgentComponentEntry]) -> Self {
        let by_id: BTreeMap<&str, &AgentComponentEntry> = heads
            .iter()
            .filter(|entry| entry.lifecycle == AgentLibraryLifecycle::Published)
            .filter(|entry| connector_of(entry).is_some())
            .map(|entry| (entry.component_id.as_str(), entry))
            .collect();
        let mut used_by: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
        for entry in by_id.values() {
            for dependency in &entry.requires {
                used_by
                    .entry(dependency.component_id.as_str())
                    .or_default()
                    .insert(entry.component_id.as_str());
            }
        }
        Self { by_id, used_by }
    }

    fn assets(&self) -> impl Iterator<Item = &'a AgentComponentEntry> + '_ {
        self.by_id
            .values()
            .copied()
            .filter(|entry| CatalogAssetKind::of(entry.kind).is_some())
    }

    /// Edges that end at `id`: connector -> server, server -> asset, user -> asset.
    fn upstream(&self, entry: &AgentComponentEntry) -> Vec<CatalogLineageEdge> {
        let mut edges = BTreeSet::new();
        let connector = connector_of(entry).unwrap_or_default();
        match server_of(entry) {
            Some(server) => {
                edges.insert(CatalogLineageEdge {
                    from: format!("connector:{connector}"),
                    to: server.clone(),
                    hop: LineageHop::PublishesServer,
                });
                edges.insert(CatalogLineageEdge {
                    from: server,
                    to: entry.component_id.clone(),
                    hop: LineageHop::Serves,
                });
            }
            None => {
                edges.insert(CatalogLineageEdge {
                    from: format!("connector:{connector}"),
                    to: entry.component_id.clone(),
                    hop: LineageHop::PublishesServer,
                });
            }
        }
        for user in self
            .used_by
            .get(entry.component_id.as_str())
            .into_iter()
            .flatten()
        {
            edges.insert(CatalogLineageEdge {
                from: (*user).to_string(),
                to: entry.component_id.clone(),
                hop: LineageHop::Uses,
            });
        }
        edges.into_iter().collect()
    }

    /// Edges that start at `id`: the pack members it uses.
    fn downstream(&self, entry: &AgentComponentEntry) -> Vec<CatalogLineageEdge> {
        entry
            .requires
            .iter()
            .filter(|dependency| self.by_id.contains_key(dependency.component_id.as_str()))
            .map(|dependency| CatalogLineageEdge {
                from: entry.component_id.clone(),
                to: dependency.component_id.clone(),
                hop: LineageHop::Uses,
            })
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    fn asset(&self, entry: &AgentComponentEntry) -> Option<CatalogAsset> {
        let kind = CatalogAssetKind::of(entry.kind)?;
        let connector = connector_of(entry)?.to_string();
        let concepts = concepts_of(entry);
        let glossary_terms = glossary_of(&concepts);
        let mut lineage = self.upstream(entry);
        lineage.extend(self.downstream(entry));
        let mut flags = vec!["owner_missing".to_string()];
        if glossary_terms.is_empty() {
            flags.push("glossary_missing".to_string());
        }
        Some(CatalogAsset {
            asset_id: entry.component_id.clone(),
            kind,
            name: name_of(entry),
            description: entry.summary.clone(),
            connector,
            server_component_id: server_of(entry),
            owner: CatalogOwner {
                principal: entry.actor_scope.clone(),
                role: "owner".to_string(),
                source: CatalogOwnerSource::Importer,
                owner_declared: false,
            },
            glossary_terms,
            concepts,
            lineage,
            pack_version: pack_version_of(entry),
            flags,
        })
    }
}

/// Points for one query word found in one field.
const NAME_EXACT: u32 = 1000;
const NAME_WORD: u32 = 400;
const GLOSSARY_WORD: u32 = 300;
const CONCEPT_WORD: u32 = 150;
const DESCRIPTION_WORD: u32 = 100;

/// Query words this short match whole words only; longer ones also match a
/// word they begin (`incident` finds `incidents`).
const MIN_PREFIX_WORD: usize = 3;

fn contains_word(haystack: &[String], word: &str) -> bool {
    haystack.iter().any(|candidate| {
        candidate == word || (word.len() >= MIN_PREFIX_WORD && candidate.starts_with(word))
    })
}

fn rank(asset: &CatalogAsset, words: &[String]) -> (CatalogRankComponents, Vec<String>) {
    let name_words = query_words(&asset.name);
    let description_words = query_words(&asset.description);
    let mut rank = CatalogRankComponents::default();
    let mut matched_terms = BTreeSet::new();
    if name_words == words {
        rank.name += NAME_EXACT;
    }
    for word in words {
        if contains_word(&name_words, word) {
            rank.name += NAME_WORD;
        }
        if contains_word(&description_words, word) {
            rank.description += DESCRIPTION_WORD;
        }
        for term in &asset.glossary_terms {
            if contains_word(&query_words(&term.label), word) {
                rank.glossary += GLOSSARY_WORD;
                matched_terms.insert(term.term_id.clone());
            }
        }
        if asset
            .concepts
            .iter()
            .any(|concept| contains_word(&query_words(concept), word))
        {
            rank.concept += CONCEPT_WORD;
        }
    }
    (rank, matched_terms.into_iter().collect())
}

fn connector_selected(connectors: &[String], entry: &AgentComponentEntry) -> bool {
    connectors.is_empty()
        || connector_of(entry).is_some_and(|connector| connectors.iter().any(|c| c == connector))
}

/// One ranked search over `heads` (every pack member head of one tenant).
pub fn catalog_search(
    request: &CatalogSearchRequest,
    heads: &[AgentComponentEntry],
) -> CatalogSearchPage {
    let view = CatalogView::new(heads);
    let words = query_words(&request.query);
    let mut scanned = 0u32;
    let mut hits: Vec<CatalogSearchHit> = view
        .assets()
        .filter(|entry| connector_selected(&request.connectors, entry))
        .filter_map(|entry| view.asset(entry))
        .filter(|asset| request.kinds.is_empty() || request.kinds.contains(&asset.kind))
        .filter_map(|asset| {
            scanned += 1;
            let (rank, matched_terms) = rank(&asset, &words);
            (rank.total() > 0).then(|| CatalogSearchHit {
                score: rank.total(),
                asset,
                rank,
                matched_terms,
            })
        })
        .collect();
    hits.sort_by(|left, right| {
        right
            .score
            .cmp(&left.score)
            .then_with(|| left.asset.asset_id.cmp(&right.asset.asset_id))
    });
    let total_matched = hits.len() as u32;
    hits.truncate(request.limit.unwrap_or(MAX_CATALOG_SEARCH_LIMIT) as usize);
    CatalogSearchPage {
        schema_version: CATALOG_SCHEMA_VERSION,
        hits,
        total_matched,
        scanned,
    }
}

/// The lineage around one asset of `heads`.
pub fn catalog_lineage(
    request: &CatalogLineageRequest,
    heads: &[AgentComponentEntry],
) -> CatalogLineage {
    let view = CatalogView::new(heads);
    let entry = view.by_id.get(request.asset_id.as_str()).copied();
    CatalogLineage {
        schema_version: CATALOG_SCHEMA_VERSION,
        asset_id: request.asset_id.clone(),
        found: entry.is_some(),
        upstream: entry.map(|entry| view.upstream(entry)).unwrap_or_default(),
        downstream: entry
            .map(|entry| view.downstream(entry))
            .unwrap_or_default(),
    }
}

fn prompt_role(entry: &AgentComponentEntry) -> PromptCandidateRole {
    let declared_system = entry
        .attributes
        .get(PROMPT_ROLE_ATTRIBUTE)
        .is_some_and(|role| role == "system");
    if entry.kind == AgentComponentKind::SystemPrompt || declared_system {
        PromptCandidateRole::System
    } else {
        PromptCandidateRole::ToolGuidance
    }
}

/// Every connector prompt of `heads` as an assembly candidate.
pub fn prompt_candidates(
    request: &PromptCandidatesRequest,
    heads: &[AgentComponentEntry],
) -> PromptCandidatePage {
    let view = CatalogView::new(heads);
    let mut candidates: Vec<PromptCandidate> = view
        .assets()
        .filter(|entry| CatalogAssetKind::of(entry.kind) == Some(CatalogAssetKind::Prompt))
        .filter(|entry| connector_selected(&request.connectors, entry))
        .filter(|entry| {
            request
                .capability
                .as_ref()
                .is_none_or(|capability| entry.satisfies_capability(capability))
        })
        .map(|entry| PromptCandidate {
            component_id: entry.component_id.clone(),
            connector: connector_of(entry).unwrap_or_default().to_string(),
            name: name_of(entry),
            description: entry.summary.clone(),
            role: prompt_role(entry),
            serves: concepts_of(entry),
            digest: entry.definition_digest.clone(),
            entry_revision: entry.entry_revision,
        })
        .filter(|candidate| !request.system_only || candidate.role == PromptCandidateRole::System)
        .collect();
    let total = candidates.len() as u32;
    candidates.truncate(request.limit.unwrap_or(MAX_PROMPT_CANDIDATE_LIMIT) as usize);
    PromptCandidatePage {
        schema_version: CATALOG_SCHEMA_VERSION,
        candidates,
        total,
    }
}

#[cfg(test)]
#[path = "catalog_tests.rs"]
mod tests;
