//! Ontology-driven cross-source coverage (MEDULLA-FEDERATED-QUERY-R072.1) read through
//! declared source authority (MEDULLA-SRC-AUTH).
//!
//! A caller names the ontology classes of a question. This module's typed model is
//! the read-only RESULT of linking those classes through object properties within a
//! hop budget, entailing subclass relations along the way, and naming — for each
//! class on the path — the approved virtual mapping that serves it, the class
//! itself as uncovered, or a typed abstention. **It plans no execution**: a
//! [`CoverageResult`] is a named verdict plus its justifying premises, never an
//! executable query plan.
//!
//! Selection reads `vg:SourceAuthority` rows. Candidates for one class (or one
//! facet of it) are ordered by a total, documented key (MEDULLA-SRC-AUTH-R005):
//!
//! 1. nearer class first (a subclass row overrides its superclass, R004.4);
//! 2. a facet row before a class row (R004.7, R005.1);
//! 3. the more specific matching scope (tenant, partition) before unscoped (R005.2);
//! 4. the higher source role under [`RoleOrder`] (R010.2);
//! 5. the lower authority rank.
//!
//! Two rows from different sources that are equal on every step abstain as
//! `Ambiguous`; a source name never breaks a tie (R005.3). Only rows whose schema
//! source origin may assert authority are effective (R003.4); proposed rows are
//! listed as `NotUsed { reason: Proposed }` and never selected (R003.5). A blocked
//! winner (unconnected, unapproved, contract mismatch, stale with no live route)
//! is a typed abstention unless its row allows fallback (R006).

use std::collections::{BTreeMap, BTreeSet, VecDeque};

pub use medulla_types::authority_audit::{
    AuditEntry, AuditSourceMode, AuthorityAudit, NotUsedReason, SourceRole,
};
use medulla_types::metadata_acceleration::{AccelerationPolicy, FragmentExecutionMode};
use serde::{Deserialize, Serialize};

#[cfg(feature = "rdf")]
mod live;
mod trust;

#[cfg(feature = "rdf")]
pub use live::{
    authority_rows_from_turtle, connected_sources_from_turtle, fixture_from_schema_sources,
};
pub use trust::{
    change_trust_mark, mark_for_origin, ContentOrigin, DefaultTrustMarkPolicy, StoredTrustMark,
    TrustMark, TrustMarkPolicy, TrustMarkRefusal, TRUST_MARK_DOWNGRADE_REFUSED,
};

/// The reason a selection abstained (MEDULLA-SRC-AUTH-R006.1, R010.4). It is the
/// closed not-used reason of the authority audit, so an abstention and its
/// audit entry never disagree.
pub type AbstainReason = NotUsedReason;

/// The step of the documented order that decided between the winner and the
/// runner-up (MEDULLA-SRC-AUTH-R005.4).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TiebreakRule {
    NearerClass,
    FacetOverClass,
    ScopeSpecificity,
    SourceRole,
    AuthorityRank,
}

/// A single justifying fact behind a coverage verdict. Every [`CoverageResult`]
/// carries the premises that led to it, so the selection is auditable rather than
/// asserted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Premise {
    /// An `rdfs:label`-style fact naming a class on the path.
    Label { class: String, label: String },
    /// An object-property fact linking two classes, traversed during the
    /// hop-budget search (one hop).
    Relation {
        from_class: String,
        property: String,
        to_class: String,
    },
    /// An entailed-or-asserted `subClassOf` fact used to find a mapping that
    /// serves a superclass of the class actually being covered.
    Subclass {
        sub_class: String,
        super_class: String,
    },
    /// The approved virtual mapping fact that serves `served_class` (which may be
    /// the queried class itself or a superclass reached via [`Premise::Subclass`]).
    Mapping {
        served_class: String,
        mapping_name: String,
    },
    /// The ontology authority row that declared the source and rank for a
    /// mapping.
    Authority {
        row: String,
        source: String,
        class: String,
        facet: Option<String>,
        rank: u64,
        scope: Option<String>,
        origin: String,
        generation: u64,
    },
    /// The source generation (append-only epoch) the above facts were read at.
    SourceGeneration { source: String, generation: u64 },
    /// No authority row exists for the class; its one approved mapping serves
    /// it implicitly (MEDULLA-SRC-AUTH-R006.5).
    Implicit {
        class: String,
        mapping_name: String,
        source: String,
    },
    /// The deciding step between the winner and the runner-up (R005.4).
    Tiebreak {
        winner: String,
        loser: String,
        rule: TiebreakRule,
    },
    /// The source role preferred the winner over a displaced source (R010.5).
    Role {
        source: String,
        role: SourceRole,
        preferred_over: String,
        displaced_role: SourceRole,
    },
    /// A blocked row whose `vg:allowFallback` let the next row answer (R006.3).
    FallbackFrom {
        from_row: String,
        from_source: String,
        reason: NotUsedReason,
        to_row: String,
    },
    /// A row considered but not used, with its closed reason (R003.5, R007.3).
    NotUsed {
        row: String,
        source: String,
        class: String,
        facet: Option<String>,
        reason: NotUsedReason,
    },
    /// The materialisation mode the winning row resolved to (R008).
    Mode {
        row: String,
        mode: AuditSourceMode,
        reason: Option<String>,
    },
}

/// One class-to-class hop on a selection path, carrying the premises that justify
/// traversing it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceSelectionHop {
    pub from_class: String,
    pub to_class: String,
    pub premises: Vec<Premise>,
}

/// The read-only, per-class (or per-facet) verdict of a cross-source coverage
/// search. No variant carries an execution plan.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CoverageResult {
    /// `class` (or its `facet`) is served by `mapping_name` of `source`.
    Covered {
        class: String,
        facet: Option<String>,
        source: String,
        mapping_name: String,
        /// The other candidates for the same class, in selection order.
        alternates: Vec<String>,
        path: Vec<SourceSelectionHop>,
        premises: Vec<Premise>,
        /// `Live` for a `virtual` row; `Accelerated` only through
        /// [`AccelerationPolicy::resolve_mode`] (R008).
        mode: AuditSourceMode,
    },
    /// `class` lies within the hop budget but no effective mapping serves it.
    Uncovered {
        class: String,
        premises: Vec<Premise>,
    },
    /// `class` has no object-property path to any queried root within the hop
    /// budget — it is not merely unmapped, it was never reached.
    Disconnected { class: String },
    /// A typed abstention: the authoritative row cannot answer and no declared
    /// fallback applies (R006.1). `source` names the blocked source when one
    /// exists.
    Abstained {
        class: String,
        facet: Option<String>,
        reason: AbstainReason,
        source: Option<String>,
        premises: Vec<Premise>,
    },
}

impl CoverageResult {
    pub fn class(&self) -> &str {
        match self {
            CoverageResult::Covered { class, .. }
            | CoverageResult::Uncovered { class, .. }
            | CoverageResult::Disconnected { class }
            | CoverageResult::Abstained { class, .. } => class,
        }
    }

    pub fn premises(&self) -> &[Premise] {
        match self {
            CoverageResult::Covered { premises, .. }
            | CoverageResult::Uncovered { premises, .. }
            | CoverageResult::Abstained { premises, .. } => premises,
            CoverageResult::Disconnected { .. } => &[],
        }
    }
}

/// How a row's source answers (`vg:materialisation`).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum Materialisation {
    /// Always read live (the default).
    #[default]
    Virtual,
    /// A governed copy may serve reads; `reason` is `vg:materialisationReason`.
    Accelerated { reason: String },
}

/// The schema-source origin a row was read from (R003.4). Only operator-side
/// origins may assert authority; connector packs and ingestion propose.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthorityOrigin {
    Core,
    Operator,
    Admin,
    Approved,
    /// The in-memory test fixture: stands for an operator-approved row.
    Fixture,
    Pack,
    Ingestion,
}

impl AuthorityOrigin {
    /// Parse the origin label a row carries. An unknown label is a proposal.
    pub fn parse(label: &str) -> Self {
        match label {
            "core" => Self::Core,
            "operator" => Self::Operator,
            "admin" => Self::Admin,
            "approved" => Self::Approved,
            "fixture" => Self::Fixture,
            "ingestion" => Self::Ingestion,
            _ => Self::Pack,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Core => "core",
            Self::Operator => "operator",
            Self::Admin => "admin",
            Self::Approved => "approved",
            Self::Fixture => "fixture",
            Self::Pack => "pack",
            Self::Ingestion => "ingestion",
        }
    }

    pub fn may_assert_authority(self) -> bool {
        !matches!(self, Self::Pack | Self::Ingestion)
    }
}

/// Effectiveness is derived from the origin, never from the `vg:approved`
/// flag alone (MEDULLA-SRC-AUTH-R003.4): a pack or ingestion row is not effective
/// even when it claims approval.
pub fn row_is_effective(origin: AuthorityOrigin, row: &AuthorityRow) -> bool {
    origin.may_assert_authority() && row.approved
}

/// One ontology `vg:SourceAuthority` row (or, with `declared == false`, a plain
/// approved mapping with no authority row).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthorityRow {
    pub row: String,
    pub source: String,
    pub class: String,
    pub mapping_name: String,
    pub facet: Option<String>,
    pub rank: u64,
    /// `vg:scopeTenant`.
    pub scope: Option<String>,
    /// `vg:scopePartition`.
    pub partition: Option<String>,
    pub origin: String,
    pub generation: u64,
    /// `vg:allowFallback`.
    pub allow_fallback: bool,
    /// Whether the row's mapping is operator-approved (`vg:approved`).
    pub approved: bool,
    /// `true` for a `vg:SourceAuthority` row, `false` for a bare mapping.
    pub declared: bool,
    pub materialisation: Materialisation,
    pub freshness_max_age_seconds: Option<u64>,
    /// `vg:authorityContract`.
    pub contract: Option<String>,
}

impl AuthorityRow {
    /// A declared, approved, unscoped, virtual row of fixture origin.
    pub fn new(class: &str, mapping_name: &str, source: &str, generation: u64, rank: u64) -> Self {
        Self {
            row: format!("{class}::{mapping_name}"),
            source: source.to_string(),
            class: class.to_string(),
            mapping_name: mapping_name.to_string(),
            facet: None,
            rank,
            scope: None,
            partition: None,
            origin: "fixture".to_string(),
            generation,
            allow_fallback: false,
            approved: true,
            declared: true,
            materialisation: Materialisation::Virtual,
            freshness_max_age_seconds: None,
            contract: None,
        }
    }

    pub fn facet(mut self, property: &str) -> Self {
        self.facet = Some(property.to_string());
        self.row = format!("{}::{}#{property}", self.class, self.mapping_name);
        self
    }

    pub fn tenant(mut self, tenant: &str) -> Self {
        self.scope = Some(tenant.to_string());
        self
    }

    pub fn partition(mut self, partition: &str) -> Self {
        self.partition = Some(partition.to_string());
        self
    }

    pub fn origin(mut self, origin: AuthorityOrigin) -> Self {
        self.origin = origin.as_str().to_string();
        self
    }

    pub fn approved(mut self, approved: bool) -> Self {
        self.approved = approved;
        self
    }

    pub fn allow_fallback(mut self) -> Self {
        self.allow_fallback = true;
        self
    }

    pub fn undeclared(mut self) -> Self {
        self.declared = false;
        self
    }

    pub fn accelerated(mut self, reason: &str, max_age_seconds: u64) -> Self {
        self.materialisation = Materialisation::Accelerated {
            reason: reason.to_string(),
        };
        self.freshness_max_age_seconds = Some(max_age_seconds);
        self
    }

    pub fn contract(mut self, contract: &str) -> Self {
        self.contract = Some(contract.to_string());
        self
    }

    fn scope_specificity(&self) -> u8 {
        u8::from(self.scope.is_some()) + u8::from(self.partition.is_some())
    }

    fn effective_origin(&self) -> AuthorityOrigin {
        AuthorityOrigin::parse(&self.origin)
    }
}

/// The source-role order: a named, replaceable default (MEDULLA-SRC-AUTH-R010.2).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RoleOrder(pub [SourceRole; 3]);

impl RoleOrder {
    /// `live_truth`, then `historical_context`, then `inspiration`.
    pub const LIVE_TRUTH_FIRST: Self = Self([
        SourceRole::LiveTruth,
        SourceRole::HistoricalContext,
        SourceRole::Inspiration,
    ]);

    /// The position of `role` in this order; lower is preferred.
    pub fn rank_of(&self, role: SourceRole) -> usize {
        self.0
            .iter()
            .position(|r| *r == role)
            .unwrap_or(self.0.len())
    }
}

impl Default for RoleOrder {
    fn default() -> Self {
        role_order()
    }
}

/// The default role order applied by [`select_sources`].
pub const fn role_order() -> RoleOrder {
    RoleOrder::LIVE_TRUTH_FIRST
}

/// What one selection is asked: the caller's scope, the engine clock, whether
/// the question needs current state, the role order, and the (class, property)
/// facets the question reads.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SelectionQuery {
    pub tenant: Option<String>,
    pub partition: Option<String>,
    pub now_epoch_seconds: u64,
    pub needs_current_state: bool,
    pub role_order: RoleOrder,
    pub facets: Vec<(String, String)>,
}

/// A small, in-memory ontology + mapping fixture: object-property facts, asserted
/// `subClassOf` facts, authority rows, and the connected-source view. The live
/// read ([`fixture_from_schema_sources`]) builds the same structure from the
/// composed graph schema sources.
#[derive(Clone, Debug, Default)]
pub struct CoverageFixture {
    object_properties: Vec<(String, String, String)>,
    subclass_of: Vec<(String, String)>,
    /// class -> all authority rows (never a silent overwrite, R004.1).
    mappings: BTreeMap<String, Vec<AuthorityRow>>,
    unconnected_sources: BTreeSet<String>,
    source_roles: BTreeMap<String, SourceRole>,
    source_kinds: BTreeMap<String, String>,
    copy_capable: BTreeSet<String>,
    copy_only: BTreeSet<String>,
    copy_watermarks: BTreeMap<String, u64>,
}

impl CoverageFixture {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_relation(mut self, from_class: &str, property: &str, to_class: &str) -> Self {
        self.object_properties.push((
            from_class.to_string(),
            property.to_string(),
            to_class.to_string(),
        ));
        self
    }

    pub fn with_subclass(mut self, sub_class: &str, super_class: &str) -> Self {
        self.subclass_of
            .push((sub_class.to_string(), super_class.to_string()));
        self
    }

    /// An approved mapping with no authority row (R006.5).
    pub fn with_mapping(
        self,
        class: &str,
        mapping_name: &str,
        source: &str,
        generation: u64,
    ) -> Self {
        self.with_row(AuthorityRow::new(class, mapping_name, source, generation, 1).undeclared())
    }

    /// Add a ranked `vg:SourceAuthority` candidate for `class`; lower ranks win.
    pub fn with_authority(
        self,
        class: &str,
        mapping_name: &str,
        source: &str,
        generation: u64,
        rank: u64,
    ) -> Self {
        self.with_row(AuthorityRow::new(
            class,
            mapping_name,
            source,
            generation,
            rank,
        ))
    }

    /// A ranked authority row that permits fallback when its source is blocked.
    pub fn with_authority_allow_fallback(
        self,
        class: &str,
        mapping_name: &str,
        source: &str,
        generation: u64,
        rank: u64,
    ) -> Self {
        self.with_row(
            AuthorityRow::new(class, mapping_name, source, generation, rank).allow_fallback(),
        )
    }

    pub fn with_row(mut self, row: AuthorityRow) -> Self {
        self.mappings
            .entry(row.class.clone())
            .or_default()
            .push(row);
        self
    }

    /// Declare `source` absent from the owner-scoped foreign catalog.
    pub fn with_unconnected_source(mut self, source: &str) -> Self {
        self.unconnected_sources.insert(source.to_string());
        self
    }

    pub fn with_source_role(mut self, source: &str, role: SourceRole) -> Self {
        self.source_roles.insert(source.to_string(), role);
        self
    }

    pub fn with_source_kind(mut self, source: &str, kind: &str) -> Self {
        self.source_kinds
            .insert(source.to_string(), kind.to_string());
        self
    }

    /// The connection declares `copy`; `live` false means it has no live read
    /// route (a copy-only source).
    pub fn with_copy_capability(mut self, source: &str, live: bool) -> Self {
        self.copy_capable.insert(source.to_string());
        if !live {
            self.copy_only.insert(source.to_string());
        }
        self
    }

    /// The watermark (epoch seconds) of the copy behind `row`.
    pub fn with_copy_watermark(mut self, row: &str, epoch_seconds: u64) -> Self {
        self.copy_watermarks.insert(row.to_string(), epoch_seconds);
        self
    }

    /// Every authority row, in class order.
    pub fn rows(&self) -> impl Iterator<Item = &AuthorityRow> {
        self.mappings.values().flatten()
    }

    fn role(&self, source: &str) -> SourceRole {
        self.source_roles
            .get(source)
            .copied()
            .unwrap_or(SourceRole::LiveTruth)
    }

    /// `class` and its asserted superclasses with their distance, nearest first.
    fn superclasses_closure(&self, class: &str) -> Vec<(String, usize)> {
        let mut seen = vec![(class.to_string(), 0)];
        let mut frontier = VecDeque::from([(class.to_string(), 0usize)]);
        while let Some((current, depth)) = frontier.pop_front() {
            for (sub, sup) in &self.subclass_of {
                if sub == &current && !seen.iter().any(|(c, _)| c == sup) {
                    seen.push((sup.clone(), depth + 1));
                    frontier.push_back((sup.clone(), depth + 1));
                }
            }
        }
        seen
    }
}

/// Links `roots` through object properties within `hop_budget` hops and returns
/// one [`CoverageResult`] per class in the fixture's universe, under the default
/// [`SelectionQuery`].
pub fn select_sources(
    fixture: &CoverageFixture,
    roots: &[String],
    hop_budget: usize,
) -> Vec<CoverageResult> {
    select_sources_with(fixture, roots, hop_budget, &SelectionQuery::default())
}

/// [`select_sources`] under an explicit query: one verdict per class, then one
/// facet verdict per requested `(class, property)` whose class was reached.
pub fn select_sources_with(
    fixture: &CoverageFixture,
    roots: &[String],
    hop_budget: usize,
    query: &SelectionQuery,
) -> Vec<CoverageResult> {
    let universe = query_universe(fixture, roots);
    let reached = reach_from_roots(fixture, roots, hop_budget);
    let mut results: Vec<CoverageResult> = universe
        .into_iter()
        .map(|class| classify_class(fixture, &reached, query, class, None))
        .collect();
    for (class, property) in &query.facets {
        results.push(classify_class(
            fixture,
            &reached,
            query,
            class.clone(),
            Some(property.as_str()),
        ));
    }
    results
}

/// The queried universe: every class the fixture mentions anywhere, plus the roots.
fn query_universe(fixture: &CoverageFixture, roots: &[String]) -> Vec<String> {
    let mut universe: Vec<String> = Vec::new();
    let mut add = |class: &String| {
        if !universe.contains(class) {
            universe.push(class.clone());
        }
    };
    for (a, _, b) in &fixture.object_properties {
        add(a);
        add(b);
    }
    for (a, b) in &fixture.subclass_of {
        add(a);
        add(b);
    }
    for c in fixture.mappings.keys() {
        add(c);
    }
    for c in roots {
        add(c);
    }
    universe
}

/// The other end of an object property touching `class`, if any.
fn property_neighbor<'a>(class: &str, from_class: &'a str, to_class: &'a str) -> Option<&'a str> {
    if from_class == class {
        Some(to_class)
    } else if to_class == class {
        Some(from_class)
    } else {
        None
    }
}

/// BFS from every root over the (undirected) object-property graph, up to
/// `hop_budget` hops, recording the hop path premises per reached class.
fn reach_from_roots(
    fixture: &CoverageFixture,
    roots: &[String],
    hop_budget: usize,
) -> BTreeMap<String, Vec<SourceSelectionHop>> {
    let mut reached: BTreeMap<String, Vec<SourceSelectionHop>> = BTreeMap::new();
    let mut frontier: Vec<(String, Vec<SourceSelectionHop>)> =
        roots.iter().map(|r| (r.clone(), Vec::new())).collect();
    for root in roots {
        reached.entry(root.clone()).or_default();
    }
    for _ in 0..hop_budget {
        let mut next_frontier = Vec::new();
        for (class, path_so_far) in &frontier {
            for (from_class, property, to_class) in &fixture.object_properties {
                let Some(neighbor) = property_neighbor(class, from_class, to_class) else {
                    continue;
                };
                if reached.contains_key(neighbor) {
                    continue;
                }
                let mut path = path_so_far.clone();
                path.push(SourceSelectionHop {
                    from_class: class.clone(),
                    to_class: neighbor.to_string(),
                    premises: vec![Premise::Relation {
                        from_class: from_class.clone(),
                        property: property.clone(),
                        to_class: to_class.clone(),
                    }],
                });
                reached.insert(neighbor.to_string(), path.clone());
                next_frontier.push((neighbor.to_string(), path));
            }
        }
        if next_frontier.is_empty() {
            break;
        }
        frontier = next_frontier;
    }
    reached
}

/// One authority row competing for a (class, facet) verdict.
#[derive(Clone, Debug)]
struct Candidate<'a> {
    row: &'a AuthorityRow,
    served_class: String,
    depth: usize,
    role: SourceRole,
    role_rank: usize,
}

impl Candidate<'_> {
    /// The documented order key (module doc, steps 1-5).
    fn key(&self) -> (usize, u8, std::cmp::Reverse<u8>, usize, u64) {
        (
            self.depth,
            u8::from(self.row.facet.is_none()),
            std::cmp::Reverse(self.row.scope_specificity()),
            self.role_rank,
            self.row.rank,
        )
    }

    /// The order key without the source-role step, used to detect a conflict
    /// that the role resolves.
    fn ties_before_role(&self, other: &Self) -> bool {
        let (a, b) = (self.key(), other.key());
        (a.0, a.1, a.2) == (b.0, b.1, b.2)
    }
}

/// The first step of the order on which `winner` beats `loser`.
fn deciding_rule(winner: &Candidate<'_>, loser: &Candidate<'_>) -> Option<TiebreakRule> {
    let (w, l) = (winner.key(), loser.key());
    if w.0 != l.0 {
        Some(TiebreakRule::NearerClass)
    } else if w.1 != l.1 {
        Some(TiebreakRule::FacetOverClass)
    } else if w.2 != l.2 {
        Some(TiebreakRule::ScopeSpecificity)
    } else if w.3 != l.3 {
        Some(TiebreakRule::SourceRole)
    } else if w.4 != l.4 {
        Some(TiebreakRule::AuthorityRank)
    } else {
        None
    }
}

fn scope_matches(row: &AuthorityRow, query: &SelectionQuery) -> bool {
    let tenant_ok = row.scope.is_none() || row.scope == query.tenant;
    let partition_ok = row.partition.is_none() || row.partition == query.partition;
    tenant_ok && partition_ok
}

fn facet_matches(row: &AuthorityRow, facet: Option<&str>) -> bool {
    match facet {
        None => row.facet.is_none(),
        Some(property) => row.facet.as_deref().is_none_or(|f| f == property),
    }
}

/// Every in-scope row for `class` or an ancestor, ordered by the documented key.
fn gather_candidates<'a>(
    fixture: &'a CoverageFixture,
    query: &SelectionQuery,
    class: &str,
    facet: Option<&str>,
) -> Vec<Candidate<'a>> {
    let mut candidates = Vec::new();
    for (ancestor, depth) in fixture.superclasses_closure(class) {
        let Some(rows) = fixture.mappings.get(&ancestor) else {
            continue;
        };
        for row in rows {
            if facet_matches(row, facet) && scope_matches(row, query) {
                let role = fixture.role(&row.source);
                candidates.push(Candidate {
                    row,
                    served_class: ancestor.clone(),
                    depth,
                    role,
                    role_rank: query.role_order.rank_of(role),
                });
            }
        }
    }
    candidates.sort_by(|a, b| {
        a.key()
            .cmp(&b.key())
            .then_with(|| a.row.row.cmp(&b.row.row))
    });
    candidates
}

fn not_used(
    row: &AuthorityRow,
    class: &str,
    facet: Option<&str>,
    reason: NotUsedReason,
) -> Premise {
    Premise::NotUsed {
        row: row.row.clone(),
        source: row.source.clone(),
        class: class.to_string(),
        facet: facet.map(str::to_string),
        reason,
    }
}

fn authority_premise(row: &AuthorityRow) -> Premise {
    Premise::Authority {
        row: row.row.clone(),
        source: row.source.clone(),
        class: row.class.clone(),
        facet: row.facet.clone(),
        rank: row.rank,
        scope: row.scope.clone(),
        origin: row.origin.clone(),
        generation: row.generation,
    }
}

/// The one (class, facet) being decided, plus the premises gathered so far.
struct Verdict<'q> {
    class: String,
    facet: Option<&'q str>,
    path: Vec<SourceSelectionHop>,
    premises: Vec<Premise>,
}

impl Verdict<'_> {
    fn abstain(self, reason: NotUsedReason, source: Option<String>) -> CoverageResult {
        CoverageResult::Abstained {
            class: self.class,
            facet: self.facet.map(str::to_string),
            reason,
            source,
            premises: self.premises,
        }
    }

    fn uncovered(self) -> CoverageResult {
        CoverageResult::Uncovered {
            class: self.class,
            premises: self.premises,
        }
    }
}

/// Classify one class (or one facet of it).
fn classify_class(
    fixture: &CoverageFixture,
    reached: &BTreeMap<String, Vec<SourceSelectionHop>>,
    query: &SelectionQuery,
    class: String,
    facet: Option<&str>,
) -> CoverageResult {
    let Some(path) = reached.get(&class) else {
        return CoverageResult::Disconnected { class };
    };
    let candidates = gather_candidates(fixture, query, &class, facet);
    let mut verdict = Verdict {
        class,
        facet,
        path: path.clone(),
        premises: Vec::new(),
    };
    if candidates.is_empty() {
        return verdict.uncovered();
    }
    if candidates.iter().any(|c| c.row.declared) {
        let declared: Vec<_> = candidates.into_iter().filter(|c| c.row.declared).collect();
        select_declared(fixture, query, &mut verdict, declared)
            .unwrap_or_else(|(reason, source)| verdict.abstain(reason, source))
    } else {
        select_implicit(fixture, query, verdict, candidates)
    }
}

type Blocked = (NotUsedReason, Option<String>);

/// Set aside proposed rows and, for a current-state question, inspiration rows.
fn admit_candidates<'a>(
    query: &SelectionQuery,
    verdict: &mut Verdict<'_>,
    candidates: Vec<Candidate<'a>>,
) -> Result<Vec<Candidate<'a>>, Blocked> {
    let mut admitted = Vec::new();
    let mut insufficient: Option<String> = None;
    for candidate in candidates {
        if !candidate.row.effective_origin().may_assert_authority() {
            verdict.premises.push(not_used(
                candidate.row,
                &verdict.class,
                verdict.facet,
                NotUsedReason::Proposed,
            ));
        } else if query.needs_current_state && candidate.role == SourceRole::Inspiration {
            verdict.premises.push(not_used(
                candidate.row,
                &verdict.class,
                verdict.facet,
                NotUsedReason::RoleInsufficient,
            ));
            insufficient.get_or_insert(candidate.row.source.clone());
        } else {
            admitted.push(candidate);
        }
    }
    match (admitted.is_empty(), insufficient) {
        (true, Some(source)) => Err((NotUsedReason::RoleInsufficient, Some(source))),
        _ => Ok(admitted),
    }
}

/// Selection among declared authority rows (R004-R006, R008, R010).
fn select_declared(
    fixture: &CoverageFixture,
    query: &SelectionQuery,
    verdict: &mut Verdict<'_>,
    candidates: Vec<Candidate<'_>>,
) -> Result<CoverageResult, Blocked> {
    let candidates = admit_candidates(query, verdict, candidates)?;
    if candidates.is_empty() {
        let premises = std::mem::take(&mut verdict.premises);
        return Ok(CoverageResult::Uncovered {
            class: verdict.class.clone(),
            premises,
        });
    }
    if let Some(tied) = ambiguous_head(&candidates) {
        verdict
            .premises
            .extend(tied.iter().map(|c| authority_premise(c.row)));
        return Err((NotUsedReason::Ambiguous, None));
    }
    let mut fallback_from: Vec<(&AuthorityRow, NotUsedReason)> = Vec::new();
    for (index, candidate) in candidates.iter().enumerate() {
        match resolve_mode(fixture, query, candidate.row) {
            Ok(mode) => {
                return Ok(covered(verdict, &candidates, index, mode, &fallback_from));
            }
            Err(reason) => {
                verdict.premises.push(authority_premise(candidate.row));
                if !candidate.row.allow_fallback {
                    return Err((reason, Some(candidate.row.source.clone())));
                }
                fallback_from.push((candidate.row, reason));
            }
        }
    }
    let (row, reason) = fallback_from.last().expect("non-empty candidates");
    Err((*reason, Some(row.source.clone())))
}

/// The rows tied with the head on every step but from different sources.
fn ambiguous_head<'c, 'a>(candidates: &'c [Candidate<'a>]) -> Option<Vec<&'c Candidate<'a>>> {
    let head = candidates.first()?;
    let tied: Vec<_> = candidates
        .iter()
        .filter(|c| c.key() == head.key())
        .collect();
    let sources: BTreeSet<_> = tied.iter().map(|c| c.row.source.as_str()).collect();
    (sources.len() > 1).then_some(tied)
}

fn contract_agrees(contract: &str, kind: &str) -> bool {
    match contract {
        "api" => matches!(kind, "api" | "graphql"),
        "mcp" => kind == "mcp",
        "a2a" => kind == "a2a",
        "virtual-graph" => matches!(kind, "sql" | "iceberg" | "warehouse" | "sparql" | "graphql"),
        "document" => kind == "document",
        _ => false,
    }
}

/// Whether `row` can answer and in which mode, or why it cannot.
fn resolve_mode(
    fixture: &CoverageFixture,
    query: &SelectionQuery,
    row: &AuthorityRow,
) -> Result<(AuditSourceMode, Option<String>), NotUsedReason> {
    if fixture.unconnected_sources.contains(&row.source) {
        return Err(NotUsedReason::Unconnected);
    }
    if !row_is_effective(row.effective_origin(), row) {
        return Err(NotUsedReason::UnapprovedMapping);
    }
    if let (Some(contract), Some(kind)) = (&row.contract, fixture.source_kinds.get(&row.source)) {
        if !contract_agrees(contract, kind) {
            return Err(NotUsedReason::ContractMismatch);
        }
    }
    let Materialisation::Accelerated { reason } = &row.materialisation else {
        // A virtual row is always live: no copy route is ever resolved for it.
        return Ok((AuditSourceMode::Live, None));
    };
    let fresh_watermark = fixture.copy_watermarks.get(&row.row).filter(|watermark| {
        row.freshness_max_age_seconds
            .is_some_and(|max_age| query.now_epoch_seconds.saturating_sub(**watermark) <= max_age)
    });
    let policy = AccelerationPolicy {
        source_supports_copy: fixture.copy_capable.contains(&row.source),
        hot_subset_predicate: row.class.clone(),
        freshness_watermark: fresh_watermark.map(u64::to_string),
    };
    match policy.resolve_mode() {
        FragmentExecutionMode::Copy => Ok((AuditSourceMode::Accelerated, Some(reason.clone()))),
        FragmentExecutionMode::Live if fixture.copy_only.contains(&row.source) => {
            Err(NotUsedReason::Stale)
        }
        FragmentExecutionMode::Live => Ok((AuditSourceMode::Live, None)),
    }
}

/// Build the covered verdict for `candidates[index]`.
fn covered(
    verdict: &mut Verdict<'_>,
    candidates: &[Candidate<'_>],
    index: usize,
    (mode, reason): (AuditSourceMode, Option<String>),
    fallback_from: &[(&AuthorityRow, NotUsedReason)],
) -> CoverageResult {
    let winner = &candidates[index];
    let mut premises = std::mem::take(&mut verdict.premises);
    premises.push(Premise::SourceGeneration {
        source: winner.row.source.clone(),
        generation: winner.row.generation,
    });
    premises.push(authority_premise(winner.row));
    if winner.depth > 0 {
        premises.push(Premise::Subclass {
            sub_class: verdict.class.clone(),
            super_class: winner.served_class.clone(),
        });
    }
    premises.push(Premise::Mapping {
        served_class: winner.served_class.clone(),
        mapping_name: winner.row.mapping_name.clone(),
    });
    premises.push(Premise::Mode {
        row: winner.row.row.clone(),
        mode,
        reason,
    });
    for (from, why) in fallback_from {
        premises.push(Premise::FallbackFrom {
            from_row: from.row.clone(),
            from_source: from.source.clone(),
            reason: *why,
            to_row: winner.row.row.clone(),
        });
    }
    if let (0, Some(runner_up)) = (index, candidates.get(1)) {
        push_tiebreak(&mut premises, winner, runner_up);
    }
    let mut alternates = Vec::new();
    for (position, candidate) in candidates.iter().enumerate() {
        if position > index {
            premises.push(authority_premise(candidate.row));
        }
        if position != index {
            alternates.push(candidate.row.mapping_name.clone());
        }
    }
    CoverageResult::Covered {
        class: verdict.class.clone(),
        facet: verdict.facet.map(str::to_string),
        source: winner.row.source.clone(),
        mapping_name: winner.row.mapping_name.clone(),
        alternates,
        path: verdict.path.clone(),
        premises,
        mode,
    }
}

fn push_tiebreak(premises: &mut Vec<Premise>, winner: &Candidate<'_>, loser: &Candidate<'_>) {
    let Some(rule) = deciding_rule(winner, loser) else {
        return;
    };
    premises.push(Premise::Tiebreak {
        winner: winner.row.row.clone(),
        loser: loser.row.row.clone(),
        rule,
    });
    if rule == TiebreakRule::SourceRole && winner.ties_before_role(loser) {
        premises.push(Premise::Role {
            source: winner.row.source.clone(),
            role: winner.role,
            preferred_over: loser.row.source.clone(),
            displaced_role: loser.role,
        });
    }
}

/// No authority row: one approved mapping serves implicitly; several abstain
/// `Ambiguous`; none approved abstains `UnapprovedMapping` (R006.5).
fn select_implicit(
    fixture: &CoverageFixture,
    query: &SelectionQuery,
    mut verdict: Verdict<'_>,
    candidates: Vec<Candidate<'_>>,
) -> CoverageResult {
    let nearest = candidates[0].depth;
    let approved: Vec<_> = candidates
        .iter()
        .filter(|c| c.depth == nearest && row_is_effective(c.row.effective_origin(), c.row))
        .collect();
    match approved.as_slice() {
        [] => {
            let source = candidates[0].row.source.clone();
            verdict.abstain(NotUsedReason::UnapprovedMapping, Some(source))
        }
        [only] => {
            let Ok(mode) = resolve_mode(fixture, query, only.row) else {
                let source = only.row.source.clone();
                return verdict.abstain(NotUsedReason::Unconnected, Some(source));
            };
            verdict.premises.push(Premise::Implicit {
                class: only.served_class.clone(),
                mapping_name: only.row.mapping_name.clone(),
                source: only.row.source.clone(),
            });
            let only = (*only).clone();
            covered(&mut verdict, &[only], 0, mode, &[])
        }
        several => {
            verdict
                .premises
                .extend(several.iter().map(|c| authority_premise(c.row)));
            verdict.abstain(NotUsedReason::Ambiguous, None)
        }
    }
}

impl Premise {
    /// The stable text one audit entry carries for this premise.
    pub fn render(&self) -> String {
        format!("{self:?}")
    }
}

/// The typed abstention of a question whose required sources are not all
/// connected: each `(source, concept)` pair the question reads is selected
/// through the same engine as any other coverage verdict, with the sources
/// outside `connected` declared absent from the foreign catalog. Returns `None`
/// when every required source is connected (the question can be answered);
/// otherwise the [`AuthorityAudit`] whose `not_used` entries name each
/// unconnected source with the closed reason `Unconnected` and whose `used`
/// list is empty — never an empty success.
pub fn question_abstention(
    required: &[(&str, &str)],
    connected: &BTreeSet<String>,
) -> Option<AuthorityAudit> {
    let verdicts: Vec<CoverageResult> = required
        .iter()
        .filter(|(source, _)| !connected.contains(*source))
        .map(|(source, concept)| {
            let fixture = CoverageFixture::new()
                .with_authority(concept, &format!("{source}-{concept}"), source, 1, 1)
                .with_unconnected_source(source);
            let verdict = select_sources(&fixture, &[concept.to_string()], 0)
                .into_iter()
                .find(|verdict| verdict.class() == *concept)
                .expect("the question's concept is in the selection universe");
            debug_assert!(matches!(
                verdict,
                CoverageResult::Abstained {
                    reason: NotUsedReason::Unconnected,
                    ..
                }
            ));
            verdict
        })
        .collect();
    if verdicts.is_empty() {
        return None;
    }
    let mut audit = audit_from(&verdicts);
    audit.used.clear();
    Some(audit)
}

/// Build the authority audit from verdicts and their premises only
/// (MEDULLA-SRC-AUTH-R007.2): every used entry carries its verdict's premises,
/// verbatim, and every `NotUsed` premise or abstention becomes a not-used
/// entry with its closed reason (R007.3). Nothing is added.
pub fn audit_from(verdicts: &[CoverageResult]) -> AuthorityAudit {
    let mut audit = AuthorityAudit::default();
    for verdict in verdicts {
        audit_verdict(&mut audit, verdict);
    }
    audit
}

fn audit_verdict(audit: &mut AuthorityAudit, verdict: &CoverageResult) {
    for premise in verdict.premises() {
        if let Premise::NotUsed {
            source,
            class,
            facet,
            reason,
            ..
        } = premise
        {
            audit
                .not_used
                .push(not_used_entry(source, class, facet.clone(), *reason));
        }
    }
    match verdict {
        CoverageResult::Covered {
            class,
            facet,
            source,
            premises,
            mode,
            ..
        } => audit.used.push(AuditEntry {
            source: source.clone(),
            concept: class.clone(),
            facet: facet.clone(),
            role: winner_role(premises),
            premises: premises.iter().map(Premise::render).collect(),
            reason: None,
            mode: Some(*mode),
            materialisation_reason: materialisation_reason(premises),
        }),
        CoverageResult::Abstained {
            class,
            facet,
            reason,
            source: Some(source),
            ..
        } => audit
            .not_used
            .push(not_used_entry(source, class, facet.clone(), *reason)),
        _ => {}
    }
}

fn not_used_entry(
    source: &str,
    class: &str,
    facet: Option<String>,
    reason: NotUsedReason,
) -> AuditEntry {
    AuditEntry {
        source: source.to_string(),
        concept: class.to_string(),
        facet,
        role: None,
        premises: Vec::new(),
        reason: Some(reason),
        mode: None,
        materialisation_reason: None,
    }
}

fn winner_role(premises: &[Premise]) -> Option<SourceRole> {
    premises.iter().find_map(|p| match p {
        Premise::Role { role, .. } => Some(*role),
        _ => None,
    })
}

fn materialisation_reason(premises: &[Premise]) -> Option<String> {
    premises.iter().find_map(|p| match p {
        Premise::Mode { reason, .. } => reason.clone(),
        _ => None,
    })
}

#[cfg(test)]
mod tests;
