//! Pattern-aware OBDA pushdown (CONCEPT:EG-KG.query.obda-predicate-pushdown, EH-563 FO-04).
//!
//! A triples map is scanned as one or more SCAN GROUPS rather than once with every pushed
//! filter. A filter is pushed into the scan that produces ONE predicate's triples, and only
//! when every query pattern using that predicate agrees on the term the filter constrains —
//! so a map read by two subjects, a predicate used twice, or a non-unique subject key can no
//! longer lose solutions (the previous whole-map filter did). On top of that:
//!
//!  * a constant subject IRI is reversed through a single-placeholder subject template into a
//!    key equality — a primary-key lookup instead of a scan;
//!  * a constant object literal becomes a column equality;
//!  * SEMI-JOIN: when the groups of a map share one subject variable, the filtered ANCHOR
//!    group runs first and the others fetch only `key IN (anchor keys)`, in batches.
//!
//! Every pushed predicate returns a SUPERSET of the rows the SPARQL evaluator keeps; the
//! evaluator still applies the whole query over the materialized view.

use std::collections::{BTreeMap, BTreeSet, HashSet};

use spargebra::algebra::GraphPattern;
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern};

use super::{
    column_filters_for_compares, is_numeric_datatype, materialize_row, template_columns_into,
    FilterContext, ForeignRow, ObdaCompare, ObdaFilter, ObdaSource, ObjectMap, TriplesMap,
    VirtualGraph, RDF_TYPE_IRI,
};

/// The most keys one semi-join scan carries (`key IN (…)`); larger key sets are batched.
const MAX_SEMI_JOIN_KEYS: usize = 1000;

/// Where a triple pattern sits: the conjunctive core, or somewhere its triples may be
/// needed without the core's constraints (OPTIONAL right side, UNION/MINUS branch, GRAPH).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Scope {
    Required,
    Conditional,
}

/// The query's triple patterns with their scope. `opaque` is set by anything whose
/// predicates cannot be enumerated (a property path, an unknown algebra node); an opaque
/// query pushes nothing.
#[derive(Debug, Default)]
pub(super) struct QueryShape {
    patterns: Vec<(TriplePattern, Scope)>,
    opaque: bool,
}

impl QueryShape {
    /// Collect every triple pattern of `query` (SERVICE bodies excluded: they never read the
    /// virtual graph).
    pub(super) fn from_query(query: &spargebra::Query) -> Self {
        use spargebra::Query;
        let pattern = match query {
            Query::Select { pattern, .. }
            | Query::Construct { pattern, .. }
            | Query::Describe { pattern, .. }
            | Query::Ask { pattern, .. } => pattern,
        };
        let mut shape = Self::default();
        shape.walk(pattern, Scope::Required);
        shape
    }

    fn walk(&mut self, p: &GraphPattern, scope: Scope) {
        if let GraphPattern::Bgp { patterns } = p {
            self.patterns
                .extend(patterns.iter().map(|tp| (tp.clone(), scope)));
            return;
        }
        if let Some((left, right, right_scope)) = binary_children(p, scope) {
            self.walk(left, scope_of_left(p, scope));
            self.walk(right, right_scope);
            return;
        }
        if let Some(inner) = same_scope_child(p) {
            self.walk(inner, scope);
            return;
        }
        if let GraphPattern::Graph { inner, .. } = p {
            self.walk(inner, Scope::Conditional);
            return;
        }
        self.opaque |= !is_inert(p);
    }

    /// Every pattern that could produce `predicate` triples.
    fn with_predicate<'s>(
        &'s self,
        predicate: &'s str,
    ) -> impl Iterator<Item = &'s (TriplePattern, Scope)> + 's {
        self.patterns.iter().filter(move |(tp, _)| {
            matches!(&tp.predicate, NamedNodePattern::NamedNode(n) if n.as_str() == predicate)
                || matches!(tp.predicate, NamedNodePattern::Variable(_))
        })
    }

    /// The one term every pattern of `predicate` puts in a position, when all of them are in
    /// the required core and agree; `None` otherwise (or when nothing uses the predicate).
    fn agreed_term<'s>(&'s self, predicate: &'s str, pick: TermSide) -> Option<&'s TermPattern> {
        if self.opaque {
            return None;
        }
        let mut agreed: Option<&TermPattern> = None;
        for (tp, scope) in self.with_predicate(predicate) {
            let term = pick.of(tp);
            if *scope != Scope::Required || agreed.is_some_and(|a| a != term) {
                return None;
            }
            agreed = Some(term);
        }
        agreed
    }
}

/// Which end of a triple pattern [`QueryShape::agreed_term`] compares.
#[derive(Clone, Copy)]
enum TermSide {
    Subject,
    Object,
}

impl TermSide {
    fn of(self, tp: &TriplePattern) -> &TermPattern {
        match self {
            TermSide::Subject => &tp.subject,
            TermSide::Object => &tp.object,
        }
    }
}

/// `(left, right, scope of right)` for the two-sided algebra nodes.
fn binary_children(
    p: &GraphPattern,
    scope: Scope,
) -> Option<(&GraphPattern, &GraphPattern, Scope)> {
    match p {
        GraphPattern::Join { left, right } => Some((left, right, scope)),
        GraphPattern::LeftJoin { left, right, .. }
        | GraphPattern::Minus { left, right }
        | GraphPattern::Union { left, right } => Some((left, right, Scope::Conditional)),
        _ => None,
    }
}

/// A UNION's left branch is as conditional as its right; every other left side keeps the
/// enclosing scope.
fn scope_of_left(p: &GraphPattern, scope: Scope) -> Scope {
    if matches!(p, GraphPattern::Union { .. }) {
        Scope::Conditional
    } else {
        scope
    }
}

/// The single child of a node that neither adds nor removes scope.
fn same_scope_child(p: &GraphPattern) -> Option<&GraphPattern> {
    match p {
        GraphPattern::Filter { inner, .. }
        | GraphPattern::Extend { inner, .. }
        | GraphPattern::OrderBy { inner, .. }
        | GraphPattern::Project { inner, .. }
        | GraphPattern::Distinct { inner }
        | GraphPattern::Reduced { inner }
        | GraphPattern::Slice { inner, .. }
        | GraphPattern::Group { inner, .. } => Some(inner),
        _ => None,
    }
}

/// Nodes that read no virtual-graph triples: inline `VALUES` and a remote `SERVICE`.
fn is_inert(p: &GraphPattern) -> bool {
    matches!(
        p,
        GraphPattern::Values { .. } | GraphPattern::Service { .. }
    )
}

/// `(prefix, column, suffix)` of a subject template with exactly one `{col}` placeholder and
/// no escaped braces — the shape whose IRIs map back to exactly one key value.
fn single_key_template(template: &str) -> Option<(&str, &str, &str)> {
    if template.contains("{{") || template.contains("}}") {
        return None;
    }
    let open = template.find('{')?;
    let close = open + template[open..].find('}')?;
    let (prefix, column, suffix) = (
        &template[..open],
        &template[open + 1..close],
        &template[close + 1..],
    );
    let single = !column.is_empty() && !suffix.contains('{') && !suffix.contains('}');
    single.then_some((prefix, column, suffix))
}

fn filter(column: &str, op: ObdaCompare, value: &str, numeric: bool) -> ObdaFilter {
    ObdaFilter {
        column: column.to_string(),
        op,
        value: value.to_string(),
        numeric,
        values: Vec::new(),
    }
}

/// A key equality for a constant subject IRI that the map's template can produce.
fn subject_key_filter(map: &TriplesMap, subject: &TermPattern) -> Option<ObdaFilter> {
    let TermPattern::NamedNode(iri) = subject else {
        return None;
    };
    let (prefix, column, suffix) = single_key_template(&map.subject_template)?;
    let key = iri.as_str().strip_prefix(prefix)?.strip_suffix(suffix)?;
    (!key.is_empty()).then(|| filter(column, ObdaCompare::Eq, key, false))
}

/// The filters a query puts on the object column of one predicate-object map.
fn object_filters(obj: &ObjectMap, object: &TermPattern, fctx: &FilterContext) -> Vec<ObdaFilter> {
    let (column, datatype) = match obj {
        ObjectMap::Column(c) => (c, None),
        ObjectMap::TypedColumn(c, dt) => (c, Some(dt.as_str())),
        _ => return Vec::new(),
    };
    let numeric = datatype.is_some_and(is_numeric_datatype);
    match object {
        TermPattern::Variable(v) => fctx
            .var_compares
            .get(v.as_str())
            .map(|compares| column_filters_for_compares(column, numeric, compares))
            .unwrap_or_default(),
        TermPattern::Literal(lit) => literal_filter(column, datatype, lit).into_iter().collect(),
        _ => Vec::new(),
    }
}

/// A constant object literal as a column equality, when the materialized literal could equal
/// it: a plain string column against a plain string; a numeric column against a number
/// (numeric equality is a superset of lexical equality); any other typed column only against
/// its own datatype.
fn literal_filter(
    column: &str,
    datatype: Option<&str>,
    lit: &oxrdf::Literal,
) -> Option<ObdaFilter> {
    const XSD_STRING: &str = "http://www.w3.org/2001/XMLSchema#string";
    if lit.language().is_some() {
        return None;
    }
    let lit_type = lit.datatype().as_str();
    match datatype {
        None => {
            (lit_type == XSD_STRING).then(|| filter(column, ObdaCompare::Eq, lit.value(), false))
        }
        Some(dt) if is_numeric_datatype(dt) && is_numeric_datatype(lit_type) => {
            Some(filter(column, ObdaCompare::Eq, lit.value(), true))
        }
        Some(dt) => (dt == lit_type).then(|| filter(column, ObdaCompare::Eq, lit.value(), false)),
    }
}

/// One scan of a map's source and the triples it feeds.
struct ScanGroup<'m> {
    poms: Vec<&'m (String, ObjectMap)>,
    class: bool,
    filters: Vec<ObdaFilter>,
    /// The subject variable every pattern of this group's predicates binds, if one.
    subject_var: Option<String>,
    /// Whether this map is the only producer of every predicate of this group.
    sole_producer: bool,
}

/// The pushdown plan for one triples map under one query.
pub(super) struct MapScan<'q, 'm> {
    vg: &'q VirtualGraph,
    map: &'m TriplesMap,
    fctx: &'q FilterContext,
}

impl<'q, 'm> MapScan<'q, 'm> {
    pub(super) fn new(vg: &'q VirtualGraph, map: &'m TriplesMap, fctx: &'q FilterContext) -> Self {
        Self { vg, map, fctx }
    }

    /// Scan `source` for the active predicate-object maps (and the class triple when wanted)
    /// and materialize their triples into `out`.
    pub(super) fn run(
        &self,
        source: &dyn ObdaSource,
        active: &[&'m (String, ObjectMap)],
        class_wanted: bool,
        out: &mut Vec<oxrdf::Triple>,
    ) -> Result<(), String> {
        let groups = self.groups(active, class_wanted);
        let key_column = single_key_template(&self.map.subject_template).map(|(_, c, _)| c);
        let anchor = key_column.and_then(|_| groups.iter().position(ScanGroup::is_anchor));
        let mut anchor_keys: Option<(String, Vec<String>)> = None;
        if let (Some(i), Some(column)) = (anchor, key_column) {
            let rows = self.scan(source, &groups[i], &[])?;
            let var = groups[i].subject_var.clone().unwrap_or_default();
            anchor_keys = Some((var, distinct_keys(&rows, column)));
            self.emit(&groups[i], &rows, out);
        }
        for (i, group) in groups.iter().enumerate() {
            if Some(i) == anchor {
                continue;
            }
            self.scan_reduced(source, group, anchor_keys.as_ref(), key_column, out)?;
        }
        Ok(())
    }

    /// Scan one non-anchor group, restricted to the anchor's keys when it shares the
    /// anchor's subject variable.
    fn scan_reduced(
        &self,
        source: &dyn ObdaSource,
        group: &ScanGroup<'_>,
        anchor: Option<&(String, Vec<String>)>,
        key_column: Option<&str>,
        out: &mut Vec<oxrdf::Triple>,
    ) -> Result<(), String> {
        let reducible =
            anchor.filter(|(var, _)| group.subject_var.as_deref() == Some(var.as_str()));
        let (Some((_, keys)), Some(column)) = (reducible, key_column) else {
            let rows = self.scan(source, group, &[])?;
            self.emit(group, &rows, out);
            return Ok(());
        };
        for chunk in keys.chunks(MAX_SEMI_JOIN_KEYS) {
            let semi = ObdaFilter {
                values: chunk.to_vec(),
                ..filter(column, ObdaCompare::In, "", false)
            };
            let rows = self.scan(source, group, &[semi])?;
            self.emit(group, &rows, out);
        }
        Ok(())
    }

    fn scan(
        &self,
        source: &dyn ObdaSource,
        group: &ScanGroup<'_>,
        extra: &[ObdaFilter],
    ) -> Result<Vec<ForeignRow>, String> {
        let mut needed = BTreeSet::new();
        template_columns_into(&self.map.subject_template, &mut needed);
        for (_, obj) in &group.poms {
            obj.columns_into(&mut needed);
        }
        let mut filters = group.filters.clone();
        filters.extend_from_slice(extra);
        source.scan(&needed, &filters)
    }

    fn emit(&self, group: &ScanGroup<'_>, rows: &[ForeignRow], out: &mut Vec<oxrdf::Triple>) {
        for row in rows {
            materialize_row(self.map, &group.poms, group.class, row, out);
        }
    }

    /// Group the active predicate-object maps (and the class triple) by the filter set that
    /// is sound for their triples; equal sets share one scan. Ordered deterministically.
    fn groups(&self, active: &[&'m (String, ObjectMap)], class_wanted: bool) -> Vec<ScanGroup<'m>> {
        let mut by_key: BTreeMap<String, ScanGroup<'m>> = BTreeMap::new();
        for &pom in active {
            let (filters, subject_var, sole) = self.plan_predicate(&pom.0, Some(&pom.1));
            by_key
                .entry(filters_key(&filters))
                .or_insert_with(|| ScanGroup::new(filters, subject_var.clone(), sole))
                .add_pom(pom, subject_var.as_deref(), sole);
        }
        if class_wanted && self.map.subject_class.is_some() {
            let (filters, subject_var, sole) = self.plan_predicate(RDF_TYPE_IRI, None);
            by_key
                .entry(filters_key(&filters))
                .or_insert_with(|| ScanGroup::new(filters, subject_var.clone(), sole))
                .add_class(subject_var.as_deref(), sole);
        }
        by_key.into_values().collect()
    }

    /// `(sound filters, shared subject variable, sole producer)` for `predicate`'s triples.
    fn plan_predicate(
        &self,
        predicate: &str,
        obj: Option<&ObjectMap>,
    ) -> (Vec<ObdaFilter>, Option<String>, bool) {
        let shape = &self.fctx.shape;
        let subject = shape.agreed_term(predicate, TermSide::Subject);
        let mut filters: Vec<ObdaFilter> = subject
            .and_then(|s| subject_key_filter(self.map, s))
            .into_iter()
            .collect();
        if let (Some(obj), Some(object)) = (obj, shape.agreed_term(predicate, TermSide::Object)) {
            filters.extend(object_filters(obj, object, self.fctx));
        }
        let subject_var = match subject {
            Some(TermPattern::Variable(v)) => Some(v.as_str().to_string()),
            _ => None,
        };
        (filters, subject_var, self.producers(predicate) == 1)
    }

    /// How many triples maps (and predicate-object maps) in the virtual graph produce
    /// `predicate`.
    fn producers(&self, predicate: &str) -> usize {
        self.vg
            .triples_maps
            .iter()
            .map(|m| {
                let class = usize::from(predicate == RDF_TYPE_IRI && m.subject_class.is_some());
                class
                    + m.predicate_object_maps
                        .iter()
                        .filter(|(p, _)| p == predicate)
                        .count()
            })
            .sum()
    }
}

impl<'m> ScanGroup<'m> {
    fn new(filters: Vec<ObdaFilter>, subject_var: Option<String>, sole_producer: bool) -> Self {
        Self {
            poms: Vec::new(),
            class: false,
            filters,
            subject_var,
            sole_producer,
        }
    }

    fn add_pom(&mut self, pom: &'m (String, ObjectMap), subject_var: Option<&str>, sole: bool) {
        self.poms.push(pom);
        self.merge(subject_var, sole);
    }

    fn add_class(&mut self, subject_var: Option<&str>, sole: bool) {
        self.class = true;
        self.merge(subject_var, sole);
    }

    /// A group keeps a subject variable only while every member agrees on it.
    fn merge(&mut self, subject_var: Option<&str>, sole: bool) {
        if self.subject_var.as_deref() != subject_var {
            self.subject_var = None;
        }
        self.sole_producer &= sole;
    }

    /// A filtered group bound to one subject variable whose predicates no other map produces:
    /// every solution's subject is among its rows' keys.
    fn is_anchor(&self) -> bool {
        !self.filters.is_empty() && self.subject_var.is_some() && self.sole_producer
    }
}

/// A stable grouping key for a filter set (order-insensitive).
fn filters_key(filters: &[ObdaFilter]) -> String {
    let mut parts: Vec<String> = filters.iter().map(|f| format!("{f:?}")).collect();
    parts.sort();
    parts.dedup();
    parts.join("\u{1f}")
}

/// The distinct non-empty values of `column` over `rows`, in first-seen order.
fn distinct_keys(rows: &[ForeignRow], column: &str) -> Vec<String> {
    let mut seen = HashSet::new();
    rows.iter()
        .filter_map(|row| row.get(column))
        .filter(|v| !v.is_empty() && seen.insert(v.as_str()))
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests;
