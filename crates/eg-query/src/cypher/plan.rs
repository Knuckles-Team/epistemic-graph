//! The Cypher-subset AST (CONCEPT:KG-2.179). Pure data — the parser builds it,
//! the executor consumes it. No engine deps here.

use serde_json::Value;

/// A whole parsed query: `MATCH <pattern> [WHERE <preds>] RETURN <items> [LIMIT k]`.
#[derive(Debug, Clone, PartialEq)]
pub struct CypherQuery {
    /// The linear path pattern: one node, then zero-or-more (edge, node) hops.
    pub pattern: Pattern,
    /// Conjunctive WHERE predicates (all must hold). Empty ⇒ no filter.
    pub where_clause: Vec<Predicate>,
    /// What to project. Each item is a bound variable or a `var.prop` access.
    pub returns: Vec<ReturnItem>,
    /// `LIMIT k` (`None` ⇒ the implicit cap in the executor).
    pub limit: Option<usize>,
}

/// A linear MATCH path: `node (edge node)*`. We support the connected-path subset
/// (no comma-separated disjoint patterns), which is what compiles cleanly to one
/// VF2 match / one BFS.
#[derive(Debug, Clone, PartialEq)]
pub struct Pattern {
    pub start: NodePat,
    pub hops: Vec<(EdgePat, NodePat)>,
}

/// `(var:Label)` — both parts optional (`()`, `(a)`, `(:Label)`, `(a:Label)`).
#[derive(Debug, Clone, PartialEq)]
pub struct NodePat {
    pub var: Option<String>,
    pub label: Option<String>,
}

/// `-[:REL]->` / `<-[:REL]-` / `-[:REL*1..3]->`. Only the relationship type and
/// an optional variable-length range are modeled.
#[derive(Debug, Clone, PartialEq)]
pub struct EdgePat {
    pub rel_type: Option<String>,
    pub direction: Direction,
    /// `Some((min,max))` for a `*min..max` variable-length path; `None` ⇒ a single
    /// fixed hop.
    pub var_len: Option<(usize, usize)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    /// `-[..]->`
    Right,
    /// `<-[..]-`
    Left,
}

/// `a.prop <op> <literal>`.
#[derive(Debug, Clone, PartialEq)]
pub struct Predicate {
    pub var: String,
    pub prop: String,
    pub op: CompareOp,
    pub value: Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompareOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

/// A RETURN projection: a bare bound variable (`a` ⇒ the node id) or a property
/// access (`a.prop` ⇒ that field from the node's properties).
#[derive(Debug, Clone, PartialEq)]
pub struct ReturnItem {
    pub var: String,
    /// `Some(prop)` ⇒ `var.prop`; `None` ⇒ the bare variable (its node id).
    pub prop: Option<String>,
}

impl ReturnItem {
    /// The column name this item produces (`a` or `a.prop`).
    pub fn column(&self) -> String {
        match &self.prop {
            Some(p) => format!("{}.{}", self.var, p),
            None => self.var.clone(),
        }
    }
}
