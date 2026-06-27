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
/// The optional `props` inline-property map (`{k: v, …}`) is used ONLY on the write
/// path (CREATE/MERGE, CONCEPT:EG-020); the read parser always leaves it `None`.
#[derive(Debug, Clone, PartialEq)]
pub struct NodePat {
    pub var: Option<String>,
    pub label: Option<String>,
    /// Inline property map for a write pattern (`(n:L {k: v})`). `None` on reads.
    pub props: Option<Vec<(String, Value)>>,
}

/// `-[:REL]->` / `<-[:REL]-` / `-[:REL*1..3]->`. The relationship type, an optional
/// variable-length range, an optional edge VARIABLE (`-[r:REL]->`, write/DELETE), and
/// an optional inline property map (`-[:REL {since: 2020}]->`, write path).
#[derive(Debug, Clone, PartialEq)]
pub struct EdgePat {
    pub rel_type: Option<String>,
    pub direction: Direction,
    /// `Some((min,max))` for a `*min..max` variable-length path; `None` ⇒ a single
    /// fixed hop.
    pub var_len: Option<(usize, usize)>,
    /// The edge variable (`-[r:REL]->`), if named — used by `DELETE r` (CONCEPT:EG-020).
    pub var: Option<String>,
    /// Inline edge properties for a write pattern. `None` on reads.
    pub props: Option<Vec<(String, Value)>>,
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

// ── write statements (CONCEPT:EG-020) ────────────────────────────────────────

/// A whole parsed Cypher statement: a read query, or a write (CONCEPT:EG-020). The
/// existing `parse` entry-point still returns a [`CypherQuery`] (reads, unchanged);
/// the new `parse_statement` returns this enum so the executor routes reads to the
/// untouched snapshot path and writes to the native-op write path.
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    /// `MATCH … WHERE … RETURN … [LIMIT k]` — read-only (the unchanged path).
    Read(CypherQuery),
    /// A mutation: an optional leading `MATCH … WHERE …` that binds variables, then
    /// one or more write clauses (`CREATE`/`MERGE`/`SET`/`DELETE`), with an optional
    /// trailing `RETURN`.
    Write(WriteQuery),
}

/// A parsed write statement (CONCEPT:EG-020).
#[derive(Debug, Clone, PartialEq)]
pub struct WriteQuery {
    /// Optional leading `MATCH <pattern>` binding existing nodes for the write clauses.
    pub match_pattern: Option<Pattern>,
    /// `WHERE` over the matched binding (conjunctive). Empty ⇒ no filter.
    pub where_clause: Vec<Predicate>,
    /// The ordered write clauses applied per matched binding (or once when no MATCH).
    pub ops: Vec<WriteOp>,
    /// Optional trailing `RETURN` projecting the post-write bindings.
    pub returns: Vec<ReturnItem>,
}

/// One write clause (CONCEPT:EG-020).
#[derive(Debug, Clone, PartialEq)]
pub enum WriteOp {
    /// `CREATE <pattern>` — create the pattern's nodes (with inline props) and the
    /// edges between consecutive nodes. A node whose variable is already bound (by a
    /// preceding MATCH or earlier CREATE) is reused, not recreated.
    Create(Pattern),
    /// `MERGE (n:Label {props})` — match a single node by label + all inline props;
    /// create it iff absent. Idempotent. Binds `n` to the matched-or-created node.
    Merge(NodePat),
    /// `SET v.prop = literal [, …]` — assign properties on bound node variables.
    Set(Vec<SetItem>),
    /// `[DETACH] DELETE v [, …]` — delete bound node/edge variables. `detach` removes
    /// a node's incident edges first (a plain node DELETE with edges is rejected).
    Delete { vars: Vec<String>, detach: bool },
}

/// One `SET v.prop = literal` assignment (CONCEPT:EG-020).
#[derive(Debug, Clone, PartialEq)]
pub struct SetItem {
    pub var: String,
    pub prop: String,
    pub value: Value,
}
