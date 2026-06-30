//! The Cypher-subset AST (CONCEPT:KG-2.179). Pure data — the parser builds it,
//! the executor consumes it. No engine deps here.
//!
//! Read clauses (CONCEPT:EG-062): a read query is a sequence of reading stages
//! (`MATCH` / `OPTIONAL MATCH` / `WITH`) terminated by a `RETURN`. `WHERE` is a
//! boolean expression tree (`OR`/`AND` + leaf tests `IN`/`STARTS WITH`/`CONTAINS`/
//! `ENDS WITH`/`IS [NOT] NULL`/comparison). `RETURN`/`WITH` items support
//! aggregation (`count`/`collect`/`sum`/`avg`/`min`/`max`), `DISTINCT`, `*`,
//! `ORDER BY`, `SKIP` and `LIMIT`.
//!
//! Variable-length generalization (CONCEPT:EG-063): a pattern may combine fixed
//! hops with a single variable-length hop, and bind a path variable (`p = (…)`).

use serde_json::Value;

/// A whole parsed read query: one-or-more reading stages then a `RETURN`
/// (CONCEPT:EG-062).
#[derive(Debug, Clone, PartialEq)]
pub struct CypherQuery {
    /// The reading stages, in order. The first is always a `MATCH`.
    pub stages: Vec<ReadStage>,
    /// The terminal `RETURN` projection (+ DISTINCT/ORDER BY/SKIP/LIMIT).
    pub ret: ReturnSpec,
}

/// One reading stage (CONCEPT:EG-062).
#[derive(Debug, Clone, PartialEq)]
pub enum ReadStage {
    /// `[OPTIONAL] MATCH [p =] <pattern> [WHERE <expr>]`. When `optional`, a binding
    /// that fails to extend is kept with the stage's new variables left unbound
    /// (projected as `null`).
    Match {
        pattern: Pattern,
        optional: bool,
        where_clause: Option<WhereExpr>,
        /// `p = (…)` path-variable binding (CONCEPT:EG-063). `None` ⇒ no path var.
        path_var: Option<String>,
    },
    /// `WITH <items> [WHERE <expr>]` — project/rename the carried variables and
    /// optionally post-filter, pipelining the result to the next stage.
    With {
        items: Vec<WithItem>,
        where_clause: Option<WhereExpr>,
    },
}

/// One `WITH` projection item: a variable, optionally aliased (`a AS b`).
#[derive(Debug, Clone, PartialEq)]
pub struct WithItem {
    pub var: String,
    pub alias: Option<String>,
}

/// A linear MATCH path: `node (edge node)*`. We support the connected-path subset
/// (no comma-separated disjoint patterns), which is what compiles cleanly to one
/// VF2 match / one BFS / one incremental walk.
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompareOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

// ── WHERE boolean expressions (CONCEPT:EG-062) ───────────────────────────────

/// A WHERE boolean expression tree: disjunctions of conjunctions of leaf
/// conditions (CONCEPT:EG-062).
#[derive(Debug, Clone, PartialEq)]
pub enum WhereExpr {
    Or(Vec<WhereExpr>),
    And(Vec<WhereExpr>),
    Cond(Condition),
}

/// A leaf WHERE condition over a `var.prop` access (CONCEPT:EG-062).
#[derive(Debug, Clone, PartialEq)]
pub struct Condition {
    pub var: String,
    pub prop: String,
    pub test: Test,
}

/// What a [`Condition`] tests against the resolved `var.prop` value (CONCEPT:EG-062).
#[derive(Debug, Clone, PartialEq)]
pub enum Test {
    /// `<op> <literal>`.
    Cmp(CompareOp, Value),
    /// `IN [l1, l2, …]`.
    In(Vec<Value>),
    /// `STARTS WITH 's'`.
    StartsWith(String),
    /// `ENDS WITH 's'`.
    EndsWith(String),
    /// `CONTAINS 's'`.
    Contains(String),
    /// `IS NULL`.
    IsNull,
    /// `IS NOT NULL`.
    IsNotNull,
}

// ── RETURN projection (CONCEPT:EG-062) ───────────────────────────────────────

/// The terminal `RETURN` (CONCEPT:EG-062): items + DISTINCT/`*`/ORDER BY/SKIP/LIMIT.
#[derive(Debug, Clone, PartialEq)]
pub struct ReturnSpec {
    pub items: Vec<ReturnItem>,
    /// `RETURN *` — project every in-scope variable.
    pub star: bool,
    /// `RETURN DISTINCT …`.
    pub distinct: bool,
    pub order_by: Vec<OrderKey>,
    pub skip: Option<usize>,
    pub limit: Option<usize>,
}

/// A RETURN projection item: an expression, optionally aliased (`expr AS name`).
#[derive(Debug, Clone, PartialEq)]
pub struct ReturnItem {
    pub expr: Expr,
    pub alias: Option<String>,
}

impl ReturnItem {
    /// The column name this item produces (`alias` if present, else the expr text).
    pub fn column(&self) -> String {
        match &self.alias {
            Some(a) => a.clone(),
            None => self.expr.column(),
        }
    }
}

/// A projection / ORDER BY expression (CONCEPT:EG-062).
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    /// A bare bound variable (`a` ⇒ its node id).
    Var(String),
    /// A property access (`a.prop`).
    Prop(String, String),
    /// `count(*)`.
    CountStar,
    /// An aggregation over a variable or `var.prop` (CONCEPT:EG-062).
    Aggregate(AggFunc, AggArg),
}

impl Expr {
    /// The default column name for this expression (no alias).
    pub fn column(&self) -> String {
        match self {
            Expr::Var(v) => v.clone(),
            Expr::Prop(v, p) => format!("{v}.{p}"),
            Expr::CountStar => "count(*)".to_string(),
            Expr::Aggregate(f, a) => format!("{}({})", f.name(), a.text()),
        }
    }
}

/// The argument to a (non-`count(*)`) aggregate.
#[derive(Debug, Clone, PartialEq)]
pub enum AggArg {
    Var(String),
    Prop(String, String),
}

impl AggArg {
    pub fn text(&self) -> String {
        match self {
            AggArg::Var(v) => v.clone(),
            AggArg::Prop(v, p) => format!("{v}.{p}"),
        }
    }
}

/// The supported aggregation functions (CONCEPT:EG-062).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggFunc {
    Count,
    Collect,
    Sum,
    Avg,
    Min,
    Max,
}

impl AggFunc {
    pub fn name(&self) -> &'static str {
        match self {
            AggFunc::Count => "count",
            AggFunc::Collect => "collect",
            AggFunc::Sum => "sum",
            AggFunc::Avg => "avg",
            AggFunc::Min => "min",
            AggFunc::Max => "max",
        }
    }
}

/// One `ORDER BY` key: an expression and a direction.
#[derive(Debug, Clone, PartialEq)]
pub struct OrderKey {
    pub expr: Expr,
    pub desc: bool,
}

// ── write statements (CONCEPT:EG-020) ────────────────────────────────────────

/// A whole parsed Cypher statement: a read query, or a write (CONCEPT:EG-020). The
/// existing `parse` entry-point still returns a [`CypherQuery`] (reads, unchanged);
/// the new `parse_statement` returns this enum so the executor routes reads to the
/// untouched snapshot path and writes to the native-op write path.
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    /// A read query (the unchanged snapshot path).
    Read(CypherQuery),
    /// A mutation: an optional leading `MATCH … WHERE …` that binds variables, then
    /// one or more write clauses (`CREATE`/`MERGE`/`SET`/`DELETE`/`REMOVE`), with an
    /// optional trailing `RETURN`.
    Write(WriteQuery),
}

/// A parsed write statement (CONCEPT:EG-020 / EG-061).
#[derive(Debug, Clone, PartialEq)]
pub struct WriteQuery {
    /// Optional leading `MATCH <pattern>` binding existing nodes for the write clauses.
    pub match_pattern: Option<Pattern>,
    /// `WHERE` over the matched binding. `None` ⇒ no filter.
    pub where_clause: Option<WhereExpr>,
    /// The ordered write clauses applied per matched binding (or once when no MATCH).
    pub ops: Vec<WriteOp>,
    /// Optional trailing `RETURN` projecting the post-write bindings (no aggregation).
    pub returns: Vec<ReturnItem>,
}

/// One write clause (CONCEPT:EG-020 / EG-061).
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
    /// `REMOVE v.prop | v:Label [, …]` — delete a property or remove a label from a
    /// bound node variable (CONCEPT:EG-061).
    Remove(Vec<RemoveItem>),
}

/// One `REMOVE` target (CONCEPT:EG-061): a property delete or a label removal.
#[derive(Debug, Clone, PartialEq)]
pub enum RemoveItem {
    /// `REMOVE v.prop` — delete the property from the bound node.
    Property { var: String, prop: String },
    /// `REMOVE v:Label` — remove the label from the bound node.
    Label { var: String, label: String },
}

/// One `SET v.prop = literal` assignment (CONCEPT:EG-020).
#[derive(Debug, Clone, PartialEq)]
pub struct SetItem {
    pub var: String,
    pub prop: String,
    pub value: Value,
}
