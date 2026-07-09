//! The Cypher-subset AST (CONCEPT:EG-KG.query.dep-free-behind). Pure data — the parser builds it,
//! the executor consumes it. No engine deps here.
//!
//! Read clauses (CONCEPT:EG-KG.query.eg-extend-read-side): a read query is a sequence of reading stages
//! (`MATCH` / `OPTIONAL MATCH` / `WITH`) terminated by a `RETURN`. `WHERE` is a
//! boolean expression tree (`OR`/`AND` + leaf tests `IN`/`STARTS WITH`/`CONTAINS`/
//! `ENDS WITH`/`IS [NOT] NULL`/comparison). `RETURN`/`WITH` items support
//! aggregation (`count`/`collect`/`sum`/`avg`/`min`/`max`), `DISTINCT`, `*`,
//! `ORDER BY`, `SKIP` and `LIMIT`.
//!
//! Variable-length generalization (CONCEPT:EG-KG.query.concept-2): a pattern may combine fixed
//! hops with a single variable-length hop, and bind a path variable (`p = (…)`).
//!
//! Quantified path patterns (CONCEPT:EG-KG.query.quantified-path-pattern, Cypher 25):
//! `((a)-[:REL]->(b)){1,3}` repeats a whole inner sub-pattern (not just one
//! relationship) `min..max` times; see [`QuantifiedGroup`].

use serde_json::Value;

/// A whole parsed read query: one-or-more reading stages then a `RETURN`
/// (CONCEPT:EG-KG.query.eg-extend-read-side).
#[derive(Debug, Clone, PartialEq)]
pub struct CypherQuery {
    /// The reading stages, in order. The first is always a `MATCH`.
    pub stages: Vec<ReadStage>,
    /// The terminal `RETURN` projection (+ DISTINCT/ORDER BY/SKIP/LIMIT).
    pub ret: ReturnSpec,
}

/// One reading stage (CONCEPT:EG-KG.query.eg-extend-read-side).
#[derive(Debug, Clone, PartialEq)]
pub enum ReadStage {
    /// `[OPTIONAL] MATCH [p =] <pattern> [WHERE <expr>]`. When `optional`, a binding
    /// that fails to extend is kept with the stage's new variables left unbound
    /// (projected as `null`).
    Match {
        pattern: Pattern,
        optional: bool,
        where_clause: Option<WhereExpr>,
        /// `p = (…)` path-variable binding (CONCEPT:EG-KG.query.concept-2). `None` ⇒ no path var.
        path_var: Option<String>,
    },
    /// `WITH <items> [WHERE <expr>]` — project/rename the carried variables and
    /// optionally post-filter, pipelining the result to the next stage.
    With {
        items: Vec<WithItem>,
        where_clause: Option<WhereExpr>,
    },
    /// `UNWIND <list> AS <var>` (CONCEPT:EG-KG.query.param-list-drives-unwind) — expand a list expression into one
    /// row per element, binding each element to `var`, pipelining downstream.
    Unwind { list: ListExpr, var: String },
    /// `CALL { <subquery> }` (CONCEPT:EG-KG.query.cypher-planning) — an evaluated read sub-query whose
    /// result rows join (cartesian) onto each incoming row.
    Call { subquery: Box<CypherQuery> },
    /// `CALL proc.name(args) YIELD col [AS alias], …` (CONCEPT:EG-KG.query.cypher-planning) — a registered
    /// procedure invocation; each yielded column binds into the pipeline.
    CallProc {
        name: String,
        args: Vec<PropVal>,
        yields: Vec<YieldItem>,
    },
}

/// One `YIELD col [AS alias]` item on a `CALL proc(...)` stage (CONCEPT:EG-KG.query.cypher-planning).
#[derive(Debug, Clone, PartialEq)]
pub struct YieldItem {
    pub col: String,
    pub alias: Option<String>,
}

/// The list operand of an `UNWIND` (CONCEPT:EG-KG.query.param-list-drives-unwind): an inline list literal, a query
/// parameter (`$name`), or a bound variable that already holds a list value.
#[derive(Debug, Clone, PartialEq)]
pub enum ListExpr {
    /// `[e1, e2, …]` — each element is a [`PropVal`] (literal / `$param` / var-ref).
    List(Vec<PropVal>),
    /// `$name` — a query parameter expected to hold a JSON array.
    Param(String),
    /// A bound variable holding a list (e.g. a path-var or a `$param`-bound list).
    Ref(String),
}

/// A property-map / argument value (CONCEPT:EG-KG.query.param-list-drives-unwind/142): a literal, a query parameter
/// (`$name`), or a reference to a bound variable. Read-side inline property maps
/// (`(n {id: x})`) and procedure args resolve `Param`/`Ref` against the live params +
/// binding; the write path resolves them the same way when realizing nodes/edges.
#[derive(Debug, Clone, PartialEq)]
pub enum PropVal {
    Lit(Value),
    Param(String),
    Ref(String),
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
/// path (CREATE/MERGE, CONCEPT:EG-KG.query.register-each-user-table); the read parser always leaves it `None`.
#[derive(Debug, Clone, PartialEq)]
pub struct NodePat {
    pub var: Option<String>,
    pub label: Option<String>,
    /// Inline property map (`(n:L {k: v})`). On the write path these realize the
    /// created/merged node; on the read path (CONCEPT:EG-KG.query.param-list-drives-unwind) they constrain the
    /// matched node (`(n {id: x})`). Values may reference params/bound vars.
    pub props: Option<Vec<(String, PropVal)>>,
}

/// `-[:REL]->` / `<-[:REL]-` / `-[:REL*1..3]->`. The relationship type, an optional
/// variable-length range, an optional edge VARIABLE (`-[r:REL]->`, write/DELETE), and
/// an optional inline property map (`-[:REL {since: 2020}]->`, write path).
#[derive(Debug, Clone, PartialEq)]
pub struct EdgePat {
    pub rel_type: Option<String>,
    pub direction: Direction,
    /// `Some((min,max))` for a `*min..max` variable-length path; `None` ⇒ a single
    /// fixed hop (or, when `group` is `Some`, this field is unused — the group
    /// carries its own quantifier).
    pub var_len: Option<(usize, usize)>,
    /// The edge variable (`-[r:REL]->`), if named — used by `DELETE r` (CONCEPT:EG-KG.query.register-each-user-table).
    pub var: Option<String>,
    /// Inline edge properties for a write pattern. `None` on reads. Values may
    /// reference params/bound vars (CONCEPT:EG-KG.query.param-list-drives-unwind).
    pub props: Option<Vec<(String, PropVal)>>,
    /// Cypher 25 quantified path pattern (CONCEPT:EG-KG.query.quantified-path-pattern):
    /// `((a)-[:REL]->(b)){min,max}`. When `Some`, this hop is a synthetic
    /// placeholder standing in for a whole repeated sub-pattern — `rel_type`/
    /// `direction`/`var_len` above are ignored and the group's own `hops` +
    /// `quantifier` drive matching (`group_reachable` in `exec.rs`), which
    /// generalizes the single-relationship `bfs_reachable` to repeated
    /// whole-subpattern expansion. `None` for every ordinary hop (the common
    /// case, unaffected).
    pub group: Option<Box<QuantifiedGroup>>,
}

/// The inner sub-pattern + repetition bounds of a Cypher 25 quantified path
/// pattern `((start)(hop)*){min,max}` (CONCEPT:EG-KG.query.quantified-path-pattern).
/// Matched by repeating whole-pattern expansion `min..=max` times (BFS over
/// "meta-edges", each meta-edge being one full application of `hops` starting
/// from `start`'s position) — see `group_reachable`/`expand_group_once` in
/// `exec.rs`. **Documented limitation (deferred):** only the group's overall
/// start (already bound going in) and the final repetition's end node
/// participate in the surrounding MATCH/WHERE/RETURN scope; per-iteration
/// variable bindings inside the group (e.g. every `a`/`b` across repetitions)
/// are NOT exposed as list values the way full Cypher 25 exposes them — this
/// engine matches reachability + the end node, not per-iteration projections.
#[derive(Debug, Clone, PartialEq)]
pub struct QuantifiedGroup {
    /// The group's own start-position constraint (label/props/var), re-applied
    /// at the start of EVERY repetition — but the var (if any) is local to the
    /// group and not threaded to the outer binding.
    pub start: NodePat,
    /// The repeated hop sequence (one or more), applied once per repetition.
    pub hops: Vec<(EdgePat, NodePat)>,
    /// `(min, max)` repetitions, both inclusive, `min` may be `0`.
    pub quantifier: (usize, usize),
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

// ── WHERE boolean expressions (CONCEPT:EG-KG.query.eg-extend-read-side) ───────────────────────────────

/// A WHERE boolean expression tree: disjunctions of conjunctions of leaf
/// conditions (CONCEPT:EG-KG.query.eg-extend-read-side).
#[derive(Debug, Clone, PartialEq)]
pub enum WhereExpr {
    Or(Vec<WhereExpr>),
    And(Vec<WhereExpr>),
    Cond(Condition),
}

/// A leaf WHERE condition over a `var.prop` access (CONCEPT:EG-KG.query.eg-extend-read-side).
#[derive(Debug, Clone, PartialEq)]
pub struct Condition {
    pub var: String,
    pub prop: String,
    pub test: Test,
}

/// What a [`Condition`] tests against the resolved `var.prop` value (CONCEPT:EG-KG.query.eg-extend-read-side).
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

// ── RETURN projection (CONCEPT:EG-KG.query.eg-extend-read-side) ───────────────────────────────────────

/// The terminal `RETURN` (CONCEPT:EG-KG.query.eg-extend-read-side): items + DISTINCT/`*`/ORDER BY/SKIP/LIMIT.
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

/// A projection / ORDER BY expression (CONCEPT:EG-KG.query.eg-extend-read-side).
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    /// A bare bound variable (`a` ⇒ its node id).
    Var(String),
    /// A property access (`a.prop`).
    Prop(String, String),
    /// `count(*)`.
    CountStar,
    /// An aggregation over a variable or `var.prop` (CONCEPT:EG-KG.query.eg-extend-read-side).
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

/// The supported aggregation functions (CONCEPT:EG-KG.query.eg-extend-read-side).
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

// ── write statements (CONCEPT:EG-KG.query.register-each-user-table) ────────────────────────────────────────

/// A whole parsed Cypher statement: a read query, or a write (CONCEPT:EG-KG.query.register-each-user-table). The
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

/// A parsed write statement (CONCEPT:EG-KG.query.register-each-user-table / EG-061).
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

/// One write clause (CONCEPT:EG-KG.query.register-each-user-table / EG-061).
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
    /// bound node variable (CONCEPT:EG-KG.query.cypher-execution).
    Remove(Vec<RemoveItem>),
}

/// One `REMOVE` target (CONCEPT:EG-KG.query.cypher-execution): a property delete or a label removal.
#[derive(Debug, Clone, PartialEq)]
pub enum RemoveItem {
    /// `REMOVE v.prop` — delete the property from the bound node.
    Property { var: String, prop: String },
    /// `REMOVE v:Label` — remove the label from the bound node.
    Label { var: String, label: String },
}

/// One `SET v.prop = literal` assignment (CONCEPT:EG-KG.query.register-each-user-table).
#[derive(Debug, Clone, PartialEq)]
pub struct SetItem {
    pub var: String,
    pub prop: String,
    pub value: Value,
}
