//! Statements (UQL-11): the `UQL <n>;` version pragma, `EXPLAIN`/`PROFILE`, named
//! sub-plans (`LET name = pipeline;`) and the `FROM name` / `JOIN a, b` heads that turn
//! a program into a DAG of the same ops a [`crate::dag::PlanDag`] carries; and the
//! statement's row annotations, `WITH PROOF` (EH-448) and `WITH KNOWLEDGE` (EH-450).
//!
//! Bindings must be defined before use (so a program can never be cyclic) and every
//! binding must be used. A program whose main pipeline and bindings use only plain heads
//! (bindings consumed by `FUSE (a, b)` inlining) stays a linear [`Plan`].
//!
//! DAG node order is the DEPTH-FIRST order in which the main pipeline first references
//! each binding — deterministic, and exactly what `print::dag_to_uql` reproduces.

use std::collections::{BTreeMap, BTreeSet};

use eg_types::wire::{Op, Plan};

use super::Parser;
use crate::uql::diag::{UqlCode, UqlError, UqlWarnCode, UqlWarning};
use crate::uql::grammar::{role_of, Role};
use crate::uql::lexer::Tok;

/// The only UQL language version this parser speaks.
pub const UQL_VERSION: u32 = 1;

/// What the caller asked to do with the query.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    /// Execute and return rows.
    Run,
    /// Plan and cost, do not execute.
    Explain,
    /// Execute, recording per-op cardinality and time.
    Profile,
}

/// One node of a program DAG: the op and the node ids it consumes (empty ⇒ source; two
/// or more ⇒ the multi-input intersect-join of `dag_exec`).
#[derive(Clone, Debug, PartialEq)]
pub struct DagNode {
    pub op: Op,
    pub inputs: Vec<usize>,
}

/// A parsed program body.
#[derive(Clone, Debug, PartialEq)]
pub enum Body {
    /// A linear pipeline (every program without `FROM`/`JOIN`).
    Pipeline(Plan),
    /// A DAG; the sink is the last node.
    Dag(Vec<DagNode>),
}

/// Statement-level row annotations: `WITH PROOF`, `WITH KNOWLEDGE [(col, …)]`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Annotations {
    /// `WITH PROOF` (EH-448): each row carries why it is in the result.
    pub proof: bool,
    /// `WITH KNOWLEDGE [(col, …)]` (EH-450): each row carries its knowledge record,
    /// projecting the named columns; `None` when not requested.
    pub knowledge: Option<Vec<String>>,
}

impl Annotations {
    /// Whether the statement asked for any annotation.
    pub fn any(&self) -> bool {
        self.proof || self.knowledge.is_some()
    }
}

/// A parsed statement.
#[derive(Clone, Debug, PartialEq)]
pub struct Statement {
    pub version: u32,
    pub mode: Mode,
    pub body: Body,
    pub annotations: Annotations,
    pub warnings: Vec<UqlWarning>,
}

/// How a pipeline starts.
#[derive(Clone, Debug, PartialEq)]
pub(in crate::uql) enum Head {
    /// With its first stage.
    Plain,
    /// `FROM name` — continue from a binding's output.
    From(String),
    /// `JOIN a, b, …` — intersect several bindings' outputs into the first stage.
    Join(Vec<String>),
}

/// A parsed pipeline before materialization.
#[derive(Clone, Debug, PartialEq)]
pub(in crate::uql) struct Chain {
    pub head: Head,
    pub ops: Vec<Op>,
}

impl<'a> Parser<'a> {
    /// `statement = [ UQL int ; ] [ EXPLAIN | PROFILE ] { binding } pipeline [ annotations ]`.
    pub(in crate::uql) fn statement(&mut self) -> Result<Statement, UqlError> {
        let version = self.version_pragma()?;
        let mode = self.mode();
        while self.eat_kw("LET") {
            self.binding()?;
        }
        let main = self.chain()?;
        let annotations = self.annotations()?;
        if !self.at_end() {
            return Err(self
                .error(
                    UqlCode::TrailingTokens,
                    "unexpected trailing tokens after the query",
                )
                .with_help("separate pipeline stages with `|>`"));
        }
        let body = self.materialize(main)?;
        self.check_params_used()?;
        Ok(Statement {
            version,
            mode,
            body,
            annotations,
            warnings: self.take_warnings(),
        })
    }

    /// `annotations = WITH annotation { , annotation }`; none without `WITH`.
    fn annotations(&mut self) -> Result<Annotations, UqlError> {
        let mut out = Annotations::default();
        if !self.eat_kw("WITH") {
            return Ok(out);
        }
        self.annotation(&mut out)?;
        while self.eat(&Tok::Comma) {
            self.annotation(&mut out)?;
        }
        Ok(out)
    }

    /// `annotation = PROOF | KNOWLEDGE [ ( name { , name } ) ]`; each at most once.
    fn annotation(&mut self, out: &mut Annotations) -> Result<(), UqlError> {
        let span = self.cur_span();
        let repeated = if self.eat_kw("PROOF") {
            std::mem::replace(&mut out.proof, true)
        } else if self.eat_kw("KNOWLEDGE") {
            let columns = self.knowledge_columns()?;
            out.knowledge.replace(columns).is_some()
        } else {
            return Err(self
                .err_here("expected `PROOF` or `KNOWLEDGE` after `WITH`")
                .expecting(vec!["`PROOF`".into(), "`KNOWLEDGE`".into()]));
        };
        if repeated {
            return Err(UqlError::new(
                UqlCode::UnexpectedToken,
                "this annotation is already given",
                span,
            ));
        }
        Ok(())
    }

    /// The optional `( name { , name } )` column list of `WITH KNOWLEDGE`.
    fn knowledge_columns(&mut self) -> Result<Vec<String>, UqlError> {
        if !self.eat(&Tok::LParen) {
            return Ok(Vec::new());
        }
        let mut columns = vec![self.name("a column name")?];
        while self.eat(&Tok::Comma) {
            columns.push(self.name("a column name")?);
        }
        self.expect(&Tok::RParen, "`)` to close the KNOWLEDGE columns")?;
        Ok(columns)
    }

    fn version_pragma(&mut self) -> Result<u32, UqlError> {
        if !self.eat_kw("UQL") {
            return Ok(UQL_VERSION);
        }
        let span = self.cur_span();
        let version = self.parse_number::<u32>("a UQL version")?;
        self.expect(&Tok::Semi, "`;` after the version pragma")?;
        if version != UQL_VERSION {
            return Err(UqlError::new(
                UqlCode::UnsupportedVersion,
                format!(
                    "UQL version {version} is not supported (this engine speaks UQL {UQL_VERSION})"
                ),
                span,
            ));
        }
        Ok(version)
    }

    /// `EXPLAIN` / `PROFILE` prefix. `EXPLAIN BELIEF` is the epistemic stage, not a mode.
    fn mode(&mut self) -> Mode {
        if self.peek_kw("PROFILE") {
            self.bump();
            return Mode::Profile;
        }
        if self.peek_kw("EXPLAIN") && !self.peek_kw_at(1, "BELIEF") {
            self.bump();
            return Mode::Explain;
        }
        Mode::Run
    }

    /// `LET name = pipeline ;` (`LET` consumed).
    fn binding(&mut self) -> Result<(), UqlError> {
        let span = self.cur_span();
        let name = self.name("a binding name")?;
        if self.bindings.contains_key(&name) {
            return Err(UqlError::new(
                UqlCode::DuplicateBinding,
                format!("`{name}` is already bound"),
                span,
            ));
        }
        self.expect(&Tok::Eq, "`=` after the binding name")?;
        let chain = self.chain()?;
        self.expect(&Tok::Semi, "`;` to end the LET binding")?;
        self.bindings.insert(name, chain);
        Ok(())
    }

    /// `head { |> stage }`.
    fn chain(&mut self) -> Result<Chain, UqlError> {
        let mut ops = Vec::new();
        let head = if self.eat_kw("FROM") {
            let name = self.bound_name()?;
            self.referenced.insert(name.clone());
            Head::From(name)
        } else if self.eat_kw("JOIN") {
            let head = self.join_head()?;
            if let Head::Join(names) = &head {
                self.referenced.extend(names.iter().cloned());
            }
            head
        } else {
            self.head_stage(&mut ops)?;
            Head::Plain
        };
        let needs_stage = matches!(head, Head::Join(_));
        if needs_stage {
            self.expect(&Tok::Pipe, "`|>` and the stage the JOIN feeds")?;
            self.stage(&mut ops)?;
        }
        while self.eat(&Tok::Pipe) {
            self.stage(&mut ops)?;
        }
        Ok(Chain { head, ops })
    }

    fn join_head(&mut self) -> Result<Head, UqlError> {
        let mut names = vec![self.bound_name()?];
        self.expect(&Tok::Comma, "`,` — a JOIN takes at least two bindings")?;
        names.push(self.bound_name()?);
        while self.eat(&Tok::Comma) {
            names.push(self.bound_name()?);
        }
        Ok(Head::Join(names))
    }

    /// A reference to an already-defined binding.
    fn bound_name(&mut self) -> Result<String, UqlError> {
        let span = self.cur_span();
        let name = self.name("a binding name")?;
        if !self.bindings.contains_key(&name) {
            return Err(UqlError::new(
                UqlCode::UnknownBinding,
                format!("`{name}` is not bound (bindings must be defined with LET before use)"),
                span,
            ));
        }
        Ok(name)
    }

    /// The first stage of a plain pipeline, warning when it is a transform.
    fn head_stage(&mut self, ops: &mut Vec<Op>) -> Result<(), UqlError> {
        let span = self.cur_span();
        let keyword = match self.peek_kind() {
            Some(Tok::Ident(w)) => w.to_ascii_uppercase(),
            _ => String::new(),
        };
        self.stage(ops)?;
        if role_of(&keyword) == Some(Role::Stage) {
            self.warn(UqlWarning {
                code: UqlWarnCode::HeadTransformYieldsEmpty,
                msg: format!(
                    "`{keyword}` transforms its input, and at the head of a pipeline that \
                     input is empty — start with a source such as `MATCH ()`"
                ),
                at: span.0,
                end: span.1,
            });
        }
        Ok(())
    }

    /// `FUSE (name, …)`: the binding's ops, inlined as one branch.
    #[cfg(feature = "text")]
    pub(super) fn inline_binding(&mut self) -> Result<Vec<Op>, UqlError> {
        let span = self.cur_span();
        let name = self.bound_name()?;
        let chain = &self.bindings[&name];
        if chain.head != Head::Plain {
            return Err(UqlError::new(
                UqlCode::UnexpectedToken,
                format!("`{name}` starts with FROM/JOIN; a FUSE branch must be a plain pipeline"),
                span,
            ));
        }
        let ops = chain.ops.clone();
        self.inlined.insert(name);
        Ok(ops)
    }

    fn check_params_used(&self) -> Result<(), UqlError> {
        let unused: Vec<&String> = self
            .params
            .keys()
            .filter(|k| !self.used_params.contains(*k))
            .collect();
        if unused.is_empty() {
            return Ok(());
        }
        Err(UqlError::new(
            UqlCode::UnusedParameter,
            format!("bound parameter(s) never referenced: {unused:?}"),
            (0, 0),
        )
        .with_help("remove them, or reference them as `$name` (a typo?)"))
    }

    fn materialize(&mut self, main: Chain) -> Result<Body, UqlError> {
        let flat = main.head == Head::Plain && self.referenced.is_empty();
        let mut dag = Materializer::default();
        let body = if flat {
            Body::Pipeline(Plan::new(main.ops))
        } else {
            dag.chain(&main, &self.bindings);
            Body::Dag(dag.nodes)
        };
        let used: BTreeSet<&String> = dag.memo.keys().chain(self.inlined.iter()).collect();
        if let Some(unused) = self.bindings.keys().find(|k| !used.contains(k)) {
            return Err(UqlError::new(
                UqlCode::UnusedBinding,
                format!("`{unused}` is bound but never used"),
                (0, 0),
            ));
        }
        Ok(body)
    }
}

/// Lazily materializes bindings into DAG nodes in first-reference order.
#[derive(Default)]
struct Materializer {
    nodes: Vec<DagNode>,
    memo: BTreeMap<String, usize>,
}

impl Materializer {
    /// Append `chain`'s nodes; the id of its last node (its output).
    fn chain(&mut self, chain: &Chain, bindings: &BTreeMap<String, Chain>) -> usize {
        let mut inputs: Vec<usize> = match &chain.head {
            Head::Plain => Vec::new(),
            Head::From(name) => vec![self.binding(name, bindings)],
            Head::Join(names) => names.iter().map(|n| self.binding(n, bindings)).collect(),
        };
        let mut last = inputs.first().copied().unwrap_or(0);
        for op in &chain.ops {
            self.nodes.push(DagNode {
                op: op.clone(),
                inputs: std::mem::take(&mut inputs),
            });
            last = self.nodes.len() - 1;
            inputs = vec![last];
        }
        last
    }

    fn binding(&mut self, name: &str, bindings: &BTreeMap<String, Chain>) -> usize {
        if let Some(&id) = self.memo.get(name) {
            return id;
        }
        let id = self.chain(&bindings[name], bindings);
        self.memo.insert(name.to_string(), id);
        id
    }
}

/// The DAG as the executor's [`crate::dag::PlanDag`].
#[cfg(feature = "query")]
pub fn to_plan_dag(nodes: &[DagNode]) -> crate::dag::PlanDag {
    crate::dag::PlanDag::new(
        nodes
            .iter()
            .map(|n| crate::dag::PlanNode::new(n.op.clone(), n.inputs.clone()))
            .collect(),
    )
}
