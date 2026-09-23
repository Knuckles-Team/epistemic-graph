//! Recursive descent over DecideText tokens into a typed request.

use std::collections::BTreeMap;

use eg_types::agent_component::{AgentComponentKind, ComponentDependency};
use eg_types::contract::BoundedVec;
use eg_types::decision::statistical::{
    CandidateSource, DecideRequest, QuestionKind, QuestionSafety, StatisticalQuestion, TypedParam,
    TypedValue,
};
use eg_types::decision::{
    AssemblyConstraints, AssemblyRequest, AssemblyRequirements, DecisionPolicyRef,
    LibraryCandidateScope,
};

use super::lexer::{Spanned, Tok};
use super::{DecideTextError, DecideTextErrorKind, DecideTextRequest};

enum Source {
    Library(LibraryCandidateScope),
    Graph { graph: String, plan: crate::Plan },
}

struct Question {
    question: StatisticalQuestion,
    features: ComponentDependency,
    head: Option<ComponentDependency>,
    max_records: Option<u16>,
}

enum Terminal {
    Decide(Box<Question>),
    Assemble { max_components: Option<u32> },
}

pub(super) struct Parser<'a> {
    toks: &'a [Spanned],
    pos: usize,
    end: usize,
    tenant_id: &'a str,
    params: &'a BTreeMap<String, TypedValue>,
    covers: Option<Vec<String>>,
    policy: DecisionPolicyRef,
    terminal: Option<Terminal>,
}

fn from_word<T: serde::de::DeserializeOwned>(
    word: &str,
    what: &str,
    at: usize,
) -> Result<T, DecideTextError> {
    serde_json::from_value(serde_json::Value::String(word.to_ascii_lowercase())).map_err(|_| {
        DecideTextError::new(
            DecideTextErrorKind::Syntax,
            format!("`{word}` is not a {what}"),
            at,
        )
    })
}

impl<'a> Parser<'a> {
    pub(super) fn new(
        toks: &'a [Spanned],
        end: usize,
        tenant_id: &'a str,
        params: &'a BTreeMap<String, TypedValue>,
    ) -> Self {
        Self {
            toks,
            pos: 0,
            end,
            tenant_id,
            params,
            covers: None,
            policy: DecisionPolicyRef::Default,
            terminal: None,
        }
    }

    fn at(&self) -> usize {
        self.toks.get(self.pos).map_or(self.end, |(_, at)| *at)
    }

    fn err(&self, kind: DecideTextErrorKind, msg: impl Into<String>) -> DecideTextError {
        DecideTextError::new(kind, msg, self.at())
    }

    fn peek(&self) -> Option<&'a Tok> {
        self.toks.get(self.pos).map(|(tok, _)| tok)
    }

    fn peek_kw(&self, kw: &str) -> bool {
        matches!(self.peek(), Some(Tok::Word(w)) if w.eq_ignore_ascii_case(kw))
    }

    fn eat_kw(&mut self, kw: &str) -> bool {
        let hit = self.peek_kw(kw);
        self.pos += usize::from(hit);
        hit
    }

    fn eat(&mut self, tok: &Tok) -> bool {
        let hit = self.peek() == Some(tok);
        self.pos += usize::from(hit);
        hit
    }

    fn expect_kw(&mut self, kw: &str) -> Result<(), DecideTextError> {
        if self.eat_kw(kw) {
            return Ok(());
        }
        Err(self.err(DecideTextErrorKind::Syntax, format!("expected `{kw}`")))
    }

    fn take(
        &mut self,
        what: &str,
        pick: impl Fn(&Tok) -> Option<String>,
    ) -> Result<String, DecideTextError> {
        let value = self.peek().and_then(&pick);
        let Some(value) = value else {
            return Err(self.err(DecideTextErrorKind::Syntax, format!("expected {what}")));
        };
        self.pos += 1;
        Ok(value)
    }

    fn word(&mut self, what: &str) -> Result<String, DecideTextError> {
        self.take(what, |t| match t {
            Tok::Word(w) => Some(w.clone()),
            _ => None,
        })
    }

    fn string(&mut self, what: &str) -> Result<String, DecideTextError> {
        self.take(what, |t| match t {
            Tok::Str(s) => Some(s.clone()),
            _ => None,
        })
    }

    fn integer<T: std::str::FromStr>(&mut self) -> Result<T, DecideTextError> {
        let at = self.at();
        let word = self.word("an integer")?;
        word.parse().map_err(|_| {
            DecideTextError::new(DecideTextErrorKind::Syntax, "expected an integer", at)
        })
    }

    fn pin(&mut self, kind: AgentComponentKind) -> Result<ComponentDependency, DecideTextError> {
        let component_id = self.string("a quoted component id")?;
        self.expect_kw("AT")?;
        let definition_digest = self.string("a quoted definition digest")?;
        Ok(ComponentDependency {
            component_id,
            kind,
            definition_digest,
        })
    }

    fn kinds(&mut self) -> Result<Vec<AgentComponentKind>, DecideTextError> {
        if !self.eat(&Tok::LBracket) {
            return Err(self.err(DecideTextErrorKind::Syntax, "expected `[` before the kinds"));
        }
        let mut kinds = Vec::new();
        loop {
            let at = self.at();
            kinds.push(from_word(
                &self.word("a component kind")?,
                "component kind",
                at,
            )?);
            if self.eat(&Tok::RBracket) {
                return Ok(kinds);
            }
            if !self.eat(&Tok::Comma) {
                return Err(self.err(DecideTextErrorKind::Syntax, "expected `,` or `]`"));
            }
        }
    }

    fn library(&mut self) -> Result<Source, DecideTextError> {
        self.expect_kw("LIBRARY")?;
        self.expect_kw("KINDS")?;
        let kinds = self.kinds()?;
        let classification_under = if self.eat_kw("UNDER") {
            Some(self.word("an IRI")?)
        } else {
            None
        };
        let kinds = BoundedVec::new(kinds).map_err(|m| self.err(DecideTextErrorKind::Syntax, m))?;
        Ok(Source::Library(LibraryCandidateScope {
            kinds,
            classification_under,
        }))
    }

    fn graph(&mut self) -> Result<Source, DecideTextError> {
        let graph = self.string("a quoted graph name")?;
        self.expect_kw("QUERY")?;
        let at = self.at();
        let text = self.take("a `{ uql }` candidate query", |t| match t {
            Tok::Block(b) => Some(b.clone()),
            _ => None,
        })?;
        let plan = crate::uql::parse(&text).map_err(|e| {
            DecideTextError::new(DecideTextErrorKind::CandidateQuery, e.to_string(), at)
        })?;
        Ok(Source::Graph { graph, plan })
    }

    fn source(&mut self) -> Result<Source, DecideTextError> {
        self.expect_kw("CANDIDATES")?;
        if self.eat_kw("AGENT") {
            return self.library();
        }
        if self.eat_kw("GRAPH") {
            return self.graph();
        }
        Err(self.err(
            DecideTextErrorKind::Syntax,
            "expected `AGENT LIBRARY` or `GRAPH`",
        ))
    }

    fn bound(&mut self) -> Result<&'a TypedValue, DecideTextError> {
        let at = self.at();
        let name = self.take("an `@parameter`", |t| match t {
            Tok::Param(p) => Some(p.clone()),
            _ => None,
        })?;
        self.params.get(&name).ok_or_else(|| {
            DecideTextError::new(
                DecideTextErrorKind::UnboundParameter,
                format!("@{name} is not bound"),
                at,
            )
        })
    }

    fn covers(&mut self) -> Result<(), DecideTextError> {
        let at = self.at();
        let TypedValue::IriList(iris) = self.bound()? else {
            return Err(DecideTextError::new(
                DecideTextErrorKind::ParameterType,
                "COVERS needs an iri_list",
                at,
            ));
        };
        self.covers = Some(iris.iter().cloned().collect());
        Ok(())
    }

    fn validate(&mut self) -> Result<(), DecideTextError> {
        self.expect_kw("POLICY")?;
        if !self.eat_kw("DEFAULT") {
            let component = self.pin(AgentComponentKind::DecisionPolicy)?;
            self.policy = DecisionPolicyRef::Pinned { component };
        }
        Ok(())
    }

    fn decide(&mut self) -> Result<Terminal, DecideTextError> {
        let at = self.at();
        let kind: QuestionKind = from_word(&self.word("a question kind")?, "question kind", at)?;
        self.expect_kw("QUESTION")?;
        let question_id = self.string("a quoted question id")?;
        let safety = if self.eat_kw("SAFETY") {
            let at = self.at();
            from_word(&self.word("a safety class")?, "safety class", at)?
        } else {
            QuestionSafety::Ordinary
        };
        self.expect_kw("FEATURES")?;
        let features = self.pin(AgentComponentKind::FeatureSchema)?;
        let head = if self.eat_kw("HEAD") {
            Some(self.pin(AgentComponentKind::DecisionHead)?)
        } else {
            None
        };
        let max_records = if self.eat_kw("MAX") {
            Some(self.integer()?)
        } else {
            None
        };
        Ok(Terminal::Decide(Box::new(Question {
            question: StatisticalQuestion {
                question_id,
                kind,
                safety,
            },
            features,
            head,
            max_records,
        })))
    }

    fn assemble(&mut self) -> Result<Terminal, DecideTextError> {
        let max_components = if self.eat_kw("MAX") {
            self.expect_kw("COMPONENTS")?;
            Some(self.integer()?)
        } else {
            None
        };
        Ok(Terminal::Assemble { max_components })
    }

    fn clause(&mut self) -> Result<(), DecideTextError> {
        if self.terminal.is_some() {
            return Err(self.err(
                DecideTextErrorKind::MissingDecision,
                "DECIDE / ASSEMBLE must be the last clause",
            ));
        }
        if self.eat_kw("COVERS") {
            return self.covers();
        }
        if self.eat_kw("VALIDATE") {
            return self.validate();
        }
        if self.eat_kw("DECIDE") {
            self.terminal = Some(self.decide()?);
            return Ok(());
        }
        if self.eat_kw("ASSEMBLE") {
            self.terminal = Some(self.assemble()?);
            return Ok(());
        }
        Err(self.err(
            DecideTextErrorKind::Syntax,
            "expected COVERS, VALIDATE, DECIDE or ASSEMBLE",
        ))
    }

    fn typed_params(&self) -> Result<BoundedVec<TypedParam, 64>, DecideTextError> {
        let params = self
            .params
            .iter()
            .map(|(name, value)| TypedParam {
                name: name.clone(),
                value: value.clone(),
            })
            .collect();
        BoundedVec::new(params).map_err(|m| self.err(DecideTextErrorKind::ParameterType, m))
    }

    pub(super) fn parse(mut self) -> Result<DecideTextRequest, DecideTextError> {
        let source = self.source()?;
        while self.eat(&Tok::Pipe) {
            self.clause()?;
        }
        if self.pos != self.toks.len() {
            return Err(self.err(DecideTextErrorKind::Syntax, "expected `|>` or end of input"));
        }
        match self.terminal.take() {
            Some(Terminal::Decide(question)) => self.build_decide(source, *question),
            Some(Terminal::Assemble { max_components }) => {
                self.build_assemble(source, max_components)
            }
            None => Err(self.err(
                DecideTextErrorKind::MissingDecision,
                "a DECIDE or ASSEMBLE clause is required",
            )),
        }
    }

    fn build_decide(
        &self,
        source: Source,
        q: Question,
    ) -> Result<DecideTextRequest, DecideTextError> {
        let candidates = match source {
            Source::Library(scope) => CandidateSource::AgentLibrary { scope },
            Source::Graph { graph, plan } => CandidateSource::Graph {
                graph,
                plan: Box::new(plan),
            },
        };
        Ok(DecideTextRequest::Decide(Box::new(DecideRequest {
            tenant_id: self.tenant_id.to_string(),
            question: q.question,
            candidates,
            feature_schema: q.features,
            head: q.head,
            policy: self.policy.clone(),
            params: self.typed_params()?,
            max_records: q.max_records,
        })))
    }

    fn build_assemble(
        &self,
        source: Source,
        max_components: Option<u32>,
    ) -> Result<DecideTextRequest, DecideTextError> {
        let Source::Library(scope) = source else {
            return Err(self.err(
                DecideTextErrorKind::Syntax,
                "ASSEMBLE reads agent-library candidates only",
            ));
        };
        let capabilities = BoundedVec::new(self.covers.clone().unwrap_or_default())
            .map_err(|m| self.err(DecideTextErrorKind::ParameterType, m))?;
        Ok(DecideTextRequest::Assemble(Box::new(AssemblyRequest {
            tenant_id: self.tenant_id.to_string(),
            requirements: AssemblyRequirements {
                capabilities,
                constraints: AssemblyConstraints {
                    max_components,
                    ..AssemblyConstraints::default()
                },
                ..AssemblyRequirements::default()
            },
            candidates: scope,
            templates: BoundedVec::default(),
            policy: self.policy.clone(),
            solver: None,
        })))
    }
}
