use super::cursor::Cursor;
use super::error::GqlError;
use super::lexer::{Tok, Token};
use super::raw_ast::{Directive, Fragment, RawDocument, RawField, RawSelection, VarDef};
use super::values::ValueParser;

pub(super) struct P<'a> {
    cursor: Cursor<'a>,
}

impl<'a> P<'a> {
    pub(super) fn new(toks: &'a [Token], end: usize) -> Self {
        Self {
            cursor: Cursor::new(toks, end),
        }
    }

    pub(super) fn expect_eof(&mut self) -> Result<(), GqlError> {
        self.cursor.expect_eof()
    }

    /// Parse the whole document: one operation (`query`/`mutation`/`subscription` or a
    /// bare `{ … }`) plus any number of `fragment` definitions, in any order.
    pub(super) fn parse_raw_document(&mut self) -> Result<RawDocument, GqlError> {
        let mut op: Option<(&'static str, Vec<VarDef>, Vec<RawSelection>)> = None;
        let mut fragments = Vec::new();
        while self.cursor.peek().is_some() {
            match self.cursor.peek() {
                Some(Tok::Name(kw)) if kw == "fragment" => {
                    fragments.push(self.parse_fragment()?);
                }
                Some(Tok::Name(kw))
                    if kw == "query" || kw == "mutation" || kw == "subscription" =>
                {
                    if op.is_some() {
                        return Err(self
                            .cursor
                            .err("only a single operation is supported per document"));
                    }
                    op = Some(self.parse_operation_def()?);
                }
                Some(Tok::LBrace) => {
                    if op.is_some() {
                        return Err(self
                            .cursor
                            .err("only a single operation is supported per document"));
                    }
                    let selections = self.parse_raw_selection_set()?;
                    op = Some(("query", Vec::new(), selections));
                }
                _ => {
                    return Err(self.cursor.err(
                        "expected an operation (`query`/`mutation`/`subscription` or `{`) \
                         or a `fragment` definition",
                    ))
                }
            }
        }
        let (op_kind, var_defs, selections) = op.ok_or_else(|| {
            self.cursor
                .err("a GraphQL document must contain an operation")
        })?;
        Ok(RawDocument {
            op_kind,
            var_defs,
            selections,
            fragments,
        })
    }

    /// `query|mutation|subscription [Name] [($v: T = d, …)] [@dir …] { … }`.
    pub(super) fn parse_operation_def(
        &mut self,
    ) -> Result<(&'static str, Vec<VarDef>, Vec<RawSelection>), GqlError> {
        let kw = self.cursor.expect_name("an operation keyword")?;
        let op_kind = match kw.as_str() {
            "mutation" => "mutation",
            "subscription" => "subscription",
            _ => "query",
        };
        // optional operation name
        if matches!(self.cursor.peek(), Some(Tok::Name(_))) {
            self.cursor.bump();
        }
        let var_defs = if self.cursor.peek_is(&Tok::LParen) {
            self.parse_var_defs()?
        } else {
            Vec::new()
        };
        // operation-level directives are parsed and ignored.
        let _ = self.parse_directives()?;
        let selections = self.parse_raw_selection_set()?;
        Ok((op_kind, var_defs, selections))
    }

    /// `fragment Name on Type [@dir …] { … }`.
    pub(super) fn parse_fragment(&mut self) -> Result<Fragment, GqlError> {
        // consume `fragment`
        let _ = self.cursor.expect_name("`fragment`")?;
        let name = self.cursor.expect_name("a fragment name")?;
        let on = self.cursor.expect_name("`on` after the fragment name")?;
        if on != "on" {
            return Err(self.cursor.err("expected `on` after the fragment name"));
        }
        let type_cond = self.cursor.expect_name("a type condition")?;
        let _ = self.parse_directives()?;
        let selections = self.parse_raw_selection_set()?;
        Ok(Fragment {
            name,
            type_cond,
            selections,
        })
    }

    /// `($name: Type [= default], …)` — variable definitions (CONCEPT:EG-KG.query.fragments-variables-directives).
    pub(super) fn parse_var_defs(&mut self) -> Result<Vec<VarDef>, GqlError> {
        self.cursor
            .expect(&Tok::LParen, "`(` to open variable definitions")?;
        let mut defs = Vec::new();
        while !self.cursor.peek_is(&Tok::RParen) && self.cursor.peek().is_some() {
            self.cursor
                .expect(&Tok::Dollar, "`$` to start a variable definition")?;
            let name = self.cursor.expect_name("a variable name")?;
            self.cursor
                .expect(&Tok::Colon, "`:` after the variable name")?;
            self.parse_type_ref()?; // type consumed + ignored (untyped surface)
            let default = if self.cursor.eat(&Tok::Eq) {
                Some(ValueParser::new(&mut self.cursor).parse_value()?)
            } else {
                None
            };
            defs.push(VarDef { name, default });
            let _ = self.cursor.eat(&Tok::Comma);
        }
        self.cursor
            .expect(&Tok::RParen, "`)` to close variable definitions")?;
        Ok(defs)
    }

    /// A type reference `Name`, `[Type]`, or either with a trailing `!`. Parsed for
    /// well-formedness then discarded — the surface is untyped.
    pub(super) fn parse_type_ref(&mut self) -> Result<(), GqlError> {
        if self.cursor.peek_is(&Tok::LBracket) {
            self.cursor.bump();
            self.parse_type_ref()?;
            self.cursor
                .expect(&Tok::RBracket, "`]` to close a list type")?;
        } else {
            let _ = self.cursor.expect_name("a type name")?;
        }
        let _ = self.cursor.eat(&Tok::Bang);
        Ok(())
    }

    /// Zero or more `@name[(args)]` directives (CONCEPT:EG-KG.query.fragments-variables-directives).
    pub(super) fn parse_directives(&mut self) -> Result<Vec<Directive>, GqlError> {
        let mut ds = Vec::new();
        while self.cursor.peek_is(&Tok::At) {
            self.cursor.bump();
            let name = self.cursor.expect_name("a directive name after `@`")?;
            let args = if self.cursor.peek_is(&Tok::LParen) {
                ValueParser::new(&mut self.cursor).parse_args()?
            } else {
                Vec::new()
            };
            ds.push(Directive { name, args });
        }
        Ok(ds)
    }

    pub(super) fn parse_raw_selection_set(&mut self) -> Result<Vec<RawSelection>, GqlError> {
        self.cursor
            .expect(&Tok::LBrace, "`{` to open a selection set")?;
        let mut sels = Vec::new();
        while !self.cursor.peek_is(&Tok::RBrace) && self.cursor.peek().is_some() {
            sels.push(self.parse_raw_selection()?);
        }
        self.cursor
            .expect(&Tok::RBrace, "`}` to close the selection set")?;
        if sels.is_empty() {
            return Err(self
                .cursor
                .err("a selection set must select at least one field"));
        }
        Ok(sels)
    }

    /// A field, a fragment spread (`...Name`), or an inline fragment
    /// (`... on Type { … }` / `... { … }`) — CONCEPT:EG-KG.query.fragments-variables-directives.
    pub(super) fn parse_raw_selection(&mut self) -> Result<RawSelection, GqlError> {
        if self.cursor.peek_is(&Tok::Spread) {
            self.cursor.bump();
            if let Some(Tok::Name(n)) = self.cursor.peek() {
                if n == "on" {
                    // inline fragment with a type condition
                    self.cursor.bump();
                    let type_cond = Some(self.cursor.expect_name("a type condition after `on`")?);
                    let directives = self.parse_directives()?;
                    let selections = self.parse_raw_selection_set()?;
                    return Ok(RawSelection::Inline {
                        type_cond,
                        directives,
                        selections,
                    });
                }
                // a named fragment spread `...Name`
                let name = self.cursor.expect_name("a fragment name")?;
                let directives = self.parse_directives()?;
                return Ok(RawSelection::Spread { name, directives });
            }
            // inline fragment with no type condition: `... @dir { … }` / `... { … }`
            let directives = self.parse_directives()?;
            let selections = self.parse_raw_selection_set()?;
            return Ok(RawSelection::Inline {
                type_cond: None,
                directives,
                selections,
            });
        }
        Ok(RawSelection::Field(self.parse_raw_field()?))
    }

    pub(super) fn parse_raw_field(&mut self) -> Result<RawField, GqlError> {
        let first = self.cursor.expect_name("a field name")?;
        // `alias: name` — a colon after the first name makes it the response alias.
        let (alias, name) = if self.cursor.eat(&Tok::Colon) {
            let real = self
                .cursor
                .expect_name("a field name after the alias `:`")?;
            (first, real)
        } else {
            (first.clone(), first)
        };
        let args = if self.cursor.peek_is(&Tok::LParen) {
            ValueParser::new(&mut self.cursor).parse_args()?
        } else {
            Vec::new()
        };
        let directives = self.parse_directives()?;
        let selections = if self.cursor.peek_is(&Tok::LBrace) {
            self.parse_raw_selection_set()?
        } else {
            Vec::new()
        };
        Ok(RawField {
            alias,
            name,
            args,
            directives,
            selections,
        })
    }
}
