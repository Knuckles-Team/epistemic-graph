//! Semantic and retrieval clauses whose executors live behind a cargo feature:
//! `TEXT`/`FUSE` (`text`), `REASON`/`SPARQL` (`owl`), `UDF` (`wasm-udf`),
//! `FOREIGN SCAN`/`FOREIGN HTTP` (`federation`) and the epistemic stages (`epistemic`).
//! Each is recognized in every build; see [`super::gated`].

#[cfg(feature = "federation")]
use eg_types::wire::ForeignSourceSpec;
use eg_types::wire::Op;

use super::Parser;
use crate::uql::diag::UqlError;
#[cfg(any(feature = "text", feature = "owl"))]
use crate::uql::lexer::Tok;

impl<'a> Parser<'a> {
    gated! { "text",
        /// `TEXT string`.
        fn text(&mut self) -> Result<Op, UqlError> {
            Ok(Op::RankText {
                query: self.string("a quoted query (`TEXT '<q>'`)")?,
            })
        }
    }

    gated! { "text",
        /// `FUSE [K num] ( [branch] {[branch]} | (name {, name}) )`.
        fn fuse(&mut self) -> Result<Op, UqlError> {
            let k = if self.eat_kw("K") {
                self.parse_number::<f32>("the RRF k constant")?
            } else {
                0.0
            };
            let branches = if self.peek(&Tok::LParen) {
                self.fuse_named_branches()?
            } else {
                self.fuse_inline_branches()?
            };
            Ok(Op::FuseRrf { branches, k })
        }
    }

    #[cfg(feature = "text")]
    fn fuse_inline_branches(&mut self) -> Result<Vec<Vec<Op>>, UqlError> {
        let mut branches = Vec::new();
        while self.eat(&Tok::LBracket) {
            self.enter()?;
            let mut ops = Vec::new();
            self.stage(&mut ops)?;
            while self.eat(&Tok::Pipe) {
                self.stage(&mut ops)?;
            }
            self.leave();
            self.expect(&Tok::RBracket, "`]` to close a FUSE branch")?;
            branches.push(ops);
        }
        if branches.is_empty() {
            return Err(self.err_here(
                "expected at least one `[branch]` after FUSE (`FUSE [RANK BY ~[…]] [TEXT 'q']`)",
            ));
        }
        Ok(branches)
    }

    /// `( a, b )` — each named `LET` binding is inlined as one branch.
    #[cfg(feature = "text")]
    fn fuse_named_branches(&mut self) -> Result<Vec<Vec<Op>>, UqlError> {
        self.expect(&Tok::LParen, "`(`")?;
        let mut branches = vec![self.inline_binding()?];
        while self.eat(&Tok::Comma) {
            branches.push(self.inline_binding()?);
        }
        self.expect(&Tok::RParen, "`)` to close the FUSE binding list")?;
        Ok(branches)
    }

    gated! { "owl",
        /// `REASON (iri | string | name) [ONTOLOGY string]`.
        fn reason(&mut self) -> Result<Op, UqlError> {
            let target_class = match self.peek_kind() {
                Some(Tok::Iri(s)) => {
                    let s = s.clone();
                    self.bump();
                    s
                }
                _ => self.id("a class name or IRI (`REASON <http://…/Class>`)")?,
            };
            let ontology = if self.eat_kw("ONTOLOGY") {
                self.string("an ontology (Turtle)")?
            } else {
                String::new()
            };
            Ok(Op::Reason {
                target_class,
                ontology,
            })
        }
    }

    gated! { "owl",
        /// `SPARQL string VAR string`.
        fn sparql(&mut self) -> Result<Op, UqlError> {
            let query = self.string("a SPARQL SELECT")?;
            self.expect_kw("VAR")?;
            Ok(Op::SparqlBgp {
                query,
                var: self.string("the projected variable")?,
            })
        }
    }

    gated! { "wasm-udf",
        /// `UDF id`.
        fn udf(&mut self) -> Result<Op, UqlError> {
            Ok(Op::Udf {
                id: self.id("a registered UDF id")?,
            })
        }
    }

    gated! { "federation",
        /// `FOREIGN SCAN string [JOIN]` | `FOREIGN HTTP string [PATH s] ID s [SCORE s] [JOIN]`
        /// (`FOREIGN` consumed).
        fn foreign_scan(&mut self) -> Result<Op, UqlError> {
            let source = if self.eat_kw("SCAN") {
                ForeignSourceSpec::Named {
                    name: self.string("a registered source name")?,
                }
            } else {
                self.expect_kw("HTTP")?;
                self.http_source()?
            };
            let join = self.eat_kw("JOIN");
            Ok(Op::ForeignScan {
                source: Box::new(source),
                join,
            })
        }
    }

    #[cfg(feature = "federation")]
    fn http_source(&mut self) -> Result<ForeignSourceSpec, UqlError> {
        let url = self.string("the source URL")?;
        let json_path = if self.eat_kw("PATH") {
            self.string("a JSON path")?
        } else {
            String::new()
        };
        self.expect_kw("ID")?;
        let id = self.string("the id field")?;
        let score = if self.eat_kw("SCORE") {
            Some(self.string("the score field")?)
        } else {
            None
        };
        Ok(ForeignSourceSpec::HttpJson {
            url,
            json_path,
            field_map: eg_types::wire::HttpFieldMap {
                id,
                score,
                columns: Default::default(),
            },
        })
    }

    gated! { "epistemic",
        /// `EVIDENCE FOR id`.
        fn evidence_for(&mut self) -> Result<Op, UqlError> {
            self.expect_kw("FOR")?;
            Ok(Op::EvidenceFor {
                claim_id: self.id("a claim id")?,
            })
        }
    }

    gated! { "epistemic",
        /// `CONTRADICTS id`.
        fn contradicts(&mut self) -> Result<Op, UqlError> {
            Ok(Op::Contradicts {
                node_id: self.id("a node id")?,
            })
        }
    }

    gated! { "epistemic",
        /// `SUPPORTED BY id`.
        fn supported_by(&mut self) -> Result<Op, UqlError> {
            self.expect_kw("BY")?;
            Ok(Op::SupportedBy {
                node_id: self.id("a node id")?,
            })
        }
    }

    gated! { "epistemic",
        /// `BELIEF AS OF ts`.
        fn belief_as_of(&mut self) -> Result<Op, UqlError> {
            self.expect_kw("AS")?;
            self.expect_kw("OF")?;
            Ok(Op::BeliefAsOf {
                ts: self.timestamp()?,
            })
        }
    }

    gated! { "epistemic",
        /// `SOURCE RELIABILITY id`.
        fn source_reliability(&mut self) -> Result<Op, UqlError> {
            self.expect_kw("RELIABILITY")?;
            Ok(Op::SourceReliability {
                source_id: self.id("a source id")?,
            })
        }
    }

    gated! { "epistemic",
        /// `CONFIDENCE`.
        fn confidence(&mut self) -> Result<Op, UqlError> {
            Ok(Op::ConfidenceOp {})
        }
    }

    gated! { "epistemic",
        /// `EXPLAIN BELIEF id`.
        fn explain_belief(&mut self) -> Result<Op, UqlError> {
            self.expect_kw("BELIEF")?;
            Ok(Op::ExplainBelief {
                node_id: self.id("a node id")?,
            })
        }
    }
}
