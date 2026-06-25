//! UQL — the Unified Query Language front-end (CONCEPT:KG-2.214).
//!
//! A human/agent-writable TEXT surface that parses to the SAME [`eg_types::wire::Plan`]
//! AST `Method::UnifiedQuery` already executes — so one query expresses
//! filter (relational) + traverse (graph) + rank (vector) across modalities without a
//! caller hand-building the Op list. This module is a pure FRONT-END: it adds no
//! executor; [`crate::execute`] / the served handler run the parsed Plan unchanged.
//!
//! The whole front-end (lexer + parser) is DEPENDENCY-FREE (no DataFusion, no regex),
//! so it ships in a default/Pi build — only EXECUTION of the resulting Plan is
//! `query`-gated, exactly as the rest of eg-plan.
//!
//! See [`parser`] for the grammar (EBNF), the per-Op mapping, and the documented
//! forward-compat seams (time/asof, text-rank, reason).
//!
//! ```
//! use eg_plan::uql;
//! use eg_types::wire::{Op, Pred};
//!
//! let plan = uql::parse(
//!     "MATCH (:Doc) WHERE year > 2024 |> TRAVERSE -[:CITES]->{1,2} \
//!      |> RANK BY ~[1.0, 0.0] |> LIMIT 10",
//! )
//! .unwrap();
//! assert_eq!(plan.ops[0], Op::Scan { label: "Doc".into() });
//! assert_eq!(
//!     plan.ops[1],
//!     Op::Filter { preds: vec![Pred::GtNum { prop: "year".into(), n: 2024.0 }] }
//! );
//! ```

mod lexer;
mod parser;

pub use parser::{parse, UqlError};

#[cfg(test)]
mod tests {
    use super::parse;
    use eg_types::wire::{Op, Plan, Pred};

    /// THE faithfulness proof: the documented pipeline query parses to the BYTE-
    /// IDENTICAL Plan a hand-built test constructs — the same AST the oracle test in
    /// `src/server/mod.rs` and `eg-plan`'s `tests.rs` build by hand.
    #[test]
    fn pipeline_parses_to_hand_built_plan() {
        let text = "MATCH (:Doc) WHERE year > 2024 \
                    |> TRAVERSE -[:CITES]->{1,2} \
                    |> RANK BY ~[1.0, 0.0, 0.0, 0.0] \
                    |> LIMIT 10";
        let parsed = parse(text).unwrap();
        let hand = Plan::new(vec![
            Op::Scan {
                label: "Doc".into(),
            },
            Op::Filter {
                preds: vec![Pred::GtNum {
                    prop: "year".into(),
                    n: 2024.0,
                }],
            },
            Op::Traverse {
                rel: "CITES".into(),
                min: 1,
                max: 2,
            },
            Op::Rank {
                query: vec![1.0, 0.0, 0.0, 0.0],
            },
            Op::Limit { k: 10 },
        ]);
        assert_eq!(parsed, hand, "UQL must parse to the hand-built Plan AST");
    }

    /// An inline `MATCH (:L) WHERE …` lowers to the same `[Scan, Filter]` as the
    /// piped `MATCH (:L) |> WHERE …` form — the two surfaces are interchangeable.
    #[test]
    fn inline_where_equals_piped_where() {
        let inline = parse("MATCH (:Doc) WHERE year > 2024").unwrap();
        let piped = parse("MATCH (:Doc) |> WHERE year > 2024").unwrap();
        assert_eq!(inline, piped);
        assert_eq!(
            inline.ops,
            vec![
                Op::Scan {
                    label: "Doc".into()
                },
                Op::Filter {
                    preds: vec![Pred::GtNum {
                        prop: "year".into(),
                        n: 2024.0
                    }]
                },
            ]
        );
    }

    /// Keywords are case-insensitive; `:` on the label and the bare-rel traverse
    /// shorthand are accepted; `{n}` is exactly-n hops; `=`/`==` are equality.
    #[test]
    fn surface_variations() {
        let p = parse(
            "match (Doc) where lang = 'en' and rank == 1 \
             |> traverse CITES{2} |> limit 5",
        )
        .unwrap();
        assert_eq!(
            p.ops,
            vec![
                Op::Scan {
                    label: "Doc".into()
                },
                Op::Filter {
                    preds: vec![
                        Pred::Eq {
                            prop: "lang".into(),
                            value: "en".into()
                        },
                        Pred::Eq {
                            prop: "rank".into(),
                            value: "1".into()
                        },
                    ]
                },
                Op::Traverse {
                    rel: "CITES".into(),
                    min: 2,
                    max: 2,
                },
                Op::Limit { k: 5 },
            ]
        );
    }

    /// `{a..b}` and `{a,b}` hop ranges are equivalent; `<` lowers to `LtNum`.
    #[test]
    fn dotdot_range_and_lt() {
        let comma = parse("MATCH (:N) |> TRAVERSE -[:R]->{1,3}").unwrap();
        let dots = parse("MATCH (:N) |> TRAVERSE -[:R]->{1..3}").unwrap();
        assert_eq!(comma, dots);
        assert_eq!(
            comma.ops[1],
            Op::Traverse {
                rel: "R".into(),
                min: 1,
                max: 3
            }
        );
        let lt = parse("MATCH (:N) |> WHERE year < 2020").unwrap();
        assert_eq!(
            lt.ops[1],
            Op::Filter {
                preds: vec![Pred::LtNum {
                    prop: "year".into(),
                    n: 2020.0
                }]
            }
        );
    }

    // ── error messages must be clear ────────────────────────────────────────

    #[test]
    fn missing_match_is_clear() {
        let e = parse("SCAN (:Doc)").unwrap_err();
        assert!(e.msg.contains("MATCH"), "got: {}", e.msg);
    }

    #[test]
    fn unknown_stage_is_clear() {
        let e = parse("MATCH (:Doc) |> FROBNICATE").unwrap_err();
        assert!(e.msg.contains("pipeline stage"), "got: {}", e.msg);
    }

    #[test]
    fn bad_limit_is_clear() {
        let e = parse("MATCH (:Doc) |> LIMIT )").unwrap_err();
        assert!(e.msg.contains("LIMIT"), "got: {}", e.msg);
        // The caret renders at the offending token.
        let rendered = e.render("MATCH (:Doc) |> LIMIT )");
        assert!(rendered.contains('^'), "render must carry a caret");
    }

    #[test]
    fn non_integer_hop_is_clear() {
        let e = parse("MATCH (:Doc) |> TRAVERSE -[:R]->{1.5}").unwrap_err();
        assert!(e.msg.contains("integer"), "got: {}", e.msg);
    }

    #[test]
    fn unterminated_string_is_clear() {
        let e = parse("MATCH (:Doc) WHERE lang = 'en").unwrap_err();
        assert!(e.msg.contains("unterminated"), "got: {}", e.msg);
    }

    /// The text-rank forward seam: a NAMED rank ref parses but is rejected with a
    /// clear "reserved forward seam" message (it lowers once `Op::TextRank` lands).
    #[test]
    fn named_rank_ref_is_reserved_seam() {
        let e = parse("MATCH (:Doc) |> RANK BY ~\"my query\"").unwrap_err();
        assert!(
            e.msg.contains("text-rank") || e.msg.contains("forward seam"),
            "got: {}",
            e.msg
        );
    }

    /// A scan-only query is legal (no stages).
    #[test]
    fn scan_only() {
        let p = parse("MATCH (:Doc)").unwrap();
        assert_eq!(
            p.ops,
            vec![Op::Scan {
                label: "Doc".into()
            }]
        );
    }

    // ── CONCEPT:KG-2.235 — surface clauses for the newer ops ─────────────────

    /// `AS OF @<ts>` parses to `Op::AsOf { ts }` (the time CONTEXT op). Proof:
    /// parse → the hand-built Plan.
    #[test]
    fn as_of_clause_parses_to_asof_op() {
        let p = parse("MATCH (:Event) |> AS OF @1700000000 |> LIMIT 5").unwrap();
        assert_eq!(
            p.ops,
            vec![
                Op::Scan {
                    label: "Event".into()
                },
                Op::AsOf {
                    ts: 1_700_000_000.0
                },
                Op::Limit { k: 5 },
            ]
        );
    }

    /// `WINDOW <dur>` parses to `Op::Window { secs }`; a bare number is seconds and a
    /// unit suffix scales it (`1h` == `3600`).
    #[test]
    fn window_clause_units_scale_to_seconds() {
        let bare = parse("MATCH (:Event) |> WINDOW 3600").unwrap();
        let unit = parse("MATCH (:Event) |> WINDOW 1 h").unwrap();
        assert_eq!(bare.ops[1], Op::Window { secs: 3600.0 });
        assert_eq!(unit.ops[1], Op::Window { secs: 3600.0 });
        let mins = parse("MATCH (:Event) |> WINDOW 30 m").unwrap();
        assert_eq!(mins.ops[1], Op::Window { secs: 1800.0 });
    }

    /// `FOREIGN "<name>"` parses to `Op::Foreign { name }` (the federation CONTEXT op).
    #[test]
    fn foreign_clause_parses_to_foreign_op() {
        let p = parse("MATCH (:Doc) |> FOREIGN \"peer-east\" |> LIMIT 3").unwrap();
        assert_eq!(
            p.ops,
            vec![
                Op::Scan {
                    label: "Doc".into()
                },
                Op::Foreign {
                    name: "peer-east".into()
                },
                Op::Limit { k: 3 },
            ]
        );
    }

    /// A COMPOSED query that threads the time + federation clauses with the core ops
    /// parses to the full hand-built Plan — proof the new clauses compose, not just
    /// parse in isolation.
    #[test]
    fn composed_time_federation_query_parses() {
        let text = "MATCH (:Event) WHERE level > 3 \
                    |> FOREIGN \"peer-west\" \
                    |> AS OF @1700000000 |> WINDOW 1 h \
                    |> TRAVERSE -[:CAUSED]->{1,2} \
                    |> LIMIT 10";
        let p = parse(text).unwrap();
        let hand = Plan::new(vec![
            Op::Scan {
                label: "Event".into(),
            },
            Op::Filter {
                preds: vec![Pred::GtNum {
                    prop: "level".into(),
                    n: 3.0,
                }],
            },
            Op::Foreign {
                name: "peer-west".into(),
            },
            Op::AsOf {
                ts: 1_700_000_000.0,
            },
            Op::Window { secs: 3600.0 },
            Op::Traverse {
                rel: "CAUSED".into(),
                min: 1,
                max: 2,
            },
            Op::Limit { k: 10 },
        ]);
        assert_eq!(p, hand);
    }

    /// `AS` without `OF` is a clear error.
    #[test]
    fn as_without_of_is_clear() {
        let e = parse("MATCH (:Event) |> AS @1").unwrap_err();
        assert!(e.msg.contains("OF"), "got: {}", e.msg);
    }

    /// `REASON <Class>` lowers to `Op::Reason` (owl feature only).
    #[cfg(feature = "owl")]
    #[test]
    fn reason_clause_parses_to_reason_op() {
        let p = parse("MATCH (:Thing) |> REASON Mammal |> LIMIT 5").unwrap();
        assert_eq!(
            p.ops[1],
            Op::Reason {
                target_class: "Mammal".into(),
                ontology: String::new(),
            }
        );
    }

    /// Without the `owl` feature the `REASON` clause is rejected with a clear message.
    #[cfg(not(feature = "owl"))]
    #[test]
    fn reason_clause_not_in_build() {
        let e = parse("MATCH (:Thing) |> REASON Mammal").unwrap_err();
        assert!(
            e.msg.contains("OWL reasoner") || e.msg.contains("not available"),
            "got: {}",
            e.msg
        );
    }

    /// `TEXT "<q>"` lowers to `Op::RankText` (text feature only).
    #[cfg(feature = "text")]
    #[test]
    fn text_clause_parses_to_ranktext_op() {
        let p = parse("MATCH (:Doc) |> TEXT \"graph databases\" |> LIMIT 5").unwrap();
        assert_eq!(
            p.ops[1],
            Op::RankText {
                query: "graph databases".into()
            }
        );
    }

    /// Without the `text` feature the `TEXT` clause is rejected with a clear message.
    #[cfg(not(feature = "text"))]
    #[test]
    fn text_clause_not_in_build() {
        let e = parse("MATCH (:Doc) |> TEXT \"q\"").unwrap_err();
        assert!(
            e.msg.contains("lexical index") || e.msg.contains("not available"),
            "got: {}",
            e.msg
        );
    }
}
