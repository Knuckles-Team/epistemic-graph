//! UQL recursive-descent parser (CONCEPT:AU-KG.query.top-nodes-by-degree) → the existing [`Plan`] AST.
//!
//! This is a pure FRONT-END: it parses a human/agent-writable text query into the
//! SAME `wire::Plan` (`Vec<Op>`) that `Method::UnifiedQuery` already executes — it
//! adds NO new execution path. The proof of faithfulness is [`super`]'s tests:
//! a UQL string parses to the byte-identical Plan a hand-built test constructs, and
//! the served query returns the same result as the structured plan AND the
//! separate-surfaces oracle.
//!
//! Dependency-free (no DataFusion, no regex) so the whole front-end ships in a
//! default/Pi build — only EXECUTION is `query`-gated.
//!
//! ## Grammar (EBNF) — the full surface
//!
//! Kept in lockstep with the user reference at `docs/uql.md` (prose, examples, the
//! Op-mapping cheat sheet, and gotchas live there).
//!
//! ```text
//! query      = source { "|>" stage } ;
//! source     = "MATCH" "(" [ ":" ] label ")" [ "WHERE" pred_list ]
//!            | "REASON" class | "FOREIGN" string ;
//! stage      = filter | traverse | rank | text | fuse | rerank
//!            | asof | window | foreign | reason | limit
//!            | evidence_for | contradicts | supported_by
//!            | belief_asof | valid_asof | source_reliability
//!            | confidence | explain_belief ;                 (* epistemic, E2 *)
//! filter     = "WHERE" pred_list ;                         (* a later-stage filter *)
//! traverse   = "TRAVERSE" edge ;
//! edge       = "-" "[" ":" rel "]" "->" [ hop_range ]
//!            | rel [ hop_range ] ;                          (* bare-rel shorthand    *)
//! hop_range  = "{" int [ ( "," | ".." ) int ] "}" ;        (* {2} = {2,2}; {1,3}    *)
//! rank       = "RANK" "BY" "~" vector_ref ;                 (* vector / text / handle *)
//! vector_ref = "[" signed_num { "," signed_num } "]"        (* inline literal vector *)
//!            | string | ident ;                             (* ~"text" (embed) / ~handle *)
//! signed_num = [ "-" ] num ;                                (* negatives allowed     *)
//! text       = "TEXT" string ;                              (* BM25 lexical rank     *)
//! fuse       = "FUSE" "[" branch "]" { "[" branch "]" } ;   (* N-way RRF hybrid      *)
//! branch     = stage { "|>" stage } ;
//! rerank     = "RERANK" ( "NODE_DISTANCE" "FROM" id
//!                       | "MENTIONS" | "MMR" num int ) ;    (* graph-native rerankers *)
//! asof       = "AS" "OF" [ "TX" | "VALID" ] "@" num ;       (* bi-temporal point-in-time *)
//! window     = "WINDOW" num [ unit ] [ agg ] ;              (* s|m|h|d ; agg selector *)
//! limit      = "LIMIT" int ;
//! pred_list  = pred { "AND" pred } ;
//! pred       = prop ( ">" | "<" | ( "=" | "==" ) ) value ;
//! value      = num | string | ident ;
//! id         = ident | string ;     (* QUOTE ids with `-` `.` `:` `@` — the lexer
//!                                       splits `-`, so `kg-2.0` must be `"kg-2.0"` *)
//! (* ── epistemic (E2, CONCEPT:EG-KG.epistemic.epistemic-substrate) ── *)
//! evidence_for        = "EVIDENCE" "FOR" id ;
//! contradicts         = "CONTRADICTS" id ;
//! supported_by        = "SUPPORTED" "BY" id ;
//! belief_asof         = "BELIEF" "AS" "OF" "@" num ;
//! valid_asof          = "VALID" "AS" "OF" "@" num ;         (* alias → AsOf{Valid}   *)
//! source_reliability  = "SOURCE" "RELIABILITY" id ;
//! confidence          = "CONFIDENCE" ;                       (* no argument           *)
//! explain_belief      = "EXPLAIN" "BELIEF" id ;
//! ```
//!
//! ### Op mapping (each grammar production → one `wire::Op`)
//! | UQL                                  | `Op`                                   |
//! |--------------------------------------|----------------------------------------|
//! | `MATCH (:Doc)`                       | `Scan { label: "Doc" }`                |
//! | `WHERE year > 2024 AND lang = 'en'`  | `Filter { preds: [GtNum, Eq] }`        |
//! | `TRAVERSE -[:CITES]->{1,2}`          | `Traverse { rel:"CITES",min:1,max:2 }` |
//! | `RANK BY ~[1.0,-0.5]`                | `Rank { query: [1.0, -0.5] }`          |
//! | `RANK BY ~"some text"`               | `RankEmbed { text: "some text" }`      |
//! | `TEXT "graphs"`                      | `RankText { query: "graphs" }`         |
//! | `FUSE [RANK BY ~[1,0]] [TEXT "q"]`   | `FuseRrf { branches, k: 0.0 }`         |
//! | `RERANK NODE_DISTANCE FROM "n1"`     | `RankNodeDistance { center: "n1" }`    |
//! | `RERANK MENTIONS`                    | `RankMentions {}`                      |
//! | `RERANK MMR 0.5 10`                  | `RankMmr { lambda: 0.5, k: 10 }`       |
//! | `AS OF @t` / `AS OF TX @t`           | `AsOf { ts, axis: Valid|Transaction }` |
//! | `VALID AS OF @t`                     | `AsOf { ts, axis: Valid }` (ALIAS)      |
//! | `WINDOW 1 h`                         | `Window { secs: 3600 }`                |
//! | `WINDOW 60 s SUM`                    | `WindowAgg { secs: 60, agg: "sum" }`   |
//! | `FOREIGN "peer"`                     | `Foreign { name: "peer" }`             |
//! | `REASON Mammal`                      | `Reason { target_class: "Mammal" }`    |
//! | `EVIDENCE FOR "c1"`                  | `EvidenceFor { claim_id: "c1" }`       |
//! | `CONTRADICTS "c1"`                   | `Contradicts { node_id: "c1" }`        |
//! | `SUPPORTED BY "c1"`                  | `SupportedBy { node_id: "c1" }`        |
//! | `BELIEF AS OF @100`                  | `BeliefAsOf { ts: 100.0 }`             |
//! | `SOURCE RELIABILITY "s1"`            | `SourceReliability { source_id: "s1" }`|
//! | `CONFIDENCE`                         | `ConfidenceOp {}`                       |
//! | `EXPLAIN BELIEF "c1"`                | `ExplainBelief { node_id: "c1" }`      |
//! | `LIMIT 10`                           | `Limit { k: 10 }`                      |
//!
//! ### Feature gating (on EXECUTION, not parsing)
//!  * **time** (CONCEPT:AU-KG.compute.kg-2) — `AS OF [TX|VALID] @<ts>` is a real bi-temporal
//!    point-in-time FILTER (valid time = "what was true"; `TX` = transaction time =
//!    "what we believed"); `WINDOW <dur>` ⇒ `Op::Window`. Dep-free; base `query`.
//!  * **rerank** (CONCEPT:EG-KG.query.uql-parser-ops/2.255) — graph-native `NODE_DISTANCE`/`MENTIONS` and
//!    diversity `MMR <lambda> <k>`. Dep-free; base `query`.
//!  * **fuse / text** (CONCEPT:AU-KG.compute.change-feed-subscription / EG-418) — `FUSE`/`TEXT` gated to `text`; a
//!    non-`text` build parses them but errors with a clear "not in this build" message.
//!  * **federation** — `FOREIGN "<name>"` (resolve: `federation`). **owl** — `REASON
//!    <Class>` gated to `owl`.
//!  * **embedder seam** (CONCEPT:EG-KG.compute.no-embedder-bound-op) — `RANK BY ~"text"` lowers to `Op::RankEmbed`,
//!    resolved at exec time by the server-side embedder bound on the `PlanCtx` (a clear
//!    typed error if none is bound). A bare-ident handle (`~handle`) stays a reserved
//!    forward seam (no by-name embedding registry yet).
//!  * **window aggregate** (CONCEPT:EG-KG.compute.tsscan-series-window-60s/414) — `WINDOW <dur>` is a real tumbling
//!    windowed MEAN over `(ts,value)` rows (e.g. from a `TsScan`); `WINDOW <dur> <agg>`
//!    selects the aggregate (`Op::WindowAgg`). The eg-tsdb aggregate is wired under
//!    `timeseries`; a non-`timeseries` build passes the rows through.
//!  * **epistemic** (CONCEPT:EG-KG.epistemic.epistemic-substrate, E2) — `EVIDENCE FOR`/
//!    `CONTRADICTS`/`SUPPORTED BY`/`BELIEF AS OF`/`VALID AS OF`/`SOURCE RELIABILITY`/
//!    `CONFIDENCE`/`EXPLAIN BELIEF` all gated to `epistemic`; a non-`epistemic` build
//!    parses them but errors with a clear "not in this build" message (the `TEXT`/`REASON`
//!    precedent). `VALID AS OF <ts>` is a pure ALIAS lowering to the SAME `Op::AsOf { axis:
//!    Valid }` the bare `AS OF @ts` / `AS OF VALID @ts` forms already produce — it is a
//!    STRICT SUPERSET (new leading-keyword surface only), so it does not change what `AS
//!    OF` byte-identically parses to.

use eg_types::wire::{Op, Plan, Pred, TimeAxis};

use super::lexer::{lex, Tok, Token};

/// A UQL parse error: a human-readable message plus the byte offset in the source it
/// occurred at. [`UqlError::render`] turns it into a multi-line caret diagnostic.
#[derive(Clone, Debug, PartialEq)]
pub struct UqlError {
    pub msg: String,
    pub at: usize,
}

impl UqlError {
    /// Render a `source`-relative caret diagnostic, e.g.
    /// ```text
    /// UQL parse error: expected `LIMIT` count, found `)`
    ///   MATCH (:Doc) |> LIMIT )
    ///                          ^
    /// ```
    pub fn render(&self, src: &str) -> String {
        let at = self.at.min(src.len());
        let caret = " ".repeat(at) + "^";
        format!(
            "UQL parse error: {}\n  {src}\n  {caret}",
            self.msg,
            src = src,
            caret = caret
        )
    }
}

impl std::fmt::Display for UqlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "UQL parse error at byte {}: {}", self.at, self.msg)
    }
}

/// How a `RANK BY ~<ref>` vector reference resolved:
///  * `Inline` — a literal vector `~[…]` (possibly with negative components, CONCEPT:EG-KG.compute.negative-vector-component-parses)
///    → `Op::Rank { query }`.
///  * `Text` — a quoted natural-language query `~ "text"` → `Op::RankEmbed { text }`, the
///    server-side NL→vector seam (CONCEPT:EG-KG.compute.no-embedder-bound-op): the executor resolves the text to a
///    query vector via the embedder bound on the `PlanCtx` (a clear error if none is bound).
///  * `Handle` — a bare-ident stored-embedding handle `~handle` → still a reserved forward
///    seam (there is no by-name embedding registry yet): rejected with a clear message.
#[derive(Clone, Debug, PartialEq)]
enum VectorRef {
    Inline(Vec<f32>),
    Text(String),
    Handle(String),
}

/// Parse a UQL string into the existing [`Plan`] AST. The returned `Plan` is exactly
/// what a hand-built `Method::UnifiedQuery { plan }` carries — the parser is a pure
/// front-end over the proven planner.
pub fn parse(src: &str) -> Result<Plan, UqlError> {
    let tokens = lex(src).map_err(|e| UqlError {
        msg: e.msg,
        at: e.at,
    })?;
    let mut p = Parser {
        toks: &tokens,
        pos: 0,
        end: src.len(),
    };
    let plan = p.parse_query()?;
    p.expect_eof()?;
    Ok(plan)
}

struct Parser<'a> {
    toks: &'a [Token],
    pos: usize,
    /// Source length, for the "end of input" caret position.
    end: usize,
}

impl<'a> Parser<'a> {
    fn parse_query(&mut self) -> Result<Plan, UqlError> {
        let mut ops = self.parse_scan()?;
        while self.eat(&Tok::Pipe) {
            self.parse_stage(&mut ops)?;
        }
        Ok(Plan::new(ops))
    }

    /// `scan = "MATCH" "(" [":"] label ")" [ "WHERE" pred_list ]`. The optional inline
    /// WHERE is sugar for a following `Filter` stage (so `MATCH (:Doc) WHERE y>1` and
    /// `MATCH (:Doc) |> WHERE y>1` produce the identical Plan).
    fn parse_scan(&mut self) -> Result<Vec<Op>, UqlError> {
        self.expect_kw("MATCH")?;
        self.expect(&Tok::LParen, "`(` after MATCH")?;
        let _ = self.eat(&Tok::Colon); // optional leading `:` on the label
        let label = self.expect_ident("a node label")?;
        self.expect(&Tok::RParen, "`)` to close the MATCH pattern")?;

        let mut ops = vec![Op::Scan { label }];
        // An inline WHERE attached to MATCH lowers to a Filter op, same as a |> WHERE.
        if self.peek_kw("WHERE") {
            self.bump();
            let preds = self.parse_pred_list()?;
            ops.push(Op::Filter { preds });
        }
        Ok(ops)
    }

    /// `stage = traverse | rank | text | limit | filter | asof | window | foreign |
    /// reason`. Dispatched on the leading keyword. The time/federation/owl/text
    /// clauses (CONCEPT:EG-KG.query.sparql-completeness) each lower to ONE `Op`; the owl (`REASON`) + text
    /// (`TEXT`) clauses are feature-gated to the same feature as the `Op` they lower
    /// to, so a build without that feature gives a clear "not in this build" error
    /// rather than a phantom Op.
    fn parse_stage(&mut self, ops: &mut Vec<Op>) -> Result<(), UqlError> {
        if self.peek_kw("TRAVERSE") {
            self.bump();
            ops.push(self.parse_traverse()?);
        } else if self.peek_kw("RANK") {
            self.bump();
            ops.push(self.parse_rank()?);
        } else if self.peek_kw("TEXT") {
            self.bump();
            ops.push(self.parse_text()?);
        } else if self.peek_kw("FUSE") {
            self.bump();
            ops.push(self.parse_fuse()?);
        } else if self.peek_kw("LIMIT") {
            self.bump();
            ops.push(self.parse_limit()?);
        } else if self.peek_kw("WHERE") {
            self.bump();
            let preds = self.parse_pred_list()?;
            ops.push(Op::Filter { preds });
        } else if self.peek_kw("AS") {
            // `AS OF @<ts>`
            self.bump();
            ops.push(self.parse_asof()?);
        } else if self.peek_kw("WINDOW") {
            self.bump();
            ops.push(self.parse_window()?);
        } else if self.peek_kw("FOREIGN") {
            self.bump();
            ops.push(self.parse_foreign()?);
        } else if self.peek_kw("RERANK") {
            self.bump();
            ops.push(self.parse_rerank()?);
        } else if self.peek_kw("REASON") {
            self.bump();
            ops.push(self.parse_reason()?);
        } else if self.peek_kw("EVIDENCE") {
            self.bump();
            ops.push(self.parse_evidence_for()?);
        } else if self.peek_kw("CONTRADICTS") {
            self.bump();
            ops.push(self.parse_contradicts()?);
        } else if self.peek_kw("SUPPORTED") {
            self.bump();
            ops.push(self.parse_supported_by()?);
        } else if self.peek_kw("BELIEF") {
            self.bump();
            ops.push(self.parse_belief_asof()?);
        } else if self.peek_kw("VALID") {
            self.bump();
            ops.push(self.parse_valid_asof()?);
        } else if self.peek_kw("SOURCE") {
            self.bump();
            ops.push(self.parse_source_reliability()?);
        } else if self.peek_kw("CONFIDENCE") {
            self.bump();
            ops.push(self.parse_confidence()?);
        } else if self.peek_kw("EXPLAIN") {
            self.bump();
            ops.push(self.parse_explain_belief()?);
        } else {
            return Err(self.err_here(
                "expected a pipeline stage (`TRAVERSE`, `RANK`, `TEXT`, `FUSE`, `LIMIT`, \
                 `WHERE`, `AS OF`, `WINDOW`, `FOREIGN`, `REASON`, `EVIDENCE FOR`, \
                 `CONTRADICTS`, `SUPPORTED BY`, `BELIEF AS OF`, `VALID AS OF`, \
                 `SOURCE RELIABILITY`, `CONFIDENCE`, or `EXPLAIN BELIEF`)",
            ));
        }
        Ok(())
    }

    /// `traverse = "-" "[" ":" rel "]" "->" [hop_range] | rel [hop_range]`.
    fn parse_traverse(&mut self) -> Result<Op, UqlError> {
        let rel = if self.eat(&Tok::Dash) {
            // Cypher-ish edge pattern: -[:REL]->
            self.expect(&Tok::LBracket, "`[` in the edge pattern `-[:REL]->`")?;
            self.expect(&Tok::Colon, "`:` before the relationship type")?;
            let rel = self.expect_ident("a relationship type")?;
            self.expect(&Tok::RBracket, "`]` to close the edge pattern")?;
            self.expect(&Tok::Arrow, "`->` after the edge pattern")?;
            rel
        } else {
            // Bare-rel shorthand: TRAVERSE CITES{1,2}
            self.expect_ident("a relationship type (or `-[:REL]->`)")?
        };
        let (min, max) = self.parse_hop_range()?;
        Ok(Op::Traverse { rel, min, max })
    }

    /// `hop_range = "{" int [ ("," | "..") int ] "}"`. Absent ⇒ exactly one hop
    /// `{1,1}`; `{n}` ⇒ `{n,n}`; `{a,b}` / `{a..b}` ⇒ `min=a,max=b`.
    fn parse_hop_range(&mut self) -> Result<(usize, usize), UqlError> {
        if !self.eat(&Tok::LBrace) {
            return Ok((1, 1));
        }
        let min = self.expect_usize("a minimum hop count")?;
        let max = if self.eat(&Tok::Comma) || self.eat(&Tok::DotDot) {
            self.expect_usize("a maximum hop count")?
        } else {
            min
        };
        self.expect(&Tok::RBrace, "`}` to close the hop range")?;
        if max < min {
            return Err(self.err_here("hop range max must be ≥ min"));
        }
        Ok((min, max))
    }

    /// `rank = "RANK" "BY" "~" vector_ref`. An inline literal vector lowers to `Op::Rank`;
    /// a quoted `~ "text"` lowers to `Op::RankEmbed` (the server-side NL→vector seam,
    /// CONCEPT:EG-KG.compute.no-embedder-bound-op); a bare-ident handle is the still-reserved by-name-embedding seam.
    fn parse_rank(&mut self) -> Result<Op, UqlError> {
        self.expect_kw("BY")?;
        self.expect(&Tok::Tilde, "`~` before the rank vector (`RANK BY ~[…]`)")?;
        match self.parse_vector_ref()? {
            VectorRef::Inline(query) => Ok(Op::Rank { query }),
            VectorRef::Text(text) => Ok(Op::RankEmbed { text }),
            VectorRef::Handle(_) => Err(self.err_at(
                self.prev_start(),
                "a bare-ident embedding handle (`RANK BY ~handle`) is a reserved forward \
                 seam (no by-name embedding registry yet) — use a quoted query \
                 `RANK BY ~\"text\"` (server-side embedded) or an inline literal vector \
                 `RANK BY ~[…]`",
            )),
        }
    }

    /// `vector_ref = "[" signed_num {"," signed_num} "]" | string | ident`. A component
    /// may be negative (`~[-0.1, 0.2, -0.3]`, CONCEPT:EG-KG.compute.negative-vector-component-parses) — a leading `-` negates the
    /// following number, matching the Rust builder / wire DTO which accept negatives.
    fn parse_vector_ref(&mut self) -> Result<VectorRef, UqlError> {
        if self.eat(&Tok::LBracket) {
            let mut v = Vec::new();
            if !self.peek(&Tok::RBracket) {
                loop {
                    let neg = self.eat(&Tok::Dash);
                    let n = self.expect_num("a vector component")? as f32;
                    v.push(if neg { -n } else { n });
                    if !self.eat(&Tok::Comma) {
                        break;
                    }
                }
            }
            self.expect(&Tok::RBracket, "`]` to close the rank vector")?;
            Ok(VectorRef::Inline(v))
        } else if let Some(Tok::Str(s)) = self.peek_kind() {
            let s = s.clone();
            self.bump();
            Ok(VectorRef::Text(s))
        } else if let Some(Tok::Ident(s)) = self.peek_kind() {
            let s = s.clone();
            self.bump();
            Ok(VectorRef::Handle(s))
        } else {
            Err(self.err_here("expected a rank vector: `[…]`, a quoted ref, or a name"))
        }
    }

    /// `limit = "LIMIT" int`.
    fn parse_limit(&mut self) -> Result<Op, UqlError> {
        let k = self.expect_usize("a `LIMIT` count")?;
        Ok(Op::Limit { k })
    }

    /// `asof = "AS" "OF" [ "TX" | "VALID" ] "@" num` → `Op::AsOf { ts, axis }`
    /// (CONCEPT:EG-KG.query.sparql-completeness / KG-2.250). The optional axis keyword selects the timeline:
    /// `TX` (or `TRANSACTION`) pins transaction time ("what we BELIEVED at ts"); the
    /// default / `VALID` pins valid time ("what was TRUE at ts"). The instant is a
    /// unix-seconds number prefixed by the `@` sigil (already lexed).
    fn parse_asof(&mut self) -> Result<Op, UqlError> {
        self.expect_kw("OF")?;
        let axis = match self.peek_kind() {
            Some(Tok::Ident(w))
                if w.eq_ignore_ascii_case("tx") || w.eq_ignore_ascii_case("transaction") =>
            {
                self.bump();
                TimeAxis::Transaction
            }
            Some(Tok::Ident(w)) if w.eq_ignore_ascii_case("valid") => {
                self.bump();
                TimeAxis::Valid
            }
            _ => TimeAxis::Valid,
        };
        self.expect(
            &Tok::At,
            "`@` before the AS-OF timestamp (`AS OF [TX] @<ts>`)",
        )?;
        let ts = self.expect_num("an AS-OF timestamp (unix seconds)")?;
        Ok(Op::AsOf { ts, axis })
    }

    /// `window = "WINDOW" num [ unit ] [ agg ]` → `Op::Window { secs }` (mean, CONCEPT:
    /// KG-2.235 / EG-413) or `Op::WindowAgg { secs, agg }` when an aggregate selector is
    /// present (CONCEPT:EG-KG.compute.trailing-aggregate-selector-lowers). A bare number is seconds; an optional unit suffix
    /// (`s`/`m`/`h`/`d`) scales it; an optional trailing aggregate keyword
    /// (`mean`/`avg`, `sum`, `min`, `max`, `count`, `first`, `last`) selects the windowed
    /// aggregate. The unit is matched FIRST, so `WINDOW 30 min` stays 30 MINUTES (not the
    /// `min` aggregate); write `WINDOW 30 s min` for a 30-second min-aggregate.
    fn parse_window(&mut self) -> Result<Op, UqlError> {
        let n = self.expect_num("a WINDOW duration (`WINDOW <dur>`)")?;
        let scale = match self.peek_kind() {
            Some(Tok::Ident(u)) => {
                let s = match u.to_ascii_lowercase().as_str() {
                    "s" | "sec" | "secs" => Some(1.0),
                    "m" | "min" | "mins" => Some(60.0),
                    "h" | "hr" | "hrs" => Some(3600.0),
                    "d" | "day" | "days" => Some(86_400.0),
                    _ => None,
                };
                match s {
                    Some(scale) => {
                        self.bump();
                        scale
                    }
                    // An un-recognized trailing word is the NEXT thing (an agg or a stage).
                    None => 1.0,
                }
            }
            _ => 1.0,
        };
        let secs = n * scale;
        // Optional trailing aggregate selector (CONCEPT:EG-KG.compute.trailing-aggregate-selector-lowers). A bareword that names a
        // known aggregate is consumed; anything else is left for the next stage.
        match self.parse_window_agg() {
            Some(agg) => Ok(Op::WindowAgg { secs, agg }),
            None => Ok(Op::Window { secs }),
        }
    }

    /// Peek an optional trailing `WINDOW` aggregate selector (CONCEPT:EG-KG.compute.trailing-aggregate-selector-lowers), consuming
    /// and returning its canonical name if the next token is a known aggregate keyword;
    /// `None` otherwise (the token is left for the next stage). Case-insensitive.
    fn parse_window_agg(&mut self) -> Option<String> {
        let name = match self.peek_kind()? {
            Tok::Ident(w) => match w.to_ascii_lowercase().as_str() {
                "mean" | "avg" | "average" => "mean",
                "sum" => "sum",
                "min" | "minimum" => "min",
                "max" | "maximum" => "max",
                "count" => "count",
                "first" => "first",
                "last" => "last",
                _ => return None,
            },
            _ => return None,
        };
        self.bump();
        Some(name.to_string())
    }

    /// `foreign = "FOREIGN" ( string | ident )` → `Op::Foreign { name }`
    /// (CONCEPT:EG-KG.query.sparql-completeness). The federation source name (a registered peer).
    fn parse_foreign(&mut self) -> Result<Op, UqlError> {
        let name = match self.peek_kind() {
            Some(Tok::Str(s)) | Some(Tok::Ident(s)) => {
                let s = s.clone();
                self.bump();
                s
            }
            _ => return Err(self.err_here("expected a FOREIGN source name (`FOREIGN \"<name>\"`)")),
        };
        Ok(Op::Foreign { name })
    }

    /// `rerank = "RERANK" ( "NODE_DISTANCE" "FROM" id | "MENTIONS" )` → a graph-native
    /// reranker op (CONCEPT:EG-KG.query.uql-parser-ops). `NODE_DISTANCE FROM <id>` re-orders by proximity
    /// to a focal node; `MENTIONS` re-orders by incoming-edge (provenance) salience.
    fn parse_rerank(&mut self) -> Result<Op, UqlError> {
        if self.peek_kw("MENTIONS") {
            self.bump();
            return Ok(Op::RankMentions {});
        }
        if self.peek_kw("MMR") {
            self.bump();
            let lambda = self.expect_num("an MMR lambda (`RERANK MMR <lambda> <k>`)")? as f32;
            let k = self.expect_usize("an MMR k count")?;
            return Ok(Op::RankMmr { lambda, k });
        }
        if self.peek_kw("NODE_DISTANCE") {
            self.bump();
            self.expect_kw("FROM")?;
            let center = match self.peek_kind() {
                Some(Tok::Str(s)) | Some(Tok::Ident(s)) => {
                    let s = s.clone();
                    self.bump();
                    s
                }
                _ => {
                    return Err(self
                        .err_here("expected a center node id (`RERANK NODE_DISTANCE FROM <id>`)"))
                }
            };
            return Ok(Op::RankNodeDistance { center });
        }
        Err(self.err_here(
            "expected `NODE_DISTANCE FROM <id>`, `MENTIONS`, or `MMR <lambda> <k>` after RERANK",
        ))
    }

    /// `reason = "REASON" (iri | ident | string)` → `Op::Reason` (CONCEPT:EG-KG.query.sparql-completeness /
    /// EG-375). The OWL-inferred members of the named class seed (or, mid-pipeline, filter)
    /// the RowSet. The class may be a bare label (`REASON Mammal`) OR an explicit angle-
    /// bracketed class IRI (`REASON <http://ex/Device>` — CONCEPT:EG-KG.query.reason-iri-parses-angle), the form the
    /// string-type↔IRI-class bridge resolves against. Feature-gated to `owl` — the feature
    /// that compiles the `Op::Reason` variant + its executor.
    #[cfg(feature = "owl")]
    fn parse_reason(&mut self) -> Result<Op, UqlError> {
        let target_class = match self.peek_kind() {
            Some(Tok::Iri(s)) | Some(Tok::Str(s)) | Some(Tok::Ident(s)) => {
                let s = s.clone();
                self.bump();
                s
            }
            _ => {
                return Err(
                    self.err_here("expected a class name or IRI (`REASON <http://…/Class>`)")
                )
            }
        };
        Ok(Op::Reason {
            target_class,
            ontology: String::new(),
        })
    }

    /// `REASON` in a build WITHOUT `owl`: the `Op::Reason` variant is cfg'd out, so the
    /// clause is accepted lexically but rejected with a clear "not in this build"
    /// message (never a phantom Op, never a panic).
    #[cfg(not(feature = "owl"))]
    fn parse_reason(&mut self) -> Result<Op, UqlError> {
        // Consume the class token so the error caret points at `REASON`, not past it.
        if matches!(
            self.peek_kind(),
            Some(Tok::Iri(_)) | Some(Tok::Str(_)) | Some(Tok::Ident(_))
        ) {
            self.bump();
        }
        Err(self.err_at(
            self.prev_start(),
            "`REASON` requires the OWL reasoner (build feature `owl`/`owl-plan`); \
             not available in this build",
        ))
    }

    // ── epistemic stage keywords (E2, CONCEPT:EG-KG.epistemic.epistemic-substrate) ──────
    //
    // Each keyword is ALWAYS recognized by `parse_stage` (so a build without `epistemic`
    // still gives a specific "not in this build" error rather than a generic "unexpected
    // token" one — the `REASON`/`TEXT` precedent above); only the resulting `Op` variant
    // is feature-gated.

    /// Parse a bare `<id>` argument (a quoted string or identifier) — the shared shape
    /// `EVIDENCE FOR <id>` / `CONTRADICTS <id>` / `SUPPORTED BY <id>` / `SOURCE
    /// RELIABILITY <id>` / `EXPLAIN BELIEF <id>` all take.
    #[cfg(feature = "epistemic")]
    fn expect_epistemic_id(&mut self, what: &str) -> Result<String, UqlError> {
        match self.peek_kind() {
            Some(Tok::Str(s)) | Some(Tok::Ident(s)) => {
                let s = s.clone();
                self.bump();
                Ok(s)
            }
            _ => Err(self.err_here(&format!("expected {what}"))),
        }
    }

    /// The shared `"AS" "OF" "@" num` tail `BELIEF AS OF <ts>` and `VALID AS OF <ts>`
    /// both parse (their leading keyword already fixed the axis, so — unlike the general
    /// `AS OF [TX|VALID] @ts` — there is no axis modifier here).
    #[cfg(feature = "epistemic")]
    fn expect_as_of_ts(&mut self) -> Result<f64, UqlError> {
        self.expect_kw("AS")?;
        self.expect_kw("OF")?;
        self.expect(&Tok::At, "`@` before the AS-OF timestamp")?;
        self.expect_num("an AS-OF timestamp (unix seconds)")
    }

    /// `evidence_for = "EVIDENCE" "FOR" id` → `Op::EvidenceFor { claim_id }`
    /// (CONCEPT:EG-KG.epistemic.epistemic-substrate). The evidence FOR the named claim —
    /// nodes linked to it by an incoming SUPPORTS-classified edge. Feature-gated to
    /// `epistemic` — the feature that compiles the `Op::EvidenceFor` variant + executor.
    #[cfg(feature = "epistemic")]
    fn parse_evidence_for(&mut self) -> Result<Op, UqlError> {
        self.expect_kw("FOR")?;
        let claim_id = self.expect_epistemic_id("a claim id (`EVIDENCE FOR <id>`)")?;
        Ok(Op::EvidenceFor { claim_id })
    }

    /// `EVIDENCE` in a build WITHOUT `epistemic`: the `Op::EvidenceFor` variant is cfg'd
    /// out, so the clause is accepted lexically but rejected with a clear message.
    #[cfg(not(feature = "epistemic"))]
    fn parse_evidence_for(&mut self) -> Result<Op, UqlError> {
        if self.peek_kw("FOR") {
            self.bump();
        }
        if matches!(self.peek_kind(), Some(Tok::Str(_)) | Some(Tok::Ident(_))) {
            self.bump();
        }
        Err(self.err_at(
            self.prev_start(),
            "`EVIDENCE FOR` requires the epistemic belief substrate (build feature \
             `epistemic`); not available in this build",
        ))
    }

    /// `contradicts = "CONTRADICTS" id` → `Op::Contradicts { node_id }`
    /// (CONCEPT:EG-KG.epistemic.epistemic-substrate). Evidence AGAINST the named node —
    /// nodes linked to it by an incoming CONTRADICTS- or ATTACKS-classified edge.
    #[cfg(feature = "epistemic")]
    fn parse_contradicts(&mut self) -> Result<Op, UqlError> {
        let node_id = self.expect_epistemic_id("a node id (`CONTRADICTS <id>`)")?;
        Ok(Op::Contradicts { node_id })
    }

    /// `CONTRADICTS` in a build WITHOUT `epistemic`: mirrors `parse_evidence_for`.
    #[cfg(not(feature = "epistemic"))]
    fn parse_contradicts(&mut self) -> Result<Op, UqlError> {
        if matches!(self.peek_kind(), Some(Tok::Str(_)) | Some(Tok::Ident(_))) {
            self.bump();
        }
        Err(self.err_at(
            self.prev_start(),
            "`CONTRADICTS` requires the epistemic belief substrate (build feature \
             `epistemic`); not available in this build",
        ))
    }

    /// `supported_by = "SUPPORTED" "BY" id` → `Op::SupportedBy { node_id }`
    /// (CONCEPT:EG-KG.epistemic.epistemic-substrate). The claims the named node itself
    /// supports — the mirror direction of `EVIDENCE FOR`.
    #[cfg(feature = "epistemic")]
    fn parse_supported_by(&mut self) -> Result<Op, UqlError> {
        self.expect_kw("BY")?;
        let node_id = self.expect_epistemic_id("a node id (`SUPPORTED BY <id>`)")?;
        Ok(Op::SupportedBy { node_id })
    }

    /// `SUPPORTED` in a build WITHOUT `epistemic`: mirrors `parse_evidence_for`.
    #[cfg(not(feature = "epistemic"))]
    fn parse_supported_by(&mut self) -> Result<Op, UqlError> {
        if self.peek_kw("BY") {
            self.bump();
        }
        if matches!(self.peek_kind(), Some(Tok::Str(_)) | Some(Tok::Ident(_))) {
            self.bump();
        }
        Err(self.err_at(
            self.prev_start(),
            "`SUPPORTED BY` requires the epistemic belief substrate (build feature \
             `epistemic`); not available in this build",
        ))
    }

    /// `belief_asof = "BELIEF" "AS" "OF" "@" num` → `Op::BeliefAsOf { ts }`
    /// (CONCEPT:EG-KG.epistemic.epistemic-substrate). Pins the TRANSACTION-time axis
    /// (what the engine BELIEVED at `ts`) and re-scores by propagated confidence — the
    /// belief-propagating sibling of the pure-alias `VALID AS OF`.
    #[cfg(feature = "epistemic")]
    fn parse_belief_asof(&mut self) -> Result<Op, UqlError> {
        let ts = self.expect_as_of_ts()?;
        Ok(Op::BeliefAsOf { ts })
    }

    /// `BELIEF` in a build WITHOUT `epistemic`: mirrors `parse_evidence_for`; best-effort
    /// consumes an `AS OF @<ts>` tail if present so the caret still lands sensibly.
    #[cfg(not(feature = "epistemic"))]
    fn parse_belief_asof(&mut self) -> Result<Op, UqlError> {
        if self.peek_kw("AS") {
            self.bump();
        }
        if self.peek_kw("OF") {
            self.bump();
        }
        let _ = self.eat(&Tok::At);
        if matches!(self.peek_kind(), Some(Tok::Num(_))) {
            self.bump();
        }
        Err(self.err_at(
            self.prev_start(),
            "`BELIEF AS OF` requires the epistemic belief substrate (build feature \
             `epistemic`); not available in this build",
        ))
    }

    /// `valid_asof = "VALID" "AS" "OF" "@" num` → `Op::AsOf { ts, axis: Valid }`
    /// (CONCEPT:EG-KG.epistemic.epistemic-substrate). A PURE ALIAS for the axis the bare
    /// `AS OF @ts` / `AS OF VALID @ts` forms already produce — a strict superset (new
    /// leading-keyword surface only); it never changes what `AS OF` itself parses to
    /// (pinned by `asof_valid_alias_is_byte_identical_to_bare_as_of` in `tests`).
    #[cfg(feature = "epistemic")]
    fn parse_valid_asof(&mut self) -> Result<Op, UqlError> {
        let ts = self.expect_as_of_ts()?;
        Ok(Op::AsOf {
            ts,
            axis: TimeAxis::Valid,
        })
    }

    /// `VALID` in a build WITHOUT `epistemic`: mirrors `parse_belief_asof`'s fallback.
    /// (The plain `AS OF VALID @ts` trailing-modifier form is UNAFFECTED — it lives in
    /// `parse_asof`, gated only on base `query`.)
    #[cfg(not(feature = "epistemic"))]
    fn parse_valid_asof(&mut self) -> Result<Op, UqlError> {
        if self.peek_kw("AS") {
            self.bump();
        }
        if self.peek_kw("OF") {
            self.bump();
        }
        let _ = self.eat(&Tok::At);
        if matches!(self.peek_kind(), Some(Tok::Num(_))) {
            self.bump();
        }
        Err(self.err_at(
            self.prev_start(),
            "`VALID AS OF` requires the epistemic belief substrate (build feature \
             `epistemic`); not available in this build",
        ))
    }

    /// `source_reliability = "SOURCE" "RELIABILITY" id` → `Op::SourceReliability
    /// { source_id }` (CONCEPT:EG-KG.epistemic.epistemic-substrate). Re-weights the
    /// current candidate set by the named source's propagated reliability.
    #[cfg(feature = "epistemic")]
    fn parse_source_reliability(&mut self) -> Result<Op, UqlError> {
        self.expect_kw("RELIABILITY")?;
        let source_id = self.expect_epistemic_id("a source id (`SOURCE RELIABILITY <id>`)")?;
        Ok(Op::SourceReliability { source_id })
    }

    /// `SOURCE` in a build WITHOUT `epistemic`: mirrors `parse_evidence_for`.
    #[cfg(not(feature = "epistemic"))]
    fn parse_source_reliability(&mut self) -> Result<Op, UqlError> {
        if self.peek_kw("RELIABILITY") {
            self.bump();
        }
        if matches!(self.peek_kind(), Some(Tok::Str(_)) | Some(Tok::Ident(_))) {
            self.bump();
        }
        Err(self.err_at(
            self.prev_start(),
            "`SOURCE RELIABILITY` requires the epistemic belief substrate (build feature \
             `epistemic`); not available in this build",
        ))
    }

    /// `confidence = "CONFIDENCE"` → `Op::ConfidenceOp {}` (CONCEPT:EG-KG.epistemic.epistemic-substrate).
    /// No argument — re-scores each row already in the RowSet by its own propagated
    /// belief confidence.
    #[cfg(feature = "epistemic")]
    fn parse_confidence(&mut self) -> Result<Op, UqlError> {
        Ok(Op::ConfidenceOp {})
    }

    /// `CONFIDENCE` in a build WITHOUT `epistemic`: the `Op::ConfidenceOp` variant is
    /// cfg'd out, so the (argument-less) clause is rejected with a clear message.
    #[cfg(not(feature = "epistemic"))]
    fn parse_confidence(&mut self) -> Result<Op, UqlError> {
        Err(self.err_at(
            self.prev_start(),
            "`CONFIDENCE` requires the epistemic belief substrate (build feature \
             `epistemic`); not available in this build",
        ))
    }

    /// `explain_belief = "EXPLAIN" "BELIEF" id` → `Op::ExplainBelief { node_id }`
    /// (CONCEPT:EG-KG.epistemic.epistemic-substrate). Flattens the recursive
    /// justification tree (`eg_epistemic::explain_belief`) into scored RowSet rows.
    #[cfg(feature = "epistemic")]
    fn parse_explain_belief(&mut self) -> Result<Op, UqlError> {
        self.expect_kw("BELIEF")?;
        let node_id = self.expect_epistemic_id("a node id (`EXPLAIN BELIEF <id>`)")?;
        Ok(Op::ExplainBelief { node_id })
    }

    /// `EXPLAIN` in a build WITHOUT `epistemic`: mirrors `parse_evidence_for`.
    #[cfg(not(feature = "epistemic"))]
    fn parse_explain_belief(&mut self) -> Result<Op, UqlError> {
        if self.peek_kw("BELIEF") {
            self.bump();
        }
        if matches!(self.peek_kind(), Some(Tok::Str(_)) | Some(Tok::Ident(_))) {
            self.bump();
        }
        Err(self.err_at(
            self.prev_start(),
            "`EXPLAIN BELIEF` requires the epistemic belief substrate (build feature \
             `epistemic`); not available in this build",
        ))
    }

    /// `text = "TEXT" string` → `Op::RankText { query }` (CONCEPT:EG-KG.query.sparql-completeness). Re-ranks
    /// the candidate RowSet by BM25 relevance to the natural-language query. Feature-
    /// gated to `text` — the feature that compiles the `Op::RankText` variant + the
    /// lexical index executor.
    #[cfg(feature = "text")]
    fn parse_text(&mut self) -> Result<Op, UqlError> {
        match self.peek_kind() {
            Some(Tok::Str(s)) => {
                let query = s.clone();
                self.bump();
                Ok(Op::RankText { query })
            }
            _ => Err(self.err_here("expected a quoted query (`TEXT \"<q>\"`)")),
        }
    }

    /// `TEXT` in a build WITHOUT `text`: the `Op::RankText` variant is cfg'd out, so the
    /// clause is accepted lexically but rejected with a clear message.
    #[cfg(not(feature = "text"))]
    fn parse_text(&mut self) -> Result<Op, UqlError> {
        if matches!(self.peek_kind(), Some(Tok::Str(_))) {
            self.bump();
        }
        Err(self.err_at(
            self.prev_start(),
            "`TEXT` requires the BM25 lexical index (build feature `text`); \
             not available in this build",
        ))
    }

    /// `fuse = "FUSE" "[" branch "]" { "[" branch "]" }` where `branch = stage { "|>"
    /// stage }` → `Op::FuseRrf { branches, k }` (CONCEPT:EG-KG.compute.fuse-stage-now-dispatches / KG-2.215/2.253). Each
    /// bracketed `[ … ]` is a sub-pipeline branch (a `Vec<Op>`); the branches are
    /// reciprocal-rank-fused. `k` defaults to `0.0` — the executor reads that as the
    /// canonical `eg_text::RRF_K` — matching the Rust builder's `Op::FuseRrf` shape (the
    /// UQL surface expresses exactly what the builder/wire already could). The canonical
    /// tri-modal hybrid is `FUSE [RANK BY ~[…]] [TEXT "q"] [RERANK NODE_DISTANCE FROM "n"]`.
    /// Feature-gated to `text` — the feature that compiles the `Op::FuseRrf` variant + the
    /// RRF executor.
    #[cfg(feature = "text")]
    fn parse_fuse(&mut self) -> Result<Op, UqlError> {
        let mut branches: Vec<Vec<Op>> = Vec::new();
        while self.peek(&Tok::LBracket) {
            self.bump(); // consume `[`
            let mut ops: Vec<Op> = Vec::new();
            self.parse_stage(&mut ops)?;
            while self.eat(&Tok::Pipe) {
                self.parse_stage(&mut ops)?;
            }
            self.expect(&Tok::RBracket, "`]` to close a FUSE branch")?;
            branches.push(ops);
        }
        if branches.is_empty() {
            return Err(self.err_here(
                "expected at least one `[branch]` after FUSE (`FUSE [RANK BY ~[…]] [TEXT \"q\"]`)",
            ));
        }
        // k = 0.0 ⇒ the executor uses the canonical `eg_text::RRF_K` default.
        Ok(Op::FuseRrf { branches, k: 0.0 })
    }

    /// `FUSE` in a build WITHOUT `text`: the `Op::FuseRrf` variant is cfg'd out, so the
    /// clause is accepted lexically but rejected with a clear message (mirrors `TEXT`).
    #[cfg(not(feature = "text"))]
    fn parse_fuse(&mut self) -> Result<Op, UqlError> {
        Err(self.err_at(
            self.prev_start(),
            "`FUSE` requires the RRF hybrid / BM25 text index (build feature `text`); \
             not available in this build",
        ))
    }

    /// `pred_list = pred { "AND" pred }`.
    fn parse_pred_list(&mut self) -> Result<Vec<Pred>, UqlError> {
        let mut preds = vec![self.parse_pred()?];
        while self.peek_kw("AND") {
            self.bump();
            preds.push(self.parse_pred()?);
        }
        Ok(preds)
    }

    /// `pred = prop ( ">" | "<" | "=" ) value`. `>`/`<` require a numeric RHS
    /// (`GtNum`/`LtNum`); `=` is string equality (`Eq`, JSON-stringified value).
    fn parse_pred(&mut self) -> Result<Pred, UqlError> {
        let prop = self.expect_ident("a property name")?;
        if self.eat(&Tok::Gt) {
            let n = self.expect_num("a numeric value after `>`")?;
            Ok(Pred::GtNum { prop, n })
        } else if self.eat(&Tok::Lt) {
            let n = self.expect_num("a numeric value after `<`")?;
            Ok(Pred::LtNum { prop, n })
        } else if self.eat(&Tok::Eq) {
            let value = self.expect_value("a value after `=`")?;
            Ok(Pred::Eq { prop, value })
        } else {
            Err(self.err_here("expected a comparison `>`, `<`, or `=`"))
        }
    }

    // ── token helpers ───────────────────────────────────────────────────────

    fn peek_kind(&self) -> Option<&Tok> {
        self.toks.get(self.pos).map(|t| &t.kind)
    }

    fn peek(&self, k: &Tok) -> bool {
        self.peek_kind() == Some(k)
    }

    /// Case-insensitive keyword lookahead (the lexer makes no keyword distinction).
    fn peek_kw(&self, kw: &str) -> bool {
        matches!(self.peek_kind(), Some(Tok::Ident(s)) if s.eq_ignore_ascii_case(kw))
    }

    fn bump(&mut self) {
        self.pos += 1;
    }

    fn eat(&mut self, k: &Tok) -> bool {
        if self.peek(k) {
            self.bump();
            true
        } else {
            false
        }
    }

    fn expect(&mut self, k: &Tok, what: &str) -> Result<(), UqlError> {
        if self.eat(k) {
            Ok(())
        } else {
            Err(self.err_here(&format!("expected {what}")))
        }
    }

    fn expect_kw(&mut self, kw: &str) -> Result<(), UqlError> {
        if self.peek_kw(kw) {
            self.bump();
            Ok(())
        } else {
            Err(self.err_here(&format!("expected keyword `{kw}`")))
        }
    }

    fn expect_ident(&mut self, what: &str) -> Result<String, UqlError> {
        match self.peek_kind() {
            Some(Tok::Ident(s)) => {
                let s = s.clone();
                self.bump();
                Ok(s)
            }
            _ => Err(self.err_here(&format!("expected {what}"))),
        }
    }

    fn expect_num(&mut self, what: &str) -> Result<f64, UqlError> {
        match self.peek_kind() {
            Some(Tok::Num(n)) => {
                let n = *n;
                self.bump();
                Ok(n)
            }
            _ => Err(self.err_here(&format!("expected {what}"))),
        }
    }

    fn expect_usize(&mut self, what: &str) -> Result<usize, UqlError> {
        let at = self.cur_start();
        let n = self.expect_num(what)?;
        if n < 0.0 || n.fract() != 0.0 {
            return Err(self.err_at(at, &format!("{what} must be a non-negative integer")));
        }
        Ok(n as usize)
    }

    /// A predicate RHS value: a string, a number (stringified), or a bare word.
    /// All become the `String` an `Eq` predicate compares against (the executor
    /// JSON-stringifies the stored value the same way).
    fn expect_value(&mut self, what: &str) -> Result<String, UqlError> {
        match self.peek_kind() {
            Some(Tok::Str(s)) => {
                let s = s.clone();
                self.bump();
                Ok(s)
            }
            Some(Tok::Ident(s)) => {
                let s = s.clone();
                self.bump();
                Ok(s)
            }
            Some(Tok::Num(n)) => {
                let n = *n;
                self.bump();
                // Render an integral f64 without a trailing `.0` so `= 3` matches the
                // stored integer's JSON string form.
                Ok(if n.fract() == 0.0 {
                    format!("{}", n as i64)
                } else {
                    format!("{n}")
                })
            }
            _ => Err(self.err_here(&format!("expected {what}"))),
        }
    }

    fn expect_eof(&mut self) -> Result<(), UqlError> {
        if self.pos >= self.toks.len() {
            Ok(())
        } else {
            Err(self.err_here("unexpected trailing tokens after the query"))
        }
    }

    // ── error positioning ───────────────────────────────────────────────────

    /// Byte offset of the current token (or end-of-input).
    fn cur_start(&self) -> usize {
        self.toks.get(self.pos).map(|t| t.start).unwrap_or(self.end)
    }

    /// Byte offset of the previously consumed token (for "this token was wrong").
    fn prev_start(&self) -> usize {
        self.pos
            .checked_sub(1)
            .and_then(|i| self.toks.get(i))
            .map(|t| t.start)
            .unwrap_or(self.end)
    }

    /// Build an error anchored at the current token, naming what was actually found.
    fn err_here(&self, msg: &str) -> UqlError {
        let found = match self.peek_kind() {
            Some(t) => format!(", found {t}"),
            None => ", found end of input".to_string(),
        };
        UqlError {
            msg: format!("{msg}{found}"),
            at: self.cur_start(),
        }
    }

    fn err_at(&self, at: usize, msg: &str) -> UqlError {
        UqlError {
            msg: msg.to_string(),
            at,
        }
    }
}
