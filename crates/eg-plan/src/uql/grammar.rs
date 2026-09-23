//! THE UQL grammar — the single source of truth (UQL-10).
//!
//! Every production is one row of [`PRODUCTIONS`]: its EBNF, what it lowers to, the
//! build feature it needs, its role (source / stage / auxiliary) and an example. From
//! this one table come:
//!  * the reference EBNF in `docs/uql.md` ([`ebnf`]; a test fails when the doc drifts),
//!  * the NL planner's system prompt ([`nl_system_prompt`]),
//!  * the "expected …" lists in parse errors ([`stage_keywords`]),
//!  * the head-of-pipeline warning (a [`Role::Stage`] leading a pipeline),
//!  * the example suite (every example parses, or fails with `UQL_FEATURE_NOT_IN_BUILD`
//!    naming exactly [`Production::feature`] in a build without it).
//!
//! The parser's dispatch table (`Parser::stage_table`) is checked against [`PRODUCTIONS`] in
//! both directions, so a keyword cannot be added to one and not the other.

/// A cargo feature of eg-plan a production's executor needs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Feature {
    Text,
    Owl,
    WasmUdf,
    Federation,
    Geo,
    Tensor,
    Stream,
    Timeseries,
    Probabilistic,
    Epistemic,
}

impl Feature {
    /// The eg-plan cargo feature name.
    pub fn name(self) -> &'static str {
        match self {
            Feature::Text => "text",
            Feature::Owl => "owl",
            Feature::WasmUdf => "wasm-udf",
            Feature::Federation => "federation",
            Feature::Geo => "geo",
            Feature::Tensor => "tensor",
            Feature::Stream => "stream",
            Feature::Timeseries => "timeseries",
            Feature::Probabilistic => "probabilistic",
            Feature::Epistemic => "epistemic",
        }
    }

    /// Is the feature compiled into this build?
    pub fn enabled(self) -> bool {
        match self {
            Feature::Text => cfg!(feature = "text"),
            Feature::Owl => cfg!(feature = "owl"),
            Feature::WasmUdf => cfg!(feature = "wasm-udf"),
            Feature::Federation => cfg!(feature = "federation"),
            Feature::Geo => cfg!(feature = "geo"),
            Feature::Tensor => cfg!(feature = "tensor"),
            Feature::Stream => cfg!(feature = "stream"),
            Feature::Timeseries => cfg!(feature = "timeseries"),
            Feature::Probabilistic => cfg!(feature = "probabilistic"),
            Feature::Epistemic => cfg!(feature = "epistemic"),
        }
    }
}

/// Where a production may stand in a pipeline.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    /// Seeds rows from nothing — belongs at the head.
    Source,
    /// Seeds at the head, narrows mid-pipeline.
    SourceOrStage,
    /// Transforms its input; at the head it sees an empty RowSet.
    Stage,
    /// A sub-rule (statement structure, predicates, literals).
    Aux,
}

/// One grammar production.
#[derive(Clone, Copy, Debug)]
pub struct Production {
    pub name: &'static str,
    pub role: Role,
    /// The leading keyword(s) that select it (`&[]` for auxiliary rules).
    pub lead: &'static [&'static str],
    pub feature: Option<Feature>,
    /// EBNF right-hand side.
    pub rule: &'static str,
    /// What it lowers to.
    pub lowers: &'static str,
    /// A complete query that uses it (empty for auxiliary rules).
    pub example: &'static str,
}

const fn p(
    name: &'static str,
    role: Role,
    lead: &'static [&'static str],
    feature: Option<Feature>,
    rule: &'static str,
    lowers: &'static str,
    example: &'static str,
) -> Production {
    Production {
        name,
        role,
        lead,
        feature,
        rule,
        lowers,
        example,
    }
}

use Feature::*;
use Role::*;

/// The whole grammar, in reference order.
pub const PRODUCTIONS: &[Production] = &[
    // ── statements ──
    p("statement", Aux, &[], None,
      "[ \"UQL\" int \";\" ] [ \"EXPLAIN\" | \"PROFILE\" ] { binding } pipeline",
      "Statement { mode, body }", ""),
    p("binding", Aux, &["LET"], None, "\"LET\" name \"=\" pipeline \";\"",
      "a named sub-plan (PlanDag node)", ""),
    p("pipeline", Aux, &[], None, "head { \"|>\" stage }", "Plan / PlanDag chain", ""),
    p("head", Aux, &["FROM", "JOIN"], None,
      "source | stage | \"FROM\" name | \"JOIN\" name \",\" name { \",\" name }",
      "a DAG input edge (FROM) or multi-input join (JOIN)", ""),
    // ── sources ──
    p("match", Source, &["MATCH"], None,
      "\"MATCH\" \"(\" [ \":\" name ] \")\" [ \"WHERE\" pred ]",
      "Scan{label} / ScanAll{} (+ Filter)", "MATCH (:Doc) WHERE year > 2024"),
    p("foreign", Source, &["FOREIGN"], None,
      "\"FOREIGN\" id | \"FOREIGN\" \"SCAN\" string [ \"JOIN\" ] | \"FOREIGN\" \"HTTP\" string \
       [ \"PATH\" string ] \"ID\" string [ \"SCORE\" string ] [ \"JOIN\" ]",
      "Foreign{name} / ForeignScan{Named|HttpJson, join} (federation)", "FOREIGN 'peer-east'"),
    p("sparql", Source, &["SPARQL"], Some(Owl), "\"SPARQL\" string \"VAR\" string",
      "SparqlBgp{query, var}", "SPARQL 'SELECT ?x WHERE { ?x a <http://ex/T> }' VAR 'x'"),
    p("tsscan", Source, &["TSSCAN"], Some(Timeseries),
      "\"TSSCAN\" string_list \"FROM\" num \"TO\" num", "TsScan{series, from, to}",
      "TSSCAN ['cpu'] FROM 0 TO 3600 |> WINDOW 60 s MEAN"),
    p("sensor", Source, &["SENSOR"], Some(Timeseries),
      "\"SENSOR\" \"FUSE\" string_list \"TOLERANCE\" int | \"SENSOR\" \"ALIGN\" \"[\" string interp \
       { \",\" string interp } \"]\" \"CLOCK\" clock [ \"TOLERANCE\" int ]",
      "SensorFuse / SensorAlign (ns integers)",
      "SENSOR ALIGN ['imu' LINEAR, 'gps' NEAREST] CLOCK UNIFORM FROM 0 TO 1000 STEP 10"),
    p("clock", Aux, &[], None,
      "\"UNIFORM\" \"FROM\" int \"TO\" int \"STEP\" int | \"TUMBLING\" \"WIDTH\" int \"STEP\" int",
      "FuseClock", ""),
    p("interp", Aux, &[], None, "\"NEAREST\" | \"LINEAR\" | \"ASOF_HOLD\"", "FuseInterp", ""),
    // ── source-or-stage (seed at the head, narrow mid-pipeline) ──
    p("filter", SourceOrStage, &["WHERE"], None, "\"WHERE\" pred", "Filter{preds}",
      "MATCH () |> WHERE status IN ('open', 'new') AND NOT archived = TRUE"),
    p("as_of", SourceOrStage, &["AS"], None,
      "\"AS\" \"OF\" [ \"TX\" | \"VALID\" ] ts", "AsOf{ts, axis}",
      "MATCH (:Event) |> AS OF TX @-86400.5"),
    p("valid_as_of", SourceOrStage, &["VALID"], None, "\"VALID\" \"AS\" \"OF\" ts",
      "AsOf{ts, axis: Valid} (alias)", "MATCH (:Event) |> VALID AS OF @1700000000"),
    p("reason", SourceOrStage, &["REASON"], Some(Owl),
      "\"REASON\" ( iri | string | name ) [ \"ONTOLOGY\" string ]",
      "Reason{target_class, ontology}", "REASON <http://ex/Device> |> LIMIT 5"),
    p("evidence_for", SourceOrStage, &["EVIDENCE"], Some(Epistemic), "\"EVIDENCE\" \"FOR\" id",
      "EvidenceFor{claim_id}", "EVIDENCE FOR 'c1' |> LIMIT 10"),
    p("contradicts", SourceOrStage, &["CONTRADICTS"], Some(Epistemic), "\"CONTRADICTS\" id",
      "Contradicts{node_id}", "MATCH (:Claim) |> CONTRADICTS 'c1'"),
    p("supported_by", SourceOrStage, &["SUPPORTED"], Some(Epistemic),
      "\"SUPPORTED\" \"BY\" id", "SupportedBy{node_id}", "MATCH (:Claim) |> SUPPORTED BY 'c1'"),
    p("explain_belief", SourceOrStage, &["EXPLAIN"], Some(Epistemic),
      "\"EXPLAIN\" \"BELIEF\" id", "ExplainBelief{node_id}", "EXPLAIN BELIEF 'c1'"),
    p("spatial_scan", Source, &["SPATIAL"], Some(Geo),
      "\"SPATIAL\" \"SCAN\" id \"BBOX\" \"[\" signed_num \",\" signed_num \",\" signed_num \",\" \
       signed_num \"]\"",
      "SpatialScan{layer, bbox}", "SPATIAL SCAN 'roads' BBOX [0, 0, 10, 10]"),
    p("tensor_scan", Source, &["TENSOR"], Some(Tensor), "\"TENSOR\" \"SCAN\" id",
      "TensorScan{layer}", "TENSOR SCAN 'frames' |> TENSOR REDUCE MEAN AXIS 0"),
    // ── stages ──
    p("traverse", Stage, &["TRAVERSE"], None,
      "\"TRAVERSE\" ( \"-\" \"[\" rel \"]\" ( \"->\" | \"-\" ) | \"<-\" \"[\" rel \"]\" \"-\" | name ) \
       [ hops ]",
      "Traverse{rel,min,max} / Expand{rel, dir, min, max, edge_preds}",
      "MATCH (:Doc) |> TRAVERSE <-[:CITES WHERE weight >= 0.5]-{1,2}"),
    p("rel", Aux, &[], None, "( \":\" name | \"*\" ) [ \"WHERE\" pred ]",
      "relationship + edge predicates", ""),
    p("hops", Aux, &[], None, "\"{\" int [ ( \",\" | \"..\" ) int ] \"}\"",
      "min..=max hops ({n} = exactly n; absent = 1)", ""),
    p("rank", Stage, &["RANK"], None, "\"RANK\" \"BY\" \"~\" ( vector | string | param )",
      "Rank{query} / RankEmbed{text}", "MATCH (:Doc) |> RANK BY ~[0.1, -0.2, 1e-3] |> LIMIT 5"),
    p("text", Stage, &["TEXT"], Some(Text), "\"TEXT\" string", "RankText{query}",
      "MATCH () |> TEXT 'graph databases' |> LIMIT 5"),
    p("fuse", Stage, &["FUSE"], Some(Text),
      "\"FUSE\" [ \"K\" num ] ( \"[\" branch \"]\" { \"[\" branch \"]\" } | \"(\" name { \",\" name } \")\" )",
      "FuseRrf{branches, k} (a named binding is inlined as a branch)",
      "MATCH (:Doc) |> FUSE K 60 [RANK BY ~[1, 0]] [TEXT 'q']"),
    p("branch", Aux, &[], None, "stage { \"|>\" stage }", "one FUSE sub-plan", ""),
    p("rerank", Stage, &["RERANK"], None,
      "\"RERANK\" ( \"NODE_DISTANCE\" \"FROM\" id | \"MENTIONS\" | \"MMR\" num int )",
      "RankNodeDistance / RankMentions / RankMmr", "MATCH (:Doc) |> RERANK MMR 0.5 10"),
    p("window", Stage, &["WINDOW"], None, "\"WINDOW\" num [ unit ] [ agg ]",
      "Window{secs} / WindowAgg{secs, agg}", "MATCH (:Reading) |> WINDOW 500 ms SUM"),
    p("unit", Aux, &[], None,
      "\"ns\" | \"us\" | \"ms\" | \"s\" | \"m\" | \"min\" | \"h\" | \"d\" (plural forms accepted)",
      "seconds scale", ""),
    p("agg", Aux, &[], None,
      "\"MEAN\" | \"AVG\" | \"SUM\" | \"MIN\" | \"MAX\" | \"COUNT\" | \"FIRST\" | \"LAST\"",
      "the WindowAgg aggregate", ""),
    p("limit", Stage, &["LIMIT"], None, "\"LIMIT\" ( int | param )", "Limit{k}",
      "MATCH (:Doc) |> LIMIT 10"),
    p("return", Stage, &["RETURN"], None, "\"RETURN\" name { \",\" name }",
      "Project{channels}",
      "MATCH (:Doc) |> RANK BY ~[1, 0] |> RETURN similarity |> LIMIT 3"),
    p("udf", Stage, &["UDF"], Some(WasmUdf), "\"UDF\" id", "Udf{id}",
      "MATCH (:Doc) |> UDF 'score-v2'"),
    p("reproject", Stage, &["REPROJECT"], Some(Geo), "\"REPROJECT\" \"TO\" int [ \"FROM\" int ]",
      "Reproject{to_epsg, from_epsg}", "SPATIAL SCAN 'roads' BBOX [0, 0, 1, 1] |> REPROJECT TO 3857"),
    p("spatial_op", Stage, &["SPATIAL"], Some(Geo),
      "\"SPATIAL\" ( \"BUFFER\" num | \"CONVEX_HULL\" | \"SIMPLIFY\" num | \"CENTROID\" \
       | ( \"UNION\" | \"INTERSECTION\" | \"DIFFERENCE\" ) string )",
      "SpatialOp{kind}", "SPATIAL SCAN 'roads' BBOX [0, 0, 1, 1] |> SPATIAL BUFFER 2.5"),
    p("tensor_op", Stage, &["TENSOR"], Some(Tensor),
      "\"TENSOR\" ( \"SLICE\" \"[\" int \":\" int { \",\" int \":\" int } \"]\" \
       | \"REDUCE\" ( \"SUM\" | \"MEAN\" | \"MAX\" | \"MIN\" ) \"AXIS\" int \
       | ( \"ADD\" | \"SUB\" | \"MUL\" | \"DIV\" ) num )",
      "TensorOp{kind}", "TENSOR SCAN 'frames' |> TENSOR SLICE [0:2, 1:4]"),
    p("cep", Stage, &["CEP"], Some(Stream),
      "\"CEP\" cep_node \"WINDOW\" ( \"SLIDING\" | \"TUMBLING\" ) int", "Cep{pattern}",
      "MATCH (:Event) |> CEP SEQ ({KEY 'trade' WHERE qty > 100}, {KEY 'cancel'}) WINDOW SLIDING 60"),
    p("cep_node", Aux, &[], None,
      "\"SEQ\" \"(\" [ matcher { \",\" matcher } ] \")\" | \"WITHIN\" int \"(\" cep_node \")\" \
       | \"ABSENCE\" matcher \"THEN\" \"NOT\" matcher \"WITHIN\" int",
      "CepNodeSpec", ""),
    p("matcher", Aux, &[], None,
      "\"{\" [ \"KEY\" string ] [ \"WHERE\" cep_pred { \"AND\" cep_pred } ] \"}\"", "CepMatcherSpec", ""),
    p("cep_pred", Aux, &[], None,
      "name ( \"=\" json | \">\" signed_num | \"<\" signed_num | \"EXISTS\" )", "CepAttrPredSpec", ""),
    p(
        "validate_shape",
        Stage,
        &["VALIDATE"],
        Some(Owl),
        "\"VALIDATE\" \"SHAPE\" ( iri | string | name ) [ \"USING\" string ] \
         [ \"KEEP\" ( \"CONFORMING\" | \"VIOLATING\" ) ]",
        "ValidateShape{shape, shapes, keep} (no USING ⇒ the graph's GraphSchema shapes)",
        "MATCH (:Person) |> VALIDATE SHAPE <http://ex/PersonShape> KEEP VIOLATING",
    ),
    p("prob", Stage, &["PROB"], Some(Probabilistic),
      "\"PROB\" ( \"EXPECTATION\" | \"MARGINAL\" [ \"AT\" num ] [ \"LABEL\" string ] \
       | \"CONDITIONAL\" ( \"BERNOULLI\" num num | \"GAUSSIAN\" num_list \"VARIANCE\" num ) \
       | \"SAMPLE\" \"SEED\" int )",
      "Probabilistic{query}", "MATCH (:Risk) |> PROB MARGINAL AT 0.5"),
    p("belief_as_of", Stage, &["BELIEF"], Some(Epistemic), "\"BELIEF\" \"AS\" \"OF\" ts",
      "BeliefAsOf{ts}", "MATCH (:Claim) |> BELIEF AS OF @1700000000"),
    p("source_reliability", Stage, &["SOURCE"], Some(Epistemic),
      "\"SOURCE\" \"RELIABILITY\" id", "SourceReliability{source_id}",
      "MATCH (:Claim) |> SOURCE RELIABILITY 's1'"),
    p("confidence", Stage, &["CONFIDENCE"], Some(Epistemic), "\"CONFIDENCE\"",
      "ConfidenceOp{}", "MATCH (:Claim) |> CONFIDENCE |> LIMIT 5"),
    // ── predicates ──
    p("pred", Aux, &[], None, "conj { \"OR\" conj }", "Or / the Filter conjunct list", ""),
    p("conj", Aux, &[], None, "neg { \"AND\" neg }", "And", ""),
    p("neg", Aux, &[], None, "\"NOT\" neg | \"(\" pred \")\" | atom", "Not / grouping", ""),
    p("atom", Aux, &[], None,
      "name cmp scalar | name [ \"NOT\" ] \"IN\" ( \"(\" scalar { \",\" scalar } \")\" | param ) \
       | name [ \"NOT\" ] \"BETWEEN\" scalar \"AND\" scalar | name \"IS\" [ \"NOT\" ] \"NULL\" \
       | path ( \"EXISTS\" | ( \"=\" | \"==\" ) json | \"@>\" json ) | spatial_pred",
      "Eq/GtNum/LtNum/Cmp/In/Between/IsNull/JsonPath", ""),
    p("cmp", Aux, &[], None, "\"=\" | \"==\" | \"!=\" | \"<>\" | \">\" | \">=\" | \"<\" | \"<=\"",
      "CmpOp", ""),
    p("spatial_pred", Aux, &["SPATIAL"], Some(Geo),
      "\"SPATIAL\" ( \"WITHIN\" | \"CONTAINS\" | \"COVERS\" | \"TOUCHES\" | \"CROSSES\" | \"OVERLAPS\" \
       | \"EQUALS\" | \"DISJOINT\" ) \"(\" name \",\" string \")\" \
       | \"SPATIAL\" \"DWITHIN\" \"(\" name \",\" string \",\" num \")\"",
      "Pred::Spatial*", ""),
    p("path", Aux, &[], None, "jsonpath | \"JSONPATH\" string", "a JSONPath (`$.a.b[0]`)", ""),
    p("json", Aux, &[], None, "scalar | \"NULL\" | \"JSON\" string", "serde_json::Value", ""),
    // ── literals ──
    p("scalar", Aux, &[], None, "string | signed_num | \"TRUE\" | \"FALSE\" | name | param",
      "PredLiteral (a bare name is a string)", ""),
    p("ts", Aux, &[], None, "\"@\" signed_num | param", "unix seconds", ""),
    p("vector", Aux, &[], None, "\"[\" [ signed_num { \",\" signed_num } ] \"]\"", "Vec<f32>", ""),
    p("string_list", Aux, &[], None, "\"[\" [ string { \",\" string } ] \"]\"", "Vec<String>", ""),
    p("num_list", Aux, &[], None, "\"[\" [ signed_num { \",\" signed_num } ] \"]\"", "Vec<f64>", ""),
    p("signed_num", Aux, &[], None, "[ \"-\" ] number | param", "f64 (exact source text)", ""),
    p("id", Aux, &[], None, "name | string | param", "an id / layer / source name", ""),
    p("name", Aux, &[], None, "ident | quoted_ident", "an identifier (`` `any text` `` quoted)", ""),
];

/// The reference EBNF, one production per line (what `docs/uql.md` embeds).
pub fn ebnf() -> String {
    let width = PRODUCTIONS.iter().map(|p| p.name.len()).max().unwrap_or(0);
    let mut out = String::new();
    for prod in PRODUCTIONS {
        let feature = prod.feature.map_or(String::new(), |f| {
            format!("   (* feature `{}` *)", f.name())
        });
        out.push_str(&format!(
            "{:width$} = {} ;{feature}\n",
            prod.name,
            prod.rule.split_whitespace().collect::<Vec<_>>().join(" ")
        ));
    }
    out
}

/// Every leading keyword of a source or stage production, in grammar order, once.
pub fn stage_keywords() -> Vec<&'static str> {
    let mut kws: Vec<&'static str> = Vec::new();
    for kw in PRODUCTIONS
        .iter()
        .filter(|p| p.role != Role::Aux)
        .flat_map(|p| p.lead.iter().copied())
    {
        if !kws.contains(&kw) {
            kws.push(kw);
        }
    }
    kws
}

/// The role of the production(s) a leading keyword selects (`Source` if any is one).
pub fn role_of(keyword: &str) -> Option<Role> {
    let roles: Vec<Role> = PRODUCTIONS
        .iter()
        .filter(|p| p.lead.iter().any(|k| k.eq_ignore_ascii_case(keyword)))
        .map(|p| p.role)
        .collect();
    [Role::Source, Role::SourceOrStage, Role::Stage]
        .into_iter()
        .find(|r| roles.contains(r))
}

/// Every double-quoted terminal word in the grammar (the keyword set). Window units and
/// aggregate names are CONTEXTUAL words (recognized only right after a `WINDOW` number,
/// where no name can stand) and are excluded.
pub fn keywords() -> Vec<String> {
    let mut words: Vec<String> = PRODUCTIONS
        .iter()
        .filter(|p| !matches!(p.name, "unit" | "agg"))
        .flat_map(|p| p.rule.split('"').skip(1).step_by(2))
        .filter(|w| !w.is_empty() && w.chars().all(|c| c.is_ascii_alphabetic() || c == '_'))
        .map(str::to_ascii_uppercase)
        .collect();
    words.sort();
    words.dedup();
    words
}

/// The NL planner's system prompt, generated from the grammar so it can only advertise
/// what the parser accepts.
pub fn nl_system_prompt() -> String {
    let mut out = String::from(
        "You translate a natural-language question into ONE Unified Query Language (UQL) \
         query for the epistemic-graph engine. A query is a SOURCE followed by `|>`-separated \
         STAGES. Keywords are case-insensitive; strings are single-quoted; names with spaces \
         are `back-quoted`.\n\nSources and stages (one example each):\n",
    );
    for prod in PRODUCTIONS.iter().filter(|p| !p.example.is_empty()) {
        if prod.feature.is_some_and(|f| !f.enabled()) {
            continue;
        }
        out.push_str(&format!("  {}\n", prod.example));
    }
    out.push_str(
        "\nPredicates (after WHERE): comparisons = != > >= < <=, IN (...), BETWEEN a AND b, \
         IS [NOT] NULL, combined with AND / OR / NOT and parentheses.\n\
         Use ONLY labels/fields named in the provided schema hint. Reply with the UQL query \
         ONLY — no explanation, no markdown code fences.",
    );
    out
}

// Compile-time proof that each eg-plan feature turns on the eg-types wire variants its
// clauses lower to (the review questioned `owl` ⇒ `owl-plan`; the manifest is not
// trusted — each line names a variant that only exists when the implication holds).
#[cfg(feature = "owl")]
const _: fn() -> eg_types::wire::Op = || eg_types::wire::Op::Reason {
    target_class: String::new(),
    ontology: String::new(),
};
#[cfg(feature = "text")]
const _: fn() -> eg_types::wire::Op = || eg_types::wire::Op::RankText {
    query: String::new(),
};
#[cfg(feature = "wasm-udf")]
const _: fn() -> eg_types::wire::Op = || eg_types::wire::Op::Udf { id: String::new() };
#[cfg(feature = "tensor")]
const _: fn() -> eg_types::wire::Op = || eg_types::wire::Op::TensorScan {
    layer: String::new(),
};
#[cfg(feature = "stream")]
const _: fn() -> eg_types::wire::OpKind = || eg_types::wire::OpKind::Cep;
#[cfg(feature = "federation")]
const _: fn() -> eg_types::wire::OpKind = || eg_types::wire::OpKind::ForeignScan;
#[cfg(feature = "timeseries")]
const _: fn() -> eg_types::wire::OpKind = || eg_types::wire::OpKind::TsScan;
#[cfg(feature = "probabilistic")]
const _: fn() -> eg_types::wire::OpKind = || eg_types::wire::OpKind::Probabilistic;
#[cfg(feature = "epistemic")]
const _: fn() -> eg_types::wire::Op = || eg_types::wire::Op::ConfidenceOp {};
#[cfg(feature = "geo")]
const _: fn() -> eg_types::wire::OpKind = || eg_types::wire::OpKind::SpatialScan;
