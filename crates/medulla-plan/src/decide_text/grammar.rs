//! THE DecideText grammar (EH-452): one [`Production`] row per rule, the same shape as
//! UQL's table ([`crate::uql::grammar`]). The reference EBNF (`docs/uql.md`'s DecideText
//! block, [`ebnf`]) and the parser's "expected" clause list ([`clause_keywords`]) come
//! from it; the parser's clause table is checked against it.

use crate::uql::grammar::{p, Production, Role::*};

/// The whole DecideText grammar, in reference order.
pub const PRODUCTIONS: &[Production] = &[
    p(
        "decide_text",
        Aux,
        &[],
        None,
        "candidates { \"|>\" clause }",
        "DecideTextRequest (Decide | Assemble)",
        "",
    ),
    p(
        "candidates",
        Source,
        &["CANDIDATES"],
        None,
        "\"CANDIDATES\" ( \"AGENT\" \"LIBRARY\" \"KINDS\" \"[\" name { \",\" name } \"]\" \
       [ \"UNDER\" ( iri | string ) ] | \"GRAPH\" string \"QUERY\" \"{\" uql \"}\" )",
        "CandidateSource::AgentLibrary{scope} / Graph{graph, plan}",
        "CANDIDATES AGENT LIBRARY KINDS [tool, skill] UNDER <eg:capability> |> ASSEMBLE",
    ),
    p(
        "covers",
        Stage,
        &["COVERS"],
        None,
        "\"COVERS\" param",
        "the capabilities an assembly must cover (an iri_list parameter)",
        "CANDIDATES AGENT LIBRARY KINDS [tool] |> COVERS $capabilities |> ASSEMBLE",
    ),
    p(
        "validate_policy",
        Stage,
        &["VALIDATE"],
        None,
        "\"VALIDATE\" \"POLICY\" ( \"DEFAULT\" | pin )",
        "DecisionPolicyRef::Default / Pinned",
        "CANDIDATES AGENT LIBRARY KINDS [tool] |> VALIDATE POLICY DEFAULT |> ASSEMBLE",
    ),
    p(
        "decide",
        Stage,
        &["DECIDE"],
        None,
        "\"DECIDE\" name \"QUESTION\" string [ \"SAFETY\" name ] \"FEATURES\" pin \
       [ \"HEAD\" pin ] [ \"MAX\" int ]",
        "DecideRequest (question kind, id, safety, feature schema, head, max records)",
        "CANDIDATES GRAPH 'kg' QUERY { MATCH (:Tool) WHERE note = '}' } \
       |> DECIDE route QUESTION 'route.tools' FEATURES 'schema-a' AT 'sha256:s' MAX 4",
    ),
    p(
        "assemble",
        Stage,
        &["ASSEMBLE"],
        None,
        "\"ASSEMBLE\" [ \"MAX\" \"COMPONENTS\" int ]",
        "AssemblyRequest",
        "CANDIDATES AGENT LIBRARY KINDS [tool] |> ASSEMBLE MAX COMPONENTS 3",
    ),
    p(
        "pin",
        Aux,
        &[],
        None,
        "string \"AT\" string",
        "ComponentDependency (component id, definition digest)",
        "",
    ),
    p(
        "param",
        Aux,
        &[],
        None,
        "\"$\" name",
        "a typed parameter, bound by name",
        "",
    ),
];

/// The reference EBNF (what `docs/uql.md`'s DecideText block embeds).
pub fn ebnf() -> String {
    crate::uql::grammar::ebnf_of(PRODUCTIONS)
}

/// Every clause keyword (the leads of the stage rows), in grammar order.
pub fn clause_keywords() -> Vec<&'static str> {
    PRODUCTIONS
        .iter()
        .filter(|p| p.role == Stage)
        .flat_map(|p| p.lead.iter().copied())
        .collect()
}
