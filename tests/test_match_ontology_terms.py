"""End-to-end round-trip of the ontology lexical gate (CONCEPT:EG-ORCH.routing.lexical-capability-escalation).

Exercises the full path Python client → MessagePack/UDS → Rust
``Method::MatchOntologyTerms`` handler → ``GraphCore::match_ontology_terms`` →
``ResultPayload::raw`` → decoded dicts, against a real engine.
"""

from __future__ import annotations


def test_match_ontology_terms_round_trip(clean_graph):
    g = clean_graph
    # Fleet capability nodes (the KG-2.133 schema): name + product synonyms.
    g.nodes.add(
        "tool_portainer-mcp_stack",
        {
            "type": "Tool",
            "name": "portainer_stack",
            "synonyms": ["portainer", "portainer-mcp"],
            "mcp_server": "portainer-mcp",
        },
    )
    g.nodes.add(
        "tool_github-mcp_issues",
        {
            "type": "Tool",
            "name": "github_issues",
            "synonyms": ["github", "github-mcp"],
            "mcp_server": "github-mcp",
        },
    )
    # A non-capability node must never seed a term.
    g.nodes.add("code1", {"type": "Code", "name": "deploy"})

    # The two validation cases — a product name, not a tool name.
    portainer = g.graph.match_ontology_terms("can you list my stacks on portainer?")
    assert any(h["term"] == "portainer" and h["node_type"] == "Tool" for h in portainer)

    github = g.graph.match_ontology_terms("use the github mcp to fetch open issues")
    assert any(h["term"] == "github" for h in github)

    # Exact tool name when spelled out.
    exact = g.graph.match_ontology_terms("call github_issues now")
    assert any(h["term"] == "github_issues" for h in exact)

    # Trivial chat names no capability → no escalation signal.
    assert g.graph.match_ontology_terms("hello, how are you today?") == []
    # A non-capability node's name is never a term; whole-word only.
    assert g.graph.match_ontology_terms("please deploy it") == []
    assert g.graph.match_ontology_terms("teleportainerish nonsense") == []
