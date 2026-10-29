"""Served catalog competency queries (EG-TYPED-PACKS-R106.7).

The fixture is the R106.6 catalog shape: one connector with two tools, one
skill, one prompt, two resources and one field for each resource.  The test
writes those RDF triples through the real served ``AddTriples`` surface, then
asks the real engine through both served SPARQL and served UQL.  The UQL
assertions compare the node ids that a graph traversal returns; SPARQL carries
the role, hop and glossary value columns that UQL's current result contract
does not project.
"""

from __future__ import annotations

import os
import uuid

import pytest
from conftest import request_context

from medulla.client import SyncEpistemicGraphClient

CATALOG = "http://knuckles.team/kg/catalog#"
CONNECTOR_PACK = "http://knuckles.team/kg/connector-pack#"
RDFS = "http://www.w3.org/2000/01/rdf-schema#"
RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
SKOS = "http://www.w3.org/2004/02/skos/core#"
EXAMPLE = "http://example.test/r1066#"

CONNECTOR = f"<{EXAMPLE}connector>"
SERVER = f"<{EXAMPLE}server>"
TOOL = f"<{EXAMPLE}tool-read>"
RESOURCE = f"<{EXAMPLE}resource-customers>"
FIELD = f"<{EXAMPLE}field-customer-id>"
OWNER = f"<{EXAMPLE}principal-data-team>"
OWNER_ROLE = f"<{CATALOG}owner>"
ASSETS = {
    f"<{EXAMPLE}tool-read>",
    f"<{EXAMPLE}tool-write>",
    f"<{EXAMPLE}skill-search>",
    f"<{EXAMPLE}prompt-search>",
    f"<{EXAMPLE}resource-customers>",
    f"<{EXAMPLE}resource-orders>",
    f"<{EXAMPLE}field-customer-id>",
    f"<{EXAMPLE}field-order-id>",
}

R106_6_FIXTURE = f"""
@prefix cat: <{CATALOG}> .
@prefix cp: <{CONNECTOR_PACK}> .
@prefix rdf: <{RDF}> .
@prefix rdfs: <{RDFS}> .
@prefix skos: <{SKOS}> .

{CONNECTOR} rdf:type cp:Connector .
{SERVER} rdf:type cp:Server .

cat:ToolAsset rdfs:subClassOf cat:Asset .
cat:SkillAsset rdfs:subClassOf cat:Asset .
cat:PromptAsset rdfs:subClassOf cat:Asset .
cat:DataResource rdfs:subClassOf cat:Asset .
cat:Field rdfs:subClassOf cat:Asset .

<{EXAMPLE}tool-read> rdf:type cat:ToolAsset ; cp:ofConnector {CONNECTOR} .
<{EXAMPLE}tool-write> rdf:type cat:ToolAsset ; cp:ofConnector {CONNECTOR} .
<{EXAMPLE}skill-search> rdf:type cat:SkillAsset ; cp:ofConnector {CONNECTOR} .
<{EXAMPLE}prompt-search> rdf:type cat:PromptAsset ; cp:ofConnector {CONNECTOR} .
<{EXAMPLE}resource-customers> rdf:type cat:DataResource ; cp:ofConnector {CONNECTOR} .
<{EXAMPLE}resource-orders> rdf:type cat:DataResource ; cp:ofConnector {CONNECTOR} .
<{EXAMPLE}field-customer-id> rdf:type cat:Field ; cp:ofConnector {CONNECTOR} .
<{EXAMPLE}field-order-id> rdf:type cat:Field ; cp:ofConnector {CONNECTOR} .

<{EXAMPLE}resource-customers> cp:servesClass <{EXAMPLE}Customer> .
<{EXAMPLE}resource-orders> cp:servesClass <{EXAMPLE}Order> .
{FIELD} cp:servesProperty <{EXAMPLE}customerId> .
<{EXAMPLE}field-order-id> cp:servesProperty <{EXAMPLE}orderId> .
<{EXAMPLE}resource-customers> cat:hasField {FIELD} .
<{EXAMPLE}resource-orders> cat:hasField <{EXAMPLE}field-order-id> .

<{EXAMPLE}assignment-read> rdf:type cat:StewardshipAssignment ;
    cat:assignedTo {TOOL} ;
    cat:principal {OWNER} ;
    cat:role {OWNER_ROLE} .
cat:owner rdf:type cat:StewardshipRole .

<{EXAMPLE}term-customer-reader> rdf:type cat:GlossaryTerm ;
    cat:definesAsset {TOOL} ;
    cat:definesClass <{EXAMPLE}Customer> ;
    skos:prefLabel "Read customer records" .

{CONNECTOR} cat:lineageTo {SERVER} .
{SERVER} cat:lineageTo {TOOL} .
{TOOL} cat:lineageTo {RESOURCE}, {FIELD} .
{RESOURCE} cat:lineageTo {FIELD} .

<{EXAMPLE}edge-connector-server> rdf:type cat:LineageEdge ;
    cat:from {CONNECTOR} ; cat:to {SERVER} ; cat:hop "connector" .
<{EXAMPLE}edge-server-tool> rdf:type cat:LineageEdge ;
    cat:from {SERVER} ; cat:to {TOOL} ; cat:hop "operation" .
<{EXAMPLE}edge-tool-resource> rdf:type cat:LineageEdge ;
    cat:from {TOOL} ; cat:to {RESOURCE} ; cat:hop "resource" .
<{EXAMPLE}edge-tool-field> rdf:type cat:LineageEdge ;
    cat:from {TOOL} ; cat:to {FIELD} ; cat:hop "field" .
<{EXAMPLE}edge-resource-field> rdf:type cat:LineageEdge ;
    cat:from {RESOURCE} ; cat:to {FIELD} ; cat:hop "field" .
"""

LINEAGE_TO = f"{CATALOG}lineageTo"
ASSIGNED_TO = f"{CATALOG}assignedTo"
DEFINES_ASSET = f"{CATALOG}definesAsset"
OF_CONNECTOR = f"{CONNECTOR_PACK}ofConnector"


def _sparql_rows(client, query: str, *columns: str) -> set[tuple[str | None, ...]]:
    return {
        tuple(row[column] for column in columns) for row in client.rdf.sparql(query)
    }


def _uql_ids(client, query: str) -> set[str]:
    result = client.query.uql(query)
    assert result["kind"] == "rows", result
    return {row["id"] for row in result["rows"]}


def _uql_relationship(iri: str) -> str:
    # UQL's quoted identifier is the current way to address an RDF predicate
    # whose relationship name contains ':' and '#'.
    quote = chr(96)
    return f"{quote}{iri}{quote}"


@pytest.fixture
def catalog_graph():
    """Give this served integration test a graph isolated from xdist peers."""

    graph = f"eg-r1067-{uuid.uuid4().hex}"
    client = SyncEpistemicGraphClient.connect(
        socket_path=os.environ["GRAPH_SERVICE_SOCKET"],
        graph_name=graph,
        verified_context=request_context(),
    )
    created = False
    try:
        client.tenants.create(graph, graph_type="Global")
        created = True
        yield client, graph
    finally:
        try:
            if created:
                client.tenants.delete(graph)
        finally:
            client.close()


def test_owner_lineage_term_and_class_queries_answer_from_the_graph(catalog_graph):
    """R106.7 vectors return the same catalog entities through SPARQL and UQL."""

    client, _graph = catalog_graph
    # RDF writes require an explicit integrity policy.  This target class is
    # absent from the fixture, so the policy permits only this test's fixture
    # without weakening the production write-time guard.
    client.rdf.icv_configure(
        "@prefix sh: <http://www.w3.org/ns/shacl#> .\n"
        "@prefix ex: <http://example.test/r1066#> .\n"
        "ex:UnusedShape a sh:NodeShape ; sh:targetClass ex:UnusedClass .\n"
    )
    report = client.rdf.add_triples(turtle=R106_6_FIXTURE)
    assert report["triples"] > 0

    prefix = (
        f"PREFIX cat: <{CATALOG}> PREFIX cp: <{CONNECTOR_PACK}> "
        f"PREFIX rdfs: <{RDFS}> PREFIX skos: <{SKOS}> "
    )

    owner_sparql = prefix + (
        f"SELECT ?principal ?role WHERE {{ ?s cat:assignedTo {TOOL} ; "
        "cat:principal ?principal ; cat:role ?role }"
    )
    assert _sparql_rows(client, owner_sparql, "principal", "role") == {
        (OWNER, OWNER_ROLE)
    }
    assert _uql_ids(
        client,
        "MATCH () WHERE type = "
        f"'{CATALOG}StewardshipAssignment' |> TRAVERSE -[:"
        f"{_uql_relationship(ASSIGNED_TO)}]->",
    ) == {TOOL}

    upstream_sparql = prefix + (f"SELECT ?up WHERE {{ ?up cat:lineageTo+ {TOOL} }}")
    assert _sparql_rows(client, upstream_sparql, "up") == {(CONNECTOR,), (SERVER,)}
    assert _uql_ids(
        client,
        f"MATCH () WHERE id = '{TOOL}' |> TRAVERSE <-[:"
        f"{_uql_relationship(LINEAGE_TO)}]-{{1,2}}",
    ) == {CONNECTOR, SERVER}

    downstream_sparql = prefix + (
        f"SELECT ?down ?hop WHERE {{ {TOOL} cat:lineageTo+ ?down . "
        f"?e cat:from {TOOL} ; cat:to ?down ; cat:hop ?hop }}"
    )
    assert _sparql_rows(client, downstream_sparql, "down", "hop") == {
        (RESOURCE, "resource"),
        (FIELD, "field"),
    }
    assert _uql_ids(
        client,
        f"MATCH () WHERE id = '{TOOL}' |> TRAVERSE -[:"
        f"{_uql_relationship(LINEAGE_TO)}]->{{1,2}}",
    ) == {RESOURCE, FIELD}

    glossary_sparql = prefix + (
        f"SELECT ?term ?label WHERE {{ ?term cat:definesAsset {TOOL} ; "
        "skos:prefLabel ?label }"
    )
    assert _sparql_rows(client, glossary_sparql, "term", "label") == {
        (f"<{EXAMPLE}term-customer-reader>", "Read customer records")
    }
    assert _uql_ids(
        client,
        "MATCH () WHERE type = "
        f"'{CATALOG}GlossaryTerm' |> TRAVERSE -[:"
        f"{_uql_relationship(DEFINES_ASSET)}]->",
    ) == {TOOL}

    parent_sparql = prefix + (
        f"SELECT ?asset WHERE {{ ?asset a/rdfs:subClassOf* cat:Asset ; "
        f"cp:ofConnector {CONNECTOR} }}"
    )
    assert {row[0] for row in _sparql_rows(client, parent_sparql, "asset")} == ASSETS
    assert (
        _uql_ids(
            client,
            f"MATCH () WHERE id = '{CONNECTOR}' |> TRAVERSE <-[:"
            f"{_uql_relationship(OF_CONNECTOR)}]-",
        )
        == ASSETS
    )
