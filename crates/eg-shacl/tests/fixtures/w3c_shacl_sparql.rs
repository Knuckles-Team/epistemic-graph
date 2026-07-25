// AUTO-EXTRACTED verbatim from the official W3C SHACL test suite
// (w3c/data-shapes, gh-pages branch, data-shapes-test-suite/tests/...).
// Each constant is BOTH the shapes graph and the data graph for its test
// (mf:action sht:dataGraph <> ; sht:shapesGraph <> -- the same document).

const SPARQL_NODE_001: &str = r##"@prefix dash: <http://datashapes.org/dash#> .
@prefix ex: <http://datashapes.org/sh/tests/sparql/node/sparql-001.test#> .
@prefix mf: <http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix sht: <http://www.w3.org/ns/shacl-test#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

ex:InvalidResource1
  rdf:type rdfs:Resource ;
  rdfs:label "Invalid resource 1" ;
.
ex:InvalidResource2
  rdf:type rdfs:Resource ;
  rdfs:label "Invalid label 1" ;
  rdfs:label "Invalid label 2" ;
.
ex:TestShape
  rdf:type sh:NodeShape ;
  rdfs:label "Test shape" ;
  sh:sparql ex:TestShape-sparql ;
  sh:targetNode ex:InvalidResource1 ;
  sh:targetNode ex:InvalidResource2 ;
  sh:targetNode ex:ValidResource1 ;
.
ex:TestShape-sparql
  sh:message "Cannot have a label" ;
  sh:prefixes <http://datashapes.org/sh/tests/sparql/node/sparql-001.test> ;
  sh:select """
  	SELECT $this ?path ?value
	WHERE {
		$this ?path ?value .
		FILTER (?path = <http://www.w3.org/2000/01/rdf-schema#label>) .
	}""" ;
.
ex:ValidResource1
  rdf:type rdfs:Resource ;
.
<>
  rdf:type mf:Manifest ;
  mf:entries (
      <sparql-001>
    ) ;
.
<sparql-001>
  rdf:type sht:Validate ;
  rdfs:label "Test of sh:sparql at node shape 001" ;
  mf:action [
      sht:dataGraph <> ;
      sht:shapesGraph <> ;
    ] ;
  mf:result [
      rdf:type sh:ValidationReport ;
      sh:conforms "false"^^xsd:boolean ;
      sh:result [
          rdf:type sh:ValidationResult ;
          sh:focusNode ex:InvalidResource1 ;
          sh:resultPath rdfs:label ;
          sh:resultSeverity sh:Violation ;
          sh:sourceConstraint ex:TestShape-sparql ;
          sh:sourceConstraintComponent sh:SPARQLConstraintComponent ;
          sh:sourceShape ex:TestShape ;
          sh:value "Invalid resource 1" ;
        ] ;
      sh:result [
          rdf:type sh:ValidationResult ;
          sh:focusNode ex:InvalidResource2 ;
          sh:resultPath rdfs:label ;
          sh:resultSeverity sh:Violation ;
          sh:sourceConstraint ex:TestShape-sparql ;
          sh:sourceConstraintComponent sh:SPARQLConstraintComponent ;
          sh:sourceShape ex:TestShape ;
          sh:value "Invalid label 1" ;
        ] ;
      sh:result [
          rdf:type sh:ValidationResult ;
          sh:focusNode ex:InvalidResource2 ;
          sh:resultPath rdfs:label ;
          sh:resultSeverity sh:Violation ;
          sh:sourceConstraint ex:TestShape-sparql ;
          sh:sourceConstraintComponent sh:SPARQLConstraintComponent ;
          sh:sourceShape ex:TestShape ;
          sh:value "Invalid label 2" ;
        ] ;
    ] ;
  mf:status sht:approved ;
.
"##;

const SPARQL_NODE_002: &str = r##"@prefix dash: <http://datashapes.org/dash#> .
@prefix ex: <http://datashapes.org/sh/tests/sparql/node/sparql-002.test#> .
@prefix mf: <http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix sht: <http://www.w3.org/ns/shacl-test#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<http://datashapes.org/sh/tests/sparql/node/sparql-002.test>
  sh:declare [
      rdf:type sh:PrefixDeclaration ;
      sh:namespace "http://datashapes.org/sh/tests/sparql/node/sparql-002.test#"^^xsd:anyURI ;
      sh:prefix "ex" ;
    ] ;
.
ex:InvalidCountry
  rdf:type ex:Country ;
  ex:germanLabel "Spain"@en ;
.
ex:LanguageExampleShape
  rdf:type sh:NodeShape ;
  rdf:type sh:SPARQLConstraint ;
  sh:message "Values are literals with German language tag." ;
  sh:prefixes <http://datashapes.org/sh/tests/sparql/node/sparql-002.test> ;
  sh:select """
			SELECT $this (ex:germanLabel AS ?path) ?value
			WHERE {
				$this ex:germanLabel ?value .
				FILTER (!isLiteral(?value) || !langMatches(lang(?value), \"de\"))
			}
			""" ;
  sh:sparql ex:LanguageExampleShape ;
  sh:targetClass ex:Country ;
.
ex:ValidCountry
  rdf:type ex:Country ;
  ex:germanLabel "Spanien"@de ;
.
<>
  rdf:type mf:Manifest ;
  mf:entries (
      <sparql-002>
    ) ;
.
<sparql-002>
  rdf:type sht:Validate ;
  rdfs:label "Test of sh:sparql at node shape 002" ;
  mf:action [
      sht:dataGraph <> ;
      sht:shapesGraph <> ;
    ] ;
  mf:result [
      rdf:type sh:ValidationReport ;
      sh:conforms "false"^^xsd:boolean ;
      sh:result [
          rdf:type sh:ValidationResult ;
          sh:focusNode ex:InvalidCountry ;
          sh:resultPath ex:germanLabel ;
          sh:resultSeverity sh:Violation ;
          sh:sourceConstraint ex:LanguageExampleShape ;
          sh:sourceConstraintComponent sh:SPARQLConstraintComponent ;
          sh:sourceShape ex:LanguageExampleShape ;
          sh:value "Spain"@en ;
        ] ;
    ] ;
  mf:status sht:approved ;
.
"##;

const SPARQL_NODE_003: &str = r##"@prefix dash: <http://datashapes.org/dash#> .
@prefix ex: <http://datashapes.org/sh/tests/sparql/node/sparql-003.test#> .
@prefix mf: <http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix sht: <http://www.w3.org/ns/shacl-test#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<http://datashapes.org/sh/tests/sparql/node/sparql-003.test>
  sh:declare [
      rdf:type sh:PrefixDeclaration ;
      sh:namespace "http://datashapes.org/sh/tests/sparql/node/sparql-003.test#"^^xsd:anyURI ;
      sh:prefix "ex" ;
    ] ;
.
ex:InvalidCountry
  rdf:type ex:Country ;
  ex:germanLabel "Spain"@en ;
.
ex:LanguageExampleShape
  rdf:type sh:NodeShape ;
  rdf:type sh:SPARQLConstraint ;
  sh:message "Values are literals with German language tag." ;
  sh:prefixes <http://datashapes.org/sh/tests/sparql/node/sparql-003.test> ;
  sh:select """
			SELECT $this (ex:germanLabel AS ?path)
			WHERE {
				$this ex:germanLabel ?value .
				FILTER (!isLiteral(?value) || !langMatches(lang(?value), \"de\"))
			}
			""" ;
  sh:severity sh:Warning ;
  sh:sparql ex:LanguageExampleShape ;
  sh:targetClass ex:Country ;
.
ex:ValidCountry
  rdf:type ex:Country ;
  ex:germanLabel "Spanien"@de ;
.
<>
  rdf:type mf:Manifest ;
  mf:entries (
      <sparql-003>
    ) ;
.
<sparql-003>
  rdf:type sht:Validate ;
  rdfs:label "Test of sh:sparql at node shape 003" ;
  mf:action [
      sht:dataGraph <> ;
      sht:shapesGraph <> ;
    ] ;
  mf:result [
      rdf:type sh:ValidationReport ;
      sh:conforms "false"^^xsd:boolean ;
      sh:result [
          rdf:type sh:ValidationResult ;
          sh:focusNode ex:InvalidCountry ;
          sh:resultPath ex:germanLabel ;
          sh:resultSeverity sh:Warning ;
          sh:sourceConstraint ex:LanguageExampleShape ;
          sh:sourceConstraintComponent sh:SPARQLConstraintComponent ;
          sh:sourceShape ex:LanguageExampleShape ;
          sh:value ex:InvalidCountry ;
        ] ;
    ] ;
  mf:status sht:approved ;
.
"##;

const SPARQL_NODE_PREFIXES_001: &str = r##"@prefix dash: <http://datashapes.org/dash#> .
@prefix ex: <http://datashapes.org/sh/tests/sparql/node/prefixes-001.test#> .
@prefix mf: <http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix sht: <http://www.w3.org/ns/shacl-test#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<http://datashapes.org/sh/tests/sparql/node/prefixes-001.test>
  sh:declare [
      rdf:type sh:PrefixDeclaration ;
      sh:namespace "http://datashapes.org/sh/tests/sparql/node/prefixes-001.test#"^^xsd:anyURI ;
      sh:prefix "ex" ;
    ] ;
.
ex:InvalidResource1
  ex:property <http://test.com/ns#Value> ;
.
ex:TestPrefixes
  owl:imports <http://datashapes.org/sh/tests/sparql/node/prefixes-001.test> ;
  sh:declare [
      sh:namespace "http://test.com/ns#"^^xsd:anyURI ;
      sh:prefix "test" ;
    ] ;
.
ex:TestSPARQL
  sh:prefixes ex:TestPrefixes ;
  sh:select """
		SELECT $this ?value
		WHERE {
			$this ex:property ?value .
            FILTER (?value = test:Value) .
		} """ ;
.
ex:TestShape
  rdf:type sh:NodeShape ;
  sh:sparql ex:TestSPARQL ;
  sh:targetNode ex:InvalidResource1 ;
  sh:targetNode ex:ValidResource1 ;
.
<>
  rdf:type mf:Manifest ;
  mf:entries (
      <prefixes-001>
    ) ;
.
<prefixes-001>
  rdf:type sht:Validate ;
  rdfs:label "Test of sh:prefixes 001" ;
  mf:action [
      sht:dataGraph <> ;
      sht:shapesGraph <> ;
    ] ;
  mf:result [
      rdf:type sh:ValidationReport ;
      sh:conforms "false"^^xsd:boolean ;
      sh:result [
          rdf:type sh:ValidationResult ;
          sh:focusNode ex:InvalidResource1 ;
          sh:resultSeverity sh:Violation ;
          sh:sourceConstraint ex:TestSPARQL ;
          sh:sourceConstraintComponent sh:SPARQLConstraintComponent ;
          sh:sourceShape ex:TestShape ;
          sh:value <http://test.com/ns#Value> ;
        ] ;
    ] ;
  mf:status sht:approved ;
.
"##;

const SPARQL_PROPERTY_001: &str = r##"@prefix dash: <http://datashapes.org/dash#> .
@prefix ex: <http://datashapes.org/sh/tests/sparql/property/sparql-001.test#> .
@prefix mf: <http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix sht: <http://www.w3.org/ns/shacl-test#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

ex:Country
  rdf:type rdfs:Class ;
.
ex:InvalidCountry
  rdf:type ex:Country ;
  ex:germanLabel "Spain"@en ;
.
ex:LanguageExamplePropertyShape
  rdf:type sh:PropertyShape ;
  sh:path ex:germanLabel ;
  sh:sparql ex:LanguageExamplePropertyShape-sparql ;
  sh:targetClass ex:Country ;
.
ex:LanguageExamplePropertyShape-sparql
  rdf:type sh:SPARQLConstraint ;
  sh:message "Values are literals with German language tag." ;
  sh:prefixes ex: ;
  sh:select """
		SELECT $this ?value
		WHERE {
			$this $PATH ?value .
			FILTER (!isLiteral(?value) || !langMatches(lang(?value), \"de\"))
		}
		""" ;
.
ex:ValidCountry
  rdf:type ex:Country ;
  ex:germanLabel "Spanien"@de ;
.
<>
  rdf:type mf:Manifest ;
  mf:entries (
      <sparql-001>
    ) ;
.
<sparql-001>
  rdf:type sht:Validate ;
  rdfs:label "Test of sh:sparql at property shape 001" ;
  mf:action [
      sht:dataGraph <> ;
      sht:shapesGraph <> ;
    ] ;
  mf:result [
      rdf:type sh:ValidationReport ;
      sh:conforms "false"^^xsd:boolean ;
      sh:result [
          rdf:type sh:ValidationResult ;
          sh:focusNode ex:InvalidCountry ;
          sh:resultPath ex:germanLabel ;
          sh:resultSeverity sh:Violation ;
          sh:sourceConstraint ex:LanguageExamplePropertyShape-sparql ;
          sh:sourceConstraintComponent sh:SPARQLConstraintComponent ;
          sh:sourceShape ex:LanguageExamplePropertyShape ;
          sh:value "Spain"@en ;
        ] ;
    ] ;
  mf:status sht:approved ;
.
"##;

const PRE_BINDING_001: &str = r##"@prefix dash: <http://datashapes.org/dash#> .
@prefix ex: <http://datashapes.org/sh/tests/sparql/pre-binding/pre-binding-001.test#> .
@prefix mf: <http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix sht: <http://www.w3.org/ns/shacl-test#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

ex:
	sh:declare [
		sh:prefix "ex" ;
		sh:namespace "http://datashapes.org/sh/tests/sparql/pre-binding/pre-binding-001.test#"^^xsd:anyURI ;
	] .

ex:TestShape
  rdf:type sh:NodeShape ;
  rdfs:label "Test shape" ;
  sh:sparql ex:TestShape-sparql ;
  sh:targetNode ex:InvalidResource ;
.
ex:TestShape-sparql
  sh:message "Test message" ;
  sh:prefixes ex: ;
  sh:select """
  	SELECT $this
	WHERE {
		FILTER ($this = ex:InvalidResource) .
	}""" ;
.
ex:ValidResource1
  rdf:type rdfs:Resource ;
.
<>
  rdf:type mf:Manifest ;
  mf:entries (
      <pre-binding-001>
    ) ;
.
<pre-binding-001>
  rdf:type sht:Validate ;
  rdfs:label "Test of pre-binding in FILTER" ;
  mf:action [
      sht:dataGraph <> ;
      sht:shapesGraph <> ;
    ] ;
  mf:result [
      rdf:type sh:ValidationReport ;
      sh:conforms "false"^^xsd:boolean ;
      sh:result [
          rdf:type sh:ValidationResult ;
          sh:focusNode ex:InvalidResource ;
          sh:resultMessage "Test message" ;
          sh:resultSeverity sh:Violation ;
          sh:sourceConstraint ex:TestShape-sparql ;
          sh:sourceConstraintComponent sh:SPARQLConstraintComponent ;
          sh:sourceShape ex:TestShape ;
          sh:value ex:InvalidResource ;
        ] ;
    ] ;
  mf:status sht:approved ;
.
"##;

const PRE_BINDING_002: &str = r##"@prefix dash: <http://datashapes.org/dash#> .
@prefix ex: <http://datashapes.org/sh/tests/sparql/pre-binding/pre-binding-002.test#> .
@prefix mf: <http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix sht: <http://www.w3.org/ns/shacl-test#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

ex:
	sh:declare [
		sh:prefix "ex" ;
		sh:namespace "http://datashapes.org/sh/tests/sparql/pre-binding/pre-binding-002.test#"^^xsd:anyURI ;
	] .

ex:TestShape
  rdf:type sh:NodeShape ;
  rdfs:label "Test shape" ;
  sh:sparql ex:TestShape-sparql ;
  sh:targetNode ex:InvalidResource ;
.
ex:TestShape-sparql
  sh:prefixes ex: ;
  sh:select """
  	SELECT $this
	WHERE {
		{
			FILTER (false) .
		}
		UNION
		{
			FILTER ($this = ex:InvalidResource) .
		}
	}""" ;
.
ex:ValidResource1
  rdf:type rdfs:Resource ;
.
<>
  rdf:type mf:Manifest ;
  mf:entries (
      <pre-binding-002>
    ) ;
.
<pre-binding-002>
  rdf:type sht:Validate ;
  rdfs:label "Test of pre-binding in UNION" ;
  mf:action [
      sht:dataGraph <> ;
      sht:shapesGraph <> ;
    ] ;
  mf:result [
      rdf:type sh:ValidationReport ;
      sh:conforms "false"^^xsd:boolean ;
      sh:result [
          rdf:type sh:ValidationResult ;
          sh:focusNode ex:InvalidResource ;
          sh:resultSeverity sh:Violation ;
          sh:sourceConstraint ex:TestShape-sparql ;
          sh:sourceConstraintComponent sh:SPARQLConstraintComponent ;
          sh:sourceShape ex:TestShape ;
          sh:value ex:InvalidResource ;
        ] ;
    ] ;
  mf:status sht:approved ;
.
"##;

const PRE_BINDING_003: &str = r##"@prefix dash: <http://datashapes.org/dash#> .
@prefix ex: <http://datashapes.org/sh/tests/sparql/pre-binding/pre-binding-003.test#> .
@prefix mf: <http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix sht: <http://www.w3.org/ns/shacl-test#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

ex:
	sh:declare [
		sh:prefix "ex" ;
		sh:namespace "http://datashapes.org/sh/tests/sparql/pre-binding/pre-binding-003.test#"^^xsd:anyURI ;
	] .

ex:TestShape
  rdf:type sh:NodeShape ;
  rdfs:label "Test shape" ;
  sh:sparql ex:TestShape-sparql ;
  sh:targetNode ex:InvalidResource ;
.
ex:TestShape-sparql
  sh:prefixes ex: ;
  sh:select """
  	SELECT $this
	WHERE {
		{
			{
				FILTER ($this = ex:InvalidResource) .
			}
			FILTER bound($this) .
		}
		FILTER (true) .
	}""" ;
.
ex:ValidResource1
  rdf:type rdfs:Resource ;
.
<>
  rdf:type mf:Manifest ;
  mf:entries (
      <pre-binding-003>
    ) ;
.
<pre-binding-003>
  rdf:type sht:Validate ;
  rdfs:label "Test of pre-binding in inner {...} blocks" ;
  mf:action [
      sht:dataGraph <> ;
      sht:shapesGraph <> ;
    ] ;
  mf:result [
      rdf:type sh:ValidationReport ;
      sh:conforms "false"^^xsd:boolean ;
      sh:result [
          rdf:type sh:ValidationResult ;
          sh:focusNode ex:InvalidResource ;
          sh:resultSeverity sh:Violation ;
          sh:sourceConstraint ex:TestShape-sparql ;
          sh:sourceConstraintComponent sh:SPARQLConstraintComponent ;
          sh:sourceShape ex:TestShape ;
          sh:value ex:InvalidResource ;
        ] ;
    ] ;
  mf:status sht:approved ;
.
"##;

const PRE_BINDING_004: &str = r##"@prefix dash: <http://datashapes.org/dash#> .
@prefix ex: <http://datashapes.org/sh/tests/sparql/pre-binding/pre-binding-004.test#> .
@prefix mf: <http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix sht: <http://www.w3.org/ns/shacl-test#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

ex:
	sh:declare [
		sh:prefix "ex" ;
		sh:namespace "http://datashapes.org/sh/tests/sparql/pre-binding/pre-binding-004.test#"^^xsd:anyURI ;
	] .

ex:TestShape
  rdf:type sh:NodeShape ;
  rdfs:label "Test shape" ;
  sh:sparql ex:TestShape-sparql ;
  sh:targetNode ex:InvalidResource ;
.
ex:TestShape-sparql
  sh:prefixes ex: ;
  sh:select """
  	SELECT $this
	WHERE {
		BIND ($this AS ?that) .
		FILTER (?that = ex:InvalidResource) .
	}""" ;
.
ex:ValidResource1
  rdf:type rdfs:Resource ;
.
<>
  rdf:type mf:Manifest ;
  mf:entries (
      <pre-binding-004>
    ) ;
.
<pre-binding-004>
  rdf:type sht:Validate ;
  rdfs:label "Test of pre-binding in BIND expressions" ;
  mf:action [
      sht:dataGraph <> ;
      sht:shapesGraph <> ;
    ] ;
  mf:result [
      rdf:type sh:ValidationReport ;
      sh:conforms "false"^^xsd:boolean ;
      sh:result [
          rdf:type sh:ValidationResult ;
          sh:focusNode ex:InvalidResource ;
          sh:resultSeverity sh:Violation ;
          sh:sourceConstraint ex:TestShape-sparql ;
          sh:sourceConstraintComponent sh:SPARQLConstraintComponent ;
          sh:sourceShape ex:TestShape ;
          sh:value ex:InvalidResource ;
        ] ;
    ] ;
  mf:status sht:approved ;
.
"##;

const PRE_BINDING_005: &str = r##"@prefix dash: <http://datashapes.org/dash#> .
@prefix ex: <http://datashapes.org/sh/tests/sparql/pre-binding/pre-binding-005.test#> .
@prefix mf: <http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix sht: <http://www.w3.org/ns/shacl-test#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

ex:
	sh:declare [
		sh:prefix "ex" ;
		sh:namespace "http://datashapes.org/sh/tests/sparql/pre-binding/pre-binding-005.test#"^^xsd:anyURI ;
	] .

ex:InvalidResource
	ex:property "Label" .

ex:TestShape
  rdf:type sh:NodeShape ;
  rdfs:label "Test shape" ;
  sh:sparql ex:TestShape-sparql ;
  sh:targetNode ex:InvalidResource ;
.
ex:TestShape-sparql
  sh:prefixes ex: ;
  sh:select """
  	SELECT $this
	WHERE {
		{
			FILTER (bound($this))
		}
		$this ex:property "Label" .
		FILTER (bound($this)) .
	}""" ;
.
ex:ValidResource1
  rdf:type rdfs:Resource ;
.
<>
  rdf:type mf:Manifest ;
  mf:entries (
      <pre-binding-005>
    ) ;
.
<pre-binding-005>
  rdf:type sht:Validate ;
  rdfs:label "Test of pre-binding in BGP and FILTER" ;
  mf:action [
      sht:dataGraph <> ;
      sht:shapesGraph <> ;
    ] ;
  mf:result [
      rdf:type sh:ValidationReport ;
      sh:conforms "false"^^xsd:boolean ;
      sh:result [
          rdf:type sh:ValidationResult ;
          sh:focusNode ex:InvalidResource ;
          sh:resultSeverity sh:Violation ;
          sh:sourceConstraint ex:TestShape-sparql ;
          sh:sourceConstraintComponent sh:SPARQLConstraintComponent ;
          sh:sourceShape ex:TestShape ;
          sh:value ex:InvalidResource ;
        ] ;
    ] ;
  mf:status sht:approved ;
.
"##;

const PRE_BINDING_006: &str = r##"@prefix dash: <http://datashapes.org/dash#> .
@prefix ex: <http://datashapes.org/sh/tests/sparql/pre-binding/pre-binding-006.test#> .
@prefix mf: <http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix sht: <http://www.w3.org/ns/shacl-test#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

ex:
	sh:declare [
		sh:prefix "ex" ;
		sh:namespace "http://datashapes.org/sh/tests/sparql/pre-binding/pre-binding-006.test#"^^xsd:anyURI ;
	] .

ex:TestShape
  rdf:type sh:NodeShape ;
  rdfs:label "Test shape" ;
  sh:sparql ex:TestShape-sparql ;
  sh:targetNode ex:InvalidResource ;
.
ex:TestShape-sparql
  sh:prefixes ex: ;
  sh:select """
  	SELECT $this
	WHERE {
		{
			SELECT *
			WHERE {
				FILTER ($this = ex:InvalidResource) .
			}
		}
	}""" ;
.
ex:ValidResource1
  rdf:type rdfs:Resource ;
.
<>
  rdf:type mf:Manifest ;
  mf:entries (
      <pre-binding-006>
    ) ;
.
<pre-binding-006>
  rdf:type sht:Validate ;
  rdfs:label "Test of pre-binding in nested SELECT" ;
  mf:action [
      sht:dataGraph <> ;
      sht:shapesGraph <> ;
    ] ;
  mf:result sht:Failure ;
  mf:status sht:approved ;
.
"##;

const PRE_BINDING_007: &str = r##"@prefix dash: <http://datashapes.org/dash#> .
@prefix ex: <http://datashapes.org/sh/tests/sparql/pre-binding/pre-binding-007.test#> .
@prefix mf: <http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix sht: <http://www.w3.org/ns/shacl-test#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

ex:
	sh:declare [
		sh:prefix "ex" ;
		sh:namespace "http://datashapes.org/sh/tests/sparql/pre-binding/pre-binding-007.test#"^^xsd:anyURI ;
	] .

ex:TestShape
  rdf:type sh:NodeShape ;
  rdfs:label "Test shape" ;
  sh:sparql ex:TestShape-sparql ;
  sh:targetNode ex:InvalidResource ;
.
ex:TestShape-sparql
  sh:prefixes ex: ;
  sh:select """
  	SELECT $this
	WHERE {
		{
			SELECT $this
			WHERE {
				FILTER ($this = ex:InvalidResource) .
			}
		}
	}""" ;
.
ex:ValidResource1
  rdf:type rdfs:Resource ;
.
<>
  rdf:type mf:Manifest ;
  mf:entries (
      <pre-binding-007>
    ) ;
.
<pre-binding-007>
  rdf:type sht:Validate ;
  rdfs:label "Test of pre-binding in nested SELECT" ;
  mf:action [
      sht:dataGraph <> ;
      sht:shapesGraph <> ;
    ] ;
  mf:result [
      rdf:type sh:ValidationReport ;
      sh:conforms "false"^^xsd:boolean ;
      sh:result [
          rdf:type sh:ValidationResult ;
          sh:focusNode ex:InvalidResource ;
          sh:resultSeverity sh:Violation ;
          sh:sourceConstraint ex:TestShape-sparql ;
          sh:sourceConstraintComponent sh:SPARQLConstraintComponent ;
          sh:sourceShape ex:TestShape ;
          sh:value ex:InvalidResource ;
        ] ;
    ] ;
  mf:status sht:approved ;
.
"##;

const SHAPES_GRAPH_001: &str = r##"@prefix ex: <http://datashapes.org/sh/tests/sparql/pre-binding/shapesGraph-001.test#> .
@prefix mf: <http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix sht: <http://www.w3.org/ns/shacl-test#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

ex:
	sh:declare [
		sh:prefix "ex" ;
		sh:namespace "http://datashapes.org/sh/tests/sparql/pre-binding/shapesGraph-001.test#"^^xsd:anyURI ;
	] .

ex:TestShape
  rdf:type sh:NodeShape ;
  rdfs:label "Test shape" ;
  sh:sparql ex:TestShape-sparql ;
  sh:targetNode ex:InvalidResource ;
  ex:property 42 ;
.
ex:TestShape-sparql
  sh:message "Test message" ;
  sh:prefixes ex: ;
  sh:select """
  	SELECT $this
	WHERE {
		FILTER bound($shapesGraph) .
		GRAPH $shapesGraph {
			FILTER bound($currentShape) .
			$currentShape ex:property 42 .
		}
	}""" ;
.
<>
  rdf:type mf:Manifest ;
  mf:entries (
      <shapesGraph-001>
    ) ;
.
<shapesGraph-001>
  rdf:type sht:Validate ;
  rdfs:label "Test of $shapesGraph and $currentShape" ;
  mf:action [
      sht:dataGraph <> ;
      sht:shapesGraph <> ;
    ] ;
  mf:result [
      rdf:type sh:ValidationReport ;
      sh:conforms "false"^^xsd:boolean ;
      sh:result [
          rdf:type sh:ValidationResult ;
          sh:focusNode ex:InvalidResource ;
          sh:resultMessage "Test message" ;
          sh:resultSeverity sh:Violation ;
          sh:sourceConstraint ex:TestShape-sparql ;
          sh:sourceConstraintComponent sh:SPARQLConstraintComponent ;
          sh:sourceShape ex:TestShape ;
          sh:value ex:InvalidResource ;
        ] ;
    ] ;
  mf:status sht:approved ;
.
"##;

const UNSUPPORTED_001_MINUS: &str = r##"@prefix ex: <http://datashapes.org/sh/tests/sparql/pre-binding/unsupported-sparql-001.test#> .
@prefix mf: <http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix sht: <http://www.w3.org/ns/shacl-test#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<>
	rdf:type mf:Manifest ;
	mf:entries ( <unsupported-sparql-001> ) .

<unsupported-sparql-001>
	rdf:type sht:Validate ;
	rdfs:label "Test of unsupported MINUS" ;
	mf:action [
		sht:dataGraph <> ;
		sht:shapesGraph <> ;
	] ;
	mf:result sht:Failure ;
	mf:status sht:approved .

ex:TestShape
	a sh:NodeShape ;
	sh:targetNode ex:InvalidResource ;
	sh:sparql [
		sh:select """
			SELECT $this
			WHERE {
				$this ?x ?any .
				MINUS { $this ?x "Value" }
			}""" ;
	] .
"##;

const UNSUPPORTED_002_VALUES: &str = r##"@prefix ex: <http://datashapes.org/sh/tests/sparql/pre-binding/unsupported-sparql-002.test#> .
@prefix mf: <http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix sht: <http://www.w3.org/ns/shacl-test#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<>
	rdf:type mf:Manifest ;
	mf:entries ( <unsupported-sparql-002> ) .

<unsupported-sparql-002>
	rdf:type sht:Validate ;
	rdfs:label "Test of unsupported VALUES" ;
	mf:action [
		sht:dataGraph <> ;
		sht:shapesGraph <> ;
	] ;
	mf:result sht:Failure ;
	mf:status sht:approved .

ex:TestShape
	a sh:NodeShape ;
	sh:targetNode ex:InvalidResource ;
	sh:sparql [
		sh:select """
			SELECT $this
			WHERE {
				VALUES ?any { true }
			}""" ;
	] .
"##;

const UNSUPPORTED_003_SERVICE: &str = r##"@prefix ex: <http://datashapes.org/sh/tests/sparql/pre-binding/unsupported-sparql-003.test#> .
@prefix mf: <http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix sht: <http://www.w3.org/ns/shacl-test#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<>
	rdf:type mf:Manifest ;
	mf:entries ( <unsupported-sparql-003> ) .

<unsupported-sparql-003>
	rdf:type sht:Validate ;
	rdfs:label "Test of unsupported SERVICE" ;
	mf:action [
		sht:dataGraph <> ;
		sht:shapesGraph <> ;
	] ;
	mf:result sht:Failure ;
	mf:status sht:approved .

ex:TestShape
	a sh:NodeShape ;
	sh:targetNode ex:InvalidResource ;
	sh:sparql [
		sh:select """
			SELECT $this
			WHERE {
				$this ?x ?any .
				SERVICE <http://aldi.de> {
					?a ?b ?c .
				}
			}""" ;
	] .
"##;

const UNSUPPORTED_004_SUBSELECT: &str = r##"@prefix ex: <http://datashapes.org/sh/tests/sparql/pre-binding/unsupported-sparql-004.test#> .
@prefix mf: <http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix sht: <http://www.w3.org/ns/shacl-test#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<>
	rdf:type mf:Manifest ;
	mf:entries ( <unsupported-sparql-004> ) .

<unsupported-sparql-004>
	rdf:type sht:Validate ;
	rdfs:label "Test of unsupported SELECT" ;
	mf:action [
		sht:dataGraph <> ;
		sht:shapesGraph <> ;
	] ;
	mf:result sht:Failure ;
	mf:status sht:approved .

ex:TestShape
	a sh:NodeShape ;
	sh:targetNode ex:InvalidResource ;
	sh:sparql [
		sh:select """
			SELECT $this
			WHERE {
				$this ?x ?any .
				{
					SELECT ?other ?b
					WHERE {
						?other ?b ?c .
					}
				}
			}""" ;
	] .
"##;

const UNSUPPORTED_005_BIND_THIS: &str = r##"@prefix ex: <http://datashapes.org/sh/tests/sparql/pre-binding/unsupported-sparql-005.test#> .
@prefix mf: <http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix sht: <http://www.w3.org/ns/shacl-test#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

<>
	rdf:type mf:Manifest ;
	mf:entries ( <unsupported-sparql-005> ) .

<unsupported-sparql-005>
	rdf:type sht:Validate ;
	rdfs:label "Test of unsupported AS ?prebound" ;
	mf:action [
		sht:dataGraph <> ;
		sht:shapesGraph <> ;
	] ;
	mf:result sht:Failure ;
	mf:status sht:approved .

ex:TestShape
	a sh:NodeShape ;
	sh:targetNode ex:InvalidResource ;
	sh:sparql [
		sh:select """
			SELECT $this
			WHERE {
				BIND (true AS $this) .
			}""" ;
	] .
"##;

const CLOSED_001: &str = r##"@prefix dash: <http://datashapes.org/dash#> .
@prefix ex: <http://datashapes.org/sh/tests/core/node/closed-001.test#> .
@prefix mf: <http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix sht: <http://www.w3.org/ns/shacl-test#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

ex:InvalidInstance1
  rdf:type ex:SomeClass ;
  ex:otherProperty 4 ;
  ex:someProperty 3 ;
.
ex:MyShape
  rdf:type sh:NodeShape ;
  sh:closed "true"^^xsd:boolean ;
  sh:property [
      sh:path ex:someProperty ;
    ] ;
  sh:targetNode ex:InvalidInstance1 ;
  sh:targetNode ex:ValidInstance1 ;
.
ex:ValidInstance1
  ex:someProperty 3 ;
.
<>
  rdf:type mf:Manifest ;
  mf:entries (
      <closed-001>
    ) ;
.
<closed-001>
  rdf:type sht:Validate ;
  rdfs:label "Test of sh:closed at node shape 001" ;
  mf:action [
      sht:dataGraph <> ;
      sht:shapesGraph <> ;
    ] ;
  mf:result [
      rdf:type sh:ValidationReport ;
      sh:conforms "false"^^xsd:boolean ;
      sh:result [
          rdf:type sh:ValidationResult ;
          sh:focusNode ex:InvalidInstance1 ;
          sh:resultPath rdf:type ;
          sh:resultSeverity sh:Violation ;
          sh:sourceConstraintComponent sh:ClosedConstraintComponent ;
          sh:sourceShape ex:MyShape ;
          sh:value ex:SomeClass ;
        ] ;
      sh:result [
          rdf:type sh:ValidationResult ;
          sh:focusNode ex:InvalidInstance1 ;
          sh:resultPath ex:otherProperty ;
          sh:resultSeverity sh:Violation ;
          sh:sourceConstraintComponent sh:ClosedConstraintComponent ;
          sh:sourceShape ex:MyShape ;
          sh:value 4 ;
        ] ;
    ] ;
  mf:status sht:approved ;
.
"##;

const CLOSED_002: &str = r##"@prefix dash: <http://datashapes.org/dash#> .
@prefix ex: <http://datashapes.org/sh/tests/core/node/closed-002.test#> .
@prefix mf: <http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix sht: <http://www.w3.org/ns/shacl-test#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

ex:InvalidInstance1
  ex:otherProperty 4 ;
  ex:someProperty 3 ;
.
ex:MyShape
  rdf:type sh:NodeShape ;
  sh:closed "true"^^xsd:boolean ;
  sh:ignoredProperties (
      rdf:type
    ) ;
  sh:property [
      sh:path ex:someProperty ;
    ] ;
  sh:targetNode ex:InvalidInstance1 ;
  sh:targetNode ex:ValidInstance1 ;
.
ex:ValidInstance1
  rdf:type ex:SomeClass ;
  ex:someProperty 3 ;
.
<>
  rdf:type mf:Manifest ;
  mf:entries (
      <closed-002>
    ) ;
.
<closed-002>
  rdf:type sht:Validate ;
  rdfs:label "Test of sh:closed at node shape 002" ;
  mf:action [
      sht:dataGraph <> ;
      sht:shapesGraph <> ;
    ] ;
  mf:result [
      rdf:type sh:ValidationReport ;
      sh:conforms "false"^^xsd:boolean ;
      sh:result [
          rdf:type sh:ValidationResult ;
          sh:focusNode ex:InvalidInstance1 ;
          sh:resultPath ex:otherProperty ;
          sh:resultSeverity sh:Violation ;
          sh:sourceConstraintComponent sh:ClosedConstraintComponent ;
          sh:sourceShape ex:MyShape ;
          sh:value 4 ;
        ] ;
    ] ;
  mf:status sht:approved ;
.
"##;
