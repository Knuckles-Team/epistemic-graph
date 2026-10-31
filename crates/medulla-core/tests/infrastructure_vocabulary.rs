//! Registry-load coverage for the infrastructure ontology's SHACL companion.

use medulla_core::graph::GraphSchemaSources;
use medulla_rdf::mapping::parse_turtle;

#[allow(non_snake_case)]
mod ontology_audit_infrastructure {
    use super::*;

    // MEDULLA-CQ-CONN-R068.1: the companion is registered and parseable even before
    // later term rows add their individual NodeShapes.
    #[test]
    fn shapes_module_loads() {
        let sources = GraphSchemaSources::default();
        let source = sources
            .core
            .get("core:infrastructure@1")
            .expect("registered infrastructure source is present");
        let shapes = source
            .shapes_ttl
            .as_deref()
            .expect("infrastructure-v1 has a registered shapes companion");

        parse_turtle(shapes).expect("infrastructure-v1 shapes companion parses");
    }
}
