//! Graph names reserved for non-graph RBAC resources (EH-378 follow-up).
//!
//! The RBAC model has no typed resource kinds: a grant's `ResourceSelector::Graph(name)`
//! is a bare string, and non-graph resources reuse it under a reserved prefix. The
//! server's foreign-source sharing names `foreign-source:<owner agent>/<name>`. If a
//! graph could carry such a name, a grant written for that graph would also convey use
//! of a registered (credential-bearing) foreign source, and vice versa. So every graph
//! creation refuses a reserved prefix with a typed `RESERVED_GRAPH_NAME` error, at the
//! registry chokepoint every creation path reaches.

/// The RBAC resource namespace of a shared foreign source (`foreign-source:<agent>/<name>`).
pub const FOREIGN_SOURCE_RESOURCE_PREFIX: &str = "foreign-source:";

/// Name prefixes that belong to non-graph RBAC resources and are never graph names.
pub const RESERVED_RBAC_RESOURCE_PREFIXES: &[&str] = &[FOREIGN_SOURCE_RESOURCE_PREFIX];

/// Refuse a graph name that falls inside a reserved RBAC resource namespace.
pub fn validate_graph_name(name: &str) -> Result<(), String> {
    match RESERVED_RBAC_RESOURCE_PREFIXES
        .iter()
        .find(|prefix| name.starts_with(*prefix))
    {
        Some(prefix) => Err(format!(
            "RESERVED_GRAPH_NAME: graph names starting with '{prefix}' are reserved for \
             non-graph RBAC resources"
        )),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reserved_rbac_resource_prefixes_are_not_graph_names() {
        let err = validate_graph_name("foreign-source:worker1/remote_docs").unwrap_err();
        assert!(err.starts_with("RESERVED_GRAPH_NAME"), "{err}");
        assert!(validate_graph_name("foreign-sources-graph").is_ok());
        assert!(validate_graph_name("__commons__").is_ok());
    }
}
