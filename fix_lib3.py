import re

with open("src/lib.rs", "r") as f:
    content = f.read()

content = content.replace(
    "    fn new() -> Self {", "    #[new]\n    fn new() -> Self {"
)
content = content.replace(
    "Ok(self.core.has_node(&node_id)?)", "Ok(self.core.has_node(&node_id))"
)
content = content.replace("Ok(self.core.get_nodes()?)", "Ok(self.core.get_nodes())")
content = content.replace(
    "Ok(self.core.get_node_properties(&node_id)?)",
    "Ok(self.core.get_node_properties(&node_id))",
)
content = content.replace("Ok(self.core.node_count()?)", "Ok(self.core.node_count())")
content = content.replace("Ok(self.core.node_ids()?)", "Ok(self.core.node_ids())")
content = content.replace(
    "Ok(self.core.has_edge(&source_id, &target_id)?)",
    "Ok(self.core.has_edge(&source_id, &target_id))",
)
content = content.replace("Ok(self.core.get_edges()?)", "Ok(self.core.get_edges())")
content = content.replace(
    "Ok(self.core.get_edge_properties(&source_id, &target_id)?)",
    "Ok(self.core.get_edge_properties(&source_id, &target_id))",
)
content = content.replace("Ok(self.core.edge_count()?)", "Ok(self.core.edge_count())")
content = content.replace("Ok(self.core.get_ledger()?)", "Ok(self.core.get_ledger())")
content = content.replace(
    "Ok(self.core.diff_against(&other.core)?)",
    "Ok(self.core.diff_against(&other.core))",
)
content = content.replace(
    "Ok(self.core.compact_nodes_by_type(&node_type, threshold)?)",
    "Ok(self.core.compact_nodes_by_type(&node_type, threshold))",
)

# Fix topological_sort
old_top = "        algorithms::topological_sort(&self.core)"
new_top = "        algorithms::topological_sort(&self.core).map_err(|e| GraphError(e))"
content = content.replace(old_top, new_top)

# Fix compute_degree_centrality
old_deg = "        algorithms::compute_degree_centrality(&self.core, &node_id)"
new_deg = "        algorithms::compute_degree_centrality(&self.core, &node_id).map_err(|e| GraphError(e))"
content = content.replace(old_deg, new_deg)

# Fix run_datalog_reasoning
old_rea = """        reasoning::run_datalog_reasoning(
            &mut self.core,
            subclass_relations,
            subproperty_relations,
            symmetric_properties,
            transitive_properties,
            inverse_properties,
        )"""
new_rea = """        reasoning::run_datalog_reasoning(
            &mut self.core,
            subclass_relations,
            subproperty_relations,
            symmetric_properties,
            transitive_properties,
            inverse_properties,
        ).map_err(|e| GraphError(e))"""
content = content.replace(old_rea, new_rea)

# Fix #[pymodule]
old_pymod = """#[pymodule]
fn _epistemic_graph(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<EpistemicGraph>()?;
    Ok(())
}"""
new_pymod = """#[pymodule]
fn _epistemic_graph(m: &pyo3::Bound<'_, pyo3::types::PyModule>) -> pyo3::PyResult<()> {
    m.add_class::<EpistemicGraph>()?;
    Ok(())
}"""
content = content.replace(old_pymod, new_pymod)

with open("src/lib.rs", "w") as f:
    f.write(content)
