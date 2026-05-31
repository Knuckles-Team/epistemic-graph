import re

with open("src/lib.rs", "r") as f:
    content = f.read()

# Fix the Ok(self.core.something()) back to self.core.something()
content = content.replace(
    "Ok(self.core.has_node(&node_id))", "self.core.has_node(&node_id)"
)
content = content.replace("Ok(self.core.get_nodes())", "self.core.get_nodes()")
content = content.replace(
    "Ok(self.core.get_node_properties(&node_id))",
    "self.core.get_node_properties(&node_id)",
)
content = content.replace("Ok(self.core.node_count())", "self.core.node_count()")
content = content.replace("Ok(self.core.node_ids())", "self.core.node_ids()")
content = content.replace(
    "Ok(self.core.has_edge(&source_id, &target_id))",
    "self.core.has_edge(&source_id, &target_id)",
)
content = content.replace("Ok(self.core.get_edges())", "self.core.get_edges()")
content = content.replace(
    "Ok(self.core.get_edge_properties(&source_id, &target_id))",
    "self.core.get_edge_properties(&source_id, &target_id)",
)
content = content.replace("Ok(self.core.edge_count())", "self.core.edge_count()")
content = content.replace("Ok(self.core.get_ledger())", "self.core.get_ledger()")
content = content.replace(
    "Ok(self.core.diff_against(&other.core))", "self.core.diff_against(&other.core)"
)
content = content.replace(
    "Ok(self.core.compact_nodes_by_type(&node_type, threshold))",
    "self.core.compact_nodes_by_type(&node_type, threshold)",
)

with open("src/lib.rs", "w") as f:
    f.write(content)
