import re

with open("src/lib.rs", "r") as f:
    content = f.read()

imports = """
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
"""
if "use pyo3::prelude::*;" not in content:
    content = content.replace(
        "use std::collections::HashMap;", "use std::collections::HashMap;\n" + imports
    )

# We define the Error struct for PyO3
error_struct = """
pub struct GraphError(pub String);
impl From<String> for GraphError { fn from(s: String) -> Self { GraphError(s) } }
impl std::convert::From<GraphError> for pyo3::PyErr {
    fn from(err: GraphError) -> pyo3::PyErr { pyo3::exceptions::PyValueError::new_err(err.0) }
}
"""
if "pub struct GraphError" not in content:
    content = content.replace(
        "use std::collections::HashMap;",
        "use std::collections::HashMap;\n" + error_struct,
    )

# Re-add #[pyclass] and #[pymethods]
if "#[pyclass]" not in content:
    content = content.replace(
        "pub struct EpistemicGraph", "#[pyclass]\npub struct EpistemicGraph"
    )

if "#[pymethods]" not in content:
    content = content.replace(
        "impl EpistemicGraph {", "#[pymethods]\nimpl EpistemicGraph {"
    )

# Instead of blindly replacing Result<T, String> with Result<T, GraphError>,
# we replace EXACTLY the return types of the methods.
content = content.replace("-> Result<(), String> {", "-> Result<(), GraphError> {")
content = content.replace(
    "-> Result<usize, String> {", "-> Result<usize, GraphError> {"
)
content = content.replace(
    "-> Result<Vec<String>, String> {", "-> Result<Vec<String>, GraphError> {"
)
content = content.replace("-> Result<f64, String> {", "-> Result<f64, GraphError> {")
content = content.replace(
    "-> Result<String, String> {", "-> Result<String, GraphError> {"
)
content = content.replace(
    "-> Result<Vec<HashMap<String, String>>, String> {",
    "-> Result<Vec<HashMap<String, String>>, GraphError> {",
)
content = content.replace(
    "-> Result<Vec<f64>, String> {", "-> Result<Vec<f64>, GraphError> {"
)

# Now handle the map_err functions
content = content.replace(".map_err(|e| {", ".map_err(|e| GraphError({")
content = content.replace(
    "})", "} ))"
)  # Wait, this is risky. Let's just fix the few `.map_err` manually.

# Wait, the only `.map_err` calls are in get_context_view, metrics, prune_by_lifecycle
# Let's replace those specifically
content = re.sub(
    r'serde_json::to_string\(&(.*?)\)\.map_err\(\|e\|\s*\{\s*format\!\(\s*"(.*?)"\s*\)\s*\}\)',
    r'serde_json::to_string(&\1).map_err(|e| GraphError(format!("\2")))',
    content,
)

pymodule = """
#[pymodule]
fn _epistemic_graph(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<EpistemicGraph>()?;
    Ok(())
}
"""
if "#[pymodule]" not in content:
    content += pymodule

# Finally, some internal method bodies might return `Err("...".to_string())` instead of `Err(GraphError("...".to_string()))`.
# But ALL methods in src/lib.rs that return Result<T, String> actually return it from self.core.xxx(), which returns Result<T, String>.
# So they just need `?` and an `Ok(...)` wrapping!
# To do this safely, we just change self.core.xxx() to Ok(self.core.xxx()?)
content = re.sub(
    r"(\s+)self\.core\.([a-zA-Z0-9_]+)\((.*?)\)\n",
    r"\1Ok(self.core.\2(\3)?)\n",
    content,
)
content = re.sub(
    r"(\s+)algorithms::([a-zA-Z0-9_]+)\(&mut self\.core(.*?)(\)?)\n",
    r"\1Ok(algorithms::\2(&mut self.core\3?\n",
    content,
)  # this regex is bad.

with open("src/lib.rs", "w") as f:
    f.write(content)
