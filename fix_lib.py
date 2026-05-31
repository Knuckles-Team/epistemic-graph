import re

with open("src/lib.rs", "r") as f:
    content = f.read()

# Add imports
imports = """
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
"""
if "use pyo3::prelude::*;" not in content:
    content = content.replace(
        "use std::collections::HashMap;", "use std::collections::HashMap;\n" + imports
    )

# Add pyclass
if "#[pyclass]" not in content:
    content = content.replace(
        "pub struct EpistemicGraph", "#[pyclass]\npub struct EpistemicGraph"
    )

# Add pymethods
if "#[pymethods]" not in content:
    content = content.replace(
        "impl EpistemicGraph {", "#[pymethods]\nimpl EpistemicGraph {"
    )

# Add pymodule
pymodule = """
#[pymodule]
fn _epistemic_graph(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<EpistemicGraph>()?;
    Ok(())
}
"""
if "#[pymodule]" not in content:
    content += pymodule

# Helper struct for custom error mapping
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

# Replace Result<T, String> with Result<T, GraphError>
content = re.sub(r"Result<([^,]+),\s*String>", r"Result<\1, GraphError>", content)
content = re.sub(r"Result<\(\),\s*String>", r"Result<(), GraphError>", content)

# But wait, there are also map_err returning String, like map_err(|e| format!("..."))
# If we return Result<T, GraphError>, any ? operator where the inner returns String will use From<String> for GraphError!
# So we don't need to change the map_err closures!

# Let's fix map_err closures to return GraphError instead of String... wait, if the closure returns String, ? will not work if the function returns Result<T, GraphError> UNLESS we use ?
# Actually, if map_err returns String, the Result is Result<T, String>. If the function returns Result<T, GraphError>, returning Result<T, String> is a type mismatch.
# Let's change .map_err(|e| format!(...)) to .map_err(|e| GraphError(format!(...)))
content = re.sub(
    r"\.map_err\(\|e\|\s*\{\s*format\!\(",
    r".map_err(|e| { GraphError(format!(",
    content,
)
content = re.sub(r"\)\s*\}\)", r")) })", content)

with open("src/lib.rs", "w") as f:
    f.write(content)
