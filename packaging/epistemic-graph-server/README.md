# epistemic-graph-server

The native `epistemic-graph-server` executable, packaged for pip. It is built from the
[epistemic-graph](https://github.com/Knuckles-Team/epistemic-graph) source at the same
version with the size-oriented `release-server` profile.

```bash
pip install epistemic-graph-server
# or, together with the client:
pip install "epistemic-graph[server]"
```

```python
from epistemic_graph_server import binary_path

print(binary_path())  # raises BinaryNotFoundError if the executable is missing
```
