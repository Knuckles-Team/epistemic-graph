# epistemic-graph-server

This name is retired. The native server executable is published as
**`medulla-stem`**; this package is a tombstone — it ships no code, no executable,
and exists only to move an old install onto the current one.

```bash
pip install medulla-stem
```

The executable is still named `epistemic-graph-server`, and the Python helper
still imports as `epistemic_graph_server` (`from epistemic_graph_server import
binary_path`). Only the distribution name moved. Installing this package pulls
`medulla-stem` in, so an existing `pip install epistemic-graph-server` keeps
working and reports the switch instead of silently holding a frozen 2.27.7
server.

The Python client and in-process engine moved to `medulla`:

```bash
pip install medulla
```

Source, documentation, and release notes live in the one repository:
<https://github.com/Knuckles-Team/medulla>
