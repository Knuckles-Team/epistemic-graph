# epistemic-graph

This name is retired. The engine is published as **`medulla`**; this package is a
tombstone — it ships no code and exists only to move an old install onto the
current one.

```bash
pip install medulla
```

The Python package still imports as `epistemic_graph`; only the distribution name
moved. Installing this package pulls `medulla` in, so an existing
`pip install epistemic-graph` keeps working and reports the switch instead of
silently holding a frozen 2.27.7 engine.

The native server executable moved to `medulla-stem`:

```bash
pip install medulla-stem
```

Source, documentation, and release notes live in the one repository:
<https://github.com/Knuckles-Team/medulla>
