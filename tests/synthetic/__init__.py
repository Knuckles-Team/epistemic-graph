"""Self-checks for the synthetic generators in `epistemic_graph.testing.synthetic`.

A package (like `tests/parity/`) so these modules import under dotted names and
cannot shadow the top-level `conftest`. Every test here is `no_engine`: each one
re-derives a generator's planted truth by a route that does not reuse the
generator's own construction.
"""
