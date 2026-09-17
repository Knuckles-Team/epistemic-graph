"""Test-support code shipped with the client so other repositories can reuse it.

Nothing in this package is used by the client at runtime. It exists so that the
engine's own end-to-end suite and downstream consumers (agent-utilities, the
connector SDK) drive the engine with the same synthetic inputs instead of each
keeping a private copy.
"""
