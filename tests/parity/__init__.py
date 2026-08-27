"""Marks `tests/parity/` as a real Python package.

BUG-CX-002: without this file, pytest's default "prepend" import mode
imported this directory's `conftest.py` as the bare top-level module
`conftest` -- exactly the same name `tests/conftest.py` (also directory-
rootless) imports as. Whichever one pytest imported first shadowed the
other in `sys.modules`, so every top-level `tests/test_*.py` module doing
`from conftest import TEST_AGENT_ID, request_context, ...` (targeting
`tests/conftest.py`) got whichever conftest module happened to be cached,
failing with `ImportError: cannot import name ... from 'conftest'` for 23
test modules and aborting collection entirely (pytest exit code 2).

Adding this `__init__.py` makes `tests/parity/` a package (`parity`, since
`tests/` itself still has no `__init__.py` and remains the sys.path
insertion point), so pytest imports this subdirectory's modules under the
distinct dotted names `parity.conftest`, `parity.test_parity_graph_ops`,
`parity.test_persist_dir_wiring` -- no more collision with the top-level
`conftest` module `tests/conftest.py` occupies. `tests/parity/conftest.py`
and `tests/parity/test_parity_graph_ops.py` were updated from plain
`from _harness import ...` to relative `from ._harness import ...` to
match (see their own comments).
"""
