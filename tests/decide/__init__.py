"""Marks `tests/decide/` as a real Python package.

The same reason `tests/parity/__init__.py` exists (BUG-CX-002): with pytest's
default "prepend" import mode, a directory without an `__init__.py` that
carries its own `conftest.py` would import that file as the bare top-level
module `conftest`, shadowing `tests/conftest.py` for every other test module
in the session. Keeping `tests/decide/` a package from day one (its test
modules import as `decide.test_*`) makes that failure mode impossible if a
`conftest.py` is ever added here.
"""
