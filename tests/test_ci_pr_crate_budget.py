import importlib.util
import pathlib

spec = importlib.util.spec_from_file_location(
    "ci_pr_crate_budget",
    pathlib.Path(__file__).resolve().parents[1] / "scripts" / "ci_pr_crate_budget.py",
)
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)


def _meta(edges):
    names = sorted({n for e in edges for n in e} | {"leaf"})
    pkgs = [
        {"id": n, "name": n, "dependencies": [{"name": d} for a, d in edges if a == n]}
        for n in names
    ]
    return {"packages": pkgs, "workspace_members": names}


def test_closure_includes_transitive_dependents():
    meta = _meta([("b", "a"), ("c", "b")])
    assert mod.rebuild_closure(meta, {"a"}) == {"a", "b", "c"}
    assert mod.rebuild_closure(meta, {"c"}) == {"c"}
