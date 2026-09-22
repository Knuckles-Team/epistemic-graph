"""D7/D8: the generated API reference pages + OpenAPI document (CONCEPT:EG-P0-1).

Static, dependency-free checks over `scripts/gen_api_docs.py` and its
committed output. Uses the `load_script` fixture (see `tests/conftest.py`) to
load the generator as a fresh module rather than re-implementing any of its
rendering logic — the same anti-drift shape as
`tests/test_mutation_batch_documentation_contract.py` and
`check_status_page.py`'s relationship to `build_status_page.py`.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

pytestmark = pytest.mark.no_engine

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def gen(load_script):
    return load_script("gen_api_docs")


def test_domains_cover_every_method(gen):
    contract = gen.Contract()
    covered = {
        m["id"] for domain in gen.DOMAINS for m in contract.methods_by_domain(domain)
    }
    all_ids = {m["id"] for m in contract.methods}
    assert covered == all_ids, (
        "gen_api_docs.DOMAINS is missing a domain present in "
        f"contract/methods.json: {sorted(all_ids - covered)}"
    )
    assert contract.method_count == len(contract.methods) == len(all_ids)


def test_committed_output_matches_the_generator(gen):
    """Freshness: the committed docs/api/*.md + docs/openapi.json are
    byte-identical to what the generator produces from the CURRENT
    contract/ tree right now — the same assertion `check_api_contract_docs.py`
    makes, kept here too so `pytest` alone (no extra script invocation)
    catches drift."""
    contract = gen.Contract()
    for path, content in gen.rendered_files(contract).items():
        assert path.is_file(), f"missing generated file: {path.relative_to(ROOT)}"
        assert path.read_text(encoding="utf-8") == content, (
            f"{path.relative_to(ROOT)} is stale relative to contract/. "
            "Run: python3 scripts/gen_api_docs.py --write"
        )


def test_generation_is_deterministic(gen, tmp_path):
    """Two renders of the same contract/ tree must be byte-identical."""
    contract = gen.Contract()
    first = gen.rendered_files(contract)
    second = gen.rendered_files(gen.Contract())
    assert set(first) == set(second)
    for path in first:
        assert first[path] == second[path]


def test_openapi_document_is_valid_json_and_self_contained():
    doc = json.loads((ROOT / "docs" / "openapi.json").read_text(encoding="utf-8"))
    assert doc["openapi"] == "3.1.0"
    assert doc["paths"], "no paths were generated"
    assert doc["components"]["schemas"], "no schemas were generated"

    # Every $ref must resolve to a real components.schemas entry, OR be the
    # one documented, pinned known-limitation self-reference to the
    # document's own root (see gen_api_docs.py's module docstring).
    schemas = doc["components"]["schemas"]
    unresolved_self_refs = []

    def _walk(node: object) -> None:
        if isinstance(node, dict):
            ref = node.get("$ref")
            if isinstance(ref, str):
                if ref == "#":
                    unresolved_self_refs.append(ref)
                else:
                    assert ref.startswith("#/components/schemas/"), (
                        f"unexpected $ref: {ref}"
                    )
                    name = ref.rsplit("/", 1)[-1]
                    assert name in schemas, f"$ref to missing schema: {ref}"
            for value in node.values():
                _walk(value)
        elif isinstance(node, list):
            for item in node:
                _walk(item)

    _walk(doc)
    # Pinned count from the module docstring's "KNOWN LIMITATION" section —
    # if this changes, the docstring (and the explanation) needs updating,
    # not just this number.
    assert len(unresolved_self_refs) == 1


def test_every_method_has_a_path_and_authz_metadata():
    doc = json.loads((ROOT / "docs" / "openapi.json").read_text(encoding="utf-8"))
    methods = json.loads(
        (ROOT / "contract" / "methods.json").read_text(encoding="utf-8")
    )
    for method in methods["methods"]:
        op = doc["paths"][f"/rpc/{method['domain']}/{method['id']}"]["post"]
        assert op["operationId"] == method["id"]
        assert op["x-authz-action"] == method["policy"]["authz_action"]
        assert op["x-stability"] == method["stability"]


def test_mkdocs_nav_includes_every_generated_page():
    config = yaml.safe_load((ROOT / "mkdocs.yml").read_text(encoding="utf-8"))

    def _flatten(nav):
        for entry in nav:
            if isinstance(entry, dict):
                for value in entry.values():
                    if isinstance(value, list):
                        yield from _flatten(value)
                    else:
                        yield value
            elif isinstance(entry, str):
                yield entry

    pages = set(_flatten(config["nav"]))
    expected = {"api/index.md", "swagger-ui.md"}
    domains_path = ROOT / "contract"
    for domain_file in sorted((domains_path.parent / "docs" / "api").glob("*.md")):
        if domain_file.name != "index.md":
            expected.add(f"api/{domain_file.name}")
    missing = expected - pages
    assert not missing, f"mkdocs.yml nav omits generated page(s): {sorted(missing)}"
    for page in expected:
        assert (ROOT / "docs" / page).is_file()


def test_swagger_ui_page_has_no_external_network_reference():
    page = (ROOT / "docs" / "swagger-ui.md").read_text(encoding="utf-8")
    assert "http://" not in page
    assert "https://" not in page
    assert "cdn." not in page.lower()
    for asset in ("swagger-ui.css", "swagger-ui-bundle.js"):
        assert (ROOT / "docs" / "assets" / "swagger-ui" / asset).is_file()


def test_pre_commit_hook_is_wired():
    document = yaml.safe_load((ROOT / ".config" / "pre-commit.yaml").read_text())
    hooks = {
        hook["id"]: hook
        for repo in document["repos"]
        for hook in repo.get("hooks", [])
        if "id" in hook
    }
    hook = hooks["api-contract-docs"]
    assert hook["entry"] == "python3 scripts/check_api_contract_docs.py"
    assert hook["always_run"] is True
    assert "pre-commit" in hook["stages"]
