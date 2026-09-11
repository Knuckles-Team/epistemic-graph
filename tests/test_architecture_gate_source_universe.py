"""Fixture-backed regression tests for what the static gates read as source.

Four always-run architecture gates went red on the integration head for one
reason: each pinned an invariant to a single facade *file* or to one exact
attribute *adjacency*, so an ordinary module decomposition (`<name>.rs` ->
`<name>.rs` + `<name>/**`) or an appended attribute read as a deleted
invariant. The repairs move every one of them onto the compiler's own view --
`scripts/rust_module_tree.py`'s declared module walk, and an attribute *block*
rather than an attribute *sequence*. These tests plant the known-bad and
known-good shapes so neither repair can quietly become a hole.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

# Pure/static tests -- never need the shared native engine (see conftest.py's
# session-scoped `start_epistemic_graph_server` fixture, which this marker
# exempts this module from triggering).
pytestmark = pytest.mark.no_engine

ROOT = Path(__file__).resolve().parents[1]


def _script(name: str):
    # Gate scripts run as `python3 scripts/<name>.py`, so scripts/ is their
    # sys.path[0] and sibling scanner modules resolve from it.  Reproduce that
    # here instead of relying on another test module having done it first.
    scripts = str(ROOT / "scripts")
    if scripts not in sys.path:
        sys.path.insert(0, scripts)
    path = ROOT / "scripts" / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


# --------------------------------------------------------------------------
# scripts/rust_module_tree.py -- conditional attribute shapes
# --------------------------------------------------------------------------


def _plant_module(root: Path, attribute: str) -> None:
    """A two-file module tree whose facade carries `attribute`."""

    (root / "src").mkdir(parents=True, exist_ok=True)
    (root / "src" / "facade.rs").write_text(
        f"{attribute}\npub struct Planted;\n\npub mod child;\n", encoding="utf-8"
    )
    (root / "src" / "facade").mkdir(exist_ok=True)
    (root / "src" / "facade" / "child.rs").write_text(
        'pub const PLANTED_CHILD_TOKEN: &str = "planted-child";\n', encoding="utf-8"
    )


def test_conditional_derive_is_inert_and_the_child_module_is_still_read(
    tmp_path: Path,
) -> None:
    """Known-good: `cfg_attr(<pred>, derive(...))` must not stop the walk.

    The audited derive expands to impls only, so it cannot move a `mod`
    declaration. Rejecting it made every `crates/eg-types` module tree
    unreadable (195 occurrences of the audited derives on the integration
    head), which is what kept the gates below pinned to single facade files.
    Unknown proc-macro derives remain rejected because they can emit items.
    """

    walker = _script("rust_module_tree")
    _plant_module(
        tmp_path,
        '#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]',
    )

    source = walker.read_module_tree("src/facade.rs", root_dir=tmp_path)

    assert "PLANTED_CHILD_TOKEN" in source


@pytest.mark.parametrize(
    "derive",
    [
        "schemars::JsonSchema",
        "serde::Serialize",
        "serde::Deserialize",
        "strum::IntoStaticStr",
    ],
)
def test_each_audited_conditional_derive_is_inert(
    tmp_path: Path, derive: str
) -> None:
    walker = _script("rust_module_tree")
    _plant_module(tmp_path, f'#[cfg_attr(feature = "x", derive({derive}))]')

    source = walker.read_module_tree("src/facade.rs", root_dir=tmp_path)

    assert "PLANTED_CHILD_TOKEN" in source


def test_audited_conditional_derives_can_be_combined(tmp_path: Path) -> None:
    walker = _script("rust_module_tree")
    _plant_module(
        tmp_path,
        '#[cfg_attr(feature = "x", derive('
        "schemars::JsonSchema, serde::Serialize, serde::Deserialize, "
        "strum::IntoStaticStr,))]",
    )

    source = walker.read_module_tree("src/facade.rs", root_dir=tmp_path)

    assert "PLANTED_CHILD_TOKEN" in source


@pytest.mark.parametrize(
    "attribute",
    [
        # A conditional `path` really can move the file a `mod` resolves to.
        '#[cfg_attr(feature = "x", path = "elsewhere.rs")]',
        # A nested `cfg_attr` can smuggle either of the above back in.
        '#[cfg_attr(feature = "x", cfg_attr(feature = "y", path = "elsewhere.rs"))]',
        # Not a derive path list: an attribute-macro invocation wearing the
        # `derive` name must not be waved through by the new allowance.
        '#[cfg_attr(feature = "x", derive(schemars::JsonSchema(with = "Vec<u8>")))]',
        '#[cfg_attr(feature = "x", derive("JsonSchema"))]',
        '#[cfg_attr(feature = "x", derive(schemars::JsonSchema, CustomMacro))]',
        # Still-unknown conditional attributes stay rejected.
        '#[cfg_attr(feature = "x", some_unknown_attribute(arg))]',
    ],
)
def test_unproven_conditional_attribute_shapes_still_fail_closed(
    tmp_path: Path, attribute: str
) -> None:
    walker = _script("rust_module_tree")
    _plant_module(tmp_path, attribute)

    with pytest.raises(SystemExit):
        walker.read_module_tree("src/facade.rs", root_dir=tmp_path)


# --------------------------------------------------------------------------
# scripts/check_epistemic_operations_protocol.py -- attribute block, not order
# --------------------------------------------------------------------------


_ATTRIBUTE_BLOCK_CASES = [
    # Known-good: the closure attribute is present, whatever follows it.
    ("#[serde(deny_unknown_fields)]", True),
    (
        "#[serde(deny_unknown_fields)]\n"
        '#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]',
        True,
    ),
    (
        '#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]\n'
        "#[serde(deny_unknown_fields)]",
        True,
    ),
    (
        "/// A doc comment between the attribute and the item.\n"
        "#[serde(deny_unknown_fields)]\n"
        '#[serde(rename_all = "snake_case")]',
        True,
    ),
    # Known-bad: the closure attribute is genuinely absent.
    ('#[serde(rename_all = "snake_case")]', False),
    ('#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]', False),
    ("", False),
]


@pytest.mark.parametrize("attributes,closed", _ATTRIBUTE_BLOCK_CASES)
def test_deny_unknown_fields_is_read_from_the_whole_attribute_block(
    attributes: str, closed: bool
) -> None:
    """Attribute order must not decide whether the DTO looks closed.

    The previous check required `#[serde(deny_unknown_fields)]` to sit
    immediately above `pub struct X {`, so appending the schemars `cfg_attr`
    reported every wire DTO in `crates/eg-types/src/epistemic_operations.rs` as
    open to unknown fields when nothing about its serde behaviour had changed.
    """

    gate = _script("check_epistemic_operations_protocol")
    source = (
        "#[derive(Clone, Debug, Serialize, Deserialize)]\n"
        f"{attributes}\n"
        "pub struct Planted {\n    pub field: String,\n}\n"
    )
    declaration = source.index("pub struct Planted {")

    block = gate._attribute_block_before(source, declaration)

    assert ("#[serde(deny_unknown_fields)]" in "".join(block.split())) is closed


def test_absent_struct_declaration_is_fatal_not_silently_open() -> None:
    gate = _script("check_epistemic_operations_protocol")

    with pytest.raises(gate.GateError):
        gate._assert_rust_closed([{"rust_type": "NoSuchDtoIsDeclaredAnywhere"}])


# --------------------------------------------------------------------------
# scripts/check_mint_lease_call_sites.py -- audited property, not a file path
# --------------------------------------------------------------------------


_PLANTED_CALLER = """\
async fn unrelated_earlier_item() {
    let _ = 1;
}

async fn dispatch_governed_stream_write_methods() {
{BODY}
}

async fn unrelated_later_item() {
    let _ = isolation.mint_policy_decision_lease(&smuggled, &graph, read);
}
"""

_AUDITED_BODY = """\
    let (auth_secret, isolation) = load(state).await;
    let mint_auth = MintAuthorization::compute_mac(&auth_secret, verified.claims())
        .and_then(|mac| MintAuthorization::new(&auth_secret, verified.claims(), &mac))?;
    let lease = isolation.mint_policy_decision_lease(&mint_auth, &graph, read)?;
"""

_CALLER_SUPPLIED_BODY = """\
    let lease = isolation.mint_policy_decision_lease(&caller_token, &graph, read)?;
"""


@pytest.mark.parametrize(
    "body,audited",
    [(_AUDITED_BODY, True), (_CALLER_SUPPLIED_BODY, False)],
)
def test_mint_lease_audit_reads_the_enclosing_function_not_the_file(
    body: str, audited: bool
) -> None:
    """The audited property is self-constructed authorization, not a path.

    The gate previously pinned the sole caller to the literal file
    `src/server/dispatch.rs`; the dispatch decomposition relocated it to
    `src/server/dispatch/router.rs` without touching the authorization it
    performs, and the gate called that a policy violation. What it checks now
    is the property the audit rests on -- and a neighbouring item that skips
    the MAC construction must not be able to borrow the audited function's
    credit.
    """

    gate = _script("check_mint_lease_call_sites")
    source = _PLANTED_CALLER.replace("{BODY}", body)
    call_line = source[: source.index(".mint_policy_decision_lease(")].count("\n") + 1

    name, window = gate.enclosing_function(source, call_line)

    assert name == "dispatch_governed_stream_write_methods"
    assert "unrelated_later_item" not in window
    satisfied = all(
        token in window for token in gate.REQUIRED_MINT_AUTHORIZATION_TOKENS
    )
    assert satisfied is audited


_IMPL_METHOD_CALLER = """\
fn audited_free_function() {
    let (auth_secret, verified) = load(state);
    let mint_auth = MintAuthorization::compute_mac(&auth_secret, verified.claims())
        .and_then(|mac| MintAuthorization::new(&auth_secret, verified.claims(), &mac))?;
}

impl Router {
    async fn smuggled_impl_method(&self) -> Result<()> {
        let lease = isolation.mint_policy_decision_lease(&caller_token, &graph, read)?;
        Ok(())
    }
}
"""

_NESTED_MODULE_CALLER = """\
fn audited_free_function() {
    let mint_auth = MintAuthorization::compute_mac(&auth_secret, verified.claims())
        .and_then(|mac| MintAuthorization::new(&auth_secret, verified.claims(), &mac))?;
}

mod outer {
    pub mod inner {
        pub async fn smuggled_nested_module_fn() {
            let lease = isolation.mint_policy_decision_lease(&caller_token, &graph, read);
        }
    }
}
"""

_WRAPPED_SIGNATURE_CALLER = """\
async fn dispatch_governed_stream_write_methods(
    state: &ServerState,
    graph: &GraphName,
) -> Result<Lease> {
    let mint_auth = MintAuthorization::compute_mac(&auth_secret, verified.claims())
        .and_then(|mac| MintAuthorization::new(&auth_secret, verified.claims(), &mac))?;
    isolation.mint_policy_decision_lease(&mint_auth, &graph, read)
}
"""


@pytest.mark.parametrize(
    "source,expected_function",
    [
        (_IMPL_METHOD_CALLER, "smuggled_impl_method"),
        (_NESTED_MODULE_CALLER, "smuggled_nested_module_fn"),
    ],
)
def test_mint_lease_audit_resolves_a_call_nested_below_column_zero(
    source: str, expected_function: str
) -> None:
    """Known-bad: an indented `fn` must not borrow a free function's tokens.

    The resolver once matched only column-zero `fn` items, so a call inside an
    `impl` block or a nested `mod` -- where nearly all real Rust lives -- had no
    recognised enclosing function. The lookup then fell back to the whole file,
    and the audited free function above satisfied
    `REQUIRED_MINT_AUTHORIZATION_TOKENS` on behalf of a call site that supplies
    a caller-provided token and never builds a `MintAuthorization` at all.
    """

    gate = _script("check_mint_lease_call_sites")
    call_line = source[: source.index(".mint_policy_decision_lease(")].count("\n") + 1

    name, window = gate.enclosing_function(source, call_line)

    assert name == expected_function
    assert "audited_free_function" not in window
    assert not all(token in window for token in gate.REQUIRED_MINT_AUTHORIZATION_TOKENS)


def test_mint_lease_audit_spans_a_wrapped_signature() -> None:
    """Known-good: a multi-line signature must not truncate the window.

    The continuation lines of a wrapped signature (`) -> Result<Lease> {`) sit
    at the declaration's own indentation, so a purely indentation-based span
    would end the body before it began and report the real production caller
    -- which looks exactly like this -- as unaudited.
    """

    gate = _script("check_mint_lease_call_sites")
    source = _WRAPPED_SIGNATURE_CALLER
    call_line = source[: source.index(".mint_policy_decision_lease(")].count("\n") + 1

    name, window = gate.enclosing_function(source, call_line)

    assert name == "dispatch_governed_stream_write_methods"
    assert all(token in window for token in gate.REQUIRED_MINT_AUTHORIZATION_TOKENS)


def test_mint_lease_gate_rejects_a_second_production_caller(tmp_path: Path) -> None:
    """Known-bad: two production call sites must fail, not pick one.

    The count is the whole control: a second production caller is a second
    place that has to get `MintAuthorization` construction right, and the gate
    exists so that nothing else has to notice.
    """

    gate = _script("check_mint_lease_call_sites")
    (tmp_path / "src" / "server" / "dispatch").mkdir(parents=True)
    (tmp_path / "src" / "server" / "dispatch.rs").write_text(
        "pub mod router;\n", encoding="utf-8"
    )
    (tmp_path / "src" / "server" / "dispatch" / "router.rs").write_text(
        _WRAPPED_SIGNATURE_CALLER, encoding="utf-8"
    )
    (tmp_path / "src" / "admin.rs").write_text(_IMPL_METHOD_CALLER, encoding="utf-8")

    with pytest.raises(SystemExit) as failure:
        gate.check(tmp_path)

    assert "expected exactly ONE production call site" in str(failure.value)


def test_mint_lease_gate_passes_on_the_current_tree() -> None:
    gate = _script("check_mint_lease_call_sites")
    gate.check(ROOT)


# --------------------------------------------------------------------------
# The repaired gates themselves
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "gate_name",
    [
        "check_epistemic_operations_protocol",
        "check_exact_fault_restart_harness",
        "check_lazy_lifecycle_architecture",
        "check_p2_analytics_reasoning_architecture",
    ],
)
def test_repaired_architecture_gate_passes_on_the_current_tree(gate_name: str) -> None:
    gate = _script(gate_name)
    result = gate.main()
    assert result in (None, 0)
