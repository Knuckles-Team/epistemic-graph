"""Named `Method` write families keep every classifier's pre-family membership.

`crates/eg-types/src/protocol/method/families.rs` declares variant families once
(`Method::write_family`, one `MethodWriteFamily::X` arm per family) and the
classifiers select families instead of repeating literal lists. This test
checks, for every method ID in the generated contract catalog
(`contract/methods.json`), that each classifier selects the ID exactly when the
literal list it had before the families existed did. Those lists are copied
below from the base commit `6a59a51f0`.

A classifier's selection is read from code only: the variant patterns in the
classifier expression or match arm plus the arm patterns of every family it
names. Comments and literals are masked, so a variant removed from a family is
missing from every classifier that selects it.
"""

from __future__ import annotations

import importlib.util
import json
import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.no_engine

ROOT = Path(__file__).resolve().parents[1]
FAMILIES = "crates/eg-types/src/protocol/method/families.rs"

_BROKER_WRITES = frozenset(
    {
        "DeclareExchange",
        "DeleteExchange",
        "BindQueue",
        "UnbindQueue",
        "Publish",
        "DeclareQueue",
        "PublishEx",
        "BrokerConsume",
        "BrokerAck",
        "BrokerReject",
        "SweepExpired",
        "StreamDeclare",
        "StreamPublish",
        "StreamTrim",
        "StreamCommitOffset",
        "PublishConfirmed",
        "PublishIdempotent",
        "BrokerAckTag",
        "BrokerNackTag",
        "BrokerRenewTag",
    }
)
_WORK_ITEM_METHODS = frozenset(
    {
        "KgDelegate",
        "SubmitWorkItem",
        "SubmitWorkItems",
        "ClaimWorkItem",
        "RenewWorkItemLease",
        "CommitWorkItemResult",
        "CancelWorkItem",
        "DeferWorkItem",
        "CasWorkItemMetadata",
    }
)
_RESOURCE_AND_CAPACITY_WRITES = frozenset(
    {
        "ReserveWorkItemResources",
        "ReleaseWorkItemResources",
        "ReclaimWorkItemResources",
        "UpdateResourceHost",
        "AcquireCapacity",
        "RenewCapacity",
        "ReleaseCapacity",
        "ReclaimExpiredCapacity",
        "UpdateCapacityCell",
    }
)
_MEMORY_SCENE_WRITES = frozenset(
    {
        "CreateSummaryNode",
        "Consolidate",
        "Reinforce",
        "DecayNode",
        "DecayMemories",
        "EvictBelow",
        "Maintain",
        "AddSceneObject",
        "SetPose",
        "Reparent",
        "StartTrajectory",
        "AppendStep",
    }
)

# `eg_core::durable_apply::is_durable_mutation` at base (broker block + tail).
_DURABLE_AT_BASE = (
    _BROKER_WRITES
    | _MEMORY_SCENE_WRITES
    | {
        "AddNode",
        "CreateNodeIfAbsent",
        "RemoveNode",
        "CompareAndSetNodeFields",
        "AddEdge",
        "RemoveEdge",
        "InvalidateEdge",
        "SupersedeEdge",
        "BatchUpdate",
        "ClaimNext",
        "ClaimWorkItem",
        "RenewWorkItemLease",
        "CommitWorkItemResult",
        "CancelWorkItem",
        "DeferWorkItem",
        "CasWorkItemMetadata",
        "ClearGraph",
        "AddEmbedding",
    }
)

# `redb_store::store_read::supports_atomic_batch_rows` at base.
_ATOMIC_BATCH_ROWS_AT_BASE = frozenset(
    {
        "AddNode",
        "RemoveNode",
        "CompareAndSetNodeFields",
        "AddEdge",
        "RemoveEdge",
        "BatchUpdate",
        "AddEmbedding",
        "ClearGraph",
        "ClearLedger",
        "CreateGraph",
        "DeleteGraph",
        "ClaimWorkItem",
        "SubmitWorkItem",
        "SubmitWorkItems",
        "RenewWorkItemLease",
        "CommitWorkItemResult",
        "CancelWorkItem",
        "DeferWorkItem",
        "CasWorkItemMetadata",
        "ReserveWorkItemResources",
        "ReleaseWorkItemResources",
        "ReclaimWorkItemResources",
        "UpdateResourceHost",
    }
)

# The final `matches!` of `server::access::requires_write` at base.
_UNCONDITIONAL_WRITES_AT_BASE = (
    _MEMORY_SCENE_WRITES
    | _RESOURCE_AND_CAPACITY_WRITES
    | (_WORK_ITEM_METHODS - {"KgDelegate", "SubmitWorkItem", "SubmitWorkItems"})
    | {
        "BeginTxn",
        "Rollback",
        "AddNode",
        "CreateNodeIfAbsent",
        "RemoveNode",
        "CompareAndSetNodeFields",
        "AddEdge",
        "RemoveEdge",
        "InvalidateEdge",
        "SupersedeEdge",
        "ClearGraph",
        "AddEmbedding",
        "PruneByLifecycle",
        "BatchUpdate",
        "EvictLRU",
        "DecaySweep",
        "TouchNodes",
        "FromMsgpack",
        "ClearLedger",
        "ApplyLedger",
        "CompactNodesByType",
        "RunDatalogReasoning",
        "ApplyChangeEnvelope",
        "ApplyChangeEnvelopes",
        "Reconcile",
        "ApplyMutation",
        "ApplyMultisigMutation",
        "IcvConfigure",
        "DeleteGraph",
        "ClaimNext",
        "SubmitWorkItem",
        "KgDelegate",
        "SubmitWorkItems",
        "MintWorkItemClaimCapability",
    }
)

# The ControlPlane arm of `mutation_batch::canonical::domain_for` at base.
_CONTROL_PLANE_DOMAIN_AT_BASE = _WORK_ITEM_METHODS | _RESOURCE_AND_CAPACITY_WRITES

# The `MutationSurface::Job` arm of `canonical::surface_for` at base (the
# separate feature-gated `AnalyticsJob` arm is unchanged and not part of it).
_JOB_SURFACE_AT_BASE = _RESOURCE_AND_CAPACITY_WRITES | {
    "KgDelegate",
    "SubmitWorkItem",
    "SubmitWorkItems",
}


def _gate_module():
    gate_path = ROOT / "scripts" / "check_persisted_mutation_contract.py"
    spec = importlib.util.spec_from_file_location("persisted_mutation_gate", gate_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _catalog_ids() -> set[str]:
    catalog = json.loads((ROOT / "contract" / "methods.json").read_text())
    return {entry["id"] for entry in catalog["methods"]}


def _selected(module, expression: str, sources: dict[str, str]) -> set[str]:
    """Variants an expression selects, with the families it names expanded."""

    expanded = module.expand_method_families(expression, sources["families"])
    return set(
        re.findall(r"\bMethod::([A-Z][A-Za-z0-9_]*)", module._rust_code_mask(expanded))
    )


def _guarded_return(module, function: str, target: str) -> str:
    """The condition of `if <condition> { return <target>; }` inside `function`."""

    code = module._rust_code_mask(function)
    pattern = rf"\bif\s+([^{{]*?)\s*\{{\s*return\s+{re.escape(target)}\s*;\s*\}}"
    matches = [match.span(1) for match in re.finditer(pattern, code)]
    assert len(matches) == 1, f"expected one guarded return of {target}"
    start, end = matches[0]
    return function[start:end]


def _arm_pattern(module, function: str, target: str) -> str:
    """The pattern of the `match` arm in `function` that yields `target` from a
    family selection (other arms yielding the same value are not part of it)."""

    code = module._rust_code_mask(function)
    arms = []
    for arm in re.finditer(rf"=>\s*{re.escape(target)}\s*,", code):
        start = _arm_start(code, arm.start())
        if "MethodWriteFamily" in code[start : arm.start()]:
            arms.append((start, arm.start()))
    assert len(arms) == 1, f"expected one family arm yielding {target}"
    start, end = arms[0]
    return function[start:end]


def _arm_start(code: str, position: int) -> int:
    """Where the arm whose `=>` is at `position` begins."""

    depth = 0
    while position > 0:
        position -= 1
        char = code[position]
        if char in ")]}":
            depth += 1
        elif char in "([{":
            if depth == 0:
                break
            depth -= 1
        elif depth == 0 and (char == "," or code[position : position + 2] == "=>"):
            break
    return position + 1


def _tail_expression(module, function: str) -> str:
    """The trailing expression after the last surface-classifier early return."""

    code = module._rust_code_mask(function)
    marker = list(re.finditer(r"return\s+result\s*;\s*\}", code))
    assert marker, "requires_write lost its surface-classifier early returns"
    return function[marker[-1].end() :]


def _sources(module) -> dict[str, str]:
    return {
        "families": (ROOT / FAMILIES).read_text(encoding="utf-8"),
        "durable_apply": module.read("crates/eg-core/src/durable_apply.rs"),
        "store_read": module.read("src/redb_store/store_read.rs"),
        "access": module.read_module_tree("src/server/access.rs"),
        "canonical": module.read("src/server/mutation_batch/canonical.rs"),
    }


def _classifier_selections(
    module, sources: dict[str, str]
) -> dict[str, tuple[set[str], frozenset[str]]]:
    fn = module._function
    canonical = sources["canonical"]
    service = fn(canonical, "service_owner_domain")
    surface = fn(canonical, "surface_for")
    native = fn(sources["access"], "requires_write_native_surface")
    expressions = {
        "is_durable_mutation": fn(sources["durable_apply"], "is_durable_mutation"),
        "supports_atomic_batch_rows": fn(
            sources["store_read"], "supports_atomic_batch_rows"
        ),
        "requires_write (unconditional tail)": _tail_expression(
            module, fn(sources["access"], "requires_write")
        ),
        "requires_write (broker)": _guarded_return(module, native, "Some(true)"),
        "is_work_item_method": fn(canonical, "is_work_item_method"),
        "domain ControlPlane": _arm_pattern(
            module, service, "DurabilityDomain::ControlPlane"
        ),
        "domain Broker": _arm_pattern(module, service, "DurabilityDomain::Broker"),
        "surface Job": _arm_pattern(module, surface, "Some(MutationSurface::Job)"),
        "surface Broker": _arm_pattern(
            module, surface, "Some(MutationSurface::Broker)"
        ),
    }
    at_base = {
        "is_durable_mutation": _DURABLE_AT_BASE,
        "supports_atomic_batch_rows": _ATOMIC_BATCH_ROWS_AT_BASE,
        "requires_write (unconditional tail)": _UNCONDITIONAL_WRITES_AT_BASE,
        "requires_write (broker)": _BROKER_WRITES,
        "is_work_item_method": _WORK_ITEM_METHODS,
        "domain ControlPlane": _CONTROL_PLANE_DOMAIN_AT_BASE,
        "domain Broker": _BROKER_WRITES,
        "surface Job": _JOB_SURFACE_AT_BASE,
        "surface Broker": _BROKER_WRITES,
    }
    return {
        name: (_selected(module, expression, sources), at_base[name])
        for name, expression in expressions.items()
    }


def _membership_changes(module, sources: dict[str, str]) -> dict[str, list[str]]:
    """Per classifier, the catalog IDs whose membership differs from base."""

    ids = _catalog_ids()
    changes: dict[str, list[str]] = {}
    for classifier, (selected, at_base) in _classifier_selections(
        module, sources
    ).items():
        unknown = sorted((selected | at_base) - ids)
        assert not unknown, f"{classifier}: not in the contract catalog: {unknown}"
        differing = sorted(i for i in ids if (i in selected) != (i in at_base))
        if differing:
            changes[classifier] = differing
    return changes


def test_family_composition_keeps_every_classifier_membership_per_method_id() -> None:
    module = _gate_module()
    assert _membership_changes(module, _sources(module)) == {}


def test_removing_a_family_member_changes_the_composing_classifiers() -> None:
    module = _gate_module()
    sources = _sources(module)
    marker = "| Self::AppendStep { .. }"
    assert sources["families"].count(marker) == 1
    sources["families"] = sources["families"].replace(marker, "", 1)

    changes = _membership_changes(module, sources)

    assert changes == {
        "is_durable_mutation": ["AppendStep"],
        "requires_write (unconditional tail)": ["AppendStep"],
    }


def test_commented_out_family_member_is_not_a_member() -> None:
    module = _gate_module()
    sources = _sources(module)
    marker = "| Self::RenewWorkItemLease { .. }"
    assert sources["families"].count(marker) == 1
    sources["families"] = sources["families"].replace(marker, f"// {marker}", 1)

    changes = _membership_changes(module, sources)

    assert set(changes) == {
        "is_durable_mutation",
        "supports_atomic_batch_rows",
        "requires_write (unconditional tail)",
        "is_work_item_method",
        "domain ControlPlane",
    }
    assert all(ids == ["RenewWorkItemLease"] for ids in changes.values())
