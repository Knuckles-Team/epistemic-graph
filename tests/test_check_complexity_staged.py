"""Plant-and-fire tests for the staged complexity gate's regression judgment.

BUG-CX-EXEMPT-SCORE: the exhaustive-dispatch exemption used to be folded into
the compared VALUE (an exempt row's cyclomatic was hard-zeroed before the
before/after comparison), so a function that was exempt-over-cap at HEAD and
was simplified below the cap -- the exemption no longer applying because
`exhaustive_dispatch_exempt` never grants it to an at-or-under-cap function --
read as a jump from a fabricated 0 to its real value, and was reported WORSE.
Simplifying a function until it no longer needed the exemption was the one
thing the gate could not distinguish from making it worse.

Every case here measures REAL Rust source through the REAL `cccc` binary and
the REAL exhaustive-dispatch rule -- nothing here mocks cccc or the rule -- so
a change to either one that reintroduces the defect fails these tests too.

`handle_shex_validate` is the real function, copied verbatim both sides: the
base from `git show e25c73fb:src/server/handlers/rdf.rs` (cyclomatic 11 /
cognitive 11, over the cyclomatic cap, exempt), the after from lane F3a's held
working-tree copy at `/var/tmp/l9/eg-f3a/rdf.rs.worktree.bak` (cyclomatic 9 /
cognitive 10, under both caps, not exempt) -- the exact edit the gate wrongly
blocked. Each is independently measured below rather than asserted by fiat.
"""

from __future__ import annotations

import importlib.util
import shutil
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
pytestmark = [
    pytest.mark.no_engine,
    pytest.mark.skipif(
        shutil.which("cccc") is None
        and not (Path.home() / ".local/bin/cccc").is_file(),
        reason="cccc is not installed on this host",
    ),
]


def _module():
    """Load ``scripts/check_complexity_staged.py`` the way its own tests for
    ``rust_exhaustive_match.py`` load their subject: by file path, not as an
    installed package, so the test exercises exactly the checked-in script."""
    scripts_dir = ROOT / "scripts"
    sys.path.insert(0, str(scripts_dir))
    path = scripts_dir / "check_complexity_staged.py"
    spec = importlib.util.spec_from_file_location("check_complexity_staged", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["check_complexity_staged"] = module
    spec.loader.exec_module(module)
    return module


def _measure(mod, tmp_path: Path, filename: str, source: str):
    path = tmp_path / filename
    path.write_text(source, encoding="utf-8")
    return mod.measure(str(path), mod.DEFAULT_MAX_CYCLOMATIC, mod.DEFAULT_MAX_COGNITIVE)


def _judge(mod, tmp_path, before_name, before_src, after_name, after_src):
    before = _measure(mod, tmp_path, before_name, before_src)
    after = _measure(mod, tmp_path, after_name, after_src)
    findings = mod.judge(
        before, after, mod.DEFAULT_MAX_CYCLOMATIC, mod.DEFAULT_MAX_COGNITIVE
    )
    return before, after, findings


# ---------------------------------------------------------------------------
# Fixture sources
# ---------------------------------------------------------------------------

# The real function at e25c73fb, verbatim. Measured: cyclomatic 11, cognitive
# 11 -- over the cyclomatic cap (10), within the cognitive cap (15) -- with
# four Result matches (8 arms total, none irrefutable), residual 11 - 8 = 3.
# All four terms of `exhaustive_dispatch_exempt` hold: exempt, over cap.
HANDLE_SHEX_VALIDATE_BASE = """
async fn handle_shex_validate(
    req_id: u64,
    graph_name: &str,
    core: &Arc<GraphCore>,
    schema: String,
    data_graph: String,
    shape_map: Vec<[String; 2]>,
) -> Response {
    let schema = match eg_shex::Schema::from_shexj(&schema) {
        Ok(s) => s,
        Err(e) => return Response::err(req_id, format!("ShexValidate: bad schema: {e}")),
    };
    // Data graph: an inline Turtle document, else the live graph's exported RDF.
    let data = if data_graph.trim().is_empty() {
        let exported = eg_rdf::mapping::export_triples(core, graph_name);
        match exported {
            Ok(triples) => {
                let mut g = eg_shex::Graph::new();
                for t in &triples {
                    g.insert(t);
                }
                g
            }
            Err(e) => {
                return Response::err(req_id, format!("ShexValidate: export live graph: {e}"))
            }
        }
    } else {
        match eg_shex::graph_from_turtle(&data_graph) {
            Ok(g) => g,
            Err(e) => return Response::err(req_id, format!("ShexValidate: bad data graph: {e}")),
        }
    };
    let pairs: Vec<(&str, &str)> = shape_map
        .iter()
        .map(|p| (p[0].as_str(), p[1].as_str()))
        .collect();
    let map = eg_shex::ShapeMap::from_iri_pairs(&pairs);
    let report = eg_shex::validate(&schema, &data, &map);
    match serde_json::to_value(&report) {
        Ok(v) => Response::ok(req_id, ResultPayload::Json(v)),
        Err(e) => Response::err(req_id, format!("ShexValidate: serialize report: {e}")),
    }
}
"""

# The real F3a fix, verbatim from /var/tmp/l9/eg-f3a/rdf.rs.worktree.bak: the
# final `match serde_json::to_value(&report) {...}` (2 arms) is gone, replaced
# by a typed `ResultPayload::of::<...>` call the new result-contract program
# makes infallible at this site. Measured: cyclomatic 9, cognitive 10 -- under
# both caps, so `exhaustive_dispatch_exempt` never grants it the exemption (it
# does not need it any more).
HANDLE_SHEX_VALIDATE_SIMPLIFIED = """
async fn handle_shex_validate(
    req_id: u64,
    graph_name: &str,
    core: &Arc<GraphCore>,
    schema: String,
    data_graph: String,
    shape_map: Vec<[String; 2]>,
) -> Response {
    let schema = match eg_shex::Schema::from_shexj(&schema) {
        Ok(s) => s,
        Err(e) => return Response::err(req_id, format!("ShexValidate: bad schema: {e}")),
    };
    // Data graph: an inline Turtle document, else the live graph's exported RDF.
    let data = if data_graph.trim().is_empty() {
        let exported = eg_rdf::mapping::export_triples(core, graph_name);
        match exported {
            Ok(triples) => {
                let mut g = eg_shex::Graph::new();
                for t in &triples {
                    g.insert(t);
                }
                g
            }
            Err(e) => {
                return Response::err(req_id, format!("ShexValidate: export live graph: {e}"))
            }
        }
    } else {
        match eg_shex::graph_from_turtle(&data_graph) {
            Ok(g) => g,
            Err(e) => return Response::err(req_id, format!("ShexValidate: bad data graph: {e}")),
        }
    };
    let pairs: Vec<(&str, &str)> = shape_map
        .iter()
        .map(|p| (p[0].as_str(), p[1].as_str()))
        .collect();
    let map = eg_shex::ShapeMap::from_iri_pairs(&pairs);
    let report = eg_shex::validate(&schema, &data, &map);
    Response::ok(
        req_id,
        ResultPayload::of::<eg_types::result_contract::reasoning::ShexValidate>(shex_report_wire(
            report,
        )),
    )
}
"""

# Ordinary branching -- an if/else-if ladder, no `match` at all -- never
# eligible for the exhaustive-dispatch exemption. Under both caps.
CLASSIFY_UNDER_CAP = """
fn classify(n: i32) -> u8 {
    if n == 0 {
        0
    } else if n == 1 {
        1
    } else if n == 2 {
        2
    } else if n == 3 {
        3
    } else if n == 4 {
        4
    } else if n == 5 {
        5
    } else {
        6
    }
}
"""

# The same shape, more branches: cyclomatic and cognitive both cross the cap.
# Still no `match`, so still never exempt.
CLASSIFY_OVER_CAP = """
fn classify(n: i32) -> u8 {
    if n == 0 {
        0
    } else if n == 1 {
        1
    } else if n == 2 {
        2
    } else if n == 3 {
        3
    } else if n == 4 {
        4
    } else if n == 5 {
        5
    } else if n == 6 {
        6
    } else if n == 7 {
        7
    } else if n == 8 {
        8
    } else if n == 9 {
        9
    } else {
        10
    }
}
"""

# A flat, exhaustive 11-arm dispatch: cyclomatic 12 (over cap), cognitive 1
# (flat, no nesting), residual 12 - 11 = 1. Exempt.
DISPATCH_BASE = """
fn dispatch_kind(kind: Kind) -> u8 {
    match kind {
        Kind::A => 1,
        Kind::B => 2,
        Kind::C => 3,
        Kind::D => 4,
        Kind::E => 5,
        Kind::F => 6,
        Kind::G => 7,
        Kind::H => 8,
        Kind::I => 9,
        Kind::J => 10,
        Kind::K => 11,
    }
}
"""

# Two more arms added (13 total), nothing else changed: cyclomatic 14, still
# over cap, cognitive still 1, residual 14 - 13 = 1 -- UNCHANGED. Still
# exempt. This is exactly "the whole point of keeping the match exhaustive":
# an added enum variant costs nothing.
DISPATCH_MORE_ARMS = """
fn dispatch_kind(kind: Kind) -> u8 {
    match kind {
        Kind::A => 1,
        Kind::B => 2,
        Kind::C => 3,
        Kind::D => 4,
        Kind::E => 5,
        Kind::F => 6,
        Kind::G => 7,
        Kind::H => 8,
        Kind::I => 9,
        Kind::J => 10,
        Kind::K => 11,
        Kind::L => 12,
        Kind::M => 13,
    }
}
"""

# Same 11 arms as DISPATCH_BASE -- no arm added -- but the first arm's body
# grows real (non-dispatch) branching: cyclomatic 14, cognitive 8 (both still
# within caps -- cognitive under 15), residual 14 - 11 = 3, UP from 1. Still
# exempt (cognitive within cap, residual within cap), but the residual grew
# for a reason that is NOT "an arm was added".
DISPATCH_NON_ARM_GROWTH = """
fn dispatch_kind(kind: Kind, flag: bool, other: bool) -> u8 {
    match kind {
        Kind::A => {
            if flag {
                if other {
                    1
                } else {
                    2
                }
            } else {
                3
            }
        }
        Kind::B => 2,
        Kind::C => 3,
        Kind::D => 4,
        Kind::E => 5,
        Kind::F => 6,
        Kind::G => 7,
        Kind::H => 8,
        Kind::I => 9,
        Kind::J => 10,
        Kind::K => 11,
    }
}
"""


# ---------------------------------------------------------------------------
# The five required cases
# ---------------------------------------------------------------------------


def test_exempt_over_cap_to_non_exempt_under_cap_passes(tmp_path):
    """handle_shex_validate's actual F3a shape: simplified below the cap, and
    the exemption it no longer needs must not read as a regression."""
    mod = _module()
    before, after, findings = _judge(
        mod,
        tmp_path,
        "before.rs",
        HANDLE_SHEX_VALIDATE_BASE,
        "after.rs",
        HANDLE_SHEX_VALIDATE_SIMPLIFIED,
    )
    (before_row,) = before["handle_shex_validate"]
    (after_row,) = after["handle_shex_validate"]
    assert (before_row.cyclomatic, before_row.cognitive) == (11, 11)
    assert before_row.exempt is True
    assert (after_row.cyclomatic, after_row.cognitive) == (9, 10)
    assert after_row.exempt is False
    assert findings == []


def test_non_exempt_under_cap_to_over_cap_fails(tmp_path):
    mod = _module()
    _, _, findings = _judge(
        mod, tmp_path, "before.rs", CLASSIFY_UNDER_CAP, "after.rs", CLASSIFY_OVER_CAP
    )
    assert len(findings) == 1
    kind, name, before_vals, after_vals = findings[0]
    assert kind == "WORSE"
    assert name == "classify"
    assert after_vals[0] > mod.DEFAULT_MAX_CYCLOMATIC


def test_exempt_function_growing_non_arm_complexity_fails(tmp_path):
    mod = _module()
    before, after, findings = _judge(
        mod,
        tmp_path,
        "before.rs",
        DISPATCH_BASE,
        "after.rs",
        DISPATCH_NON_ARM_GROWTH,
    )
    (before_row,) = before["dispatch_kind"]
    (after_row,) = after["dispatch_kind"]
    assert before_row.exempt is True and after_row.exempt is True
    assert after_row.residual > before_row.residual
    assert len(findings) == 1
    assert findings[0][0] == "WORSE"


def test_exempt_function_adding_exhaustive_arms_passes(tmp_path):
    mod = _module()
    before, after, findings = _judge(
        mod, tmp_path, "before.rs", DISPATCH_BASE, "after.rs", DISPATCH_MORE_ARMS
    )
    (before_row,) = before["dispatch_kind"]
    (after_row,) = after["dispatch_kind"]
    assert before_row.exempt is True and after_row.exempt is True
    assert after_row.cyclomatic > before_row.cyclomatic  # more arms, raw grew
    assert after_row.residual == before_row.residual  # but nothing else did
    assert findings == []


def test_new_over_cap_function_fails(tmp_path):
    mod = _module()
    after = _measure(mod, tmp_path, "after.rs", CLASSIFY_OVER_CAP)
    findings = mod.judge(
        {}, after, mod.DEFAULT_MAX_CYCLOMATIC, mod.DEFAULT_MAX_COGNITIVE
    )
    assert len(findings) == 1
    kind, name, before_vals, after_vals = findings[0]
    assert kind == "NEW"
    assert name == "classify"
    assert before_vals == (0, 0)


# ---------------------------------------------------------------------------
# A real regression the fixed gate must still catch
# ---------------------------------------------------------------------------


def test_exempt_function_losing_exhaustiveness_still_fails(tmp_path):
    """Not one of the five required cases, but the scenario the old grading
    scheme relied on `graded_cyclomatic`'s zero-vs-raw jump to catch: an
    exempt dispatcher that grows a catch-all arm leaves the accepted class
    while staying over the cap. Must still fail under the corrected rule,
    which no longer zeroes an exempt row at all."""
    mod = _module()
    broke_exhaustiveness = DISPATCH_BASE.replace(
        "Kind::K => 11,", "Kind::K => 11,\n        other => other as u8,"
    )
    before, after, findings = _judge(
        mod,
        tmp_path,
        "before.rs",
        DISPATCH_BASE,
        "after.rs",
        broke_exhaustiveness,
    )
    (before_row,) = before["dispatch_kind"]
    (after_row,) = after["dispatch_kind"]
    assert before_row.exempt is True
    assert after_row.exempt is False
    assert after_row.cyclomatic > mod.DEFAULT_MAX_CYCLOMATIC
    assert len(findings) == 1
    assert findings[0][0] == "WORSE"
