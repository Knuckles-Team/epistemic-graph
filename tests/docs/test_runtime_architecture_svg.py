"""The runtime-architecture.svg stays accessible, aligned and in step with source."""

from __future__ import annotations

from pathlib import Path
from xml.etree import ElementTree

ROOT = Path(__file__).resolve().parents[2]
SVG_NS = "{http://www.w3.org/2000/svg}"
RUNTIME_DIAGRAM = ROOT / "docs/assets/runtime-architecture.svg"
EXPECTED_CONNECTORS = {
    "M120 90V100H692": ((120, 90), (692, 100)),
    "M120 100V135": ((120, 100), (120, 135)),
    "M310 100V135": ((310, 100), (310, 135)),
    "M500 100V135": ((500, 100), (500, 135)),
    "M692 100V135": ((692, 100), (692, 135)),
    "M780 180H825": ((780, 180), (825, 180)),
    "M780 285H825": ((780, 285), (825, 285)),
    "M1050 305V335": ((1050, 305), (1050, 335)),
    "M1050 435V485": ((1050, 435), (1050, 485)),
    "M255 565H445": ((255, 565), (445, 565)),
    "M705 565H825": ((705, 565), (825, 565)),
    "M960 645V705": ((960, 645), (960, 705)),
}
BOXES = (
    (35, 35, 205, 90),
    (35, 135, 205, 225),
    (225, 135, 395, 225),
    (415, 135, 585, 225),
    (605, 135, 780, 225),
    (415, 245, 780, 325),
    (825, 135, 1275, 305),
    (825, 335, 1275, 435),
    (825, 485, 1275, 645),
    (825, 705, 1275, 815),
    (35, 520, 255, 610),
    (445, 520, 705, 610),
)


def _runtime_svg() -> ElementTree.Element:
    return ElementTree.parse(RUNTIME_DIAGRAM).getroot()


def _runtime_flows(svg_root: ElementTree.Element) -> ElementTree.Element:
    flows = svg_root.find(f".//{SVG_NS}g[@class='connections']")
    assert flows is not None
    return flows


def _is_box_border(point: tuple[int, int], bounds: tuple[int, int, int, int]) -> bool:
    x, y = point
    left, top, right, bottom = bounds
    return (x in (left, right) and top <= y <= bottom) or (
        y in (top, bottom) and left <= x <= right
    )


def _on_any_box_border(point: tuple[int, int]) -> bool:
    return any(_is_box_border(point, box) for box in BOXES)


def _rect_where(
    rectangles: list[ElementTree.Element], **attributes: str
) -> ElementTree.Element:
    return next(
        rect
        for rect in rectangles
        if all(rect.attrib.get(name) == value for name, value in attributes.items())
    )


def test_runtime_svg_is_accessible_and_theme_aware() -> None:
    svg_root = _runtime_svg()
    background = svg_root.find(f"{SVG_NS}rect[@class='bg']")

    assert svg_root.attrib["role"] == "img"
    assert svg_root.find(f"{SVG_NS}title") is not None
    assert svg_root.find(f"{SVG_NS}desc") is not None
    assert background is not None and background.attrib["fill"] == "none"
    styles = " ".join(element.text or "" for element in svg_root.iter(f"{SVG_NS}style"))
    assert "prefers-color-scheme: light" in styles
    assert "prefers-color-scheme: dark" in styles
    marker = svg_root.find(f".//{SVG_NS}marker[@id='arrow']")
    assert marker is not None and marker.attrib["refX"] == "10"


def test_runtime_svg_has_all_entrypoint_flows() -> None:
    svg_root = _runtime_svg()
    flows = _runtime_flows(svg_root)

    assert len(list(flows)) == 12
    assert list(svg_root).index(flows) == len(list(svg_root)) - 1
    path_values = [path.attrib["d"] for path in flows]
    assert path_values == list(EXPECTED_CONNECTORS)
    assert list(flows)[0].attrib.get("class") == "bus"
    assert all(path.attrib.get("class") == "arrow" for path in list(flows)[1:])
    assert not {
        "M210 180H825",
        "M400 180H825",
        "M590 180H825",
        "M610 285H825",
        "M805 285H825",
        "M705 590H845",
        "M705 590H825V548",
    }.intersection(path_values)


def test_runtime_svg_box_geometry_is_aligned() -> None:
    rectangles = list(_runtime_svg().iter(f"{SVG_NS}rect"))
    mcp_box = _rect_where(rectangles, x="415", y="245")
    assert mcp_box.attrib["width"] == "365"
    assert int(mcp_box.attrib["x"]) + int(mcp_box.attrib["width"]) == 780
    graph_os_box = _rect_where(rectangles, x="825", width="450")
    assert graph_os_box.attrib["y"] == "135"
    assert graph_os_box.attrib["height"] == "170"
    lower_boxes = [
        rect for rect in rectangles if rect.attrib.get("y") in {"485", "520"}
    ]
    assert {
        int(rect.attrib["y"]) + int(rect.attrib["height"]) for rect in lower_boxes
    } == {610, 645}


def test_runtime_svg_connectors_touch_box_borders() -> None:
    bus = "M120 90V100H692"
    bus_start, bus_end = EXPECTED_CONNECTORS[bus]
    assert _on_any_box_border(bus_start)
    assert bus_end == (692, 100)
    for path, (start, end) in EXPECTED_CONNECTORS.items():
        if path == bus:
            continue
        if start[1] == 100:
            assert start[0] in (120, 310, 500, 692)
        else:
            assert _on_any_box_border(start)
        assert _on_any_box_border(end)


def test_runtime_svg_labels_match_mermaid_source() -> None:
    svg_root = _runtime_svg()
    text = " ".join(element.text or "" for element in svg_root.iter(f"{SVG_NS}text"))
    mermaid = RUNTIME_DIAGRAM.with_suffix(".mmd").read_text(encoding="utf-8")

    for label in (
        "Arbora web UI",
        "browser experience",
        "Agent Terminal UI",
        "Geniusbot",
        "Messaging",
        "Arbora",
        "governed runtime gateway",
        "Agent Utilities",
        "agent control plane",
        "Epistemic Graph",
        "Agent Connector SDK",
        "typed source integration",
    ):
        assert label in text
        assert label.split()[0] in mermaid
    for edge in (
        "WebUI --> Arbora",
        "TUI -->|REST| Arbora",
        "Arbora --> AU",
        "AU --> EG",
        "Sources --> SDK",
        "SDK --> EG",
    ):
        assert edge in mermaid
