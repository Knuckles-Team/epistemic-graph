//! **KML — Keyhole Markup Language** reader + writer (CONCEPT:EG-KG.domains.geo-formats).
//!
//! KML is the XML map format of Google Earth / Google Maps / My Maps and a common export from
//! GIS tools. This module round-trips the geometry-bearing subset — `<Placemark>` wrapping a
//! `<Point>` / `<LineString>` / `<Polygon>` (or a `<MultiGeometry>` of them) — to and from
//! eg-geo [`Geometry`]s, preserving each placemark's `<name>`.
//!
//! It is a *hand-rolled* pure-Rust codec — NO `quick-xml`/`serde-xml` dep — matching the
//! [`crate::gpx`] house style, but going further: KML puts coordinates in element **text**
//! (`<coordinates>lon,lat,alt …</coordinates>`), not attributes, so this ships a tiny
//! well-formed-XML DOM ([`parse_xml`]) that the reader walks. The writer emits compact,
//! valid KML 2.2.
//!
//! Coordinate convention: KML tuples are `lon,lat[,alt]` (KML §16.1) and eg-geo geometries are
//! `(x = lon, y = lat)`, so longitude/latitude map straight across; the optional altitude is
//! read and discarded (eg-geo is planar 2-D). Polygon rings use
//! `<outerBoundaryIs><LinearRing>…` for the exterior and `<innerBoundaryIs><LinearRing>…` for
//! each hole (CONCEPT:EG-KG.domains.geometry-collections interior rings).

use crate::geometry::{Geometry, LineString, Point, Polygon};

// ── public value model ───────────────────────────────────────────────────────────────

/// One KML `<Placemark>`: an optional name and geometry (CONCEPT:EG-KG.domains.geo-formats).
#[derive(Clone, Debug, PartialEq)]
pub struct Placemark {
    pub name: Option<String>,
    pub geometry: Option<Geometry>,
}

/// A parsed KML document — its placemarks (CONCEPT:EG-KG.domains.geo-formats).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Kml {
    pub placemarks: Vec<Placemark>,
}

impl Kml {
    /// Flatten all placemark geometries into one [`Geometry::GeometryCollection`]
    /// (CONCEPT:EG-KG.domains.geo-formats); placemarks without a geometry are skipped.
    pub fn to_geometry(&self) -> Geometry {
        Geometry::GeometryCollection(
            self.placemarks
                .iter()
                .filter_map(|p| p.geometry.clone())
                .collect(),
        )
    }
}

// ── reader ─────────────────────────────────────────────────────────────────────────

/// Parse KML XML text into a [`Kml`] (CONCEPT:EG-KG.domains.geo-formats). Finds every `<Placemark>` (at any depth
/// under `<Document>`/`<Folder>`), extracting its `<name>` and first geometry.
pub fn read_kml(xml: &str) -> Result<Kml, String> {
    let root = parse_xml(xml)?;
    let mut placemarks = Vec::new();
    collect_placemarks(&root, &mut placemarks)?;
    Ok(Kml { placemarks })
}

fn collect_placemarks(node: &XmlNode, out: &mut Vec<Placemark>) -> Result<(), String> {
    if node.name.eq_ignore_ascii_case("Placemark") {
        let name = node
            .child("name")
            .map(|n| n.text.trim().to_string())
            .filter(|s| !s.is_empty());
        let geometry = parse_placemark_geometry(node)?;
        out.push(Placemark { name, geometry });
        return Ok(()); // don't descend into a placemark's own sub-elements
    }
    for c in &node.children {
        collect_placemarks(c, out)?;
    }
    Ok(())
}

/// Pull the first geometry element out of a `<Placemark>` (or `<MultiGeometry>`).
fn parse_placemark_geometry(node: &XmlNode) -> Result<Option<Geometry>, String> {
    for c in &node.children {
        if let Some(g) = parse_geometry_node(c)? {
            return Ok(Some(g));
        }
    }
    Ok(None)
}

/// Decode a single KML geometry element into a [`Geometry`], if `node` is one.
fn parse_geometry_node(node: &XmlNode) -> Result<Option<Geometry>, String> {
    let g = match () {
        _ if node.name.eq_ignore_ascii_case("Point") => {
            let pts = coords_of(node)?;
            let p = *pts
                .first()
                .ok_or_else(|| "KML: <Point> has no coordinates".to_string())?;
            Some(Geometry::Point(p))
        }
        _ if node.name.eq_ignore_ascii_case("LineString") => {
            Some(Geometry::LineString(LineString::new(coords_of(node)?)))
        }
        _ if node.name.eq_ignore_ascii_case("LinearRing") => {
            // A bare LinearRing (rare at Placemark level) → a closed LineString.
            Some(Geometry::LineString(LineString::new(coords_of(node)?)))
        }
        _ if node.name.eq_ignore_ascii_case("Polygon") => {
            Some(Geometry::Polygon(parse_polygon(node)?))
        }
        _ if node.name.eq_ignore_ascii_case("MultiGeometry") => {
            let mut parts = Vec::new();
            for c in &node.children {
                if let Some(g) = parse_geometry_node(c)? {
                    parts.push(g);
                }
            }
            Some(collapse_multigeometry(parts))
        }
        _ => None,
    };
    Ok(g)
}

/// Prefer a homogeneous Multi* over a mixed GeometryCollection when a `<MultiGeometry>` holds
/// only points / only lines / only polygons — the natural KML → eg-geo mapping.
fn collapse_multigeometry(parts: Vec<Geometry>) -> Geometry {
    if !parts.is_empty() && parts.iter().all(|g| matches!(g, Geometry::Point(_))) {
        return Geometry::MultiPoint(
            parts
                .into_iter()
                .map(|g| match g {
                    Geometry::Point(p) => p,
                    _ => unreachable!(),
                })
                .collect(),
        );
    }
    if !parts.is_empty() && parts.iter().all(|g| matches!(g, Geometry::LineString(_))) {
        return Geometry::MultiLineString(
            parts
                .into_iter()
                .map(|g| match g {
                    Geometry::LineString(l) => l,
                    _ => unreachable!(),
                })
                .collect(),
        );
    }
    if !parts.is_empty() && parts.iter().all(|g| matches!(g, Geometry::Polygon(_))) {
        return Geometry::MultiPolygon(
            parts
                .into_iter()
                .map(|g| match g {
                    Geometry::Polygon(p) => p,
                    _ => unreachable!(),
                })
                .collect(),
        );
    }
    Geometry::GeometryCollection(parts)
}

/// Parse a `<Polygon>`: `<outerBoundaryIs><LinearRing><coordinates>` exterior plus any
/// `<innerBoundaryIs>` holes (CONCEPT:EG-KG.domains.geometry-collections).
fn parse_polygon(node: &XmlNode) -> Result<Polygon, String> {
    let outer = node
        .child("outerBoundaryIs")
        .ok_or_else(|| "KML: <Polygon> missing <outerBoundaryIs>".to_string())?;
    let exterior = LineString::new(coords_of(outer)?);
    let mut interiors = Vec::new();
    for c in &node.children {
        if c.name.eq_ignore_ascii_case("innerBoundaryIs") {
            interiors.push(LineString::new(coords_of(c)?));
        }
    }
    Ok(Polygon::new(exterior, interiors))
}

/// Find the first `<coordinates>` element in `node`'s subtree and parse its text.
fn coords_of(node: &XmlNode) -> Result<Vec<Point>, String> {
    let text = find_coordinates_text(node)
        .ok_or_else(|| format!("KML: <{}> missing <coordinates>", node.name))?;
    parse_coordinates(text)
}

fn find_coordinates_text(node: &XmlNode) -> Option<&str> {
    if node.name.eq_ignore_ascii_case("coordinates") {
        return Some(node.text.as_str());
    }
    for c in &node.children {
        if let Some(t) = find_coordinates_text(c) {
            return Some(t);
        }
    }
    None
}

/// Parse a KML `<coordinates>` blob: whitespace-separated `lon,lat[,alt]` tuples
/// (CONCEPT:EG-KG.domains.geo-formats). Altitude is read and discarded (eg-geo is 2-D).
fn parse_coordinates(text: &str) -> Result<Vec<Point>, String> {
    let mut pts = Vec::new();
    for tuple in text.split_whitespace() {
        let mut it = tuple.split(',');
        let lon = it
            .next()
            .ok_or_else(|| "KML: empty coordinate tuple".to_string())?;
        let lat = it
            .next()
            .ok_or_else(|| format!("KML: coordinate {tuple:?} missing latitude"))?;
        let x: f64 = lon
            .parse()
            .map_err(|_| format!("KML: bad longitude {lon:?}"))?;
        let y: f64 = lat
            .parse()
            .map_err(|_| format!("KML: bad latitude {lat:?}"))?;
        pts.push(Point::new(x, y));
    }
    Ok(pts)
}

// ── writer ─────────────────────────────────────────────────────────────────────────

/// Serialise a [`Kml`] to a KML 2.2 document string (CONCEPT:EG-KG.domains.geo-formats).
pub fn write_kml(kml: &Kml) -> String {
    let mut s = String::new();
    s.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    s.push_str("<kml xmlns=\"http://www.opengis.net/kml/2.2\"><Document>");
    for pm in &kml.placemarks {
        s.push_str("<Placemark>");
        if let Some(name) = &pm.name {
            s.push_str("<name>");
            push_escaped(&mut s, name);
            s.push_str("</name>");
        }
        if let Some(g) = &pm.geometry {
            write_geometry(&mut s, g);
        }
        s.push_str("</Placemark>");
    }
    s.push_str("</Document></kml>");
    s
}

fn write_geometry(s: &mut String, g: &Geometry) {
    match g {
        Geometry::Point(p) => {
            s.push_str("<Point><coordinates>");
            push_coord(s, p);
            s.push_str("</coordinates></Point>");
        }
        Geometry::LineString(l) => {
            s.push_str("<LineString><coordinates>");
            push_line(s, l);
            s.push_str("</coordinates></LineString>");
        }
        Geometry::Polygon(pg) => write_polygon(s, pg),
        Geometry::MultiPoint(ps) => write_multi_geometry(s, ps.iter().map(|p| Geometry::Point(*p))),
        Geometry::MultiLineString(ls) => {
            write_multi_geometry(s, ls.iter().map(|l| Geometry::LineString(l.clone())))
        }
        Geometry::MultiPolygon(pgs) => {
            write_multi_geometry(s, pgs.iter().map(|pg| Geometry::Polygon(pg.clone())))
        }
        Geometry::GeometryCollection(gs) => {
            s.push_str("<MultiGeometry>");
            for g in gs {
                write_geometry(s, g);
            }
            s.push_str("</MultiGeometry>");
        }
    }
}

/// Wrap a KML `<MultiGeometry>` around each member's own geometry element — the shape
/// every multi-part KML geometry serialises to. Members are produced lazily.
fn write_multi_geometry(s: &mut String, members: impl Iterator<Item = Geometry>) {
    s.push_str("<MultiGeometry>");
    for g in members {
        write_geometry(s, &g);
    }
    s.push_str("</MultiGeometry>");
}

fn write_polygon(s: &mut String, pg: &Polygon) {
    s.push_str("<Polygon><outerBoundaryIs><LinearRing><coordinates>");
    push_line(s, &pg.exterior);
    s.push_str("</coordinates></LinearRing></outerBoundaryIs>");
    for hole in &pg.interiors {
        s.push_str("<innerBoundaryIs><LinearRing><coordinates>");
        push_line(s, hole);
        s.push_str("</coordinates></LinearRing></innerBoundaryIs>");
    }
    s.push_str("</Polygon>");
}

fn push_coord(s: &mut String, p: &Point) {
    s.push_str(&format!("{},{}", p.x, p.y));
}

fn push_line(s: &mut String, l: &LineString) {
    for (i, p) in l.points.iter().enumerate() {
        if i > 0 {
            s.push(' ');
        }
        push_coord(s, p);
    }
}

fn push_escaped(s: &mut String, text: &str) {
    for c in text.chars() {
        match c {
            '&' => s.push_str("&amp;"),
            '<' => s.push_str("&lt;"),
            '>' => s.push_str("&gt;"),
            other => s.push(other),
        }
    }
}

// ── a minimal well-formed-XML DOM (CONCEPT:EG-KG.domains.geo-formats) ───────────────────────────────────

/// A tiny XML element node — enough for KML: element name, concatenated direct text, and child
/// elements. Attributes are parsed but dropped (KML geometry needs none).
#[derive(Clone, Debug, Default)]
pub struct XmlNode {
    pub name: String,
    pub text: String,
    pub children: Vec<XmlNode>,
}

impl XmlNode {
    /// The first direct child whose (case-insensitive) element name is `name`.
    pub fn child(&self, name: &str) -> Option<&XmlNode> {
        self.children
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(name))
    }
}

/// Parse a well-formed XML string into a single root [`XmlNode`] (CONCEPT:EG-KG.domains.geo-formats). Handles open
/// / close / self-closing tags, text (with `&amp;`/`&lt;`/`&gt;`/`&quot;`/`&apos;` entities and
/// `<![CDATA[…]]>`), and skips the `<?xml?>` prolog, comments and `<!…>` declarations. Returns
/// the outermost element (e.g. `<kml>`).
pub fn parse_xml(xml: &str) -> Result<XmlNode, String> {
    // A synthetic root collects the top-level element(s).
    let mut stack: Vec<XmlNode> = vec![XmlNode::default()];
    let mut i = 0usize;
    while i < xml.len() {
        let (unit, next) = lex_markup(xml, i)?;
        i = next;
        match unit {
            Markup::Ignored => {}
            Markup::Text(text) => {
                if let Some(top) = stack.last_mut() {
                    top.text.push_str(&text);
                }
            }
            Markup::Close(name) => close_element(&mut stack, name)?,
            Markup::Open { inner, self_close } => open_element(&mut stack, inner, self_close)?,
        }
    }
    let mut root = stack.pop().ok_or_else(|| "XML: no root".to_string())?;
    if !stack.is_empty() {
        return Err("XML: unclosed element(s)".to_string());
    }
    // Multiple top-level elements are tolerated; KML has a single <kml> root — take the last
    // (outermost) collected element as the document root.
    root.children
        .pop()
        .ok_or_else(|| "XML: document has no root element".to_string())
}

/// One lexical unit of an XML document.
enum Markup<'a> {
    /// A comment, XML prolog or `<!…>` declaration — contributes nothing to the tree.
    Ignored,
    /// Character data belonging to the innermost open element, entity-decoded (CDATA is
    /// taken verbatim, as the XML spec requires).
    Text(String),
    /// A closing tag `</name>`, carrying the name.
    Close(&'a str),
    /// An opening tag, carrying its inner text (name plus any attributes) with the
    /// self-closing slash already trimmed.
    Open { inner: &'a str, self_close: bool },
}

/// Lex the one markup unit starting at byte `i`, returning it and the index just past it.
fn lex_markup(xml: &str, i: usize) -> Result<(Markup<'_>, usize), String> {
    if !xml[i..].starts_with('<') {
        // Text run up to the next '<'.
        let next = xml[i..].find('<').map(|p| p + i).unwrap_or(xml.len());
        return Ok((Markup::Text(decode_entities(&xml[i..next])), next));
    }
    if xml[i..].starts_with("<!--") {
        let end = xml[i..]
            .find("-->")
            .ok_or_else(|| "XML: unterminated comment".to_string())?;
        return Ok((Markup::Ignored, i + end + 3));
    }
    if xml[i..].starts_with("<![CDATA[") {
        let end = xml[i + 9..]
            .find("]]>")
            .ok_or_else(|| "XML: unterminated CDATA".to_string())?;
        return Ok((
            Markup::Text(xml[i + 9..i + 9 + end].to_string()),
            i + 9 + end + 3,
        ));
    }
    let gt = xml[i..]
        .find('>')
        .ok_or_else(|| "XML: unterminated tag".to_string())?
        + i;
    let raw = &xml[i + 1..gt];
    let next = gt + 1;
    if raw.starts_with('?') || raw.starts_with('!') {
        return Ok((Markup::Ignored, next)); // prolog / doctype
    }
    Ok(match raw.strip_prefix('/') {
        Some(close) => (Markup::Close(close.trim()), next),
        None => (
            Markup::Open {
                inner: raw.trim_end_matches('/').trim(),
                self_close: raw.ends_with('/'),
            },
            next,
        ),
    })
}

/// Close the innermost open element: pop it, require its name to match `name`
/// (case-insensitively), and attach it to its parent.
fn close_element(stack: &mut Vec<XmlNode>, name: &str) -> Result<(), String> {
    let node = stack
        .pop()
        .ok_or_else(|| "XML: close tag without open".to_string())?;
    if !node.name.eq_ignore_ascii_case(name) {
        return Err(format!(
            "XML: mismatched close </{}> for <{}>",
            name, node.name
        ));
    }
    stack
        .last_mut()
        .ok_or_else(|| "XML: close tag underflow".to_string())?
        .children
        .push(node);
    Ok(())
}

/// Start an element from an opening tag's inner text. A self-closing tag is attached to
/// its parent at once; any other becomes the new innermost open element.
fn open_element(stack: &mut Vec<XmlNode>, inner: &str, self_close: bool) -> Result<(), String> {
    let name = inner
        .split(|c: char| c.is_whitespace())
        .next()
        .unwrap_or("")
        .to_string();
    if name.is_empty() {
        return Err("XML: empty tag name".to_string());
    }
    let node = XmlNode {
        name,
        ..Default::default()
    };
    if self_close {
        stack
            .last_mut()
            .ok_or_else(|| "XML: self-closing tag without an open element".to_string())?
            .children
            .push(node);
    } else {
        stack.push(node);
    }
    Ok(())
}

/// Decode the five predefined XML entities (all KML needs).
fn decode_entities(s: &str) -> String {
    if !s.contains('&') {
        return s.to_string();
    }
    s.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&amp;", "&")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn eg306_kml_reads_point_line_polygon() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://www.opengis.net/kml/2.2"><Document>
  <Placemark><name>Paris</name>
    <Point><coordinates>2.3522,48.8566,0</coordinates></Point>
  </Placemark>
  <Placemark><name>route</name>
    <LineString><coordinates>-0.1278,51.5074 13.4050,52.5200</coordinates></LineString>
  </Placemark>
  <Placemark><name>area</name>
    <Polygon>
      <outerBoundaryIs><LinearRing><coordinates>
        0,0 10,0 10,10 0,10 0,0
      </coordinates></LinearRing></outerBoundaryIs>
      <innerBoundaryIs><LinearRing><coordinates>
        3,3 7,3 7,7 3,7 3,3
      </coordinates></LinearRing></innerBoundaryIs>
    </Polygon>
  </Placemark>
</Document></kml>"#;
        let kml = read_kml(xml).expect("parse KML");
        assert_eq!(kml.placemarks.len(), 3);
        assert_eq!(kml.placemarks[0].name.as_deref(), Some("Paris"));
        assert_eq!(
            kml.placemarks[0].geometry,
            Some(Geometry::Point(Point::new(2.3522, 48.8566)))
        );
        match &kml.placemarks[1].geometry {
            Some(Geometry::LineString(l)) => {
                assert_eq!(l.points.len(), 2);
                assert_eq!(l.points[0], Point::new(-0.1278, 51.5074));
            }
            other => panic!("expected LineString, got {other:?}"),
        }
        match &kml.placemarks[2].geometry {
            Some(Geometry::Polygon(pg)) => {
                assert_eq!(pg.exterior.points.len(), 5);
                assert_eq!(pg.interiors.len(), 1);
                assert_eq!(pg.interiors[0].points[0], Point::new(3.0, 3.0));
            }
            other => panic!("expected Polygon, got {other:?}"),
        }
    }

    fn round_trip(g: Geometry, name: &str) {
        let kml = Kml {
            placemarks: vec![Placemark {
                name: Some(name.to_string()),
                geometry: Some(g.clone()),
            }],
        };
        let s = write_kml(&kml);
        let back = read_kml(&s).expect("re-parse written KML");
        assert_eq!(back.placemarks.len(), 1, "round-trip lost placemark: {s}");
        assert_eq!(back.placemarks[0].name.as_deref(), Some(name));
        assert_eq!(
            back.placemarks[0].geometry,
            Some(g),
            "KML geometry round-trip mismatch: {s}"
        );
    }

    #[test]
    fn eg306_kml_round_trip_all_geometry_types() {
        round_trip(Geometry::Point(Point::new(30.0, 10.0)), "pt");
        round_trip(
            Geometry::LineString(LineString::new(vec![
                Point::new(30.0, 10.0),
                Point::new(10.0, 30.0),
                Point::new(40.0, 40.0),
            ])),
            "line",
        );
        round_trip(
            Geometry::Polygon(Polygon::new(
                LineString::new(vec![
                    Point::new(0.0, 0.0),
                    Point::new(10.0, 0.0),
                    Point::new(10.0, 10.0),
                    Point::new(0.0, 10.0),
                    Point::new(0.0, 0.0),
                ]),
                vec![LineString::new(vec![
                    Point::new(3.0, 3.0),
                    Point::new(7.0, 3.0),
                    Point::new(7.0, 7.0),
                    Point::new(3.0, 7.0),
                    Point::new(3.0, 3.0),
                ])],
            )),
            "poly-hole",
        );
        round_trip(
            Geometry::MultiPoint(vec![Point::new(1.0, 2.0), Point::new(3.0, 4.0)]),
            "mpt",
        );
        round_trip(
            Geometry::MultiLineString(vec![
                LineString::new(vec![Point::new(0.0, 0.0), Point::new(1.0, 1.0)]),
                LineString::new(vec![Point::new(2.0, 2.0), Point::new(3.0, 3.0)]),
            ]),
            "mline",
        );
        round_trip(
            Geometry::MultiPolygon(vec![Polygon::new(
                LineString::new(vec![
                    Point::new(0.0, 0.0),
                    Point::new(1.0, 0.0),
                    Point::new(1.0, 1.0),
                    Point::new(0.0, 0.0),
                ]),
                Vec::new(),
            )]),
            "mpoly",
        );
    }

    #[test]
    fn eg306_kml_multigeometry_reads_as_multipoint() {
        let xml = r#"<kml><Document><Placemark>
            <MultiGeometry>
              <Point><coordinates>1,2</coordinates></Point>
              <Point><coordinates>3,4</coordinates></Point>
            </MultiGeometry>
        </Placemark></Document></kml>"#;
        let kml = read_kml(xml).unwrap();
        assert_eq!(
            kml.placemarks[0].geometry,
            Some(Geometry::MultiPoint(vec![
                Point::new(1.0, 2.0),
                Point::new(3.0, 4.0)
            ]))
        );
    }

    #[test]
    fn eg306_kml_to_geometry_collection() {
        let xml = r#"<kml><Placemark><Point><coordinates>1,1</coordinates></Point></Placemark>
            <Placemark><Point><coordinates>2,2</coordinates></Point></Placemark></kml>"#;
        let kml = read_kml(xml).unwrap();
        match kml.to_geometry() {
            Geometry::GeometryCollection(parts) => assert_eq!(parts.len(), 2),
            other => panic!("expected GeometryCollection, got {other:?}"),
        }
    }

    #[test]
    fn eg306_kml_name_entity_escaping_round_trips() {
        round_trip(Geometry::Point(Point::new(0.0, 0.0)), "A & B <tag>");
    }

    #[test]
    fn eg306_kml_bad_coordinate_errors() {
        let xml = r#"<kml><Placemark><Point><coordinates>notanumber,1</coordinates></Point></Placemark></kml>"#;
        assert!(read_kml(xml).is_err());
    }

    #[test]
    fn eg306_xml_parser_handles_comments_and_cdata() {
        let xml = r#"<root><!-- comment --><a>x</a><b><![CDATA[y<z]]></b></root>"#;
        let node = parse_xml(xml).unwrap();
        assert_eq!(node.name, "root");
        assert_eq!(node.child("a").unwrap().text, "x");
        assert_eq!(node.child("b").unwrap().text, "y<z");
    }
}
