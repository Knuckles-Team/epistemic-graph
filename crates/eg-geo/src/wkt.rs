//! A hand-written WKT / EWKT (Well-Known Text) codec for the full geometry model:
//! `POINT` / `LINESTRING` / `POLYGON` (with interior rings) from EG-083, extended
//! (CONCEPT:EG-KG.domains.geometry-collections) to `MULTIPOINT` / `MULTILINESTRING` / `MULTIPOLYGON` /
//! `GEOMETRYCOLLECTION`. No dependency — a recursive-descent parser + a `Display`-style
//! serializer, so the crate stays pure-Rust and Pi-lean.
//!
//! An **EWKT** `SRID=<n>;` prefix is tolerated (and ignored — the SRID/CRS lives in the
//! EG-255 tag, not the geometry value).
//!
//! Grammar (case-insensitive keyword, whitespace-tolerant):
//! ```text
//! POINT (x y)
//! LINESTRING (x y, x y, ...)
//! POLYGON ((ext ring), (hole ring), ...)
//! MULTIPOINT (x y, x y)   |   MULTIPOINT ((x y), (x y))
//! MULTILINESTRING ((x y, ...), (x y, ...))
//! MULTIPOLYGON (((ring), (hole)), ((ring)))
//! GEOMETRYCOLLECTION (POINT (x y), LINESTRING (...), ...)
//! ```

use crate::geometry::{Geometry, LineString, Point, Polygon};

/// Parse a WKT/EWKT string into a [`Geometry`]. Returns `Err(msg)` on malformed input.
/// A leading EWKT `SRID=<n>;` prefix is tolerated and ignored — use [`parse_with_srid`]
/// to also recover the SRID/CRS tag (CONCEPT:EG-KG.domains.coordinate-reference-system).
pub fn parse(input: &str) -> Result<Geometry, String> {
    let s = strip_srid(input.trim());
    parse_geometry(s.trim())
}

/// Parse a WKT/EWKT string into a `(Option<srid>, Geometry)` pair (CONCEPT:EG-KG.domains.coordinate-reference-system): the
/// EWKT `SRID=<n>;` prefix EG-257 already tolerated is captured here as the geometry's
/// CRS tag (`None` when absent). The geometry value itself is identical to [`parse`]'s.
pub fn parse_with_srid(input: &str) -> Result<(Option<u32>, Geometry), String> {
    let trimmed = input.trim();
    let (srid, rest) = split_srid(trimmed);
    Ok((srid, parse_geometry(rest.trim())?))
}

/// Drop a leading EWKT `SRID=<n>;` prefix (case-insensitive), returning the remainder.
fn strip_srid(s: &str) -> &str {
    split_srid(s).1
}

/// Split a leading EWKT `SRID=<n>;` prefix (case-insensitive) into the parsed SRID (when
/// the digits parse) and the remaining geometry text.
fn split_srid(s: &str) -> (Option<u32>, &str) {
    if s.len() >= 5 && s[..5].eq_ignore_ascii_case("SRID=") {
        if let Some(semi) = s.find(';') {
            let srid = s[5..semi].trim().parse::<u32>().ok();
            return (srid, s[semi + 1..].trim_start());
        }
    }
    (None, s)
}

/// Parse one geometry (the recursive entry point used by `GEOMETRYCOLLECTION`).
fn parse_geometry(s: &str) -> Result<Geometry, String> {
    let s = s.trim();
    let upper = s.to_ascii_uppercase();
    // Longest keywords first so MULTIPOINT isn't shadowed by POINT, etc.
    if let Some(rest) = upper.strip_prefix("MULTIPOLYGON") {
        let body = outer_body(after(s, rest))?;
        let polys = split_top_level(&body)
            .into_iter()
            .map(|p| parse_polygon_body(&strip_outer_parens(p.trim())?))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Geometry::MultiPolygon(polys))
    } else if let Some(rest) = upper.strip_prefix("MULTILINESTRING") {
        let body = outer_body(after(s, rest))?;
        let lines = split_top_level(&body)
            .into_iter()
            .map(|l| {
                Ok(LineString::new(parse_coord_list(&strip_outer_parens(
                    l.trim(),
                )?)?))
            })
            .collect::<Result<Vec<_>, String>>()?;
        Ok(Geometry::MultiLineString(lines))
    } else if let Some(rest) = upper.strip_prefix("MULTIPOINT") {
        let body = outer_body(after(s, rest))?;
        // Tolerate both `MULTIPOINT (x y, x y)` and `MULTIPOINT ((x y), (x y))`.
        let mut pts = Vec::new();
        for part in split_top_level(&body) {
            let part = part.trim();
            let coord = if part.starts_with('(') {
                strip_outer_parens(part)?
            } else {
                part.to_string()
            };
            let one = parse_coord_list(&coord)?;
            if one.len() != 1 {
                return Err(format!("MULTIPOINT part expects 1 coordinate: '{part}'"));
            }
            pts.push(one[0]);
        }
        if pts.is_empty() {
            return Err("MULTIPOINT expects >= 1 coordinate".into());
        }
        Ok(Geometry::MultiPoint(pts))
    } else if let Some(rest) = upper.strip_prefix("GEOMETRYCOLLECTION") {
        let body = outer_body(after(s, rest))?;
        let geoms = split_top_level(&body)
            .into_iter()
            .map(|g| parse_geometry(g.trim()))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Geometry::GeometryCollection(geoms))
    } else if let Some(rest) = upper.strip_prefix("POLYGON") {
        let body = outer_body(after(s, rest))?;
        Ok(Geometry::Polygon(parse_polygon_body(&body)?))
    } else if let Some(rest) = upper.strip_prefix("LINESTRING") {
        let body = outer_body(after(s, rest))?;
        let pts = parse_coord_list(&body)?;
        if pts.len() < 2 {
            return Err("LINESTRING expects >= 2 coordinates".into());
        }
        Ok(Geometry::LineString(LineString::new(pts)))
    } else if let Some(rest) = upper.strip_prefix("POINT") {
        let body = outer_body(after(s, rest))?;
        let pts = parse_coord_list(&body)?;
        if pts.len() != 1 {
            return Err(format!("POINT expects 1 coordinate, got {}", pts.len()));
        }
        Ok(Geometry::Point(pts[0]))
    } else {
        Err(format!("unsupported or malformed WKT: {s}"))
    }
}

/// Parse the inside of a `POLYGON (...)` — one or more parenthesized rings, the first
/// being the exterior and the rest interior rings/holes (CONCEPT:EG-KG.domains.geometry-collections).
fn parse_polygon_body(body: &str) -> Result<Polygon, String> {
    let rings = split_top_level(body);
    if rings.is_empty() {
        return Err("POLYGON expects >= 1 ring".into());
    }
    let mut parsed: Vec<LineString> = Vec::new();
    for ring in rings {
        let inner = strip_outer_parens(ring.trim())?;
        let pts = parse_coord_list(&inner)?;
        if pts.len() < 3 {
            return Err("POLYGON ring expects >= 3 coordinates".into());
        }
        parsed.push(LineString::new(pts));
    }
    let exterior = parsed.remove(0);
    Ok(Polygon::with_interiors(exterior, parsed))
}

/// The substring of the ORIGINAL (mixed-case) `s` corresponding to the uppercased
/// `rest` (i.e. everything after the matched keyword), so numeric parsing sees the
/// original text.
fn after<'a>(s: &'a str, rest: &str) -> &'a str {
    &s[s.len() - rest.len()..]
}

/// Extract the text between the OUTERMOST matched parentheses of `s`.
fn outer_body(s: &str) -> Result<String, String> {
    strip_outer_parens(s.trim())
}

/// Strip one balanced outer `(...)` pair from `s`, returning the inside.
fn strip_outer_parens(s: &str) -> Result<String, String> {
    let start = s
        .find('(')
        .ok_or_else(|| format!("WKT: missing '(' in {s}"))?;
    let end = s
        .rfind(')')
        .ok_or_else(|| format!("WKT: missing ')' in {s}"))?;
    if end <= start {
        return Err(format!("WKT: unbalanced parens in {s}"));
    }
    Ok(s[start + 1..end].to_string())
}

/// Split `body` on top-level commas — commas nested inside parentheses are ignored.
fn split_top_level(body: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut depth = 0i32;
    let mut cur = String::new();
    for ch in body.chars() {
        match ch {
            '(' => {
                depth += 1;
                cur.push(ch);
            }
            ')' => {
                depth -= 1;
                cur.push(ch);
            }
            ',' if depth == 0 => {
                if !cur.trim().is_empty() {
                    parts.push(cur.trim().to_string());
                }
                cur.clear();
            }
            _ => cur.push(ch),
        }
    }
    if !cur.trim().is_empty() {
        parts.push(cur.trim().to_string());
    }
    parts
}

/// Parse a comma-separated list of `x y` coordinate pairs (no nested parens).
fn parse_coord_list(body: &str) -> Result<Vec<Point>, String> {
    let mut out = Vec::new();
    for pair in body.split(',') {
        let pair = pair.trim();
        if pair.is_empty() {
            continue;
        }
        let mut nums = pair.split_whitespace();
        let x = nums
            .next()
            .ok_or_else(|| format!("WKT: missing x in '{pair}'"))?
            .parse::<f64>()
            .map_err(|e| format!("WKT: bad x '{pair}': {e}"))?;
        let y = nums
            .next()
            .ok_or_else(|| format!("WKT: missing y in '{pair}'"))?
            .parse::<f64>()
            .map_err(|e| format!("WKT: bad y '{pair}': {e}"))?;
        out.push(Point::new(x, y));
    }
    if out.is_empty() {
        return Err("WKT: empty coordinate list".into());
    }
    Ok(out)
}

/// Serialize a [`Geometry`] back to canonical WKT.
pub fn to_wkt(g: &Geometry) -> String {
    match g {
        Geometry::Point(p) => format!("POINT ({})", coord(p)),
        Geometry::LineString(l) => format!("LINESTRING ({})", coords(&l.points)),
        Geometry::Polygon(pg) => format!("POLYGON ({})", polygon_rings(pg)),
        Geometry::MultiPoint(ps) => {
            let inner = ps.iter().map(coord).collect::<Vec<_>>().join(", ");
            format!("MULTIPOINT ({inner})")
        }
        Geometry::MultiLineString(ls) => {
            let inner = ls
                .iter()
                .map(|l| format!("({})", coords(&l.points)))
                .collect::<Vec<_>>()
                .join(", ");
            format!("MULTILINESTRING ({inner})")
        }
        Geometry::MultiPolygon(pgs) => {
            let inner = pgs
                .iter()
                .map(|pg| format!("({})", polygon_rings(pg)))
                .collect::<Vec<_>>()
                .join(", ");
            format!("MULTIPOLYGON ({inner})")
        }
        Geometry::GeometryCollection(gs) => {
            let inner = gs.iter().map(to_wkt).collect::<Vec<_>>().join(", ");
            format!("GEOMETRYCOLLECTION ({inner})")
        }
    }
}

/// The `(ext), (hole), ...` ring group inside a `POLYGON (...)`.
fn polygon_rings(pg: &Polygon) -> String {
    std::iter::once(&pg.exterior)
        .chain(pg.interiors.iter())
        .map(|r| format!("({})", coords(&r.points)))
        .collect::<Vec<_>>()
        .join(", ")
}

fn coord(p: &Point) -> String {
    format!("{} {}", fmt_num(p.x), fmt_num(p.y))
}

fn coords(pts: &[Point]) -> String {
    pts.iter().map(coord).collect::<Vec<_>>().join(", ")
}

/// Format a coordinate without a trailing `.0` for whole numbers (canonical WKT).
fn fmt_num(n: f64) -> String {
    if n.fract() == 0.0 && n.is_finite() {
        format!("{}", n as i64)
    } else {
        format!("{n}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn point_round_trip() {
        let g = parse("POINT (30 10)").unwrap();
        assert_eq!(g, Geometry::Point(Point::new(30.0, 10.0)));
        assert_eq!(to_wkt(&g), "POINT (30 10)");
    }

    #[test]
    fn linestring_round_trip() {
        let g = parse("LINESTRING (30 10, 10 30, 40 40)").unwrap();
        assert_eq!(to_wkt(&g), "LINESTRING (30 10, 10 30, 40 40)");
        assert_eq!(parse(&to_wkt(&g)).unwrap(), g);
    }

    #[test]
    fn polygon_round_trip() {
        let src = "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))";
        let g = parse(src).unwrap();
        assert_eq!(to_wkt(&g), src);
        assert_eq!(parse(&to_wkt(&g)).unwrap(), g);
    }

    #[test]
    fn polygon_with_hole_round_trip() {
        // Exterior ring + one interior ring (hole) (CONCEPT:EG-KG.domains.geometry-collections).
        let src = "POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0), (3 3, 7 3, 7 7, 3 7, 3 3))";
        let g = parse(src).unwrap();
        if let Geometry::Polygon(pg) = &g {
            assert_eq!(pg.interiors.len(), 1);
        } else {
            panic!("expected Polygon");
        }
        assert_eq!(to_wkt(&g), src);
        assert_eq!(parse(&to_wkt(&g)).unwrap(), g);
    }

    #[test]
    fn multipoint_round_trip_both_forms() {
        // Non-parenthesized form.
        let g = parse("MULTIPOINT (10 40, 40 30, 20 20)").unwrap();
        assert_eq!(
            g,
            Geometry::MultiPoint(vec![
                Point::new(10.0, 40.0),
                Point::new(40.0, 30.0),
                Point::new(20.0, 20.0),
            ])
        );
        assert_eq!(to_wkt(&g), "MULTIPOINT (10 40, 40 30, 20 20)");
        assert_eq!(parse(&to_wkt(&g)).unwrap(), g);
        // Parenthesized form parses to the same value.
        let g2 = parse("MULTIPOINT ((10 40), (40 30), (20 20))").unwrap();
        assert_eq!(g2, g);
    }

    #[test]
    fn multilinestring_round_trip() {
        let src = "MULTILINESTRING ((10 10, 20 20), (40 40, 30 30, 40 20))";
        let g = parse(src).unwrap();
        assert!(matches!(g, Geometry::MultiLineString(ref v) if v.len() == 2));
        assert_eq!(to_wkt(&g), src);
        assert_eq!(parse(&to_wkt(&g)).unwrap(), g);
    }

    #[test]
    fn multipolygon_round_trip_with_hole() {
        let src = "MULTIPOLYGON (((0 0, 4 0, 4 4, 0 4, 0 0)), ((10 10, 20 10, 20 20, 10 20, 10 10), (13 13, 17 13, 17 17, 13 17, 13 13)))";
        let g = parse(src).unwrap();
        if let Geometry::MultiPolygon(v) = &g {
            assert_eq!(v.len(), 2);
            assert_eq!(v[1].interiors.len(), 1);
        } else {
            panic!("expected MultiPolygon");
        }
        assert_eq!(to_wkt(&g), src);
        assert_eq!(parse(&to_wkt(&g)).unwrap(), g);
    }

    #[test]
    fn geometry_collection_round_trip() {
        let src = "GEOMETRYCOLLECTION (POINT (4 6), LINESTRING (4 6, 7 10))";
        let g = parse(src).unwrap();
        assert!(matches!(g, Geometry::GeometryCollection(ref v) if v.len() == 2));
        assert_eq!(to_wkt(&g), src);
        assert_eq!(parse(&to_wkt(&g)).unwrap(), g);
    }

    #[test]
    fn ewkt_srid_prefix_tolerated() {
        let g = parse("SRID=4326;POINT (30 10)").unwrap();
        assert_eq!(g, Geometry::Point(Point::new(30.0, 10.0)));
        // whitespace + lowercase srid
        let g2 = parse("srid=4326; POLYGON ((0 0, 1 0, 1 1, 0 0))").unwrap();
        assert!(matches!(g2, Geometry::Polygon(_)));
    }

    #[test]
    fn parse_with_srid_recovers_tag() {
        let (srid, g) = parse_with_srid("SRID=4326;POINT (30 10)").unwrap();
        assert_eq!(srid, Some(4326));
        assert_eq!(g, Geometry::Point(Point::new(30.0, 10.0)));
        // No prefix ⇒ no tag; geometry still parses.
        let (srid2, g2) = parse_with_srid("POINT (1 2)").unwrap();
        assert_eq!(srid2, None);
        assert_eq!(g2, Geometry::Point(Point::new(1.0, 2.0)));
        // Web-Mercator EWKT.
        let (srid3, _) = parse_with_srid("srid=3857; POINT (0 0)").unwrap();
        assert_eq!(srid3, Some(3857));
    }

    #[test]
    fn case_insensitive_and_fractional() {
        let g = parse("point (1.5 -2.25)").unwrap();
        assert_eq!(g, Geometry::Point(Point::new(1.5, -2.25)));
        assert_eq!(to_wkt(&g), "POINT (1.5 -2.25)");
    }

    #[test]
    fn malformed_errors() {
        assert!(parse("POINT (30)").is_err());
        assert!(parse("TRIANGLE (1 2)").is_err());
        assert!(parse("POINT 30 10").is_err());
    }
}
