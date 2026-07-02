//! A hand-written WKT (Well-Known Text) codec (CONCEPT:EG-083) for the geometry
//! subset the engine models: `POINT`, `LINESTRING`, `POLYGON` (exterior ring). No
//! dependency — a tiny recursive-descent parser + a `Display`-style serializer, so
//! the crate stays pure-Rust and Pi-lean.
//!
//! Grammar (case-insensitive keyword, whitespace-tolerant):
//! ```text
//! POINT (x y)
//! LINESTRING (x y, x y, ...)
//! POLYGON ((x y, x y, ...))
//! ```

use crate::geometry::{Geometry, LineString, Point, Polygon};

/// Parse a WKT string into a [`Geometry`]. Returns `Err(msg)` on any malformed input.
pub fn parse(input: &str) -> Result<Geometry, String> {
    let s = input.trim();
    let upper = s.to_ascii_uppercase();
    if let Some(rest) = upper.strip_prefix("POINT") {
        let body = coord_body(&s[s.len() - rest.len()..])?;
        let pts = parse_coord_list(&body)?;
        if pts.len() != 1 {
            return Err(format!("POINT expects 1 coordinate, got {}", pts.len()));
        }
        Ok(Geometry::Point(pts[0]))
    } else if let Some(rest) = upper.strip_prefix("LINESTRING") {
        let body = coord_body(&s[s.len() - rest.len()..])?;
        let pts = parse_coord_list(&body)?;
        if pts.len() < 2 {
            return Err("LINESTRING expects >= 2 coordinates".into());
        }
        Ok(Geometry::LineString(LineString::new(pts)))
    } else if let Some(rest) = upper.strip_prefix("POLYGON") {
        // POLYGON ( ( ring ) ) — v1 reads the FIRST (exterior) ring; extra rings/holes
        // are a documented follow-up.
        let outer = coord_body(&s[s.len() - rest.len()..])?;
        let inner = coord_body(outer.trim())?;
        let pts = parse_coord_list(&inner)?;
        if pts.len() < 3 {
            return Err("POLYGON ring expects >= 3 coordinates".into());
        }
        Ok(Geometry::Polygon(Polygon::new(LineString::new(pts))))
    } else {
        Err(format!("unsupported or malformed WKT: {input}"))
    }
}

/// Extract the text between the OUTERMOST matched parentheses of `s`.
fn coord_body(s: &str) -> Result<String, String> {
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

/// Parse a comma-separated list of `x y` coordinate pairs.
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
    fn coords(pts: &[Point]) -> String {
        pts.iter()
            .map(|p| format!("{} {}", fmt_num(p.x), fmt_num(p.y)))
            .collect::<Vec<_>>()
            .join(", ")
    }
    match g {
        Geometry::Point(p) => format!("POINT ({} {})", fmt_num(p.x), fmt_num(p.y)),
        Geometry::LineString(l) => format!("LINESTRING ({})", coords(&l.points)),
        Geometry::Polygon(pg) => format!("POLYGON (({}))", coords(&pg.exterior.points)),
    }
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
        // re-parse the serialized form → identical geometry.
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
