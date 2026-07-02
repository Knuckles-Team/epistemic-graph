//! **GPX — GPS Exchange Format** track/route/waypoint reader (CONCEPT:EG-264).
//!
//! GPX is the ubiquitous XML container for GPS traces (Garmin, Strava, OSM, phone apps). This
//! is a *hand-rolled* lightweight reader — NO XML/serde-xml C dep — that extracts the three
//! GPX geometry-bearing elements into eg-geo geometries:
//!
//! * `<wpt lat=".." lon="..">` — standalone **waypoints** → [`Point`]s.
//! * `<rte>…<rtept lat lon>…</rte>` — a **route** → one [`LineString`] per `<rte>`.
//! * `<trk>…<trkseg>…<trkpt lat lon>…</trkseg>…</trk>` — a **track**: one [`LineString`] per
//!   `<trkseg>` (a track's multiple segments each become a line).
//!
//! GPX stores coordinates as `lat`/`lon` **attributes** (WGS84, EPSG:4326); eg-geo geometries
//! are `(x = lon, y = lat)`, so this reader maps `x = lon`, `y = lat` to match the WKT/geodesic
//! convention. Writing GPX and elevation/time tags are documented follow-ups.

use crate::geometry::{Geometry, LineString, Point};

/// The parsed contents of a GPX document (CONCEPT:EG-264).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Gpx {
    /// Standalone `<wpt>` waypoints as points (`x = lon`, `y = lat`).
    pub waypoints: Vec<Point>,
    /// One line per `<rte>` route.
    pub routes: Vec<LineString>,
    /// One line per `<trkseg>` track segment (across all `<trk>`s).
    pub tracks: Vec<LineString>,
}

impl Gpx {
    /// Flatten everything into a single [`Geometry::GeometryCollection`] (CONCEPT:EG-264):
    /// waypoints as `Point`s, then routes and track segments as `LineString`s.
    pub fn to_geometry(&self) -> Geometry {
        let mut parts: Vec<Geometry> = Vec::new();
        for p in &self.waypoints {
            parts.push(Geometry::Point(*p));
        }
        for r in &self.routes {
            parts.push(Geometry::LineString(r.clone()));
        }
        for t in &self.tracks {
            parts.push(Geometry::LineString(t.clone()));
        }
        Geometry::GeometryCollection(parts)
    }
}

/// Parse GPX XML text into a [`Gpx`] (CONCEPT:EG-264). Tolerant of attribute order,
/// single/double quotes and self-closing tags; ignores unknown elements. Errors only when a
/// `lat`/`lon` attribute is present but unparsable.
pub fn read_gpx(xml: &str) -> Result<Gpx, String> {
    let mut gpx = Gpx::default();
    // Context: are we inside a <trkseg> / <rte>, and the line being accumulated.
    let mut in_trkseg = false;
    let mut in_rte = false;
    let mut cur_seg: Vec<Point> = Vec::new();
    let mut cur_rte: Vec<Point> = Vec::new();

    for tag in TagIter::new(xml) {
        let name = tag.name();
        match name {
            "trkseg" => {
                if tag.is_close {
                    gpx.tracks
                        .push(LineString::new(std::mem::take(&mut cur_seg)));
                    in_trkseg = false;
                } else if !tag.is_self_closing {
                    in_trkseg = true;
                    cur_seg.clear();
                }
            }
            "rte" => {
                if tag.is_close {
                    gpx.routes
                        .push(LineString::new(std::mem::take(&mut cur_rte)));
                    in_rte = false;
                } else if !tag.is_self_closing {
                    in_rte = true;
                    cur_rte.clear();
                }
            }
            "trkpt" | "rtept" | "wpt" if !tag.is_close => {
                let p = tag.lat_lon_point()?;
                match name {
                    "trkpt" if in_trkseg => cur_seg.push(p),
                    "rtept" if in_rte => cur_rte.push(p),
                    "wpt" => gpx.waypoints.push(p),
                    // A trkpt/rtept outside its container: attach best-effort.
                    "trkpt" => cur_seg.push(p),
                    "rtept" => cur_rte.push(p),
                    _ => {}
                }
            }
            _ => {}
        }
    }
    // Flush any unterminated containers (lenient).
    if !cur_seg.is_empty() {
        gpx.tracks.push(LineString::new(cur_seg));
    }
    if !cur_rte.is_empty() {
        gpx.routes.push(LineString::new(cur_rte));
    }
    Ok(gpx)
}

// ── a minimal XML tag scanner ──────────────────────────────────────────────────────

/// One scanned XML tag: its raw inner text (between `<` and `>`), open/close/self-closing flags.
struct Tag<'a> {
    inner: &'a str,
    is_close: bool,
    is_self_closing: bool,
}

impl<'a> Tag<'a> {
    /// The element name (lower-cased comparison is avoided; GPX element names are lower-case).
    fn name(&self) -> &'a str {
        let s = self.inner.trim_start_matches('/').trim_start();
        let end = s
            .find(|c: char| c.is_whitespace() || c == '/')
            .unwrap_or(s.len());
        &s[..end]
    }

    /// Read the `lat`/`lon` attributes as a `(x = lon, y = lat)` point.
    fn lat_lon_point(&self) -> Result<Point, String> {
        let lat = self
            .attr("lat")
            .ok_or_else(|| "GPX: point missing lat".to_string())?;
        let lon = self
            .attr("lon")
            .ok_or_else(|| "GPX: point missing lon".to_string())?;
        let lat: f64 = lat.parse().map_err(|_| format!("GPX: bad lat {lat:?}"))?;
        let lon: f64 = lon.parse().map_err(|_| format!("GPX: bad lon {lon:?}"))?;
        Ok(Point::new(lon, lat))
    }

    /// Extract the value of attribute `key` (`key="v"` or `key='v'`), if present.
    fn attr(&self, key: &str) -> Option<&'a str> {
        let mut rest = self.inner;
        while let Some(pos) = rest.find(key) {
            let after = &rest[pos + key.len()..];
            let trimmed = after.trim_start();
            // Ensure this is the attribute name (preceded by whitespace/start, followed by `=`).
            let boundary_ok = pos == 0
                || rest[..pos]
                    .chars()
                    .next_back()
                    .map(|c| c.is_whitespace())
                    .unwrap_or(false);
            if boundary_ok && trimmed.starts_with('=') {
                let val = trimmed[1..].trim_start();
                let quote = val.chars().next()?;
                if quote == '"' || quote == '\'' {
                    let end = val[1..].find(quote)? + 1;
                    return Some(&val[1..end]);
                }
            }
            rest = &rest[pos + key.len()..];
        }
        None
    }
}

/// Iterator yielding each `<…>` tag in an XML string, skipping comments/`<?…?>`/`<!…>`.
struct TagIter<'a> {
    s: &'a str,
    pos: usize,
}

impl<'a> TagIter<'a> {
    fn new(s: &'a str) -> Self {
        Self { s, pos: 0 }
    }
}

impl<'a> Iterator for TagIter<'a> {
    type Item = Tag<'a>;

    fn next(&mut self) -> Option<Tag<'a>> {
        let bytes = self.s.as_bytes();
        while self.pos < self.s.len() {
            let lt = self.s[self.pos..].find('<')? + self.pos;
            let gt = self.s[lt..].find('>')? + lt;
            let raw = &self.s[lt + 1..gt];
            self.pos = gt + 1;
            // Skip declarations, comments and processing instructions.
            if raw.starts_with('?') || raw.starts_with('!') {
                continue;
            }
            let is_close = raw.starts_with('/');
            let is_self_closing = raw.ends_with('/');
            let inner = raw.trim_end_matches('/');
            let _ = bytes; // keep byte view for potential future speedups
            return Some(Tag {
                inner,
                is_close,
                is_self_closing,
            });
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"<?xml version="1.0"?>
<gpx version="1.1" creator="eg-geo">
  <wpt lat="48.8566" lon="2.3522"><name>Paris</name></wpt>
  <rte>
    <rtept lat="51.5074" lon="-0.1278"/>
    <rtept lat="52.5200" lon="13.4050"/>
  </rte>
  <trk>
    <trkseg>
      <trkpt lat="40.7128" lon="-74.0060"/>
      <trkpt lat="34.0522" lon="-118.2437"/>
      <trkpt lat="41.8781" lon="-87.6298"/>
    </trkseg>
    <trkseg>
      <trkpt lat="37.7749" lon="-122.4194"/>
      <trkpt lat="47.6062" lon="-122.3321"/>
    </trkseg>
  </trk>
</gpx>"#;

    #[test]
    fn eg264_gpx_reads_waypoints_routes_tracks() {
        let g = read_gpx(SAMPLE).expect("parse GPX");
        // One waypoint (lon, lat).
        assert_eq!(g.waypoints, vec![Point::new(2.3522, 48.8566)]);
        // One route with two points.
        assert_eq!(g.routes.len(), 1);
        assert_eq!(g.routes[0].points.len(), 2);
        assert_eq!(g.routes[0].points[0], Point::new(-0.1278, 51.5074));
        // Two track segments (3 and 2 points).
        assert_eq!(g.tracks.len(), 2);
        assert_eq!(g.tracks[0].points.len(), 3);
        assert_eq!(g.tracks[1].points.len(), 2);
        assert_eq!(g.tracks[0].points[0], Point::new(-74.0060, 40.7128));
    }

    #[test]
    fn eg264_gpx_to_geometry_collection() {
        let g = read_gpx(SAMPLE).unwrap();
        match g.to_geometry() {
            Geometry::GeometryCollection(parts) => {
                // 1 waypoint + 1 route + 2 track segments = 4 parts.
                assert_eq!(parts.len(), 4);
                assert!(matches!(parts[0], Geometry::Point(_)));
                assert!(matches!(parts[1], Geometry::LineString(_)));
            }
            _ => panic!("expected GeometryCollection"),
        }
    }

    #[test]
    fn eg264_gpx_handles_single_quotes_and_attr_order() {
        let xml = r#"<gpx><wpt lon='10.5' lat='20.25'/></gpx>"#;
        let g = read_gpx(xml).unwrap();
        assert_eq!(g.waypoints, vec![Point::new(10.5, 20.25)]);
    }

    #[test]
    fn eg264_gpx_bad_coordinate_errors() {
        let xml = r#"<gpx><wpt lat="notanumber" lon="1.0"/></gpx>"#;
        assert!(read_gpx(xml).is_err());
    }

    #[test]
    fn eg264_gpx_empty_document() {
        let g = read_gpx("<gpx></gpx>").unwrap();
        assert!(g.waypoints.is_empty() && g.routes.is_empty() && g.tracks.is_empty());
    }
}
