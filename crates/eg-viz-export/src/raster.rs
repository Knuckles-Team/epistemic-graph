//! A minimal software rasterizer: RGBA8 [`Canvas`] + [`RenderPlan`] draw-op
//! playback (D-VZ-1 lane V3a). No text/font rendering — labels/legends are a
//! documented gap for a later pass; this lane draws the marks themselves.

use crate::plan::{DrawOp, RenderPlan};

pub struct Canvas {
    pub width: u32,
    pub height: u32,
    /// Row-major RGBA8, `4 * width * height` bytes.
    pub pixels: Vec<u8>,
}

impl Canvas {
    pub fn new(width: u32, height: u32, background: [u8; 4]) -> Self {
        let mut pixels = Vec::with_capacity((width as usize) * (height as usize) * 4);
        for _ in 0..(width as usize * height as usize) {
            pixels.extend_from_slice(&background);
        }
        Self {
            width,
            height,
            pixels,
        }
    }

    #[inline]
    fn in_bounds(&self, x: i64, y: i64) -> bool {
        x >= 0 && y >= 0 && (x as u32) < self.width && (y as u32) < self.height
    }

    /// Alpha-blend `color` onto pixel `(x, y)` (source-over). A fully opaque
    /// `color` (alpha 255) is a plain overwrite.
    pub fn blend_pixel(&mut self, x: i64, y: i64, color: [u8; 4]) {
        if !self.in_bounds(x, y) {
            return;
        }
        let idx = 4 * (y as usize * self.width as usize + x as usize);
        let a = color[3] as f32 / 255.0;
        if a >= 0.999 {
            self.pixels[idx..idx + 4].copy_from_slice(&color);
            return;
        }
        for (c, &channel) in color.iter().enumerate().take(3) {
            let src = channel as f32;
            let dst = self.pixels[idx + c] as f32;
            self.pixels[idx + c] = (src * a + dst * (1.0 - a)).round().clamp(0.0, 255.0) as u8;
        }
        self.pixels[idx + 3] = 255;
    }

    /// Bresenham line, `(x0,y0)` to `(x1,y1)`, integer pixel coordinates.
    pub fn draw_line(&mut self, x0: f32, y0: f32, x1: f32, y1: f32, color: [u8; 4]) {
        let (mut x0, mut y0, x1, y1) = (
            x0.round() as i64,
            y0.round() as i64,
            x1.round() as i64,
            y1.round() as i64,
        );
        let dx = (x1 - x0).abs();
        let dy = -(y1 - y0).abs();
        let sx = if x0 < x1 { 1 } else { -1 };
        let sy = if y0 < y1 { 1 } else { -1 };
        let mut err = dx + dy;
        loop {
            self.blend_pixel(x0, y0, color);
            if x0 == x1 && y0 == y1 {
                break;
            }
            let e2 = 2 * err;
            if e2 >= dy {
                err += dy;
                x0 += sx;
            }
            if e2 <= dx {
                err += dx;
                y0 += sy;
            }
        }
    }

    pub fn fill_circle(&mut self, cx: f32, cy: f32, radius: f32, color: [u8; 4]) {
        let r = radius.max(0.5);
        let (cx_i, cy_i) = (cx.round() as i64, cy.round() as i64);
        let r_i = r.ceil() as i64;
        for dy in -r_i..=r_i {
            for dx in -r_i..=r_i {
                if (dx as f32).powi(2) + (dy as f32).powi(2) <= r * r {
                    self.blend_pixel(cx_i + dx, cy_i + dy, color);
                }
            }
        }
    }

    pub fn fill_rect(&mut self, x: f32, y: f32, w: f32, h: f32, color: [u8; 4]) {
        let x0 = x.floor().max(0.0) as i64;
        let y0 = y.floor().max(0.0) as i64;
        let x1 = (x + w).ceil() as i64;
        let y1 = (y + h).ceil() as i64;
        for py in y0..y1 {
            for px in x0..x1 {
                self.blend_pixel(px, py, color);
            }
        }
    }

    pub fn play(&mut self, plan: &RenderPlan) {
        for op in &plan.ops {
            match *op {
                DrawOp::Point {
                    x,
                    y,
                    radius,
                    color,
                } => self.fill_circle(x, y, radius, color),
                DrawOp::Segment {
                    x0,
                    y0,
                    x1,
                    y1,
                    color,
                } => self.draw_line(x0, y0, x1, y1, color),
                DrawOp::Rect { x, y, w, h, color } => self.fill_rect(x, y, w, h, color),
            }
        }
    }
}

/// Rasterize `plan` to a fresh [`Canvas`] (background fill + every draw op
/// played back in order).
pub fn rasterize(plan: &RenderPlan) -> Canvas {
    let mut canvas = Canvas::new(plan.width_px, plan.height_px, plan.background);
    canvas.play(plan);
    canvas
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_canvas_is_filled_with_background() {
        let canvas = Canvas::new(4, 4, [10, 20, 30, 255]);
        assert_eq!(&canvas.pixels[0..4], &[10, 20, 30, 255]);
        assert_eq!(canvas.pixels.len(), 4 * 4 * 4);
    }

    #[test]
    fn opaque_pixel_overwrites_background() {
        let mut canvas = Canvas::new(4, 4, [0, 0, 0, 255]);
        let (x, y, width) = (1usize, 1usize, 4usize);
        canvas.blend_pixel(x as i64, y as i64, [255, 0, 0, 255]);
        let idx = 4 * (y * width + x);
        assert_eq!(&canvas.pixels[idx..idx + 4], &[255, 0, 0, 255]);
    }

    #[test]
    fn out_of_bounds_draw_is_a_silent_noop_not_a_panic() {
        let mut canvas = Canvas::new(2, 2, [0, 0, 0, 255]);
        canvas.blend_pixel(-5, 100, [255, 255, 255, 255]);
        canvas.fill_circle(-10.0, -10.0, 3.0, [255, 255, 255, 255]);
        assert_eq!(canvas.pixels.len(), 2 * 2 * 4);
    }
}
