//! CONCEPT:EG-087 — scene-graph / 3D world-model primitives.
//!
//! Pure, deterministic 3D math (no clock/RNG, no I/O) backing the native
//! scene-graph modality on [`crate::graph::GraphCore`]: a `:SceneObject` carries a
//! local [`Pose`] (translation + rotation quaternion + scale); a parent/child
//! transform hierarchy composes local poses up the chain into a WORLD [`Pose`];
//! and an axis-aligned [`Aabb`] bounding volume supports `contains` / `intersects`
//! queries. Robotics / AR / urban-3D world models model their scene as graph nodes
//! + typed edges and get transform composition + spatial predicates natively.
//!
//! The types serialize to/from the arbitrary-JSON node property map (via
//! [`Pose::to_json`] / [`Pose::from_json`]) so they need NO storage-schema change —
//! exactly the convention the `Distribution` properties (CONCEPT:EG-086) use. All
//! math here is total and side-effect-free, so a `world_transform` replays
//! identically from the WAL / on a Raft follower.
//!
//! Transform convention (matches glTF / Unity local TRS): a point `p` in an
//! object's local space maps to its parent's space by
//! `parent(p) = translation + rotation · (scale ⊙ p)`. Two poses compose (parent ∘
//! child) as [`Pose::compose`]. NOTE: non-uniform scale under a parent rotation is
//! the standard scene-graph APPROXIMATION (it can introduce shear a single TRS
//! cannot represent); it is exact for uniform scale or axis-aligned rotations.

use serde_json::Value;

/// A 3-component vector (position / scale / axis). Deterministic pure math.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Vec3 {
    pub x: f64,
    pub y: f64,
    pub z: f64,
}

impl Vec3 {
    pub const ZERO: Vec3 = Vec3 { x: 0.0, y: 0.0, z: 0.0 };
    pub const ONE: Vec3 = Vec3 { x: 1.0, y: 1.0, z: 1.0 };

    pub fn new(x: f64, y: f64, z: f64) -> Self {
        Vec3 { x, y, z }
    }

    /// Component-wise (Hadamard) product — used to apply a scale to a vector.
    pub fn mul_comp(self, o: Vec3) -> Vec3 {
        Vec3::new(self.x * o.x, self.y * o.y, self.z * o.z)
    }

    pub fn add_vec(self, o: Vec3) -> Vec3 {
        Vec3::new(self.x + o.x, self.y + o.y, self.z + o.z)
    }

    fn cross(self, o: Vec3) -> Vec3 {
        Vec3::new(
            self.y * o.z - self.z * o.y,
            self.z * o.x - self.x * o.z,
            self.x * o.y - self.y * o.x,
        )
    }

    fn to_json(self) -> Value {
        serde_json::json!({ "x": self.x, "y": self.y, "z": self.z })
    }

    fn from_json(v: &Value) -> Option<Vec3> {
        let o = v.as_object()?;
        Some(Vec3::new(
            o.get("x")?.as_f64()?,
            o.get("y")?.as_f64()?,
            o.get("z")?.as_f64()?,
        ))
    }
}

/// A rotation quaternion `(x, y, z, w)`. Assumed unit-length (the caller supplies a
/// normalized rotation); math here is the standard Hamilton-product / sandwich
/// rotation and does not renormalize.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Quat {
    pub x: f64,
    pub y: f64,
    pub z: f64,
    pub w: f64,
}

impl Quat {
    /// The identity rotation.
    pub const IDENTITY: Quat = Quat { x: 0.0, y: 0.0, z: 0.0, w: 1.0 };

    pub fn new(x: f64, y: f64, z: f64, w: f64) -> Self {
        Quat { x, y, z, w }
    }

    /// Hamilton product `self * rhs` (apply `self`'s rotation after `rhs`'s).
    pub fn mul_quat(self, r: Quat) -> Quat {
        Quat {
            w: self.w * r.w - self.x * r.x - self.y * r.y - self.z * r.z,
            x: self.w * r.x + self.x * r.w + self.y * r.z - self.z * r.y,
            y: self.w * r.y - self.x * r.z + self.y * r.w + self.z * r.x,
            z: self.w * r.z + self.x * r.y - self.y * r.x + self.z * r.w,
        }
    }

    /// Rotate a vector by this quaternion (`v' = q·v·q⁻¹`, optimized form).
    pub fn rotate(self, v: Vec3) -> Vec3 {
        let u = Vec3::new(self.x, self.y, self.z);
        let t = u.cross(v);
        let t = Vec3::new(2.0 * t.x, 2.0 * t.y, 2.0 * t.z);
        v.add_vec(Vec3::new(self.w * t.x, self.w * t.y, self.w * t.z))
            .add_vec(u.cross(t))
    }

    fn to_json(self) -> Value {
        serde_json::json!({ "x": self.x, "y": self.y, "z": self.z, "w": self.w })
    }

    fn from_json(v: &Value) -> Option<Quat> {
        let o = v.as_object()?;
        Some(Quat::new(
            o.get("x")?.as_f64()?,
            o.get("y")?.as_f64()?,
            o.get("z")?.as_f64()?,
            o.get("w")?.as_f64()?,
        ))
    }
}

/// A local 3D transform: translation + rotation (quaternion) + scale (CONCEPT:
/// EG-087). Stored on a `:SceneObject` node and composed up the parent chain into a
/// world transform.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Pose {
    pub translation: Vec3,
    pub rotation: Quat,
    pub scale: Vec3,
}

impl Pose {
    /// The identity pose (origin, no rotation, unit scale).
    pub fn identity() -> Pose {
        Pose {
            translation: Vec3::ZERO,
            rotation: Quat::IDENTITY,
            scale: Vec3::ONE,
        }
    }

    /// Map a point from this pose's local space into its parent space:
    /// `translation + rotation · (scale ⊙ p)`.
    pub fn transform_point(&self, p: Vec3) -> Vec3 {
        self.translation
            .add_vec(self.rotation.rotate(self.scale.mul_comp(p)))
    }

    /// Compose `self` (the PARENT world pose) with `child` (a child's LOCAL pose),
    /// yielding the child's WORLD pose — i.e. `self ∘ child`. Standard local-TRS
    /// composition (glTF / Unity): scales multiply component-wise, rotations
    /// compose by quaternion product, and the child's translation is scaled +
    /// rotated by the parent before being added. Exact for uniform scale / axis-
    /// aligned rotation (see module doc for the non-uniform-scale caveat).
    pub fn compose(&self, child: &Pose) -> Pose {
        Pose {
            translation: self
                .translation
                .add_vec(self.rotation.rotate(self.scale.mul_comp(child.translation))),
            rotation: self.rotation.mul_quat(child.rotation),
            scale: self.scale.mul_comp(child.scale),
        }
    }

    /// Serialize to the `{translation, rotation, scale}` JSON stored under the
    /// node's `pose` property.
    pub fn to_json(&self) -> Value {
        serde_json::json!({
            "translation": self.translation.to_json(),
            "rotation": self.rotation.to_json(),
            "scale": self.scale.to_json(),
        })
    }

    /// Parse a [`Pose`] from a stored `pose` JSON value. A missing translation or
    /// rotation defaults to identity; a missing scale defaults to unit scale (so a
    /// pose written without scale still reads back sensibly). `None` only if a
    /// present sub-object is malformed.
    pub fn from_json(v: &Value) -> Option<Pose> {
        let o = v.as_object()?;
        let translation = match o.get("translation") {
            Some(t) => Vec3::from_json(t)?,
            None => Vec3::ZERO,
        };
        let rotation = match o.get("rotation") {
            Some(r) => Quat::from_json(r)?,
            None => Quat::IDENTITY,
        };
        let scale = match o.get("scale") {
            Some(s) => Vec3::from_json(s)?,
            None => Vec3::ONE,
        };
        Some(Pose {
            translation,
            rotation,
            scale,
        })
    }
}

/// An axis-aligned bounding volume (AABB) in an object's own frame (CONCEPT:
/// EG-087). Pure-math `contains` / `intersects` predicates back spatial queries.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Aabb {
    pub min: Vec3,
    pub max: Vec3,
}

impl Aabb {
    pub fn new(min: Vec3, max: Vec3) -> Self {
        Aabb { min, max }
    }

    /// Does this box contain point `p` (inclusive on all faces)?
    pub fn contains_point(&self, p: Vec3) -> bool {
        p.x >= self.min.x
            && p.x <= self.max.x
            && p.y >= self.min.y
            && p.y <= self.max.y
            && p.z >= self.min.z
            && p.z <= self.max.z
    }

    /// Does this box fully contain `other` (inclusive)?
    pub fn contains(&self, other: &Aabb) -> bool {
        self.contains_point(other.min) && self.contains_point(other.max)
    }

    /// Do the two boxes overlap (inclusive / touching counts as intersecting)?
    pub fn intersects(&self, other: &Aabb) -> bool {
        self.min.x <= other.max.x
            && self.max.x >= other.min.x
            && self.min.y <= other.max.y
            && self.max.y >= other.min.y
            && self.min.z <= other.max.z
            && self.max.z >= other.min.z
    }

    /// Serialize to the `{min, max}` JSON stored under the node's `aabb` property.
    pub fn to_json(&self) -> Value {
        serde_json::json!({ "min": self.min.to_json(), "max": self.max.to_json() })
    }

    /// Parse an [`Aabb`] from a stored `aabb` JSON value; `None` if malformed.
    pub fn from_json(v: &Value) -> Option<Aabb> {
        let o = v.as_object()?;
        Some(Aabb::new(
            Vec3::from_json(o.get("min")?)?,
            Vec3::from_json(o.get("max")?)?,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn approx(a: Vec3, b: Vec3) {
        let e = 1e-9;
        assert!(
            (a.x - b.x).abs() < e && (a.y - b.y).abs() < e && (a.z - b.z).abs() < e,
            "{a:?} != {b:?}"
        );
    }

    #[test]
    fn eg087_quat_rotate_90_about_z() {
        // 90° about +Z takes +X to +Y.
        let s = (std::f64::consts::FRAC_PI_4).sin();
        let q = Quat::new(0.0, 0.0, s, s); // (0,0,sin45,cos45)
        approx(q.rotate(Vec3::new(1.0, 0.0, 0.0)), Vec3::new(0.0, 1.0, 0.0));
    }

    #[test]
    fn eg087_pose_compose_identity_is_neutral() {
        let child = Pose {
            translation: Vec3::new(1.0, 2.0, 3.0),
            rotation: Quat::IDENTITY,
            scale: Vec3::new(2.0, 2.0, 2.0),
        };
        assert_eq!(Pose::identity().compose(&child), child);
    }

    #[test]
    fn eg087_aabb_contains_and_intersects() {
        let outer = Aabb::new(Vec3::ZERO, Vec3::new(10.0, 10.0, 10.0));
        let inner = Aabb::new(Vec3::new(1.0, 1.0, 1.0), Vec3::new(2.0, 2.0, 2.0));
        assert!(outer.contains(&inner));
        assert!(!inner.contains(&outer));
        assert!(outer.intersects(&inner));
        let disjoint = Aabb::new(Vec3::new(20.0, 20.0, 20.0), Vec3::new(21.0, 21.0, 21.0));
        assert!(!outer.intersects(&disjoint));
        // Touching faces count as intersecting.
        let touching = Aabb::new(Vec3::new(10.0, 0.0, 0.0), Vec3::new(11.0, 10.0, 10.0));
        assert!(outer.intersects(&touching));
    }

    #[test]
    fn eg087_pose_json_roundtrip() {
        let p = Pose {
            translation: Vec3::new(1.0, -2.0, 3.5),
            rotation: Quat::new(0.0, 0.0, 0.7071, 0.7071),
            scale: Vec3::new(1.0, 2.0, 3.0),
        };
        assert_eq!(Pose::from_json(&p.to_json()), Some(p));
    }
}
