//! Exact wide fixed-point accumulation (EH-562): every finite `f64` is an integer multiple
//! of `2^-1074`, and every product of two is an integer multiple of `2^-2148`, so a sum of
//! values (or of products) held as one wide integer at that scale is EXACT. Adding and
//! removing a value are exact inverses — no floating-point residue ever accumulates, a
//! flat window's variance numerator is exactly zero, and a step costs O(1) limb updates
//! whatever the window length.
//!
//! [`Wide`] is the accumulator: radix-`2^32` limbs held in `i64` so updates are lazy (no
//! carry propagation per update) and normalised before a read. [`Mag`] is an unsigned
//! magnitude read out of it, with the few exact operations the rolling statistics need
//! (product, scalar product and quotient, signed difference) and correct rounding to `f64`.

/// Bits per limb.
const LIMB_BITS: u32 = 32;
/// The low limb bits.
const MASK: i64 = 0xFFFF_FFFF;
/// Zero digits a quotient's dividend is extended by (96 bits of guard).
const GUARD_DIGITS: usize = 3;
/// Lazy updates allowed between normalisations: each adds < 2^32 to a limb, so a limb
/// stays far inside `i64`.
const LAZY_LIMIT: u32 = 1 << 28;

/// Bit position of `2^0` in a value accumulator: the smallest `f64` quantum is `2^-1074`.
pub(super) const SUM_BIAS: i32 = 1074;
/// Bit position of `2^0` in a product accumulator (`2^-1074 · 2^-1074`).
pub(super) const PRODUCT_BIAS: i32 = 2 * SUM_BIAS;
/// Limbs of a value accumulator: `2^-1074 ..` a window (≤ 2^20 values) of `< 2^1024`, plus a
/// signed top limb of headroom.
pub(super) const SUM_LIMBS: usize = 70;
/// Limbs of a product accumulator: `2^-2148 ..` a window of products `< 2^2048`, plus headroom.
pub(super) const PRODUCT_LIMBS: usize = 135;

/// Which way a quantity points.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Sign {
    Plus,
    Minus,
}

impl Sign {
    /// The sign of a product.
    pub(super) fn times(self, other: Sign) -> Sign {
        if self == other {
            Sign::Plus
        } else {
            Sign::Minus
        }
    }

    fn apply(self, v: i64) -> i64 {
        match self {
            Sign::Plus => v,
            Sign::Minus => -v,
        }
    }

    /// `±1.0`.
    pub(super) fn unit(self) -> f64 {
        match self {
            Sign::Plus => 1.0,
            Sign::Minus => -1.0,
        }
    }
}

/// A finite `f64` as the exact integer `± mantissa · 2^exponent` (`exponent ≥ -1074`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Exact {
    pub sign: Sign,
    pub mantissa: u64,
    pub exponent: i32,
}

impl Exact {
    /// `x` decomposed exactly (`None` when it is not finite).
    pub(super) fn of(x: f64) -> Option<Self> {
        if !x.is_finite() {
            return None;
        }
        let bits = x.to_bits();
        let biased = ((bits >> 52) & 0x7FF) as i32;
        let fraction = bits & ((1u64 << 52) - 1);
        let (mantissa, exponent) = match biased {
            0 => (fraction, -1074),
            _ => (fraction | (1u64 << 52), biased - 1075),
        };
        let sign = if bits >> 63 == 1 { Sign::Minus } else { Sign::Plus };
        Some(Self {
            sign,
            mantissa,
            exponent,
        })
    }
}

/// An exact signed wide integer accumulator (see the module docs).
#[derive(Clone, Debug)]
pub(super) struct Wide {
    limbs: Vec<i64>,
    /// Lowest limb ever touched (`limbs.len()` while empty).
    lo: usize,
    /// Highest limb that may be non-zero.
    hi: usize,
    lazy: u32,
}

impl Wide {
    pub(super) fn new(limbs: usize) -> Self {
        Self {
            limbs: vec![0; limbs],
            lo: limbs,
            hi: 0,
            lazy: 0,
        }
    }

    /// Add `sign · magnitude · 2^(bit − bias)` — `bit` is the absolute bit position.
    pub(super) fn add(&mut self, sign: Sign, magnitude: u128, bit: usize) {
        self.add_word(sign, magnitude as u64, bit);
        self.add_word(sign, (magnitude >> 64) as u64, bit + 64);
    }

    fn add_word(&mut self, sign: Sign, word: u64, bit: usize) {
        if word == 0 {
            return;
        }
        let limb = bit / LIMB_BITS as usize;
        let shifted = u128::from(word) << (bit % LIMB_BITS as usize);
        for k in 0..3 {
            let chunk = ((shifted >> (LIMB_BITS as usize * k)) as i64) & MASK;
            if chunk != 0 {
                self.limbs[limb + k] += sign.apply(chunk);
                self.lo = self.lo.min(limb + k);
                self.hi = self.hi.max(limb + k);
            }
        }
        self.lazy += 1;
        if self.lazy >= LAZY_LIMIT {
            self.normalize();
        }
    }

    /// Propagate carries: limbs below `hi` land in `[0, 2^32)`, the top limb keeps the
    /// sign (spilling upward while it does not fit a signed 32-bit limb).
    fn normalize(&mut self) {
        self.lazy = 0;
        if self.lo > self.hi {
            return;
        }
        let mut carry = 0i64;
        for limb in &mut self.limbs[self.lo..self.hi] {
            let v = *limb + carry;
            *limb = v & MASK;
            carry = v >> LIMB_BITS;
        }
        let mut top = self.limbs[self.hi] + carry;
        while !(-(1i64 << 31)..(1i64 << 31)).contains(&top) && self.hi + 1 < self.limbs.len() {
            self.limbs[self.hi] = top & MASK;
            top >>= LIMB_BITS;
            self.hi += 1;
            top += self.limbs[self.hi];
        }
        self.limbs[self.hi] = top;
    }

    /// The exact value as a sign and a magnitude.
    pub(super) fn read(&mut self) -> (Sign, Mag) {
        self.normalize();
        if self.lo > self.hi {
            return (Sign::Plus, Mag::zero());
        }
        let active = &self.limbs[self.lo..=self.hi];
        let sign = if active[active.len() - 1] < 0 {
            Sign::Minus
        } else {
            Sign::Plus
        };
        (sign, Mag::from_signed_limbs(self.lo as isize, active, sign))
    }
}

/// An unsigned wide integer `Σ digits[i] · 2^(32·(base + i))`, digits little-endian with
/// no zero top digit (zero is no digits).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Mag {
    base: isize,
    digits: Vec<u32>,
}

impl Mag {
    pub(super) fn zero() -> Self {
        Self {
            base: 0,
            digits: Vec::new(),
        }
    }

    pub(super) fn is_zero(&self) -> bool {
        self.digits.is_empty()
    }

    /// `|value|` of normalised signed limbs (`sign` is the value's sign).
    fn from_signed_limbs(base: isize, limbs: &[i64], sign: Sign) -> Self {
        let mut digits = Vec::with_capacity(limbs.len() + 1);
        let mut carry = 0i64;
        for &limb in limbs {
            let v = sign.apply(limb) + carry;
            digits.push((v & MASK) as u32);
            carry = v >> LIMB_BITS;
        }
        if carry > 0 {
            digits.push(carry as u32);
        }
        Self { base, digits }.trimmed()
    }

    fn trimmed(mut self) -> Self {
        while self.digits.last() == Some(&0) {
            self.digits.pop();
        }
        self
    }

    /// The exact product.
    pub(super) fn mul(&self, other: &Mag) -> Mag {
        if self.is_zero() || other.is_zero() {
            return Mag::zero();
        }
        let mut out = vec![0u64; self.digits.len() + other.digits.len()];
        for (i, &a) in self.digits.iter().enumerate() {
            let mut carry = 0u64;
            for (j, &b) in other.digits.iter().enumerate() {
                let t = out[i + j] + u64::from(a) * u64::from(b) + carry;
                out[i + j] = t & MASK as u64;
                carry = t >> LIMB_BITS;
            }
            out[i + other.digits.len()] = carry;
        }
        Mag {
            base: self.base + other.base,
            digits: out.into_iter().map(|d| d as u32).collect(),
        }
        .trimmed()
    }

    /// The exact product with a small factor.
    pub(super) fn scale(&self, k: u32) -> Mag {
        let mut digits = Vec::with_capacity(self.digits.len() + 1);
        let mut carry = 0u64;
        for &d in &self.digits {
            let t = u64::from(d) * u64::from(k) + carry;
            digits.push(t as u32);
            carry = t >> LIMB_BITS;
        }
        digits.push(carry as u32);
        Mag {
            base: self.base,
            digits,
        }
        .trimmed()
    }

    /// Divide in place by a small non-zero divisor; whether a remainder was dropped. The
    /// dividend is first extended by [`GUARD_DIGITS`] zero digits, so a non-zero quotient
    /// keeps more than 64 significant bits and a dropped remainder lies below the window
    /// [`Mag::round`] reads — the quotient then rounds correctly.
    pub(super) fn divide(&mut self, k: u32) -> bool {
        if self.is_zero() {
            return false;
        }
        self.digits.splice(0..0, [0u32; GUARD_DIGITS]);
        self.base -= GUARD_DIGITS as isize;
        let mut rem = 0u64;
        for d in self.digits.iter_mut().rev() {
            let cur = (rem << LIMB_BITS) | u64::from(*d);
            *d = (cur / u64::from(k)) as u32;
            rem = cur % u64::from(k);
        }
        *self = std::mem::replace(self, Mag::zero()).trimmed();
        rem != 0
    }

    /// Digit `i` counted from limb 0 (zero outside the stored range).
    fn digit_at(&self, limb: isize) -> u32 {
        usize::try_from(limb - self.base)
            .ok()
            .and_then(|i| self.digits.get(i))
            .copied()
            .unwrap_or(0)
    }

    fn top_limb(&self) -> isize {
        self.base + self.digits.len() as isize
    }

    /// `self − other` exactly, as a sign and a magnitude.
    pub(super) fn minus(&self, other: &Mag) -> (Sign, Mag) {
        let (big, small, sign) = match self.compare(other) {
            std::cmp::Ordering::Less => (other, self, Sign::Minus),
            std::cmp::Ordering::Equal | std::cmp::Ordering::Greater => (self, other, Sign::Plus),
        };
        let base = big.base.min(small.base);
        let mut digits = Vec::with_capacity((big.top_limb() - base) as usize);
        let mut borrow = 0i64;
        for limb in base..big.top_limb() {
            let v = i64::from(big.digit_at(limb)) - i64::from(small.digit_at(limb)) - borrow;
            digits.push((v & MASK) as u32);
            borrow = i64::from(v < 0);
        }
        (sign, Mag { base, digits }.trimmed())
    }

    /// `self + other` exactly.
    pub(super) fn plus(&self, other: &Mag) -> Mag {
        let base = self.base.min(other.base);
        let top = self.top_limb().max(other.top_limb());
        let mut digits = Vec::with_capacity((top - base) as usize + 1);
        let mut carry = 0u64;
        for limb in base..top {
            let t = u64::from(self.digit_at(limb)) + u64::from(other.digit_at(limb)) + carry;
            digits.push(t as u32);
            carry = t >> LIMB_BITS;
        }
        digits.push(carry as u32);
        Mag { base, digits }.trimmed()
    }

    fn compare(&self, other: &Mag) -> std::cmp::Ordering {
        let top = self.top_limb().max(other.top_limb());
        let low = self.base.min(other.base);
        (low..top)
            .rev()
            .map(|limb| self.digit_at(limb).cmp(&other.digit_at(limb)))
            .find(|o| o.is_ne())
            .unwrap_or(std::cmp::Ordering::Equal)
    }

    /// `floor(log2(value))` relative to `bias` (`None` for zero).
    pub(super) fn log2(&self, bias: i32) -> Option<i32> {
        let top = *self.digits.last()?;
        let bit = LIMB_BITS as isize * (self.top_limb() - 1) + (31 - top.leading_zeros()) as isize;
        Some(bit as i32 - bias)
    }

    /// `value · 2^(−bias + shift)` correctly rounded to nearest-even, with `inexact` marking
    /// a non-zero remainder below the stored digits (a dropped quotient remainder).
    pub(super) fn round(&self, bias: i32, shift: i32, inexact: bool) -> f64 {
        let Some(top) = self.log2(0) else {
            return 0.0;
        };
        let low = top - 63;
        let mut bits = 0u64;
        let mut sticky = inexact;
        for limb in self.base..self.top_limb() {
            let d = u64::from(self.digit_at(limb));
            let at = (LIMB_BITS as isize * limb) as i32 - low;
            bits |= place(d, at, &mut sticky);
        }
        let leading = (bits | u64::from(sticky)) as f64;
        scale_pow2(leading, low - bias + shift)
    }
}

/// Place digit `d` (worth `2^at` in the 64-bit window) into the window; bits that fall
/// below the window set `sticky`.
fn place(d: u64, at: i32, sticky: &mut bool) -> u64 {
    if at >= 64 || d == 0 {
        return 0;
    }
    if at >= 0 {
        return d << at;
    }
    let drop = (-at) as u32;
    if drop >= 64 {
        *sticky = true;
        return 0;
    }
    *sticky |= d & ((1u64 << drop) - 1) != 0;
    d >> drop
}

/// `x · 2^k`, exact while the result stays normal (in steps that never overshoot).
pub(super) fn scale_pow2(mut x: f64, mut k: i32) -> f64 {
    while k != 0 && x != 0.0 && x.is_finite() {
        let step = k.clamp(-1000, 1000);
        x *= f64::from_bits(((1023 + step) as u64) << 52);
        k -= step;
    }
    x
}

/// Load `x` (+ or −) into a value accumulator; `false` when `x` is not finite.
pub(super) fn add_value(acc: &mut Wide, x: f64, direction: Sign) -> bool {
    let Some(e) = Exact::of(x) else {
        return false;
    };
    let bit = (e.exponent + SUM_BIAS) as usize;
    acc.add(e.sign.times(direction), u128::from(e.mantissa), bit);
    true
}

/// Load `x · y` (+ or −) into a product accumulator; `false` when either is not finite.
pub(super) fn add_product(acc: &mut Wide, x: f64, y: f64, direction: Sign) -> bool {
    let (Some(a), Some(b)) = (Exact::of(x), Exact::of(y)) else {
        return false;
    };
    let bit = (a.exponent + b.exponent + PRODUCT_BIAS) as usize;
    let magnitude = u128::from(a.mantissa) * u128::from(b.mantissa);
    acc.add(a.sign.times(b.sign).times(direction), magnitude, bit);
    true
}
