//! One declaration form for the wave's closed refusal-code vocabularies.
//!
//! A typed refusal travels as the `"CODE: detail"` prefix of an error response
//! (the shape `CLUSTER_MUTATION_UNAVAILABLE` already uses), so each surface
//! needs the same three things: a closed enum, the exact token each variant is
//! reported as, and an enumeration of the whole set for the tests that pin it.
//! Writing those three by hand per surface is how five near-identical `as_str`
//! matches appear; this macro is the single definition they all expand from.

/// Declare a closed refusal-code enum with its wire tokens.
macro_rules! closed_error_codes {
    (
        $(#[$enum_meta:meta])*
        $visibility:vis enum $name:ident {
            $($(#[$variant_meta:meta])* $variant:ident => $token:literal),+ $(,)?
        }
    ) => {
        $(#[$enum_meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        $visibility enum $name {
            $($(#[$variant_meta])* $variant),+
        }

        impl $name {
            /// Every code this surface can report, in declaration order.
            $visibility const ALL: &'static [Self] = &[$(Self::$variant),+];

            /// The exact token the refusal is reported under.
            $visibility fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $token),+
                }
            }
        }

        impl core::fmt::Display for $name {
            fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
                formatter.write_str(self.as_str())
            }
        }
    };
}

pub(crate) use closed_error_codes;
