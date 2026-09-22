//! Opaque, tenant-bound resume cursors for keyset-paged reads.
//!
//! One owner for the cursor shape `AgentComponent.Search` introduced and every
//! later paged read reuses (`ListWorkItems`): a truncated SHA-256 tag over
//! `(family domain, tenant, resume key)` followed by the hex resume key.
//!
//! The tag is what stops a cursor from being transplanted. Resumption always
//! scans under the REQUEST's tenant, so a foreign cursor could never reach
//! another tenant's rows -- but it could silently resume at a meaningless
//! offset, and a named refusal is better than a quiet wrong answer. The family
//! domain does the same between reads: a search cursor never resumes a
//! WorkItem listing.

use sha2::{Digest, Sha256};

/// How many bytes of the SHA-256 tag a cursor carries.
const TAG_BYTES: usize = 16;

/// One paged read's cursor family.
#[derive(Debug, Clone, Copy)]
pub struct CursorFamily {
    /// Domain-separation prefix hashed ahead of the tenant and key.
    pub domain: &'static [u8],
    /// Longest cursor a caller may hand back.
    pub max_bytes: usize,
    /// How refusals name this read, e.g. `"agent component search"`.
    pub noun: &'static str,
}

impl CursorFamily {
    /// Mint the cursor that resumes this family's scan after `resume_key`.
    pub fn encode(&self, tenant_id: &str, resume_key: &str) -> String {
        let tag = self.tag(tenant_id, resume_key);
        format!(
            "{}{}",
            hex::encode(&tag[..TAG_BYTES]),
            hex::encode(resume_key.as_bytes())
        )
    }

    /// Recover the resume key from `cursor`, or refuse it by name.
    ///
    /// `key_is_valid` is the family's own rule for a resume key; a key it
    /// rejects is reported as malformed, exactly like undecodable hex.
    pub fn decode(
        &self,
        tenant_id: &str,
        cursor: &str,
        key_is_valid: fn(&str) -> bool,
    ) -> Result<String, String> {
        let resume_key = self
            .resume_key(cursor)
            .filter(|key| key_is_valid(key))
            .ok_or_else(|| format!("{} cursor is malformed", self.noun))?;
        if self.encode(tenant_id, &resume_key) != cursor {
            return Err(format!(
                "{} cursor was not minted for this tenant",
                self.noun
            ));
        }
        Ok(resume_key)
    }

    /// The hex-decoded key half of a cursor, when its framing is well formed.
    /// `str::get` (not indexing) so a non-ASCII cursor is refused rather than
    /// panicking on a char boundary.
    fn resume_key(&self, cursor: &str) -> Option<String> {
        const TAG_HEX: usize = TAG_BYTES * 2;
        if cursor.len() <= TAG_HEX
            || cursor.len() > self.max_bytes
            || !cursor.len().is_multiple_of(2)
        {
            return None;
        }
        let raw = hex::decode(cursor.get(TAG_HEX..)?).ok()?;
        String::from_utf8(raw).ok()
    }

    fn tag(&self, tenant_id: &str, resume_key: &str) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(self.domain);
        for text in [tenant_id, resume_key] {
            hasher.update((text.len() as u64).to_be_bytes());
            hasher.update(text.as_bytes());
        }
        hasher.finalize().into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const FAMILY: CursorFamily = CursorFamily {
        domain: b"eg/test-cursor/v1",
        max_bytes: 256,
        noun: "test listing",
    };

    const OTHER: CursorFamily = CursorFamily {
        domain: b"eg/other-cursor/v1",
        ..FAMILY
    };

    fn any_key(_: &str) -> bool {
        true
    }

    #[test]
    fn a_cursor_round_trips_only_for_its_own_tenant_and_family() {
        let cursor = FAMILY.encode("tenant-a", "row-7");
        assert_eq!(
            FAMILY.decode("tenant-a", &cursor, any_key).unwrap(),
            "row-7"
        );
        let foreign = FAMILY.decode("tenant-b", &cursor, any_key).unwrap_err();
        assert_eq!(
            foreign,
            "test listing cursor was not minted for this tenant"
        );
        assert!(OTHER.decode("tenant-a", &cursor, any_key).is_err());
    }

    #[test]
    fn malformed_cursors_are_refused_without_panicking() {
        let too_long = "a".repeat(FAMILY.max_bytes + 2);
        let non_ascii = format!("{}é{}", "a".repeat(31), "b".repeat(33));
        for cursor in ["", "abc", "zz", too_long.as_str(), non_ascii.as_str()] {
            assert_eq!(
                FAMILY.decode("tenant-a", cursor, any_key).unwrap_err(),
                "test listing cursor is malformed",
                "{cursor:?}"
            );
        }
        let cursor = FAMILY.encode("tenant-a", "row-7");
        assert!(FAMILY.decode("tenant-a", &cursor, |_| false).is_err());
    }
}
