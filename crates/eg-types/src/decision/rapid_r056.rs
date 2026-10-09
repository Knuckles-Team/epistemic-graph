//! EG-DECISION-ENGINE-R056 (`.1` slice): a statistical-path solver
//! certificate with the same verification shape as the core assembly path.
//!
//! The requirement is that the exact 0-1 solver and its certificate
//! verification apply to the statistical decision path with the same
//! deterministic guarantees as the core assembly solver -- not a looser,
//! unverified sibling. This module gives the statistical path its own
//! certificate type whose [`StatisticalSolverCertificate::verify`] refuses a
//! mismatched model digest exactly as the assembly path's certificate check
//! does. Routing an actual statistical solve through it is a later slice.

/// A certificate produced by solving the statistical path's bounded 0-1
/// program: which model it was solved against, and how many branches the
/// deterministic solver explored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatisticalSolverCertificate {
    pub model_digest: String,
    pub branch_count: u64,
}

/// Why a certificate was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CertificateRefusal {
    /// The certificate was produced against a different model than the one
    /// being verified against now.
    ModelDigestMismatch { certified: String, expected: String },
}

impl StatisticalSolverCertificate {
    /// Verify this certificate was produced against `expected_model_digest`.
    /// A statistical-path certificate that does not match fails closed, with
    /// the same guarantee the core assembly path's certificate check gives.
    pub fn verify(&self, expected_model_digest: &str) -> Result<(), CertificateRefusal> {
        if self.model_digest != expected_model_digest {
            return Err(CertificateRefusal::ModelDigestMismatch {
                certified: self.model_digest.clone(),
                expected: expected_model_digest.to_string(),
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verifies_a_matching_certificate() {
        let certificate = StatisticalSolverCertificate {
            model_digest: "digest-a".to_string(),
            branch_count: 12,
        };
        assert!(certificate.verify("digest-a").is_ok());
    }

    // spec: EG-DECISION-ENGINE-R056.1
    #[test]
    fn refuses_a_stale_model_digest() {
        let certificate = StatisticalSolverCertificate {
            model_digest: "digest-a".to_string(),
            branch_count: 12,
        };
        let refusal = certificate.verify("digest-b").unwrap_err();
        assert_eq!(
            refusal,
            CertificateRefusal::ModelDigestMismatch {
                certified: "digest-a".to_string(),
                expected: "digest-b".to_string(),
            }
        );
    }
}
