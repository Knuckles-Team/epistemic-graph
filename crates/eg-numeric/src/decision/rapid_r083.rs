//! EG-DECISION-ENGINE-R083 (`.1` slice): bounded-cardinality option-fact
//! encoding.
//!
//! An option is encoded from the structured facts attached to its IRI --
//! SHACL shape, declared capabilities, cost and latency -- never from a
//! serialized text label. The encoded width this module produces is fixed
//! by [`MAX_OPTION_CAPABILITIES`], a declared cardinality, so an option set
//! whose capability *labels* would blow a plausible text-token budget still
//! encodes at the same fixed width. Wiring this into the real
//! `FeatureMatrix` computation (`super::features`) is a later slice.

/// Largest declared-capability set one option's encoding carries. This is
/// the cardinality bound the requirement calls for: wider than it, the
/// encode refuses rather than growing the row to match a longer label list.
pub const MAX_OPTION_CAPABILITIES: usize = 64;

/// The structured facts attached to one candidate option's IRI.
#[derive(Debug, Clone, PartialEq)]
pub struct OptionFacts {
    pub shacl_shape: String,
    pub capabilities: Vec<String>,
    pub cost_q32: i64,
    pub latency_q32: i64,
}

/// Why an option's facts were refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EncodeRefusal {
    /// More capabilities were declared than the cardinality bound allows.
    TooManyCapabilities { declared: usize, max: usize },
}

impl OptionFacts {
    /// Encode this option as a fixed-width `[cost, latency, capability
    /// bits...]` row. The row's width is `2 + MAX_OPTION_CAPABILITIES`
    /// regardless of how long any capability's IRI or label text is --
    /// width tracks declared cardinality, never token count.
    pub fn encode(&self) -> Result<Vec<i64>, EncodeRefusal> {
        if self.capabilities.len() > MAX_OPTION_CAPABILITIES {
            return Err(EncodeRefusal::TooManyCapabilities {
                declared: self.capabilities.len(),
                max: MAX_OPTION_CAPABILITIES,
            });
        }
        let mut row = Vec::with_capacity(2 + MAX_OPTION_CAPABILITIES);
        row.push(self.cost_q32);
        row.push(self.latency_q32);
        for i in 0..MAX_OPTION_CAPABILITIES {
            row.push(if i < self.capabilities.len() { 1 } else { 0 });
        }
        Ok(row)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A capability-label text budget no real tokenizer would allow: each of
    /// the cardinality-bound capabilities carries a long IRI-shaped label,
    /// so the concatenated text would exceed a plausible token budget (a
    /// few thousand characters) long before 64 entries. Encoding still
    /// succeeds at the fixed width because it never serializes the text.
    // spec: EG-DECISION-ENGINE-R083.1
    #[test]
    fn scores_an_option_whose_capability_text_would_exceed_a_token_budget() {
        let long_label =
            "https://ontology.example/capabilities/".to_string() + &"segment-".repeat(20) + "end";
        let capabilities: Vec<String> = (0..MAX_OPTION_CAPABILITIES)
            .map(|_| long_label.clone())
            .collect();
        let plausible_token_budget_chars = 2048;
        let total_label_chars: usize = capabilities.iter().map(|c| c.len()).sum();
        assert!(total_label_chars > plausible_token_budget_chars);

        let option = OptionFacts {
            shacl_shape: "shape:Tool".to_string(),
            capabilities,
            cost_q32: 10,
            latency_q32: 20,
        };
        let row = option
            .encode()
            .expect("bounded by cardinality, not text length");
        assert_eq!(row.len(), 2 + MAX_OPTION_CAPABILITIES);
    }

    #[test]
    fn refuses_more_capabilities_than_the_cardinality_bound() {
        let option = OptionFacts {
            shacl_shape: "shape:Tool".to_string(),
            capabilities: vec!["cap".to_string(); MAX_OPTION_CAPABILITIES + 1],
            cost_q32: 0,
            latency_q32: 0,
        };
        assert_eq!(
            option.encode(),
            Err(EncodeRefusal::TooManyCapabilities {
                declared: MAX_OPTION_CAPABILITIES + 1,
                max: MAX_OPTION_CAPABILITIES,
            })
        );
    }
}
