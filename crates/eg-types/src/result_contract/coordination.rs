//! Declared results of the `coordination` contract domain.

use crate::delegation::KgDelegateResult;

method_results! {
    visit_coordination;
    KgDelegate(KgDelegate) => Raw<KgDelegateResult>;
}
