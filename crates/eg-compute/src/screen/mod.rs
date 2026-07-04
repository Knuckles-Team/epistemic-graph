//! Screen-observation enrichment (CONCEPT:AU-KG.ontology.owl-screen-bridge): turn a captured desktop frame
//! (screenshot + accessibility tree) into durable session/frame/UIElement graph
//! entities. See `observe.rs`. Always compiled (dep-free).
pub mod observe;

pub use observe::{
    observe_screen, ScreenObservationInput, ScreenObservationResult, UiElementInput,
};
