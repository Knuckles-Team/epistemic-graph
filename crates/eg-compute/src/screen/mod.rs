//! Screen-observation enrichment (CONCEPT:KG-2.185): turn a captured desktop frame
//! (screenshot + accessibility tree) into durable session/frame/UIElement graph
//! entities. See `observe.rs`. Always compiled (dep-free).
pub mod observe;

pub use observe::{
    observe_screen, ScreenObservationInput, ScreenObservationResult, UiElementInput,
};
