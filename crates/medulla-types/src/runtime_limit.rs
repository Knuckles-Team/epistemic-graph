//! Shared parsing for positive runtime limits read from the environment.

/// Read a positive setting on every call, falling back to `default` for an
/// absent, malformed, or zero value. This is shared by the server maintenance
/// limits and the federation query budget.
pub fn positive_from_env<T>(variable: &str, default: T) -> T
where
    T: std::str::FromStr + PartialOrd + Default,
{
    std::env::var(variable)
        .ok()
        .and_then(|value| value.trim().parse::<T>().ok())
        .filter(|value| *value > T::default())
        .unwrap_or(default)
}
