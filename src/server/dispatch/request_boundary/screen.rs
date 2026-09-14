use super::*;

#[derive(serde::Deserialize)]
struct ScreenObservationWire {
    session_id: String,
    #[serde(default)]
    frame_seq: u64,
    #[serde(default)]
    prev_frame_id: String,
    #[serde(default)]
    prev_hash: u64,
    #[serde(with = "serde_bytes", default)]
    png: Vec<u8>,
    #[serde(default)]
    elements: Vec<crate::screen::UiElementInput>,
}

/// Session identity and previous-frame lineage. Checked in the ORIGINAL order:
/// the session identifier first, then the previous-frame identifier's own shape,
/// then whether it names an earlier frame of THIS session — an observation
/// invalid on more than one of these keeps reporting the first.
fn validate_screen_session(wire: &ScreenObservationWire) -> Result<(), String> {
    if wire.session_id.is_empty()
        || wire.session_id.len() > MAX_SCREEN_SESSION_ID_BYTES
        || !wire
            .session_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err("invalid screen observation session identifier".to_string());
    }
    if wire.prev_frame_id.len() > MAX_SCREEN_PREVIOUS_ID_BYTES
        || wire.prev_frame_id.chars().any(char::is_control)
    {
        return Err("invalid previous screen observation identifier".to_string());
    }
    if wire.prev_frame_id.is_empty() {
        return Ok(());
    }
    let prefix = format!("screenobservation:{}:", wire.session_id);
    let previous_sequence = wire
        .prev_frame_id
        .strip_prefix(&prefix)
        .and_then(|suffix| suffix.parse::<u64>().ok())
        .filter(|sequence| *sequence < wire.frame_seq);
    if previous_sequence.is_none() {
        return Err(
            "previous screen observation must belong to the same earlier session frame".to_string(),
        );
    }
    Ok(())
}

fn validate_screen_png_dimensions(width: u32, height: u32) -> Result<(), String> {
    if width == 0
        || height == 0
        || width > MAX_SCREEN_DIMENSION
        || height > MAX_SCREEN_DIMENSION
        || u64::from(width) * u64::from(height) > MAX_SCREEN_PIXELS
    {
        return Err("screen image dimensions exceed the resource limit".to_string());
    }
    Ok(())
}

/// Byte budget first, then the PNG signature/IHDR header, then the declared
/// dimensions read out of that header. An absent image is allowed.
fn validate_screen_png(png: &[u8]) -> Result<(), String> {
    if png.len() > MAX_SCREEN_PNG_BYTES {
        return Err("screen image exceeds the resource limit".to_string());
    }
    if png.is_empty() {
        return Ok(());
    }
    const PNG_SIGNATURE: &[u8] = b"\x89PNG\r\n\x1a\n";
    if png.len() < 24 || !png.starts_with(PNG_SIGNATURE) || &png[12..16] != b"IHDR" {
        return Err("screen image must be a PNG".to_string());
    }
    let width = u32::from_be_bytes(png[16..20].try_into().unwrap_or([0; 4]));
    let height = u32::from_be_bytes(png[20..24].try_into().unwrap_or([0; 4]));
    validate_screen_png_dimensions(width, height)
}

fn screen_element_text_within_policy(element: &crate::screen::UiElementInput) -> bool {
    !element.role.is_empty()
        && element.role.len() <= MAX_SCREEN_ROLE_BYTES
        && !element.role.chars().any(char::is_control)
        && element.name.len() <= MAX_SCREEN_ELEMENT_NAME_BYTES
        && !element.name.contains('\0')
}

fn screen_element_box_within_policy(element: &crate::screen::UiElementInput) -> bool {
    element.x.unsigned_abs() <= MAX_SCREEN_COORDINATE_ABS as u64
        && element.y.unsigned_abs() <= MAX_SCREEN_COORDINATE_ABS as u64
        && element.w >= 0
        && element.h >= 0
        && element.w <= MAX_SCREEN_COORDINATE_ABS
        && element.h <= MAX_SCREEN_COORDINATE_ABS
}

/// Element cardinality, then each element's own policy, then the RUNNING text
/// budget — the running total is what bounds a request built from many small
/// but individually legal elements.
fn validate_screen_elements(elements: &[crate::screen::UiElementInput]) -> Result<(), String> {
    if elements.len() > MAX_SCREEN_ELEMENTS {
        return Err("screen element count exceeds the resource limit".to_string());
    }
    let mut text_bytes = 0usize;
    for element in elements {
        if !screen_element_text_within_policy(element) || !screen_element_box_within_policy(element)
        {
            return Err("screen element violates the input policy".to_string());
        }
        text_bytes = text_bytes
            .checked_add(element.role.len())
            .and_then(|total| total.checked_add(element.name.len()))
            .ok_or_else(|| "screen element text exceeds the resource limit".to_string())?;
        if text_bytes > MAX_SCREEN_TOTAL_TEXT_BYTES {
            return Err("screen element text exceeds the resource limit".to_string());
        }
    }
    Ok(())
}

pub(crate) fn decode_screen_observation(
    obs_msgpack: &[u8],
) -> Result<crate::screen::ScreenObservationInput, String> {
    let wire: ScreenObservationWire = eg_types::msgpack::decode_bounded(
        obs_msgpack,
        eg_types::msgpack::MsgpackLimits::new(
            MAX_SCREEN_OBSERVATION_BYTES,
            MAX_SCREEN_OBSERVATION_ITEMS,
            64,
        ),
    )
    .map_err(|_| "invalid screen observation payload".to_string())?;

    validate_screen_session(&wire)?;
    validate_screen_png(&wire.png)?;
    validate_screen_elements(&wire.elements)?;

    Ok(crate::screen::ScreenObservationInput {
        session_id: wire.session_id,
        frame_seq: wire.frame_seq,
        prev_frame_id: wire.prev_frame_id,
        prev_hash: wire.prev_hash,
        png: wire.png,
        elements: wire.elements,
    })
}
