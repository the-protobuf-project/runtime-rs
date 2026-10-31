//! The service name as ASCII art, in the same font the Go runtime uses.

use std::sync::OnceLock;

use super::font::Font;

/// The FIGlet font the banner renders in, vendored from `go-figure` so both runtimes print the
/// same thing.
///
/// It is `speed.flf` — Claude Martins, 1995, based on Slant — which carries its own permission
/// to redistribute. Embedding it rather than reading it at startup means a deployed binary has
/// no file to lose.
const SPEED_FONT: &str = include_str!("../../assets/speed.flf");

/// The parsed font, built once. Parsing is cheap but the banner is printed once per process,
/// so there is no reason for it to be more than that.
fn speed() -> Option<&'static Font> {
    static FONT: OnceLock<Option<Font>> = OnceLock::new();
    FONT.get_or_init(|| Font::parse(SPEED_FONT)).as_ref()
}

/// Renders `text` as ASCII art, trimmed of leading and trailing blank rows.
///
/// A banner is decoration: a font that will not parse returns no rows rather than an error,
/// because a server that came up correctly should not fail on the way to announcing itself.
pub fn render(text: &str) -> Vec<String> {
    let Some(font) = speed() else {
        return Vec::new();
    };
    let rows = font.render(text);

    let first = rows
        .iter()
        .position(|row| !row.trim().is_empty())
        .unwrap_or(rows.len());
    let last = rows
        .iter()
        .rposition(|row| !row.trim().is_empty())
        .map_or(first, |i| i + 1);

    rows[first..last].to_vec()
}
