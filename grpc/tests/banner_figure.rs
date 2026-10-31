//! The banner's ASCII art, against output captured from the Go runtime.
//!
//! The fixture beside this file is `go-figure`'s own output for the same three names, printed
//! by the same `speed` font and trimmed the same way. It is the whole point of the exercise:
//! the two runtimes should announce themselves identically, and a rendering difference is
//! invisible in a unit test that only checks its own expectations.

/// Renders the fixture's names in the fixture's format, so a mismatch shows as a diff of the
/// art rather than a boolean.
fn rendered() -> String {
    ["bookstore", "my-service", "vision"]
        .iter()
        .flat_map(|name| {
            std::iter::once(format!("==={name}===")).chain(
                grpc::banner::render_figure(name)
                    .into_iter()
                    .map(|line| format!("[{line}]")),
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn the_art_matches_the_go_runtime_exactly() {
    let golden = include_str!("fixtures/figure_golden.txt");
    assert_eq!(
        rendered().trim_end(),
        golden.trim_end(),
        "banner art drifted from the Go runtime"
    );
}

#[test]
fn a_descender_earns_an_extra_row() {
    // `speed` reserves a sixth row below the baseline. A name without a descender should not
    // print it, or every banner carries a ragged blank line.
    assert_eq!(grpc::banner::render_figure("vision").len(), 5);
    assert_eq!(grpc::banner::render_figure("my-service").len(), 6);
}

#[test]
fn rows_carry_no_trailing_whitespace() {
    for line in grpc::banner::render_figure("bookstore") {
        assert_eq!(line.trim_end(), line, "trailing space in {line:?}");
    }
}

#[test]
fn a_character_the_font_lacks_is_drawn_rather_than_dropped() {
    // Outside printable ASCII the font has no glyph; Go substitutes '?' so the gap is visible.
    let art = grpc::banner::render_figure("a→b");
    assert!(!art.is_empty());
    assert_eq!(art.len(), grpc::banner::render_figure("a?b").len());
}

#[test]
fn an_empty_name_renders_nothing_rather_than_panicking() {
    assert!(grpc::banner::render_figure("").is_empty());
}
